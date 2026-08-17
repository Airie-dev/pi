import { access, mkdir, open as openFile, readdir, rm } from "node:fs/promises";
import { basename, join } from "node:path";
import type {
	Context,
	Entry,
	ForkOptions,
	Register,
	RegisterNamespace,
	SessionCreateOptions,
} from "@earendil-works/pi-agent-core";
import { StorageBackedSession } from "@earendil-works/pi-agent-core";
import { uuidv7 } from "@earendil-works/pi-ai";
import { applyInitialSchema } from "./migrations.ts";
import { appendEntryToBranchIndex, scanBranchEntries } from "./session/branch-entries.ts";
import { decodeEntryRow, type EntryRow, EntryRowWriter } from "./session/entries.ts";
import { insertInitialMainLaneRegisters } from "./session/registers.ts";
import {
	insertSessionRow,
	metadataFromSessionRow,
	readSessionRowCount,
	readSingleSessionRow,
	type SqliteSessionMetadata,
} from "./session/session-row.ts";
import {
	claimWriterLease,
	readWriterLease,
	releaseWriterLease,
	renewWriterLease,
	type WriterLeaseRow,
} from "./session/writer-lease.ts";
import { SqliteOpenSession } from "./session.ts";
import { sql } from "./sql.ts";
import { SqliteStorage, type SqliteStorageSnapshot } from "./storage.ts";
import type { SqliteDatabase, SqliteDatabaseFactory } from "./types.ts";

export const SQLITE_STORAGE_VERSION = 1;
export const SQLITE_SESSION_EXTENSION = ".sqlite";

const DEFAULT_WRITER_LEASE_MS = 30_000;
const WRITER_LEASE_RENEW_INTERVAL_MS = DEFAULT_WRITER_LEASE_MS / 2;
const FIRST_AVAILABLE_COMMIT_SEQ = 3;

export type SqliteSessionCreateOptions = SessionCreateOptions;

export interface SqliteSessionRepoOptions {
	directory: string;
	databaseFactory: SqliteDatabaseFactory;
	now?: () => number;
}

function sessionPath(directory: string, id: string): string {
	return join(directory, `${id}${SQLITE_SESSION_EXTENSION}`);
}

function sessionIdFromPath(path: string): string {
	const name = basename(path);
	return name.endsWith(SQLITE_SESSION_EXTENSION) ? name.slice(0, -SQLITE_SESSION_EXTENSION.length) : name;
}

async function removeSessionFiles(path: string, options: { force: boolean }): Promise<void> {
	await rm(path, { force: options.force });
	await rm(`${path}-wal`, { force: true });
	await rm(`${path}-shm`, { force: true });
}

function configureConnection(db: SqliteDatabase): void {
	db.exec("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;");
}

interface RegisterRow {
	namespace: RegisterNamespace;
	key: string;
	seq: number;
	value: string;
}

interface ForkSnapshot {
	entries: Entry[];
	registers: Register[];
	messageCount: number;
	nextSeq: number;
}

function readSourceEntries(db: SqliteDatabase): Entry[] {
	return sql`SELECT id, parent_id, seq, type, custom_type, timestamp, payload
		FROM entries ORDER BY seq ASC`
		.all<EntryRow>(db)
		.map(decodeEntryRow);
}

function readSourceRegisters(db: SqliteDatabase): Register[] {
	return sql`SELECT namespace, key, seq, value FROM registers ORDER BY seq ASC`
		.all<RegisterRow>(db)
		.map(
			(row) => ({ namespace: row.namespace, key: row.key, seq: row.seq, value: JSON.parse(row.value) }) as Register,
		);
}

function isRegisterNamespace<TNamespace extends RegisterNamespace>(
	register: Register,
	namespace: TNamespace,
): register is Register<TNamespace> {
	return register.namespace === namespace;
}

function hasRegister(registers: readonly Register[], namespace: RegisterNamespace, key: string): boolean {
	return registers.some((register) => register.namespace === namespace && register.key === key);
}

function validateTreeForkSourceSnapshot(
	entriesById: ReadonlyMap<string, Entry>,
	registers: readonly Register[],
	sourceLeaves: readonly Register<"lane.leaf">[],
): void {
	const sourceLeafKeys = new Set(sourceLeaves.map((register) => register.key));
	if (!sourceLeafKeys.has("main")) throw new Error("Source session is missing main lane");
	for (const register of registers) {
		if (
			(register.namespace === "lane.config" ||
				register.namespace === "lane.state" ||
				register.namespace === "lane.lastResult") &&
			!sourceLeafKeys.has(register.key)
		) {
			throw new Error(`Source session lane ${JSON.stringify(register.key)} is missing lane.leaf`);
		}
	}
	for (const leaf of sourceLeaves) {
		if (!hasRegister(registers, "lane.state", leaf.key)) {
			throw new Error(`Source session lane ${JSON.stringify(leaf.key)} is missing lane.state`);
		}
		if (leaf.key !== "main" && !hasRegister(registers, "lane.config", leaf.key)) {
			throw new Error(`Source session lane ${JSON.stringify(leaf.key)} is missing lane.config`);
		}
		if (leaf.value !== null && !entriesById.has(leaf.value)) {
			throw new Error(`Source session lane ${JSON.stringify(leaf.key)} has an unknown leaf`);
		}
	}
}

function validateBranchForkSourceSnapshot(
	registers: readonly Register[],
	sourceLeaves: readonly Register<"lane.leaf">[],
): void {
	if (!sourceLeaves.some((register) => register.key === "main"))
		throw new Error("Source session is missing main lane");
	if (!hasRegister(registers, "lane.state", "main"))
		throw new Error('Source session lane "main" is missing lane.state');
}

function selectForkContents(
	entriesById: ReadonlyMap<string, Entry>,
	sourceLeaves: readonly Register<"lane.leaf">[],
	options: ForkOptions,
): { entryIds: Set<string>; laneToLeafId: Map<string, string | null> } {
	const entryIds = new Set<string>();
	const laneToLeafId = new Map<string, string | null>();
	if (options.scope === "tree") {
		for (const id of entriesById.keys()) entryIds.add(id);
		for (const register of sourceLeaves) laneToLeafId.set(register.key, register.value);
		return { entryIds, laneToLeafId };
	}

	const mainLeaf = sourceLeaves.find((register) => register.key === "main");
	if (mainLeaf === undefined) throw new Error("Source session is missing main lane");
	const requested = options.entryId ?? mainLeaf.value;
	let leaf = requested;
	if (requested !== null) {
		const target = entriesById.get(requested);
		if (target === undefined) throw new Error(`Unknown fork entry: ${requested}`);
		if (options.position === "before") leaf = target.parentId;
	}

	let entryId = leaf;
	while (entryId !== null) {
		const entry = entriesById.get(entryId);
		if (entry === undefined) throw new Error(`Corrupt source branch: missing parent ${entryId}`);
		entryIds.add(entryId);
		entryId = entry.parentId;
	}
	laneToLeafId.set("main", leaf);
	return { entryIds, laneToLeafId };
}

function buildForkSnapshot(source: SqliteStorageSnapshot, options: ForkOptions): ForkSnapshot {
	const sourceEntries = source.entries;
	const sourceRegisters = source.registers;
	const sourceEntriesById = new Map(sourceEntries.map((entry) => [entry.id, entry]));
	const sourceLeaves = sourceRegisters.filter((register) => isRegisterNamespace(register, "lane.leaf"));
	if (options.scope === "tree") validateTreeForkSourceSnapshot(sourceEntriesById, sourceRegisters, sourceLeaves);
	else validateBranchForkSourceSnapshot(sourceRegisters, sourceLeaves);
	const { entryIds, laneToLeafId } = selectForkContents(sourceEntriesById, sourceLeaves, options);
	const entries = sourceEntries.filter((entry) => entryIds.has(entry.id));
	const registers: Register[] = [];
	let nextSeq = Math.max(0, ...entries.map((entry) => entry.seq)) + 1;
	const setRegister = (namespace: RegisterNamespace, key: string, value: Register["value"]): void => {
		registers.push({ namespace, key, value, seq: nextSeq++ } as Register);
	};
	for (const [lane, leaf] of laneToLeafId) {
		const configuration = sourceRegisters.find(
			(register) => register.namespace === "lane.config" && register.key === lane,
		);
		if (configuration !== undefined) setRegister("lane.config", lane, configuration.value);
		setRegister("lane.leaf", lane, leaf);
		setRegister("lane.state", lane, { currentOperationId: null, pendingNextRun: [] });
	}
	for (const register of sourceRegisters) {
		if (
			register.namespace === "fact.name" ||
			register.namespace === "fact.custom" ||
			(register.namespace === "fact.label" && entryIds.has(register.key))
		) {
			setRegister(register.namespace, register.key, register.value);
		}
	}
	return {
		entries,
		registers,
		messageCount: entries.filter((entry) => entry.type === "message").length,
		nextSeq,
	};
}

function readForkSourceEntries(db: SqliteDatabase, registers: readonly Register[], options: ForkOptions): Entry[] {
	if (options.scope === "tree") return readSourceEntries(db);
	const mainLeaf = registers.find((register) => register.namespace === "lane.leaf" && register.key === "main") as
		| Register<"lane.leaf">
		| undefined;
	if (mainLeaf === undefined) throw new Error("Source session is missing main lane");
	const requested = options.entryId ?? mainLeaf.value;
	return requested === null ? [] : scanBranchEntries(db, { start: requested, order: "oldestFirst" });
}

function createSqliteForkSnapshot(sourceDb: SqliteDatabase, options: ForkOptions): ForkSnapshot {
	sourceDb.exec("BEGIN");
	let committed = false;
	try {
		const registers = readSourceRegisters(sourceDb);
		const snapshot = buildForkSnapshot(
			{ entries: readForkSourceEntries(sourceDb, registers, options), registers },
			options,
		);
		sourceDb.exec("COMMIT");
		committed = true;
		return snapshot;
	} catch (error) {
		if (!committed) sourceDb.exec("ROLLBACK");
		throw error;
	}
}

function insertForkRegister(db: SqliteDatabase, register: Register): void {
	sql`INSERT INTO registers (namespace, key, seq, value)
		VALUES (${register.namespace}, ${register.key}, ${register.seq}, ${JSON.stringify(register.value)})`.run(db);
}

function updateForkSessionStats(db: SqliteDatabase, messageCount: number): void {
	sql`UPDATE session SET message_count = ${messageCount}`.run(db);
}

export class SqliteSessionRepo {
	private readonly directory: string;
	private readonly databaseFactory: SqliteDatabaseFactory;
	private readonly now: () => number;
	private readonly pendingIds = new Set<string>();
	private readonly openStorages = new Map<string, SqliteStorage>();

	constructor(options: SqliteSessionRepoOptions) {
		this.directory = options.directory;
		this.databaseFactory = options.databaseFactory;
		this.now = options.now ?? Date.now;
	}

	async create(options: SqliteSessionCreateOptions | undefined, _context: Context): Promise<SqliteOpenSession> {
		options ??= {};
		await mkdir(this.directory, { recursive: true });
		const createdAt = this.now();
		const id = options.id ?? uuidv7(createdAt);
		this.reserveId(id);
		const path = sessionPath(this.directory, id);
		let db: SqliteDatabase | undefined;
		let lease: WriterLeaseRow | undefined;
		let reservedFile = false;
		let initialized = false;
		let session: SqliteOpenSession | undefined;
		try {
			const file = await openFile(path, "wx");
			await file.close();
			reservedFile = true;
			const activeDb = await this.databaseFactory.open(path);
			db = activeDb;
			configureConnection(activeDb);
			await applyInitialSchema(activeDb);
			const metadata: SqliteSessionMetadata = {
				id,
				createdAt,
				storageVersion: SQLITE_STORAGE_VERSION,
				...(options.parentSessionId === undefined ? {} : { parentSessionId: options.parentSessionId }),
				path,
			};
			lease = activeDb.transaction(() => {
				if (readSessionRowCount(activeDb) !== 0) throw new Error(`SQLite session already exists at ${path}`);
				insertSessionRow(activeDb, metadata, SQLITE_STORAGE_VERSION, FIRST_AVAILABLE_COMMIT_SEQ);
				insertInitialMainLaneRegisters(activeDb);
				return claimWriterLease(activeDb, uuidv7(this.now()), this.now(), DEFAULT_WRITER_LEASE_MS);
			});
			initialized = true;
			session = this.openStorageBackedSession(metadata, activeDb, lease);
			return session;
		} catch (error) {
			if (reservedFile && !initialized) await removeSessionFiles(path, { force: true });
			throw error;
		} finally {
			if (session === undefined) {
				const failedDb = db;
				const failedLease = lease;
				if (failedDb !== undefined && failedLease !== undefined) {
					failedDb.transaction(() => releaseWriterLease(failedDb, failedLease.owner_id, failedLease.fence));
				}
				db?.close();
				this.pendingIds.delete(id);
			}
		}
	}

	async open(metadata: SqliteSessionMetadata, _context: Context): Promise<SqliteOpenSession> {
		this.reserveId(metadata.id);
		let db: SqliteDatabase | undefined;
		let lease: WriterLeaseRow | undefined;
		let session: SqliteOpenSession | undefined;
		try {
			await access(metadata.path);
			const activeDb = await this.databaseFactory.open(metadata.path);
			db = activeDb;
			configureConnection(activeDb);
			const stored = activeDb.transaction(() => {
				const id = sessionIdFromPath(metadata.path);
				const stored = metadataFromSessionRow(
					metadata.path,
					id,
					readSingleSessionRow(activeDb),
					SQLITE_STORAGE_VERSION,
				);
				if (stored.id !== metadata.id) {
					throw new Error(`SQLite session path ${metadata.path} contains ${stored.id}, not ${metadata.id}`);
				}
				lease = claimWriterLease(activeDb, uuidv7(this.now()), this.now(), DEFAULT_WRITER_LEASE_MS);
				return stored;
			});
			if (lease === undefined) throw new Error("Failed to claim SQLite writer lease");
			session = this.openStorageBackedSession(stored, activeDb, lease);
			return session;
		} finally {
			if (session === undefined) {
				const failedDb = db;
				const failedLease = lease;
				if (failedDb !== undefined && failedLease !== undefined) {
					failedDb.transaction(() => releaseWriterLease(failedDb, failedLease.owner_id, failedLease.fence));
				}
				db?.close();
				this.pendingIds.delete(metadata.id);
			}
		}
	}

	async list(_options: undefined, _context: Context): Promise<SqliteSessionMetadata[]> {
		await mkdir(this.directory, { recursive: true });
		const names = await readdir(this.directory);
		const sessions: SqliteSessionMetadata[] = [];
		for (const name of names) {
			if (!name.endsWith(SQLITE_SESSION_EXTENSION)) continue;
			const path = join(this.directory, name);
			let db: SqliteDatabase | undefined;
			try {
				db = await this.databaseFactory.open(path);
				configureConnection(db);
				sessions.push(
					metadataFromSessionRow(path, sessionIdFromPath(path), readSingleSessionRow(db), SQLITE_STORAGE_VERSION),
				);
			} catch {
				// Discovery is best-effort: corrupt files, incompatible versions, and
				// unrelated *.sqlite files are reported when explicitly opened.
			} finally {
				db?.close();
			}
		}
		return sessions.sort((left, right) => right.createdAt - left.createdAt);
	}

	async delete(metadata: SqliteSessionMetadata, _context: Context): Promise<void> {
		if (this.pendingIds.has(metadata.id)) throw new Error(`Session is open: ${metadata.id}`);
		await access(metadata.path);
		const db = await this.databaseFactory.open(metadata.path);
		try {
			configureConnection(db);
			const lease = readWriterLease(db);
			if (lease !== undefined && lease.expires_at_ms > this.now()) {
				throw new Error(`SQLite session is already claimed by writer ${lease.owner_id}`);
			}
		} finally {
			db.close();
		}
		await removeSessionFiles(metadata.path, { force: false });
	}

	async fork(source: SqliteSessionMetadata, options: ForkOptions, context: Context): Promise<SqliteOpenSession> {
		const createdAt = this.now();
		const id = options.id ?? uuidv7(createdAt);
		this.reserveId(id);
		const sourceStorage = this.openStorages.get(source.id);
		const activeSourceSnapshot = sourceStorage?.snapshot(undefined, context);
		await mkdir(this.directory, { recursive: true });
		const path = sessionPath(this.directory, id);
		let db: SqliteDatabase | undefined;
		let lease: WriterLeaseRow | undefined;
		let reservedFile = false;
		let initialized = false;
		let session: SqliteOpenSession | undefined;
		try {
			const file = await openFile(path, "wx");
			await file.close();
			reservedFile = true;

			const snapshot =
				activeSourceSnapshot === undefined
					? await this.createForkSnapshotFromClosedSource(source, options)
					: buildForkSnapshot(await activeSourceSnapshot, options);

			const activeDb = await this.databaseFactory.open(path);
			db = activeDb;
			configureConnection(activeDb);
			await applyInitialSchema(activeDb);
			const metadata: SqliteSessionMetadata = {
				id,
				createdAt,
				storageVersion: SQLITE_STORAGE_VERSION,
				parentSessionId: source.id,
				path,
			};
			lease = activeDb.transaction(() => {
				insertSessionRow(activeDb, metadata, SQLITE_STORAGE_VERSION, snapshot.nextSeq);
				const entryWriter = new EntryRowWriter(activeDb);
				for (const entry of snapshot.entries) {
					entryWriter.insert(entry);
					appendEntryToBranchIndex(activeDb, entry);
				}
				for (const register of snapshot.registers) insertForkRegister(activeDb, register);
				updateForkSessionStats(activeDb, snapshot.messageCount);
				return claimWriterLease(activeDb, uuidv7(this.now()), this.now(), DEFAULT_WRITER_LEASE_MS);
			});
			initialized = true;
			session = this.openStorageBackedSession(metadata, activeDb, lease);
			return session;
		} catch (error) {
			if (reservedFile && !initialized) await removeSessionFiles(path, { force: true });
			throw error;
		} finally {
			if (session === undefined) {
				const failedDb = db;
				const failedLease = lease;
				if (failedDb !== undefined && failedLease !== undefined) {
					failedDb.transaction(() => releaseWriterLease(failedDb, failedLease.owner_id, failedLease.fence));
				}
				db?.close();
				this.pendingIds.delete(id);
			}
		}
	}

	private async createForkSnapshotFromClosedSource(
		source: SqliteSessionMetadata,
		options: ForkOptions,
	): Promise<ForkSnapshot> {
		const sourceDb = await this.databaseFactory.open(source.path);
		try {
			configureConnection(sourceDb);
			const storedSource = metadataFromSessionRow(
				source.path,
				sessionIdFromPath(source.path),
				readSingleSessionRow(sourceDb),
				SQLITE_STORAGE_VERSION,
			);
			if (storedSource.id !== source.id) {
				throw new Error(`SQLite session path ${source.path} contains ${storedSource.id}, not ${source.id}`);
			}
			return createSqliteForkSnapshot(sourceDb, options);
		} finally {
			sourceDb.close();
		}
	}

	private openStorageBackedSession(
		metadata: SqliteSessionMetadata,
		db: SqliteDatabase,
		initialLease: WriterLeaseRow,
	): SqliteOpenSession {
		let lease = initialLease;
		const renew = () => {
			lease = db.transaction(() =>
				renewWriterLease(db, lease.owner_id, lease.fence, this.now(), DEFAULT_WRITER_LEASE_MS),
			);
		};
		const storage = new SqliteStorage(db, { now: this.now, beforeCommit: renew });
		this.openStorages.set(metadata.id, storage);
		const session = new StorageBackedSession(metadata, storage);
		return new SqliteOpenSession(session, {
			renewWriterLease: renew,
			releaseWriterLease: () => db.transaction(() => releaseWriterLease(db, lease.owner_id, lease.fence)),
			renewIntervalMs: WRITER_LEASE_RENEW_INTERVAL_MS,
			onClose: () => {
				db.close();
				this.openStorages.delete(metadata.id);
				this.pendingIds.delete(metadata.id);
			},
		});
	}

	private reserveId(id: string): void {
		if (this.pendingIds.has(id)) throw new Error(`Session is already open: ${id}`);
		this.pendingIds.add(id);
	}
}
