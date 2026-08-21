import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { analyzeTypeDiscoverability } from "./check-type-discoverability.mjs";

async function createProject(files) {
	const directory = await mkdtemp(join(tmpdir(), "pi-type-discoverability-"));
	await writeFile(
		join(directory, "tsconfig.json"),
		`${JSON.stringify(
			{
				compilerOptions: {
					strict: true,
					target: "ES2022",
					module: "NodeNext",
					moduleResolution: "NodeNext",
					types: [],
					noEmit: true,
				},
				include: ["**/*.ts"],
			},
			null,
			"\t",
		)}\n`,
	);
	for (const [fileName, source] of Object.entries(files)) {
		const path = join(directory, fileName);
		await mkdir(dirname(path), { recursive: true });
		await writeFile(path, source);
	}
	return directory;
}

test("finds structural implementations that lose their named type through generic inference", async () => {
	const directory = await createProject({
		"example.ts": `interface ClientTuiServer {
\tattachSession(sessionId: number): string;
}

class ExperimentalClientTui {
\tconstructor(options: { servers: ClientTuiServer[] }) {}
}

const servers = [1, 2, 3];
new ExperimentalClientTui({
\tservers: servers.map(() => ({
\t\tattachSession: () => "inferred",
\t})),
});
new ExperimentalClientTui({
\tservers: servers.map<ClientTuiServer>(() => ({
\t\tattachSession: () => "generic",
\t})),
});
new ExperimentalClientTui({
\tservers: servers.map((): ClientTuiServer => ({
\t\tattachSession: () => "return type",
\t})),
});
new ExperimentalClientTui({
\tservers: servers.map(() => ({
\t\tattachSession: () => "satisfies",
\t} satisfies ClientTuiServer)),
});

function identity<T>(value: T): T {
\treturn value;
}
function consume(server: ClientTuiServer): void {}
consume(identity({
\tattachSession: () => "not map-specific",
}));

const unrelated = {
\tattachSession: () => "no named destination",
};
const direct: ClientTuiServer = {
\tattachSession: () => "direct",
};

interface Product {
\tname: string;
}
function makeProduct(options: { name: string }): Product {
\treturn { name: options.name };
}
function consumeProduct(product: Product): void {}
consumeProduct(makeProduct({ name: "factory input does not become the product" }));
`,
	});

	try {
		const issues = analyzeTypeDiscoverability(join(directory, "tsconfig.json"));
		assert.deepEqual(
			issues.map(({ line, target }) => ({ line, target })),
			[
				{ line: 11, target: "ClientTuiServer" },
				{ line: 35, target: "ClientTuiServer" },
			],
		);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("accepts a directly typed member of a broader named union", async () => {
	const directory = await createProject({
		"example.ts": `interface UserMessage { role: "user"; content: string }
interface AssistantMessage { role: "assistant"; content: string }
type AgentMessage = UserMessage | AssistantMessage;
function identity<T>(value: T): T { return value; }
function consume(message: AgentMessage): void {}
consume(identity({
\trole: "user",
\tcontent: "hello",
} satisfies UserMessage));
`,
	});

	try {
		const issues = analyzeTypeDiscoverability(join(directory, "tsconfig.json"));
		assert.deepEqual(issues, []);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("checks named type aliases and classes without configuration", async () => {
	const directory = await createProject({
		"example.ts": `type NamedAlias = { run(): void };
class NamedClass { label = ""; }
function identity<T>(value: T): T { return value; }
function consumeAlias(value: NamedAlias): void {}
function consumeClass(value: NamedClass): void {}
consumeAlias(identity({ run() {} }));
consumeClass(identity({ label: "class" }));
`,
	});

	try {
		const issues = analyzeTypeDiscoverability(join(directory, "tsconfig.json"));
		assert.deepEqual(
			issues.map(({ target }) => target),
			["NamedAlias", "NamedClass"],
		);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("checks explicitly selected project files and directories", async () => {
	const source = `interface NamedValue { run(): void }
function identity<T>(value: T): T { return value; }
function consume(value: NamedValue): void {}
consume(identity({ run() {} }));
`;
	const directory = await createProject({ "first.ts": source, "nested/second.ts": source });

	try {
		const fileIssues = analyzeTypeDiscoverability(join(directory, "tsconfig.json"), [join(directory, "first.ts")]);
		assert.equal(fileIssues.length, 1);
		assert.equal(fileIssues[0].fileName, join(directory, "first.ts"));

		const directoryIssues = analyzeTypeDiscoverability(join(directory, "tsconfig.json"), [join(directory, "nested")]);
		assert.equal(directoryIssues.length, 1);
		assert.equal(directoryIssues[0].fileName, join(directory, "nested/second.ts"));
		assert.equal(directoryIssues[0].target, "NamedValue");
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
