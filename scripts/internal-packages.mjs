import { lstatSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { extname, join } from "node:path";

export const INTERNAL_SCOPE = "@airie-dev";
export const UPSTREAM_SCOPE = "@earendil-works";
export const FORK_REPOSITORY_URL = "git+https://github.com/Airie-dev/pi.git";

const TEXT_EXTENSIONS = new Set([
	".cjs",
	".css",
	".d.ts",
	".html",
	".js",
	".json",
	".jsx",
	".md",
	".mjs",
	".map",
	".sh",
	".ts",
	".tsx",
	".txt",
	".yaml",
	".yml",
]);
const TEXT_FILENAMES = new Set(["LICENSE", "README", "CHANGELOG"]);
const DEPENDENCY_FIELDS = ["dependencies", "devDependencies", "optionalDependencies", "peerDependencies"];

export function validateInternalVersion(version) {
	if (!/^\d+\.\d+\.\d+-airie\.\d+$/.test(version)) {
		throw new Error(`Internal version must match <major>.<minor>.<patch>-airie.<number>: ${version}`);
	}
}

export function createPackageNameMap(packageNames) {
	const nameMap = new Map();
	for (const name of packageNames) {
		if (!name.startsWith(`${UPSTREAM_SCOPE}/`)) {
			throw new Error(`Unexpected public package outside ${UPSTREAM_SCOPE}: ${name}`);
		}
		nameMap.set(name, `${INTERNAL_SCOPE}/${name.slice(`${UPSTREAM_SCOPE}/`.length)}`);
	}
	return nameMap;
}

export function replacePackageNames(content, nameMap) {
	let rewritten = content;
	for (const [upstreamName, internalName] of [...nameMap.entries()].sort(([a], [b]) => b.length - a.length)) {
		rewritten = rewritten.replaceAll(upstreamName, internalName);
	}
	return rewritten;
}

function rewriteDependencyFields(record, internalVersion, internalNames) {
	for (const field of DEPENDENCY_FIELDS) {
		const dependencies = record[field];
		if (!dependencies || typeof dependencies !== "object") {
			continue;
		}
		for (const dependencyName of Object.keys(dependencies)) {
			if (internalNames.has(dependencyName)) {
				dependencies[dependencyName] = internalVersion;
			}
		}
	}
}

export function rewritePackageManifest(manifest, internalVersion, nameMap) {
	const rewritten = JSON.parse(replacePackageNames(JSON.stringify(manifest), nameMap));
	const internalNames = new Set(nameMap.values());

	if (internalNames.has(rewritten.name)) {
		rewritten.version = internalVersion;
		rewritten.repository = {
			type: "git",
			url: FORK_REPOSITORY_URL,
			...(manifest.repository?.directory ? { directory: manifest.repository.directory } : {}),
		};
	}
	rewriteDependencyFields(rewritten, internalVersion, internalNames);
	return rewritten;
}

function rewriteLockedPackage(record, internalVersion, internalNames, forceInternal) {
	if (!record || typeof record !== "object") {
		return;
	}
	const internal = forceInternal || internalNames.has(record.name);
	if (internal) {
		record.version = internalVersion;
		delete record.resolved;
		delete record.integrity;
	}
	rewriteDependencyFields(record, internalVersion, internalNames);
	if (record.requires && typeof record.requires === "object") {
		for (const dependencyName of Object.keys(record.requires)) {
			if (internalNames.has(dependencyName)) {
				record.requires[dependencyName] = internalVersion;
			}
		}
	}
}

export function rewritePackageLock(lock, internalVersion, nameMap) {
	const rewritten = JSON.parse(replacePackageNames(JSON.stringify(lock), nameMap));
	const internalNames = new Set(nameMap.values());

	if (internalNames.has(rewritten.name)) {
		rewritten.version = internalVersion;
	}

	if (rewritten.packages && typeof rewritten.packages === "object") {
		for (const [packagePath, record] of Object.entries(rewritten.packages)) {
			const packageName = packagePath.startsWith("node_modules/") ? packagePath.slice("node_modules/".length) : null;
			const forceInternal = packagePath === "" ? internalNames.has(rewritten.name) : packageName !== null && internalNames.has(packageName);
			rewriteLockedPackage(record, internalVersion, internalNames, forceInternal);
		}
	}

	if (rewritten.dependencies && typeof rewritten.dependencies === "object") {
		for (const [dependencyName, record] of Object.entries(rewritten.dependencies)) {
			rewriteLockedPackage(record, internalVersion, internalNames, internalNames.has(dependencyName));
		}
	}

	return rewritten;
}

function isTextFile(path) {
	const extension = extname(path);
	if (TEXT_EXTENSIONS.has(extension) || path.endsWith(".d.ts")) {
		return true;
	}
	const fileName = path.split(/[\\/]/).at(-1);
	return fileName ? TEXT_FILENAMES.has(fileName) : false;
}

export function rewritePackedPackageTree(root, internalVersion, nameMap) {
	const upstreamNames = [...nameMap.keys()];
	const remainingReferences = [];

	function visit(directory) {
		for (const entry of readdirSync(directory, { withFileTypes: true })) {
			const path = join(directory, entry.name);
			if (entry.isDirectory()) {
				visit(path);
				continue;
			}
			if (entry.isSymbolicLink()) {
				continue;
			}
			if (!entry.isFile() || !isTextFile(path)) {
				continue;
			}

			const original = readFileSync(path, "utf8");
			let rewritten;
			if (entry.name === "package.json") {
				rewritten = `${JSON.stringify(rewritePackageManifest(JSON.parse(original), internalVersion, nameMap), null, "\t")}\n`;
			} else if (entry.name === "package-lock.json" || entry.name === "npm-shrinkwrap.json") {
				rewritten = `${JSON.stringify(rewritePackageLock(JSON.parse(original), internalVersion, nameMap), null, "\t")}\n`;
			} else {
				rewritten = replacePackageNames(original, nameMap);
			}
			if (rewritten !== original) {
				writeFileSync(path, rewritten);
			}
			if (upstreamNames.some((name) => rewritten.includes(name))) {
				remainingReferences.push(path);
			}
		}
	}

	if (!lstatSync(root).isDirectory()) {
		throw new Error(`Packed package root is not a directory: ${root}`);
	}
	visit(root);
	if (remainingReferences.length > 0) {
		throw new Error(`Packed package still references upstream package names:\n${remainingReferences.join("\n")}`);
	}
}

export function sortPackagesForPublication(packages) {
	const byName = new Map(packages.map((pkg) => [pkg.name, pkg]));
	const visiting = new Set();
	const visited = new Set();
	const sorted = [];

	function visit(pkg) {
		if (visited.has(pkg.name)) {
			return;
		}
		if (visiting.has(pkg.name)) {
			throw new Error(`Circular public package dependency involving ${pkg.name}`);
		}
		visiting.add(pkg.name);
		for (const field of DEPENDENCY_FIELDS) {
			for (const dependencyName of Object.keys(pkg.manifest[field] ?? {})) {
				const dependency = byName.get(dependencyName);
				if (dependency) {
					visit(dependency);
				}
			}
		}
		visiting.delete(pkg.name);
		visited.add(pkg.name);
		sorted.push(pkg);
	}

	for (const pkg of packages) {
		visit(pkg);
	}
	return sorted;
}
