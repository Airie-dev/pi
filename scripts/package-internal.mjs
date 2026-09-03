#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createPackageNameMap, INTERNAL_SCOPE, rewritePackedPackageTree, sortPackagesForPublication, validateInternalVersion } from "./internal-packages.mjs";
import { getPublicWorkspacePackages } from "./release-packages.mjs";

const repoRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));

function usage() {
	console.error("Usage: node scripts/package-internal.mjs --version <x.y.z-airie.n> --out <directory>");
}

function parseArgs(args) {
	let version;
	let out;
	for (let index = 0; index < args.length; index++) {
		const arg = args[index];
		if (arg === "--version") {
			version = args[++index];
		} else if (arg === "--out") {
			out = args[++index];
		} else {
			throw new Error(`Unknown argument: ${arg}`);
		}
	}
	if (!version || !out) {
		usage();
		process.exit(1);
	}
	validateInternalVersion(version);
	return { out: resolve(out), version };
}

function commandForPlatform(command) {
	return process.platform === "win32" && command === "npm" ? "npm.cmd" : command;
}

function run(command, args, options = {}) {
	console.log(`$ ${[command, ...args].join(" ")}`);
	const result = spawnSync(commandForPlatform(command), args, {
		cwd: options.cwd ?? repoRoot,
		encoding: "utf8",
		stdio: options.capture ? ["ignore", "pipe", "pipe"] : "inherit",
	});
	if (result.status !== 0) {
		const output = [result.stdout, result.stderr].filter(Boolean).join("\n");
		throw new Error(output ? `Command failed: ${command} ${args.join(" ")}\n${output}` : `Command failed: ${command} ${args.join(" ")}`);
	}
	return result;
}

function prepareOutputDirectory(out) {
	if (existsSync(out) && readdirSync(out).length > 0) {
		throw new Error(`Output directory is not empty: ${out}`);
	}
	mkdirSync(out, { recursive: true });
}

function readPublicPackages() {
	return getPublicWorkspacePackages().map((pkg) => ({
		...pkg,
		manifest: JSON.parse(readFileSync(join(repoRoot, pkg.directory, "package.json"), "utf8")),
	}));
}

function parsePackResult(result) {
	const output = JSON.parse(result.stdout);
	if (!Array.isArray(output) || output.length !== 1) {
		throw new Error(`Unexpected npm pack output: ${result.stdout}`);
	}
	return output[0];
}

function gitOutput(...args) {
	return run("git", args, { capture: true }).stdout.trim();
}

function packageWorkspace(pkg, internalVersion, nameMap, out, tempRoot) {
	const packageDirectory = join(repoRoot, pkg.directory);
	if (!existsSync(join(packageDirectory, "dist"))) {
		throw new Error(`${pkg.directory}/dist does not exist. Build workspace packages before creating internal tarballs.`);
	}

	const incoming = join(tempRoot, "incoming", basename(pkg.directory));
	const staging = join(tempRoot, "staging", basename(pkg.directory));
	mkdirSync(incoming, { recursive: true });
	mkdirSync(staging, { recursive: true });

	const upstreamPack = parsePackResult(
		run("npm", ["pack", "--ignore-scripts", "--json", "--pack-destination", incoming], {
			capture: true,
			cwd: packageDirectory,
		}),
	);
	const upstreamTarball = join(incoming, upstreamPack.filename);
	run("tar", ["-xzf", upstreamTarball, "-C", staging]);
	const stagedPackage = join(staging, "package");
	rewritePackedPackageTree(stagedPackage, internalVersion, nameMap);

	const internalPack = parsePackResult(
		run("npm", ["pack", "--ignore-scripts", "--json", "--pack-destination", out], {
			capture: true,
			cwd: stagedPackage,
		}),
	);
	const expectedName = nameMap.get(pkg.name);
	if (internalPack.name !== expectedName || internalPack.version !== internalVersion) {
		throw new Error(
			`Unexpected internal package identity for ${pkg.name}: ${internalPack.name}@${internalPack.version}; expected ${expectedName}@${internalVersion}`,
		);
	}

	console.log(`  ${internalPack.filename}: ${internalPack.files.length} files, ${internalPack.size} bytes packed`);
	return {
		dependencies: Object.keys(pkg.manifest.dependencies ?? {}).filter((name) => nameMap.has(name)).map((name) => nameMap.get(name)),
		directory: pkg.directory,
		filename: internalPack.filename,
		integrity: internalPack.integrity,
		name: internalPack.name,
		shasum: internalPack.shasum,
		upstreamName: pkg.name,
		version: internalPack.version,
	};
}

const { out, version } = parseArgs(process.argv.slice(2));
const publicPackages = sortPackagesForPublication(readPublicPackages());
const versions = new Set(publicPackages.map((pkg) => pkg.version));
if (versions.size !== 1) {
	throw new Error(`Upstream public packages are not lockstep versioned: ${[...versions].join(", ")}`);
}
const nameMap = createPackageNameMap(publicPackages.map((pkg) => pkg.name));
prepareOutputDirectory(out);
const tempRoot = mkdtempSync(join(tmpdir(), "pi-internal-packages-"));

try {
	console.log(`Packaging ${publicPackages.length} public workspaces under ${INTERNAL_SCOPE} at ${version}\n`);
	const packaged = publicPackages.map((pkg) => packageWorkspace(pkg, version, nameMap, out, tempRoot));
	const manifest = {
		formatVersion: 1,
		generatedAt: new Date().toISOString(),
		internalScope: INTERNAL_SCOPE,
		packages: packaged,
		source: {
			branch: gitOutput("branch", "--show-current"),
			commit: gitOutput("rev-parse", "HEAD"),
			repository: "https://github.com/Airie-dev/pi",
			upstreamVersion: [...versions][0],
		},
		version,
	};
	const manifestPath = join(out, "manifest.json");
	writeFileSync(manifestPath, `${JSON.stringify(manifest, null, "\t")}\n`);
	console.log(`\nWrote ${packaged.length} tarballs and ${manifestPath}`);
} finally {
	rmSync(tempRoot, { force: true, recursive: true });
}
