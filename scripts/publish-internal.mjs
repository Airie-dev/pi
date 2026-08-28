#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";

function usage() {
	console.error(
		"Usage: node scripts/publish-internal.mjs --manifest <manifest.json> --registry <url> [--tag <dist-tag>] (--dry-run | --publish)",
	);
}

function parseArgs(args) {
	let manifest;
	let registry;
	let mode;
	let tag = "dev";
	for (let index = 0; index < args.length; index++) {
		const arg = args[index];
		if (arg === "--manifest") {
			manifest = args[++index];
		} else if (arg === "--registry") {
			registry = args[++index];
		} else if (arg === "--tag") {
			tag = args[++index];
		} else if (arg === "--dry-run" || arg === "--publish") {
			if (mode) {
				throw new Error("Choose exactly one of --dry-run or --publish");
			}
			mode = arg.slice(2);
		} else {
			throw new Error(`Unknown argument: ${arg}`);
		}
	}
	if (!manifest || !registry || !mode || !tag) {
		usage();
		process.exit(1);
	}
	const parsedRegistry = new URL(registry);
	if (parsedRegistry.protocol !== "https:" && parsedRegistry.protocol !== "http:") {
		throw new Error(`Registry must use HTTP or HTTPS: ${registry}`);
	}
	if (!/^[a-z][a-z0-9._-]*$/.test(tag)) {
		throw new Error(`Invalid npm dist-tag: ${tag}`);
	}
	return { manifest: resolve(manifest), mode, registry: parsedRegistry.href, tag };
}

function commandForPlatform(command) {
	return process.platform === "win32" ? `${command}.cmd` : command;
}

function runNpm(args, options = {}) {
	console.log(`$ npm ${args.join(" ")}`);
	const result = spawnSync(commandForPlatform("npm"), args, {
		encoding: "utf8",
		stdio: options.capture ? ["ignore", "pipe", "pipe"] : "inherit",
	});
	if (result.status !== 0 && !options.allowFailure) {
		const output = [result.stdout, result.stderr].filter(Boolean).join("\n");
		throw new Error(output ? `Command failed: npm ${args.join(" ")}\n${output}` : `Command failed: npm ${args.join(" ")}`);
	}
	return result;
}

function readManifest(path) {
	const manifest = JSON.parse(readFileSync(path, "utf8"));
	if (manifest.formatVersion !== 1 || !Array.isArray(manifest.packages) || typeof manifest.version !== "string") {
		throw new Error(`Unsupported internal package manifest: ${path}`);
	}
	return manifest;
}

function publishedIntegrity(name, version, registry) {
	const result = runNpm(["view", `${name}@${version}`, "dist.integrity", "--json", `--registry=${registry}`], {
		allowFailure: true,
		capture: true,
	});
	if (result.status === 0 && result.stdout.trim()) {
		return JSON.parse(result.stdout);
	}
	const output = [result.stdout, result.stderr].filter(Boolean).join("\n");
	if (output.includes("E404") || output.includes("404 Not Found")) {
		return null;
	}
	throw new Error(output ? `Failed to query ${name}@${version}\n${output}` : `Failed to query ${name}@${version}`);
}

const { manifest: manifestPath, mode, registry, tag } = parseArgs(process.argv.slice(2));
if (!existsSync(manifestPath)) {
	throw new Error(`Manifest does not exist: ${manifestPath}`);
}
const manifest = readManifest(manifestPath);
const packageDirectory = dirname(manifestPath);

runNpm(["whoami", `--registry=${registry}`]);
console.log(`\n${mode === "dry-run" ? "Validating" : "Publishing"} ${manifest.packages.length} packages at ${manifest.version}`);
console.log(`Registry: ${registry}`);
console.log(`Dist-tag: ${tag}\n`);

for (const pkg of manifest.packages) {
	const tarball = join(packageDirectory, pkg.filename);
	if (!existsSync(tarball)) {
		throw new Error(`Tarball does not exist: ${tarball}`);
	}

	if (mode === "publish") {
		const integrity = publishedIntegrity(pkg.name, pkg.version, registry);
		if (integrity !== null) {
			if (integrity !== pkg.integrity) {
				throw new Error(`${pkg.name}@${pkg.version} already exists with different contents`);
			}
			console.log(`Skipping ${pkg.name}@${pkg.version}: identical tarball already published\n`);
			continue;
		}
	}

	const args = ["publish", tarball, "--access", "public", "--ignore-scripts", `--registry=${registry}`, "--tag", tag];
	if (mode === "dry-run") {
		args.push("--dry-run");
	}
	runNpm(args);
	console.log();
}
