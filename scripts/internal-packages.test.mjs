import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
	createPackageNameMap,
	rewritePackageLock,
	rewritePackageManifest,
	rewritePackedPackageTree,
	sortPackagesForPublication,
	validateInternalVersion,
} from "./internal-packages.mjs";

const version = "0.84.3-airie.1";
const nameMap = createPackageNameMap([
	"@earendil-works/pi-ai",
	"@earendil-works/pi-agent-core",
	"@earendil-works/pi-telemetry",
]);

test("validates the internal version convention", () => {
	assert.doesNotThrow(() => validateInternalVersion(version));
	assert.throws(() => validateInternalVersion("0.84.3"), /must match/);
	assert.throws(() => validateInternalVersion("0.84.3-dev.1"), /must match/);
});

test("maps every public upstream package into the internal scope", () => {
	const packages = createPackageNameMap(["@earendil-works/pi-ai", "@earendil-works/chord"]);

	assert.equal(packages.get("@earendil-works/pi-ai"), "@airie-dev/pi-ai");
	assert.equal(packages.get("@earendil-works/chord"), "@airie-dev/chord");
});

test("rewrites package identity and pins internal dependencies", () => {
	const manifest = rewritePackageManifest(
		{
			name: "@earendil-works/pi-agent-core",
			version: "0.84.3",
			repository: {
				type: "git",
				url: "git+https://github.com/earendil-works/pi.git",
				directory: "packages/agent",
			},
			dependencies: {
				"@earendil-works/pi-ai": "^0.84.3",
				"@earendil-works/pi-telemetry": "^0.84.3",
				diff: "8.0.4",
			},
		},
		version,
		nameMap,
	);

	assert.equal(manifest.name, "@airie-dev/pi-agent-core");
	assert.equal(manifest.version, version);
	assert.equal(manifest.dependencies["@airie-dev/pi-ai"], version);
	assert.equal(manifest.dependencies["@airie-dev/pi-telemetry"], version);
	assert.equal(manifest.dependencies.diff, "8.0.4");
	assert.equal(manifest.repository.url, "git+https://github.com/Airie-dev/pi.git");
	assert.equal(manifest.repository.directory, "packages/agent");
});

test("rewrites shrinkwrap package records without retaining upstream tarball integrity", () => {
	const lock = rewritePackageLock(
		{
			name: "@earendil-works/pi-agent-core",
			version: "0.84.3",
			lockfileVersion: 3,
			packages: {
				"": {
					name: "@earendil-works/pi-agent-core",
					version: "0.84.3",
					dependencies: { "@earendil-works/pi-ai": "^0.84.3" },
				},
				"node_modules/@earendil-works/pi-ai": {
					version: "0.84.3",
					resolved: "https://registry.npmjs.org/@earendil-works/pi-ai/-/pi-ai-0.84.3.tgz",
					integrity: "sha512-upstream",
					dependencies: { "@earendil-works/pi-telemetry": "^0.84.3" },
				},
			},
		},
		version,
		nameMap,
	);

	assert.equal(lock.name, "@airie-dev/pi-agent-core");
	assert.equal(lock.version, version);
	assert.equal(lock.packages[""].dependencies["@airie-dev/pi-ai"], version);
	const ai = lock.packages["node_modules/@airie-dev/pi-ai"];
	assert.equal(ai.version, version);
	assert.equal(ai.dependencies["@airie-dev/pi-telemetry"], version);
	assert.equal("resolved" in ai, false);
	assert.equal("integrity" in ai, false);
});

test("rewrites package files while preserving binary files", () => {
	const root = mkdtempSync(join(tmpdir(), "pi-internal-package-test-"));
	try {
		mkdirSync(join(root, "dist"));
		writeFileSync(
			join(root, "package.json"),
			JSON.stringify({
				name: "@earendil-works/pi-agent-core",
				version: "0.84.3",
				dependencies: { "@earendil-works/pi-ai": "^0.84.3" },
			}),
		);
		writeFileSync(join(root, "dist", "index.js"), 'export * from "@earendil-works/pi-ai";\n');
		writeFileSync(join(root, "dist", "index.js.map"), '{"sourcesContent":["@earendil-works/pi-ai"]}\n');
		const binary = Buffer.from([0, 1, 2, 3]);
		writeFileSync(join(root, "dist", "native.node"), binary);

		rewritePackedPackageTree(root, version, nameMap);

		const manifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
		assert.equal(manifest.name, "@airie-dev/pi-agent-core");
		assert.equal(manifest.dependencies["@airie-dev/pi-ai"], version);
		assert.equal(readFileSync(join(root, "dist", "index.js"), "utf8"), 'export * from "@airie-dev/pi-ai";\n');
		assert.equal(readFileSync(join(root, "dist", "index.js.map"), "utf8"), '{"sourcesContent":["@airie-dev/pi-ai"]}\n');
		assert.deepEqual(readFileSync(join(root, "dist", "native.node")), binary);
	} finally {
		rmSync(root, { force: true, recursive: true });
	}
});

test("sorts public packages before their dependents", () => {
	const packages = [
		{
			name: "@earendil-works/pi-agent-core",
			manifest: { dependencies: { "@earendil-works/pi-ai": "^0.84.3" } },
		},
		{
			name: "@earendil-works/pi-ai",
			manifest: { dependencies: { "@earendil-works/pi-telemetry": "^0.84.3" } },
		},
		{ name: "@earendil-works/pi-telemetry", manifest: {} },
	];
	assert.deepEqual(
		sortPackagesForPublication(packages).map((pkg) => pkg.name),
		["@earendil-works/pi-telemetry", "@earendil-works/pi-ai", "@earendil-works/pi-agent-core"],
	);
});
