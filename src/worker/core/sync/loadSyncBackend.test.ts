import { assert, describe, test } from "vitest";

// THE GUARD ON THE BUNDLE SPLIT.
//
// The Firebase SDK is 428 KB of what used to be a 2,517 KB worker bundle, and it
// sits in its own lazily-fetched chunk only for as long as nothing in the eager
// graph names it. One ordinary `import` added anywhere in that graph silently
// pulls all of it back into the main bundle: the app still works, every other
// test still passes, and every single-player user starts paying for a feature
// they never opened. Nothing else in the suite would notice, so this does.
//
// The rule: a module may import `firebase/*` only if it is reachable ONLY
// through the dynamic import in loadSyncBackend.ts.

// Read through Vite's own glob rather than node:fs, so this file type-checks
// under the web tsconfig like the rest of src/.
type GlobbedSource = Record<string, string>;
const globRaw = (
	import.meta as unknown as {
		glob: (
			pattern: string[],
			options: { query: string; import: string; eager: true },
		) => GlobbedSource;
	}
).glob(["../../**/*.ts", "../../../ui/**/*.{ts,tsx}"], {
	query: "?raw",
	import: "default",
	eager: true,
});

// Modules allowed to touch the SDK. Every one of them is behind firebaseLazy.ts.
const BEHIND_THE_BOUNDARY = new Set([
	"firebaseApp.ts",
	"auth.ts",
	"FirebaseTransport.ts",
	"adminRooms.ts",
	"firebaseLazy.ts",
]);

// Glob keys are relative to THIS file, so the sync directory's own modules come
// back as "./auth.ts" while everything else carries a path. Both halves matter:
// a file called auth.ts somewhere else in the worker is not one of these.
const fileName = (file: string) => file.split("/").at(-1)!;
const inSyncDir = (file: string) =>
	file.startsWith("./") || file.includes("/sync/");

const isBehindTheBoundary = (file: string) =>
	inSyncDir(file) && BEHIND_THE_BOUNDARY.has(fileName(file));

const sources = Object.entries(globRaw).filter(
	([file]) => !file.includes(".test."),
);

// A static `import ... from "x"` or `export ... from "x"`. Deliberately NOT
// `import("x")` - deferring is the whole point - and not `import type`, which is
// erased before the bundler ever sees it.
const staticImportsOf = (code: string): string[] => {
	const re =
		/(?:^|\n)\s*(?:import|export)\s+(?!type\b)(?:[^;]*?\bfrom\s*)?"([^"]+)"/g;
	const out: string[] = [];
	let m: RegExpExecArray | null;
	while ((m = re.exec(code)) !== null) {
		out.push(m[1]!);
	}
	return out;
};

describe("the Firebase SDK stays out of the eager bundles", () => {
	test("the glob actually found the source files", () => {
		// A guard that silently matches nothing would pass forever.
		assert.ok(
			sources.length > 100,
			`expected to scan the worker and ui trees, got ${sources.length} files`,
		);
	});

	test("only modules behind the boundary import firebase/*", () => {
		const offenders = sources
			.filter(
				([file, code]) =>
					staticImportsOf(code).some((spec) => spec.startsWith("firebase/")) &&
					!isBehindTheBoundary(file),
			)
			.map(([file]) => file);
		assert.deepStrictEqual(
			offenders,
			[],
			`these modules import the Firebase SDK eagerly instead of through a dynamic import: ${offenders.join(", ")}`,
		);
	});

	test("nothing outside the boundary statically imports a module inside it", () => {
		const offenders: string[] = [];
		for (const [file, code] of sources) {
			if (isBehindTheBoundary(file)) {
				continue;
			}
			for (const spec of staticImportsOf(code)) {
				if (BEHIND_THE_BOUNDARY.has(fileName(spec))) {
					offenders.push(`${file} -> ${spec}`);
				}
			}
		}
		assert.deepStrictEqual(offenders, [], offenders.join("; "));
	});

	test("loadSyncBackend reaches the Firebase side only by deferring", () => {
		const entry = sources.find(([file]) => file.endsWith("loadSyncBackend.ts"));
		assert.ok(entry, "loadSyncBackend.ts was not found by the glob");
		const code = entry![1];
		assert.ok(
			code.includes('import("./firebaseLazy.ts")'),
			"loadSyncBackend must reach the Firebase side through a dynamic import",
		);
		assert.deepStrictEqual(
			staticImportsOf(code).filter((spec) => spec.includes("firebase")),
			[],
			"loadSyncBackend must not statically import anything firebase-flavored",
		);
	});
});
