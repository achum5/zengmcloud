import { assert, describe, test } from "vitest";
import { fetchDeltaOverRest, fromRestFields } from "./restDelta.ts";

const docId = (version: number, index: number) => `v2delta_${version}_${index}`;

const chunkDoc = (fields: Record<string, unknown>) => ({
	name: "ignored",
	fields: Object.fromEntries(
		Object.entries(fields).map(([key, value]) => [
			key,
			typeof value === "string"
				? { stringValue: value }
				: Number.isInteger(value)
					? { integerValue: String(value) }
					: { doubleValue: value },
		]),
	),
});

// A fake fetch serving the given docs by id, recording what was asked.
const serve = (docs: Record<string, unknown>) => {
	const asked: { url: string; auth: string | undefined }[] = [];
	const fetchFn = (async (url: string, init?: { headers?: unknown }) => {
		const id = url.split("/").at(-1)!;
		asked.push({
			url,
			auth: (init?.headers as Record<string, string> | undefined)
				?.Authorization,
		});
		const doc = docs[id];
		return {
			ok: doc !== undefined,
			json: async () => doc,
		} as Response;
	}) as unknown as typeof fetch;
	return { fetchFn, asked };
};

const meta = { authorId: "A", action: "playMenu.sim", at: 1234 };

describe("delta chunks over REST", () => {
	test("plain values out of Firestore's typed fields", () => {
		assert.deepStrictEqual(
			fromRestFields({
				fields: {
					a: { stringValue: "x" },
					b: { integerValue: "42" },
					c: { doubleValue: 1.5 },
					d: { booleanValue: true },
					e: { nullValue: null },
					f: { timestampValue: "2026-01-01T00:00:00Z" },
				},
			}),
			{ a: "x", b: 42, c: 1.5, d: true, e: null },
		);
		assert.deepStrictEqual(fromRestFields(undefined), {});
	});

	test("reassembles a multi-chunk payload, signed in as the user", async () => {
		const { fetchFn, asked } = serve({
			v2delta_7_0: chunkDoc({ ...meta, version: 7, chunkCount: 3, data: "ab" }),
			v2delta_7_1: chunkDoc({ ...meta, version: 7, chunkCount: 3, data: "cd" }),
			v2delta_7_2: chunkDoc({ ...meta, version: 7, chunkCount: 3, data: "e" }),
		});
		const delta = await fetchDeltaOverRest({
			projectId: "proj",
			code: "room",
			token: "tok",
			version: 7,
			docId,
			fetchFn,
		});
		assert.deepStrictEqual(delta, { serialized: "abcde", ...meta });
		assert.strictEqual(asked.length, 3);
		assert.ok(
			asked[0]!.url.startsWith(
				"https://firestore.googleapis.com/v1/projects/proj/databases/(default)/documents/leagues/room/control/",
			),
		);
		assert.ok(asked.every((a) => a.auth === "Bearer tok"));
	});

	test("anything short of a whole, consistent payload is no rescue", async () => {
		const run = (docs: Record<string, unknown>) =>
			fetchDeltaOverRest({
				projectId: "p",
				code: "c",
				token: "t",
				version: 7,
				docId,
				fetchFn: serve(docs).fetchFn,
			});
		// Not there at all.
		assert.strictEqual(await run({}), undefined);
		// A chunk missing.
		assert.strictEqual(
			await run({
				v2delta_7_0: chunkDoc({
					...meta,
					version: 7,
					chunkCount: 2,
					data: "a",
				}),
			}),
			undefined,
		);
		// Two writers' chunks spliced together.
		assert.strictEqual(
			await run({
				v2delta_7_0: chunkDoc({
					...meta,
					version: 7,
					chunkCount: 2,
					data: "a",
				}),
				v2delta_7_1: chunkDoc({
					...meta,
					authorId: "B",
					version: 7,
					chunkCount: 2,
					data: "b",
				}),
			}),
			undefined,
		);
		// Some other version's payload.
		assert.strictEqual(
			await run({
				v2delta_7_0: chunkDoc({
					...meta,
					version: 8,
					chunkCount: 1,
					data: "a",
				}),
			}),
			undefined,
		);
	});
});
