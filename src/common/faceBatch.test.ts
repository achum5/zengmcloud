import { assert, describe, test } from "vitest";
import { generate } from "facesjs";
import { buildBatchPrompt, parseFaceBatch } from "./faceBatch.ts";

const face = (overrides: Record<string, unknown> = {}) => ({
	...generate(),
	...overrides,
});

describe("parseFaceBatch", () => {
	test("one object keyed by player id, one face per line", () => {
		const a = face({ fatness: 0.1 });
		const b = face({ fatness: 0.9 });
		const text = `\`\`\`json
{
"12": ${JSON.stringify(a)},
"345": ${JSON.stringify(b)}
}
\`\`\`

Notes:
- #2 Someone: stubble was hard to read`;
		const { faces, unknown } = parseFaceBatch(text, [12, 345]);
		assert.deepEqual([...faces.keys()], [12, 345]);
		assert.strictEqual(faces.get(12)!.face.fatness, 0.1);
		assert.strictEqual(faces.get(345)!.face.fatness, 0.9);
		assert.deepEqual(unknown, []);
	});

	test("pretty-printed faces, decorated keys, curly quotes", () => {
		const a = JSON.stringify(face(), undefined, 2).replaceAll('"', "“");
		const text = `{ “#1 · id 77 · First Last”: ${a} }`;
		const { faces } = parseFaceBatch(text, [77]);
		assert.deepEqual([...faces.keys()], [77]);
	});

	test("several code blocks and a reply cut off mid-face", () => {
		const text = `\`\`\`json
{"1": ${JSON.stringify(face())}}
\`\`\`
\`\`\`json
{"2": ${JSON.stringify(face())},
"3": {"fatness": 0.2, "head": {"id": "head1"`;
		const { faces } = parseFaceBatch(text, [1, 2, 3]);
		assert.deepEqual([...faces.keys()], [1, 2]);
	});

	test("ids not in the batch are reported, never applied", () => {
		const text = `{"9": ${JSON.stringify(face())}, "10": ${JSON.stringify(face())}}`;
		const { faces, unknown } = parseFaceBatch(text, [9]);
		assert.deepEqual([...faces.keys()], [9]);
		assert.deepEqual(unknown, [10]);
	});

	test("missing slots are filled and flagged, unknown ids flagged", () => {
		const raw = face({ nose: { id: "nose99", size: 1, flip: false } });
		delete (raw as any).hairBg;
		delete (raw as any).eyeLine;
		const { faces } = parseFaceBatch(`{"5": ${JSON.stringify(raw)}}`, [5]);
		const checked = faces.get(5)!;
		assert.deepEqual((checked.face as any).hairBg, { id: "none" });
		assert.include(checked.warnings, "hairBg missing");
		assert.include(checked.warnings, "eyeLine missing");
		assert.include(checked.warnings, 'nose: no such id "nose99"');
	});

	test("a face's own inner objects are never taken for players", () => {
		const { faces } = parseFaceBatch(
			`{"42": ${JSON.stringify(face())}}`,
			undefined,
		);
		assert.deepEqual([...faces.keys()], [42]);
	});
});

describe("buildBatchPrompt", () => {
	test("roster lines, the single-photo method, and the reply format at both ends", () => {
		const prompt = buildBatchPrompt(
			[
				{ n: 1, pid: 101, name: "A One" },
				{ n: 2, pid: 202, name: "B Two" },
			],
			"sheet",
		);
		assert.include(prompt, "#1 · id 101 · A One");
		assert.include(prompt, "#2 · id 202 · B Two");
		assert.include(prompt, "## What every option looks like");
		assert.include(prompt, "CONTACT SHEET");
		const formatAt = prompt.indexOf("## Batch reply format");
		const againAt = prompt.lastIndexOf("## Batch reply format, again");
		assert.isAbove(formatAt, -1);
		assert.isAbove(againAt, formatAt);
		assert.include(prompt, '"101": {');
	});
});
