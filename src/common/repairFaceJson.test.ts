import { assert, describe, test } from "vitest";
import { parseFaceJson, repairFaceJson } from "./repairFaceJson.ts";

const CONFIG = {
	fatness: 0.38,
	teamColors: ["#89bfd3", "#7a1319", "#07364f"],
	head: { id: "head5", shave: "rgba(0,0,0,0.3)" },
	hair: { id: "crop-fade", color: "#272421", flip: false },
	nose: { id: "nose11", flip: false, size: 1 },
};

describe("a face config pasted out of a chat AI", () => {
	test("clean JSON is returned untouched", () => {
		const text = JSON.stringify(CONFIG, undefined, 2);
		assert.strictEqual(repairFaceJson(text), text);
		assert.deepEqual(parseFaceJson(text), CONFIG);
	});

	// The reported failure: "Bad control character in string literal in JSON at
	// position 34". A value wrapped across two lines by the chat UI.
	test("a line break inside a string is closed up", () => {
		const text = '{\n  "head": { "shave": "rgba(0,0,0,\n0.3)" }\n}';
		assert.deepEqual(parseFaceJson(text), {
			head: { shave: "rgba(0,0,0, 0.3)" },
		});
	});

	test("a tab inside a string is closed up", () => {
		assert.deepEqual(parseFaceJson('{ "hair": { "id": "crop\t-fade" } }'), {
			hair: { id: "crop -fade" },
		});
	});

	test("curly quotes are straightened", () => {
		assert.deepEqual(parseFaceJson("{ “nose”: { “id”: “nose11” } }"), {
			nose: { id: "nose11" },
		});
	});

	test("a markdown fence is stripped", () => {
		const text = "```json\n" + JSON.stringify(CONFIG) + "\n```";
		assert.deepEqual(parseFaceJson(text), CONFIG);
	});

	test("prose either side of the object is dropped", () => {
		const text = `Here you go!\n\n${JSON.stringify(CONFIG)}\n\nNotes:\n- Guessed the hair color.`;
		assert.deepEqual(parseFaceJson(text), CONFIG);
	});

	test("a trailing comma is dropped", () => {
		assert.deepEqual(parseFaceJson('{ "nose": { "id": "nose11", }, }'), {
			nose: { id: "nose11" },
		});
	});

	// rgba() is full of commas and none of them are trailing.
	test("commas inside a value survive", () => {
		assert.deepEqual(
			parseFaceJson('{ "head": { "shave": "rgba(0,0,0,0.35)" } }'),
			{
				head: { shave: "rgba(0,0,0,0.35)" },
			},
		);
	});

	test("an escaped quote inside a string is not read as the end of it", () => {
		assert.deepEqual(parseFaceJson(String.raw`{ "a": "say \"hi\"", "b": 1 }`), {
			a: 'say "hi"',
			b: 1,
		});
	});

	test("everything wrong at once", () => {
		const text =
			'Sure!\n```json\n{\n  “fatness”: 0.38,\n  "head": { "shave": "rgba(0,0,0,\n0.3)" },\n  "nose": { "id": "nose11", },\n}\n```\nNotes: none.';
		assert.deepEqual(parseFaceJson(text), {
			fatness: 0.38,
			head: { shave: "rgba(0,0,0, 0.3)" },
			nose: { id: "nose11" },
		});
	});

	test("something that isn't a config at all gives up rather than guessing", () => {
		assert.strictEqual(parseFaceJson("I couldn't see the photo."), undefined);
	});
});
