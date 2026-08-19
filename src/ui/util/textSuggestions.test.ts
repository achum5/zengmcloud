import { assert, describe, test } from "vitest";
import {
	afterKeyText,
	applySuggestion,
	currentWordOf,
	shouldAutoShift,
	suggestionsFor,
	withinOneTypo,
	WORDS,
} from "./textSuggestions.ts";

describe("withinOneTypo", () => {
	test("one substitution", () => {
		assert.ok(withinOneTypo("gane", "game"));
	});

	test("one missing letter", () => {
		assert.ok(withinOneTypo("gme", "game"));
	});

	test("one extra letter", () => {
		assert.ok(withinOneTypo("gamme", "game"));
	});

	test("adjacent swap counts as one, not two", () => {
		assert.ok(withinOneTypo("hte", "the"));
		assert.ok(withinOneTypo("gaem", "game"));
	});

	test("two typos are not forgiven", () => {
		assert.ok(!withinOneTypo("gnae", "game"));
		assert.ok(!withinOneTypo("ga", "game"));
	});
});

describe("suggestionsFor", () => {
	test("completes a word being typed", () => {
		assert.deepStrictEqual(suggestionsFor("basketba"), ["basketball"]);
	});

	test("corrects a typo, correction first", () => {
		assert.strictEqual(suggestionsFor("hte")[0], "the");
		assert.strictEqual(suggestionsFor("gaem")[0], "game");
	});

	test("a real word only completes, never 'corrects'", () => {
		// "the" is a word; nothing should try to fix it.
		for (const s of suggestionsFor("the")) {
			assert.ok(s.startsWith("the"), s);
		}
	});

	test("keeps the typed capitalization", () => {
		assert.strictEqual(suggestionsFor("Hte")[0], "The");
		assert.strictEqual(suggestionsFor("Basketba")[0], "Basketball");
	});

	test("too short to guess about", () => {
		assert.deepStrictEqual(suggestionsFor("t"), []);
		assert.deepStrictEqual(suggestionsFor(""), []);
	});

	test("never more than the limit", () => {
		assert.ok(suggestionsFor("th").length <= 3);
	});
});

describe("applySuggestion", () => {
	test("replaces the word being typed and opens the next one", () => {
		assert.strictEqual(applySuggestion("great gaem", "game"), "great game ");
	});

	test("works mid-thought with contractions", () => {
		assert.strictEqual(applySuggestion("i don", "don't"), "i don't ");
	});
});

describe("currentWordOf", () => {
	test("the trailing letters are the word", () => {
		assert.strictEqual(currentWordOf("great gam"), "gam");
	});

	test("a boundary means no word", () => {
		assert.strictEqual(currentWordOf("great game "), "");
		assert.strictEqual(currentWordOf(""), "");
	});
});

describe("afterKeyText", () => {
	test("a lone i capitalizes when sealed off", () => {
		assert.strictEqual(afterKeyText("i", " "), "I ");
		assert.strictEqual(afterKeyText("am i", "?"), "am I?");
	});

	test("an i inside a word is left alone", () => {
		assert.strictEqual(afterKeyText("hi", " "), "hi ");
	});

	test("ordinary keys just append", () => {
		assert.strictEqual(afterKeyText("ga", "m"), "gam");
	});
});

describe("shouldAutoShift", () => {
	test("the start of a message", () => {
		assert.ok(shouldAutoShift(""));
	});

	test("after a sentence ends", () => {
		assert.ok(shouldAutoShift("What a shot. "));
		assert.ok(shouldAutoShift("Seriously? "));
	});

	test("not mid-sentence", () => {
		assert.ok(!shouldAutoShift("What a "));
		assert.ok(!shouldAutoShift("What"));
	});
});

describe("WORDS", () => {
	test("no duplicates that would waste rank order", () => {
		// A repeat is harmless at runtime (first rank wins) but means the list
		// has drifted; keep it tidy.
		const seen = new Set<string>();
		const dupes: string[] = [];
		for (const w of WORDS) {
			if (seen.has(w)) {
				dupes.push(w);
			}
			seen.add(w);
		}
		assert.deepStrictEqual(dupes, []);
	});

	test("everything is lowercase letters and apostrophes", () => {
		for (const w of WORDS) {
			assert.ok(/^['a-z-]+$/.test(w), w);
		}
	});
});
