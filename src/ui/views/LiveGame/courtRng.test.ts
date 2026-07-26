import { assert, describe, test } from "vitest";
import {
	clearCourtRng,
	courtRandom,
	makeCourtRng,
	seedCourtRng,
} from "./courtRng.ts";

const draw = (seed: string, n: number) => {
	const rng = makeCourtRng(seed);
	return Array.from({ length: n }, () => rng());
};

describe("makeCourtRng", () => {
	test("the same seed gives the same sequence", () => {
		// This is the whole point: two devices watching one broadcast derive the
		// same seed and must invent the same court positions from it.
		assert.deepStrictEqual(draw("g7|41", 20), draw("g7|41", 20));
	});

	test("different plays get different geometry", () => {
		assert.notStrictEqual(
			JSON.stringify(draw("g7|41", 8)),
			JSON.stringify(draw("g7|42", 8)),
		);
	});

	test("different games get different geometry", () => {
		assert.notStrictEqual(
			JSON.stringify(draw("g7|41", 8)),
			JSON.stringify(draw("g8|41", 8)),
		);
	});

	test("values stay in [0, 1)", () => {
		for (const v of draw("seed", 500)) {
			assert.ok(v >= 0 && v < 1, `out of range: ${v}`);
		}
	});

	test("it actually varies - not a constant dressed up as a generator", () => {
		const values = draw("seed", 200);
		assert.ok(new Set(values).size > 190);
		const mean = values.reduce((a, b) => a + b, 0) / values.length;
		// Loose bound; this is a smoke test for a stuck generator, not a
		// statistical proof.
		assert.ok(mean > 0.35 && mean < 0.65, `suspicious mean ${mean}`);
	});

	test("a one-character seed change moves the whole stream", () => {
		const a = draw("g7|1", 5);
		const b = draw("g7|2", 5);
		// Guards against a weak hash where adjacent play indexes land on nearly
		// the same stream, which would put consecutive plays in the same spot.
		assert.ok(Math.abs(a[0]! - b[0]!) > 0.01, "adjacent seeds too close");
	});
});

describe("the shared stream", () => {
	test("re-seeding restarts it, so a replayed play looks identical", () => {
		seedCourtRng("g1|5");
		const first = [courtRandom(), courtRandom(), courtRandom()];
		seedCourtRng("g1|5");
		const second = [courtRandom(), courtRandom(), courtRandom()];
		assert.deepStrictEqual(first, second);
		clearCourtRng();
	});

	test("unseeded it still produces usable randomness", () => {
		// The court editor's preview wants variety, not reproducibility, and must
		// not break just because nothing seeded the stream.
		clearCourtRng();
		const values = Array.from({ length: 50 }, () => courtRandom());
		assert.ok(values.every((v) => v >= 0 && v < 1));
		assert.ok(new Set(values).size > 40);
	});
});
