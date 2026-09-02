import { assert, describe, test } from "vitest";
import {
	createPhrasePool,
	hashSeed,
	poolKey,
	rngFromSeed,
	seededShuffle,
} from "./phrasePool.ts";

// Independent draws, exactly as a real caller makes them: each item seeds its
// own stream from its own id, which is what makes collisions likely.
const streams = (n: number) =>
	Array.from({ length: n }, (_, i) => rngFromSeed((i + 1) * 2654435761));

describe("rngFromSeed", () => {
	test("the same seed always gives the same stream", () => {
		const a = rngFromSeed(12345);
		const b = rngFromSeed(12345);
		assert.deepStrictEqual([a(), a(), a(), a()], [b(), b(), b(), b()]);
	});

	test("different seeds diverge", () => {
		const a = rngFromSeed(1);
		const b = rngFromSeed(2);
		assert.notStrictEqual(a(), b());
	});

	test("values stay in [0, 1)", () => {
		const rng = rngFromSeed(99);
		for (let i = 0; i < 500; i++) {
			const v = rng();
			assert.strictEqual(v >= 0 && v < 1, true);
		}
	});
});

describe("hashSeed", () => {
	test("is stable and unsigned", () => {
		assert.strictEqual(hashSeed("p:1234"), hashSeed("p:1234"));
		assert.strictEqual(hashSeed("p:1234") >= 0, true);
	});

	test("adjacent ids do not produce adjacent streams", () => {
		// Ids in this app are overwhelmingly sequential ("p:1", "p:2"), so a
		// hash that barely moves would make neighbouring accounts read alike.
		const a = rngFromSeed(hashSeed("p:1"))();
		const b = rngFromSeed(hashSeed("p:2"))();
		assert.strictEqual(Math.abs(a - b) > 0.05, true);
	});
});

describe("poolKey", () => {
	test("the same template keys the same however it rendered", () => {
		// The whole reason the key is not the joined text: this pool is one
		// authored template, and it renders differently in every game.
		assert.strictEqual(
			poolKey(["Boston won by 12", "Boston held on"]),
			poolKey(["Sacramento won by 4", "Sacramento held on"]),
		);
	});

	test("a two-word nickname keys the same as a one-word one", () => {
		assert.strictEqual(
			poolKey(["the Trail Blazers rolled"]),
			poolKey(["the Heat rolled"]),
		);
	});

	test("genuinely different templates key differently", () => {
		assert.notStrictEqual(poolKey(["Boston won"]), poolKey(["Boston lost"]));
	});

	test("decimals collapse like whole numbers", () => {
		assert.strictEqual(
			poolKey(["scored 28.5 a game"]),
			poolKey(["scored 9 a game"]),
		);
	});
});

describe("createPhrasePool", () => {
	const OPTIONS = ["routed", "edged", "held off", "beat", "got past", "downed"];

	test("a batch works through the pool before repeating anything", () => {
		const pool = createPhrasePool();
		pool.beginBatch();
		const picked = streams(6).map((rng) => pool.pick(rng, OPTIONS));
		assert.strictEqual(new Set(picked).size, 6);
	});

	test("this is the failure it exists to stop", () => {
		// Without memory, independent seeds collide badly. Prove the naive
		// version really does repeat, so the test above is measuring something.
		const naive = streams(6).map(
			(rng) => OPTIONS[Math.floor(rng() * OPTIONS.length)]!,
		);
		assert.strictEqual(new Set(naive).size < 6, true);
	});

	test("an exhausted pool starts over rather than going silent", () => {
		const pool = createPhrasePool();
		pool.beginBatch();
		const picked = streams(14).map((rng) => pool.pick(rng, OPTIONS));
		assert.strictEqual(picked.length, 14);
		assert.strictEqual(
			picked.every((p) => OPTIONS.includes(p)),
			true,
		);
	});

	test("interpolated pools rotate too, because memory is by index", () => {
		// Remembering rendered strings would never engage here: every option
		// list is unique, so every call would look like a brand new pool.
		const pool = createPhrasePool();
		pool.beginBatch();
		const picked = streams(4).map((rng, i) =>
			pool.pick(rng, [
				`won by ${i + 3}`,
				`held on ${i + 3}`,
				`pulled away ${i + 3}`,
				`survived ${i + 3}`,
			]),
		);
		const shapes = picked.map((p) => p.replace(/\d+/, ""));
		assert.strictEqual(new Set(shapes).size, 4);
	});

	test("unrelated pools never interfere", () => {
		const pool = createPhrasePool();
		pool.beginBatch();
		const rng = rngFromSeed(7);
		const a = pool.pick(rng, ["alpha", "beta"]);
		const b = pool.pick(rng, ["gamma", "delta"]);
		assert.strictEqual(["alpha", "beta"].includes(a), true);
		assert.strictEqual(["gamma", "delta"].includes(b), true);
	});

	test("outside a batch, one item is reproducible on its own", () => {
		// The box score page opens one recap at a time and it must read the
		// same every time, so memory must not leak between calls.
		const pool = createPhrasePool();
		const first = pool.pick(rngFromSeed(42), OPTIONS);
		const second = pool.pick(rngFromSeed(42), OPTIONS);
		assert.strictEqual(first, second);
	});

	test("inside a batch, the same seed is deliberately steered away", () => {
		const pool = createPhrasePool();
		pool.beginBatch();
		const first = pool.pick(rngFromSeed(42), OPTIONS);
		const second = pool.pick(rngFromSeed(42), OPTIONS);
		assert.notStrictEqual(first, second);
	});

	test("ending a batch restores per-item reproducibility", () => {
		const pool = createPhrasePool();
		pool.beginBatch();
		pool.pick(rngFromSeed(42), OPTIONS);
		pool.endBatch();
		assert.strictEqual(
			pool.pick(rngFromSeed(42), OPTIONS),
			pool.pick(rngFromSeed(42), OPTIONS),
		);
	});

	test("a poolId shares one rotation across differently-shaped call sites", () => {
		const pool = createPhrasePool();
		pool.beginBatch();
		const a = pool.pick(rngFromSeed(1), ["x", "y", "z"], "shared");
		const b = pool.pick(rngFromSeed(1), ["x", "y", "z"], "shared");
		assert.notStrictEqual(a, b);
	});

	test("a one-option pool is returned as-is without consuming rng", () => {
		const pool = createPhrasePool();
		assert.strictEqual(pool.pick(rngFromSeed(1), ["only"]), "only");
	});

	test("takeUnclaimed skips words another pool already used", () => {
		// Per-pool rotation cannot see that "edged" appeared in a different
		// sentence pool. The ledger can. Checked across several seeds, each
		// against a fresh ledger, because one batch claiming every option is a
		// different case - see the fallback test below.
		for (let i = 0; i < 6; i++) {
			const pool = createPhrasePool();
			pool.beginBatch();
			pool.claim("edged");
			assert.notStrictEqual(
				pool.takeUnclaimed(rngFromSeed(i + 1), [
					"edged",
					"downed",
					"beat",
					"routed",
				]),
				"edged",
			);
		}
	});

	test("takeUnclaimed claims what it returns", () => {
		const pool = createPhrasePool();
		pool.beginBatch();
		const first = pool.takeUnclaimed(rngFromSeed(3), ["a", "b", "c"]);
		assert.strictEqual(pool.isClaimed(first), true);
	});

	test("takeUnclaimed still answers when every word is taken", () => {
		const pool = createPhrasePool();
		pool.beginBatch();
		pool.claim("a");
		pool.claim("b");
		assert.strictEqual(
			["a", "b"].includes(pool.takeUnclaimed(rngFromSeed(5), ["a", "b"])),
			true,
		);
	});

	test("two pools do not share memory", () => {
		// The reason this is a factory. If the feed and the recap engine shared
		// a rotation, generating a recap would silently change a post.
		const a = createPhrasePool();
		const b = createPhrasePool();
		a.beginBatch();
		b.beginBatch();
		assert.strictEqual(
			a.pick(rngFromSeed(42), OPTIONS),
			b.pick(rngFromSeed(42), OPTIONS),
		);
	});

	test("reset clears the rotation and the ledger", () => {
		const pool = createPhrasePool();
		pool.beginBatch();
		pool.claim("edged");
		pool.reset();
		assert.strictEqual(pool.isClaimed("edged"), false);
	});
});

describe("seededShuffle", () => {
	test("is a permutation, and the same one for the same seed", () => {
		const input = [1, 2, 3, 4, 5, 6, 7, 8];
		const a = seededShuffle(rngFromSeed(9), input);
		const b = seededShuffle(rngFromSeed(9), input);
		assert.deepStrictEqual(a, b);
		assert.deepStrictEqual(
			[...a].sort((x, y) => x - y),
			input,
		);
	});

	test("does not mutate its input", () => {
		const input = [1, 2, 3];
		seededShuffle(rngFromSeed(1), input);
		assert.deepStrictEqual(input, [1, 2, 3]);
	});
});
