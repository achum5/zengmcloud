import { assert, describe, test } from "vitest";
import { svgsIndex } from "facesjs";
import type { FaceConfig } from "facesjs";
import { resetG } from "../../test/helpers.ts";
import { g } from "./index.ts";
import { generateFace } from "./face.ts";
import {
	ageFace,
	applyRealisticFace,
	bandForAge,
	applyWrinkles,
	baldingProne,
	growsFacialHair,
	inferRaceFromFace,
	MAX_WRINKLE_LEVEL,
	wrinkleLevelForAge,
	wrinkleLevelOf,
	FACIAL_HAIR_TIERS,
	facialHairForAge,
	HAIR_RARE,
	HAIR_TEXTURES,
	hairAllowedForRace,
	hairPoolForRace,
	HAIR_BALD,
	HAIR_THINNING,
	jitterColor,
	tierOf,
} from "./realisticFaces.ts";

// A deterministic stand-in for Math.random, so every probabilistic branch can
// be driven to the exact case under test.
const fixed = (value: number) => () => value;
const sequence = (values: number[]) => {
	let i = 0;
	return () => values[i++ % values.length]!;
};

const face = (overrides: Record<string, any> = {}) =>
	({
		smileLine: { id: "none", size: 1 },
		eyeLine: { id: "none" },
		miscLine: { id: "none" },
		facialHair: { id: "none" },
		hair: { id: "short", color: "#272421", flip: false },
		glasses: { id: "none" },
		body: { id: "body", color: "#ad6453", size: 1 },
		...overrides,
	}) as unknown as FaceConfig;

describe("style groups", () => {
	test("cover every facesjs style exactly once", () => {
		// The groups were assigned by rendering all 83 styles and looking at
		// them. If facesjs ever adds or renames one, it must be classified
		// rather than silently dropping out of circulation - so this fails
		// loudly on a library upgrade.
		const grouped = Object.values(FACIAL_HAIR_TIERS).flat();
		assert.strictEqual(
			new Set(grouped).size,
			grouped.length,
			"a style is in two groups",
		);
		assert.deepStrictEqual(
			[...grouped, "none"].toSorted(),
			[...svgsIndex.facialHair].toSorted(),
		);
	});

	test("the balding hairstyles exist in facesjs", () => {
		assert.include(svgsIndex.hair, HAIR_THINNING);
		assert.include(svgsIndex.hair, HAIR_BALD);
	});

	test("the texture groups cover the male hair catalog exactly", () => {
		// Classified from rendered appearance (juice and high are hi-top fades -
		// a name alone would never tell you). Female styles are outside the male
		// generator's reach and stay unclassified. A facesjs upgrade that adds a
		// style must be classified, not silently left uniform.
		const grouped = Object.values(HAIR_TEXTURES).flat();
		assert.strictEqual(
			new Set(grouped).size,
			grouped.length,
			"a style is in two texture groups",
		);
		const male = svgsIndex.hair.filter((id) => !id.startsWith("female"));
		assert.deepStrictEqual([...grouped].toSorted(), [...male].toSorted());
	});
});

describe("hairAllowedForRace", () => {
	test("straight flowing styles never land on Black players", () => {
		for (const id of HAIR_TEXTURES.straight) {
			assert.isFalse(hairAllowedForRace(id, "black"), id);
		}
	});

	test("tightly coiled styles never land on white or asian players", () => {
		for (const id of HAIR_TEXTURES.coiled) {
			assert.isFalse(hairAllowedForRace(id, "white"), id);
			assert.isFalse(hairAllowedForRace(id, "asian"), id);
		}
	});

	test("universal styles land on everyone", () => {
		for (const id of HAIR_TEXTURES.universal) {
			for (const race of ["white", "black", "brown", "asian"] as const) {
				assert.isTrue(hairAllowedForRace(id, race), `${id} for ${race}`);
			}
		}
	});

	test("brown spans the widest real range and keeps everything but the long styles", () => {
		for (const id of [...HAIR_TEXTURES.straight, ...HAIR_TEXTURES.coiled]) {
			if (HAIR_RARE.includes(id)) {
				continue;
			}
			assert.isTrue(hairAllowedForRace(id, "brown"), id);
		}
	});

	test("hair worn long and loose is white-only", () => {
		for (const id of HAIR_RARE) {
			assert.isTrue(hairAllowedForRace(id, "white"), id);
			for (const race of ["black", "brown", "asian"] as const) {
				assert.isFalse(hairAllowedForRace(id, race), `${id} for ${race}`);
			}
		}
	});

	test("no race known means nothing to rule out", () => {
		// Generated relatives inherit a face rather than a race.
		assert.isTrue(hairAllowedForRace("middle-part", undefined));
	});
});

describe("HAIR_RARE", () => {
	test("names real styles, and they are classified for texture too", () => {
		// Era and texture are separate axes: a style being held back does not
		// exempt it from the coverage test above.
		const classified = Object.values(HAIR_TEXTURES).flat();
		for (const id of HAIR_RARE) {
			assert.include(classified, id, id);
		}
	});

	test("a re-roll never lands back on one", () => {
		// Otherwise thinning them out would just reshuffle among themselves.
		for (const race of ["white", "black", "brown", "asian"] as const) {
			for (const id of hairPoolForRace(race)) {
				assert.notInclude(HAIR_RARE, id, `${id} for ${race}`);
			}
		}
	});

	test("mostly re-rolled away, occasionally kept", () => {
		// rand() >= keep re-rolls, so a high roll drops it and a low roll keeps
		// it. Both paths must exist or the style is either gone or unchanged.
		// White, where the style is permitted at all - so what is being tested
		// here is the rarity roll and not the ancestry rule.
		const dropped = face({
			hair: { id: "longHair", color: "#272421", flip: false },
		});
		applyRealisticFace(dropped, { age: 25, race: "white", rand: fixed(0.9) });
		assert.notStrictEqual(dropped.hair.id, "longHair");

		const kept = face({
			hair: { id: "longHair", color: "#272421", flip: false },
		});
		// High roll for the facial hair chance (so it stays "none" and consumes
		// nothing more), low roll for the keep, then high again so the balding
		// chance further down does not claim the hair instead.
		applyRealisticFace(kept, {
			age: 25,
			race: "white",
			rand: sequence([0.9, 0.01, 0.9, 0.9, 0.9, 0.9]),
		});
		assert.strictEqual(kept.hair.id, "longHair");
	});

	test("the reported face: long curtains on a brown-skinned 30-year-old", () => {
		// Reported from a freshly created league with the setting on, so the
		// rule itself was wrong rather than un-applied: these read as one
		// ancestry, and are rare even there.
		assert.isFalse(hairAllowedForRace("longHair", "brown"));
		const p = face({ hair: { id: "longHair", color: "#272421", flip: false } });
		applyRealisticFace(p, { age: 30, race: "brown", rand: fixed(0.5) });
		assert.notStrictEqual(p.hair.id, "longHair");
	});
});

describe("hairPoolForRace", () => {
	test("every pool entry is allowed for its race, and balding looks stay out", () => {
		for (const race of ["white", "black", "brown", "asian"] as const) {
			const pool = hairPoolForRace(race);
			assert.isAbove(pool.length, 10);
			for (const id of pool) {
				assert.isTrue(hairAllowedForRace(id, race), `${id} for ${race}`);
				assert.notInclude([HAIR_THINNING, HAIR_BALD], id);
			}
		}
	});
});

describe("facialHairForAge", () => {
	test("a prospect never gets a beard or a period style", () => {
		// Drive the roll so facial hair is always chosen, then check every
		// style a 19-year-old can possibly land on.
		for (let i = 0; i < 200; i++) {
			const id = facialHairForAge(19, sequence([0, i / 200]));
			if (id !== "none") {
				assert.strictEqual(
					tierOf(id),
					"light",
					`19-year-old should not get ${id}`,
				);
			}
		}
	});

	test("period styles are reachable only for the oldest players", () => {
		const reachable = (age: number) => {
			const tiers = new Set<string | undefined>();
			for (let i = 0; i < 400; i++) {
				const id = facialHairForAge(age, sequence([0, i / 400, i / 400]));
				if (id !== "none") {
					tiers.add(tierOf(id));
				}
			}
			return tiers;
		};
		assert.notInclude([...reachable(22)], "period");
		assert.notInclude([...reachable(26)], "period");
		assert.notInclude([...reachable(30)], "period");
		assert.include([...reachable(33)], "period");
	});

	test("older players get facial hair more often than prospects", () => {
		assert.isAbove(bandForAge(33).facialHair, bandForAge(19).facialHair);
		assert.isAbove(bandForAge(28).balding, bandForAge(19).balding);
	});
});

describe("applyRealisticFace", () => {
	test("a prospect is never balding, even if the generator said so", () => {
		for (const id of [HAIR_THINNING, HAIR_BALD]) {
			const f = face({ hair: { id, color: "#272421", flip: false } });
			applyRealisticFace(f, { age: 19, rand: fixed(0.99) });
			assert.notStrictEqual(f.hair.id, id);
		}
	});

	test("an older player can be balding", () => {
		const f = face();
		applyRealisticFace(f, { age: 34, rand: fixed(0.001) });
		assert.include([HAIR_THINNING, HAIR_BALD], f.hair.id);
	});

	test("a texture-implausible style is re-rolled, a plausible one is kept", () => {
		const f = face({
			hair: { id: "middle-part", color: "#272421", flip: false },
		});
		applyRealisticFace(f, { age: 25, race: "black", rand: fixed(0.4) });
		assert.isTrue(hairAllowedForRace(f.hair.id, "black"), f.hair.id);

		const f2 = face({ hair: { id: "afro", color: "#272421", flip: false } });
		applyRealisticFace(f2, { age: 25, race: "white", rand: fixed(0.4) });
		assert.isTrue(hairAllowedForRace(f2.hair.id, "white"), f2.hair.id);

		const f3 = face({ hair: { id: "afro", color: "#272421", flip: false } });
		applyRealisticFace(f3, { age: 25, race: "black", rand: fixed(0.4) });
		assert.strictEqual(f3.hair.id, "afro");

		// No race, no re-roll - a relative's inherited face stays as generated.
		const f4 = face({
			hair: { id: "middle-part", color: "#272421", flip: false },
		});
		applyRealisticFace(f4, { age: 25, rand: fixed(0.4) });
		assert.strictEqual(f4.hair.id, "middle-part");
	});

	test("colors are nudged but stay recognizably the same shade", () => {
		const f = face();
		applyRealisticFace(f, { age: 25, rand: fixed(0.9) });
		assert.notStrictEqual(f.body.color, "#ad6453");
		// Same hue family - a jitter, not a recolor.
		const [r, g, b] = [1, 3, 5].map((i) =>
			Number.parseInt(f.body.color.slice(i, i + 2), 16),
		);
		assert.isAbove(r!, g!);
		assert.isAbove(g!, b!);
	});
});

describe("jitterColor", () => {
	test("leaves anything that is not a plain hex color alone", () => {
		assert.strictEqual(jitterColor("none", Math.random, 0.1), "none");
	});

	test("stays inside the byte range at the extremes", () => {
		assert.strictEqual(jitterColor("#ffffff", fixed(1), 0.5), "#ffffff");
		assert.strictEqual(jitterColor("#000000", fixed(0), 0.5), "#000000");
	});
});

describe("ageFace", () => {
	// Aging used to fire at three fixed ages, so a career was three jumps and
	// anyone past the last one never changed again. It now rolls every
	// preseason, which spreads change across a career - and, crucially, is
	// gated on a per-player trait so it does not happen to everyone.

	const proneToBald = (() => {
		for (let pid = 0; pid < 500; pid++) {
			if (baldingProne(pid)) {
				return pid;
			}
		}
		throw new Error("no balding-prone pid found");
	})();

	const neverBalds = (() => {
		for (let pid = 0; pid < 500; pid++) {
			if (!baldingProne(pid)) {
				return pid;
			}
		}
		throw new Error("no balding-immune pid found");
	})();

	test("a player who was never going to lose it keeps it at any age", () => {
		// The reported gap: some players still have their hair in their 30s.
		// Even with the roll forced to always succeed, this one never balds.
		const f = face();
		for (let age = 19; age <= 40; age++) {
			ageFace(f, age, neverBalds, fixed(0));
		}
		assert.strictEqual(f.hair.id, "short");
	});

	test("a susceptible player can lose it, one step at a time", () => {
		const f = face();
		ageFace(f, 28, proneToBald, fixed(0.001));
		assert.strictEqual(f.hair.id, HAIR_THINNING);
		ageFace(f, 33, proneToBald, fixed(0.001));
		assert.strictEqual(f.hair.id, HAIR_BALD);
		// Already gone: it stays gone, however the roll lands.
		ageFace(f, 34, proneToBald, fixed(0.001));
		assert.strictEqual(f.hair.id, HAIR_BALD);
	});

	test("no hairline loss before the twenties bands, even when susceptible", () => {
		const f = face();
		ageFace(f, 20, proneToBald, fixed(0));
		assert.strictEqual(f.hair.id, "short");
	});

	test("it can still change past the old last threshold", () => {
		// The old three-threshold scheme froze a 32-year-old forever.
		const f = face();
		ageFace(f, 36, proneToBald, fixed(0.001));
		assert.strictEqual(f.hair.id, HAIR_THINNING);
	});

	test("facial hair never thins back out, at any age", () => {
		for (const age of [24, 28, 33, 38]) {
			for (const start of ["goatee1", "fullgoatee", "beard1"]) {
				const f = face({ facialHair: { id: start } });
				ageFace(f, age, 1, sequence([0, 0.5, 0.5, 0.99]));
				const order = ["light", "medium", "heavy", "period"];
				assert.isAtLeast(
					order.indexOf(tierOf(f.facialHair.id)!),
					order.indexOf(tierOf(start)!),
					`${start} regressed to ${f.facialHair.id} at ${age}`,
				);
			}
		}
	});

	test("a typical season changes nothing", () => {
		// Rolling every year only works if most years are quiet, or a player
		// would be unrecognisable from one season to the next.
		let changed = 0;
		for (let pid = 0; pid < 300; pid++) {
			const f = face();
			if (ageFace(f, 28, pid)) {
				changed += 1;
			}
		}
		// Wrinkles advance more often than hair does, but a line step is a
		// subtle thing - the bar is that most seasons still change nothing.
		assert.isBelow(changed, 150, `${changed}/300 changed in one season`);
	});
});

describe("per-player traits", () => {
	test("the same player always gets the same answer", () => {
		// Derived from the id rather than stored, so it survives reloads,
		// exports and every device in a synced league.
		for (const pid of [0, 7, 1234]) {
			assert.strictEqual(baldingProne(pid), baldingProne(pid));
			assert.strictEqual(growsFacialHair(pid), growsFacialHair(pid));
		}
	});

	test("susceptibility is a minority, and beards are a majority", () => {
		let bald = 0;
		let beard = 0;
		const N = 4000;
		for (let pid = 0; pid < N; pid++) {
			if (baldingProne(pid)) {
				bald += 1;
			}
			if (growsFacialHair(pid)) {
				beard += 1;
			}
		}
		assert.isAbove(bald / N, 0.3);
		assert.isBelow(bald / N, 0.5);
		assert.isAbove(beard / N, 0.7);
	});

	test("the two traits are independent of each other", () => {
		// Same hash with a different salt, so a balding player is no more or
		// less likely to grow a beard.
		let both = 0;
		const N = 4000;
		for (let pid = 0; pid < N; pid++) {
			if (baldingProne(pid) && growsFacialHair(pid)) {
				both += 1;
			}
		}
		// 0.4 * 0.8 = 0.32 if independent.
		assert.isAbove(both / N, 0.27);
		assert.isBelow(both / N, 0.37);
	});

	test("an unknown player still ages, but never balds", () => {
		// No pid (a face edited outside a league): the safe answer is to leave
		// the hairline alone rather than guess.
		assert.isFalse(baldingProne(undefined));
		assert.isTrue(growsFacialHair(undefined));
	});
});

describe("generateFace plumbing", () => {
	// g.get throws on an attribute it doesn't know about, so a missing entry
	// anywhere in the chain would crash every player generation rather than
	// fail quietly.
	test("on by default, and generation works with it on", () => {
		resetG();
		assert.strictEqual(g.get("realisticFaces"), true);
		assert.doesNotThrow(() => generateFace({ age: 19 }));
	});

	test("with it on, a draft class has no period styles and nobody balding", () => {
		resetG();
		g.setWithoutSavingToDB("realisticFaces", true);
		for (let i = 0; i < 200; i++) {
			const face = generateFace({ age: 19 + (i % 4) });
			const tier = tierOf(face.facialHair.id);
			assert.notStrictEqual(tier, "period", face.facialHair.id);
			assert.notStrictEqual(tier, "heavy", face.facialHair.id);
			assert.notInclude([HAIR_THINNING, HAIR_BALD], face.hair.id);
		}
	});
});

describe("wrinkles", () => {
	test("nobody is weathered in their early twenties", () => {
		assert.strictEqual(wrinkleLevelForAge(19), 0);
		assert.strictEqual(wrinkleLevelForAge(22), 0);
		assert.isAbove(wrinkleLevelForAge(28), 0);
		assert.strictEqual(wrinkleLevelForAge(38), MAX_WRINKLE_LEVEL);
	});

	test("the ceiling never falls as a player gets older", () => {
		for (let age = 19; age < 45; age++) {
			assert.isAtLeast(wrinkleLevelForAge(age + 1), wrinkleLevelForAge(age));
		}
	});

	test("a level round-trips through a face", () => {
		// Aging reads the level back off the face to know where to go next, so
		// writing a level and reading it must agree.
		for (let level = 0; level <= MAX_WRINKLE_LEVEL; level++) {
			const f = face();
			applyWrinkles(f, level, fixed(0.9));
			assert.strictEqual(wrinkleLevelOf(f), level);
		}
	});

	test("higher levels mean deeper folds", () => {
		const young = face();
		applyWrinkles(young, 0, fixed(0.9));
		const old = face();
		applyWrinkles(old, MAX_WRINKLE_LEVEL, fixed(0.9));
		assert.isAbove(old.smileLine.size, young.smileLine.size);
		assert.strictEqual(young.eyeLine.id, "none");
		assert.notStrictEqual(old.eyeLine.id, "none");
		assert.notStrictEqual(old.miscLine.id, "none");
	});

	test("freckles and blush survive aging", () => {
		// They share a slot with the brow lines but are not age - a freckled
		// player should still be freckled at 38.
		for (const id of ["freckles1", "blush", "chin1"]) {
			const f = face({ miscLine: { id } });
			applyWrinkles(f, MAX_WRINKLE_LEVEL, fixed(0.9));
			assert.strictEqual(f.miscLine.id, id);
			// ...but he still ages everywhere else.
			assert.notStrictEqual(f.smileLine.id, "none");
		}
	});

	test("lines never go past what the age allows", () => {
		const f = face();
		for (let age = 19; age <= 26; age++) {
			ageFace(f, age, 1, fixed(0));
		}
		assert.isAtMost(wrinkleLevelOf(f), wrinkleLevelForAge(26));
	});

	test("a long career visibly weathers a face", () => {
		const f = face();
		for (let age = 20; age <= 39; age++) {
			ageFace(f, age, 1, fixed(0));
		}
		assert.strictEqual(wrinkleLevelOf(f), MAX_WRINKLE_LEVEL);
	});
});

describe("inferRaceFromFace", () => {
	test("reads a palette skin color back to its race", () => {
		// Existing players do not store a race, so the retroactive pass has to
		// recover it from the one durable trace.
		const cases = [
			["#f2d6cb", "white"],
			["#eab687", "asian"],
			["#a67358", "brown"],
			["#5c3937", "black"],
		] as const;
		for (const [color, race] of cases) {
			assert.strictEqual(
				inferRaceFromFace({ body: { color } } as any),
				race,
				color,
			);
		}
	});

	test("a jittered color still lands on its own palette", () => {
		// Generation nudges skin lightness, so the stored color is never an
		// exact palette entry.
		assert.strictEqual(
			inferRaceFromFace({ body: { color: "#5b3836" } } as any),
			"black",
		);
	});

	test("no usable color, no guess", () => {
		assert.isUndefined(inferRaceFromFace({} as any));
		assert.isUndefined(inferRaceFromFace({ body: { color: "red" } } as any));
	});
});
