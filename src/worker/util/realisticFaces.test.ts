import { assert, describe, test } from "vitest";
import { svgsIndex } from "facesjs";
import type { FaceConfig } from "facesjs";
import { resetG } from "../../test/helpers.ts";
import { g } from "./index.ts";
import { generateFace } from "./face.ts";
import { mulberry32 } from "../../common/sportsbookOdds.ts";
import {
	ageFace,
	applyRealisticFace,
	bandForAge,
	applyWrinkles,
	lineStylesFor,
	smileSizeForAge,
	baldingProne,
	growsFacialHair,
	inferRaceFromFace,
	MAX_WRINKLE_LEVEL,
	weathersLess,
	wrinkleCeiling,
	wrinkleLevelForAge,
	wrinkleLevelOf,
	FACE_AGE_BANDS,
	FACIAL_HAIR_TIERS,
	GENERATED_FACIAL_HAIR,
	NEVER_GENERATE,
	facialHairForAge,
	HAIR_RARE,
	HAIR_TEXTURES,
	hairAllowedForRace,
	hairPoolForRace,
	HAIR_BALD,
	HAIR_THINNING,
	jitterColor,
	tierOf,
	shavesHead,
	shavesHeadAtAge,
	familySeed,
	fatnessGainByAge,
	HAIR_VOLUMINOUS,
	SHAVES_HEAD_SHARE,
	BALDING_PRONE_SHARE,
	HAIR_PERIOD,
	applyFaceAgingHistory,
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
		hairBg: { id: "none" },
		head: { id: "head1", shave: "rgba(0,0,0,0)" },
		fatness: 0.4,
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

	// THE STYLES THE LEAGUE NEVER GROWS.
	//
	// Named here rather than read back out of NEVER_GENERATE, which would make
	// this test agree with whatever the code currently says. These are the ones
	// that were looked at and rejected, and the list is the assertion.
	const REJECTED = [
		// The field report: a 23-year-old in a mustache and flared chops.
		// mustache1SB1 is a perfectly ordinary medium-tier style and he was old
		// enough for the medium tier, so no age rule was ever going to keep it
		// off him.
		"mustache1SB1",
		"mustache1SB2",
		// Beaded, which renders as pale blue blocks under the chin.
		"beard5",
		"beard6",
		"fullgoatee5",
		"fullgoatee6",
		// Costume: biker horseshoe, Amish chin curtain, Wolverine chops, mutton
		// chops, neckbeard, Wilt's sideburns.
		"harley1",
		"honest-abe",
		"logan",
		"mutton",
		"neckbeard",
		"wilt-sideburns-long",
	];

	test("a rejected style is never generated, at any age", () => {
		for (const id of REJECTED) {
			assert.isTrue(NEVER_GENERATE.has(id), `${id} should be excluded`);
		}
		for (const age of [19, 22, 26, 30, 33, 38, 44]) {
			for (let i = 0; i < 400; i++) {
				const id = facialHairForAge(age, sequence([0, i / 400, i / 400]));
				assert.notInclude(REJECTED, id, `${age}-year-old should not get ${id}`);
			}
		}
	});

	test("a rejected style is never grown into either", () => {
		// The other way a face gets one: thickening off an existing style,
		// which reads its own list and used to read the unfiltered one.
		for (const age of [22, 26, 31, 36]) {
			for (let i = 0; i < 300; i++) {
				const f = face();
				f.facialHair.id = "goatee1";
				ageFace(f, age, 7, sequence([0, i / 300, i / 300]));
				assert.notInclude(REJECTED, f.facialHair.id);
			}
		}
	});

	test("the period tier is listed for classification but never grown", () => {
		// The list still has to exist: it is what says a face that already has
		// mutton chops is at the top tier and should not thicken past it.
		assert.isAbove(FACIAL_HAIR_TIERS.period.length, 0);
		for (const id of FACIAL_HAIR_TIERS.period) {
			assert.isTrue(NEVER_GENERATE.has(id), id);
			assert.strictEqual(tierOf(id), "period", id);
		}
		assert.isUndefined(GENERATED_FACIAL_HAIR.period);
	});

	test("every tier that can still be grown has something to grow", () => {
		// A tier left in the age bands with an empty generated list would be
		// picked and then have nothing to return.
		for (const band of FACE_AGE_BANDS) {
			for (const tier of Object.keys(band.tiers)) {
				assert.isAbove(
					GENERATED_FACIAL_HAIR[tier as keyof typeof FACIAL_HAIR_TIERS]
						?.length ?? 0,
					0,
					`${tier} is in a band but has nothing to generate`,
				);
			}
		}
	});

	test("older players get facial hair more often than prospects", () => {
		assert.isAbove(bandForAge(33).facialHair, bandForAge(19).facialHair);
		assert.isAbove(bandForAge(28).balding, bandForAge(19).balding);
	});
});

describe("applyRealisticFace", () => {
	// A RECEDING hairline is age. A SHAVED head is a haircut, and forcing it off
	// young players meant nobody in the league could turn up with the most
	// recognisable look in basketball until his late twenties.
	test("a prospect never has a receding hairline, even if the generator said so", () => {
		const f = face({
			hair: { id: HAIR_THINNING, color: "#272421", flip: false },
		});
		applyRealisticFace(f, { age: 19, rand: fixed(0.99) });
		assert.notStrictEqual(f.hair.id, HAIR_THINNING);
	});

	test("a prospect may have a shaved head, because that is a choice", () => {
		const f = face({ hair: { id: HAIR_BALD, color: "#272421", flip: false } });
		applyRealisticFace(f, { age: 19, rand: fixed(0.99) });
		assert.strictEqual(f.hair.id, HAIR_BALD);
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

	// Not prone to losing it AND not one of the players who shaves it off, so
	// the only thing that could change his hair is the balding ladder.
	const keepsHisHair = (() => {
		for (let pid = 0; pid < 500; pid++) {
			if (!baldingProne(pid) && !shavesHead(pid)) {
				return pid;
			}
		}
		throw new Error("no pid found who keeps his hair");
	})();

	test("a player who was never going to lose it keeps it at any age", () => {
		// The reported gap: some players still have their hair in their 30s.
		// Even with the roll forced to always succeed, this one never balds.
		const f = face();
		for (let age = 19; age <= 40; age++) {
			ageFace(f, age, keepsHisHair, fixed(0));
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
			// A shaved head is allowed at this age; a receding one is not.
			assert.notStrictEqual(face.hair.id, HAIR_THINNING);
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
		for (let level = 0; level <= MAX_WRINKLE_LEVEL; level++) {
			const f = face();
			applyWrinkles(f, level, 7);
			assert.strictEqual(wrinkleLevelOf(f), level);
		}
	});

	test("features turn on in order and never switch style", () => {
		// The reported bug: the line features are variants, not degrees, so
		// walking a "ladder" swapped one mark for another and a player's eye
		// bags disappeared at 35. Each player has one style per feature now.
		const styles = lineStylesFor(7);
		const f = face();
		applyWrinkles(f, 1, 7);
		assert.strictEqual(f.smileLine.id, styles.smile);
		assert.strictEqual(f.eyeLine.id, "none");

		applyWrinkles(f, 2, 7);
		assert.strictEqual(f.smileLine.id, styles.smile, "style must not change");
		assert.strictEqual(f.eyeLine.id, styles.eye);

		applyWrinkles(f, 3, 7);
		assert.strictEqual(f.smileLine.id, styles.smile);
		assert.strictEqual(f.eyeLine.id, styles.eye, "style must not change");
		assert.strictEqual(f.miscLine.id, styles.forehead);
	});

	test("a player's styles are the same every time", () => {
		assert.deepStrictEqual(lineStylesFor(42), lineStylesFor(42));
	});

	test("nothing a face already has is ever taken away", () => {
		// The invariant that makes un-aging impossible: reading the level off a
		// face returns the HIGHEST feature present, so re-applying it can only
		// fill in what is missing.
		const f = face({ miscLine: { id: "forehead2" } });
		assert.strictEqual(wrinkleLevelOf(f), 3);
		applyWrinkles(f, wrinkleLevelOf(f), 7);
		assert.notStrictEqual(f.miscLine.id, "none");
		assert.notStrictEqual(f.smileLine.id, "none");
		assert.notStrictEqual(f.eyeLine.id, "none");
	});

	test("freckles and blush survive aging", () => {
		for (const id of ["freckles1", "blush", "chin1"]) {
			const f = face({ miscLine: { id } });
			applyWrinkles(f, MAX_WRINKLE_LEVEL, 7);
			assert.strictEqual(f.miscLine.id, id);
			assert.notStrictEqual(f.smileLine.id, "none");
		}
	});

	test("folds deepen with age and never shrink", () => {
		let previous = 0;
		for (let age = 19; age <= 45; age++) {
			const size = smileSizeForAge(age);
			assert.isAtLeast(size, previous, `age ${age}`);
			previous = size;
		}
		assert.isAbove(smileSizeForAge(38), smileSizeForAge(22));
	});

	test("lines never go past what the age allows", () => {
		const f = face();
		for (let age = 19; age <= 26; age++) {
			ageFace(f, age, 1, fixed(0));
		}
		assert.isAtMost(wrinkleLevelOf(f), wrinkleLevelForAge(26));
	});

	test("a long career weathers a face to its own ceiling", () => {
		const f = face();
		for (let age = 20; age <= 39; age++) {
			ageFace(f, age, 1, fixed(0));
		}
		assert.strictEqual(wrinkleLevelOf(f), wrinkleCeiling(39, 1));
		assert.isAbove(wrinkleLevelOf(f), 0);
	});

	test("A WHOLE CAREER NEVER GOES BACKWARDS", () => {
		// The guarantee the reported bug violated, checked directly over many
		// full careers: no feature ever turns off, no style ever changes, and
		// the folds never shrink.
		for (let pid = 0; pid < 300; pid++) {
			const f = face();
			applyWrinkles(f, 0, pid, smileSizeForAge(19));
			let level = wrinkleLevelOf(f);
			let size = f.smileLine.size;
			const seen: Record<string, string> = {};
			for (let age = 20; age <= 40; age++) {
				ageFace(f, age, pid);
				assert.isAtLeast(wrinkleLevelOf(f), level, `pid ${pid} age ${age}`);
				assert.isAtLeast(f.smileLine.size, size, `pid ${pid} age ${age} size`);
				for (const key of ["smileLine", "eyeLine", "miscLine"] as const) {
					const id = (f as any)[key].id;
					if (id !== "none") {
						if (seen[key] !== undefined) {
							assert.strictEqual(
								id,
								seen[key],
								`pid ${pid} ${key} changed style at ${age}`,
							);
						}
						seen[key] = id;
					}
				}
				level = wrinkleLevelOf(f);
				size = f.smileLine.size;
			}
		}
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

describe("the continuous half of aging", () => {
	// Steps are lumpy by nature - nothing for four years and then a man is
	// suddenly bald. This is the part that moves a little every season, so most
	// years look slightly different without anything dramatic happening.

	test("folds deepen between level steps, and stop at the ceiling", () => {
		const f = face({ smileLine: { id: "none", size: 0.6 } });
		const sizes: number[] = [];
		for (let age = 20; age <= 40; age++) {
			ageFace(f, age, 1, fixed(0.99));
			sizes.push(f.smileLine.size);
		}
		// fixed(0.99) refuses every discrete roll, so any growth here is the
		// continuous creep alone.
		assert.isAbove(sizes.at(-1)!, 0.6);
		for (let i = 1; i < sizes.length; i++) {
			assert.isAtLeast(sizes[i]!, sizes[i - 1]!);
		}
	});

	test("a season of only deepening folds is not worth a history entry", () => {
		// The player is written back every preseason anyway; recording one
		// snapshot per season would store twenty near-identical faces to
		// capture something only visible across a decade.
		const f = face({ smileLine: { id: "none", size: 0.6 } });
		assert.isFalse(ageFace(f, 30, 1, fixed(0.99)));
	});
});

describe("weathering varies by player", () => {
	test("a quarter of players never reach the last step", () => {
		let less = 0;
		const N = 4000;
		for (let pid = 0; pid < N; pid++) {
			if (weathersLess(pid)) {
				less += 1;
			}
		}
		assert.isAbove(less / N, 0.2);
		assert.isBelow(less / N, 0.31);
	});

	test("their ceiling is one lower, and never negative", () => {
		const heavy = (() => {
			for (let pid = 0; pid < 500; pid++) {
				if (weathersLess(pid)) {
					return pid;
				}
			}
			throw new Error("none found");
		})();
		assert.strictEqual(wrinkleCeiling(40, heavy), wrinkleLevelForAge(40) - 1);
		assert.strictEqual(wrinkleCeiling(19, heavy), 0);
	});
});

describe("hairline loss is gradual", () => {
	test("going fully bald is rarer than starting to recede", () => {
		// Otherwise a man goes from a full head to bald in two seasons, which
		// reads as a glitch rather than as aging.
		const prone = (() => {
			for (let pid = 0; pid < 500; pid++) {
				if (baldingProne(pid)) {
					return pid;
				}
			}
			throw new Error("none found");
		})();

		const countStep = (startId: string) => {
			let hits = 0;
			for (let trial = 0; trial < 4000; trial++) {
				const f = face({
					hair: { id: startId, color: "#272421", flip: false },
				});
				ageFace(f, 33, prone);
				if (f.hair.id !== startId) {
					hits += 1;
				}
			}
			return hits;
		};

		const first = countStep("short");
		const second = countStep(HAIR_THINNING);
		assert.isAbove(first, second, `${first} vs ${second}`);
	});
});

describe("replaying a career that already happened", () => {
	// applyFaceAgingToLeague starts from the face the player already has. If
	// that pass re-coloured him, running it twice would move him further from
	// himself each time - and would drift a father and son apart independently,
	// undoing the resemblance the relative code goes to trouble to keep.
	test("aging an existing league leaves his colours alone", () => {
		const before = face();
		before.body.color = "#ad6453";
		before.hair.color = "#272421";
		applyFaceAgingHistory({
			face: before,
			rookieAge: 19,
			currentAge: 27,
			pid: 3,
			race: "black",
			rand: fixed(0.5),
		});
		assert.strictEqual(before.body.color, "#ad6453");
		assert.strictEqual(before.hair.color, "#272421");
	});

	test("two runs produce the same man", () => {
		const run = () => {
			const f = face();
			f.body.color = "#ad6453";
			f.hair.color = "#272421";
			applyFaceAgingHistory({
				face: f,
				rookieAge: 19,
				currentAge: 34,
				pid: 11,
				race: "black",
				rand: fixed(0.5),
			});
			return f;
		};
		const first = run();
		const second = run();
		applyFaceAgingHistory({
			face: first,
			rookieAge: 19,
			currentAge: 34,
			pid: 11,
			race: "black",
			rand: fixed(0.5),
		});
		assert.strictEqual(first.body.color, second.body.color);
	});
});

describe("period cuts", () => {
	test("the hi-top fades are real facesjs hair, and out of the pool", () => {
		for (const id of HAIR_PERIOD) {
			assert.include(svgsIndex.hair, id, id);
		}
		for (const race of ["black", "brown"] as const) {
			for (const id of HAIR_PERIOD) {
				assert.notInclude(hairPoolForRace(race), id, `${race}/${id}`);
			}
		}
	});

	// Thinned, not deleted: a league can be set in any era, and deleting them
	// outright would cost variety it cannot spare.
	test("thinned rather than removed", () => {
		// Under the keep rate, and clear of the age band's balding roll.
		const kept = face({ hair: { id: "high", color: "#272421", flip: false } });
		applyRealisticFace(kept, { age: 25, race: "black", rand: fixed(0.1) });
		assert.strictEqual(kept.hair.id, "high");

		const dropped = face({
			hair: { id: "high", color: "#272421", flip: false },
		});
		applyRealisticFace(dropped, { age: 25, race: "black", rand: fixed(0.99) });
		assert.notStrictEqual(dropped.hair.id, "high");
	});

	// Unlike HAIR_RARE, the thinning here is not about who can wear these. They
	// stay ordinary coiled styles, judged on texture exactly like an afro; all
	// that changes is how OFTEN they turn up.
	test("they are still ordinary coiled styles, judged on texture", () => {
		for (const id of HAIR_PERIOD) {
			assert.isTrue(hairAllowedForRace(id, "black"), id);
			assert.isTrue(hairAllowedForRace(id, "brown"), id);
			assert.isFalse(hairAllowedForRace(id, "white"), id);
		}
	});
});

describe("a shaved head", () => {
	const shaver = (() => {
		for (let pid = 0; pid < 500; pid++) {
			if (shavesHead(pid)) {
				return pid;
			}
		}
		throw new Error("no head-shaving pid found");
	})();

	test("about one player in ten, and always the same ones", () => {
		let count = 0;
		for (let pid = 0; pid < 4000; pid++) {
			if (shavesHead(pid)) {
				count += 1;
			}
			assert.strictEqual(shavesHead(pid), shavesHead(pid));
		}
		assert.closeTo(count / 4000, SHAVES_HEAD_SHARE, 0.03);
	});

	test("he does it young, and it sticks", () => {
		const age = shavesHeadAtAge(shaver);
		assert.isAtLeast(age, 21);
		assert.isBelow(age, 29);

		const f = face();
		ageFace(f, age - 1, shaver, fixed(0.99));
		assert.strictEqual(f.hair.id, "short");
		ageFace(f, age, shaver, fixed(0.99));
		assert.strictEqual(f.hair.id, HAIR_BALD);
		for (let a = age + 1; a <= 40; a++) {
			ageFace(f, a, shaver, fixed(0.99));
			assert.strictEqual(f.hair.id, HAIR_BALD);
		}
	});

	// Long hair hanging off the back of a head with none on top is the one
	// combination facesjs will draw and no head has ever worn.
	test("shaving clears anything hanging off the back of the head", () => {
		const f = face();
		f.hairBg.id = "longHair";
		ageFace(f, shavesHeadAtAge(shaver), shaver, fixed(0.99));
		assert.strictEqual(f.hairBg.id, "none");
	});

	test("a shaved scalp gets a shadow, so it reads as shaved and not hairless", () => {
		const f = face();
		f.head.shave = "rgba(0,0,0,0)";
		ageFace(f, shavesHeadAtAge(shaver), shaver, fixed(0.99));
		assert.notStrictEqual(f.head.shave, "rgba(0,0,0,0)");
	});
});

describe("the balding ladder", () => {
	const proneNoShave = (() => {
		for (let pid = 0; pid < 500; pid++) {
			if (baldingProne(pid) && !shavesHead(pid)) {
				return pid;
			}
		}
		throw new Error("no prone, non-shaving pid found");
	})();

	test("the voluminous styles are real facesjs hair", () => {
		for (const id of HAIR_VOLUMINOUS) {
			assert.include(svgsIndex.hair, id, id);
		}
	});

	// Volume goes before the hairline does. Dreads to a horseshoe in one
	// preseason reads as a glitch, not as aging.
	test("big hair is cut back before the hairline shows", () => {
		for (const id of ["afro", "dreads", "longHair", "high"]) {
			const f = face({ hair: { id, color: "#272421", flip: false } });
			ageFace(f, 34, proneNoShave, fixed(0.001));
			assert.notStrictEqual(f.hair.id, HAIR_THINNING, id);
			assert.notStrictEqual(f.hair.id, HAIR_BALD, id);
			assert.notStrictEqual(f.hair.id, id, id);

			// And from there it carries on down the ladder.
			ageFace(f, 35, proneNoShave, fixed(0.001));
			assert.strictEqual(f.hair.id, HAIR_THINNING, id);
			ageFace(f, 36, proneNoShave, fixed(0.001));
			assert.strictEqual(f.hair.id, HAIR_BALD, id);
		}
	});

	test("a receding hairline clears the hair behind it too", () => {
		const f = face();
		f.hairBg.id = "longHair";
		ageFace(f, 34, proneNoShave, fixed(0.001));
		assert.strictEqual(f.hair.id, HAIR_THINNING);
		assert.strictEqual(f.hairBg.id, "none");
	});
});

describe("baldness in families", () => {
	test("everyone in a family reads the same seed, whoever you ask", () => {
		const father = 40;
		const sons = [91, 12, 77];
		assert.strictEqual(
			familySeed(
				father,
				sons.map((pid) => ({ pid })),
			),
			12,
		);
		assert.strictEqual(
			familySeed(91, [{ pid: father }, { pid: 12 }, { pid: 77 }]),
			12,
		);
		assert.strictEqual(familySeed(12, [{ pid: father }]), 12);
	});

	test("a player with nobody in the league is judged on his own", () => {
		assert.isUndefined(familySeed(7, []));
		assert.isUndefined(familySeed(7, undefined));
		assert.isUndefined(familySeed(undefined, [{ pid: 1 }]));
		for (let pid = 0; pid < 200; pid++) {
			assert.strictEqual(baldingProne(pid, pid), baldingProne(pid));
		}
	});

	// Heritable, not deterministic: plenty of sons of bald men keep their hair.
	test("a family that loses its hair passes that on without settling it", () => {
		let inProne = 0;
		let inProneTotal = 0;
		let outProne = 0;
		let outProneTotal = 0;
		for (let seed = 0; seed < 60; seed++) {
			const familyProne = baldingProne(seed, seed);
			for (let pid = 1000; pid < 1060; pid++) {
				const prone = baldingProne(pid, seed);
				if (familyProne) {
					inProneTotal += 1;
					if (prone) {
						inProne += 1;
					}
				} else {
					outProneTotal += 1;
					if (prone) {
						outProne += 1;
					}
				}
			}
		}
		const inRate = inProne / inProneTotal;
		const outRate = outProne / outProneTotal;
		assert.isAbove(inRate, 0.6, `${inRate}`);
		assert.isBelow(inRate, 0.95, `${inRate}`);
		assert.isBelow(outRate, 0.3, `${outRate}`);
		// And the league-wide rate is left where it was.
		const overall = (inProne + outProne) / (inProneTotal + outProneTotal);
		assert.closeTo(overall, BALDING_PRONE_SHARE, 0.08);
	});
});

describe("filling out", () => {
	test("nothing happens to a young player, and it only goes one way", () => {
		assert.strictEqual(fatnessGainByAge(19), 0);
		assert.strictEqual(fatnessGainByAge(27), 0);
		assert.isAbove(fatnessGainByAge(38), fatnessGainByAge(30));
		let previous = 0;
		for (let age = 19; age <= 45; age++) {
			const gain = fatnessGainByAge(age);
			assert.isAtLeast(gain, previous);
			previous = gain;
		}
	});

	test("a career adds weight, but not a comical amount", () => {
		const f = face();
		f.fatness = 0.3;
		for (let age = 20; age <= 38; age++) {
			ageFace(f, age, 1, fixed(0.99));
		}
		assert.isAbove(f.fatness, 0.3);
		assert.isBelow(f.fatness, 0.55);
	});

	test("it never goes past what facesjs will draw", () => {
		const f = face();
		f.fatness = 0.99;
		for (let age = 20; age <= 60; age++) {
			ageFace(f, age, 1, fixed(0.99));
		}
		assert.isAtMost(f.fatness, 1);
	});
});

describe("relatives keep the family's colours", () => {
	// facesjs hands a son his father's skin and hair verbatim, and that IS the
	// resemblance. The per-player colour nudge used to pull them apart again by
	// a few points every generation - visible side by side on a roster page.
	test("a son's skin and hair are exactly his father's", () => {
		resetG();
		g.setWithoutSavingToDB("realisticFaces", true);
		for (let i = 0; i < 40; i++) {
			const father = generateFace({ race: "black", age: 30, pid: i });
			const son = generateFace({ relative: father, age: 20, pid: 5000 + i });
			assert.strictEqual(son.body.color, father.body.color);
			assert.strictEqual(son.hair.color, father.hair.color);
		}
	});

	test("a face drawn from scratch still gets colours of its own", () => {
		resetG();
		g.setWithoutSavingToDB("realisticFaces", true);
		const skins = new Set<string>();
		for (let i = 0; i < 60; i++) {
			skins.add(generateFace({ race: "black", age: 25, pid: i }).body.color);
		}
		// The whole point of the nudge: a league is not three exact colours.
		assert.isAbove(skins.size, 20);
	});

	test("a son is not born with his father's beard or hairline", () => {
		resetG();
		g.setWithoutSavingToDB("realisticFaces", true);
		for (let i = 0; i < 60; i++) {
			const father = generateFace({ race: "white", age: 36, pid: i });
			const son = generateFace({ relative: father, age: 19, pid: 6000 + i });
			assert.notStrictEqual(son.hair.id, HAIR_THINNING);
			assert.strictEqual(son.smileLine.id, "none");
			assert.strictEqual(son.eyeLine.id, "none");
		}
	});
});

describe("marks a face already carries", () => {
	// A face built before its player had an id - every generated player, since
	// the pid does not exist until the row is written - picks up marks from
	// nobody's style, and used to swap them for its own the first time it aged.
	test("a line style is never swapped for another", () => {
		const f = face();
		f.smileLine.id = "line4";
		f.eyeLine.id = "line2";
		applyWrinkles(f, MAX_WRINKLE_LEVEL, 7);
		assert.strictEqual(f.smileLine.id, "line4");
		assert.strictEqual(f.eyeLine.id, "line2");
	});

	test("a mark below the level is still cleared", () => {
		const f = face();
		f.smileLine.id = "line4";
		f.eyeLine.id = "line2";
		applyWrinkles(f, 0, 7);
		assert.strictEqual(f.smileLine.id, "none");
		assert.strictEqual(f.eyeLine.id, "none");
	});
});

// HOW MUCH OF THE LEAGUE ACTUALLY LOSES ITS HAIR.
//
// The per-year rates in FACE_AGE_BANDS are the only thing standing between a
// league that occasionally balds someone and a league where every veteran is
// wearing a horseshoe, and nothing else in this file would notice if they
// drifted: every other balding test forces the roll, so all of them pass at
// any rate at all. This one plays real careers at real odds.
//
// Bands are wide on purpose. The point is to catch a rate that doubled or
// went to zero, not to freeze the exact number - see the measured table on
// AgeBand.baldingPerYear for what it is now.
describe("how much of a league goes bald", () => {
	// A shaved head is NOT hair loss: facesjs draws `bald` as an ordinary style
	// at any age, and shavesHead adds more deliberately. Only a hairline that
	// receded during the replayed career counts.
	const shareWhoLostHair = (currentAge: number, n: number) => {
		const realRandom = Math.random;
		let lost = 0;
		try {
			// Seeded, so this is the same n men every run and the test cannot
			// flake on an unlucky draw.
			Math.random = mulberry32(12345);
			const rand = Math.random;
			for (let i = 0; i < n; i++) {
				const pid = i * 7 + 3;
				const f = generateFace();
				const bornBald = f.hair.id === HAIR_BALD;
				applyFaceAgingHistory({
					face: f,
					rookieAge: 20,
					currentAge,
					pid,
					rand,
				});
				if (
					f.hair.id === HAIR_THINNING ||
					(f.hair.id === HAIR_BALD && !bornBald && !shavesHead(pid))
				) {
					lost += 1;
				}
			}
		} finally {
			Math.random = realRandom;
		}
		return lost / n;
	};

	test("a 34-year-old league has a few balding men, not a room full", () => {
		const share = shareWhoLostHair(34, 3000);
		assert.isAbove(share, 0.02, `only ${(100 * share).toFixed(1)}% by 34`);
		assert.isBelow(share, 0.07, `${(100 * share).toFixed(1)}% balding by 34`);
	});

	test("and it keeps climbing to the end of a career", () => {
		const at30 = shareWhoLostHair(30, 3000);
		const at38 = shareWhoLostHair(38, 3000);
		assert.isAbove(at38, at30);
		assert.isBelow(at38, 0.1, `${(100 * at38).toFixed(1)}% balding by 38`);
	});

	test("nobody has lost a hairline before the rates start", () => {
		// The youngest band is 0 per year, so a 22-year-old who is not wearing a
		// shave by choice still has his hairline.
		assert.strictEqual(shareWhoLostHair(22, 1500), 0);
	});
});
