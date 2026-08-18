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
	FACE_AGE_THRESHOLDS,
	FACIAL_HAIR_TIERS,
	facialHairForAge,
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

	test("brown spans the widest real range and keeps everything", () => {
		for (const id of [...HAIR_TEXTURES.straight, ...HAIR_TEXTURES.coiled]) {
			assert.isTrue(hairAllowedForRace(id, "brown"), id);
		}
	});

	test("no race known means nothing to rule out", () => {
		// Generated relatives inherit a face rather than a race.
		assert.isTrue(hairAllowedForRace("middle-part", undefined));
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
	test("does nothing except at a threshold age", () => {
		for (const age of [20, 24, 28, 35]) {
			assert.isFalse(FACE_AGE_THRESHOLDS.includes(age));
			const f = face();
			assert.isFalse(ageFace(f, age, fixed(0)));
			assert.strictEqual(f.facialHair.id, "none");
		}
	});

	test("facial hair grows in at a threshold", () => {
		const f = face();
		const changed = ageFace(f, FACE_AGE_THRESHOLDS[0]!, fixed(0));
		assert.isTrue(changed);
		assert.notStrictEqual(f.facialHair.id, "none");
	});

	test("facial hair never thins back out", () => {
		// The whole reason aging is monotonic: a re-roll every year would have
		// players growing and shaving a beard at random for a decade.
		for (const threshold of FACE_AGE_THRESHOLDS) {
			for (const start of ["goatee1", "fullgoatee", "beard1"]) {
				const f = face({ facialHair: { id: start } });
				ageFace(f, threshold, sequence([0, 0.5, 0.5, 0.99]));
				const before = tierOf(start)!;
				const after = tierOf(f.facialHair.id);
				assert.isNotNull(after ?? null);
				const order = ["light", "medium", "heavy", "period"];
				assert.isAtLeast(
					order.indexOf(after!),
					order.indexOf(before),
					`${start} regressed to ${f.facialHair.id} at ${threshold}`,
				);
			}
		}
	});

	test("a hairline recedes one step at a time and never regrows", () => {
		const f = face();
		ageFace(f, FACE_AGE_THRESHOLDS[1]!, fixed(0.001));
		assert.strictEqual(f.hair.id, HAIR_THINNING);
		ageFace(f, FACE_AGE_THRESHOLDS[2]!, fixed(0.001));
		assert.strictEqual(f.hair.id, HAIR_BALD);
		// Already gone: it stays gone, however the roll lands.
		ageFace(f, FACE_AGE_THRESHOLDS[2]!, fixed(0.001));
		assert.strictEqual(f.hair.id, HAIR_BALD);
	});

	test("reports no change when there is nothing left to mature", () => {
		// Bald already, and facial hair at the top group with nothing above it -
		// so the caller can skip writing this player back to the database.
		const f = face({
			facialHair: { id: "mutton" },
			hair: { id: HAIR_BALD, color: "#272421", flip: false },
		});
		assert.isFalse(ageFace(f, FACE_AGE_THRESHOLDS[2]!, fixed(0.001)));
	});
});

describe("generateFace plumbing", () => {
	// g.get throws on an attribute it doesn't know about, so a missing entry
	// anywhere in the chain would crash every player generation rather than
	// fail quietly.
	test("defaults to off, so existing leagues are unchanged", () => {
		resetG();
		assert.strictEqual(g.get("realisticFaces"), false);
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
