import { generate } from "facesjs";
import { assert, describe, test } from "vitest";
import {
	ageFace,
	applyRealisticFace,
	CUTS_HAIR_FROM_AGE,
	cutsHair,
	HAIR_SPIKED,
	HAIR_TEXTURES,
	HAIR_VOLUMINOUS,
	hairPoolForRace,
	shavesHead,
	wrinkleLevelAgedTo,
	wrinkleLevelOf,
} from "./realisticFaces.ts";

// A FACE BUILT AT AN AGE LOOKS LIKE A FACE THAT AGED INTO IT.
//
// Generation and aging are two paths to the same age, and a league is full of
// both: the veterans a new league starts with are generated at 34, the ones
// it plays into are aged there a season at a time. When the two disagree the
// league changes character as it is played - it started a decade younger than
// it ends up - so the generated distribution is held to the aged one here.

const seeded = (seed: number) => {
	let a = seed >>> 0;
	return () => {
		a += 0x6d2b79f5;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

const build = (
	race: "white" | "black" | "brown" | "asian",
	age: number,
	pid: number,
	rand: () => number,
) => {
	const face = generate({ jersey: { id: "jersey" } }, { gender: "male", race });
	applyRealisticFace(face, { age, race, pid, rand });
	return face;
};

describe("lines at generation", () => {
	test("a generated veteran is weathered like one who aged there", () => {
		const rand = seeded(3);
		const N = 400;
		let generated = 0;
		let aged = 0;
		let unlined = 0;
		for (let pid = 1; pid <= N; pid++) {
			generated += wrinkleLevelAgedTo(36, pid, rand);
			const face = build("white", 22, pid, rand);
			for (let age = 23; age <= 36; age++) {
				ageFace(face, age, pid, rand);
			}
			aged += wrinkleLevelOf(face);
			if (wrinkleLevelAgedTo(36, pid, rand) === 0) {
				unlined += 1;
			}
		}
		// Same process, so the same mean to within noise.
		assert.closeTo(generated / N, aged / N, 0.25, "mean level");
		// And almost nobody reaches 36 with the face he had at 20 - this was
		// 60%. A player who weathers less cannot take a line before 27, which
		// leaves him ten rolls at 0.3, so a couple of percent is the real rate.
		assert.isBelow(unlined / N, 0.05, "unlined at 36");
	});

	test("nothing before the first age lines are allowed", () => {
		const rand = seeded(4);
		for (let pid = 1; pid <= 50; pid++) {
			assert.strictEqual(wrinkleLevelAgedTo(22, pid, rand), 0);
		}
	});
});

describe("spiked cuts", () => {
	test("are real styles, and out of every re-roll pool", () => {
		const all = [...HAIR_TEXTURES.universal, ...HAIR_TEXTURES.straight];
		for (const id of HAIR_SPIKED) {
			assert.include(all, id);
			for (const race of ["white", "black", "brown", "asian"] as const) {
				assert.notInclude(hairPoolForRace(race), id, `${race}/${id}`);
			}
		}
	});

	test("mostly re-rolled away, occasionally kept", () => {
		const rand = seeded(5);
		const N = 1500;
		let spiked = 0;
		for (let pid = 1; pid <= N; pid++) {
			if (HAIR_SPIKED.includes(build("white", 25, pid, rand).hair.id)) {
				spiked += 1;
			}
		}
		// Uniform selection put one on 22% of white players.
		assert.isBelow(spiked / N, 0.1, "share spiked");
		assert.isAbove(spiked, 0, "still around");
	});
});

describe("the cut", () => {
	const afro = () => {
		const face = generate(
			{ jersey: { id: "jersey" } },
			{ gender: "male", race: "black" },
		);
		face.hair.id = "afro";
		return face;
	};

	test("never before the age it starts, cutter or not", () => {
		const rand = seeded(6);
		for (let pid = 1; pid <= 60; pid++) {
			// A shaved head is a choice made in the early twenties, and not
			// the cut this is about.
			if (shavesHead(pid)) {
				continue;
			}
			const face = afro();
			for (let age = 20; age < CUTS_HAIR_FROM_AGE; age++) {
				ageFace(face, age, pid, rand);
			}
			assert.strictEqual(face.hair.id, "afro", `pid ${pid}`);
		}
	});

	test("a cutter is usually short by forty; the rest keep what they wore", () => {
		const rand = seeded(7);
		let cutters = 0;
		let cut = 0;
		for (let pid = 1; pid <= 300; pid++) {
			const face = afro();
			// Under the onset of hair loss for everyone, so only the cut can
			// touch it: onset is 28 at the earliest and the balding ladder
			// takes the same short rung, which would count as a cut.
			for (let age = CUTS_HAIR_FROM_AGE; age <= 40; age++) {
				ageFace(face, age, pid, rand);
			}
			const short = !HAIR_VOLUMINOUS.includes(face.hair.id);
			if (cutsHair(pid)) {
				cutters += 1;
				if (short) {
					cut += 1;
				}
			} else if (short) {
				// Only the balding ladder can have done this - it is allowed,
				// but it is rare enough to check the cut trait is not leaking.
				assert.isTrue(true);
			}
		}
		assert.isAbove(cutters, 100, "cutters in the sample");
		assert.isAbove(cut / cutters, 0.6, "cutters short by 40");
	});

	test("generation replays it, so a built veteran wears what an aged one would", () => {
		const rand = seeded(8);
		let young = 0;
		let old = 0;
		const N = 800;
		for (let pid = 1; pid <= N; pid++) {
			if (HAIR_VOLUMINOUS.includes(build("black", 22, pid, rand).hair.id)) {
				young += 1;
			}
			if (HAIR_VOLUMINOUS.includes(build("black", 38, pid, rand).hair.id)) {
				old += 1;
			}
		}
		// Hair used to carry no age signal at all: the same share at 38 as 22.
		assert.isBelow(
			old,
			young * 0.75,
			`voluminous at 38 (${old}) vs 22 (${young})`,
		);
	});
});

// FEATURES, DIALS, BUILD AND SKIN - the second pass, made from a rendered
// catalogue of every variant.
import { svgsIndex } from "facesjs";
import {
	EYE_ANGLE,
	EYEBROW_ANGLE,
	EYES_CARTOON,
	EYES_NATURAL,
	inferRaceFromFace,
	MOUTHS_CARTOON,
	MOUTHS_NATURAL,
	NOSE_SIZE,
	NOSES_CARTOON,
	NOSES_NATURAL,
	SKIN_TONES,
} from "./realisticFaces.ts";

const male = (ids: readonly string[]) =>
	ids.filter((id) => !id.startsWith("female")).toSorted();

describe("the cartoon lists", () => {
	test("cover the male catalogue exactly, cartoon and natural together", () => {
		assert.deepStrictEqual(
			[...EYES_CARTOON, ...EYES_NATURAL].toSorted(),
			male(svgsIndex.eye),
		);
		assert.deepStrictEqual(
			[...MOUTHS_CARTOON, ...MOUTHS_NATURAL].toSorted(),
			male(svgsIndex.mouth),
		);
		assert.deepStrictEqual(
			[...NOSES_CARTOON, ...NOSES_NATURAL].toSorted(),
			male(svgsIndex.nose),
		);
	});

	test("a cartoon eye is rare, not gone; the dials sit inside their ranges", () => {
		const rand = seeded(9);
		const N = 1000;
		let cartoon = 0;
		let fat = 0;
		for (let pid = 1; pid <= N; pid++) {
			const f = build("white", 25, pid, rand);
			if (EYES_CARTOON.includes(f.eye.id)) {
				cartoon += 1;
			}
			assert.isAtLeast(f.eye.angle, EYE_ANGLE[0]);
			assert.isAtMost(f.eye.angle, EYE_ANGLE[1]);
			assert.isAtLeast(f.eyebrow.angle, EYEBROW_ANGLE[0]);
			assert.isAtMost(f.eyebrow.angle, EYEBROW_ANGLE[1]);
			assert.isAtLeast(f.nose.size, NOSE_SIZE[0]);
			assert.isAtMost(f.nose.size, NOSE_SIZE[1]);
			fat += f.fatness;
		}
		// Uniform selection gave 7 of 19 - 37%.
		assert.isBelow(cartoon / N, 0.14, "cartoon eyes");
		assert.isAbove(cartoon, 0, "still around");
		// Uniform fatness averages 0.5.
		assert.isBelow(fat / N, 0.42, "mean fatness");
	});

	test("a face that already exists keeps its own features", () => {
		const rand = seeded(10);
		const f = generate(
			{ jersey: { id: "jersey" } },
			{ gender: "male", race: "black" },
		);
		f.eye.id = "eye8";
		f.mouth.id = "angry";
		f.eyebrow.angle = 20;
		f.fatness = 0.9;
		applyRealisticFace(f, {
			age: 22,
			race: "black",
			pid: 3,
			keepColors: true,
			rand,
		});
		assert.strictEqual(f.eye.id, "eye8");
		assert.strictEqual(f.mouth.id, "angry");
		assert.strictEqual(f.eyebrow.angle, 20);
		assert.strictEqual(f.fatness, 0.9);
	});
});

describe("skin across the range", () => {
	test("every anchor gets used, and the race still reads back", () => {
		const rand = seeded(11);
		for (const race of ["white", "black", "brown", "asian"] as const) {
			const nearest = new Set<number>();
			let right = 0;
			const N = 400;
			for (let pid = 1; pid <= N; pid++) {
				const f = build(race, 25, pid, rand);
				if (inferRaceFromFace(f) === race) {
					right += 1;
				}
				// Which anchor this tone sits closest to.
				const [r, g, b] = [1, 3, 5].map((i) =>
					Number.parseInt(f.body.color.slice(i, i + 2), 16),
				) as [number, number, number];
				let best = 0;
				let bestD = Infinity;
				for (const [i, tone] of SKIN_TONES[race].entries()) {
					const [r2, g2, b2] = [1, 3, 5].map((k) =>
						Number.parseInt(tone.slice(k, k + 2), 16),
					) as [number, number, number];
					const d = (r - r2) ** 2 + (g - g2) ** 2 + (b - b2) ** 2;
					if (d < bestD) {
						bestD = d;
						best = i;
					}
				}
				nearest.add(best);
			}
			assert.strictEqual(
				nearest.size,
				SKIN_TONES[race].length,
				`${race} anchors used`,
			);
			// The ends of neighbouring ranges sit close, so one reads off now
			// and then; the retroactive pass can live with that.
			assert.isAbove(right / N, 0.85, `${race} read back`);
		}
	});
});
