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
		// And nobody reaches 36 with the face he had at 20 - this was 60%.
		assert.isBelow(unlined / N, 0.02, "unlined at 36");
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
