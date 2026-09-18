import { generate } from "facesjs";
import { assert, describe, test } from "vitest";
import {
	ageFace,
	applyRealisticFace,
	EYE_ANGLE_AGED_MIN,
	eyeDroopByAge,
	HAIR_BALD,
	HAIR_VOLUMINOUS,
	shavesHead,
	shavesHeadAtAge,
} from "./realisticFaces.ts";

// HOW AGING IS PACED, measured over replayed careers rather than argued about.
//
// A face changes in two ways. STEPS swap a feature for another one - a beard
// arrives, a hairline goes - and are what a reader actually notices. DRIFT
// moves a number a few thousandths and is invisible in any one season but is
// the whole difference across a decade.
//
// The complaint this measures is "a few big changes rather than continual
// ones", and it had two causes. Every player ran off one league-wide schedule,
// so the steps piled up on the same three ages: a third of the league changed
// at 23, 27 and 31 and almost nobody in between. And drift only touched two
// dials, neither of which moved before age 22, so early seasons were frozen
// solid. Both are asserted against here.

const roundTo3 = (v: number): number => Math.round(v * 1000) / 1000;

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

const RACES = ["white", "black", "brown", "asian"] as const;

// What a viewer would see change: the styles, and separately the dials.
const stepKey = (f: any) =>
	[
		f.facialHair.id,
		f.hair.id,
		f.eyeLine.id,
		f.smileLine.id,
		f.miscLine.id,
		f.head?.shave ?? "",
	].join("|");
const driftKey = (f: any) =>
	[f.smileLine.size, f.fatness, f.ear?.size, f.nose?.size]
		.map((n) => Number(n ?? 0).toFixed(3))
		.join("|");

const FROM = 19;
const TO = 38;

const replay = (careers: number) => {
	// Share of players who take a step at each age, and the share of seasons
	// in which nothing at all moved.
	const stepAtAge = new Map<number, number>();
	let frozen = 0;
	let steps = 0;

	for (let pid = 0; pid < careers; pid++) {
		const rand = seeded(pid * 7919 + 13);
		const race = RACES[pid % RACES.length]!;
		const face = generate(
			{ jersey: { id: "jersey" } },
			{ gender: "male", race },
		);
		applyRealisticFace(face, { age: FROM, race, pid, rand });

		for (let age = FROM + 1; age <= TO; age++) {
			const beforeStep = stepKey(face);
			const beforeDrift = driftKey(face);
			ageFace(face, age, pid, rand);
			const stepped = stepKey(face) !== beforeStep;
			const drifted = driftKey(face) !== beforeDrift;
			if (stepped) {
				steps += 1;
				stepAtAge.set(age, (stepAtAge.get(age) ?? 0) + 1);
			}
			if (!stepped && !drifted) {
				frozen += 1;
			}
		}
	}

	return {
		stepAtAge,
		frozenShare: frozen / (careers * (TO - FROM)),
		stepsPerCareer: steps / careers,
	};
};

describe("aging is paced, not staged", () => {
	test("no age is a tentpole the whole league changes on", () => {
		const { stepAtAge } = replay(600);

		// Over the years where aging is actually running, the busiest age must
		// not tower over its neighbours. The old fixed schedule put 39% of the
		// league on age 27 against 16% on 26 - a jump of 23 points - and did
		// the same at 23 and 31. Comparing each age to the one before it is
		// what catches a tentpole; a smooth ramp moves a few points a year.
		let worstJump = 0;
		let worstAge = 0;
		for (let age = 22; age <= TO; age++) {
			const here = (stepAtAge.get(age) ?? 0) / 600;
			const prev = (stepAtAge.get(age - 1) ?? 0) / 600;
			if (here - prev > worstJump) {
				worstJump = here - prev;
				worstAge = age;
			}
		}
		assert.isBelow(
			worstJump,
			0.15,
			`age ${worstAge} is a tentpole: ${(worstJump * 100).toFixed(0)} points above the year before`,
		);
	});

	test("something moves every season", () => {
		// Drift is what fills the years between steps. With it starting at the
		// draft and running on four dials, a frozen season should be vanishing.
		const { frozenShare } = replay(400);
		assert.isBelow(
			frozenShare,
			0.02,
			`${(frozenShare * 100).toFixed(1)}% of seasons changed nothing at all`,
		);
	});

	test("a face aged to an age matches one built at it, drift for drift", () => {
		// The contract the module is built on: generation and aging are two
		// paths to the same age and they have to agree. The fold DEPTH broke it
		// - the per-season creep was slower than the curve it was chasing, so an
		// aged 38-year-old carried 1.4 where a generated one carried the full 2,
		// and every veteran the league played into was smoother than the ones it
		// started with.
		//
		// Ears and nose ADD to whatever was generated rather than overwriting
		// it, so the two faces have to start from the same head or there is
		// nothing to compare. They are the drift most at risk here: the per-year
		// step is small enough that a rounding step coarser than the step size
		// silently freezes the aged path while the built one keeps accumulating.
		for (const pid of [1, 7, 23, 44, 61]) {
			const base: any = generate(
				{ jersey: { id: "jersey" } },
				{ gender: "male", race: "white" },
			);
			// A nose outside the module's band gets re-drawn from `rand`, and
			// how far into that stream the draw lands depends on which
			// age-dependent branches ran above it - so the two paths would
			// disagree on a random nose rather than on the drift, which is what
			// is under test here (the eye angle has the same re-draw). Nothing
			// replays generation at two ages in the
			// game (a face is built once and then aged), so that re-draw is a
			// different player's nose, not a contradiction. Start in band.
			base.nose.size = 0.8;
			base.eye.angle = 4;

			const aged: any = structuredClone(base);
			applyRealisticFace(aged, {
				age: FROM,
				race: "white",
				pid,
				rand: seeded(pid),
			});
			for (let age = FROM + 1; age <= TO; age++) {
				ageFace(aged, age, pid, seeded(pid + age));
			}

			const built: any = structuredClone(base);
			applyRealisticFace(built, {
				age: TO,
				race: "white",
				pid,
				rand: seeded(pid),
			});

			for (const [what, a, b] of [
				["folds", aged.smileLine.size, built.smileLine.size],
				["ears", aged.ear.size, built.ear.size],
				["nose", aged.nose.size, built.nose.size],
				["eye angle", aged.eye.angle, built.eye.angle],
			] as [string, number, number][]) {
				assert.strictEqual(
					a,
					b,
					`pid ${pid}: aged to ${TO} has ${what} ${a}, built at ${TO} has ${b}`,
				);
			}

			// And the drift has to have actually happened, or the check above
			// passes on two faces that both stood still.
			assert.ok(
				aged.ear.size > base.ear.size &&
					aged.nose.size > base.nose.size &&
					aged.eye.angle < base.eye.angle,
				`pid ${pid}: ears ${base.ear.size}->${aged.ear.size}, nose ${base.nose.size}->${aged.nose.size}, eyes ${base.eye.angle}->${aged.eye.angle} over ${TO - FROM} seasons`,
			);
		}
	});

	test("a man reaches for the clippers before the razor", () => {
		// Shaving used to take a head of dreads to a bare scalp between two
		// roster pages. The balding ladder already refuses that jump - "straight
		// from dreads to a horseshoe in a single preseason is the jump that
		// reads as a glitch" - and it applies just as much to a man who decides
		// to shave, so he goes through something short first.
		const volume = new Set<string>(HAIR_VOLUMINOUS);
		let checked = 0;
		for (let pid = 0; pid < 400; pid++) {
			if (!shavesHead(pid)) {
				continue;
			}
			const face: any = generate(
				{ jersey: { id: "jersey" } },
				{ gender: "male", race: "black" },
			);
			// Start him with volume, at an age before he would shave.
			face.hair.id = HAIR_VOLUMINOUS[pid % HAIR_VOLUMINOUS.length]!;
			const start = Math.min(FROM, shavesHeadAtAge(pid) - 1);
			let previous = face.hair.id;
			for (let age = start + 1; age <= TO; age++) {
				ageFace(face, age, pid, seeded(pid + age));
				if (face.hair.id === HAIR_BALD && volume.has(previous)) {
					assert.fail(
						`pid ${pid} went from ${previous} straight to a shaved head at ${age}`,
					);
				}
				previous = face.hair.id;
			}
			checked += 1;
			if (checked >= 40) {
				break;
			}
		}
		assert.isAbove(checked, 0, "no head-shavers found to check");
	});

	test("the eyes keep coming down when the steps have stopped", () => {
		// The late thirties were the thinnest part of a career: the wrinkle
		// ceiling has been reached and the hairline has resolved, so the STEP
		// rate peaks around 31 and tapers from there - a man changed less at 40
		// than he did at 31, which is backwards. The droop does not care that
		// the steps have stopped, so it has to still be moving out there.
		for (const pid of [2, 13, 29, 51, 77]) {
			const face: any = generate(
				{ jersey: { id: "jersey" } },
				{ gender: "male", race: "white" },
			);
			face.eye.angle = 4;
			applyRealisticFace(face, {
				age: 19,
				race: "white",
				pid,
				rand: seeded(pid),
			});

			let previous = face.eye.angle;
			let movedAfter35 = 0;
			for (let age = 20; age <= 42; age++) {
				ageFace(face, age, pid, seeded(pid + age));
				// One way only, like everything else here: no roll can tilt an
				// eye back up, so a face can never un-age.
				assert.isAtMost(
					face.eye.angle,
					previous,
					`pid ${pid}: eye went back up at ${age}`,
				);
				if (age > 35 && face.eye.angle < previous) {
					movedAfter35 += 1;
				}
				previous = face.eye.angle;
			}
			assert.isAbove(
				movedAfter35,
				0,
				`pid ${pid}: nothing moved in the eyes after 35`,
			);
			// And never past the floor, however long the career runs.
			assert.isAtLeast(face.eye.angle, EYE_ANGLE_AGED_MIN);
		}
	});

	test("some men hold their eyes up longer than others", () => {
		// Nobody ages on one schedule. weathersLess already decides whose lines
		// come in slowly; the same men keep their eyes level longer, so a league
		// does not droop in lockstep.
		const droops = new Set<number>();
		for (let pid = 1; pid <= 60; pid++) {
			droops.add(roundTo3(eyeDroopByAge(38, pid)));
		}
		assert.isAbove(droops.size, 1, "every player drooped at the same rate");
		// Both rates are real drift, not one of them being nothing.
		for (const d of droops) {
			assert.isAbove(d, 0.5, "a rate too small to see across a career");
		}
	});

	test("spreading the steps did not remove them", () => {
		// The point is to pace the same career, not to age players less. Before
		// the change this was 4.0 steps per career.
		const { stepsPerCareer } = replay(400);
		assert.isAbove(stepsPerCareer, 3, `only ${stepsPerCareer} steps/career`);
		assert.isBelow(stepsPerCareer, 5.5, `${stepsPerCareer} steps/career`);
	});
});
