import { generate } from "facesjs";
import { assert, describe, test } from "vitest";
import { ageFace, applyRealisticFace } from "./realisticFaces.ts";

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

	test("spreading the steps did not remove them", () => {
		// The point is to pace the same career, not to age players less. Before
		// the change this was 4.0 steps per career.
		const { stepsPerCareer } = replay(400);
		assert.isAbove(stepsPerCareer, 3, `only ${stepsPerCareer} steps/career`);
		assert.isBelow(stepsPerCareer, 5.5, `${stepsPerCareer} steps/career`);
	});
});
