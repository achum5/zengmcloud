// HOW MUCH A CAREER MOVES, AGE BY AGE. Not an assertion - a measurement, for
// deciding where aging is thin before changing anything. The cadence test
// asserts the conclusions this is used to reach.
//
// Prints one row per age: STEP is the share of players who had a feature
// swapped that season, DRIFT ONLY the share who had only numbers move, FROZEN
// the share for whom the season changed nothing at all.
//
// Skipped unless AGING_MEASURE is set, like the other harnesses, so CI pays
// nothing.
//
//   SPORT=basketball AGING_MEASURE=/tmp/aging-cadence.txt \
//     npx vitest run --project basketball src/worker/util/agingMeasure.test.ts

import { generate } from "facesjs";
import { describe, test } from "vitest";
import { ageFace, applyRealisticFace } from "./realisticFaces.ts";

const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const OUT = nodeEnv.AGING_MEASURE;

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

const RACES = ["black", "white", "brown", "asian"] as const;

const FROM = 19;
const TO = 42;

describe.runIf(OUT)("aging cadence", () => {
	test("what moves, age by age", async () => {
		const N = Number(nodeEnv.AGING_MEASURE_N ?? 1200);
		const step: number[] = [];
		const drift: number[] = [];
		const total: number[] = [];
		for (let age = FROM + 1; age <= TO; age++) {
			step[age] = 0;
			drift[age] = 0;
			total[age] = 0;
		}

		for (let i = 0; i < N; i++) {
			const pid = i * 7 + 3;
			const race = RACES[pid % RACES.length]!;
			const face: any = generate(
				{ jersey: { id: "jersey" } },
				{ gender: "male", race },
			);
			applyRealisticFace(face, { age: FROM, race, pid, rand: seeded(pid) });
			for (let age = FROM + 1; age <= TO; age++) {
				const before = JSON.stringify(face);
				// ageFace reports STEPS. Drift is deliberately not a "change" as
				// far as it is concerned - see the note on recording in
				// realisticFaces - so it has to be read off the face itself.
				const changed = ageFace(face, age, pid, seeded(pid * 31 + age));
				total[age]!++;
				if (changed) {
					step[age]!++;
				} else if (JSON.stringify(face) !== before) {
					drift[age]!++;
				}
			}
		}

		const pct = (n: number, d: number) =>
			`${((n / d) * 100).toFixed(1).padStart(5)}%`;
		const rows: string[] = [];
		for (let age = FROM + 1; age <= TO; age++) {
			const t = total[age]!;
			rows.push(
				`age ${String(age).padStart(2)}  step ${pct(step[age]!, t)}  driftOnly ${pct(drift[age]!, t)}  frozen ${pct(t - step[age]! - drift[age]!, t)}`,
			);
		}

		const fs = await import(("node" + ":fs") as any);
		fs.writeFileSync(OUT!, `${rows.join("\n")}\n`);
	});
});
