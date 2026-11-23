import { assert, describe, test } from "vitest";
import { lenMost, poseNow, shortSteps } from "../poses.ts";
import { inchesOf, stepsPerSecond } from "./gait.ts";
import { PACKAGES, packagesFor, type MoveType } from "./packages.ts";
import { signatureOf } from "./signature.ts";

const FT = 3.28084;

describe("3D possession sim: how quickly his steps go round", () => {
	test("a man the size of the runners measured steps as they did", () => {
		// (Dorn, Schache & Pandy 2012, table 2: steps a second at each pace.)
		for (const [mps, steps] of [
			[3.49, 2.62],
			[5.17, 2.94],
			[6.96, 3.5],
			[8.99, 4.36],
		] as const) {
			assert.closeTo(stepsPerSecond("run", mps * FT, 69.3), steps, 0.02);
		}
	});

	test("faster, each step is longer - and quicker only nearer flat out", () => {
		let lastLen = 0;
		let lastRate = 0;
		for (let v = 8; v <= 22; v += 1) {
			const rate = stepsPerSecond("run", v, 79);
			const len = v / rate;
			assert.isAbove(len, lastLen);
			assert.isAtLeast(rate, lastRate);
			lastLen = len;
			lastRate = rate;
		}
		// (Going 18 ft/s, a 6-foot-7 man's steps are each about six and a
		// half feet - not four and a quarter, patting along at four a second.)
		assert.closeTo(18 / stepsPerSecond("run", 18, 79), 6.5, 0.3);
	});

	test("a taller man's steps go round more slowly at the same pace", () => {
		for (const v of [4, 10, 16, 20]) {
			const how = v < 6 ? "walk" : "run";
			assert.isBelow(
				stepsPerSecond(how, v, 84),
				stepsPerSecond(how, v, 74),
				`${v} ft/s`,
			);
		}
	});

	test("walking, a step a second and a half to two", () => {
		assert.closeTo(stepsPerSecond("walk", 4.4, 69.3), 1.86, 0.05);
		assert.isBelow(
			stepsPerSecond("walk", 2, 79),
			stepsPerSecond("walk", 5, 79),
		);
	});

	test("a run's stride as long as a sprint's is a sprint's - and a jog's as long as a run's, a run's", () => {
		for (const [anim, up] of [
			["run", "sprint"],
			["jog", "run"],
		] as const) {
			const m = lenMost(anim);
			for (let ph = 0; ph < 1; ph += 0.05) {
				const q = shortSteps(poseNow(anim, ph), anim, [m, m], ph);
				const want = poseNow(up, ph);
				for (const k of [
					"hipN",
					"kneeN",
					"hipF",
					"kneeF",
					"lean",
					"shN",
					"elF",
				] as const) {
					assert.closeTo(
						q[k],
						want[k],
						0.5,
						`${anim} ${k} at ${ph.toFixed(2)}`,
					);
				}
			}
			// (And no further: longer, he is on the next gait up.)
			assert.closeTo(m, up === "sprint" ? 12.4 / 8.6 : 8.6 / 6.2, 1e-9);
		}
	});

	test("the height rating is the league's own height", () => {
		assert.strictEqual(inchesOf(0), 66);
		assert.strictEqual(inchesOf(100), 93);
	});
});

describe("3D possession sim: each man's own packages", () => {
	test("every package has an id of its own, and every kind more than one", () => {
		assert.strictEqual(
			new Set(PACKAGES.map((p) => p.id)).size,
			PACKAGES.length,
		);
		for (const type of ["walk", "run", "stance", "backpedal"] as MoveType[]) {
			assert.isAbove(packagesFor(type).length, 1, type);
			for (const p of packagesFor(type)) {
				assert.isTrue(p.id.startsWith(`${type}.`), p.id);
			}
		}
	});

	test("the same man gets the same signature every time", () => {
		const r = { hgt: 60, spd: 55, jmp: 50, stre: 60, diq: 50, oiq: 50 };
		assert.deepEqual(signatureOf(1234, r), signatureOf(1234, r));
	});

	test("a saved signature is his, where it names a package of that kind", () => {
		const sig = signatureOf(
			7,
			{ spd: 70 },
			{
				run: "run.heavy",
				walk: "run.drive",
			},
		);
		assert.strictEqual(sig.run, "run.heavy");
		assert.notStrictEqual(sig.walk, "run.drive");
	});

	test("his ratings lean him: slow bigs run heavy, quick small guards quick", () => {
		const share = (r: Record<string, number>, id: string) => {
			let n = 0;
			for (let pid = 0; pid < 2000; pid++) {
				n += signatureOf(pid, r).run === id ? 1 : 0;
			}
			return n / 2000;
		};
		const big = { hgt: 85, spd: 30, stre: 70, jmp: 35 };
		const guard = { hgt: 15, spd: 80, stre: 40, jmp: 70 };
		assert.isAbove(share(big, "run.heavy"), 0.3);
		assert.isBelow(share(guard, "run.heavy"), 0.08);
		assert.isAbove(share(guard, "run.quick"), 0.25);
		// (Too slow for it, never.)
		assert.strictEqual(share(big, "run.drive"), 0);
		assert.strictEqual(share(big, "run.quick"), 0);
	});
});
