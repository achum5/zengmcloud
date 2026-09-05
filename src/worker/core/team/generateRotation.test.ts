import { assert, describe, test } from "vitest";
import {
	generateRotation,
	rotationTargets,
	type RotationCandidate,
} from "./generateRotation.ts";
import {
	plannedMinutes,
	playersPerMinute,
	rotationToGrid,
} from "../../../common/rotation.ts";

const roster = (n = 13): RotationCandidate[] =>
	Array.from({ length: n }, (_, i) => ({
		pid: i + 1,
		value: 100 - i * 4,
		ptModifier: 1,
		injured: false,
	}));

const settings = { numPeriods: 4, periodLength: 12, numPlayersOnCourt: 5 };

describe("rotationTargets", () => {
	test("targets fill the floor's minutes exactly", () => {
		const targets = rotationTargets(roster(), {
			gameMinutes: 48,
			numPlayersOnCourt: 5,
		});
		const sum = [...targets.values()].reduce((s, x) => s + x, 0);
		assert.strictEqual(sum, 240);
	});

	test("minutes follow ability", () => {
		const targets = rotationTargets(roster(), {
			gameMinutes: 48,
			numPlayersOnCourt: 5,
		});
		const list = [...targets.entries()].sort((a, b) => a[0] - b[0]);
		for (let i = 1; i < list.length; i++) {
			assert.isAtMost(list[i]![1], list[i - 1]![1]);
		}
	});

	test("nobody plays the whole game by default", () => {
		const targets = rotationTargets(roster(6), {
			gameMinutes: 48,
			numPlayersOnCourt: 5,
		});
		for (const minutes of targets.values()) {
			assert.isAtMost(minutes, 44);
		}
	});

	// The playing-time settings a team already has are honored, as far as the
	// floor's minutes allow.
	test("playing time settings tilt the split", () => {
		const players = roster();
		players[5]!.ptModifier = 1.5;
		players[0]!.ptModifier = 0.75;
		const targets = rotationTargets(players, {
			gameMinutes: 48,
			numPlayersOnCourt: 5,
		});
		const plain = rotationTargets(roster(), {
			gameMinutes: 48,
			numPlayersOnCourt: 5,
		});
		assert.isAbove(targets.get(6)!, plain.get(6)!);
		assert.isBelow(targets.get(1)!, plain.get(1)!);
	});

	test("the injured and the benched get nothing", () => {
		const players = roster();
		players[2]!.injured = true;
		players[3]!.ptModifier = 0;
		const targets = rotationTargets(players, {
			gameMinutes: 48,
			numPlayersOnCourt: 5,
		});
		assert.isFalse(targets.has(3));
		assert.isFalse(targets.has(4));
	});

	test("a different game shape", () => {
		const targets = rotationTargets(roster(), {
			gameMinutes: 40,
			numPlayersOnCourt: 5,
		});
		const sum = [...targets.values()].reduce((s, x) => s + x, 0);
		assert.strictEqual(sum, 200);
	});
});

describe("generateRotation", () => {
	test("five on the floor every minute", () => {
		const stints = generateRotation(roster(), settings);
		const grid = rotationToGrid(
			stints,
			roster().map((p) => p.pid),
			4,
			12,
		);
		for (const period of playersPerMinute(grid, 4, 12)) {
			for (const count of period) {
				assert.strictEqual(count, 5);
			}
		}
	});

	test("minutes land near their targets", () => {
		const players = roster();
		const stints = generateRotation(players, settings);
		const targets = rotationTargets(players, {
			gameMinutes: 48,
			numPlayersOnCourt: 5,
		});
		for (const [pid, target] of targets) {
			assert.closeTo(plannedMinutes(stints, pid, 12), target, 3, `pid ${pid}`);
		}
	});

	test("the starters open both halves", () => {
		const stints = generateRotation(roster(), settings);
		for (const period of [0, 2]) {
			const opening = stints
				.filter((s) => s.period === period && s.start === 0)
				.map((s) => s.pid)
				.sort((a, b) => a - b);
			assert.deepStrictEqual(opening, [1, 2, 3, 4, 5]);
		}
	});

	// A plan that changes five men every minute is noise, not a rotation.
	test("stints are not choppy", () => {
		const stints = generateRotation(roster(), settings);
		assert.isBelow(stints.length, 60);
		for (const stint of stints) {
			// Anything shorter than two minutes is a period boundary, not a
			// real change.
			const minutes = (stint.end - stint.start) * 12;
			assert.isTrue(
				minutes >= 2 || stint.end === 1 || stint.start === 0,
				JSON.stringify(stint),
			);
		}
	});

	test("an injured player never appears", () => {
		const players = roster();
		players[1]!.injured = true;
		const stints = generateRotation(players, settings);
		assert.isFalse(stints.some((s) => s.pid === 2));
	});

	test("a short roster still fills the floor", () => {
		const stints = generateRotation(roster(6), settings);
		const grid = rotationToGrid(
			stints,
			roster(6).map((p) => p.pid),
			4,
			12,
		);
		for (const period of playersPerMinute(grid, 4, 12)) {
			for (const count of period) {
				assert.strictEqual(count, 5);
			}
		}
	});

	test("nobody available, nothing planned", () => {
		assert.deepStrictEqual(generateRotation([], settings), []);
	});

	test("the same roster always gets the same plan", () => {
		assert.deepStrictEqual(
			generateRotation(roster(), settings),
			generateRotation(roster(), settings),
		);
	});
});
