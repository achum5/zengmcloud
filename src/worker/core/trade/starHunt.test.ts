import { assert, describe, test } from "vitest";
import {
	ballastNeeded,
	HUNT_CHANCE,
	HUNT_CHANCE_STAR_GAP,
	incomingCeiling,
	MAX_BALLAST,
	pickBallast,
} from "./starHunt.ts";
import { inStrikingDistance, STRIKING_CONTENTION } from "./tradePosture.ts";

describe("inStrikingDistance", () => {
	test("an all-in team is always there", () => {
		assert.isTrue(
			inStrikingDistance({ tier: "allIn", elite: false, contention: 0.5 }),
		);
	});

	test("a buyer is there with a top roster or a top record, not otherwise", () => {
		assert.isTrue(
			inStrikingDistance({ tier: "buyer", elite: true, contention: 0.5 }),
		);
		assert.isTrue(
			inStrikingDistance({
				tier: "buyer",
				elite: false,
				contention: STRIKING_CONTENTION,
			}),
		);
		assert.isFalse(
			inStrikingDistance({ tier: "buyer", elite: false, contention: 0.5 }),
		);
	});

	test("nobody who is not contending is, however good the roster reads", () => {
		for (const tier of ["fringe", "seller", "teardown"] as const) {
			assert.isFalse(
				inStrikingDistance({ tier, elite: true, contention: 0.9 }),
				tier,
			);
		}
	});

	test("a team with no star hunts more often than one with a star", () => {
		assert.isAbove(HUNT_CHANCE_STAR_GAP, HUNT_CHANCE);
		assert.isAbove(HUNT_CHANCE, 0);
		assert.isAtMost(HUNT_CHANCE_STAR_GAP, 1);
	});
});

describe("ballastNeeded", () => {
	const base = {
		payroll: 90_000,
		salaryCap: 100_000,
		softCapTradeSalaryMatch: 125,
	};

	test("nothing is owed when the star fits under the cap", () => {
		assert.strictEqual(
			ballastNeeded({ ...base, incoming: 8_000, salaryCapType: "soft" }),
			0,
		);
	});

	test("nothing is owed when there is no cap", () => {
		assert.strictEqual(
			ballastNeeded({ ...base, incoming: 50_000, salaryCapType: "none" }),
			0,
		);
	});

	test("under a soft cap, the cheaper of getting under and matching", () => {
		// 30M incoming on a 90M payroll: 20M gets under the cap, 24M matches.
		assert.strictEqual(
			ballastNeeded({ ...base, incoming: 30_000, salaryCapType: "soft" }),
			20_000,
		);
		// 60M incoming: 50M to get under, 48M to match - match wins.
		assert.strictEqual(
			ballastNeeded({ ...base, incoming: 60_000, salaryCapType: "soft" }),
			48_000,
		);
	});

	test("under a hard cap, only getting under the cap will do", () => {
		assert.strictEqual(
			ballastNeeded({ ...base, incoming: 60_000, salaryCapType: "hard" }),
			50_000,
		);
	});

	test("what is owed never falls as the star's salary rises", () => {
		for (const salaryCapType of ["soft", "hard"]) {
			let prev = -1;
			for (let incoming = 0; incoming <= 80_000; incoming += 5_000) {
				const now = ballastNeeded({ ...base, incoming, salaryCapType });
				assert.isAtLeast(now, prev, `${salaryCapType} ${incoming}`);
				assert.isAtMost(now, incoming);
				prev = now;
			}
		}
	});

	test("survives anything a strange league can hand it", () => {
		for (const incoming of [Number.NaN, -1, 0, Infinity]) {
			for (const salaryCapType of ["soft", "hard", "none"]) {
				const v = ballastNeeded({ ...base, incoming, salaryCapType });
				assert.isTrue(Number.isFinite(v) || v === Infinity);
				assert.isAtLeast(v, 0);
			}
		}
		assert.strictEqual(
			ballastNeeded({
				...base,
				incoming: 60_000,
				salaryCapType: "soft",
				softCapTradeSalaryMatch: 0,
			}),
			50_000,
		);
	});
});

describe("incomingCeiling", () => {
	const base = {
		outgoing: 30_000,
		salaryCap: 100_000,
		softCapTradeSalaryMatch: 125,
	};

	test("no cap, no ceiling", () => {
		assert.strictEqual(
			incomingCeiling({ ...base, payroll: 150_000, salaryCapType: "none" }),
			Infinity,
		);
	});

	test("a seller with room can take back up to the cap", () => {
		// 80M payroll sending 30M: 50M back keeps it at the cap.
		assert.strictEqual(
			incomingCeiling({ ...base, payroll: 80_000, salaryCapType: "soft" }),
			50_000,
		);
	});

	test("a seller over the cap can take back the match ratio at least", () => {
		assert.strictEqual(
			incomingCeiling({ ...base, payroll: 120_000, salaryCapType: "soft" }),
			37_500,
		);
		// Under a hard cap only what fits under the line.
		assert.strictEqual(
			incomingCeiling({ ...base, payroll: 120_000, salaryCapType: "hard" }),
			10_000,
		);
	});
});

describe("pickBallast", () => {
	const roster = [
		{ pid: 1, value: 70, amount: 30_000 }, // the star: great value per dollar
		{ pid: 2, value: 40, amount: 20_000 }, // the albatross
		{ pid: 3, value: 55, amount: 10_000 },
		{ pid: 4, value: 45, amount: 5_000 },
		{ pid: 5, value: 48, amount: 1_000 },
	];

	test("nothing owed, nothing sent", () => {
		assert.deepStrictEqual(pickBallast(roster, 0), []);
	});

	test("the worst contract per dollar goes first", () => {
		assert.deepStrictEqual(pickBallast(roster, 15_000), [2]);
	});

	test("an expiring contract goes before a longer one, whatever it is worth", () => {
		const withYears = roster.map((p) => ({
			...p,
			yearsLeft: p.pid === 3 ? 0 : 2,
		}));
		assert.deepStrictEqual(pickBallast(withYears, 8_000), [3]);
	});

	test("never sends more than the other side may take back", () => {
		// 20M alone would cover 15M but breaks a 18M ceiling; 10M + 5M fits.
		const out = pickBallast(roster, 15_000, MAX_BALLAST, 18_000)!;
		assert.isDefined(out);
		const sent = out.reduce(
			(a, pid) => a + roster.find((p) => p.pid === pid)!.amount,
			0,
		);
		assert.isAtLeast(sent, 15_000);
		assert.isAtMost(sent, 18_000);
		assert.isUndefined(pickBallast(roster, 15_000, MAX_BALLAST, 14_000));
	});

	test("keeps adding until the money is covered", () => {
		const out = pickBallast(roster, 35_000)!;
		assert.isDefined(out);
		let sent = 0;
		for (const pid of out) {
			sent += roster.find((p) => p.pid === pid)!.amount;
		}
		assert.isAtLeast(sent, 35_000);
		assert.isAtMost(out.length, MAX_BALLAST);
	});

	test("gives up rather than send half a roster", () => {
		// Three bodies cover 60M at most; more than that is off.
		assert.isUndefined(pickBallast(roster, 70_000));
		assert.isUndefined(pickBallast([], 1));
	});

	test("whatever it sends covers the shortfall, for any roster", () => {
		let seed = 7;
		const rand = () => {
			seed = (seed * 1_664_525 + 1_013_904_223) >>> 0;
			return seed / 4_294_967_296;
		};
		for (let i = 0; i < 300; i++) {
			const players = Array.from(
				{ length: Math.floor(rand() * 8) },
				(_, k) => ({
					pid: k,
					value: rand() * 80,
					amount: Math.floor(rand() * 40_000),
				}),
			);
			const needed = rand() * 60_000;
			const out = pickBallast(players, needed);
			if (out === undefined) {
				continue;
			}
			assert.isAtMost(out.length, MAX_BALLAST);
			assert.strictEqual(new Set(out).size, out.length, "a pid twice");
			const sent = out.reduce(
				(a, pid) => a + players.find((p) => p.pid === pid)!.amount,
				0,
			);
			assert.isAtLeast(sent, needed);
		}
	});
});
