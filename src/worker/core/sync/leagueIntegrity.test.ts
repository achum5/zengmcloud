import { assert, describe, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import {
	findIntegrityProblems,
	findPayloadIntegrityProblems,
} from "./leagueIntegrity.ts";

// The catastrophe detector. It has one job: notice that a database is no
// longer a league - stripped rosters, vanished teams - before that state can
// sim, publish, or be restored onto anyone else. And one anti-job: never fire
// on a legitimate league state, because quarantining healthy devices is worse
// than the disease.

const fullRoster = (tid: number, count = 12) =>
	Array.from({ length: count }, (_, i) => ({ tid, pid: tid * 100 + i }));

const league = (perTeam: number[]) => ({
	players: perTeam.flatMap((count, tid) => fullRoster(tid, count)),
	teams: perTeam.map((_, tid) => ({ tid })),
});

describe("findIntegrityProblems", () => {
	test("a healthy league passes", () => {
		assert.deepStrictEqual(
			findIntegrityProblems({
				...league([12, 13, 15]),
				phase: PHASE.REGULAR_SEASON,
			}),
			[],
		);
	});

	// The exact league from the incident: 2-3 players a team, mid-season,
	// games on the schedule.
	test("stripped rosters mid-season are the catastrophe this exists for", () => {
		const problems = findIntegrityProblems({
			...league([2, 3, 2]),
			phase: PHASE.REGULAR_SEASON,
		});
		assert.strictEqual(problems.length, 1);
		assert.match(problems[0]!, /rosters stripped/);
	});

	test("it fires in every phase where games are played", () => {
		for (const phase of [
			PHASE.PRESEASON,
			PHASE.REGULAR_SEASON,
			PHASE.AFTER_TRADE_DEADLINE,
			PHASE.PLAYOFFS,
		]) {
			assert.ok(
				findIntegrityProblems({ ...league([2, 2]), phase }).length > 0,
				`phase ${phase}`,
			);
		}
	});

	// Thin rosters are NORMAL in the offseason - re-signing and free agency
	// legitimately empty them out. Firing there would quarantine every healthy
	// device once a year.
	test("it stays quiet through the whole offseason", () => {
		for (const phase of [
			PHASE.DRAFT_LOTTERY,
			PHASE.DRAFT,
			PHASE.AFTER_DRAFT,
			PHASE.RESIGN_PLAYERS,
			PHASE.FREE_AGENCY,
		]) {
			assert.deepStrictEqual(
				findIntegrityProblems({ ...league([2, 0]), phase }),
				[],
				`phase ${phase}`,
			);
		}
	});

	test("a fantasy draft empties every roster BY DESIGN", () => {
		// Rosters at zero, everyone in the temp draft pool.
		const problems = findIntegrityProblems({
			players: Array.from({ length: 30 }, (_, i) => ({ tid: -6, pid: i })),
			teams: [{ tid: 0 }, { tid: 1 }],
			phase: PHASE.FANTASY_DRAFT,
		});
		assert.deepStrictEqual(problems, []);
	});

	test("an expansion draft's new team starts empty BY DESIGN", () => {
		assert.deepStrictEqual(
			findIntegrityProblems({ ...league([12, 0]), phase: PHASE.EXPANSION_DRAFT }),
			[],
		);
	});

	test("a disabled team is allowed to be empty", () => {
		const problems = findIntegrityProblems({
			players: fullRoster(0),
			teams: [{ tid: 0 }, { tid: 1, disabled: true }],
			phase: PHASE.REGULAR_SEASON,
		});
		assert.deepStrictEqual(problems, []);
	});

	test("free agents and prospects don't count toward anyone's roster", () => {
		const problems = findIntegrityProblems({
			players: [
				...fullRoster(0),
				// A big FA pool must not mask team 1 having nobody.
				...Array.from({ length: 40 }, (_, i) => ({ tid: -1, pid: 900 + i })),
			],
			teams: [{ tid: 0 }, { tid: 1 }],
			phase: PHASE.REGULAR_SEASON,
		});
		assert.strictEqual(problems.length, 1);
	});

	test("no teams and no players are named separately", () => {
		const problems = findIntegrityProblems({
			players: [],
			teams: [],
			phase: PHASE.REGULAR_SEASON,
		});
		assert.ok(problems.includes("no teams"));
		assert.ok(problems.includes("no players"));
	});

	test("an unknown phase judges structure only, never rosters", () => {
		assert.deepStrictEqual(
			findIntegrityProblems({ ...league([2, 2]), phase: undefined }),
			[],
		);
	});

	test("the message stays readable when many teams are hit", () => {
		const problems = findIntegrityProblems({
			...league([1, 1, 1, 1, 1, 1, 1, 1]),
			phase: PHASE.REGULAR_SEASON,
		});
		assert.strictEqual(problems.length, 1);
		assert.match(problems[0]!, /and 3 more/);
	});
});

describe("findPayloadIntegrityProblems", () => {
	test("reads the phase out of gameAttributes rows", () => {
		const problems = findPayloadIntegrityProblems({
			players: fullRoster(0, 2),
			teams: [{ tid: 0 }],
			gameAttributes: [
				{ key: "season", value: 2006 },
				{ key: "phase", value: PHASE.REGULAR_SEASON },
			],
		});
		assert.strictEqual(problems.length, 1);
		assert.match(problems[0]!, /rosters stripped/);
	});

	test("understands the wrapped {start, value} attribute shape", () => {
		const problems = findPayloadIntegrityProblems({
			players: fullRoster(0, 2),
			teams: [{ tid: 0 }],
			gameAttributes: [
				{
					key: "phase",
					value: [
						{ start: 2000, value: PHASE.PLAYOFFS },
					],
				},
			],
		});
		assert.strictEqual(problems.length, 1);
	});

	test("a payload with no phase attribute judges structure only", () => {
		assert.deepStrictEqual(
			findPayloadIntegrityProblems({
				players: fullRoster(0, 2),
				teams: [{ tid: 0 }],
				gameAttributes: [{ key: "season", value: 2006 }],
			}),
			[],
		);
	});
});
