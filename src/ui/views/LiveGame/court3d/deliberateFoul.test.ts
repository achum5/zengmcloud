import { assert, describe, test } from "vitest";
import { compileCourt, type CourtPlayer, type RawEvent } from "./director.ts";
import { evalBall, evalPlayer } from "./evaluate.ts";
import { bodyFor } from "./directorTesting.ts";

// Fouling on purpose late to stop the clock: the man the play-by-play sends
// to the line is the one they foul - the ball in his hands as they get to
// him - not whoever happened to be bringing it up, a man who never goes to
// the line chased down while somebody else shoots the free throws.
const game = (lead: RawEvent[]) => {
	const players: CourtPlayer[] = [];
	const POS = ["PG", "SG", "SF", "PF", "C"];
	const events: RawEvent[] = [{ type: "init", boxScore: {} }];
	for (const raw of [0, 1] as const) {
		for (let j = 0; j < 5; j++) {
			const pid = raw * 10 + j + 1;
			players.push({ pid, team: raw === 0 ? 1 : 0, pos: POS[j] });
			events.push({ type: "stat", t: raw, pid, s: "gs", amt: 1 });
		}
	}
	events.push(
		{ type: "jumpBall", t: 0, pid: 5, pid2: 15, clock: 720 },
		{ type: "fgaMidRange", t: 0, pid: 2, clock: 710, desperation: false },
		{ type: "fgMidRange", t: 0, pid: 2, clock: 709.6 },
		{ type: "stat", t: 0, pid: 2, s: "pts", amt: 2 },
		{ type: "endOfPeriod", t: 1, reason: "noShot", clock: 0 },
		{ type: "period", period: 4, clock: 720 },
		...lead,
		// Down two with under two minutes left: fouled at once - and it is
		// the SF who goes to the line.
		{ type: "pfBonus", t: 1, pid: 13, pidShooting: 3, clock: 102 },
		{ type: "ft", t: 0, pid: 3, clock: 102 },
		{ type: "stat", t: 0, pid: 3, s: "pts", amt: 1 },
		{ type: "ft", t: 0, pid: 3, clock: 102 },
		{ type: "stat", t: 0, pid: 3, s: "pts", amt: 1 },
		{ type: "endOfPeriod", t: 1, reason: "noShot", clock: 0 },
		{ type: "gameOver" },
	);
	return compileCourt({ events, players, gid: 7 });
};

const check = (tl: ReturnType<typeof game>) => {
	const b = tl.beats.find((x) => x.type === "pfBonus");
	assert.isDefined(b);
	const hit = b!.actionStart;
	assert.strictEqual(
		evalBall(tl, hit, bodyFor).holder,
		3,
		"the man fouled on purpose has the ball as they get to him",
	);
	const V = evalPlayer(tl, 3, hit);
	const F = evalPlayer(tl, 13, hit);
	assert.isBelow(Math.hypot(V.x - F.x, V.y - F.y), 3.5, "the fouler is on him");
};

describe("fouled on purpose late", () => {
	test("off a defensive board: the ball to the man they foul, and him fouled", () => {
		check(
			game([
				{ type: "fgaMidRange", t: 1, pid: 12, clock: 106, desperation: false },
				{ type: "missMidRange", t: 1, pid: 12, clock: 105.4 },
				{ type: "drb", t: 0, pid: 4, clock: 104 },
			]),
		);
	});

	test("after their basket: in to the man they foul, and him fouled", () => {
		check(
			game([
				// (Up four, then two: still behind, they foul.)
				{ type: "fgaMidRange", t: 0, pid: 1, clock: 112, desperation: false },
				{ type: "fgMidRange", t: 0, pid: 1, clock: 111.4 },
				{ type: "stat", t: 0, pid: 1, s: "pts", amt: 2 },
				{ type: "fgaMidRange", t: 1, pid: 12, clock: 106, desperation: false },
				{ type: "fgMidRange", t: 1, pid: 12, clock: 105.4 },
				{ type: "stat", t: 1, pid: 12, s: "pts", amt: 2 },
			]),
		);
	});
});
