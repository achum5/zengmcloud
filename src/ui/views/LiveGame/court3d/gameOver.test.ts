import { assert, describe, test } from "vitest";
import { compileCourt, type CourtPlayer, type RawEvent } from "./director.ts";
import { evalPlayer } from "./evaluate.ts";

// The last seconds of a game, the sim's way, won (or not) on the last shot.
const ending = (lastShot: RawEvent[]) => {
	const players: CourtPlayer[] = [];
	const POS = ["PG", "SG", "SF", "PF", "C"];
	const events: RawEvent[] = [{ type: "init", boxScore: {} }];
	for (const raw of [0, 1] as const) {
		for (let j = 0; j < 5; j++) {
			const pid = raw * 10 + j + 1;
			players.push({ pid, team: raw === 0 ? 1 : 0, pos: POS[j] });
			events.push(
				{ type: "stat", t: raw, pid, s: "pts", amt: 0 },
				{ type: "stat", t: raw, pid, s: "gs", amt: 1 },
			);
		}
	}
	events.push(
		{ type: "jumpBall", t: 0, pid: 5, pid2: 15, clock: 9 },
		{ type: "fgaMidRange", t: 1, pid: 12, clock: 6, desperation: false },
		{ type: "fgMidRange", t: 1, pid: 12, clock: 5.6 },
		{ type: "stat", t: 1, pid: 12, s: "pts", amt: 2 },
		...lastShot,
		{ type: "endOfPeriod", t: 0, reason: "noShot", clock: 0 },
		{ type: "gameOver" },
	);
	return compileCourt({ events, players, gid: 3 });
};

describe("3D end of game", () => {
	test("a game-winner is mobbed, and everybody daps up down the line", () => {
		const tl = ending([
			{ type: "fgaTp", t: 0, pid: 2, clock: 1.4, desperation: false },
			{ type: "tp", t: 0, pid: 2, clock: 0.6 },
			{ type: "stat", t: 0, pid: 2, s: "pts", amt: 3 },
		]);
		const over = tl.beats.at(-1)!;
		// His teammates all over him.
		const t = over.actionStart + 1800;
		const H = evalPlayer(tl, 2, t);
		for (const mate of [1, 3, 4, 5]) {
			const M = evalPlayer(tl, mate, t);
			assert.isBelow(Math.hypot(M.x - H.x, M.y - H.y), 4, `${mate}`);
		}
		// Everybody slaps hands with at least two of the other team.
		for (const pid of [1, 2, 3, 4, 5, 11, 12, 13, 14, 15]) {
			const daps = tl.tracks
				.get(pid)!
				.acts.filter(
					(a) =>
						(a.anim === "lowFive" || a.anim === "highFive") &&
						a.t0 > over.actionStart,
				).length;
			assert.isAtLeast(daps, 2, `${pid}`);
		}
	});
});
