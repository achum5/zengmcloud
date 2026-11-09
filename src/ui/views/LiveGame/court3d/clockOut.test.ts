import { assert, describe, test } from "vitest";
import { compileCourt, type CourtPlayer, type RawEvent } from "./director.ts";
import { evalPlayer } from "./evaluate.ts";

// Ten men on the floor, then whatever lines are given.
const game = (lines: RawEvent[]) => {
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
		...lines,
		{ type: "gameOver" },
	);
	return { tl: compileCourt({ events, players, gid: 1 }), events };
};

describe("3D clock running out", () => {
	test("a second or so left after a basket, it comes in before the horn", () => {
		const { tl, events } = game([
			{ type: "fgaAtRim", t: 0, pid: 2, clock: 3.1 },
			{ type: "fgAtRim", t: 0, pid: 2, clock: 1.2 },
			{ type: "endOfPeriod", t: 1, clock: 0, reason: "noShot" },
		]);
		const horn = tl.beats.find(
			(b) => events[b.i]?.type === "endOfPeriod",
		)!.actionStart;
		// In to a man of theirs, and his for the second and a bit that was
		// left - not the horn with the ball still on the floor.
		const held = tl.ball.findLast((s) => s.t0 <= horn);
		assert.strictEqual(held?.kind, "hold");
		const pid = (held as { pid: number }).pid;
		assert.strictEqual(tl.tracks.get(pid)!.team, 0);
		const caught = tl.ball.findLast(
			(s) =>
				s.kind === "fly" &&
				s.t1 <= horn &&
				"pid" in s.to &&
				s.to.pid === pid &&
				// (A bounce pass in: up off the floor to him.)
				!("pid" in s.from && s.from.pid === pid),
		);
		assert.isDefined(caught);
		const left = horn - (caught as { t1: number }).t1;
		assert.isAbove(left, 900);
		assert.isBelow(left, 1700);
	});

	test("running out the clock at the end, his man stands off the dribbler", () => {
		const { tl, events } = game([
			{ type: "fgaTp", t: 0, pid: 2, clock: 700, desperation: false },
			{ type: "tp", t: 0, pid: 2, clock: 699 },
			{ type: "fgaMidRange", t: 1, pid: 12, clock: 21.4, desperation: false },
			{ type: "missMidRange", t: 1, pid: 12, clock: 20.6 },
			{ type: "drb", t: 0, pid: 5, clock: 20 },
			{ type: "endOfPeriod", t: 0, clock: 0, reason: "runOutClock" },
		]);
		const horn = tl.beats.find(
			(b) => events[b.i]?.type === "endOfPeriod",
		)!.actionStart;
		const held = tl.ball.findLast((s) => s.t0 <= horn);
		assert.strictEqual(held?.kind, "hold");
		const pid = (held as { pid: number }).pid;
		const team = tl.tracks.get(pid)!.team;
		// Nobody up on him the last second of it...
		for (const t of [horn - 1000, horn - 500, horn]) {
			const P = evalPlayer(tl, pid, t);
			const near = Math.min(
				...[...tl.tracks.values()]
					.filter((o) => o.pid >= 0 && o.team !== team)
					.map((o) => {
						const Q = evalPlayer(tl, o.pid, t);
						return Math.hypot(Q.x - P.x, Q.y - P.y);
					}),
			);
			assert.isAbove(near, 5, `${Math.round(horn - t)} ms before the horn`);
		}
		// ... and nothing for him to sell a fake to, then or after.
		const sold = tl.tracks
			.get(pid)!
			.acts.filter(
				(a) =>
					/^(jab|shotFake|dribbleHesi|triple)$/.test(a.anim) &&
					a.t1 > horn - 1500,
			);
		assert.deepEqual(
			sold.map((a) => a.anim),
			[],
		);
	});
});
