import { assert, describe, test } from "vitest";
import { compileCourt, type CourtPlayer, type RawEvent } from "./director.ts";
import { evalPlayer } from "./evaluate.ts";
import { rimX, type Side } from "./geometry.ts";

// A quarter's last few seconds, the sim's way: whatever came before, then a
// desperation three with the clock all but gone.
const endOfQuarter = (before: RawEvent[], shooter: number, team: 0 | 1) => {
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
		{ type: "jumpBall", t: 0, pid: 5, pid2: 15, clock: 9 },
		...before,
		{ type: "fgaTp", t: team, pid: shooter, clock: 0.6, desperation: true },
		{ type: "missTp", t: team, pid: shooter, clock: 0 },
		{ type: "endOfPeriod", t: team, reason: "noShot", clock: 0 },
		{ type: "gameOver" },
	);
	return { events, players };
};

describe("3D heaves at the buzzer", () => {
	test(
		"he races up the floor with it and lets it go from deep - never backing out to it",
		{ timeout: 60_000 },
		() => {
			const cases = [
				// Off a defensive board at the other end, five seconds left.
				endOfQuarter(
					[
						{
							type: "fgaMidRange",
							t: 0,
							pid: 2,
							clock: 6.2,
							desperation: false,
						},
						{ type: "missMidRange", t: 0, pid: 2, clock: 6.0 },
						{ type: "drb", t: 1, pid: 15, clock: 5.7 },
					],
					11,
					1,
				),
				// Off a make, two seconds left.
				endOfQuarter(
					[
						{ type: "fgaAtRim", t: 0, pid: 5, clock: 2.4, desperation: false },
						{ type: "fgAtRim", t: 0, pid: 5, clock: 2.1 },
						{ type: "stat", t: 0, pid: 5, s: "pts", amt: 2 },
					],
					12,
					1,
				),
			];
			for (const [k, { events, players }] of cases.entries()) {
				const tl = compileCourt({ events, players, gid: 7 + k });
				const shot = tl.beats.find((b) => events[b.i]!.desperation === true)!;
				const pid = events[shot.i]!.pid as number;
				const team = tl.tracks.get(pid)!.team as Side;
				const rim = { x: rimX(team), y: 25 };
				const far = (t: number) => {
					const st = evalPlayer(tl, pid, t);
					return Math.hypot(st.x - rim.x, st.y - rim.y);
				};
				assert.isAtLeast(far(shot.actionStart), 27, `case ${k}: from deep`);
				let backed = 0;
				for (let t = shot.actionStart - 1500; t < shot.actionStart; t += 100) {
					backed += Math.max(0, far(t + 100) - far(t));
				}
				// (A step or two back to meet the inbound is fine; backing out to
				// half court to shoot it is not.)
				assert.isBelow(
					backed,
					6,
					`case ${k}: backed out ${backed.toFixed(1)} ft`,
				);
			}
		},
	);
});
