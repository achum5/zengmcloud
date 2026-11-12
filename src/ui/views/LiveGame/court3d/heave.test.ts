import { assert, describe, test } from "vitest";
import { compileCourt, type CourtPlayer, type RawEvent } from "./director.ts";
import { BALL_R, evalBall, evalPlayer } from "./evaluate.ts";
import { rimX, type Side } from "./geometry.ts";
import { bodyFor } from "./directorTesting.ts";

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

const buzzerCases = () => [
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

describe("3D heaves at the buzzer", () => {
	test(
		"he races up the floor with it and lets it go from deep - never backing out to it",
		{ timeout: 60_000 },
		() => {
			const cases = buzzerCases();
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

	// Off the rim with the horn gone and nobody after it, it bounces the way
	// a ball does: on the way it came down (not off somewhere else), each hop
	// lower than the last, and rolls to a stop where it stops.
	test(
		"nobody after it, a heave off the rim bounces on the way it came off, lower every time",
		{ timeout: 60_000 },
		() => {
			let seen = 0;
			for (const [k, { events, players }] of buzzerCases().entries()) {
				const tl = compileCourt({ events, players, gid: 7 + k });
				const shot = tl.beats.find((b) => events[b.i]!.desperation === true)!;
				const drop = tl.ball.find(
					(s) => s.kind === "bounce" && s.free && s.t0 > shot.actionStart,
				);
				if (!drop || drop.kind !== "bounce") {
					continue;
				}
				seen += 1;
				const at = (t: number) => evalBall(tl, t, bodyFor);
				// Its way along the floor as it comes down.
				const a = at(drop.t0 - 30);
				const b = at(drop.t0 - 10);
				const L = Math.hypot(b.x - a.x, b.y - a.y);
				const way = { x: (b.x - a.x) / L, y: (b.y - a.y) / L };
				const tops: number[] = [];
				let prev = at(drop.t0);
				let up = false;
				for (let t = drop.t0 + 10; t <= drop.t1; t += 10) {
					const q = at(t);
					const dx = q.x - prev.x;
					const dy = q.y - prev.y;
					if (Math.hypot(dx, dy) > 0.01) {
						assert.isAbove(
							(dx * way.x + dy * way.y) / Math.hypot(dx, dy),
							0.99,
							`case ${k}: on its way at ${t - drop.t0}`,
						);
					}
					if (q.z > prev.z) {
						up = true;
					} else if (up && q.z < prev.z) {
						tops.push(prev.z);
						up = false;
					}
					prev = q;
				}
				assert.isAtLeast(tops.length, 2, `case ${k}: hops`);
				for (let i = 1; i < tops.length; i++) {
					assert.isBelow(tops[i]!, tops[i - 1]!, `case ${k}: hop ${i}`);
				}
				// (Up off the floor as a ball comes up off it from the rim -
				// feet, not inches.)
				assert.isAbove(tops[0]! - BALL_R, 3, `case ${k}: first hop`);
				const end = at(drop.t1);
				assert.isBelow(
					Math.hypot(end.x - drop.to.x, end.y - drop.to.y),
					0.05,
					`case ${k}: stops where it stops`,
				);
			}
			assert.isAbove(seen, 0);
		},
	);
});
