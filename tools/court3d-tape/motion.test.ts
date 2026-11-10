// HOW SCRIPTED THE MOVEMENT LOOKS (see SIM.md): over the given games, at
// real speed in the half court -
// - the man with the ball: how much of his dribbling he stands, how much he
//   goes at one steady pace, how often he changes direction on the move;
// - the men off the ball: how much they stand while he has it;
// - lockstep: how often two or more men set off within a tenth of a second
//   of each other;
// - reaction: how long after the man with the ball changes direction the
//   man nearest him does;
// - shadowing: how long after any man near the rim goes, the man on him
//   goes the same way;
// - cuts: how often anybody on the move turns hard.
//
//   MOTION_GAMES=/tmp/games/real_11.json,... MOTION_OUT=/tmp/motion.txt \
//     npx vitest run --project basketball tools/court3d-tape/motion.test.ts
//
// MOTION_STEP=1 measures the games stepped (see stepper.ts).

import { readFileSync, writeFileSync } from "node:fs";
import process from "node:process";
import { test } from "vitest";
import {
	compileCourt,
	type CourtTimeline,
} from "../../src/ui/views/LiveGame/court3d/director.ts";
import {
	evalPlayer,
	fastAt,
} from "../../src/ui/views/LiveGame/court3d/evaluate.ts";
import { rimX } from "../../src/ui/views/LiveGame/court3d/geometry.ts";

const games = (process.env.MOTION_GAMES ?? "").split(",").filter(Boolean);
type Pt = { x: number; y: number };
const dist = (a: Pt, b: Pt) => Math.hypot(a.x - b.x, a.y - b.y);

// Feet a second at t, over a tenth of a second either side.
const speed = (tl: CourtTimeline, pid: number, t: number) => {
	const a = evalPlayer(tl, pid, t - 50);
	const b = evalPlayer(tl, pid, t + 50);
	return { v: dist(a, b) * 10, dx: b.x - a.x, dy: b.y - a.y };
};

export const measure = (tl: CourtTimeline) => {
	const DT = 100;
	let dribbling = 0;
	let standing = 0;
	let steady = 0;
	let turns = 0;
	let offBall = 0;
	let offStill = 0;
	const lags: number[] = [];
	for (let k = 0; k < tl.ball.length; k++) {
		const s = tl.ball[k]!;
		if (s.kind !== "hold" || s.style === "hold") {
			continue;
		}
		const pid = s.pid;
		const end = tl.ball[k + 1]?.t0 ?? s.t0;
		const tr = tl.tracks.get(pid);
		if (!tr) {
			continue;
		}
		const rim = { x: rimX(tr.team), y: 25 };
		let prev: { v: number; dx: number; dy: number } | undefined;
		let heading: Pt | undefined;
		for (let t = s.t0 + 200; t < end - 100; t += DT) {
			if (fastAt(tl, t) > 1.01) {
				prev = undefined;
				heading = undefined;
				continue;
			}
			const P = evalPlayer(tl, pid, t);
			if (dist(P, rim) > 47) {
				continue;
			}
			dribbling += DT;
			const sp = speed(tl, pid, t);
			if (sp.v < 0.7) {
				standing += DT;
			} else if (prev && Math.abs(sp.v - prev.v) < 0.4) {
				steady += DT;
			}
			prev = sp;
			// Changes of direction over 0.4 s, on the move.
			if ((t - s.t0) % 400 < DT) {
				const A = evalPlayer(tl, pid, t);
				const B = evalPlayer(tl, pid, t + 400);
				const h = { x: B.x - A.x, y: B.y - A.y };
				const L = Math.hypot(h.x, h.y);
				if (L >= 0.6) {
					if (heading) {
						const c =
							(h.x * heading.x + h.y * heading.y) /
							(L * Math.hypot(heading.x, heading.y));
						if (c < 0.7) {
							turns += 1;
							// His man: the nearest of theirs - how long till he
							// turns the same way.
							let near: number | undefined;
							let nd = 8;
							for (const o of tl.tracks.values()) {
								if (o.pid < 0 || o.team === tr.team) {
									continue;
								}
								const d = dist(evalPlayer(tl, o.pid, t), A);
								if (d < nd) {
									nd = d;
									near = o.pid;
								}
							}
							if (near !== undefined) {
								for (let lag = 0; lag <= 1500; lag += 50) {
									const C = evalPlayer(tl, near, t + lag);
									const D = evalPlayer(tl, near, t + lag + 200);
									const g = { x: D.x - C.x, y: D.y - C.y };
									const gl = Math.hypot(g.x, g.y);
									if (gl > 0.3 && (g.x * h.x + g.y * h.y) / (gl * L) > 0.7) {
										lags.push(lag);
										break;
									}
								}
							}
						}
					}
					heading = h;
				} else {
					heading = undefined;
				}
			}
			for (const o of tl.tracks.values()) {
				if (o.pid < 0 || o.team !== tr.team || o.pid === pid) {
					continue;
				}
				if (!evalPlayer(tl, o.pid, t).shown) {
					continue;
				}
				offBall += DT;
				if (speed(tl, o.pid, t).v < 0.7) {
					offStill += DT;
				}
			}
		}
	}
	// Lockstep: moments two or more men set off from standing within a tenth
	// of a second of each other, out of all setting off - each read off
	// where he is (stood a third of a second, then going), however his runs
	// are laid down.
	const starts: number[] = [];
	for (const tr of tl.tracks.values()) {
		if (tr.pid < 0) {
			continue;
		}
		let still = 0;
		let prev: { x: number; y: number; shown: boolean } | undefined;
		for (let t = 0; t < tl.end; t += 50) {
			const P = evalPlayer(tl, tr.pid, t);
			if (!P.shown || fastAt(tl, t) > 1.01 || !prev?.shown) {
				still = 0;
				prev = P;
				continue;
			}
			const v = dist(P, prev) * 20;
			prev = P;
			if (v < 0.6) {
				still += 50;
			} else {
				if (still >= 300 && v > 1.5) {
					starts.push(t);
				}
				still = 0;
			}
		}
	}
	starts.sort((a, b) => a - b);
	let together = 0;
	for (let i = 0; i < starts.length; i++) {
		if (
			(i > 0 && starts[i]! - starts[i - 1]! <= 100) ||
			(i + 1 < starts.length && starts[i + 1]! - starts[i]! <= 100)
		) {
			together += 1;
		}
	}
	lags.sort((a, b) => a - b);
	// Shadowing: how long after a man goes his man goes the same way - the
	// delay his man's going matches his best at (negative: ahead of him).
	const SHADOW = [
		-400, -300, -200, -100, -50, 0, 50, 100, 150, 200, 250, 300, 400, 500, 600,
	];
	const sums = SHADOW.map(() => 0);
	let mm = 0;
	let dd = 0;
	const vel = (pid: number, t: number) => {
		const a = evalPlayer(tl, pid, t - 50);
		const b = evalPlayer(tl, pid, t + 50);
		return {
			x: (b.x - a.x) * 10,
			y: (b.y - a.y) * 10,
			shown: a.shown && b.shown,
		};
	};
	for (let t = 1000; t < tl.end - 1000; t += 200) {
		if (fastAt(tl, t) > 1.01) {
			continue;
		}
		const ball = tl.ball.findLast((s) => s.t0 <= t);
		if (ball?.kind !== "hold") {
			continue;
		}
		const holder = tl.tracks.get(ball.pid);
		if (!holder) {
			continue;
		}
		for (const o of tl.tracks.values()) {
			if (o.pid < 0 || o.team !== holder.team) {
				continue;
			}
			const P = evalPlayer(tl, o.pid, t);
			if (!P.shown || dist(P, { x: rimX(o.team), y: 25 }) > 30) {
				continue;
			}
			let near: number | undefined;
			let nd = 6;
			for (const d of tl.tracks.values()) {
				if (d.pid < 0 || d.team === o.team) {
					continue;
				}
				const Q = evalPlayer(tl, d.pid, t);
				if (Q.shown && dist(P, Q) < nd) {
					nd = dist(P, Q);
					near = d.pid;
				}
			}
			const vm = vel(o.pid, t);
			if (near === undefined || Math.hypot(vm.x, vm.y) < 2) {
				continue;
			}
			mm += vm.x * vm.x + vm.y * vm.y;
			SHADOW.forEach((L, k) => {
				const vd = vel(near!, t + L);
				sums[k]! += vm.x * vd.x + vm.y * vd.y;
				if (L === 0) {
					dd += vd.x * vd.x + vd.y * vd.y;
				}
			});
		}
	}
	let best = 0;
	SHADOW.forEach((_, k) => {
		if (sums[k]! > sums[best]!) {
			best = k;
		}
	});
	// Cuts: how often anybody moving in the half court turns hard - more
	// than 60 degrees between the 0.3 s before and the 0.3 s after, a foot
	// and a half each way - a minute of moving.
	let moving = 0;
	let cuts = 0;
	for (const tr of tl.tracks.values()) {
		if (tr.pid < 0) {
			continue;
		}
		let last = -Infinity;
		for (let t = 1000; t < tl.end - 1000; t += 100) {
			if (fastAt(tl, t) > 1.01) {
				continue;
			}
			const A = evalPlayer(tl, tr.pid, t - 300);
			const P = evalPlayer(tl, tr.pid, t);
			const B = evalPlayer(tl, tr.pid, t + 300);
			if (
				!A.shown ||
				!P.shown ||
				!B.shown ||
				dist(P, { x: rimX(tr.team), y: 25 }) > 47
			) {
				continue;
			}
			const li = dist(A, P);
			const lo = dist(P, B);
			if (li + lo > 1.2) {
				moving += 100;
			}
			if (li < 1.5 || lo < 1.5) {
				continue;
			}
			const c =
				((P.x - A.x) * (B.x - P.x) + (P.y - A.y) * (B.y - P.y)) / (li * lo);
			if (c < 0.5 && t - last > 600) {
				cuts += 1;
				last = t;
			}
		}
	}
	const minutes = dribbling / 60000;
	return {
		dribblingMin: +minutes.toFixed(1),
		standing: +(standing / Math.max(1, dribbling)).toFixed(3),
		steady: +(steady / Math.max(1, dribbling)).toFixed(3),
		turnsPerMin: +(turns / Math.max(0.01, minutes)).toFixed(2),
		offBallStill: +(offStill / Math.max(1, offBall)).toFixed(3),
		lockstep: +(together / Math.max(1, starts.length)).toFixed(3),
		reactionMs: lags.length ? lags[Math.floor(lags.length / 2)]! : undefined,
		cutsPerMin: +(cuts / Math.max(0.01, moving / 60000)).toFixed(2),
		shadowMs: SHADOW[best],
		shadowFit: +(sums[best]! / Math.max(1e-9, Math.sqrt(mm * dd))).toFixed(2),
	};
};

test.skipIf(games.length === 0)(
	"how scripted the movement looks",
	() => {
		const rows: string[] = [];
		for (const g of games) {
			const { events, players } = JSON.parse(readFileSync(g, "utf8"));
			const tl = compileCourt({
				events,
				players,
				gid: 1,
				step: !!process.env.MOTION_STEP,
			});
			rows.push(`${g.split("/").at(-1)} ${JSON.stringify(measure(tl))}`);
		}
		const out = rows.join("\n");
		if (process.env.MOTION_OUT) {
			writeFileSync(process.env.MOTION_OUT, `${out}\n`);
		}
		console.log(out);
	},
	3_600_000,
);
