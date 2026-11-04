// A TOOL, not a check: the 3D GAME TAPE. A whole game as the 3D court plays
// it - every player's spot, facing and animation and the ball, every tenth of
// a second, possession by possession - with every moment that looks
// unnatural at real speed flagged: a man running backward at a run, sliding
// without stepping, jumping from one spot to another, two men inside each
// other, a man stood with the ball, an open lane to the rim passed up.
// Skipped unless asked:
//
//   TAPE_GAMES=/dir/real_11.json,lane-a TAPE_DIR=/out npx vitest run \
//     --project basketball tools/court3d-tape/gameTape.test.ts
//
// A game is a file from realGames.test.ts next to this one (the real sim)
// or a test-game seed.
// For each it writes /out/tape_<game>.json (the whole game) and
// /out/tape_<game>.txt (what was flagged, where); view.py and sheet.py here draw
// a possession, or a sheet of every flag of one kind, from the json.
import { readFileSync, writeFileSync } from "node:fs";
import process from "node:process";
import { test } from "vitest";
import { compileCourt } from "../../src/ui/views/LiveGame/court3d/director.ts";
import {
	evalBall,
	evalPlayer,
	fastAt,
} from "../../src/ui/views/LiveGame/court3d/evaluate.ts";
import {
	COURT_H,
	COURT_W,
	rimX,
} from "../../src/ui/views/LiveGame/court3d/geometry.ts";
import { bodyOf } from "../../src/ui/views/LiveGame/court3d/poses.ts";
import { compile as compileFake } from "../../src/ui/views/LiveGame/court3d/testGame.ts";

const body = bodyOf();
const STEP = 100;
const RUNS = new Set(["run", "jog", "sprint", "walk"]);
const STEPPING = new Set([
	"run",
	"jog",
	"sprint",
	"walk",
	"shuffle",
	"slide",
	"back",
	"backpedal",
	"sideStep",
]);
const SHOT_PLAYS = /^(fg|miss|tp|ast|tov|stl|pf|blk|orb|drb)/;

const load = (game: string) => {
	if (game.endsWith(".json")) {
		const { events, players } = JSON.parse(readFileSync(game, "utf8"));
		return compileCourt({ events, players, gid: 1 });
	}
	return compileFake(game, 90).tl;
};

test.skipIf(!process.env.TAPE_GAMES)(
	"3D game tape",
	() => {
		for (const game of process.env.TAPE_GAMES!.split(",")) {
			const tl = load(game);
			const tracks = [...tl.tracks.values()].filter((t) => t.pid >= 0);
			const name = game
				.split("/")
				.at(-1)!
				.replace(/\.json$/, "");
			const holderAt = (t: number): number | undefined => {
				let lo = 0;
				let hi = tl.ball.length - 1;
				let k = -1;
				while (lo <= hi) {
					const mid = (lo + hi) >> 1;
					if (tl.ball[mid]!.t0 <= t) {
						k = mid;
						lo = mid + 1;
					} else {
						hi = mid - 1;
					}
				}
				const s = tl.ball[k];
				return s?.kind === "hold" ? s.pid : undefined;
			};
			const cutNear = (a: number, b: number) =>
				tl.cuts.some((c) => c > a - 1 && c <= b + 1);
			const poss = tl.poss;
			const out: any[] = [];
			const flagsAll: string[] = [];
			const counts: Record<string, number> = {};
			for (let i = 0; i < poss.length; i++) {
				const t0 = poss[i]![0];
				const t1 = poss[i + 1]?.[0] ?? t0 + 20_000;
				if (t1 - t0 < 1000 || t1 - t0 > 60_000) {
					continue;
				}
				const beats = tl.beats
					.filter((b) => b.actionStart >= t0 - 500 && b.actionStart < t1)
					.map((b) => ({ type: b.type, t: Math.round(b.actionStart) }));
				const frames: any[] = [];
				const flags: { t: number; kind: string; pid?: number; text: string }[] =
					[];
				const flag = (
					t: number,
					kind: string,
					pid: number | undefined,
					text: string,
				) => {
					flags.push({ t: Math.round(t), kind, pid, text });
					counts[kind] = (counts[kind] ?? 0) + 1;
				};
				const prev = new Map<number, any>();
				const run = new Map<string, number>();
				const sustain = (
					key: string,
					on: boolean,
					t: number,
					need: number,
				): boolean => {
					if (!on) {
						run.delete(key);
						return false;
					}
					const since = run.get(key) ?? t;
					run.set(key, since);
					return t - since >= need && t - since < need + STEP;
				};
				for (let t = t0; t < t1; t += STEP) {
					const ballP: any = evalBall(tl, t, () => body);
					const holder = holderAt(t);
					const ps = tracks.map((tr) => {
						const q = evalPlayer(tl, tr.pid, t);
						return {
							pid: tr.pid,
							team: tr.team,
							x: q.x,
							y: q.y,
							yaw: q.yaw,
							anim: q.anim,
							shown: q.shown,
							moving: q.moving,
						};
					});
					const beat = tl.beats.findLast((b) => b.preStart <= t);
					const real = fastAt(tl, t) < 1.2;
					const live =
						real &&
						!!beat &&
						SHOT_PLAYS.test(beat.type) &&
						!/ft$/i.test(beat.type);
					frames.push({
						t: Math.round(t),
						f: +fastAt(tl, t).toFixed(1),
						b: [
							+ballP.x.toFixed(2),
							+ballP.y.toFixed(2),
							+ballP.z.toFixed(2),
							holder ?? -1,
						],
						p: ps.map((p) => [
							p.pid,
							+p.x.toFixed(2),
							+p.y.toFixed(2),
							+p.yaw.toFixed(2),
							p.anim,
							p.shown ? 1 : 0,
						]),
					});
					for (const p of ps) {
						const q = prev.get(p.pid);
						prev.set(p.pid, p);
						if (!q || !p.shown || !q.shown) {
							continue;
						}
						const vx = (p.x - q.x) * (1000 / STEP);
						const vy = (p.y - q.y) * (1000 / STEP);
						const v = Math.hypot(vx, vy);
						if (!real) {
							continue;
						}
						if (v > 30 && !cutNear(t - STEP, t)) {
							flag(
								t,
								"teleport",
								p.pid,
								`${p.pid} jumps ${(v / 10).toFixed(1)} ft in 0.1 s`,
							);
						}
						const fx = Math.cos(p.yaw);
						const fy = Math.sin(p.yaw);
						const back =
							v > 4 && (vx * fx + vy * fy) / v < -0.3 && RUNS.has(p.anim);
						if (sustain(`mw${p.pid}`, back, t, 400)) {
							flag(
								t,
								"moonwalk",
								p.pid,
								`${p.pid} running backwards (${p.anim}) at ${v.toFixed(1)} ft/s`,
							);
						}
						const glide = v > 3 && !p.moving && !STEPPING.has(p.anim);
						if (sustain(`gl${p.pid}`, glide, t, 300)) {
							flag(
								t,
								"glide",
								p.pid,
								`${p.pid} slides ${v.toFixed(1)} ft/s without stepping (${p.anim})`,
							);
						}
						const offFloor =
							p.x < -1 || p.x > COURT_W + 1 || p.y < -1 || p.y > COURT_H + 1;
						const out =
							offFloor &&
							!tl.ball.some(
								(z) =>
									z.t0 > t - 4000 &&
									z.t0 < t + 4000 &&
									((z.kind === "hold" && z.pid === p.pid) ||
										(z.kind === "fly" &&
											"pid" in z.from &&
											z.from.pid === p.pid)),
							);
						if (live && sustain(`ob${p.pid}`, out, t, 1500)) {
							flag(t, "offCourt", p.pid, `${p.pid} off the floor in play`);
						}
						if (live && p.pid === holder) {
							const still = v < 0.5;
							if (sustain(`fr${p.pid}`, still, t, 3000)) {
								flag(t, "frozen", p.pid, `${p.pid} stands with the ball 3 s`);
							}
						}
					}
					for (let a = 0; a < ps.length; a++) {
						for (let c = a + 1; c < ps.length; c++) {
							const A = ps[a]!;
							const C = ps[c]!;
							const close =
								A.shown && C.shown && Math.hypot(A.x - C.x, A.y - C.y) < 1.0;
							if (live && sustain(`ov${A.pid}-${C.pid}`, close, t, 300)) {
								flag(
									t,
									"overlap",
									A.pid,
									`${A.pid} and ${C.pid} inside each other`,
								);
							}
						}
					}
				}
				// Open lane passed up, and stops/veers on an open drive: at 50 ms.
				for (let k = 0; k < tl.ball.length; k++) {
					const s = tl.ball[k]!;
					if (s.t0 < t0 || s.t0 >= t1) {
						continue;
					}
					if (s.kind === "fly" && "pid" in s.from && s.from.pid >= 0) {
						const h = s.from.pid;
						const me = tl.tracks.get(h)!;
						const toMate =
							"pid" in s.to &&
							s.to.pid !== h &&
							tl.tracks.get(s.to.pid)?.team === me.team;
						if ("pid" in s.to && !toMate) {
							continue;
						}
						const beat = tl.beats.findLast((x) => x.preStart <= s.t0);
						if (
							!beat ||
							/ft$/i.test(beat.type) ||
							!SHOT_PLAYS.test(beat.type)
						) {
							continue;
						}
						let since = s.t0;
						for (let j = k - 1; j >= 0; j--) {
							const z = tl.ball[j]!;
							if (z.kind !== "hold" || z.pid !== h) {
								break;
							}
							since = z.t0;
						}
						if (s.t0 - since < 700) {
							continue;
						}
						const t = s.t0 - 150;
						const P = evalPlayer(tl, h, t);
						const rim = { x: rimX(me.team), y: COURT_H / 2 };
						const d = Math.hypot(rim.x - P.x, rim.y - P.y);
						if (d < 6 || d > 24) {
							continue;
						}
						const u = { x: (rim.x - P.x) / d, y: (rim.y - P.y) / d };
						let blocked = false;
						let near = Infinity;
						for (const o of tracks) {
							if (o.team === me.team) {
								continue;
							}
							const q = evalPlayer(tl, o.pid, t);
							if (!q.shown) {
								continue;
							}
							const along = (q.x - P.x) * u.x + (q.y - P.y) * u.y;
							const off = Math.abs((q.x - P.x) * u.y - (q.y - P.y) * u.x);
							if (along > 0.5 && along < d + 2 && off < 2.2 + along * 0.08) {
								blocked = true;
							}
							near = Math.min(near, Math.hypot(q.x - P.x, q.y - P.y));
						}
						if (!blocked && near > 5) {
							const what = "pid" in s.to ? "passes" : "shoots";
							flag(
								s.t0,
								"openLane",
								h,
								`${h} ${what} from ${d.toFixed(0)} ft with an open lane (${beat.type})`,
							);
						}
					}
				}
				out.push({
					i,
					t0: Math.round(t0),
					t1: Math.round(t1),
					side: poss[i]![1],
					beats,
					flags,
					frames,
				});
				for (const f of flags) {
					flagsAll.push(`${name} poss ${i} t=${f.t} ${f.kind}: ${f.text}`);
				}
			}
			const dir = process.env.TAPE_DIR ?? ".";
			writeFileSync(
				`${dir}/tape_${name}.json`,
				JSON.stringify({
					game: name,
					players: tracks.map((tr) => ({ pid: tr.pid, team: tr.team })),
					possessions: out,
				}),
			);
			writeFileSync(
				`${dir}/tape_${name}.txt`,
				`${name}: ${out.length} possessions\n${JSON.stringify(counts)}\n${flagsAll.join("\n")}\n`,
			);
		}
	},
	3_600_000,
);
