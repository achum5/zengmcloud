// A TOOL, not a check: the football 3D GAME TAPE, as basketball's (see
// tools/court3d-tape/gameTape.test.ts). A whole game as the 3D field plays
// it - every man's spot, facing and move and the ball, every tenth of a
// second, play by play - with every moment that looks unnatural at real
// speed flagged:
//
// - teleport: a man jumps more than 4.5 ft in a tenth of a second
// - lurch / lurchUp: his pace changes by more than 8 ft/s in a tenth of a
//   second, slowing / speeding up (lurchEng: in or out of a block - an
//   engage - where some of that is the hit itself)
// - corner: at speed, his way swings round more than 40 degrees in one step
// - snap: stood, his facing swings round more than 70 degrees in one step
// - moonwalk: running backward in a gait that faces where it goes
// - glide: sliding along without stepping
// - fastwalk / fastjog: a walk quicker than 10 ft/s, a jog than 18
// - statue: not a step or a turn, the same still move (not one that plays
//   through, a look round, say), 3 s on end
//
// Skipped unless asked:
//
//   TAPE_GAMES=/dir/real_7.json,23 TAPE_DIR=/out npx vitest run \
//     --project football tools/field3d-tape/gameTape.football.test.ts
//
// A game is a file from realGames.football.test.ts next to this one (or the
// bare play-by-play, its roster read off the box score), or a test-game seed.
// TAPE_SPAN is the part of each play looked at: live (snap to whistle, the
// default), pre (out of the huddle to the snap), after (whistle to the next
// play) or all. TAPE_GID sets the game id the field is laid out with (it
// seeds its choices; a seed's own number by default).
// For each game it writes /out/tape_<game>.json (the whole game) and
// /out/tape_<game>.txt (the counts, then what was flagged, where); view.py and
// sheet.py here draw a play, or a sheet of every flag of one kind, from the
// json.
import { readFileSync, writeFileSync } from "node:fs";
import process from "node:process";
import { test } from "vitest";
import {
	compileField,
	type FieldTimeline,
	type Play,
} from "../../src/ui/views/LiveGame/field3d/director.ts";
import { momentAt } from "../../src/ui/views/LiveGame/field3d/evaluate.ts";
import type { RosterMan } from "../../src/ui/views/LiveGame/field3d/personnel.ts";
import { moveKind } from "../../src/ui/views/LiveGame/field3d/poses.ts";
import { simGames } from "../../src/ui/views/LiveGame/field3d/testGames.ts";

const STEP = 100;
// (Facings are not kept to one turn round: the angle between two, -pi to pi.)
const wrap = (a: number) => a - 2 * Math.PI * Math.round(a / (2 * Math.PI));
// Moves a man is meant to go backward or sideways in: not moonwalks.
const BACKWARD = /back|pedal|shuffle|slide|passset|set$|engage|drive/i;
// Moves a man is meant to hold still in: not statues.
const HELD = /Ready$|pocket/;

// (A bare play-by-play: the roster off its box score, raw team 0 the home
// team - display side 1 - as field3d/testGames.ts has it.)
const rosterOf = (events: any[]): RosterMan[] =>
	([0, 1] as const).flatMap((rt) =>
		events[0].boxScore[rt].player.map((p: any) => ({
			pid: p.id,
			team: rt === 0 ? 1 : 0,
			pos: p.pos,
			name: p.name,
		})),
	);

const load = async (game: string): Promise<FieldTimeline> => {
	const gidOf = (n: number) => Number(process.env.TAPE_GID ?? n);
	if (game.endsWith(".json")) {
		const raw = JSON.parse(readFileSync(game, "utf8"));
		const events = Array.isArray(raw) ? raw : raw.events;
		const players = Array.isArray(raw) ? rosterOf(events) : raw.roster;
		const n = Number(/(\d+)\.json$/.exec(game)?.[1] ?? 1);
		return compileField({ events, players, gid: gidOf(n) });
	}
	const [sim] = await simGames([Number(game)]);
	return compileField({
		events: sim!.events,
		players: sim!.roster,
		gid: gidOf(Number(game)),
	});
};

const spanOf = (p: Play): [number, number] => {
	switch (process.env.TAPE_SPAN ?? "live") {
		case "pre":
			return [p.t0, p.snap];
		case "after":
			return [p.whistle, p.t1];
		case "all":
			return [p.t0, p.t1];
		default:
			return [p.snap, p.whistle];
	}
};

test.skipIf(!process.env.TAPE_GAMES)(
	"football 3D game tape",
	async () => {
		for (const game of process.env.TAPE_GAMES!.split(",")) {
			const tl = await load(game);
			const name = game
				.split("/")
				.at(-1)!
				.replace(/\.json$/, "");
			const cutNear = (a: number, b: number) =>
				tl.cuts.some((c) => c > a - 1 && c <= b + 1);
			const out: any[] = [];
			const flagsAll: string[] = [];
			const counts: Record<string, number> = {};
			tl.plays.forEach((p, i) => {
				const [t0, t1] = spanOf(p);
				if (!(t1 > t0)) {
					return;
				}
				const roles = new Map(p.parts.map((q) => [q.pid, q.role]));
				const holderAt = (t: number) => {
					const s = p.ball.findLast((z) => z.t0 <= t);
					return s?.kind === "held" ? s.pid : -1;
				};
				const frames: any[] = [];
				const flags: {
					t: number;
					kind: string;
					pid: number;
					text: string;
				}[] = [];
				const flag = (t: number, kind: string, pid: number, text: string) => {
					flags.push({ t: Math.round(t - p.snap), kind, pid, text });
					counts[kind] = (counts[kind] ?? 0) + 1;
				};
				const prev = new Map<number, any>();
				const still0 = new Map<
					number,
					{ x: number; y: number; yaw: number; move: string; t: number }
				>();
				for (let t = t0; t < t1; t += STEP) {
					const m = momentAt(tl, t);
					const men = m.players.filter((st) => st.id >= 0);
					const ball = m.focus;
					frames.push({
						t: Math.round(t - p.snap),
						b: [
							+(ball?.x ?? -1).toFixed(2),
							+(ball?.y ?? -1).toFixed(2),
							+(ball?.z ?? 0).toFixed(2),
							holderAt(t),
						],
						p: men.map((st) => [
							st.id,
							+st.x.toFixed(2),
							+st.y.toFixed(2),
							+st.yaw.toFixed(2),
							st.move,
						]),
					});
					const cut = cutNear(t - 2 * STEP, t);
					for (const st of men) {
						const q = prev.get(st.id);
						const cur: any = {
							x: st.x,
							y: st.y,
							yaw: st.yaw,
							move: st.move,
						};
						prev.set(st.id, cur);
						if (!q || cut) {
							continue;
						}
						const vx = ((st.x - q.x) * 1000) / STEP;
						const vy = ((st.y - q.y) * 1000) / STEP;
						const v = Math.hypot(vx, vy);
						cur.v = v;
						cur.vx = vx;
						cur.vy = vy;
						const who = `${roles.get(st.id) ?? "?"}${st.id}`;
						const moves = `(${q.move}>${st.move})`;
						if (
							(st.move === "walk" && v > 10) ||
							(st.move === "jog" && v > 18)
						) {
							flag(
								t,
								`fast${st.move}`,
								st.id,
								`${who} ${st.move}s at ${v.toFixed(0)} ft/s`,
							);
						}
						if (v > 45) {
							flag(
								t,
								"teleport",
								st.id,
								`${who} jumps ${(v / 10).toFixed(1)} ft in 0.1 s ${moves}`,
							);
						}
						const turn = Math.abs(wrap(st.yaw - q.yaw));
						if (v < 2 && turn > (70 * Math.PI) / 180) {
							flag(
								t,
								"snap",
								st.id,
								`${who} spins ${((turn * 180) / Math.PI).toFixed(0)} deg in 0.1 s ${moves}`,
							);
						}
						if (v > 8 && q.v > 8) {
							const swing = Math.abs(
								wrap(Math.atan2(vy, vx) - Math.atan2(q.vy, q.vx)),
							);
							if (swing > (40 * Math.PI) / 180) {
								flag(
									t,
									"corner",
									st.id,
									`${who} turns ${((swing * 180) / Math.PI).toFixed(0)} deg at ${v.toFixed(0)} ft/s in 0.1 s ${moves}`,
								);
							}
						}
						if (q.v !== undefined && Math.abs(v - q.v) > 8) {
							const kind =
								st.move === "engage" || q.move === "engage"
									? "lurchEng"
									: v > q.v
										? "lurchUp"
										: "lurch";
							flag(
								t,
								kind,
								st.id,
								`${who} ${q.v.toFixed(0)} to ${v.toFixed(0)} ft/s in 0.1 s ${moves}`,
							);
						}
						const kind = moveKind(st.move);
						const fx = Math.cos(st.yaw);
						const fy = Math.sin(st.yaw);
						const backward =
							v > 4 &&
							(vx * fx + vy * fy) / v < -0.3 &&
							kind === "cycle" &&
							!BACKWARD.test(st.move);
						cur.mw = backward ? (q.mw ?? 0) + 1 : 0;
						if (cur.mw === 3) {
							flag(
								t,
								"moonwalk",
								st.id,
								`${who} running backward (${st.move}) at ${v.toFixed(0)} ft/s`,
							);
						}
						const glide = v > 3 && kind === "still";
						cur.gl = glide ? (q.gl ?? 0) + 1 : 0;
						if (cur.gl === 3) {
							flag(
								t,
								"glide",
								st.id,
								`${who} slides ${v.toFixed(0)} ft/s without stepping (${st.move})`,
							);
						}
						const r0 = still0.get(st.id);
						if (
							!r0 ||
							Math.hypot(st.x - r0.x, st.y - r0.y) > 0.4 ||
							Math.abs(wrap(st.yaw - r0.yaw)) > (8 * Math.PI) / 180 ||
							st.move !== r0.move
						) {
							still0.set(st.id, {
								x: st.x,
								y: st.y,
								yaw: st.yaw,
								move: st.move,
								t,
							});
						} else if (
							t - r0.t >= 3000 &&
							t - r0.t < 3000 + STEP &&
							!HELD.test(st.move) &&
							kind !== "loop" &&
							kind !== "act"
						) {
							flag(t, "statue", st.id, `${who} stock still 3 s (${st.move})`);
						}
					}
				}
				out.push({
					i,
					kind: p.kind,
					quarter: p.quarter,
					offense: p.offense,
					dir: p.dir,
					losX: p.losX,
					situation: p.situation,
					whistle: Math.round(p.whistle - p.snap),
					roles: Object.fromEntries(roles),
					flags,
					frames,
				});
				for (const f of flags) {
					flagsAll.push(
						`${name} play ${i} (${p.kind}) ${(f.t / 1000).toFixed(1)}s ${f.kind}: ${f.text}`,
					);
				}
			});
			const teams = new Map<number, number>();
			for (const p of tl.plays) {
				for (const q of p.parts) {
					if (q.pid >= 0) {
						teams.set(q.pid, q.team);
					}
				}
			}
			const dir = process.env.TAPE_DIR ?? ".";
			writeFileSync(
				`${dir}/tape_${name}.json`,
				JSON.stringify({
					game: name,
					players: [...teams].map(([pid, team]) => ({ pid, team })),
					plays: out,
				}),
			);
			writeFileSync(
				`${dir}/tape_${name}.txt`,
				`${name}: ${out.length} plays\n${JSON.stringify(counts)}\n${flagsAll.join("\n")}\n`,
			);
		}
	},
	3_600_000,
);
