import { assert, beforeAll, describe, test } from "vitest";
import processLiveGameEvents, {
	DEFAULT_SPORT_STATE,
	type SportState,
} from "../../../util/processLiveGameEvents.football.tsx";
import { HERO_CAM_FT } from "./action.ts";
import { aimFor } from "./aim.ts";
import { FIGURE_SCALE } from "./body.ts";
import {
	compileField,
	isLineItem,
	QB_CAM_FT,
	TABLET_CAM_FT,
	targetForCursor,
	type BallSeg,
	type FieldTimeline,
	type Leg,
	type Play,
} from "./director.ts";
import { momentAt } from "./evaluate.ts";
import {
	CROSSBAR_Z,
	dirFor,
	FIELD_L,
	FIELD_W,
	GOAL_L,
	GOAL_R,
	MID_X,
	MID_Y,
	NET_BACK,
	TEAM_AREA,
	yardLineX,
} from "./geometry.ts";
import { NET_HALF, NET_TOP, netAt } from "./net.ts";
import { makeFieldClock, REPLAY_SPEED, stepField } from "./playback.ts";
import { replayFor, replaySky } from "./replays.ts";
import { coachId, headCoachAt, sidelineAt, sidelineFor } from "./sideline.ts";
import type { FieldPlayerState } from "./sprite.ts";
import { stageField, stageOpening } from "./stageField.ts";
import { simGames, type SimGame } from "./testGames.ts";

let games: SimGame[] = [];

beforeAll(async () => {
	games = await simGames([18, 19, 20]);
}, 120_000);

// How far over a sideline a man run out of bounds may go (see action.ts
// runOut).
const OUT_OF_BOUNDS = 10;

const stage = ({ events, roster }: SimGame, gid = 1): FieldTimeline =>
	compileField({ events, players: roster, gid });

// The leg a man is in at t.
const legAt = (part: { legs: Leg[] }, t: number): Leg =>
	part.legs.findLast((l) => l.t0 <= t) ?? part.legs[0]!;

describe("compileField", () => {
	test("the opening, staged on its own, is the whole game's to the man - up to where the whole game takes over", () => {
		for (const game of games) {
			const input = { events: game.events, players: game.roster, gid: 1 };
			const opening = stageOpening(input)!;
			const full = compileField(input);
			const { tl, upTo } = opening;
			assert.isAbove(tl.end, 10_000);
			assert.deepEqual(
				tl.beats.map((b) => [b.i, b.actionStart]),
				full.beats.filter((b) => b.i < upTo).map((b) => [b.i, b.actionStart]),
			);
			for (const b of full.beats) {
				if (b.i >= upTo) {
					assert.isAtLeast(b.actionStart, tl.end);
				}
			}
			for (let t = 0; t <= tl.end; t += 100) {
				const a = momentAt(tl, t);
				const b = momentAt(full, t);
				assert.deepEqual(
					a.players.map((x) => [x.id, x.x, x.y]),
					b.players.map((x) => [x.id, x.x, x.y]),
					`t ${t}`,
				);
				assert.deepEqual(a.ball?.at, b.ball?.at, `t ${t}`);
			}
		}
	}, 60_000);

	test("staged with no worker to be had (as here): on the page, the same game", async () => {
		const game = games[0]!;
		const off = await stageField({
			events: game.events,
			players: game.roster,
			gid: 1,
		});
		const on = stage(game);
		assert.strictEqual(off.end, on.end);
		assert.deepEqual(
			off.beats.map((b) => [b.i, b.actionStart]),
			on.beats.map((b) => [b.i, b.actionStart]),
		);
	}, 60_000);

	test("every line the page shows gets its moment, in order", () => {
		for (const game of games) {
			const tl = stage(game);
			// The lines, as the page itself takes them off the play-by-play.
			const sportState: SportState = structuredClone(DEFAULT_SPORT_STATE);
			const raw = game.events[0]!.boxScore;
			const team = (rt: 0 | 1) =>
				({
					abbrev: rt === 0 ? "HOM" : "AWY",
					players: raw[rt].player.map((p: any) => ({
						pid: p.id,
						name: p.name,
						injury: { type: "Healthy", gamesRemaining: 0 },
						seasonStats: {},
						inGame: false,
					})),
					ptsQtrs: [0],
					timeouts: 3,
				}) as any;
			const boxScore = {
				gid: 1000 + games.indexOf(game),
				quarter: "",
				quarterShort: "",
				numPeriods: 4,
				possession: undefined,
				teams: [team(0), team(1)] as any,
				time: "",
				scoringSummary: [],
			};
			const queue = [...game.events];
			const quarters: string[] = [];
			const lines: number[] = [];
			while (queue.length > 0) {
				const before = game.events.length - queue.length;
				const out = processLiveGameEvents({
					events: queue as any,
					boxScore,
					overtimes: 0,
					quarters,
					sportState,
				});
				const after = game.events.length - queue.length;
				if (out.text !== undefined) {
					lines.push(after - 1);
				}
				assert.isAbove(after, before);
			}
			assert.deepStrictEqual(
				tl.beats.map((b) => b.i),
				lines,
			);
			assert.deepStrictEqual(
				lines,
				game.events.flatMap((e, i) => (isLineItem(e) ? [i] : [])),
			);
			for (let k = 1; k < tl.beats.length; k++) {
				assert.isAtLeast(
					tl.beats[k]!.actionStart,
					tl.beats[k - 1]!.actionStart,
				);
			}
			// The clock never has to run backward to get to the next line.
			let last = 0;
			for (let cursor = 0; cursor <= game.events.length; cursor++) {
				const t = targetForCursor(tl, cursor);
				assert.isAtLeast(t, last);
				last = t;
			}
		}
	}, 20_000);

	test("eleven a side on the field, everyone the stats name among them", () => {
		for (const game of games) {
			const tl = stage(game);
			tl.plays.forEach((play, k) => {
				const sides = [0, 0];
				const on = new Set<number>();
				for (const part of play.parts) {
					sides[part.team]! += 1;
					assert.isFalse(on.has(part.pid), `${part.pid} on twice`);
					on.add(part.pid);
					// (A man hurt on it off to his sideline - see "a man hurt". A
					// man run out of bounds, a stride or two over a sideline.)
					const hurt = part.legs.some(
						(l) => l.move === "hurt" || l.move === "hurtProne",
					);
					for (const leg of hurt ? [] : part.legs) {
						for (const p of [leg.from, leg.to]) {
							assert.isTrue(
								p.x >= 0 &&
									p.x <= FIELD_L &&
									p.y >= -OUT_OF_BOUNDS &&
									p.y <= FIELD_W + OUT_OF_BOUNDS,
								`${part.pid} at ${p.x}, ${p.y}`,
							);
						}
					}
				}
				assert.deepStrictEqual(sides, [11, 11], `play at ${play.i}`);
				const end = tl.plays[k + 1]?.i ?? game.events.length;
				for (let i = play.i; i < end; i++) {
					const e = game.events[i]!;
					if (e.type === "stat" && typeof e.pid === "number") {
						assert.isTrue(on.has(e.pid), `${e.s} by ${e.pid} at ${i}`);
					}
				}
			});
		}
	}, 20_000);

	test("each side lines up on its own side of the ball, going the right way", () => {
		for (const game of games) {
			const tl = stage(game);
			for (const play of tl.plays) {
				if (play.kind !== "pass" && play.kind !== "run") {
					continue;
				}
				const clock = game.events[play.i]!;
				assert.strictEqual(play.dir, dirFor(play.offense, play.quarter));
				assert.strictEqual(play.losX, yardLineX(clock.scrimmage, play.dir));
				for (const part of play.parts) {
					const set = legAt(part, play.snap - 1).to;
					const ahead = play.dir * (set.x - play.losX);
					if (part.team === play.offense) {
						assert.isAtMost(ahead, 0, `${part.role} offside`);
					} else {
						assert.isAtLeast(ahead, 0, `${part.role} offside`);
					}
				}
			}
		}
	}, 20_000);

	test("the quarterback in the gun or under center, the ball in the center's hand", () => {
		const tl = stage(games[0]!);
		for (const play of tl.plays) {
			if (play.kind !== "pass") {
				continue;
			}
			const qb = play.parts.find((p) => p.role === "QB")!;
			const center = play.parts.find((p) => p.role === "C")!;
			const back = -play.dir * (legAt(qb, play.snap - 1).to.x - play.losX);
			assert.isTrue(back > 2 && back < 16, `QB ${back} ft back`);
			assert.strictEqual(legAt(center, play.snap - 1).ball, "snap");
			assert.isTrue(
				play.ball.some((b) => b.kind === "held" && b.pid === center.pid),
			);
		}
	}, 20_000);

	test("a game takes about as long to watch as a broadcast", () => {
		for (const game of games) {
			const minutes = stage(game).end / 60_000;
			assert.isTrue(minutes > 15 && minutes < 45, `${minutes} minutes`);
		}
	}, 20_000);

	test("the same game is staged the same way every time", () => {
		const a = stage(games[1]!, 5);
		const b = stage(games[1]!, 5);
		assert.deepStrictEqual(a, b);
	}, 20_000);
});

describe("replays", () => {
	test("a score, a pick, a long gain, a big sack shown again once the play is over", () => {
		let shown = 0;
		let sacks = 0;
		for (const game of games) {
			const tl = stage(game);
			tl.plays.forEach((play, k) => {
				const end = tl.plays[k + 1]?.i ?? game.events.length;
				const seg = game.events.slice(play.i, end);
				const r = play.replay;
				const stands = seg.some(
					(e) => e.type === "penalty" && e.decision !== "decline",
				);
				const td = seg.some(
					(e) =>
						e.td === true &&
						["run", "passComplete", "interceptionReturn"].includes(e.type),
				);
				if (td && !stands) {
					assert.isDefined(r, `a score on play ${play.i}`);
				}
				const sack = seg.some(
					(e) => e.type === "sack" && typeof e.yds === "number" && e.yds <= -9,
				);
				if (sack && !stands) {
					assert.isDefined(r, `a sack on play ${play.i}`);
					sacks += 1;
				}
				if (!r) {
					return;
				}
				shown += 1;
				// Of the play itself, cut to after it, before the next.
				assert.isAtLeast(r.from, play.t0, `play ${play.i}`);
				assert.isAbove(r.to, r.from + 1000, `play ${play.i}`);
				assert.isAtMost(r.to, r.cue, `play ${play.i}`);
				assert.isBelow(r.cue, play.t1, `play ${play.i}`);
				assert.isAbove(r.cue, play.snap, `play ${play.i}`);
			});
		}
		assert.isAbove(shown, 15);
		assert.isAbove(sacks, 0);
	}, 20_000);

	test("a score's cut to from behind the end zone it was scored in, for the last of it - nothing else's", () => {
		let behind = 0;
		for (const game of games) {
			const tl = stage(game);
			tl.plays.forEach((play, k) => {
				const r = play.replay;
				const ez = r?.endZone;
				const end = tl.plays[k + 1]?.i ?? game.events.length;
				const td = game.events
					.slice(play.i, end)
					.some((e) => e.td === true && e.type !== "extraPoint");
				if (!r || !ez) {
					return;
				}
				behind += 1;
				assert.isTrue(td, `play ${play.i}`);
				assert.isAbove(ez.from, r.from + 1000, `play ${play.i}`);
				assert.isBelow(ez.from, r.to - 900, `play ${play.i}`);
				// (The ball in that end zone, or all but, by the end of it.)
				const ball = momentAt(tl, r.to).focus!;
				if (ez.end === 0) {
					assert.isBelow(ball.x, GOAL_L + 3, `play ${play.i}`);
				} else {
					assert.isAbove(ball.x, GOAL_R - 3, `play ${play.i}`);
				}
			});
		}
		assert.isAbove(behind, 3);
	}, 20_000);

	test("a long field goal, or one late that ties it or puts them ahead: from behind the kicker, onto the posts", () => {
		const snap = 1000;
		const ball: BallSeg[] = [
			{ kind: "held", t0: 0, pid: 1 },
			{
				kind: "fly",
				t0: snap,
				t1: snap + 350,
				from: { x: 300, y: 80, z: 1 },
				to: { x: 279, y: 80, z: 1 },
				peak: 0,
			},
			{ kind: "held", t0: snap + 350, pid: 2 },
			{ kind: "spot", t0: snap + 700, at: { x: 279, y: 80, z: 0 } },
			{
				kind: "fly",
				t0: snap + 1300,
				t1: snap + 3100,
				from: { x: 279, y: 80, z: 0.5 },
				to: { x: 380, y: 82, z: 0 },
				peak: 30,
			},
		];
		const shown = (yds: number, made: boolean, clutch: boolean) =>
			replayFor({
				kind: "fieldGoal",
				dir: 1,
				events: [{ type: "fieldGoal", made, yds }],
				beats: [
					{
						i: 0,
						type: "fieldGoal",
						preStart: snap + 1300,
						actionStart: snap + 2400,
						end: snap + 5000,
					},
				],
				t0: 0,
				snap,
				whistle: snap + 3200,
				t1: snap + 9000,
				ball,
				fouled: false,
				clutch,
			});
		const long = shown(52, true, false);
		const sky = long?.sky;
		assert.isDefined(sky);
		assert.isUndefined(long!.endZone);
		// (Behind where it is kicked from, from before it is.)
		assert.isBelow(sky!.at!, 279);
		assert.isBelow(sky!.from, snap + 1300);
		// (Over the bar at the end line, and the replay on a moment past it.)
		assert.strictEqual(sky!.goal!.x, FIELD_L);
		assert.isAbove(sky!.goal!.z, CROSSBAR_Z);
		const over = snap + 1300 + 1800 * ((FIELD_L - 279) / 101);
		assert.approximately(long!.to, over + 900, 1);
		assert.isUndefined(shown(35, true, false));
		assert.isDefined(shown(35, true, true)?.sky?.goal);
		assert.isUndefined(shown(52, false, true));
	});

	test("anything else's cut to from the Skycam, looking the way it went, for the last of it", () => {
		let sky = 0;
		let scores = 0;
		for (const game of games) {
			const tl = stage(game);
			for (const play of tl.plays) {
				const r = play.replay;
				if (!r?.sky) {
					continue;
				}
				sky += 1;
				assert.isAtLeast(r.sky.from, r.from + 1000, `play ${play.i}`);
				// (A long score: the Skycam between the first angle and the end
				// zone's, looking the way he went in.)
				if (r.endZone) {
					scores += 1;
					assert.isAtMost(r.sky.from, r.endZone.from - 1400, `play ${play.i}`);
					assert.strictEqual(
						r.sky.way,
						r.endZone.end === 1 ? 1 : -1,
						`play ${play.i}`,
					);
					assert.isUndefined(replaySky(r, r.endZone.from), `play ${play.i}`);
					continue;
				}
				assert.isAtMost(r.sky.from, r.to - 1000, `play ${play.i}`);
				// (Anything but a kick's return, the way the offense went.)
				if (play.kind !== "kickoff" && play.kind !== "punt") {
					assert.strictEqual(r.sky.way, play.dir, `play ${play.i}`);
				}
			}
		}
		assert.isAbove(sky, 3);
		assert.isAbove(scores, 0);
	}, 20_000);

	test("the live picture held while one shows, slowed down - and not on fast", () => {
		const tl = stage(games[0]!);
		const play = tl.plays.find((p) => p.replay)!;
		const r = play.replay!;
		const input = {
			cursor: 1e9,
			paused: false,
			rate: 1,
			follower: false,
			eventsLength: 1e9,
			onReady: () => {},
		};
		const run = (rate: number) => {
			const s = makeFieldClock(false, 0);
			s.t = r.cue - 500;
			let now = 0;
			stepField(s, { ...input, rate }, tl, now);
			const seen: { t: number; at?: number }[] = [];
			for (let k = 0; k < 2000; k++) {
				now += 16;
				stepField(s, { ...input, rate }, tl, now);
				seen.push({ t: s.t, ...(s.replay ? { at: s.replay.at } : {}) });
			}
			return seen;
		};
		const seen = run(1);
		const during = seen.filter((x) => x.at !== undefined);
		assert.isAbove(during.length, 10);
		for (const x of during) {
			assert.strictEqual(x.t, r.cue);
		}
		// From the start of it to the end, at its own pace.
		const spanMs = during.length * 16;
		assert.closeTo(spanMs, (r.to - r.from) / REPLAY_SPEED, 40);
		assert.isAbove(seen.at(-1)!.t, r.cue);
		// On fast: on through it.
		assert.isTrue(run(4).every((x) => x.at === undefined));
	}, 20_000);
});

describe("the net behind the posts", () => {
	test("up for a kick, where it catches the ball, and down again after", () => {
		let caught = 0;
		for (const game of games) {
			const tl = stage(game);
			for (const play of tl.plays) {
				const kick = play.kind === "fieldGoal" || play.kind === "extraPoint";
				if (!kick) {
					assert.isUndefined(netAt(play, play.snap), `play ${play.i}`);
					continue;
				}
				assert.isUndefined(netAt(play, play.t0), `play ${play.i}`);
				const side = play.dir === 1 ? 1 : 0;
				const x = side === 1 ? FIELD_L + NET_BACK : -NET_BACK;
				assert.deepEqual(netAt(play, play.snap), { side, up: 1 });
				for (const b of play.ball) {
					if (b.kind !== "fly" || b.to.z < 0.1 || Math.abs(b.to.x - x) > 0.01) {
						continue;
					}
					caught += 1;
					assert.strictEqual(netAt(play, b.t1)?.up, 1, `play ${play.i}`);
					assert.isBelow(Math.abs(b.to.y - MID_Y), NET_HALF, `play ${play.i}`);
					assert.isBelow(b.to.z * FIGURE_SCALE, NET_TOP, `play ${play.i}`);
				}
				assert.isUndefined(netAt(play, play.whistle + 4000), `play ${play.i}`);
			}
		}
		assert.isAbove(caught, 10);
	}, 20_000);
});

describe("in close on a big play's man", () => {
	test("on his own in his moment, the camera on the field, out before anything else", () => {
		let close = 0;
		for (const game of games) {
			const tl = stage(game);
			for (const play of tl.plays) {
				const c = play.heroClose;
				if (!c) {
					continue;
				}
				close += 1;
				const at = `play ${play.i}`;
				assert.strictEqual(play.party?.from !== undefined, true, at);
				assert.isAtLeast(c.t1 - c.t0, 1300, at);
				assert.isAtMost(c.t1 - c.t0, 2600, at);
				assert.isBelow(c.at.y + HERO_CAM_FT, FIELD_W, at);
				assert.isAtMost(c.t1, play.t1 - 400, at);
				if (play.refCall) {
					assert.isAtMost(c.t1, play.refCall.t0, at);
				}
				if (play.replay) {
					assert.isAtMost(c.t1, play.replay.cue - 300, at);
				}
				const him = play.parts.find((p) => p.pid === c.pid)!;
				for (let t = c.t0; t < c.t1; t += 100) {
					const l = legAt(him, t);
					const u = Math.min(1, Math.max(0, (t - l.t0) / (l.t1 - l.t0)));
					const x = l.from.x + (l.to.x - l.from.x) * u;
					const y = l.from.y + (l.to.y - l.from.y) * u;
					assert.isAtMost(Math.hypot(x - c.at.x, y - c.at.y), 6.01, at);
				}
			}
		}
		assert.isAbove(close, 2);
	}, 20_000);
});

// Before the snap on a big down, in close on the quarterback in the gun
// reading the defense - from the near side, nobody in the way or stood just
// behind him in the picture - and out wide again a while before the snap.
describe("in close on the quarterback", () => {
	test("in the gun, the camera on him clear, out before the snap", () => {
		let close = 0;
		for (const game of games) {
			const tl = stage(game);
			for (const play of tl.plays) {
				const c = play.qbClose;
				if (!c) {
					continue;
				}
				close += 1;
				const at = `play ${play.i}`;
				assert.include(["pass", "run"], play.kind, at);
				assert.isAtLeast(c.t0, play.t0 + 1500, at);
				assert.isAtLeast(c.t1 - c.t0, 2599, at);
				assert.isAtMost(c.t1 - c.t0, 3600, at);
				assert.isAtMost(c.t1, play.snap - 1400, at);
				assert.strictEqual(Math.sign(c.shift), play.dir, at);
				const qb = play.parts.find((p) => p.pid === c.pid)!;
				assert.strictEqual(qb.role, "QB", at);
				assert.include(
					["shotgun", "gunCall", "gunLook"],
					legAt(qb, c.t1).move,
					at,
				);
				const shot = aimFor(momentAt(tl, c.t0 + 100));
				assert.isBelow(shot.width, 20, at);
				for (let t = c.t0; t < c.t1; t += 100) {
					const m = momentAt(tl, t);
					const him = m.players.find((p) => p.id === c.pid)!;
					if (t >= c.t0 + 1500) {
						assert.isBelow(
							Math.hypot(him.x - c.at.x, him.y - c.at.y),
							0.01,
							at,
						);
					}
					for (const p of m.players) {
						const d = p.y - c.at.y;
						if (
							p.id === c.pid ||
							Math.abs(d) <= 1.5 ||
							d <= -8 ||
							d >= QB_CAM_FT
						) {
							continue;
						}
						const x = c.at.x + (c.shift * d) / QB_CAM_FT;
						assert.isAtLeast(
							Math.abs(p.x - x),
							2.5,
							`${at} ${p.id} @${t - c.t0}`,
						);
					}
				}
			}
		}
		assert.isAbove(close, 0);
	}, 30_000);
});

describe("the chain crew", () => {
	// Where a man is at t.
	const posAt = (part: { legs: Leg[] }, t: number) => {
		const l = legAt(part, t);
		const u =
			l.t1 > l.t0 ? Math.min(1, Math.max(0, (t - l.t0) / (l.t1 - l.t0))) : 1;
		return {
			x: l.from.x + (l.to.x - l.from.x) * u,
			y: l.from.y + (l.to.y - l.from.y) * u,
		};
	};

	test("the box on the line, the rods ten yards apart up to the line to gain - on the far sideline, the near one after the half", () => {
		for (const game of games) {
			const tl = stage(game);
			const bad: string[] = [];
			let prev: (typeof tl.plays)[number] | undefined;
			for (const play of tl.plays) {
				const man = (id: number) => play.chains.find((c) => c.pid === id)!;
				const [box, rear, front, aux] = [-8, -9, -10, -11].map(man);
				if (!box || !rear || !front || !aux) {
					bad.push(`play ${play.i}: ${play.chains.length} in the chain crew`);
					continue;
				}
				const far = play.quarter <= 2;
				for (const m of [box, rear, front]) {
					const y = posAt(m, play.snap).y;
					if (far ? y >= 0 : y <= FIELD_W) {
						bad.push(`play ${play.i}: ${m.pid} at y ${y.toFixed(0)}`);
					}
				}
				const auxY = posAt(aux, play.snap).y;
				if (far ? auxY <= FIELD_W : auxY >= 0) {
					bad.push(`play ${play.i}: the other box at y ${auxY.toFixed(0)}`);
				}
				if (play.gainX === undefined) {
					prev = play;
					continue;
				}
				const at = (m: { legs: Leg[] }) => posAt(m, play.snap).x;
				if (Math.abs(at(box) - play.losX) > 0.01) {
					bad.push(
						`play ${play.i}: the box at ${at(box)}, the line at ${play.losX}`,
					);
				}
				if (Math.abs(at(front) - play.gainX) > 0.01) {
					bad.push(`play ${play.i}: the front rod off the line to gain`);
				}
				if (Math.abs((at(front) - at(rear)) * play.dir - 30) > 0.01) {
					bad.push(`play ${play.i}: the rods not ten yards apart`);
				}
				for (const m of [box, rear, front]) {
					if (legAt(m, play.snap).move !== "holdPole") {
						bad.push(`play ${play.i}: ${m.pid} not holding his pole up`);
					}
				}
				// (Nothing to move the chain for, it stays put.)
				if (
					prev?.quarter === play.quarter &&
					prev.gainX === play.gainX &&
					[rear, front].some((m) =>
						m.legs.some(
							(l) =>
								l.t0 < play.snap &&
								(l.from.x !== l.to.x || l.from.y !== l.to.y),
						),
					)
				) {
					bad.push(`play ${play.i}: the chain moved, the line to gain didn't`);
				}
				prev = play;
			}
			assert.deepEqual(bad.slice(0, 8), []);
		}
	}, 20_000);
});

describe("pre-snap motion", () => {
	test("now and then on a pass play, one man off the line going across at the snap - never toward the line", () => {
		let passes = 0;
		let moving = 0;
		for (const game of games) {
			const tl = stage(game);
			for (const play of tl.plays) {
				if (play.kind !== "pass" && play.kind !== "run") {
					continue;
				}
				const going = play.parts.filter((p) => {
					const l = legAt(p, play.snap - 1);
					return (
						p.team === play.offense &&
						l.t1 > play.snap + 50 &&
						(l.from.x !== l.to.x || l.from.y !== l.to.y)
					);
				});
				// (A run: only the man going round for it on a sweep.)
				assert.isAtMost(going.length, 1, `play ${play.i}`);
				passes += play.kind === "pass" ? 1 : 0;
				const him = going[0];
				if (!him) {
					continue;
				}
				const l = legAt(him, play.snap - 1);
				const depth = (x: number) => (play.losX - x) * play.dir;
				assert.isAtLeast(
					depth(l.to.x),
					depth(l.from.x) - 0.01,
					`play ${play.i}: toward the line`,
				);
				assert.isAbove(depth(l.to.x), 3, `play ${play.i}: on the line`);
				if (play.kind === "pass") {
					moving += 1;
					assert.strictEqual(l.from.x, l.to.x, `play ${play.i}: not across`);
				}
			}
		}
		assert.isAbove(moving / passes, 0.15);
		assert.isBelow(moving / passes, 0.6);
	}, 20_000);
});

describe("the sidelines", () => {
	test("everyone not in for the snap on his own sideline, in his team's area - the home team's across the field", () => {
		for (const game of games) {
			const tl = stage(game);
			const men = sidelineFor(game.roster, 1);
			const teamOf = new Map(game.roster.map((p) => [p.pid, p.team]));
			const bad: string[] = [];
			for (const play of tl.plays.filter((_, k) => k % 7 === 0)) {
				const m = momentAt(tl, play.snap);
				const on = new Set(play.parts.map((p) => p.pid));
				const off = sidelineAt(men, tl, m);
				const seen = new Set<number>();
				for (const st of off) {
					if (on.has(st.id) || seen.has(st.id)) {
						bad.push(`play ${play.i}: ${st.id} on the field and the sideline`);
					}
					seen.add(st.id);
					const far = st.y < 0;
					if (far !== (st.team === 1) || (st.y >= -6 && st.y <= FIELD_W + 6)) {
						bad.push(`play ${play.i}: ${st.id} at y ${st.y.toFixed(0)}`);
					}
					if (st.x < TEAM_AREA[0] || st.x > TEAM_AREA[1]) {
						bad.push(`play ${play.i}: ${st.id} out of the team area`);
					}
				}
				for (const [pid, team] of teamOf) {
					if (!on.has(pid) && !seen.has(pid)) {
						bad.push(`play ${play.i}: ${pid} (team ${team}) nowhere`);
					}
				}
			}
			assert.deepEqual(bad.slice(0, 8), []);
		}
	}, 20_000);
});

describe("the sidelines, packed", () => {
	test("the whole team and its staff on hand, nobody stood on anybody, most of the unit just off sat down", () => {
		for (const game of games) {
			const tl = stage(game);
			const men = sidelineFor(game.roster, 1);
			let sat = 0;
			let resting = 0;
			for (const play of tl.plays.filter((_, k) => k % 5 === 0)) {
				const off = sidelineAt(men, tl, momentAt(tl, play.snap));
				for (const [k, a] of off.entries()) {
					for (const b of off.slice(k + 1)) {
						assert.isAtLeast(
							Math.hypot(a.x - b.x, a.y - b.y),
							1.5,
							`play ${play.i}: ${a.id} on ${b.id}`,
						);
					}
				}
				const on = new Set(play.parts.map((p) => p.pid));
				for (const team of [0, 1] as const) {
					const here = off.filter((s) => s.team === team);
					const players = game.roster.filter(
						(p) => p.team === team && !on.has(p.pid),
					);
					// (Every man off the field, and the coaches and the staff.)
					assert.isAtLeast(here.length, players.length + 10);
				}
				if (play.kind === "pass" || play.kind === "run") {
					sat += off.filter((s) => s.move === "sit").length;
					resting += 1;
				}
			}
			assert.isAbove(sat / resting, 8);
		}
	}, 80_000);
});

describe("a long drive", () => {
	// Out there a dozen snaps in a row and more, no break between - no
	// timeout, no quarter over - a defense gets its breath after the
	// whistle: men bent over, hands on their knees. Fresh, none of it.
	test("the defense worn, bent over getting its breath - and fresh, not", () => {
		let worn = 0;
		let fresh = 0;
		for (const game of games) {
			const tl = stage(game);
			let drive: { offense: number; snaps: number } | undefined;
			let quarter = 1;
			for (const p of tl.plays) {
				if (p.quarter !== quarter) {
					quarter = p.quarter;
					drive = undefined;
				}
				if (p.kind === "run" || p.kind === "pass" || p.kind === "kneel") {
					drive = {
						offense: p.offense,
						snaps: drive?.offense === p.offense ? drive.snaps + 1 : 1,
					};
				} else if (p.kind !== "none") {
					drive = undefined;
				}
				const bent = p.parts.filter(
					(part) =>
						part.team !== p.offense &&
						part.legs.some(
							(l) => l.move === "winded" && l.t0 >= p.whistle && l.t0 < p.t1,
						),
				).length;
				if (!p.timeout && !p.measure && drive && p.kind !== "none") {
					if (drive.snaps >= 11) {
						worn += bent;
					} else if (drive.snaps <= 5) {
						fresh += 1;
						assert.strictEqual(bent, 0, `${p.i}`);
					}
				}
				if (p.timeout) {
					drive = undefined;
				}
			}
		}
		assert.isAbove(fresh, 50);
		assert.isAbove(worn, 3);
	}, 20_000);
});

describe("a man hurt", () => {
	test("down after the whistle, the trainers out to him from his sideline, and helped off toward it", () => {
		let hurt = 0;
		for (const game of games) {
			const tl = stage(game);
			for (const play of tl.plays) {
				if (!play.hurt) {
					continue;
				}
				hurt += 1;
				const him = play.parts.find((p) => p.pid === play.hurt!.pid)!;
				const far = him.team === 1;
				const off = (y: number) => (far ? y < 0 : y > FIELD_W);
				assert.isAtLeast(play.hurt.from, play.whistle, `play ${play.i}`);
				const down = momentAt(tl, play.hurt.from + 100).players.find(
					(p) => p.id === him.pid,
				);
				assert.include(
					["hurt", "hurtProne", "tackled", "tackledBack"],
					down?.move,
					`play ${play.i}`,
				);
				const at = legAt(him, play.hurt.from + 100).to;
				const medics = play.medics ?? [];
				assert.isAbove(medics.length, 0, `play ${play.i}`);
				for (const m of medics) {
					assert.isTrue(off(m.legs[0]!.from.y), `play ${play.i}: ${m.pid}`);
					const tend = m.legs.find((l) => l.move === "tend");
					assert.isAtMost(
						Math.hypot(tend!.to.x - at.x, tend!.to.y - at.y),
						4,
						`play ${play.i}: ${m.pid}`,
					);
				}
				const last = him.legs.at(-1)!;
				assert.isTrue(off(last.to.y), `play ${play.i}: off the field`);
				assert.isBelow(play.t1 - play.whistle, 30_000, `play ${play.i}`);
				assert.isAtLeast(
					momentAt(tl, play.t1 - 1).players.length,
					22 + medics.length,
					`play ${play.i}`,
				);
			}
		}
		assert.isAbove(hurt, 0);
	}, 20_000);
});

describe("the TV cameras", () => {
	test("a man on each sideline, off the field, after the ball at a man's speed", () => {
		const bad: string[] = [];
		for (const game of games) {
			const tl = stage(game);
			for (const play of tl.plays) {
				if (play.tv.length !== 2) {
					bad.push(`play ${play.i}: ${play.tv.length} cameramen`);
				}
				for (const m of play.tv) {
					m.legs.forEach((l, k) => {
						const prev = m.legs[k - 1];
						if (prev && Math.abs(l.t0 - prev.t1) > 1e-6) {
							bad.push(`play ${play.i}: ${m.pid} gap at ${l.t0}`);
						}
						if (l.to.y >= 0 && l.to.y <= FIELD_W) {
							bad.push(`play ${play.i}: ${m.pid} on the field`);
						}
						const secs = (l.t1 - l.t0) / 1000;
						if (secs > 0.05 && Math.hypot(l.to.x - l.from.x) / secs > 24) {
							bad.push(`play ${play.i}: ${m.pid} too fast`);
						}
					});
					if (m.legs.at(-1)!.t1 < play.t1 - 1e-6) {
						bad.push(`play ${play.i}: ${m.pid} gone before the end`);
					}
				}
			}
		}
		assert.deepEqual(bad.slice(0, 8), []);
	}, 20_000);
});

describe("the game over", () => {
	test("everyone on the field shakes hands with one of the other side's, and the head coaches out to shake in the middle", () => {
		for (const game of games) {
			const tl = stage(game);
			const play = tl.plays.at(-1)!;
			const post = play.postgame!;
			assert.isDefined(post, "a postgame");
			// (The star's interview in it, after the head coaches.)
			assert.isBelow(play.t1 - play.whistle, 35_000);
			for (const part of play.parts) {
				const shakes = part.legs.filter((l) => l.move === "shake");
				assert.isAbove(shakes.length, 0, `${part.pid} shook nobody's hand`);
				for (const l of shakes) {
					const t = (l.t0 + l.t1) / 2;
					const other = play.parts.find(
						(p) =>
							p.team !== part.team &&
							legAt(p, t).move === "shake" &&
							Math.hypot(legAt(p, t).to.x - l.to.x, legAt(p, t).to.y - l.to.y) <
								4,
					);
					assert.isDefined(other, `${part.pid}'s hand shaken by nobody`);
				}
			}
			// (The head coaches - and, under the cooler, the two men with it.)
			const coaches = (play.coaches ?? []).filter((c) => c.pid < 0);
			assert.lengthOf(coaches, 2);
			const [a, b] = coaches.map((c) => legAt(c, post.shake + 200));
			assert.equal(a!.move, "shake");
			assert.equal(b!.move, "shake");
			assert.isBelow(Math.hypot(a!.to.x - b!.to.x, a!.to.y - b!.to.y), 4);
			// (Out on the field, not on the sideline as well.)
			const off = sidelineAt(
				sidelineFor(game.roster, 1),
				tl,
				momentAt(tl, post.shake),
			);
			for (const c of play.coaches ?? []) {
				assert.isUndefined(off.find((s) => s.id === c.pid));
			}
		}
	}, 20_000);

	test("the hands all shaken, the picture cut away to the whole stadium - and held on it, the game over", () => {
		for (const game of games) {
			const tl = stage(game);
			const play = tl.plays.at(-1)!;
			const post = play.postgame!;
			assert.isAbove(post.look, post.shake);
			assert.include(tl.cuts, post.look);
			assert.isAtLeast(play.t1 - post.look, 3000);
			assert.strictEqual(tl.beats.at(-1)!.end, play.t1);
			assert.isBelow(aimFor(momentAt(tl, post.look - 50)).width, 150);
			const last = aimFor(momentAt(tl, play.t1));
			assert.strictEqual(last.x, MID_X);
			assert.isAbove(last.width, FIELD_L);
		}
	}, 20_000);

	test("a playoff round won: the winners' sideline out onto the field at once, at a run, and the building up for it all the way - the championship's with confetti", () => {
		let clinched = 0;
		for (const game of games) {
			const staged = (toWin?: number) =>
				compileField({
					events: game.events,
					players: game.roster,
					gid: 1,
					...(toWin ? { stakes: { won: [0, 0], toWin, finals: true } } : {}),
				});
			const plain = staged();
			const won = staged(1);
			const play = won.plays.at(-1)!;
			const post = play.postgame!;
			assert.isUndefined(plain.plays.at(-1)!.postgame!.clinch);
			// (A series not won yet.)
			assert.isUndefined(staged(4).plays.at(-1)!.postgame!.clinch);
			if (post.winner === undefined) {
				assert.isUndefined(post.clinch);
				continue;
			}
			clinched += 1;
			assert.deepEqual(post.clinch, { title: true });
			assert.equal(play.cheer?.side, post.winner);
			assert.isAtLeast(post.from + play.cheer!.hold!, post.look);
			const squad = sidelineFor(game.roster, 1);
			const running = (tl: FieldTimeline) =>
				sidelineAt(squad, tl, momentAt(tl, post.from + 2500)).filter(
					(s) => s.team === post.winner && s.move === "run",
				).length;
			assert.isAbove(running(won), 15);
			assert.isBelow(running(plain), running(won) / 3);
		}
		assert.isAbove(clinched, 0);
	}, 60_000);

	test("the winners' head coach under the cooler, the game won: two of his men off the field with it, after the whistle", () => {
		let showers = 0;
		for (const [k, game] of games.entries()) {
			for (const gid of [1, 2, 3, 4, 5, 6]) {
				const tl = stage(game, gid);
				const play = tl.plays.at(-1)!;
				const post = play.postgame!;
				const sh = post.shower;
				if (!sh) {
					continue;
				}
				showers += 1;
				const at = `game ${k} (${gid})`;
				assert.equal(sh.team, post.winner, at);
				assert.isAbove(sh.dump, play.whistle, at);
				const men = (play.coaches ?? []).filter((c) => sh.men.includes(c.pid));
				assert.lengthOf(men, 2, at);
				for (const m of men) {
					assert.equal(m.team, sh.team, at);
					assert.isUndefined(
						play.parts.find((p) => p.pid === m.pid),
						`${at}: ${m.pid} on the field`,
					);
					assert.equal(legAt(m, sh.dump + 500).move, "coolerDump", at);
				}
				const coach = (play.coaches ?? []).find((c) => c.pid === sh.coach)!;
				const doused = coach.legs.find((l) => l.move === "doused");
				assert.isDefined(doused, at);
				assert.isAbove(doused!.t0, sh.dump, at);
				assert.isBelow(doused!.t1, post.shake, at);
			}
		}
		assert.isAbove(showers, 0);
	}, 120_000);
});

describe("the head coach", () => {
	test("pacing his spot, stood a new way every so often - on edge in a tight finish - clapping his side's big plays, arms out at a call against it now and then, into each over a moment", () => {
		const bosses = new Set([coachId(0, 0), coachId(1, 0)]);
		const ways = new Set<string>();
		const bad: string[] = [];
		let claps = 0;
		let calls = 0;
		let protests = 0;
		for (const game of games) {
			const tl = stage(game);
			const men = sidelineFor(game.roster, 1);
			const at = (t: number) =>
				sidelineAt(men, tl, momentAt(tl, t)).filter((s) => bosses.has(s.id));
			for (const play of tl.plays) {
				if (play.postgame || play.timeout) {
					continue;
				}
				// (Never from one way to another in a frame, nor anywhere in
				// one but by a step - stepping out of the way of a man run
				// into them, off his line, aside for that.)
				let last = new Map<number, FieldPlayerState>();
				for (let t = play.t0; t < play.t1; t += 80) {
					const now = new Map<number, FieldPlayerState>();
					for (const s of at(t)) {
						now.set(s.id, s);
						ways.add(s.move);
						const was = last.get(s.id);
						if (!was) {
							continue;
						}
						if (was.move !== s.move && (!s.from || s.from.w < 0.7)) {
							bad.push(`play ${play.i}: ${s.id} ${was.move} to ${s.move}`);
						}
						const home = headCoachAt(s.team, play.i).y;
						if (s.y === home && was.y === home && Math.abs(s.x - was.x) > 0.4) {
							bad.push(`play ${play.i}: ${s.id} jumped`);
						}
						if (
							(play.tense ?? 0) >= 1 &&
							(s.move === "armsFolded" || s.move === "stand")
						) {
							bad.push(`play ${play.i}: ${s.id} at ease, the game on the line`);
						}
					}
					last = now;
				}
				const c = play.cheer;
				if (c && c.t + 1000 < play.t1) {
					const s = at(c.t + 1000).find((s) => s.team === c.side);
					if (s) {
						claps += 1;
						assert.strictEqual(s.move, "clap", `play ${play.i}`);
					}
				}
				for (const call of play.calls ?? []) {
					const s = at(call.t + 600).find((s) => s.team === call.side);
					if (s && cheerless(play, call.t)) {
						calls += 1;
						if (s.move === "protest") {
							protests += 1;
						}
					}
				}
			}
		}
		assert.deepEqual(bad.slice(0, 8), []);
		for (const way of ["armsFolded", "hands", "onHips", "coachTalk", "walk"]) {
			assert.isTrue(ways.has(way), way);
		}
		assert.isAbove(claps, 10);
		assert.isAbove(protests, 0);
		assert.isBelow(protests, calls);
	}, 120_000);

	// Now and then before the home side's third down, the picture on its head
	// coach across the field calling it in, the play sheet up over his mouth
	// - on his spot for it, and on to the next play from there.
	test("taking a big play: a fist pumped, or his hands on his head", () => {
		let shots = 0;
		for (const game of games) {
			const tl = stage(game);
			const men = sidelineFor(game.roster, 1);
			for (const play of tl.plays) {
				const react = play.coachReact;
				if (!react) {
					continue;
				}
				shots += 1;
				const at = `play ${play.i}`;
				assert.isDefined(play.party, at);
				assert.isUndefined(play.callIn, at);
				assert.closeTo(react.t0, play.party!.until + 500, 0.01, at);
				assert.closeTo(react.t1 - react.t0, 1800, 0.01, at);
				assert.include(tl.cuts, react.t0, at);
				assert.include(tl.cuts, react.t1, at);
				assert.isAtLeast(play.t1, react.t1 + 900, at);
				// (The replay straight after him, not over him.)
				if (play.replay) {
					assert.isAtLeast(play.replay.cue, react.t1, at);
				}
				const shot = aimFor(momentAt(tl, react.t0 + 100));
				assert.isBelow(shot.width, 20, at);
				assert.closeTo(shot.x, react.at.x, 0.01, at);
				const him = sidelineAt(men, tl, momentAt(tl, react.t0 + 600)).find(
					(s) => s.id === coachId(1, 0),
				)!;
				assert.include(
					react.glad ? ["fistPump", "clap"] : ["handsOnHelmet"],
					him.move,
					at,
				);
				assert.isBelow(
					Math.hypot(him.x - react.at.x, him.y - react.at.y),
					2,
					at,
				);
			}
		}
		assert.isAbove(shots, 2);
	}, 120_000);

	test("calling in a third down, the play sheet up over his mouth", () => {
		let shots = 0;
		for (const game of games) {
			const tl = stage(game);
			const men = sidelineFor(game.roster, 1);
			for (const [k, play] of tl.plays.entries()) {
				const call = play.callIn;
				if (!call) {
					continue;
				}
				shots += 1;
				const next = tl.plays[k + 1]!;
				assert.strictEqual(next.offense, 1, `${play.i}`);
				assert.match(next.situation, /^3rd/, `${play.i}`);
				assert.include(tl.cuts, call.t0, `${play.i}`);
				assert.strictEqual(call.t1, play.t1, `${play.i}`);
				assert.isAbove(call.t0, play.whistle, `${play.i}`);
				assert.isUndefined(play.replay, `${play.i}`);
				const t = call.t0 + 200;
				const m = momentAt(tl, t);
				const shot = aimFor(m);
				assert.isBelow(shot.width, 20, `${play.i}`);
				assert.closeTo(shot.x, call.at.x, 0.01, `${play.i}`);
				const him = sidelineAt(men, tl, m).find((s) => s.id === coachId(1, 0))!;
				assert.strictEqual(him.move, "callIn", `${play.i}`);
				// (In the shot - over a step, if a man out of bounds by him has
				// him out of the way.)
				assert.closeTo(him.x, call.at.x, 2, `${play.i}`);
				assert.closeTo(him.y, call.at.y, 2, `${play.i}`);
			}
		}
		assert.isAbove(shots, 2);
	}, 120_000);
});

// (Nobody's cheer up then.)
const cheerless = (play: Play, t: number) =>
	!play.cheer || t < play.cheer.t || t > play.cheer.t + 6000;

// As the visitors' drive starts, now and then, the home side's quarterback
// off the field over the tablet with his coordinator: the picture on the two
// of them, nobody between, and out wide again a while before the snap.
describe("the quarterback over the tablet", () => {
	test("after his drive, with his coordinator, in the picture", () => {
		let shots = 0;
		for (const game of games) {
			const tl = stage(game);
			const men = sidelineFor(game.roster, 1);
			for (const [k, play] of tl.plays.entries()) {
				const tab = play.tablet;
				if (!tab) {
					continue;
				}
				shots += 1;
				const at = `play ${play.i}`;
				assert.strictEqual(play.offense, 0, at);
				assert.include(["pass", "run"], play.kind, at);
				const before = tl.plays
					.slice(0, k)
					.findLast((p) => ["pass", "run", "kneel"].includes(p.kind))!;
				assert.strictEqual(before.offense, 1, at);
				assert.strictEqual(
					before.parts.find((p) => p.team === 1 && p.role === "QB")?.pid,
					tab.pid,
					at,
				);
				assert.isFalse(
					play.parts.some((p) => p.pid === tab.pid),
					at,
				);
				assert.strictEqual(tab.t0, play.t0, at);
				assert.isAtMost(tab.t1, play.snap - 1800, at);
				assert.include(tl.cuts, tab.t1, at);
				const mid = {
					x: (tab.at.x + tab.oc.x) / 2,
					y: (tab.at.y + tab.oc.y) / 2,
				};
				const shot = aimFor(momentAt(tl, tab.t0 + 200));
				assert.isBelow(shot.width, 20, at);
				assert.closeTo(shot.x, mid.x, 0.01, at);
				// (Out wide again after.)
				assert.isAbove(aimFor(momentAt(tl, tab.t1 + 50)).width, 40, at);
				for (let t = tab.t0; t < tab.t1; t += 200) {
					const m = momentAt(tl, t);
					const side = sidelineAt(men, tl, m);
					const qb = side.find((s) => s.id === tab.pid)!;
					assert.strictEqual(qb.move, "tablet", at);
					assert.isTrue(qb.bare, at);
					assert.isBelow(Math.hypot(qb.x - tab.at.x, qb.y - tab.at.y), 2, at);
					const oc = side.find((s) => s.id === coachId(1, 1))!;
					assert.strictEqual(oc.move, "coachTalk", at);
					assert.isBelow(Math.hypot(oc.x - tab.oc.x, oc.y - tab.oc.y), 2, at);
					for (const p of m.players) {
						const d = p.y - mid.y;
						assert.isFalse(
							Math.abs(p.x - mid.x) < 4.5 && d > -1.5 && d < TABLET_CAM_FT,
							`${at} ${p.id} @${t - tab.t0}`,
						);
					}
				}
			}
		}
		assert.isAbove(shots, 0);
	}, 60_000);
});

describe("the sidelines, up for a big play", () => {
	test("up off the line for it - jumping, an arm up - and back down, into it and out of it over a moment, never in a frame", () => {
		const bosses = new Set([coachId(0, 0), coachId(1, 0)]);
		const bad: string[] = [];
		let ups = 0;
		const game = games[0]!;
		const tl = stage(game);
		const men = sidelineFor(game.roster, 1);
		for (const play of tl.plays.filter((p) => p.cheer && !p.postgame)) {
			const c = play.cheer!;
			let last = new Map<number, FieldPlayerState>();
			for (let t = c.t; t < Math.min(play.t1, c.t + 8000); t += 50) {
				const now = new Map<number, FieldPlayerState>();
				for (const s of sidelineAt(men, tl, momentAt(tl, t))) {
					now.set(s.id, s);
					const was = last.get(s.id);
					if (!was || bosses.has(s.id) || s.team !== c.side) {
						continue;
					}
					if (s.move === "leap" || s.arms?.move === "ballUp") {
						ups += 1;
					}
					if (
						(was.move !== s.move || was.arms?.move !== s.arms?.move) &&
						(!s.from || s.from.w < 0.7)
					) {
						bad.push(
							`play ${play.i}: ${s.id} ${was.move}/${was.arms?.move} to ${s.move}/${s.arms?.move}`,
						);
					}
					if (was.move === "leap" && s.move !== "leap" && was.z > 0.6) {
						bad.push(`play ${play.i}: ${s.id} down out of the air`);
					}
				}
				last = now;
			}
		}
		assert.deepEqual(bad.slice(0, 8), []);
		assert.isAbove(ups, 100);
	}, 120_000);
});
