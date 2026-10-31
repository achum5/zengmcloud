import { assert, beforeAll, describe, test } from "vitest";
import processLiveGameEvents, {
	DEFAULT_SPORT_STATE,
	type SportState,
} from "../../../util/processLiveGameEvents.football.tsx";
import {
	compileField,
	isLineItem,
	targetForCursor,
	type FieldTimeline,
	type Leg,
} from "./director.ts";
import { momentAt } from "./evaluate.ts";
import { dirFor, FIELD_L, FIELD_W, TEAM_AREA, yardLineX } from "./geometry.ts";
import { makeFieldClock, REPLAY_SPEED, stepField } from "./playback.ts";
import { sidelineAt, sidelineFor } from "./sideline.ts";
import { stageField } from "./stageField.ts";
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
	test("a score, a pick, a long gain shown again once the play is over", () => {
		let shown = 0;
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
	}, 20_000);

	test("the live picture held while one shows, slowed down - and not on fast", () => {
		const tl = stage(games[0]!);
		const play = tl.plays.find((p) => p.replay)!;
		const r = play.replay!;
		const input = {
			cursor: 1e9,
			paused: false,
			rate: 1,
			follower: true,
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
			assert.isBelow(play.t1 - play.whistle, 30_000);
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
