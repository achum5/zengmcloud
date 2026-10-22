import { assert, beforeAll, describe, test } from "vitest";
import GameSim from "../../../../worker/core/GameSim.football/index.ts";
import loadTeams from "../../../../worker/core/game/loadTeams.ts";
import { player, team } from "../../../../worker/core/index.ts";
import { g, helpers } from "../../../../worker/util/index.ts";
import { resetCache, resetG } from "../../../../test/helpers.ts";
import { DEFAULT_LEVEL } from "../../../../common/budgetLevels.ts";
import { range } from "../../../../common/utils.ts";
import processLiveGameEvents, {
	DEFAULT_SPORT_STATE,
	type SportState,
} from "../../../util/processLiveGameEvents.football.tsx";
import {
	compileField,
	isLineItem,
	targetForCursor,
	type FieldTimeline,
	type RawEvent,
} from "./director.ts";
import { dirFor, FIELD_L, FIELD_W, yardLineX } from "./geometry.ts";
import { GROUPS } from "./personnel.ts";
import type { RosterMan } from "./personnel.ts";

// Real games, simulated: whatever the sim says, the field has to stage.
type Game = { events: RawEvent[]; roster: RosterMan[] };
const games: Game[] = [];

beforeAll(async () => {
	resetG();
	g.setWithoutSavingToDB("season", 2013);
	const teamsDefault = helpers.getTeamsDefault().slice(0, 2);
	await resetCache({
		players: [
			...range(53).map(() => player.generate(0, 25, 2010, true, DEFAULT_LEVEL)),
			...range(53).map(() => player.generate(1, 25, 2010, true, DEFAULT_LEVEL)),
		],
		teams: teamsDefault.map(team.generate),
		teamSeasons: teamsDefault.map((t) => team.genSeasonRow(t)),
		teamStats: teamsDefault.map((t) => team.genStatsRow(t.tid)),
	});
	for (const gid of [11, 12, 13]) {
		const teams = await loadTeams([0, 1], {});
		for (const t of [teams[0], teams[1]]) {
			// Each group's chart, best first - the way a team's own is set.
			const score = (p: any, pos: string) =>
				(p.ovrs[pos] ?? 0) + (p.pos === pos ? 15 : 0);
			const depth: any = {};
			for (const pos of GROUPS) {
				depth[pos] = [...t.player].sort(
					(a: any, b: any) => score(b, pos) - score(a, pos),
				);
			}
			t.depth = depth;
		}
		const sim = new GameSim({
			gid,
			teams: [teams[0], teams[1]],
			baseInjuryRate: g.get("injuryRate"),
			doPlayByPlay: true,
			homeCourtFactor: 1,
			allStarGame: false,
			neutralSite: false,
		});
		const events = sim.run().playByPlay as RawEvent[];
		// (Raw team 0 is the home team: display side 1.)
		const raw = events[0]!.boxScore;
		const roster: RosterMan[] = [];
		for (const rt of [0, 1] as const) {
			for (const p of raw[rt].player) {
				roster.push({ pid: p.id, team: rt === 0 ? 1 : 0, pos: p.pos });
			}
		}
		games.push({ events, roster });
	}
}, 120_000);

const stage = ({ events, roster }: Game, gid = 1): FieldTimeline =>
	compileField({ events, players: roster, gid });

describe("compileField", () => {
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
	});

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
					for (const leg of part.legs) {
						for (const p of [leg.from, leg.to]) {
							assert.isTrue(
								p.x >= 0 && p.x <= FIELD_L && p.y >= 0 && p.y <= FIELD_W,
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
	});

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
					const set = part.legs.at(-1)!.to;
					const ahead = play.dir * (set.x - play.losX);
					if (part.team === play.offense) {
						assert.isAtMost(ahead, 0, `${part.role} offside`);
					} else {
						assert.isAtLeast(ahead, 0, `${part.role} offside`);
					}
				}
			}
		}
	});

	test("the quarterback in the gun or under center, the ball in the center's hand", () => {
		const tl = stage(games[0]!);
		for (const play of tl.plays) {
			if (play.kind !== "pass") {
				continue;
			}
			const qb = play.parts.find((p) => p.role === "QB")!;
			const center = play.parts.find((p) => p.role === "C")!;
			const back = -play.dir * (qb.legs.at(-1)!.to.x - play.losX);
			assert.isTrue(back > 2 && back < 16, `QB ${back} ft back`);
			assert.strictEqual(center.legs.at(-1)!.ball, "snap");
			assert.isTrue(
				play.ball.some((b) => b.kind === "held" && b.pid === center.pid),
			);
		}
	});

	test("a game takes about as long to watch as a broadcast", () => {
		for (const game of games) {
			const minutes = stage(game).end / 60_000;
			assert.isTrue(minutes > 15 && minutes < 45, `${minutes} minutes`);
		}
	});

	test("the same game is staged the same way every time", () => {
		const a = stage(games[1]!, 5);
		const b = stage(games[1]!, 5);
		assert.deepStrictEqual(a, b);
	});
});
