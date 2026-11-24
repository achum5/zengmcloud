// WIN PROBABILITY CALIBRATION HARNESS - how ui/views/LiveGame/winProb.ts was
// measured against the engine, kept runnable so it can be measured again when
// the engine changes.
//
// Skipped unless WIN_PROB_CALIBRATION is set (to a folder to write to). It
// makes a league of random players and plays WIN_PROB_CALIBRATION_GAMES
// games between its teams (default 20,000: about three minutes), play-by-play
// on. Then, for the model as shipped, read the way the pages read it - the
// charts from the score alone, every two seconds; the key plays from every
// event - it prints how often the team given each chance won, and the
// finish (winProb's FINISH) as this run measured it, ready to paste.
//
// With WIN_PROB_CALIBRATION_DUMP=1 it also writes how every game stood at
// the start of every possession (possessions.f32: rows of game, period,
// clock, team 0's points, team 1's, the team with the ball - float32) and
// how each ended (games.json). The curve's constants (FADE, LUCK,
// FINISH_LUCK, BALL, BALL_EARLY) were fit to that by maximum likelihood -
// the chance of the actual result at every possession start with more than
// 45 seconds left - over 160,000 games in three leagues, each league's
// spreads first lined up with its results (a new random league's spreads
// run a little off, and that is the spread's to fix, not this).
//
// What it found:
//   - The old curve (luck growing with the square root of the time left,
//     nothing else) was overconfident everywhere: of moments it gave a team
//     2-5%, that team won 10%; of its 90-95%, 86%. Leads in the sim fade - a
//     team 10 up at the half gives back about 2 of it on average - so a lead
//     is worth less than luck alone says.
//   - In the last two minutes it was far worse: its 2-5% won 13-15% of the
//     time, its 90-95% 82%; in overtime its 10-20% won 27%. Down 2 with 9
//     seconds left it said 0.3%: a team wins from there about a quarter of
//     the time with the ball, one time in twelve without.
//   - In the last minute the spread still counts, about 0.026 on the
//     log-odds a point (a better team's possessions, and its overtime).
//   - As shipped, on three leagues it was not fit to (30,000 games each):
//     every bin within 2.4 points over the game (the tip aside: that is the
//     spread's), 3.8 in the last minute, 5 in overtime (one league's home
//     teams did better there); down 2 with 5-15 seconds left in regulation,
//     the chart said 18%, and 19-20% won.
import { test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import GameSim from "../GameSim.basketball/index.ts";
import { processTeam } from "./loadTeams.ts";
import { createRandomPlayers } from "../league/create/createRandomPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { getGameSpread } from "../../../common/getGameSpread.ts";
import { pregameLineupSynergy } from "../GameSim.basketball/synergy.ts";
import { decodeScoring, encodeTimeline } from "../../util/gameTimeline.ts";
import { flowChance, momentsOf } from "../../../ui/views/LiveGame/winProb.ts";

const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const OUT = nodeEnv.WIN_PROB_CALIBRATION;
const GAMES = Number(nodeEnv.WIN_PROB_CALIBRATION_GAMES ?? 20_000);
const DUMP = nodeEnv.WIN_PROB_CALIBRATION_DUMP === "1";
const NUM_TEAMS = 30;

// Predicted-chance bins for the reliability tables.
const EDGES = [
	0, 0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98,
	1.0001,
];

class Reliability {
	n = EDGES.slice(1).map(() => 0);
	sumP = EDGES.slice(1).map(() => 0);
	won = EDGES.slice(1).map(() => 0);
	brier = 0;
	count = 0;
	add(p: number, y: number) {
		const k = EDGES.findIndex((e, i) => p >= e && p < EDGES[i + 1]!);
		this.n[k]! += 1;
		this.sumP[k]! += p;
		this.won[k]! += y;
		this.brier += (p - y) ** 2;
		this.count += 1;
	}
	print(label: string) {
		const rows = [
			`${label}  (n ${this.count}, Brier ${(this.brier / this.count).toFixed(4)})`,
		];
		let worst = 0;
		for (let k = 0; k < this.n.length; k++) {
			const n = this.n[k]!;
			if (n === 0) {
				continue;
			}
			const p = this.sumP[k]! / n;
			const a = this.won[k]! / n;
			if (n >= 200) {
				worst = Math.max(worst, Math.abs(a - p));
			}
			const lo = (100 * EDGES[k]!).toFixed(0).padStart(3);
			const hi = Math.min(100, 100 * EDGES[k + 1]!)
				.toFixed(0)
				.padStart(3);
			rows.push(
				`  ${lo}-${hi}%  predicted ${(100 * p).toFixed(1).padStart(5)}  actual ${(100 * a).toFixed(1).padStart(5)}  n ${n}`,
			);
		}
		rows.push(`  worst bin (n >= 200): ${(100 * worst).toFixed(1)} points`);
		console.log(rows.join("\n"));
	}
}

// THE FINISH, as measured: every half second of the last minute of the last
// period (and of each overtime), from the side with the ball - how far
// ahead it is, the spread from its side - and whether it won. Kept as
// counts per (FINISH_S node, margin, spread), a moment split between the two
// nodes either side of it in proportion.
const FINISH_S = [
	0, 1, 2, 3, 4.5, 6, 8, 10, 13, 16, 20, 25, 30, 36, 42, 48, 54, 60,
];
const FINISH_MARGIN = 6;

class Finish {
	// node -> "margin|spread" -> [weight, won]
	cells = FINISH_S.map(() => new Map<string, [number, number]>());
	add(secs: number, ahead: number, spread: number, won: number) {
		if (Math.abs(ahead) > FINISH_MARGIN || secs >= FINISH_S.at(-1)!) {
			return;
		}
		let k = 0;
		while (FINISH_S[k + 1]! <= secs) {
			k += 1;
		}
		const w = (secs - FINISH_S[k]!) / (FINISH_S[k + 1]! - FINISH_S[k]!);
		for (const [node, weight] of [
			[k, 1 - w],
			[k + 1, w],
		] as const) {
			const key = `${ahead}|${spread}`;
			const cell = this.cells[node]!.get(key) ?? [0, 0];
			cell[0] += weight;
			cell[1] += weight * won;
			this.cells[node]!.set(key, cell);
		}
	}
	// Per node: a logistic fit, one level per margin and one slope for the
	// spread; the levels then kept rising with the margin (pooled where
	// they don't).
	fit() {
		const K = 2 * FINISH_MARGIN + 1;
		const table = Array.from({ length: K }, () => FINISH_S.map(() => 0));
		const slopes: number[] = [];
		this.cells.forEach((cells, node) => {
			const rows = [...cells].map(([key, [w, won]]) => {
				const [d, s] = key.split("|").map(Number) as [number, number];
				return { i: d + FINISH_MARGIN, s, w, won };
			});
			const a = new Array<number>(K).fill(0);
			let c = 0;
			for (let iter = 0; iter < 50; iter++) {
				const ga = new Array<number>(K).fill(0);
				const ha = new Array<number>(K).fill(0);
				const hac = new Array<number>(K).fill(0);
				let gc = 0;
				let hcc = 0;
				for (const r of rows) {
					const p = 1 / (1 + Math.exp(-(a[r.i]! + c * r.s)));
					const g1 = r.won - r.w * p;
					const h = r.w * p * (1 - p);
					ga[r.i]! += g1;
					ha[r.i]! += h;
					hac[r.i]! += h * r.s;
					gc += g1 * r.s;
					hcc += h * r.s * r.s;
				}
				// (A weak pull to even, for a margin where every team won.)
				for (let i = 0; i < K; i++) {
					ga[i]! -= 0.05 * a[i]!;
					ha[i]! += 0.05;
				}
				// The slope first (the levels' block is diagonal), then each level.
				let num = gc;
				let den = hcc;
				for (let i = 0; i < K; i++) {
					num -= (hac[i]! * ga[i]!) / ha[i]!;
					den -= (hac[i]! * hac[i]!) / ha[i]!;
				}
				const dc = num / den;
				let biggest = Math.abs(dc);
				c += dc;
				for (let i = 0; i < K; i++) {
					const da = (ga[i]! - hac[i]! * dc) / ha[i]!;
					a[i]! += da;
					biggest = Math.max(biggest, Math.abs(da));
				}
				if (biggest < 1e-9) {
					break;
				}
			}
			slopes.push(c);
			// Rising with the margin (pool adjacent violators).
			const weights = new Array<number>(K).fill(0);
			for (const r of rows) {
				weights[r.i]! += r.w;
			}
			const blocks: { v: number; w: number; n: number }[] = [];
			for (let i = 0; i < K; i++) {
				blocks.push({
					v: 1 / (1 + Math.exp(-a[i]!)),
					w: weights[i]!,
					n: 1,
				});
				while (blocks.length > 1 && blocks.at(-2)!.v > blocks.at(-1)!.v) {
					const b2 = blocks.pop()!;
					const b1 = blocks.pop()!;
					const w = b1.w + b2.w;
					blocks.push({
						v: w > 0 ? (b1.v * b1.w + b2.v * b2.w) / w : (b1.v + b2.v) / 2,
						w,
						n: b1.n + b2.n,
					});
				}
			}
			let i = 0;
			for (const b of blocks) {
				for (let j = 0; j < b.n; j++) {
					table[i]![node] = Math.round(1000 * b.v);
					i += 1;
				}
			}
		});
		return { table, slopes };
	}
}

test.skipIf(!OUT || __SPORT !== "basketball")(
	"win probability vs the engine",
	{ timeout: 7_200_000 },
	async () => {
		const fs = await import(("node" + ":fs") as any);
		const path = await import(("node" + ":path") as any);
		fs.mkdirSync(OUT!, { recursive: true });

		resetG();
		g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
		g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		g.setWithoutSavingToDB("userTids", []);
		g.setWithoutSavingToDB("userTid", 0);
		const teams: any[] = [];
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			teams.push(
				team.generate({
					tid,
					cid: 0,
					did: 0,
					region: `Region${tid}`,
					name: `Name${tid}`,
					abbrev: `T${tid}`,
					pop: 1,
					imgURL: "",
				} as any),
			);
		}
		const players = await createRandomPlayers({
			activeTids: teams.map((t) => t.tid),
			onlyFreeAgents: false,
			scoutingLevel: DEFAULT_LEVEL,
			teams,
		});
		await resetCache({ players, teams, draftPicks: [] });
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			const t = (await idb.cache.teams.get(tid))!;
			await idb.cache.teamSeasons.add(team.genSeasonRow(t) as any);
		}
		for (const p of await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		])) {
			await player.updateValues(p);
			p.injury = { type: "Healthy", gamesRemaining: 0 };
			await idb.cache.players.put(p);
		}
		const sides: any[] = [];
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			const t = await idb.cache.teams.get(tid);
			const teamSeason = await idb.cache.teamSeasons.indexGet(
				"teamSeasonsBySeasonTid",
				[g.get("season"), tid],
			);
			const ps = await idb.cache.players.indexGetAll("playersByTid", tid);
			sides.push(await processTeam(t!, teamSeason!, ps));
		}
		const syn = sides.map((t) => pregameLineupSynergy(t.player));

		// Every possession's start: period, clock, score, who has the ball.
		const proto = GameSim.prototype as any;
		const original = proto.simPossession;
		let rows: number[] = [];
		proto.simPossession = function (this: any) {
			rows.push(
				this.team[0].stat.ptsQtrs.length,
				this.t,
				this.team[0].stat.pts,
				this.team[1].stat.pts,
				this.o === 1 ? 0 : 1,
			);
			return original.call(this);
		};

		const quarterLength = g.get("quarterLength");
		const numPeriods = g.get("numPeriods");

		const games: {
			home: number;
			away: number;
			spread: number;
			pts: [number, number];
			ot: number;
		}[] = [];
		const rel = {
			tip: new Reliability(),
			chart: new Reliability(),
			chartLate: new Reliability(),
			chartMinute: new Reliability(),
			chartOT: new Reliability(),
			moments: new Reliability(),
			momentsLate: new Reliability(),
		};
		// Down 2 with 5 to 15 seconds left in regulation, as the chart has it.
		const down2 = { n: 0, p: 0, won: 0 };
		const finish = new Finish();
		const dumped: number[] = [];
		const dumpFile = DUMP ? path.join(OUT!, "possessions.f32") : undefined;
		if (dumpFile) {
			fs.writeFileSync(dumpFile, new Uint8Array(0));
		}
		const started = Date.now();
		for (let k = 0; k < GAMES; k++) {
			rows = [];
			const home = Math.floor(Math.random() * NUM_TEAMS);
			let away = Math.floor(Math.random() * (NUM_TEAMS - 1));
			if (away >= home) {
				away += 1;
			}
			const spread = getGameSpread({
				ovr0: sides[home].ovr,
				ovr1: sides[away].ovr,
				homeCourtAdvantage: g.get("homeCourtAdvantage"),
				neutralSite: false,
				numPeriods,
				quarterLength,
				synergyDiff:
					syn[home] === undefined || syn[away] === undefined
						? undefined
						: syn[home]! - syn[away]!,
			})!;
			const result: any = new GameSim({
				gid: k,
				day: 1,
				teams: helpers.deepCopy([sides[home], sides[away]]) as any,
				doPlayByPlay: true,
				homeCourtFactor: 1,
				neutralSite: false,
				allStarGame: false,
				baseInjuryRate: 0,
			} as any).run();
			const pts: [number, number] = [
				result.team[0].stat.pts,
				result.team[1].stat.pts,
			];
			const y = pts[0] > pts[1] ? 1 : 0;
			games.push({ home, away, spread, pts, ot: result.overtimes });

			// As the key plays read it: every event.
			const events = result.playByPlay.slice(1);
			const moments = momentsOf(events, { numPeriods, spread });
			for (const m of moments.slice(0, -1)) {
				rel.moments.add(m.p0, y);
				if (m.period >= numPeriods && m.clock <= 120) {
					rel.momentsLate.add(m.p0, y);
				}
			}

			// As the charts read it: the score over the game, every two seconds.
			const tl = encodeTimeline(result.timeline);
			const periodEnds = tl.periodEnds;
			const total = periodEnds.at(-1)!;
			const scores: [number, number, number][] = [[0, 0, 0]];
			const now = [0, 0];
			for (const s of decodeScoring(tl.scoring)) {
				now[s.side]! += s.pts;
				scores.push([s.t, now[0]!, now[1]!]);
			}
			const regEnd = periodEnds[numPeriods - 1] ?? total;
			const chance = flowChance(
				{ scores, periodEnds, total },
				{ numPeriods, spread, over: true },
			);
			let i = 0;
			for (let t = 0; t < total; t += 2) {
				while (i + 1 < scores.length && scores[i + 1]![0] <= t) {
					i += 1;
				}
				const p = chance(t);
				rel.chart.add(p, y);
				if (t === 0) {
					rel.tip.add(p, y);
				}
				if (t >= regEnd - 120 && t < regEnd) {
					rel.chartLate.add(p, y);
				}
				if (t >= regEnd - 60 && t < regEnd) {
					rel.chartMinute.add(p, y);
				}
				if (t >= regEnd) {
					rel.chartOT.add(p, y);
				}
				const margin = scores[i]![1] - scores[i]![2];
				if (Math.abs(margin) === 2 && regEnd - t >= 5 && regEnd - t <= 15) {
					down2.n += 1;
					down2.p += margin < 0 ? p : 1 - p;
					down2.won += margin < 0 ? y : 1 - y;
				}
			}

			// The finish, a half second at a time, from the side with the ball.
			for (let r = 0; r < rows.length; r += 5) {
				const period = rows[r]!;
				if (period < numPeriods) {
					continue;
				}
				const from = rows[r + 1]!;
				const nextSame = r + 5 < rows.length && rows[r + 5] === period;
				const to = nextSame ? rows[r + 6]! : 0;
				const side = rows[r + 4]!;
				const ahead = (side === 0 ? 1 : -1) * (rows[r + 2]! - rows[r + 3]!);
				const won = side === 0 ? y : 1 - y;
				const sideSpread = side === 0 ? spread : -spread;
				for (
					let secs = Math.floor(Math.min(from, 60) * 2) / 2 + 0.25;
					secs > to && secs > 0;
					secs -= 0.5
				) {
					if (secs < from) {
						finish.add(secs, ahead, sideSpread, won);
					}
				}
			}

			if (dumpFile) {
				for (let r = 0; r < rows.length; r += 5) {
					dumped.push(k, ...rows.slice(r, r + 5));
				}
				if (dumped.length > 600_000) {
					fs.appendFileSync(dumpFile, new Float32Array(dumped));
					dumped.length = 0;
				}
			}
			if ((k + 1) % 10_000 === 0) {
				console.log(
					`${k + 1} games, ${((Date.now() - started) / 1000).toFixed(0)} s`,
				);
			}
		}
		proto.simPossession = original;
		if (dumpFile) {
			fs.appendFileSync(dumpFile, new Float32Array(dumped));
			fs.writeFileSync(
				path.join(OUT!, "games.json"),
				JSON.stringify({
					reg: numPeriods * quarterLength * 60,
					numPeriods,
					quarterLength,
					games,
				}),
			);
		}

		const margins = games.map((x) => x.pts[0] - x.pts[1] - x.spread);
		const mean = margins.reduce((a, b) => a + b, 0) / margins.length;
		const sd = Math.sqrt(
			margins.reduce((a, b) => a + (b - mean) ** 2, 0) / margins.length,
		);
		console.log(
			`${GAMES} games: final margin less the spread ${mean.toFixed(2)} on average, sd ${sd.toFixed(2)}; ${games.filter((x) => x.ot > 0).length} to overtime`,
		);
		rel.tip.print("At the tip");
		rel.chart.print("Chart (the score alone), every 2 s");
		rel.chartLate.print("Chart, last 2 min of regulation");
		rel.chartMinute.print("Chart, last minute of regulation");
		rel.chartOT.print("Chart, overtime");
		rel.moments.print("Key plays (every event)");
		rel.momentsLate.print("Key plays, last 2 min of regulation and overtime");
		console.log(
			`Down 2 with 5-15 s left (chart): predicted ${((100 * down2.p) / down2.n).toFixed(1)}%, won ${((100 * down2.won) / down2.n).toFixed(1)}% (n ${down2.n})`,
		);

		const { table, slopes } = finish.fit();
		console.log(
			`Finish, spread slope per node: ${slopes.map((s) => s.toFixed(3)).join(" ")}`,
		);
		console.log(
			[
				"const FINISH: number[][] = [",
				...table.map(
					(row, i) => `\t// ${i - FINISH_MARGIN}\n\t[${row.join(", ")}],`,
				),
				"];",
			].join("\n"),
		);
	},
);
