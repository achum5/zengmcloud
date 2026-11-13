import { PHASE } from "../../common/constants.ts";
import { makeSeededRandom } from "../../common/random.ts";
import type { DraftPick, Player } from "../../common/types.ts";
import { last } from "../../common/utils.ts";
import { team } from "../../worker/core/index.ts";
import { getContractSummaryOrThrow } from "../../worker/core/contract/index.ts";
import isUntradable from "../../worker/core/trade/isUntradable.ts";
import {
	playerLine,
	rosterPoints,
	type StrengthPlayer,
	type ValueRatings,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers, local } from "../../worker/util/index.ts";
import { tradeDifficulty } from "../../worker/util/difficulty.ts";
import { getHardCap } from "../../worker/util/getHardCap.ts";
import {
	inGameOrder,
	play,
	toSimTeam,
	type SimTeam,
} from "../impact/simTools.ts";
import type { BenchContext, BenchPolicy, TradeOffer } from "./policy.ts";

// AUTOPILOT: every decision the game's own, as for a user team on auto play
// (the bench's fallbacks), under the human rules. Its title rate is the
// bench's base rate.
export const autopilot = (): BenchPolicy => ({ name: "autopilot" });

// THE SHARK (src/test/shark/shark.test.ts), on the bench: an expert human
// hunting for trades the CPU misprices, with the game's own sim as ground
// truth, everything else on auto play. In the preseason and a third and two
// thirds of the way to the deadline it hunts:
//
//   1. Candidates, with every CPU team: 1-for-1, 2-for-1 and 1-for-2 among
//      each side's best healthy tradable players, legal under the trade
//      screen's salary rule, the hard cap and the roster limit.
//   2. Ranked by the strength model's read of what each does for the shark
//      this season (realisticFrontOffice/strength.ts); the best are put to
//      the CPU's own test (what trade.propose would answer), a pick of the
//      shark's added to the best refused ones.
//   3. The yeses are played out in a quick sim, the shark's roster before and
//      after against every other team on the same random numbers; the best
//      is played out again on fresh numbers and proposed if it helps by more
//      than twice the sim's noise.
//
// The original judged the realistic front offices' offers by their own
// pricing (priceFor, marginOf, judgeHumanOffer); here the judge is whatever
// trade.propose uses in the league, vanilla's value change by default.

export type SharkOptions = {
	seed: number;
	// Players a side that deals are built from.
	top?: number;
	// Candidates put to the CPU's test a hunt, at most.
	judgeBudget?: number;
	// CPU yeses played out in the quick sim, a hunt and a team.
	quickSims?: number;
	perTeam?: number;
	// Games a sim.
	screenGames?: number;
	decideGames?: number;
	// Deals a hunt.
	deals?: number;
};

type Stat = { mean: number; se: number; n: number };

const pairedStat = (after: number[], before: number[]): Stat => {
	const n = after.length;
	const d = after.map((x, i) => x - before[i]!);
	const mean = d.reduce((a, x) => a + x, 0) / n;
	const variance = d.reduce((a, x) => a + (x - mean) ** 2, 0) / (n - 1);
	return { mean, se: Math.sqrt(variance / n), n };
};

const healthy = (players: readonly Player[]) =>
	players.map((p) => {
		const copy = helpers.deepCopy(p);
		copy.injury = { type: "Healthy", gamesRemaining: 0 };
		return copy;
	});

const simTeamOf = (tid: number, players: readonly Player[]) =>
	toSimTeam(tid, inGameOrder(healthy(players)));

const lineCache = new Map<string, StrengthPlayer>();
const lineOf = (p: Player): StrengthPlayer => {
	const r = last(p.ratings);
	const key = `${p.pid}:${r.season}:${r.ovr}:${p.valueNoPot}`;
	let line = lineCache.get(key);
	if (!line) {
		line = {
			...playerLine(r as unknown as ValueRatings, r.pos),
			valueNoPot: p.valueNoPot,
		};
		lineCache.set(key, line);
	}
	return line;
};
const modelPoints = (players: readonly Player[]) =>
	rosterPoints(players.map(lineOf));

const amountOf = (p: Player) => getContractSummaryOrThrow(p).amount;

// The trade screen's salary rule and the secondary hard cap, for one side
// (trade/summary.ts), in thousands.
const salaryOk = (
	tid: number,
	payroll: number,
	outgoing: number,
	incoming: number,
) => {
	const after = payroll + incoming - outgoing;
	const overCap = after > g.get("salaryCap");
	const ratio =
		outgoing > 0
			? Math.floor((100 * incoming) / outgoing)
			: incoming > 0
				? Infinity
				: 100;
	if (
		g.get("salaryCapType") === "soft" &&
		overCap &&
		ratio > g.get("softCapTradeSalaryMatch")
	) {
		return false;
	}
	if (g.get("salaryCapType") === "hard" && overCap && after > payroll) {
		return false;
	}
	if (after > getHardCap(tid) && after > payroll) {
		return false;
	}
	return true;
};

// A cheap read of how the CPU prices a player in a deal (vanilla's
// ValueChangeCalculator: his value as a z-score, to the 7th power above 1),
// to choose which candidates to put to its real test.
const cpuTradeValue = (p: Player) => {
	const z = (p.value - local.playerOvrMean) / local.playerOvrStd;
	return z > 1 ? z ** 7 : z;
};

type Candidate = {
	cpuTid: number;
	give: Player[];
	givePicks: DraftPick[];
	get: Player[];
	modelShark: number;
	// The CPU's gain by cpuTradeValue, its own players marked up as against
	// a human.
	cpuRead: number;
	quick?: Stat;
};

export const shark = (options: SharkOptions): BenchPolicy => {
	const top = options.top ?? 8;
	const judgeBudget = options.judgeBudget ?? 80;
	const quickSims = options.quickSims ?? 8;
	const perTeam = options.perTeam ?? 2;
	const screenGames = options.screenGames ?? 150;
	const decideGames = options.decideGames ?? 400;
	const dealsPerHunt = options.deals ?? 3;
	let huntNo = 0;

	const hunt = async (ctx: BenchContext, label: string) => {
		huntNo += 1;
		const seedBase = (options.seed * 10_000 + huntNo) * 10;
		const sharkTid = ctx.tid;
		const numTeams = g.get("numTeams");
		const userTids = g.get("userTids");
		const inSeason = g.get("phase") === PHASE.REGULAR_SEASON;
		const maxRoster = g.get("maxRosterSize");

		const rosters = new Map<number, Player[]>();
		const payrolls = new Map<number, number>();
		for (let tid = 0; tid < numTeams; tid++) {
			rosters.set(
				tid,
				await idb.cache.players.indexGetAll("playersByTid", tid),
			);
			payrolls.set(tid, await team.getPayroll(tid));
		}
		const sharkRoster = rosters.get(sharkTid)!;
		const best = (players: Player[]) =>
			players
				.filter(
					(p) => p.injury.gamesRemaining === 0 && !isUntradable(p).untradable,
				)
				.sort((a, b) => b.valueNoPot - a.valueNoPot)
				.slice(0, top);
		const sharkPlayers = best(sharkRoster);
		// Picks to sweeten with, cheapest first: second rounds, then the
		// farthest away.
		const sweeteners = (await ctx.picks())
			.filter(
				(dp) =>
					typeof dp.season === "number" && dp.season <= g.get("season") + 3,
			)
			.sort(
				(a, b) =>
					b.round - a.round || (b.season as number) - (a.season as number),
			)
			.slice(0, 4);

		// 1. Candidates.
		const markup = 1 + 0.1 * tradeDifficulty();
		const before = modelPoints(sharkRoster);
		const candidates: Candidate[] = [];
		let built = 0;
		for (let cpuTid = 0; cpuTid < numTeams; cpuTid++) {
			if (userTids.includes(cpuTid)) {
				continue;
			}
			const cpuRoster = rosters.get(cpuTid)!;
			const cpuPlayers = best(cpuRoster);
			const pairs = (xs: Player[]) =>
				xs.flatMap((x, i) => xs.slice(i + 1).map((y) => [x, y]));
			const shapes: [Player[], Player[]][] = [];
			for (const a of sharkPlayers) {
				for (const b of cpuPlayers) {
					shapes.push([[a], [b]]);
				}
				for (const bs of pairs(cpuPlayers)) {
					shapes.push([[a], bs]);
				}
			}
			for (const as of pairs(sharkPlayers)) {
				for (const b of cpuPlayers) {
					shapes.push([as, [b]]);
				}
			}
			for (const [give, get] of shapes) {
				const sharkSize = sharkRoster.length - give.length + get.length;
				const cpuSize = cpuRoster.length - get.length + give.length;
				if (sharkSize > maxRoster || (inSeason && cpuSize > maxRoster)) {
					continue;
				}
				const out = give.reduce((a, p) => a + amountOf(p), 0);
				const into = get.reduce((a, p) => a + amountOf(p), 0);
				if (
					!salaryOk(sharkTid, payrolls.get(sharkTid)!, out, into) ||
					!salaryOk(cpuTid, payrolls.get(cpuTid)!, into, out)
				) {
					continue;
				}
				built += 1;
				const after = [...sharkRoster.filter((p) => !give.includes(p)), ...get];
				const modelShark = modelPoints(after) - before;
				if (modelShark > 0) {
					const cpuRead =
						give.reduce((a, p) => a + cpuTradeValue(p), 0) -
						markup * get.reduce((a, p) => a + cpuTradeValue(p), 0);
					candidates.push({
						cpuTid,
						give,
						givePicks: [],
						get,
						modelShark,
						cpuRead,
					});
				}
			}
		}
		// Those the CPU's cheap read says it would take first, best for the
		// shark first; then the rest, closest first.
		const near = (c: Candidate) => c.cpuRead >= 0;
		candidates.sort(
			(a, b) =>
				Number(near(b)) - Number(near(a)) ||
				(near(a) ? b.modelShark - a.modelShark : b.cpuRead - a.cpuRead),
		);

		// 2. The CPU's own test, best for the shark first.
		const offerOf = (c: Candidate): TradeOffer => ({
			give: {
				pids: c.give.map((p) => p.pid),
				dpids: c.givePicks.map((dp) => dp.dpid),
			},
			get: { tid: c.cpuTid, pids: c.get.map((p) => p.pid), dpids: [] },
		});
		const yes: Candidate[] = [];
		const yesBy = new Map<number, number>();
		const refusedBy = new Map<number, number>();
		let judged = 0;
		for (const c of candidates) {
			if (judged >= judgeBudget || yes.length >= quickSims) {
				break;
			}
			if ((yesBy.get(c.cpuTid) ?? 0) >= perTeam) {
				continue;
			}
			judged += 1;
			let verdict = await ctx.tradeVerdict(offerOf(c));
			if (!verdict.accepted && !verdict.warning) {
				// A pick to get over the line, for the best refused with each
				// team.
				const refused = (refusedBy.get(c.cpuTid) ?? 0) + 1;
				refusedBy.set(c.cpuTid, refused);
				if (refused <= 2 * perTeam) {
					for (const dp of sweeteners) {
						if (judged >= judgeBudget) {
							break;
						}
						judged += 1;
						const sweetened = { ...c, givePicks: [dp] };
						verdict = await ctx.tradeVerdict(offerOf(sweetened));
						if (verdict.accepted) {
							c.givePicks = [dp];
							break;
						}
					}
				}
			}
			if (verdict.accepted) {
				yes.push(c);
				yesBy.set(c.cpuTid, (yesBy.get(c.cpuTid) ?? 0) + 1);
			}
		}
		if (yes.length === 0) {
			ctx.log(
				`${g.get("season")} ${label} hunt ${huntNo}: ${built} built, ${candidates.length} help, ${judged} judged, no yes`,
			);
			return false;
		}

		// 3. The sim.
		const opponents: SimTeam[] = [];
		for (let tid = 0; tid < numTeams; tid++) {
			opponents.push(await simTeamOf(tid, rosters.get(tid)!));
		}
		const leagueRandom = Math.random;
		const margins = async (
			players: readonly Player[],
			stage: number,
			n: number,
		) => {
			const subject = await simTeamOf(sharkTid, players);
			const out: number[] = [];
			try {
				for (let i = 0; i < n; i++) {
					const oppTid = (sharkTid + 1 + (i % (numTeams - 1))) % numTeams;
					Math.random = makeSeededRandom((seedBase + stage) * 1_000_003 + i);
					const home = i % 2 === 0;
					const result = home
						? play(
								helpers.deepCopy(subject),
								helpers.deepCopy(opponents[oppTid]!),
							)
						: play(
								helpers.deepCopy(opponents[oppTid]!),
								helpers.deepCopy(subject),
							);
					const margin = result.team[0].stat.pts - result.team[1].stat.pts;
					out.push(home ? margin : -margin);
				}
			} finally {
				Math.random = leagueRandom;
			}
			return out;
		};
		const afterOf = (c: Candidate) => [
			...sharkRoster.filter((p) => !c.give.includes(p)),
			...c.get,
		];
		const quickBefore = await margins(sharkRoster, 1, screenGames);
		for (const c of yes) {
			c.quick = pairedStat(
				await margins(afterOf(c), 1, screenGames),
				quickBefore,
			);
		}
		const pick = [...yes].sort((a, b) => b.quick!.mean - a.quick!.mean)[0]!;
		if (pick.quick!.mean <= 0) {
			ctx.log(
				`${g.get("season")} ${label} hunt ${huntNo}: ${built} built, ${judged} judged, ${yes.length} yes, none helps in the sim`,
			);
			return false;
		}
		const decide = pairedStat(
			await margins(afterOf(pick), 2, decideGames),
			await margins(sharkRoster, 2, decideGames),
		);
		if (decide.mean <= 2 * decide.se) {
			ctx.log(
				`${g.get("season")} ${label} hunt ${huntNo}: ${built} built, ${judged} judged, ${yes.length} yes, best ${decide.mean.toFixed(2)}±${decide.se.toFixed(2)}, no deal`,
			);
			return false;
		}
		const result = await ctx.proposeTrade(offerOf(pick), {
			hunt: huntNo,
			label,
			modelShark: Math.round(pick.modelShark * 100) / 100,
			quick: Math.round(pick.quick!.mean * 100) / 100,
			decide: Math.round(decide.mean * 100) / 100,
			se: Math.round(decide.se * 100) / 100,
		});
		ctx.log(
			`${g.get("season")} ${label} hunt ${huntNo}: ${built} built, ${judged} judged, ${yes.length} yes, best ${decide.mean.toFixed(2)}±${decide.se.toFixed(2)} with ${pick.cpuTid}: ${result.accepted ? "DEAL" : `refused (${result.message})`}`,
		);
		if (!result.accepted) {
			return false;
		}
		await ctx.autopilot.fixRoster();
		return true;
	};

	const huntUpTo = async (ctx: BenchContext, label: string) => {
		for (let i = 0; i < dealsPerHunt; i++) {
			if (!(await hunt(ctx, label))) {
				break;
			}
		}
	};

	return {
		name: "shark",
		preseason: async (ctx) => {
			// Within the roster limit first, as the shark always kept it.
			await ctx.autopilot.fixRoster();
			await huntUpTo(ctx, "preseason");
		},
		inSeason: {
			stops: (daysToDeadline) => [
				Math.round((daysToDeadline * 2) / 3),
				Math.round(daysToDeadline / 3),
			],
			act: async (ctx, daysToDeadline) => {
				if (daysToDeadline !== undefined) {
					await huntUpTo(ctx, `${daysToDeadline} days to deadline`);
				}
			},
		},
	};
};
