import { PHASE } from "../../common/constants.ts";
import type { Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { player } from "../../worker/core/index.ts";
import isUntradable from "../../worker/core/trade/isUntradable.ts";
import { projectPlayer } from "../../worker/core/realisticFrontOffice/outlook.ts";
import { contractOf } from "../../worker/core/realisticFrontOffice/contractOf.ts";
import { g } from "../../worker/util/index.ts";
import type {
	BenchContext,
	BenchPolicy,
	TradeOffer,
	TradeSide,
} from "./policy.ts";

// A HUMAN-STRENGTH BOT (docs/smart-cpu.md, "The bench"): one team run the way
// the league's best humans run theirs, to be the yardstick for how hard a CPU
// is to beat. It judges by what wins - its team's strength this season and
// the next two, from each player's projected overall (the game's own team
// rating, from the game's own development code) - and it trades through the
// screens a human uses: "What would make this work" for each player it wants,
// its best players marked untouchable, and the trading block for those the
// CPU values more than it does. It re-signs every good player willing to stay
// whatever he asks (his own team may go over the cap for him, nobody else's
// can), signs the best free agents who will negotiate, and drafts the
// prospect it projects best. The CPU's answers are the league's own (vanilla,
// or the smart CPU), so the same bot measures either.
//
// Why not the smart CPU's own worth (gmPolicy.ts): as a human it traded its
// young players and picks for veterans at every hunt and let eight willing
// players go one summer, counting on cap room it could not use - its outlook
// prices room as a CPU team spends it, and a human over the cap can sign only
// minimum players.

export type ProPolicyOptions = {
	// Players asked about a hunt ("What would make this work"), the best for
	// it league-wide.
	targets?: number;
	// Its own players and picks put on the trading block a hunt.
	blockAssets?: number;
	// Deals a hunt.
	deals?: number;
	// The least a deal must add to its strength, in titles' worth (below).
	margin?: number;
	// Trades as free agency opens.
	faHunt?: boolean;
};

// A season's weight: this one, the next, the one after.
const WEIGHTS = [1, 0.65, 0.4];
// A roster's empty places, filled with a minimum player of about this overall.
const REPLACEMENT_OVR = 42;
// The game's team rating (team/ovr.basketball.ts): predicted margin of
// victory from the ten best players' overalls, best first.
const MOV_A = 0.3334;
const MOV_B = -0.1609;
const MOV_K = 102.98;

const movOf = (ovrs: number[]) => {
	const top = [...ovrs].sort((a, b) => b - a).slice(0, 10);
	while (top.length < 10) {
		top.push(REPLACEMENT_OVR);
	}
	let mov = -MOV_K;
	for (let i = 0; i < 10; i++) {
		mov += MOV_A * Math.exp(MOV_B * i) * top[i]!;
	}
	return mov;
};

const tradable = (p: Player) =>
	!isUntradable(p).untradable && p.injury.gamesRemaining === 0;

export const proPolicy = (options: ProPolicyOptions = {}): BenchPolicy => {
	const numTargets = options.targets ?? 20;
	const blockAssets = options.blockAssets ?? 4;
	const dealsPerHunt = options.deals ?? 3;
	const margin = options.margin ?? 0.01;
	let huntDays = new Set<number>();

	// The season the next games are in: the coming one, once this one's
	// playoffs are over.
	const baseSeason = () =>
		g.get("season") + (g.get("phase") > PHASE.PLAYOFFS ? 1 : 0);

	// His expected overall y seasons from the base season.
	const ovrIn = (p: Player, y: number) => {
		const r = p.ratings.at(-1)!;
		const k = baseSeason() + y - r.season;
		if (k <= 0) {
			return r.ovr;
		}
		const projection = projectPlayer(p, r.season);
		return projection.ovr[Math.min(k, projection.ovr.length - 1)]!;
	};

	// His chance of being the team's that season: sure under contract, else
	// the chance he would re-sign with it now (his mood's), as a human
	// re-signs every good player who will stay.
	const stayCache = new Map<number, number>();
	const stayChance = async (p: Player, tid: number, y: number) => {
		const exp = contractOf(p).exp;
		if (exp >= baseSeason() + y) {
			return 1;
		}
		let chance = stayCache.get(p.pid);
		if (chance === undefined) {
			chance = (await player.moodInfo(p, tid)).probWilling;
			stayCache.set(p.pid, chance);
		}
		return chance;
	};

	// The best other team's rating now: the bar a title is won over.
	let bar = 0;
	const setBar = async (me: number) => {
		const humans = new Set(g.get("userTids") as number[]);
		let best = -Infinity;
		for (const t of await idb.cache.teams.getAll()) {
			if (t.disabled || t.tid === me || humans.has(t.tid)) {
				continue;
			}
			const players = await idb.cache.players.indexGetAll(
				"playersByTid",
				t.tid,
			);
			best = Math.max(best, movOf(players.map((p) => p.ratings.at(-1)!.ovr)));
		}
		bar = Number.isFinite(best) ? best : 0;
	};

	// What a roster is worth to it: each season's chance of being the best
	// team (a logistic in its rating against the bar), and a little for
	// being better at all, so a bad team still climbs.
	const worthOf = async (tid: number, roster: readonly Player[]) => {
		let total = 0;
		for (const [y, w] of WEIGHTS.entries()) {
			// Each his expected overall: a player who may leave counts as
			// a minimum player as often as he would.
			const ovrs: number[] = [];
			for (const p of roster) {
				if (y === 0 && p.injury.gamesRemaining > 25) {
					continue;
				}
				const r = await stayChance(p, tid, y);
				ovrs.push(r * ovrIn(p, y) + (1 - r) * REPLACEMENT_OVR);
			}
			const mov = movOf(ovrs);
			total += w * (1 / (1 + Math.exp(-(mov - bar) / 2.5)) + 0.02 * mov);
		}
		return total;
	};

	const rosterAfter = async (tid: number, offer: TradeOffer) => {
		const gone = new Set(offer.give.pids);
		const mine = (
			await idb.cache.players.indexGetAll("playersByTid", tid)
		).filter((p) => !gone.has(p.pid));
		for (const pid of offer.get.pids) {
			const p = await idb.cache.players.get(pid);
			if (p) {
				mine.push(p);
			}
		}
		// Over the limit, its worst go (the roster screen's cut).
		const max = g.get("maxRosterSize");
		if (mine.length > max) {
			mine.sort((a, b) => ovrIn(b, 0) - ovrIn(a, 0));
			mine.length = max;
		}
		return mine;
	};

	const hunt = async (ctx: BenchContext, label: string) => {
		const me = ctx.tid;
		const humans = new Set(g.get("userTids") as number[]);
		const traded = new Set<number>();
		for (let deal = 0; deal < dealsPerHunt; deal++) {
			stayCache.clear();
			await setBar(me);
			const roster = await ctx.roster();
			const before = await worthOf(me, roster);
			// Its core, untouchable: the three it would miss most.
			const missed: { p: Player; loss: number }[] = [];
			for (const p of roster) {
				missed.push({
					p,
					loss:
						before -
						(await worthOf(
							me,
							roster.filter((x) => x !== p),
						)),
				});
			}
			missed.sort((a, b) => b.loss - a.loss);
			const core = missed.slice(0, 3).map((m) => m.p.pid);
			const keep: TradeSide = { pids: core, dpids: [] };

			const offers: { offer: TradeOffer; gain: number; how: string }[] = [];
			const weigh = async (offer: TradeOffer, how: string) => {
				if (traded.has(offer.get.tid)) {
					return;
				}
				const gain = (await worthOf(me, await rosterAfter(me, offer))) - before;
				if (gain > margin) {
					offers.push({ offer, gain, how });
				}
			};

			// The players it wants most, league-wide: each asked about.
			const wants: { tid: number; p: Player; gain: number }[] = [];
			for (const t of await idb.cache.teams.getAll()) {
				if (t.disabled || t.tid === me || humans.has(t.tid)) {
					continue;
				}
				const theirs = (
					await idb.cache.players.indexGetAll("playersByTid", t.tid)
				)
					.filter(tradable)
					.sort((a, b) => ovrIn(b, 1) - ovrIn(a, 1))
					.slice(0, 3);
				for (const p of theirs) {
					wants.push({
						tid: t.tid,
						p,
						gain: (await worthOf(me, [...roster, p])) - before,
					});
				}
			}
			// Only those who would start for it: depth is what it trades
			// away, not what it asks for.
			const starters = [...roster]
				.map((p) => Math.max(ovrIn(p, 0), ovrIn(p, 1)))
				.sort((a, b) => b - a);
			const fifth = starters[4] ?? REPLACEMENT_OVR;
			wants.sort((a, b) => b.gain - a.gain);
			for (const w of wants
				.filter((x) => Math.max(ovrIn(x.p, 0), ovrIn(x.p, 1)) > fifth)
				.slice(0, numTargets)) {
				const made = await ctx.makeItWork(
					{
						give: { pids: [], dpids: [] },
						get: { tid: w.tid, pids: [w.p.pid], dpids: [] },
					},
					keep,
				);
				if (made) {
					await weigh(made, `asked for ${w.p.pid}`);
				}
			}

			// Its players it would miss least put up for offers (not its
			// picks: sold one at a time they bought only role players - a
			// pick goes in a deal for someone it asked for).
			const spare = missed
				.slice(3)
				.filter((m) => tradable(m.p))
				.reverse()
				.slice(0, blockAssets - 1)
				.map((m) => m.p.pid);
			for (const pid of spare) {
				for (const offer of await ctx.tradingBlock({
					pids: [pid],
					dpids: [],
				})) {
					await weigh(offer, `block ${pid}`);
				}
			}
			offers.sort((a, b) => b.gain - a.gain);
			let done = false;
			for (const o of offers.slice(0, 5)) {
				const result = await ctx.proposeTrade(o.offer, {
					gain: o.gain,
					how: o.how,
				});
				if (result.accepted) {
					ctx.log(
						`${g.get("season")} ${label}: traded with ${o.offer.get.tid} (${o.how}), gain ${o.gain.toFixed(3)} of ${offers.length} worth it`,
					);
					traded.add(o.offer.get.tid);
					done = true;
					break;
				}
			}
			if (!done) {
				ctx.log(
					`${g.get("season")} ${label}: ${offers.length} worth it, none made`,
				);
				return;
			}
			await ctx.autopilot.fixRoster();
		}
	};

	// The free agent who adds most, at what he asks of it; then the next.
	const sign = async (ctx: BenchContext, label: string, most: number) => {
		const me = ctx.tid;
		for (let k = 0; k < most; k++) {
			const roster = await ctx.roster();
			if (roster.length >= g.get("maxRosterSize")) {
				return;
			}
			stayCache.clear();
			await setBar(me);
			const before = await worthOf(me, roster);
			const pool = (await ctx.freeAgents())
				.sort((a, b) => ovrIn(b, 0) - ovrIn(a, 0))
				.slice(0, 12);
			const ranked: { p: Player; gain: number }[] = [];
			for (const p of pool) {
				ranked.push({ p, gain: (await worthOf(me, [...roster, p])) - before });
			}
			ranked.sort((a, b) => b.gain - a.gain);
			let signed = false;
			for (const { p, gain } of ranked.slice(0, 6)) {
				// Filling a short roster is worth it even for nothing.
				if (gain <= 0 && roster.length >= g.get("minRosterSize")) {
					break;
				}
				const terms = await ctx.contractOptions(p.pid);
				if (typeof terms === "string") {
					continue;
				}
				// Old players short, young ones long, as a human signs them.
				const age = g.get("season") - p.born.year;
				const exp =
					age >= 30
						? Math.min(...terms.map((t) => t.exp))
						: age <= 25
							? Math.max(...terms.map((t) => t.exp))
							: undefined;
				const error = await ctx.sign(p.pid, exp);
				if (!error) {
					ctx.log(
						`${g.get("season")} ${label}: signed ${p.pid} (${age}y ${p.ratings.at(-1)!.ovr}), gain ${gain.toFixed(3)}`,
					);
					signed = true;
					break;
				}
			}
			if (!signed) {
				return;
			}
		}
	};

	return {
		name: "pro",
		preseason: async (ctx) => {
			await hunt(ctx, "preseason");
			await sign(ctx, "preseason", 2);
			await ctx.autopilot.fixRoster();
		},
		inSeason: {
			// Trades two thirds and a third of the way to the deadline and on
			// its last day; a short roster filled every week.
			stops: (daysToDeadline) => {
				huntDays = new Set([
					Math.round((daysToDeadline * 2) / 3),
					Math.round(daysToDeadline / 3),
					1,
				]);
				return [...huntDays];
			},
			every: 7,
			act: async (ctx, daysToDeadline) => {
				if (daysToDeadline !== undefined && huntDays.has(daysToDeadline)) {
					await hunt(ctx, `${daysToDeadline} days out`);
				}
				if ((await ctx.roster()).length < g.get("minRosterSize") + 1) {
					await sign(ctx, "in season", 2);
				}
				await ctx.autopilot.fixRoster();
			},
		},
		inSeasonSignings: false,
		// The prospect it projects best three seasons on.
		draftPick: async (_ctx, _dp, prospects) => {
			let best: { pid: number; ovr: number } | undefined;
			for (const p of prospects) {
				const ovr = ovrIn(p, 3);
				if (!best || ovr > best.ovr) {
					best = { pid: p.pid, ovr };
				}
			}
			return best?.pid;
		},
		// Every player who would make next season's top nine, or who is young
		// and projects to: re-signed at what he asks, if he will stay.
		reSign: async (ctx, expiring) => {
			const me = ctx.tid;
			const roster = await ctx.roster();
			const nextYear = (p: Player) => Math.max(ovrIn(p, 0), ovrIn(p, 1));
			const ranked = [...roster, ...expiring].sort(
				(a, b) => nextYear(b) - nextYear(a),
			);
			const ninth = nextYear(ranked[Math.min(8, ranked.length - 1)]!);
			const letGo: number[] = [];
			for (const p of expiring) {
				const age = g.get("season") - p.born.year;
				const wanted =
					nextYear(p) >= ninth || (age <= 24 && ovrIn(p, 2) >= ninth);
				if (!wanted) {
					letGo.push(p.pid);
					continue;
				}
				const error = await ctx.sign(p.pid);
				ctx.log(
					`${g.get("season")} re-sign ${p.pid} ${age}y ${p.ratings.at(-1)!.ovr}: ${error ?? "re-signed"}`,
				);
				if (error) {
					letGo.push(p.pid);
				}
			}
			void me;
			return { letGo };
		},
		freeAgencyDay: async (ctx, daysLeft) => {
			// The best go early: most of its signing on the first days.
			await sign(ctx, `free agency ${daysLeft}`, daysLeft >= 28 ? 3 : 1);
			// Trades as free agency opens, once the rosters have settled.
			if (daysLeft === 25 && (options.faHunt ?? true)) {
				await hunt(ctx, "free agency");
			}
			return true;
		},
	};
};

// The shark's trades (policies.ts: every deal played out in the game's own
// sim before it is proposed), with this bot's re-signing, free agency and
// draft in place of auto play's - the shark won 62% of its games for three
// seasons, then sank as auto play let its players go.
export const sharkPro = (
	shark: BenchPolicy,
	options: ProPolicyOptions = {},
): BenchPolicy => {
	const pro = proPolicy({ ...options, faHunt: false });
	return {
		name: "sharkpro",
		preseason: async (ctx) => {
			await shark.preseason?.(ctx);
			await ctx.autopilot.fixRoster();
		},
		inSeason: shark.inSeason,
		inSeasonSignings: shark.inSeasonSignings,
		draftPick: pro.draftPick,
		reSign: pro.reSign,
		freeAgencyDay: pro.freeAgencyDay,
	};
};
