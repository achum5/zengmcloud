import type { DraftPick, Player } from "../../common/types.ts";
import isUntradable from "../../worker/core/trade/isUntradable.ts";
import { currentOutlook } from "../../worker/core/realisticFrontOffice/market.ts";
import { contractOf } from "../../worker/core/realisticFrontOffice/contractOf.ts";
import { smartDraftChoice } from "../../worker/core/smartCpu/draft.ts";
import {
	prepareSignings,
	smartSigningPrice,
	viewFor,
} from "../../worker/core/smartCpu/signing.ts";
import { salaryRulesAllow } from "../../worker/core/smartCpu/tradeRules.ts";
import type { Holdings } from "../../worker/core/smartCpu/worth.ts";
import { g, local } from "../../worker/util/index.ts";
import { tradeDifficulty } from "../../worker/util/difficulty.ts";
import type { BenchContext, BenchPolicy, TradeOffer } from "./policy.ts";

// THE GM AS A HUMAN (docs/smart-cpu.md, layer 4): one team run by the smart
// CPU's GM under the human rules - trades only as the CPU accepts them
// through the trade screen, contracts only at what a player asks this team
// and only if he will negotiate, the draft, re-signing. Everything it decides
// by its own worth (layer 1), the same as every CPU team's GM: against
// today's CPU, its rate is how good the GM is next to the humans.
//
// Trades, in the preseason and three times before the deadline: what it
// could give (its best tradable players and nearest picks) and what it could
// get (every CPU team's best players and nearest picks), ranked by what each
// does for it alone, the shapes the shark tries (one for one, two for one,
// one for two, a player for picks), within the trade rules on salary and the
// roster limit; the best worked out in full, and put to the CPU, best first.

export type GmPolicyOptions = {
	// Of each side's players, the best this many.
	top?: number;
	// Candidates worked out in full a hunt.
	budget?: number;
	// Deals a hunt, and with any one team.
	deals?: number;
	// Signings a day.
	signings?: number;
};

const tradable = (p: Player) =>
	!isUntradable(p).untradable && p.injury.gamesRemaining === 0;

const nearestPicks = (picks: readonly DraftPick[], n: number) =>
	[...picks]
		.filter((dp) => typeof dp.season === "number")
		.sort(
			(a, b) =>
				(a.season as number) - (b.season as number) ||
				a.round - b.round ||
				a.dpid - b.dpid,
		)
		.slice(0, n);

// A cheap read of how today's CPU prices a player in a deal (the shark's:
// vanilla's value as a z-score, to the 7th power above 1), to put first the
// deals it may take - its players marked up as against a human. A human does
// the same: he knows what the CPU likes.
const cpuTradeValue = (p: Player) => {
	const z = (p.value - local.playerOvrMean) / local.playerOvrStd;
	return z > 1 ? z ** 7 : z;
};

const salaryOf = (h: Holdings) =>
	h.players.reduce((sum, p) => sum + contractOf(p).amount, 0);

const sizeAfter = (h: Holdings, out: Holdings, inn: Holdings) =>
	h.players.length - out.players.length + inn.players.length;

export const gmPolicy = (options: GmPolicyOptions = {}): BenchPolicy => {
	const top = options.top ?? 8;
	const budget = options.budget ?? 40;
	const dealsPerHunt = options.deals ?? 2;
	const signingsPerDay = options.signings ?? 3;
	let huntNo = 0;
	let huntDays = new Set<number>();

	const hunt = async (ctx: BenchContext, label: string) => {
		huntNo += 1;
		const me = ctx.tid;
		const userTids = g.get("userTids") as number[];
		const maxRoster = g.get("maxRosterSize");
		const made = new Set<number>();
		for (let deal = 0; deal < dealsPerHunt; deal++) {
			const outlook = await currentOutlook();
			const view = viewFor(outlook);
			await view.prime();
			const mine = view.heldNow(me);
			const myPlayers = mine.players
				.filter(tradable)
				.sort((a, b) => b.value - a.value || a.pid - b.pid)
				.slice(0, top);
			const myPicks = nearestPicks(mine.picks, 4);
			// What each does for it alone.
			const use = new Map<Player | DraftPick, number>();
			for (const x of [...myPlayers, ...myPicks]) {
				use.set(
					x,
					view.useOfNow(
						me,
						"pid" in x
							? { players: [x], picks: [] }
							: { players: [], picks: [x] },
					),
				);
			}
			const myPayroll = await view.tradePayroll(me);
			type Shape = {
				tid: number;
				out: Holdings;
				inn: Holdings;
				est: number;
				cpuRead: number;
			};
			const markup = 1 + 0.1 * tradeDifficulty();
			const shapes: Shape[] = [];
			for (const tid of view.outlook.teams.keys()) {
				if (tid === me || userTids.includes(tid) || made.has(tid)) {
					continue;
				}
				const theirs = view.heldNow(tid);
				const theirPlayers = theirs.players
					.filter(tradable)
					.sort((a, b) => b.value - a.value || a.pid - b.pid)
					.slice(0, top);
				const theirPicks = nearestPicks(theirs.picks, 2);
				const want = new Map<Player | DraftPick, number>();
				for (const x of [...theirPlayers, ...theirPicks]) {
					want.set(
						x,
						await view.useOf(
							me,
							"pid" in x
								? { players: [x], picks: [] }
								: { players: [], picks: [x] },
						),
					);
				}
				const theirPayroll = await view.tradePayroll(tid);
				const add = (out: Holdings, inn: Holdings) => {
					if (
						sizeAfter(mine, out, inn) > maxRoster ||
						sizeAfter(theirs, inn, out) > maxRoster
					) {
						return;
					}
					const sent = salaryOf(out);
					const taken = salaryOf(inn);
					if (
						!salaryRulesAllow(me, myPayroll, taken, sent) ||
						!salaryRulesAllow(tid, theirPayroll, sent, taken)
					) {
						return;
					}
					const est =
						[...inn.players, ...inn.picks].reduce(
							(a, x) => a + want.get(x)!,
							0,
						) -
						[...out.players, ...out.picks].reduce((a, x) => a + use.get(x)!, 0);
					// Picks read as the CPU's average player, about.
					const cpuRead =
						out.players.reduce((a, p) => a + cpuTradeValue(p), 0) +
						0.5 * out.picks.length -
						markup *
							(inn.players.reduce((a, p) => a + cpuTradeValue(p), 0) +
								0.5 * inn.picks.length);
					if (est > 0) {
						shapes.push({ tid, out, inn, est, cpuRead });
					}
				};
				const one = (x: Player | DraftPick): Holdings =>
					"pid" in x
						? { players: [x], picks: [] }
						: { players: [], picks: [x] };
				const both = (x: Holdings, y: Holdings): Holdings => ({
					players: [...x.players, ...y.players],
					picks: [...x.picks, ...y.picks],
				});
				for (const a of myPlayers) {
					for (const b of theirPlayers) {
						add(one(a), one(b));
						for (const dp of myPicks) {
							add(both(one(a), one(dp)), one(b));
						}
					}
					for (let i = 0; i < theirPlayers.length; i++) {
						for (let j = i + 1; j < theirPlayers.length; j++) {
							add(one(a), both(one(theirPlayers[i]!), one(theirPlayers[j]!)));
						}
					}
					for (const dp of theirPicks) {
						add(one(a), one(dp));
					}
				}
				for (let i = 0; i < myPlayers.length; i++) {
					for (let j = i + 1; j < myPlayers.length; j++) {
						for (const b of theirPlayers) {
							add(both(one(myPlayers[i]!), one(myPlayers[j]!)), one(b));
						}
					}
				}
			}
			// Those the CPU may take first, best for it by its own reckoning;
			// then the rest, closest first.
			const near = (x: Shape) => x.cpuRead >= 0;
			shapes.sort(
				(a, b) =>
					Number(near(b)) - Number(near(a)) ||
					(near(a) ? b.est - a.est : b.cpuRead - a.cpuRead),
			);
			// The best worked out in full, by its worth.
			const worked: (Shape & { worth: number })[] = [];
			for (const s of shapes.slice(0, budget)) {
				const worth = await view.dealWorth(me, s.tid, s.out, s.inn);
				if (worth > 0) {
					worked.push({ ...s, worth });
				}
			}
			worked.sort((a, b) => b.worth - a.worth);
			let done = false;
			for (const s of worked) {
				const offer: TradeOffer = {
					give: {
						pids: s.out.players.map((p) => p.pid),
						dpids: s.out.picks.map((dp) => dp.dpid),
					},
					get: {
						tid: s.tid,
						pids: s.inn.players.map((p) => p.pid),
						dpids: s.inn.picks.map((dp) => dp.dpid),
					},
				};
				let verdict = await ctx.tradeVerdict(offer);
				if (!verdict.accepted && !verdict.warning) {
					// A pick of its own to get over the line, if it is still
					// worth it.
					for (const dp of [...myPicks].reverse()) {
						if (s.out.picks.includes(dp)) {
							continue;
						}
						const out = { players: s.out.players, picks: [...s.out.picks, dp] };
						const worth = await view.dealWorth(me, s.tid, out, s.inn);
						if (!(worth > 0)) {
							continue;
						}
						const sweetened = {
							...offer,
							give: { ...offer.give, dpids: [...offer.give.dpids, dp.dpid] },
						};
						verdict = await ctx.tradeVerdict(sweetened);
						if (verdict.accepted) {
							offer.give = sweetened.give;
							s.worth = worth;
							break;
						}
					}
				}
				if (!verdict.accepted) {
					continue;
				}
				const result = await ctx.proposeTrade(offer, {
					worth: s.worth,
					est: s.est,
				});
				if (result.accepted) {
					ctx.log(
						`${g.get("season")} ${label} hunt ${huntNo}: traded with ${s.tid}, worth ${s.worth.toFixed(4)} (${shapes.length} shapes, ${worked.length} worth it)`,
					);
					made.add(s.tid);
					done = true;
					break;
				}
			}
			if (!done) {
				ctx.log(
					`${g.get("season")} ${label} hunt ${huntNo}: ${shapes.length} shapes, ${worked.length} worth it, none accepted`,
				);
				return;
			}
		}
	};

	// Free agents worth signing at what they ask this team, best first.
	const sign = async (ctx: BenchContext, label: string) => {
		const me = ctx.tid;
		for (let k = 0; k < signingsPerDay; k++) {
			if ((await ctx.roster()).length >= g.get("maxRosterSize")) {
				return;
			}
			const pool = (await ctx.freeAgents())
				.sort((a, b) => b.value - a.value || a.pid - b.pid)
				.slice(0, 15);
			if (pool.length === 0) {
				return;
			}
			const outlook = await currentOutlook();
			await prepareSignings(outlook, pool);
			const view = viewFor(outlook);
			const h = view.heldNow(me);
			let best: { p: Player; exp: number; gain: number } | undefined;
			for (const p of pool) {
				const options = await ctx.contractOptions(p.pid);
				if (typeof options === "string") {
					continue;
				}
				for (const o of options) {
					const { net } = smartSigningPrice(
						outlook,
						{ tid: me, players: h.players, picks: h.picks },
						p,
						{ amount: o.amount, exp: o.exp },
					);
					if (net > 0 && (!best || net > best.gain)) {
						best = { p, exp: o.exp, gain: net };
					}
				}
			}
			if (!best) {
				return;
			}
			const error = await ctx.sign(best.p.pid, best.exp);
			if (error) {
				ctx.log(`${g.get("season")} ${label}: ${best.p.pid} - ${error}`);
				return;
			}
			ctx.log(
				`${g.get("season")} ${label}: signed ${best.p.pid} through ${best.exp}, worth ${best.gain.toFixed(4)}`,
			);
		}
	};

	return {
		name: "gm",
		preseason: async (ctx) => {
			await hunt(ctx, "preseason");
			await ctx.autopilot.fixRoster();
		},
		inSeason: {
			// Trades two thirds and a third of the way to the deadline and on
			// its last day; signings every week.
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
				await sign(ctx, "in season");
				await ctx.autopilot.fixRoster();
			},
		},
		inSeasonSignings: false,
		draftPick: async (_ctx, dp, prospects) =>
			(await smartDraftChoice(dp, prospects)).choice?.pid,
		reSign: async (ctx, expiring) => {
			const me = ctx.tid;
			const outlook = await currentOutlook();
			await prepareSignings(outlook, expiring);
			const view = viewFor(outlook);
			const letGo: number[] = [];
			for (const p of expiring) {
				const h = view.heldNow(me);
				const options = await ctx.contractOptions(p.pid);
				let best: { exp: number; gain: number } | undefined;
				if (typeof options !== "string") {
					for (const o of options) {
						const { net } = smartSigningPrice(
							outlook,
							{ tid: me, players: h.players, picks: h.picks },
							p,
							{ amount: o.amount, exp: o.exp },
						);
						if (net > 0 && (!best || net > best.gain)) {
							best = { exp: o.exp, gain: net };
						}
					}
				}
				const r = p.ratings.at(-1)!;
				ctx.log(
					`${g.get("season")} re-sign ${p.pid} ${g.get("season") - p.born.year}y ${r.ovr}/${r.pot}: ${
						typeof options === "string"
							? options
							: options
									.map((o) => `$${(o.amount / 1000).toFixed(1)}M->${o.exp}`)
									.join(" ")
					}; ${best ? `best ${best.exp}, worth ${best.gain.toFixed(4)}` : "none worth it"}`,
				);
				if (best && !(await ctx.sign(p.pid, best.exp))) {
					await view.prime();
				} else {
					letGo.push(p.pid);
				}
			}
			return { letGo };
		},
		freeAgencyDay: async (ctx, daysLeft) => {
			await sign(ctx, `free agency ${daysLeft}`);
			return true;
		},
	};
};
