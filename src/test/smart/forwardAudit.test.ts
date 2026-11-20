import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE FORWARD AUDIT: a league played on FA_SEASONS seasons as the owner plays
// one, with every market deal written out as made (each side's gain, split
// into its own goal and the market's price premiums, its title odds then),
// every pick of the first round with what the team weighed, and at the end
// the league itself - players with their stats, ratings and contracts, teams
// with their seasons, every event - in an export's shape, for the reviews
// done on owners' files (trades, signings, drafts, records).
//   FA_LEAGUE=<league.json | arena:<lid>> FA_OUT=<file> [FA_SEASONS=3] \
//     npx vitest --run --project basketball src/test/smart/forwardAudit.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.FA_LEAGUE)(
	"forward audit",
	{ timeout: 28_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const out = env.FA_OUT ?? "/tmp/forwardAudit.txt";
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(out, lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.FA_LEAGUE!);
		const { league, game, phase, draft, freeAgents } =
			await import("../../worker/core/index.ts");
		const { PHASE, PLAYER } = await import("../../common/constants.ts");
		const { idb } = await import("../../worker/db/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		// FA_CLEAR_PRICE=0: players sent to clear room counted at their use
		// alone, as before 2026-11-20.
		if (env.FA_CLEAR_PRICE === "0") {
			const { clearRoomSettings } =
				await import("../../worker/core/smartCpu/clearRoom.ts");
			clearRoomSettings.atPrice = false;
		}
		// FA_GAMES_BETWEEN=<n>: the market meets every n games a team on
		// vanilla's rules, not marketSettings.vanillaGamesBetween's.
		if (env.FA_GAMES_BETWEEN) {
			const { marketSettings } =
				await import("../../worker/core/smartCpu/market.ts");
			marketSettings.vanillaGamesBetween = Number(env.FA_GAMES_BETWEEN);
		}
		const { smartDealWatchers, smartTraceWatchers } =
			await import("../../worker/core/smartCpu/market.ts");
		// FA_STARS=<n>: where every one of the league's n best players got to
		// each time he was shopped, and what stopped him.
		if (env.FA_STARS) {
			const top = Number(env.FA_STARS);
			let rankFor = "";
			let rank = new Map<number, number>();
			smartTraceWatchers.push((t, view: any) => {
				const key = `${g.get("season")}:${g.get("phase")}`;
				if (key !== rankFor) {
					rankFor = key;
					rank = new Map();
				}
				if (rank.size === 0) {
					const rows: { pid: number; ovr: number }[] = [];
					for (const tid of view.outlook.teams.keys()) {
						for (const p of view.heldNow(tid).players) {
							rows.push({ pid: p.pid, ovr: p.ratings.at(-1).ovr });
						}
					}
					rows.sort((a, b) => b.ovr - a.ovr);
					rows.forEach((r, i) => rank.set(r.pid, i + 1));
				}
				const r = rank.get(t.p.pid);
				if (r === undefined || r > top) {
					return;
				}
				const ab = (tid?: number) =>
					tid === undefined ? "-" : (abbrev.get(tid) ?? String(tid));
				const title = (tid?: number) =>
					tid === undefined
						? ""
						: ` (${(100 * (view.outlook.teams.get(tid)?.title?.[0] ?? 0)).toFixed(1)}%)`;
				const offer = t.offer
					? ` for [${[...t.offer.players.map((x: any) => `p${x.pid} ${g.get("season") - x.born.year}y ${x.ratings.at(-1).ovr}`), ...t.offer.picks.map((x: any) => `${x.season} R${x.round}`)].join("; ")}]`
					: "";
				// The seller's side of it: his use to it, his price, and its gain
				// in parts.
				let parts = "";
				if (t.offer && t.buyer !== undefined) {
					const one = { players: [t.p], picks: [] };
					const d = view.dealPartsNow(t.seller, t.buyer, one, t.offer);
					parts = ` | seller: his use ${view.useOfNow(t.seller, one).toFixed(3)}, price ${view.priceNow(one, [t.seller, t.buyer]).toFixed(3)}; goal ${d.goal.toFixed(3)} premium got ${d.got.toFixed(3)} given ${d.given.toFixed(3)}; offer's use ${view.useOfNow(t.seller, t.offer).toFixed(3)} price ${view.priceNow(t.offer, [t.seller, t.buyer]).toFixed(3)}; buyer's use for him ${view.useOfNow(t.buyer, one).toFixed(3)}`;
				}
				log(
					`STAR ${g.get("season")} ph${g.get("phase")} ${t.stage}: #${r} p${t.p.pid} ${g.get("season") - t.p.born.year}y ${t.p.ratings.at(-1)!.ovr} of ${ab(t.seller)}${title(t.seller)}${t.buyer !== undefined ? ` to ${ab(t.buyer)}${title(t.buyer)}` : ""}${t.buyers ? ` [buyers ${t.buyers.map(ab).join(",")}]` : ""}${t.sellerGain !== undefined ? ` seller ${t.sellerGain.toFixed(3)}` : ""}${t.buyerGain !== undefined ? ` buyer ${t.buyerGain.toFixed(3)}` : ""}${t.why ? ` | ${t.why}` : ""}${offer}${parts}`,
				);
			});
		}
		const { smartDraftWatchers } =
			await import("../../worker/core/smartCpu/draft.ts");
		const { smartSettings } =
			await import("../../worker/core/smartCpu/worth.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const { retentionOf } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const named = (x: any, tid?: number, view?: any) => {
			if (!("pid" in x)) {
				return `${x.season} R${x.round} ${abbrev.get(x.originalTid)}`;
			}
			const c = contractOf(x);
			const stay =
				view && tid !== undefined && c.exp <= g.get("season") + 1
					? ` stay ${retentionOf(view.outlook, x.pid, tid).toFixed(2)}`
					: "";
			return `${x.firstName} ${x.lastName} ${g.get("season") - x.born.year}y ${x.ratings.at(-1).ovr}/${x.ratings.at(-1).pot} thru ${c.exp}${stay}`;
		};
		const title = (view: any, tid: number) =>
			(100 * (view.outlook.teams.get(tid)?.title?.[0] ?? 0)).toFixed(1);
		const split = (
			view: any,
			tid: number,
			other: number,
			inn: any,
			outs: any,
			gain: number,
		) => {
			const h = view.heldNow(tid);
			const pin = view.premiumNow(
				tid,
				smartSettings.boughtAtUse ? { players: [], picks: inn.picks } : inn,
				[tid, other],
				h,
			);
			const pout = view.premiumNow(tid, outs, [tid, other], h);
			return `goal ${(gain - pin + pout).toFixed(4)}, premium got ${pin.toFixed(4)} given ${pout.toFixed(4)}`;
		};
		// Each asset a side gets or gives: its use to the side, and its price.
		const each = (view: any, tid: number, other: number, a: any) =>
			[...a.players, ...a.picks]
				.map((x: any) => {
					const one =
						"pid" in x
							? { players: [x], picks: [] }
							: { players: [], picks: [x] };
					return `${"pid" in x ? x.lastName : `${x.season}R${x.round}`} use ${view.useOfNow(tid, one).toFixed(4)} price ${view.priceNow(one, [tid, other]).toFixed(4)}`;
				})
				.join(", ");
		smartDealWatchers.push((deal: any, view: any) => {
			const margin = (tid: number, give: any, get: any) =>
				view.marginNow ? view.marginNow(tid, give, get).toFixed(4) : "?";
			log(
				`  ${abbrev.get(deal.seller)} margin ${margin(deal.seller, deal.out, deal.offer)}: gives ${each(view, deal.seller, deal.buyer, deal.out)}; gets ${each(view, deal.seller, deal.buyer, deal.offer)} || ${abbrev.get(deal.buyer)} margin ${margin(deal.buyer, deal.offer, deal.out)}: gives ${each(view, deal.buyer, deal.seller, deal.offer)}; gets ${each(view, deal.buyer, deal.seller, deal.out)}`,
			);
			log(
				`DEAL ${g.get("season")} ph${g.get("phase")}: ${abbrev.get(deal.seller)} (${title(view, deal.seller)}%) gives [${[...deal.out.players, ...deal.out.picks].map((x: any) => named(x, deal.seller, view)).join("; ")}] to ${abbrev.get(deal.buyer)} (${title(view, deal.buyer)}%) for [${[...deal.offer.players, ...deal.offer.picks].map((x: any) => named(x, deal.buyer, view)).join("; ")}] | ${abbrev.get(deal.seller)} +${deal.sellerGain.toFixed(4)} (${split(view, deal.seller, deal.buyer, deal.offer, deal.out, deal.sellerGain)}) | ${abbrev.get(deal.buyer)} +${deal.buyerGain.toFixed(4)} (${split(view, deal.buyer, deal.seller, deal.out, deal.offer, deal.buyerGain)})`,
			);
		});
		// FA_OFFERS=<names> from FA_OFFERS_FROM (a season): at each market
		// moment while each is still with his team, the best real offer for
		// him from each of the eight teams keenest on him - what it would give,
		// its worth to his team, what the buyer would gain.
		const watched = (env.FA_OFFERS ?? "").split(",").filter(Boolean);
		const { smartMomentWatchers } =
			await import("../../worker/core/smartCpu/market.ts");
		if (watched.length > 0) {
			smartMomentWatchers.push(async (_open: any, view: any) => {
				if (g.get("season") < Number(env.FA_OFFERS_FROM ?? 0)) {
					return;
				}
				const all = await idb.cache.players.indexGetAll("playersByTid", [
					0,
					Infinity,
				]);
				for (const p of all as any[]) {
					if (!watched.includes(`${p.firstName} ${p.lastName}`)) {
						continue;
					}
					const seller = p.tid;
					const out = { players: [p], picks: [] };
					const wants: { tid: number; want: number }[] = [];
					for (const t of teams) {
						if (t.tid !== seller && !t.disabled) {
							wants.push({ tid: t.tid, want: await view.useOf(t.tid, out) });
						}
					}
					wants.sort((a, b) => b.want - a.want || a.tid - b.tid);
					log(
						`OFFERS ${g.get("season")} ph${g.get("phase")} for ${named(p, seller, view)} at ${abbrev.get(seller)} (price ${(await view.price(out, [seller])).toFixed(3)}, use ${view.useOfNow(seller, out).toFixed(3)})`,
					);
					for (const { tid, want } of wants.slice(0, 8)) {
						const real = await view.bestRealOffer(
							seller,
							out,
							-1,
							new Set(),
							1,
							[tid],
						);
						if (!real) {
							log(`  ${abbrev.get(tid)} wants ${want.toFixed(3)}: no offer`);
							continue;
						}
						const buyerGain = await view.dealWorth(
							tid,
							seller,
							real.offer,
							out,
						);
						log(
							`  ${abbrev.get(tid)} wants ${want.toFixed(3)}: [${[...real.offer.players, ...real.offer.picks].map((x: any) => named(x)).join("; ")}] worth ${real.worth.toFixed(3)} to ${abbrev.get(seller)} (sure ${view.sureWorthNow(seller, tid, out, real.offer).toFixed(3)}), buyer +${buyerGain.toFixed(3)}`,
						);
					}
				}
			});
		}
		// FA_PICKS=1: each preseason, every team's own first-round picks in the
		// next four drafts, as the outlook expects them to land - the chance of
		// each pick in the round, the lottery included - for setting against
		// where they did land.
		const projectPicks = async () => {
			const o =
				await import("../../worker/core/realisticFrontOffice/outlook.ts");
			const { currentOutlook } =
				await import("../../worker/core/realisticFrontOffice/market.ts");
			const outlook: any = await currentOutlook();
			const next = g.get("season") + (g.get("phase") >= PHASE.DRAFT ? 1 : 0);
			const lottery = outlook.slotPicks?.[0];
			for (const dp of (await idb.cache.draftPicks.getAll()) as any[]) {
				if (dp.round !== 1 || typeof dp.season !== "number") {
					continue;
				}
				if (dp.season < next || dp.season > next + 3) {
					continue;
				}
				let weights: number[] = o.finishWeights(outlook, dp);
				if (lottery && lottery.length === weights.length) {
					const picks = weights.map(() => 0);
					weights.forEach((w, k) => {
						lottery[k].forEach((p: number, pick: number) => {
							picks[pick] = (picks[pick] ?? 0) + w * p;
						});
					});
					weights = picks;
				}
				const ts = await idb.cache.teamSeasons.indexGet(
					"teamSeasonsBySeasonTid",
					[g.get("season") - 1, dp.originalTid],
				);
				log(
					`PICKPROJ ${JSON.stringify({
						season: g.get("season"),
						dpid: dp.dpid,
						orig: dp.originalTid,
						holder: dp.tid,
						draft: dp.season,
						center: o.expectedPlace(outlook, dp),
						spread: o.draftPlaceSpread(outlook, dp),
						lastWinp: ts ? ts.won / Math.max(1, ts.won + ts.lost) : null,
						strength: outlook.teams.get(dp.originalTid)?.strength?.slice(0, 4),
						weights: weights.map((w) => Math.round(w * 1e4) / 1e4),
					})}`,
				);
			}
		};
		let board = new Map<number, number>();
		let boardFor = -1;
		let pickNo = 0;
		smartDraftWatchers.push((dp, worths, view: any) => {
			if (boardFor !== g.get("season")) {
				boardFor = g.get("season");
				pickNo = 0;
			}
			pickNo += 1;
			if (dp.round !== 1) {
				return;
			}
			log(
				`DRAFT ${g.get("season")} #${pickNo} ${abbrev.get(dp.tid)} (${title(view, dp.tid)}%): ${worths
					.slice(0, 4)
					.map(
						({ p, worth }) =>
							`${p.firstName} ${p.lastName} ${g.get("season") - p.born.year}y ${p.ratings.at(-1)!.ovr}/${p.ratings.at(-1)!.pot} board #${board.get(p.pid) ?? "?"} stay ${retentionOf(view.outlook, p.pid, dp.tid).toFixed(2)} worth ${worth.toFixed(4)}`,
					)
					.join(" | ")}`,
			);
		});
		const startSeason = g.get("season");
		const seasons = Number(env.FA_SEASONS ?? 3);
		log(`${startSeason} phase ${g.get("phase")}, ${seasons} seasons`);
		const t0 = performance.now();
		let guard = 0;
		while (
			!(
				g.get("season") >= startSeason + seasons &&
				g.get("phase") === PHASE.PRESEASON
			) &&
			guard++ < 10000
		) {
			const ph = g.get("phase");
			if (
				ph === PHASE.PLAYOFFS ||
				ph === PHASE.REGULAR_SEASON ||
				ph === PHASE.AFTER_TRADE_DEADLINE
			) {
				await game.play(ph === PHASE.PLAYOFFS ? 100 : 7, {});
			} else if (ph === PHASE.DRAFT) {
				const prospects = (
					await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
				).filter((p) => p.draft.year === g.get("season"));
				board = new Map(
					[...prospects]
						.sort((a, b) => b.value - a.value)
						.map((p, i) => [p.pid, i + 1]),
				);
				await draft.runPicks({ type: "untilEnd" }, {});
				if (g.get("phase") === PHASE.DRAFT) {
					await phase.newPhase(PHASE.AFTER_DRAFT, {});
				}
			} else if (ph === PHASE.FREE_AGENCY) {
				await freeAgents.play(g.get("daysLeft"), {});
				if (g.get("phase") === PHASE.FREE_AGENCY) {
					await phase.newPhase(PHASE.PRESEASON, {});
				}
			} else {
				await phase.newPhase((ph + 1) as any, {});
			}
			if (
				env.FA_PICKS &&
				g.get("phase") === PHASE.PRESEASON &&
				ph !== PHASE.PRESEASON
			) {
				await projectPicks();
			}
			if (g.get("phase") === PHASE.PRESEASON && ph !== PHASE.PRESEASON) {
				log(
					`-- ${g.get("season")} preseason, ${((performance.now() - t0) / 60000).toFixed(1)} min`,
				);
			}
		}
		log(
			`to ${g.get("season")} preseason in ${((performance.now() - t0) / 60000).toFixed(1)} min`,
		);
		// The league, in an export's shape.
		const players = await idb.merged.players.getAll({
			activeAndRetired: true,
		} as any);
		const teamSeasons: any[] = [];
		const events: any[] = [];
		for (let s = g.get("startingSeason"); s <= g.get("season"); s++) {
			teamSeasons.push(
				...((await idb.merged.teamSeasons.getAll({
					season: s,
				} as any)) as any[]),
			);
			events.push(
				...((await idb.merged.events.getAll({ season: s } as any)) as any[]),
			);
		}
		const gameAttributes: Record<string, unknown> = {};
		for (const key of [
			"season",
			"phase",
			"startingSeason",
			"smartCpu",
			"realisticFrontOffices",
			"userTids",
			"salaryCap",
			"luxuryPayroll",
			"minContract",
			"maxContract",
		]) {
			gameAttributes[key] = g.get(key as any);
		}
		fs.writeFileSync(
			`${out}.json`,
			JSON.stringify({
				version: 0,
				gameAttributes,
				players,
				teams: teams.map((t) => ({
					...t,
					seasons: teamSeasons.filter((ts) => ts.tid === t.tid),
				})),
				events,
			}),
		);
		log(`dumped to ${out}.json`);
	},
);
