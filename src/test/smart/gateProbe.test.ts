import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// WHAT STOPS THE MARKET'S DEALS: an owner's league from opening night to the
// trade deadline, every market moment's players shopped tallied by where
// each got to (market.ts MarketTrace), with how far short the sellers who
// said no were, and the trades made - to try the market's settings on
// (MARKET_SET, MARGIN_SET, SMART_SET: loadLeague.ts).
// GP_SUMMER=1 plays on through the playoffs, the draft and free agency to
// the next opening night, and lists the trades made on the way.
//   GP_LEAGUE=<league.json> GP_OUT=<file> [GP_SEED=77] [GP_SUMMER=1] \
//     npx vitest --run --project basketball src/test/smart/gateProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

const quantiles = (xs: number[]) => {
	if (xs.length === 0) {
		return "-";
	}
	const s = [...xs].sort((a, b) => a - b);
	const at = (q: number) =>
		s[Math.min(s.length - 1, Math.floor(q * s.length))]!.toFixed(3);
	return `p10 ${at(0.1)}, median ${at(0.5)}, p90 ${at(0.9)}`;
};

test.skipIf(!env.GP_LEAGUE)(
	"what stops the market's deals",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.GP_OUT!, lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.GP_LEAGUE!);
		Math.random = makeSeededRandom(Number(env.GP_SEED ?? 77));
		const { g } = await import("../../worker/util/index.ts");
		const { idb } = await import("../../worker/db/index.ts");
		const { phase, league, game, draft, freeAgents } =
			await import("../../worker/core/index.ts");
		const { PHASE, PHASE_TEXT } = await import("../../common/constants.ts");
		const { smartHumans } =
			await import("../../worker/core/smartCpu/signing.ts");
		const market = await import("../../worker/core/smartCpu/market.ts");
		const { offerStats } = await import("../../worker/core/smartCpu/worth.ts");
		const { marginSettings } =
			await import("../../worker/core/realisticFrontOffice/margin.ts");
		smartHumans.tids = [...g.get("userTids")];
		await league.setGameAttributes({ spectator: true });
		await phase.newPhase(PHASE.REGULAR_SEASON, {});
		const season = g.get("season");
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		log(
			`season ${season}: minGain ${market.marketSettings.minGain}, margin z ${marginSettings.z}, margins ${market.marketSettings.margins}, sellerShare ${market.marketSettings.sellerShare.toFixed(3)}, waitShare ${market.marketSettings.waitShare}, offerBudget ${market.marketSettings.offerBudget}`,
		);
		const stages = new Map<string, number>();
		// "no offer" split: nobody wanted him more than his team needs, or
		// someone did and no offer cleared their own test.
		let noBidders = 0;
		const buyersMade = new Map<string, number>();
		const sayNo: number[] = [];
		const nearMisses: string[] = [];
		const name = (p: any) =>
			`${p.firstName} ${p.lastName} (${season - p.born.year}, ${p.ratings.at(-1).ovr})`;
		market.smartTraceWatchers.push((t) => {
			stages.set(t.stage, (stages.get(t.stage) ?? 0) + 1);
			if (t.stage === "no offer" && (t.buyers?.length ?? 0) === 0) {
				noBidders += 1;
			}
			if (t.stage === "seller says no" && t.sellerGain !== undefined) {
				sayNo.push(t.sellerGain);
			}
			if (
				t.stage === "within margin" ||
				t.stage === "unfair" ||
				t.stage === "waits" ||
				t.stage === "changed"
			) {
				nearMisses.push(
					`${t.stage}: ${abbrev.get(t.buyer!)} for ${name(t.p)} of ${abbrev.get(t.seller)}, seller ${t.sellerGain?.toFixed(4)}, buyer ${t.buyerGain?.toFixed(4)}${t.why ? ` (${t.why})` : ""}`,
				);
			}
		});
		const made: string[] = [];
		market.smartDealWatchers.push((d) => {
			for (const tid of [d.buyer, d.seller]) {
				const a = abbrev.get(tid)!;
				buyersMade.set(a, (buyersMade.get(a) ?? 0) + 1);
			}
			made.push(
				`${env.GP_SUMMER ? `[${(PHASE_TEXT as any)[g.get("phase")]}] ` : ""}${abbrev.get(d.buyer)} gets ${d.out.players.map(name).join("; ")} from ${abbrev.get(d.seller)} for [${[...d.offer.players.map(name), ...d.offer.picks.map((dp: any) => `${dp.season} R${dp.round} ${abbrev.get(dp.originalTid)}`)].join("; ")}]: seller ${d.sellerGain.toFixed(4)}, buyer ${d.buyerGain.toFixed(4)}`,
			);
		});
		let moments = 0;
		let found = 0;
		market.smartMomentWatchers.push(async (deals) => {
			moments += 1;
			found += deals.length;
		});
		const start = performance.now();
		for (let night = 1; night < 300; night++) {
			if (g.get("phase") !== PHASE.REGULAR_SEASON) {
				break;
			}
			await game.play(1, {});
		}
		log(
			`to the deadline: ${moments} moments, ${found} deals found, ${made.length} made, ${offerStats.tried} offers asked, ${((performance.now() - start) / 60000).toFixed(1)} min`,
		);
		log(
			`players shopped, by where each got to: ${[...stages]
				.sort((a, b) => b[1] - a[1])
				.map(([k, v]) => `${k} ${v}`)
				.join(", ")}`,
		);
		log(
			`  no offer with nobody bidding: ${noBidders}; teams in the trades made: ${[
				...buyersMade,
			]
				.sort((a, b) => b[1] - a[1])
				.map(([k, v]) => `${k} ${v}`)
				.join(", ")}`,
		);
		log(`seller says no, the seller's gain: ${quantiles(sayNo)}`);
		log(
			`  within 0.01 of yes: ${sayNo.filter((x) => x > -0.01).length}, within 0.03: ${sayNo.filter((x) => x > -0.03).length}`,
		);
		log(`\nmade:`);
		for (const m of made) {
			log(`  ${m}`);
		}
		log(`\nnear misses (${nearMisses.length}):`);
		for (const m of nearMisses) {
			log(`  ${m}`);
		}
		// The rumor mill's stories (smartCpu/rumors.ts), as the News Feed has
		// them.
		const rumors = (await idb.cache.events.getAll()).filter(
			(e) => e.type === "rumor" && e.season === season,
		);
		log(`\nrumors (${rumors.length}):`);
		for (const e of rumors) {
			log(
				`  ${e.date ?? ""} [${e.rumor?.tier}] ${(e.text ?? "").replace(/<[^>]+>/g, "")}`,
			);
		}
		if (!env.GP_SUMMER) {
			return;
		}
		const toDeadline = made.length;
		let guard = 0;
		while (
			!(g.get("season") > season && g.get("phase") === PHASE.REGULAR_SEASON) &&
			guard++ < 1000
		) {
			const ph = g.get("phase");
			if (
				ph === PHASE.PLAYOFFS ||
				ph === PHASE.REGULAR_SEASON ||
				ph === PHASE.AFTER_TRADE_DEADLINE
			) {
				await game.play(ph === PHASE.PLAYOFFS ? 100 : 7, {});
			} else if (ph === PHASE.DRAFT) {
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
		}
		log(
			`\nafter the deadline to ${g.get("season")}'s opening night: ${made.length - toDeadline} made, ${((performance.now() - start) / 60000).toFixed(1)} min in all`,
		);
		for (const m of made.slice(toDeadline)) {
			log(`  ${m}`);
		}
	},
);
