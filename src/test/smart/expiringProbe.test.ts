import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// How the market sees the good players whose deals run out this season, on
// teams out of the race: his chance of staying (the game's own willingness),
// his use to his team, what the keenest teams would pay, and whether he is
// put up for sale at all - to see why such players walk unsold.
//   EX_LEAGUE=<league.json> EX_OUT=<file> [EX_DAYS=60] npx vitest --run \
//     --project basketball src/test/smart/expiringProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.EX_LEAGUE)(
	"expiring players probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.EX_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.EX_LEAGUE!);
		const { league, game, phase } = await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		if (env.EX_DAYS) {
			if (g.get("phase") < PHASE.REGULAR_SEASON) {
				await phase.newPhase(PHASE.REGULAR_SEASON, {});
			}
			await game.play(Number(env.EX_DAYS), {});
		}
		const { idb } = await import("../../worker/db/index.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { ensureRetention, retentionKey } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		await view.prime();
		const keen: number[] = await view.keenest();
		const season = g.get("season");
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const title = (tid: number) => outlook.teams.get(tid)?.title?.[0] ?? 0;
		log(
			`${season} phase ${g.get("phase")}; keenest: ${keen.map((t) => `${abbrev.get(t)} ${(100 * title(t)).toFixed(1)}%`).join(", ")}`,
		);
		const players = (
			await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
		).filter(
			(p: any) =>
				// EX_ALL: veterans under contract too, not only expiring deals.
				(env.EX_ALL
					? season - p.born.year >= 27
					: contractOf(p).exp <= season) &&
				p.ratings.at(-1).ovr >= Number(env.EX_OVR ?? 60) &&
				title(p.tid) < 0.02,
		);
		for (const p of players as any[]) {
			const a = { players: [p], picks: [] };
			await ensureRetention(outlook, [p], p.tid);
			const stay = outlook.retention.get(retentionKey(p.pid, p.tid));
			const use = await view.useOf(p.tid, a);
			const price = await view.price(a, [p.tid]);
			const bids = keen
				.filter((t) => t !== p.tid)
				.map((t) => ({ t, bid: view.bidNow(t, a) }))
				.sort((x, y) => y.bid - x.bid);
			const top = bids[0];
			// The market's best real offer for him, as a moment would ask.
			let offer = "";
			if (env.EX_ALL || (stay !== undefined && stay < 0.6)) {
				const real = await view.bestRealOffer(p.tid, a, -1);
				offer = real
					? ` || best offer ${abbrev.get(real.buyer)}: [${[...real.offer.players.map((x: any) => `${x.firstName} ${x.lastName} ${x.ratings.at(-1).ovr}`), ...real.offer.picks.map((dp: any) => `${dp.season} R${dp.round}`)].join("; ")}] worth ${real.worth.toFixed(4)} to the seller`
					: " || no real offer";
			}
			log(
				`${p.firstName} ${p.lastName} ${season - p.born.year}y ${p.ratings.at(-1).ovr} $${(contractOf(p).amount / 1000).toFixed(1)}M, ${abbrev.get(p.tid)} (${(100 * title(p.tid)).toFixed(1)}%) traits ${(p.moodTraits ?? []).join("")}: stay ${stay?.toFixed(2)}, use ${use.toFixed(4)}, price ${price.toFixed(4)}, top bid ${top ? `${abbrev.get(top.t)} ${top.bid.toFixed(4)}` : "-"} => surplus ${top ? (top.bid - Math.max(use, price)).toFixed(4) : "-"} | bids ${bids.map((b) => `${abbrev.get(b.t)} ${Number.isFinite(b.bid) ? b.bid.toFixed(3) : "no"}`).join(", ")}${offer}`,
			);
		}
		const { offerStats } = await import("../../worker/core/smartCpu/worth.ts");
		log(`offer stats: ${JSON.stringify(offerStats)}`);
	},
);
