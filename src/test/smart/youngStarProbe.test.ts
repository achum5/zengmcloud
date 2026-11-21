import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE YOUNG STAR PROBE: what a young player is worth to the team that has
// him, season by season, against what he is worth to the teams keenest on
// him - and whether the market would sell him now, and for what. Why do
// rebuilding teams sell the players they should build around?
//   YS_LEAGUE=<league.json> YS_OUT=<file> YS_PIDS=<pids> npx vitest --run \
//     --project basketball src/test/smart/youngStarProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.YS_LEAGUE)(
	"young star probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.YS_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.YS_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const o = await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		await view.prime();
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const season = g.get("season");
		const keen: number[] = [...view.outlook.teams.keys()];
		const none = { players: [], picks: [] };
		const fmt = (xs: number[], k = 3) => xs.map((x) => x.toFixed(k)).join(" ");
		for (const pid of (env.YS_PIDS ?? "").split(",").map(Number)) {
			const p = await idb.cache.players.get(pid);
			if (!p) {
				continue;
			}
			const tid = p.tid;
			await o.ensureRetention(outlook, [p], tid);
			for (const k of keen) {
				await o.ensureRetention(outlook, [p], k);
			}
			const r = p.ratings.at(-1)!;
			const one = { players: [p], picks: [] };
			const title = (100 * (outlook.teams.get(tid)?.title?.[0] ?? 0)).toFixed(
				1,
			);
			log(
				`\n${p.firstName} ${p.lastName} ${season - p.born.year}y ${r.ovr}/${r.pot}, ${abbrev.get(tid)} (${title}% title now); his chance of re-signing there ${o.retentionOf(outlook, pid, tid).toFixed(2)}`,
			);
			{
				const { default: moodInfo } =
					await import("../../worker/core/player/moodInfo.ts");
				const mi: any = await moodInfo(p, tid);
				const c = mi.components;
				log(
					`  mood at ${abbrev.get(tid)}: ${Object.entries(c)
						.filter(([, v]) => v !== 0)
						.map(([k, v]: any) => `${k} ${v.toFixed(2)}`)
						.join(", ")}; sum ${Object.values(c)
						.filter((v: any) => typeof v === "number")
						.reduce((a: number, v: any) => a + v, 0)
						.toFixed(
							2,
						)}, value ${p.value.toFixed(1)}, willing ${mi.probWilling.toFixed(2)}, contract ${JSON.stringify((await import("../../worker/core/realisticFrontOffice/contractOf.ts")).contractOf(p))}, traits ${p.moodTraits.join("")}`,
				);
			}
			// His team, season by season, with and without him: strength, title
			// chance, and what each season is worth (title and run, discounted).
			const h = view.heldNow(tid);
			const without = view.swappedFrom(h, one, none);
			const path = (x: any) =>
				o.teamStrengths(outlook, tid, x.players, x.picks);
			const a = path(h);
			const b = path(without);
			const sv = (s: number[]) =>
				s.map((x, y) => o.seasonValue(outlook, y, x, tid));
			const va = sv(a);
			const vb = sv(b);
			log(`  ${abbrev.get(tid)} strength with him:    ${fmt(a, 1)}`);
			log(`  ${abbrev.get(tid)} strength without him: ${fmt(b, 1)}`);
			log(
				`  his worth to ${abbrev.get(tid)} by season: ${fmt(va.map((x, y) => x - vb[y]!))} = ${(va.reduce((s, x) => s + x, 0) - vb.reduce((s, x) => s + x, 0)).toFixed(3)} (use ${view.useOfNow(tid, one).toFixed(3)})`,
			);
			log(
				`  ${abbrev.get(tid)} title chance by season with him: ${a.map((x, y) => (100 * o.curveAt(o.curvesFor(outlook, tid).title[y]!, x)).toFixed(1)).join(" ")}`,
			);
			// The keenest teams' worth for him, the market's price.
			const uses = [];
			for (const k of keen) {
				if (k !== tid) {
					uses.push(`${abbrev.get(k)} ${view.useOfNow(k, one).toFixed(3)}`);
				}
			}
			log(
				`  worth to the keenest: ${uses.join(", ")}; market price ${view.priceNow(one, [tid]).toFixed(3)}`,
			);
			// For a contender, by season.
			const top = keen.find((k) => k !== tid)!;
			const hk = view.heldNow(top);
			const ka = path2(o, outlook, top, view.swappedFrom(hk, none, one));
			const kb = path2(o, outlook, top, hk);
			const kva = ka.map((x: number, y: number) =>
				o.seasonValue(outlook, y, x, top),
			);
			const kvb = kb.map((x: number, y: number) =>
				o.seasonValue(outlook, y, x, top),
			);
			log(
				`  his worth to ${abbrev.get(top)} by season: ${fmt(kva.map((x: number, y: number) => x - kvb[y]!))}`,
			);
			// Would the market sell him now? (And, offer by offer from the
			// keenest, why not: YS_WHY=1.)
			if (env.YS_WHY) {
				const { offerStats } =
					await import("../../worker/core/smartCpu/worth.ts");
				const { contractOf } =
					await import("../../worker/core/realisticFrontOffice/contractOf.ts");
				const piece = (x: any) =>
					"pid" in x
						? `${x.lastName} ${x.ratings.at(-1).ovr} (${(contractOf(x).amount / 1000).toFixed(1)}M)`
						: `${x.season} R${x.round}`;
				for (const k of keen.filter((k) => k !== tid)) {
					const before: Record<string, number> = { ...offerStats };
					const made = await view.offerFrom(k, tid, one);
					const why = Object.entries(offerStats)
						.filter(([key, v]) => v !== before[key] && key !== "tried")
						.map(([key]) => key)
						.join(",");
					if (!made) {
						log(`  ${abbrev.get(k)}: no offer (${why})`);
						continue;
					}
					const toSeller = await view.dealWorth(tid, k, one, made.offer);
					const sure = view.sureWorthNow(tid, k, one, made.offer);
					const toBuyer = await view.dealWorth(k, tid, made.offer, one);
					log(
						`  ${abbrev.get(k)}: offers ${[...made.offer.players, ...made.offer.picks].map(piece).join("; ")} - to seller ${toSeller.toFixed(3)} (sure ${sure.toFixed(3)}, margin ${view.marginNow(tid, one, made.offer).toFixed(3)}), to buyer ${toBuyer.toFixed(3)}`,
					);
				}
			}
			const real = await view.bestRealOffer(tid, one, -1);
			if (real) {
				const name = (x: any) =>
					"pid" in x
						? `${x.firstName} ${x.lastName} ${season - x.born.year}y ${x.ratings.at(-1).ovr}/${x.ratings.at(-1).pot}`
						: `${x.season} R${x.round} ${abbrev.get(x.originalTid)}`;
				log(
					`  best real offer: ${abbrev.get(real.buyer)} [${[...real.offer.players, ...real.offer.picks].map(name).join("; ")}] - worth ${real.worth.toFixed(3)} to ${abbrev.get(tid)} (its margin ${view.marginNow(tid, one, real.offer).toFixed(3)})`,
				);
			} else {
				log("  no real offer");
			}
		}
	},
);

const path2 = (o: any, outlook: any, tid: number, h: any) =>
	o.teamStrengths(outlook, tid, h.players, h.picks);
