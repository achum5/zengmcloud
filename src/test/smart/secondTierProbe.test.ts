import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE SECOND TIER: one market moment from a league file, each deal open and
// each made with its buyer's place among the franchises (by franchise value)
// and its gain - to see whether teams just below the best are ever offered
// the players they would gain from, and where the offers fall away.
// ST_BUDGET sets the offers a moment may ask for.
//   ST_LEAGUE=<league.json> ST_OUT=<file> [ST_BUDGET=400] \
//     npx vitest --run --project basketball src/test/smart/secondTierProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.ST_LEAGUE)(
	"second tier probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.ST_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.ST_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		// ST_PHASE=<n>: the moment as if in that phase (1: a file saved just
		// past the deadline, read as before it).
		if (env.ST_PHASE) {
			await league.setGameAttributes({ phase: Number(env.ST_PHASE) as any });
		}
		const { idb } = await import("../../worker/db/index.ts");
		const market = await import("../../worker/core/smartCpu/market.ts");
		if (env.ST_BUDGET) {
			market.marketSettings.offerBudget = Number(env.ST_BUDGET);
		}
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		let rank = new Map<number, number>();
		const title = new Map<number, number>();
		const who = (tid: number) =>
			`${abbrev.get(tid)} (#${rank.get(tid)}, ${(100 * (title.get(tid) ?? 0)).toFixed(1)}%)`;
		const name = (x: any) =>
			"pid" in x
				? `${x.firstName} ${x.lastName} ${x.ratings.at(-1).ovr}`
				: `${x.season} R${x.round} ${abbrev.get(x.originalTid)}`;
		market.smartMomentWatchers.push(async (open, view: any) => {
			const rows = [];
			for (const tid of view.outlook.teams.keys()) {
				rows.push({ tid, v: view.value(tid, view.heldNow(tid)) });
				title.set(tid, view.outlook.teams.get(tid)?.title?.[0] ?? 0);
			}
			rows.sort((a, b) => b.v - a.v);
			rank = new Map(rows.map((r, i) => [r.tid, i + 1]));
			log(`OPEN ${open.length} deals`);
			for (const d of open) {
				log(
					`  ${who(d.buyer)} buys ${d.out.players.map(name).join("; ")} from ${who(d.seller)}; seller +${d.sellerGain.toFixed(4)}`,
				);
			}
		});
		market.smartTraceWatchers.push((t) => {
			if (t.stage === "shopped" || t.stage === "no offer") {
				return;
			}
			log(
				`TRACE ${t.stage}: ${name(t.p)} of ${who(t.seller)}${t.buyer !== undefined ? ` to ${who(t.buyer)}` : ""}${t.sellerGain !== undefined ? ` seller ${t.sellerGain.toFixed(4)}` : ""}${t.buyerGain !== undefined ? ` buyer ${t.buyerGain.toFixed(4)}` : ""}${t.why ? ` | ${t.why}` : ""}${t.offer ? ` for [${[...t.offer.players, ...t.offer.picks].map(name).join("; ")}]` : ""}`,
			);
		});
		market.smartDealWatchers.push((d) => {
			log(
				`MADE ${who(d.buyer)} gets [${d.out.players.map(name).join("; ")}] from ${who(d.seller)} for [${[...d.offer.players, ...d.offer.picks].map(name).join("; ")}] | buyer +${d.buyerGain.toFixed(4)} seller +${d.sellerGain.toFixed(4)}`,
			);
		});
		// ST_PROFILE=<file>: the moment's CPU profile, for chrome://inspect.
		const inspector = env.ST_PROFILE
			? await importNode("node:inspector")
			: null;
		const session = inspector ? new inspector.Session() : null;
		const post = (method: string, params?: object) =>
			new Promise<any>((resolve, reject) =>
				session.post(method, params ?? {}, (err: any, res: any) =>
					err ? reject(err) : resolve(res),
				),
			);
		if (session) {
			session.connect();
			await post("Profiler.enable");
			await post("Profiler.setSamplingInterval", { interval: 1000 });
			await post("Profiler.start");
		}
		const t0 = Date.now();
		await market.marketMoment();
		if (session) {
			const { profile } = await post("Profiler.stop");
			fs.writeFileSync(env.ST_PROFILE!, JSON.stringify(profile));
		}
		const { smartClock } = await import("../../worker/core/smartCpu/clock.ts");
		log(
			Object.entries(smartClock)
				.filter(([k]) => !k.includes("@"))
				.sort((a, b) => b[1].ms - a[1].ms)
				.map(([k, v]) => `  ${k}: ${(v.ms / 1000).toFixed(1)}s in ${v.n}`)
				.join("\n"),
		);
		log(
			`season ${g.get("season")} phase ${g.get("phase")}: ${((Date.now() - t0) / 1000).toFixed(0)} s; ${JSON.stringify(market.marketStats)}; offers ${JSON.stringify((await import("../../worker/core/smartCpu/worth.ts")).offerStats)}`,
		);
	},
);
