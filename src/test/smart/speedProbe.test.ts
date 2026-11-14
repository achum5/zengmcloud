import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import type { Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import isUntradable from "../../worker/core/trade/isUntradable.ts";
import { advanceSeasons, createArenaLeague } from "../arena/runLeague.ts";

// What the GM's pieces cost (layer 2's speed budget): a franchise value, a
// use, a price, a deal's worth, a best real offer.
//
//   SPEED_PROBE=1 SPEED_OUT=<dir> npx vitest --run --project basketball \
//     src/test/smart/speedProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.SPEED_PROBE === "1")(
	"speed probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { buildOutlook } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { SmartView } = await import("../../worker/core/smartCpu/worth.ts");
		Math.random = makeSeededRandom(77);
		(idb as any).league = undefined;
		await createArenaLeague(9177);
		await advanceSeasons(1);
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		const t0 = performance.now();
		const outlook = await buildOutlook({ iterations: 1000 });
		const outlookSeconds = (performance.now() - t0) / 1000;
		const tids = (await idb.cache.teams.getAll())
			.filter((t) => !t.disabled)
			.map((t) => t.tid);
		const out: Record<string, number> = { outlookSeconds };
		const time = async (name: string, n: number, f: () => Promise<unknown>) => {
			const t = performance.now();
			await f();
			out[name] = (performance.now() - t) / n;
		};
		const view = new SmartView(outlook, []);
		const players: Player[] = [];
		for (const tid of tids) {
			players.push(
				...(await idb.cache.players.indexGetAll("playersByTid", tid))
					.filter((p) => !isUntradable(p).untradable)
					.sort((a, b) => b.value - a.value)
					.slice(0, 2),
			);
		}
		await time("holdingsMs", tids.length, async () => {
			for (const tid of tids) {
				await view.holdingsOf(tid);
			}
		});
		await time("valueMs (fresh holdings)", tids.length, async () => {
			for (const tid of tids) {
				view.value(tid, await view.holdingsOf(tid));
			}
		});
		await time("useOfMs (other team)", players.length, async () => {
			for (const [i, p] of players.entries()) {
				await view.useOf(tids[(i * 7) % tids.length]!, {
					players: [p],
					picks: [],
				});
			}
		});
		await time("priceMs", players.length, async () => {
			for (const p of players) {
				await view.price({ players: [p], picks: [] }, [p.tid]);
			}
		});
		await time("dealWorthMs (1 for 1)", players.length, async () => {
			for (const [i, p] of players.entries()) {
				const q = players[(i + 9) % players.length]!;
				await view.dealWorth(
					p.tid,
					q.tid,
					{ players: [p], picks: [] },
					{ players: [q], picks: [] },
				);
			}
		});
		// Where a franchise value's time goes (SPEED_PROFILE=1): a CPU profile of
		// values of rosters a player short, read with a summary script.
		if (env.SPEED_PROFILE === "1") {
			const inspector = await importNode("node:inspector/promises");
			const session = new inspector.Session();
			session.connect();
			await session.post("Profiler.enable");
			await session.post("Profiler.setSamplingInterval", { interval: 100 });
			await session.post("Profiler.start");
			const t = performance.now();
			let n = 0;
			for (const tid of tids) {
				const h = await view.holdingsOf(tid);
				for (const p of h.players.slice(0, 6)) {
					view.value(tid, {
						players: h.players.filter((x) => x !== p),
						picks: h.picks,
					});
					n += 1;
				}
			}
			out["profiledValueMs"] = (performance.now() - t) / n;
			const { profile } = await session.post("Profiler.stop");
			await fs.writeFile(
				`${env.SPEED_OUT ?? "."}/value.cpuprofile`,
				JSON.stringify(profile),
			);
			session.disconnect();
		}
		const fresh = new SmartView(outlook, []);
		await time("bestRealOfferMs (fresh view)", 20, async () => {
			for (const p of players.slice(0, 20)) {
				await fresh.bestRealOffer(p.tid, { players: [p], picks: [] }, -1);
			}
		});
		await fs.writeFile(
			`${env.SPEED_OUT ?? "."}/speed-probe.json`,
			JSON.stringify(out, null, 1),
		);
	},
);
