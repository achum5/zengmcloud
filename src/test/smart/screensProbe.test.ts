import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import type { TradeTeams } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { advanceSeasons, createArenaLeague } from "../arena/runLeague.ts";

// The trade screens with the smart CPU on, timed (layer 4, before it ships):
// the trading block (every CPU team's offer for a player of the user's), what
// would make this work (a CPU player asked for), and a human offer judged.
//   SCREENS_PROBE=1 SCREENS_OUT=<dir> npx vitest --run --project basketball \
//     src/test/smart/screensProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.SCREENS_PROBE === "1")(
	"screens probe",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { default: makeItWork } =
			await import("../../worker/core/trade/makeItWork.ts");
		const { tradeValueCalculator } =
			await import("../../worker/core/vanillaPlus/index.ts");
		const { smartJudgeOffer } =
			await import("../../worker/core/smartCpu/humanTrades.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const isUntradable = (
			await import("../../worker/core/trade/isUntradable.ts")
		).default;
		Math.random = makeSeededRandom(Number(env.SCREENS_SEED ?? 77));
		(idb as any).league = undefined;
		await createArenaLeague(Number(env.SCREENS_LEAGUE ?? 9177));
		await advanceSeasons(1);
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		g.setWithoutSavingToDB("smartCpu", true);
		const user = 0;
		g.setWithoutSavingToDB("spectator", false);
		g.setWithoutSavingToDB("userTid", user);
		g.setWithoutSavingToDB("userTids", [user]);
		const t0 = performance.now();
		await currentOutlook();
		const outlookSeconds = (performance.now() - t0) / 1000;
		const mine = (await idb.cache.players.indexGetAll("playersByTid", user))
			.filter((p) => !isUntradable(p).untradable)
			.sort((a, b) => b.value - a.value);
		const valueChangeCalculator = await tradeValueCalculator();
		const rows: Record<string, unknown>[] = [];
		const time = async <T>(label: string, f: () => Promise<T>) => {
			const t = performance.now();
			const result = await f();
			rows.push({ label, seconds: (performance.now() - t) / 1000 });
			return result;
		};
		// The trading block, for the user's second best and fifth best.
		for (const p of [mine[1]!, mine[4]!]) {
			let offers = 0;
			await time(`trading block for ${p.pid} (value ${p.value})`, async () => {
				for (const t of await idb.cache.teams.getAll()) {
					if (t.disabled || t.tid === user) {
						continue;
					}
					const teams: TradeTeams = [
						{
							tid: user,
							pids: [p.pid],
							pidsExcluded: [],
							dpids: [],
							dpidsExcluded: [],
						},
						{
							tid: t.tid,
							pids: [],
							pidsExcluded: [],
							dpids: [],
							dpidsExcluded: [],
						},
					];
					const found = await makeItWork(teams, {
						holdUserConstant: true,
						maxAssetsToAdd: 5,
						valueChangeCalculator,
					});
					if (found) {
						offers += 1;
					}
				}
			});
			rows.at(-1)!.offers = offers;
		}
		// What would make this work: a CPU team's best player asked for.
		for (const tid of [5, 17]) {
			const theirs = (await idb.cache.players.indexGetAll("playersByTid", tid))
				.filter((p) => !isUntradable(p).untradable)
				.sort((a, b) => b.value - a.value);
			const teams: TradeTeams = [
				{
					tid: user,
					pids: [],
					pidsExcluded: [],
					dpids: [],
					dpidsExcluded: [],
				},
				{
					tid,
					pids: [theirs[0]!.pid],
					pidsExcluded: [],
					dpids: [],
					dpidsExcluded: [],
				},
			];
			const found = await time(`make it work for ${theirs[0]!.pid}`, () =>
				makeItWork(teams, { holdUserConstant: false, valueChangeCalculator }),
			);
			rows.at(-1)!.found = found
				? found.map((side) => [side.pids.length, side.dpids.length])
				: null;
			// The offer judged, as the trade screen's Propose does.
			if (found) {
				const verdict = await time(
					`judge the offer for ${theirs[0]!.pid}`,
					() =>
						smartJudgeOffer(
							tid,
							user,
							{ pids: found[1].pids, dpids: found[1].dpids },
							{ pids: found[0].pids, dpids: found[0].dpids },
						),
				);
				rows.at(-1)!.verdict = verdict;
			}
		}
		await fs.writeFile(
			`${env.SCREENS_OUT ?? "."}/screens.json`,
			JSON.stringify({ outlookSeconds, rows }, null, 1),
		);
	},
);
