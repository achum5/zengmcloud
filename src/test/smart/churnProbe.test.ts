import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Why a team signs a free agent in season only to release him (the owner's
// league under the smart CPU, 2023 after the deadline): every release logged
// with where it came from and the roster around it.
//   CHURN_LEAGUE=<league.json> [CHURN_TEAM=MEM] [CHURN_DAYS=20] npx vitest \
//     --run --project basketball src/test/smart/churnProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.CHURN_LEAGUE)(
	"churn probe",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const { game, league, player } = await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		await loadLeagueFile(env.CHURN_LEAGUE!);
		await league.setGameAttributes({
			realisticFrontOffices: true,
			smartCpu: true,
			spectator: true,
		});
		const teams = await idb.cache.teams.getAll();
		const focus = teams.find(
			(t) => t.abbrev === (env.CHURN_TEAM ?? "MEM"),
		)!.tid;
		const label = (p: any) =>
			`${p.firstName} ${p.lastName} ${p.ratings.at(-1).ovr} v${Math.round(p.value)} $${contractOf(p).amount}->${contractOf(p).exp}`;
		const release = player.release;
		(player as any).release = async (p: any, justDrafted: boolean) => {
			if (p.tid === focus) {
				const roster = await idb.cache.players.indexGetAll(
					"playersByTid",
					focus,
				);
				// Both readings of whom it can best do without, on this roster.
				const { currentOutlook } =
					await import("../../worker/core/realisticFrontOffice/market.ts");
				const { viewFor } =
					await import("../../worker/core/smartCpu/signing.ts");
				const view: any = viewFor(await currentOutlook());
				const h = await view.holdingsOf(focus);
				const lows = [...h.players]
					.sort((a: any, b: any) => a.value - b.value || a.pid - b.pid)
					.slice(0, 8);
				const [kept] = view.afterCuts(focus, h);
				const cutByAfterCuts = h.players.filter(
					(x: any) => !kept.players.includes(x),
				);
				console.log(
					`  afterCuts cuts ${cutByAfterCuts.map(label).join(", ")}; value with all ${view.value(focus, h).toFixed(5)}`,
				);
				for (const x of lows) {
					const without = {
						players: h.players.filter((y: any) => y !== x),
						picks: h.picks,
					};
					console.log(
						`  cut ${label(x)}: value ${view.value(focus, without, view.owedIfReleased(x)).toFixed(5)} (raw, no dead ${view.raw(focus, without, []).toFixed(5)})`,
					);
				}
				const stack = (new Error().stack ?? "")
					.split("\n")
					.slice(2, 7)
					.map((l) => l.trim().replace(/\(.*\/src\//, "(src/"))
					.join(" <- ");
				console.log(
					`RELEASE day ${g.get("daysLeft")} ph${g.get("phase")} ${label(p)} | roster ${roster.length}: ${roster
						.sort((a: any, b: any) => a.value - b.value)
						.slice(0, 5)
						.map(label)
						.join("; ")}\n   from ${stack}`,
				);
			}
			return release(p, justDrafted);
		};
		const conditions = {} as any;
		while (g.get("phase") < PHASE.AFTER_TRADE_DEADLINE) {
			await game.play(1, conditions);
		}
		let seenEid = Infinity;
		const evs = await idb.cache.events.getAll();
		seenEid = evs.reduce((m: number, e: any) => Math.max(m, e.eid), 0);
		for (let d = 0; d < Number(env.CHURN_DAYS ?? 20); d++) {
			await game.play(1, conditions);
			const now = (await idb.cache.events.getAll()).filter(
				(e: any) => e.eid > seenEid && e.tids?.includes(focus),
			);
			for (const e of now) {
				seenEid = Math.max(seenEid, e.eid);
				if (e.type === "freeAgent" || e.type === "release") {
					const p = await idb.cache.players.get(e.pids![0]);
					console.log(`EVENT ${e.type} ${p ? label(p) : e.pids}`);
				}
			}
		}
	},
);
