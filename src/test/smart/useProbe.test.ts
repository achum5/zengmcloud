import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Where a player's use to a team comes from, season by season: his
// projected rating, the team's strength with and without him, and the title
// odds each gives (docs/smart-cpu.md).
//   USE_LEAGUE=<league.json> USE_TEAM=LAC USE_PID=3250 npx vitest --run \
//     --project basketball src/test/smart/useProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.USE_LEAGUE)("use probe", { timeout: 1_800_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	await loadLeagueFile(env.USE_LEAGUE!);
	const { league } = await import("../../worker/core/index.ts");
	await league.setGameAttributes({
		realisticFrontOffices: true,
		smartCpu: true,
		spectator: true,
	});
	const { currentOutlook } =
		await import("../../worker/core/realisticFrontOffice/market.ts");
	const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
	const outlookMod =
		await import("../../worker/core/realisticFrontOffice/outlook.ts");
	const outlook: any = await currentOutlook();
	const view: any = viewFor(outlook);
	await view.prime();
	const teams = await idb.cache.teams.getAll();
	const tid = teams.find((t) => t.abbrev === (env.USE_TEAM ?? "LAC"))!.tid;
	const pids = (env.USE_PID ?? "3250").split(",").map(Number);
	const h = await view.holdingsOf(tid);
	for (const pid of pids) {
		const p: any = await idb.cache.players.get(pid);
		const held = h.players.some((x: any) => x.pid === pid);
		const withHim = held ? h : { players: [...h.players, p], picks: h.picks };
		const without = {
			players: withHim.players.filter((x: any) => x.pid !== pid),
			picks: withHim.picks,
		};
		const proj = outlookMod.projectPlayer(p, g.get("season"));
		const strengthOf = (x: any) =>
			outlookMod.teamStrengths(outlook, tid, x.players, x.picks);
		const sW = strengthOf(withHim);
		const sO = strengthOf(without);
		console.log(
			`${p.firstName} ${p.lastName} age ${g.get("season") - p.born.year} ovr ${p.ratings.at(-1).ovr}/${p.ratings.at(-1).pot} on ${held ? "the team" : "another team"}; use ${view.useOfNow(tid, { players: [p], picks: [] }).toFixed(4)}`,
		);
		console.log(
			" y | his ovr | strength with / without | title with / without | weighted diff",
		);
		let total = 0;
		for (let y = 0; y < sW.length; y++) {
			const vW = outlookMod.seasonValue(outlook, y, sW[y]!, tid);
			const vO = outlookMod.seasonValue(outlook, y, sO[y]!, tid);
			total += vW - vO;
			console.log(
				`${String(y).padStart(2)} | ${proj.ovr?.[y]?.toFixed?.(1) ?? "?"} | ${sW[y]!.toFixed(2)} / ${sO[y]!.toFixed(2)} | ${(vW / 0.9 ** y).toFixed(4)} / ${(vO / 0.9 ** y).toFixed(4)} | ${(vW - vO).toFixed(4)}`,
			);
		}
		console.log(`sum of season values ${total.toFixed(4)} (before money)`);
	}
});
