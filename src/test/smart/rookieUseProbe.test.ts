import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Where a contender's bid for a next-draft prospect as a rookie comes from:
// its strength each season with and without him, and what each season is
// worth - for prospects at RU_SLOTS of the next class.
//   RU_LEAGUE=<league.json> RU_OUT=<file> [RU_SLOTS=1,10,30,45] npx vitest \
//     --run --project basketball src/test/smart/rookieUseProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.RU_LEAGUE)(
	"rookie use probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.RU_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.RU_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const { PLAYER } = await import("../../common/constants.ts");
		const o = await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const { pickSalaries } =
			await import("../../worker/core/realisticFrontOffice/payroll.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		await view.prime();
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const keen: number[] = await view.keenest();
		const next = g.get("season") + 1;
		const n = teams.length;
		const salary = pickSalaries();
		const prospects = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
		)
			.filter((p) => p.draft.year === next)
			.sort((a, b) => b.value - a.value || a.pid - b.pid);
		for (const pair of (env.RU_PIDS ?? "").split(",").filter(Boolean)) {
			const [pidS, ab] = pair.split(":");
			const p: any = await idb.cache.players.get(Number(pidS));
			const tid = teams.find((t) => t.abbrev === ab)!.tid;
			await o.ensureRetention(outlook, [p], tid);
			const h = view.heldNow(tid);
			const has = h.players.some((x: any) => x.pid === p.pid);
			const withP = has ? h.players : [...h.players, p];
			const withoutP = h.players.filter((x: any) => x.pid !== p.pid);
			const a = o.teamStrengths(outlook, tid, withP, h.picks);
			const b = o.teamStrengths(outlook, tid, withoutP, h.picks);
			const va = a.map((x: number, y: number) =>
				o.seasonValue(outlook, y, x, tid),
			);
			const vb = b.map((x: number, y: number) =>
				o.seasonValue(outlook, y, x, tid),
			);
			const r = p.ratings.at(-1)!;
			const proj = o.projectPlayer(p, g.get("season"));
			log(
				`\n${p.firstName} ${p.lastName} ${g.get("season") - p.born.year}y ${r.ovr}/${r.pot} at ${ab}, proj ${[1, 2, 3, 4, 5, 6].map((y) => proj.ovr[y]!.toFixed(0)).join(" ")}; use ${view.useOfNow(tid, { players: [p], picks: [] }).toFixed(3)} stay ${o.retentionOf(outlook, p.pid, tid).toFixed(2)}`,
			);
			log(`  strength ${b.map((x: number) => x.toFixed(1)).join(" ")}`);
			log(
				`  +him     ${a.map((x: number, y: number) => (x - b[y]!).toFixed(2)).join(" ")}`,
			);
			log(
				`  value +  ${va.map((x: number, y: number) => (x - vb[y]!).toFixed(3)).join(" ")}`,
			);
		}
		for (const slot1 of (env.RU_SLOTS ?? "1,10,30,45").split(",").map(Number)) {
			const slot = slot1 - 1;
			const p = prospects[slot]!;
			const round = Math.floor(slot / n) + 1;
			const rookie = {
				...p,
				contractDemand: {
					amount: salary({ slot, round, years: 1, ovr: 0 }),
					exp: next + (round === 1 ? 3 : 2),
					rookie: true,
				},
			};
			const r = p.ratings.at(-1)!;
			const proj = o.projectPlayer(p, g.get("season"));
			log(
				`\n#${slot1} ${p.firstName} ${p.lastName} ${next - p.born.year}y ${r.ovr}/${r.pot}, proj ${[1, 2, 3, 4, 5, 6].map((y) => proj.ovr[y]!.toFixed(0)).join(" ")}`,
			);
			const forTeams = env.RU_TEAMS
				? env.RU_TEAMS.split(",").map(
						(ab) => teams.find((t) => t.abbrev === ab)!.tid,
					)
				: keen.slice(0, 3);
			for (const tid of forTeams) {
				await o.ensureRetention(outlook, [rookie as any], tid);
				const h = view.heldNow(tid);
				const a = o.teamStrengths(
					outlook,
					tid,
					[...h.players, rookie],
					h.picks,
				);
				const b = o.teamStrengths(outlook, tid, h.players, h.picks);
				const v = (s: number[]) =>
					s.map((x, y) => o.seasonValue(outlook, y, x, tid));
				const va = v(a);
				const vb = v(b);
				const lines: any[] = [];
				o.teamStrengths(
					outlook,
					tid,
					[...h.players, rookie],
					h.picks,
					undefined,
					undefined,
					undefined,
					undefined,
					(y: number, roster: readonly any[]) => {
						const me = roster.filter(
							(l: any) => l.pid === p.pid || l === undefined,
						);
						lines[y] = me.length;
					},
				);
				log(
					`  ${abbrev.get(tid)} use ${view.useOfNow(tid, { players: [rookie], picks: [] }).toFixed(3)} stay ${o.retentionOf(outlook, p.pid, tid).toFixed(2)}; strength +${a.map((x: number, y: number) => (x - b[y]!).toFixed(2)).join(" ")}; value +${va.map((x: number, y: number) => (x - vb[y]!).toFixed(3)).join(" ")}`,
				);
			}
		}
	},
);
