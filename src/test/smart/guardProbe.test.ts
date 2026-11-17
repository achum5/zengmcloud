import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// The smart CPU on vanilla's rules, a human's offers: for a human team and
// a few CPU teams, offers of each kind - a contender's best player for picks
// and prospects, a rebuilding team's best young player for a veteran, a
// swap of bench players - and what vanilla at the league's trade difficulty
// and the smart CPU's guard each say (smartCpu/humanTrades.ts).
//   GUARD_LEAGUE=<league.json> GUARD_HUMAN=GSW GUARD_OUT=<file> npx vitest \
//     --run --project basketball src/test/smart/guardProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.GUARD_LEAGUE)(
	"guard probe",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => lines.push(x);
		await loadLeagueFile(env.GUARD_LEAGUE!);
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const tidOf = (a: string) => teams.find((t) => t.abbrev === a)!.tid;
		const human = tidOf(env.GUARD_HUMAN ?? "GSW");
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			realisticFrontOffices: false,
			smartCpu: true,
			spectator: false,
			userTid: human,
			userTids: [human],
		});
		const { smartGuardOffer } =
			await import("../../worker/core/smartCpu/humanTrades.ts");
		const { tradeValueCalculator } =
			await import("../../worker/core/vanillaPlus/index.ts");
		const calc = await tradeValueCalculator();
		const byValue = async (tid: number) =>
			(await idb.cache.players.indexGetAll("playersByTid", tid)).sort(
				(a, b) => b.value - a.value,
			);
		const picksOf = async (tid: number) =>
			(await idb.cache.draftPicks.indexGetAll("draftPicksByTid", tid)).filter(
				(dp) => typeof dp.season === "number",
			);
		const name = (p: any) =>
			`${p.firstName} ${p.lastName} ${p.ratings.at(-1).ovr}/${p.ratings.at(-1).pot} ${g.get("season") - p.born.year}y`;
		const tryOffer = async (
			label: string,
			cpu: number,
			cpuGives: { pids: number[]; dpids: number[] },
			humanGives: { pids: number[]; dpids: number[] },
		) => {
			const dv = await calc.evaluate({
				tid: cpu,
				pidsAdd: humanGives.pids,
				pidsRemove: cpuGives.pids,
				dpidsAdd: humanGives.dpids,
				dpidsRemove: cpuGives.dpids,
				tradingPartnerTid: human,
			});
			const guard = await smartGuardOffer(cpu, human, cpuGives, humanGives);
			log(
				`${label} [${abbrev.get(cpu)} with ${abbrev.get(human)}]: vanilla ${dv > 0 ? "yes" : "no"} (${dv.toFixed(1)}), guard ${guard.ok ? "ok" : `${guard.reason}: ${guard.message}`}; ${abbrev.get(cpu)} ${guard.cpu?.map((x: number) => (100 * x).toFixed(1)).join("->")}%${guard.human ? `, ${abbrev.get(human)} ${guard.human.map((x: number) => (100 * x).toFixed(1)).join("->")}%` : ""}`,
			);
		};
		const humanPlayers = await byValue(human);
		const humanPicks = await picksOf(human);
		const cpuTeams = (env.GUARD_CPU ?? "CLE,BOS,MEM,BKN,ORL,DEN")
			.split(",")
			.map(tidOf);
		for (const cpu of cpuTeams) {
			const theirs = await byValue(cpu);
			const star = theirs[0]!;
			const young = theirs.find(
				(p) =>
					g.get("season") - p.born.year <= 22 && p.ratings.at(-1)!.pot >= 60,
			);
			const bench = theirs.at(-3)!;
			const vet = humanPlayers.find(
				(p) => g.get("season") - p.born.year >= 30 && p.value > 50,
			);
			log(
				`\n${abbrev.get(cpu)}: star ${name(star)}${young ? `, young ${name(young)}` : ""}`,
			);
			// Their star for the human's picks and its best young player.
			const humanYoung = humanPlayers.find(
				(p) => g.get("season") - p.born.year <= 23,
			);
			await tryOffer(
				"star for picks + young",
				cpu,
				{ pids: [star.pid], dpids: [] },
				{
					pids: humanYoung ? [humanYoung.pid] : [],
					dpids: humanPicks.slice(0, 3).map((dp) => dp.dpid),
				},
			);
			// Their star for the human's second-best player and a pick.
			await tryOffer(
				"star for #2 + pick",
				cpu,
				{ pids: [star.pid], dpids: [] },
				{
					pids: [humanPlayers[1]!.pid],
					dpids: humanPicks.slice(0, 1).map((dp) => dp.dpid),
				},
			);
			// Their second- and fourth-best players for picks only, and for a lesser
			// player and a pick: a contender poaching a piece.
			await tryOffer(
				"#2 for 3 picks",
				cpu,
				{ pids: [theirs[1]!.pid], dpids: [] },
				{ pids: [], dpids: humanPicks.slice(0, 3).map((dp) => dp.dpid) },
			);
			await tryOffer(
				"#4 for #6 + pick",
				cpu,
				{ pids: [theirs[3]!.pid], dpids: [] },
				{
					pids: [humanPlayers[5]!.pid],
					dpids: humanPicks.slice(0, 1).map((dp) => dp.dpid),
				},
			);
			// Their third-best player for the human's picks and young player.
			await tryOffer(
				"#3 for picks + young",
				cpu,
				{ pids: [theirs[2]!.pid], dpids: [] },
				{
					pids: humanYoung ? [humanYoung.pid] : [],
					dpids: humanPicks.slice(0, 2).map((dp) => dp.dpid),
				},
			);
			if (young && vet) {
				await tryOffer(
					"young for veteran",
					cpu,
					{ pids: [young.pid], dpids: [] },
					{ pids: [vet.pid], dpids: [] },
				);
			}
			await tryOffer(
				"bench swap",
				cpu,
				{ pids: [bench.pid], dpids: [] },
				{ pids: [humanPlayers.at(-3)!.pid], dpids: [] },
			);
		}
		fs.writeFileSync(
			env.GUARD_OUT ?? "guardProbe.txt",
			`${lines.join("\n")}\n`,
		);
	},
);
