import { test } from "vitest";
import { PHASE, PLAYER } from "../../common/constants.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { contractOf } from "../../worker/core/realisticFrontOffice/contractOf.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Free agency on vanilla's rules with the smart CPU, a few days of it, on a
// real league: the league is put in free agency with its best players whose
// contracts run out this season made free agents, and CPU teams sign day by
// day (the pool measured, every team looking, room cleared by trade where
// worth it). Who signed where, for what, and the trades made.
//   FA_LEAGUE=<league.json> FA_OUT=<file> FA_DAYS=5 npx vitest --run \
//     --project basketball src/test/smart/faSmokeProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.FA_LEAGUE)(
	"free agency smoke probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.FA_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.FA_LEAGUE!);
		const { freeAgents, league, player } =
			await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			realisticFrontOffices: false,
			smartCpu: true,
			spectator: true,
			phase: PHASE.FREE_AGENCY,
			daysLeft: 30,
		});
		const season = g.get("season");
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		// The best players with a year left become free agents, as if their
		// deals had just run out.
		const all = (await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		])) as any[];
		const freed = all
			.filter((p) => contractOf(p).exp <= season)
			.sort((a, b) => b.value - a.value)
			.slice(0, Number(env.FA_FREE ?? 25));
		for (const p of freed) {
			log(
				`freed: ${p.firstName} ${p.lastName} ${p.ratings.at(-1).ovr} ${season - p.born.year}y from ${abbrev.get(p.tid)}`,
			);
			player.addToFreeAgents(p, {}, null);
			await idb.cache.players.put(p);
		}
		const eventsBefore = (await idb.cache.events.getAll()).length;
		const days = Number(env.FA_DAYS ?? 5);
		for (let d = 0; d < days; d++) {
			const started = performance.now();
			await freeAgents.autoSign();
			await league.setGameAttributes({ daysLeft: g.get("daysLeft") - 1 });
			const events = (await idb.cache.events.getAll()).slice(eventsBefore);
			log(
				`day ${d + 1}: ${Math.round(performance.now() - started)} ms, events so far ${events.length}`,
			);
		}
		const events = (await idb.cache.events.getAll()).slice(eventsBefore);
		const nameOf = async (pid: number) => {
			const p = await idb.cache.players.get(pid);
			if (!p) {
				return `pid${pid}`;
			}
			const salary = (contractOf(p).amount / 1000).toFixed(1);
			return `${p.firstName} ${p.lastName} (${p.ratings.at(-1)!.ovr}, ${season - p.born.year}y, ${salary}M)`;
		};
		for (const e of events as any[]) {
			if (e.type === "freeAgent") {
				const who = await nameOf(e.pids[0]);
				log(`signed: ${abbrev.get(e.tids[0])} ${who}`);
			} else if (e.type === "trade") {
				const sides = [];
				for (let i = 0; i < 2; i++) {
					const got = [];
					for (const a of e.teams[i].assets) {
						got.push("pid" in a ? await nameOf(a.pid) : `pick ${a.dpid}`);
					}
					sides.push(
						`${abbrev.get(e.tids[i])} gets ${got.join(", ") || "nothing"}`,
					);
				}
				log(`trade [${e.aiTrade?.motivation}]: ${sides.join(" | ")}`);
			}
		}
		const left = await idb.cache.players.indexGetAll(
			"playersByTid",
			PLAYER.FREE_AGENT,
		);
		log(
			`unsigned among the freed: ${freed.filter((p) => left.some((x) => x.pid === p.pid)).length} of ${freed.length}`,
		);
	},
);
