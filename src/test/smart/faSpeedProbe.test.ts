import { test } from "vitest";
import { PHASE, PLAYER } from "../../common/constants.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { contractOf } from "../../worker/core/realisticFrontOffice/contractOf.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Free agency on a real league, timed, and every roster, contract and pick
// after it written out whole - so a change meant only for speed can be
// checked to make the very same moves. FAS_DAYS: regular-season days played
// after it too (a market moment every week).
//   FAS_LEAGUE=<league.json> FAS_OUT=<dir> [FAS_DAYS=7] npx vitest --run \
//     --project basketball src/test/smart/faSpeedProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.FAS_LEAGUE)(
	"free agency speed probe",
	{ timeout: 20_000_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const out = env.FAS_OUT ?? ".";
		fs.mkdirSync(out, { recursive: true });
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(`${out}/report.txt`, lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.FAS_LEAGUE!);
		const { game, phase, draft, freeAgents, league } =
			await import("../../worker/core/index.ts");
		const { smartClock, valueCount } =
			await import("../../worker/core/smartCpu/clock.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const conditions = {};
		const step = async () => {
			const ph = g.get("phase");
			if (
				ph === PHASE.PLAYOFFS ||
				ph === PHASE.REGULAR_SEASON ||
				ph === PHASE.AFTER_TRADE_DEADLINE
			) {
				await game.play(ph === PHASE.PLAYOFFS ? 100 : 7, conditions);
			} else if (ph === PHASE.DRAFT) {
				await draft.runPicks({ type: "untilEnd" }, conditions);
				if (g.get("phase") === PHASE.DRAFT) {
					await phase.newPhase(PHASE.AFTER_DRAFT, conditions);
				}
			} else {
				await phase.newPhase((ph + 1) as any, conditions);
			}
		};
		let guard = 0;
		while (g.get("phase") !== PHASE.FREE_AGENCY && guard++ < 500) {
			await step();
		}
		const digest = async (name: string) => {
			const rows: string[] = [];
			for (const p of await idb.cache.players.indexGetAll("playersByTid", [
				PLAYER.FREE_AGENT,
				Infinity,
			])) {
				const c = contractOf(p);
				rows.push(`p ${p.pid} ${p.tid} ${c.amount} ${c.exp}`);
			}
			for (const dp of await idb.cache.draftPicks.getAll()) {
				rows.push(`d ${dp.dpid} ${dp.tid}`);
			}
			rows.sort();
			fs.writeFileSync(`${out}/${name}.txt`, rows.join("\n") + "\n");
			let h = 0;
			for (const ch of rows.join("\n")) {
				h = (h * 31 + ch.charCodeAt(0)) | 0;
			}
			return h;
		};
		const clock = () => {
			const rows = Object.entries(smartClock)
				.filter(([k]) => !k.includes("@"))
				.sort((a, b) => b[1].ms - a[1].ms);
			for (const [k, c] of rows) {
				log(
					`  ${k}: ${(c.ms / 1000).toFixed(1)} s in ${c.n} calls, ${c.values} values`,
				);
			}
			for (const k of Object.keys(smartClock)) {
				delete smartClock[k];
			}
		};
		log(`at ${g.get("season")} phase ${g.get("phase")}`);
		for (const k of Object.keys(smartClock)) {
			delete smartClock[k];
		}
		valueCount.n = 0;
		let t = performance.now();
		await freeAgents.play(g.get("daysLeft"), conditions);
		if (g.get("phase") === PHASE.FREE_AGENCY) {
			await phase.newPhase(PHASE.PRESEASON, conditions);
		}
		const faMs = performance.now() - t;
		log(
			`free agency: ${(faMs / 1000).toFixed(1)} s, ${valueCount.n} values, digest ${await digest("after-fa")}`,
		);
		clock();
		const days = Number(env.FAS_DAYS ?? 0);
		if (days > 0) {
			while (g.get("phase") !== PHASE.REGULAR_SEASON) {
				await step();
			}
			valueCount.n = 0;
			t = performance.now();
			await game.play(days, conditions);
			const daysMs = performance.now() - t;
			log(
				`${days} days: ${(daysMs / 1000).toFixed(1)} s, ${valueCount.n} values, digest ${await digest("after-days")}`,
			);
			clock();
		}
	},
);
