import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// Why a look past the cool-off finds what it finds: the players in a
// cool-off the market would shop once they can be traded -
// CL_LEAGUE=<league.json> CL_OUT=<file>.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.CL_LEAGUE)(
	"cool-off look",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.CL_OUT!, lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.CL_LEAGUE!);
		const { idb } = await import("../../worker/db/index.ts");
		const { candidates } = await import("../../worker/core/smartCpu/market.ts");
		const { coolOff, inCoolOff } =
			await import("../../worker/core/smartCpu/tradeRules.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const view = viewFor(await currentOutlook());
		await view.prime();
		const cpu = new Set(
			teams
				.filter((t) => !t.disabled && !view.humans.has(t.tid))
				.map((t) => t.tid),
		);
		const cooling = new Set<number>();
		for (const tid of cpu) {
			for (const p of view.heldNow(tid).players) {
				if (inCoolOff(p)) {
					cooling.add(p.pid);
				}
			}
		}
		log(`${cooling.size} in a cool-off on CPU teams`);
		coolOff.lifted = true;
		const found = await candidates(
			view,
			cpu,
			new Set(),
			new Map(),
			new Map(),
			(p) => cooling.has(p.pid),
		);
		const { dealsNow, smartTraceWatchers } =
			await import("../../worker/core/smartCpu/market.ts");
		const stages = new Map<string, number>();
		smartTraceWatchers.push((t) => {
			stages.set(t.stage, (stages.get(t.stage) ?? 0) + 1);
			if (t.stage !== "shopped" && lines.length < 120) {
				log(
					`  trace ${t.stage}: ${t.p.firstName} ${t.p.lastName} (${abbrev.get(t.seller)}) buyer ${t.buyer === undefined ? "-" : abbrev.get(t.buyer)} sellerGain ${t.sellerGain?.toFixed(3)} ${t.why ?? ""}`,
				);
			}
		});
		const ahead = await dealsNow(view, cpu, new Map(), undefined, (p) =>
			cooling.has(p.pid),
		);
		log(
			`dry run: ${ahead.length} deals; stages ${JSON.stringify([...stages])}`,
		);
		coolOff.lifted = false;
		log(`${found.length} would be shopped`);
		for (const c of found.slice(0, 20)) {
			log(
				`  ${c.p.firstName} ${c.p.lastName} ${c.p.ratings.at(-1)!.ovr} (${abbrev.get(c.seller)}): keenest ${abbrev.get(c.buyer)}, surplus ${c.surplus.toFixed(3)}`,
			);
		}
		// The best of them by the game's value, and what their teams and the
		// keenest others read them at.
		const best = (await idb.cache.players.getAll())
			.filter((p) => cooling.has(p.pid))
			.sort((a, b) => b.value - a.value)
			.slice(0, 8);
		for (const p of best) {
			const own = view.roughUseOfNow(p.tid, { players: [p], picks: [] });
			let top = { tid: -1, bid: -Infinity };
			for (const k of cpu) {
				if (k !== p.tid) {
					const bid = view.roughUseOfNow(k, { players: [p], picks: [] });
					if (bid > top.bid) {
						top = { tid: k, bid };
					}
				}
			}
			log(
				`  ${p.firstName} ${p.lastName} ${p.ratings.at(-1)!.ovr} (${abbrev.get(p.tid)}): own ${own.toFixed(3)}, keenest ${abbrev.get(top.tid)} ${top.bid.toFixed(3)}`,
			);
		}
	},
);
