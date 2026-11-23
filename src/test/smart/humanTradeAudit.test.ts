import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE HUMAN TRADE AUDIT: what a person can get from CPU teams by trade, at a
// trade difficulty, with the smart CPU on and off. On vanilla's rules both
// judge a person's offer by vanilla's value at the difficulty; the smart CPU
// then turns down what goes against a team's direction or helps a rival
// contender (smartCpu/humanTrades.ts) - so one pass on one league answers
// both. For a person's team, every 1-for-1 and 2-for-1 deal with every CPU
// team among the best players on each side (and one with a first added) that
// the salary rules allow: vanilla's verdict, the smart CPU's guard, and what
// the deal is worth by the smart CPU's own read to the CPU team and to the
// person's. A line of JSON a deal vanilla takes. HT_MIW: "What would make
// this work" for each CPU team's best three and the trading block for the
// person's best three, by vanilla alone and with the CPU team's own read
// (ownReadValuer) - how long each takes and whether the team would take
// what comes back.
//   HT_LEAGUE=<league.json> HT_OUT=<file> [HT_DIFFICULTY=3.5] [HT_DAYS=20] \
//     [HT_USERS=<tid,...>] [HT_MIW=1] \
//     npx vitest --run --project basketball src/test/smart/humanTradeAudit.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.HT_LEAGUE)(
	"human trade audit",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const out = env.HT_OUT ?? "/tmp/humanTrades.txt";
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(out, lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.HT_LEAGUE!);
		const { league, phase, game, trade } =
			await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		const { idb } = await import("../../worker/db/index.ts");
		const { tradeValueCalculator } =
			await import("../../worker/core/vanillaPlus/index.ts");
		const { ownReadValuer, smartGuardOffer } =
			await import("../../worker/core/smartCpu/humanTrades.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const { ensureRetention } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const isUntradable = (
			await import("../../worker/core/trade/isUntradable.ts")
		).default;
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		// Into the season, CPU teams on the smart CPU as in the owner's league.
		while (g.get("phase") !== PHASE.REGULAR_SEASON) {
			await phase.newPhase((g.get("phase") + 1) as any, {});
		}
		await game.play(Number(env.HT_DAYS ?? 20), {});
		const difficulty = Number(env.HT_DIFFICULTY ?? 3.5);
		await league.setGameAttributes({
			spectator: false,
			difficulty,
			difficultyTrade: null,
		});
		const season = g.get("season");
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const outlook0 = await currentOutlook();
		const title = (tid: number) =>
			(outlook0.teams.get(tid)?.title?.[0] ?? 0) +
			(outlook0.teams.get(tid)?.title?.[1] ?? 0);
		// The person's team: a contender, the middle, a bad team.
		const byTitle = [...teams].sort((a, b) => title(b.tid) - title(a.tid));
		const users = (env.HT_USERS ?? "").split(",").filter(Boolean).map(Number);
		if (users.length === 0) {
			users.push(
				byTitle[2]!.tid,
				byTitle[Math.floor(byTitle.length / 2)]!.tid,
				byTitle.at(-3)!.tid,
			);
		}
		const label = (p: any) =>
			`${p.firstName} ${p.lastName} ${season - p.born.year}y ${p.ratings.at(-1).ovr}/${p.ratings.at(-1).pot} $${(contractOf(p).amount / 1000).toFixed(1)}M-${contractOf(p).exp}`;
		const pickLabel = (dp: any) =>
			`${dp.season} R${dp.round}${dp.originalTid !== dp.tid ? ` (${abbrev.get(dp.originalTid)})` : ""}`;
		log(
			`${season}, ${g.get("numGames") - 0} games, after ${env.HT_DAYS ?? 20} days; trade difficulty ${difficulty}`,
		);

		for (const userTid of users) {
			await league.setGameAttributes({ userTid, userTids: [userTid] });
			const outlook = await currentOutlook();
			const view = viewFor(outlook);
			await view.prime();
			const calc = await tradeValueCalculator();
			const tradable = async (tid: number) =>
				(await idb.cache.players.indexGetAll("playersByTid", tid))
					.filter((p) => !isUntradable(p).untradable)
					.sort((a, b) => b.value - a.value);
			const mine = (await tradable(userTid)).slice(0, 9);
			const myFirst = (
				await idb.cache.draftPicks.indexGetAll("draftPicksByTid", userTid)
			)
				.filter((dp) => dp.round === 1)
				.sort((a, b) => Number(b.season) - Number(a.season))[0];
			log(
				`\n==== YOU: ${abbrev.get(userTid)} (${(100 * title(userTid)).toFixed(1)}% to win the title this season or next) - ${mine
					.slice(0, 6)
					.map(label)
					.join(", ")}`,
			);
			const stats = {
				tried: 0,
				rules: 0,
				vanilla: 0,
				guard: { now: 0, later: 0, rival: 0, value: 0 } as Record<
					string,
					number
				>,
			};
			const rows: any[] = [];
			const desc = async (a: { pids: number[]; dpids: number[] }) => {
				const xs: string[] = [];
				for (const pid of a.pids) {
					xs.push(label(await idb.cache.players.get(pid)));
				}
				for (const dpid of a.dpids) {
					xs.push(pickLabel(await idb.cache.draftPicks.get(dpid)));
				}
				return xs.join(" + ") || "nothing";
			};
			for (const t of teams) {
				const cpuTid = t.tid;
				if (cpuTid === userTid) {
					continue;
				}
				// HT_DEALS=0: none of these, for the HT_MIW part alone.
				const theirs =
					env.HT_DEALS === "0" ? [] : (await tradable(cpuTid)).slice(0, 8);
				const deals: { give: number[]; dpids: number[]; get: number }[] = [];
				for (const x of theirs) {
					for (let i = 0; i < mine.length; i++) {
						deals.push({ give: [mine[i]!.pid], dpids: [], get: x.pid });
						if (myFirst) {
							deals.push({
								give: [mine[i]!.pid],
								dpids: [myFirst.dpid],
								get: x.pid,
							});
						}
						for (let j = i + 1; j < Math.min(mine.length, 7); j++) {
							deals.push({
								give: [mine[i]!.pid, mine[j]!.pid],
								dpids: [],
								get: x.pid,
							});
						}
					}
				}
				for (const d of deals) {
					stats.tried += 1;
					const give = { pids: d.give, dpids: d.dpids };
					const get = { pids: [d.get], dpids: [] as number[] };
					const tt: any = [
						{ tid: userTid, ...give, pidsExcluded: [], dpidsExcluded: [] },
						{ tid: cpuTid, ...get, pidsExcluded: [], dpidsExcluded: [] },
					];
					const s = await trade.summary(tt);
					if (s.warning) {
						continue;
					}
					stats.rules += 1;
					const dv = await calc.evaluate({
						tid: cpuTid,
						pidsAdd: give.pids,
						pidsRemove: get.pids,
						dpidsAdd: give.dpids,
						dpidsRemove: get.dpids,
						tradingPartnerTid: userTid,
					});
					if (!(dv > 0)) {
						continue;
					}
					stats.vanilla += 1;
					const players: any[] = [];
					for (const pid of [...give.pids, ...get.pids]) {
						players.push(await idb.cache.players.get(pid));
					}
					for (const tid of [userTid, cpuTid]) {
						await ensureRetention(outlook, players, tid);
					}
					const guard = await smartGuardOffer(cpuTid, userTid, get, give);
					if (!guard.ok) {
						stats.guard[guard.reason] = (stats.guard[guard.reason] ?? 0) + 1;
					}
					const cpuWorth = (
						await view.verdict(cpuTid, userTid, get, give, { market: false })
					).worth;
					const userWorth = (
						await view.verdict(userTid, cpuTid, give, get, { market: false })
					).worth;
					const row = {
						user: abbrev.get(userTid),
						userTitle: title(userTid),
						cpu: abbrev.get(cpuTid),
						cpuTitle: title(cpuTid),
						n: give.pids.length + give.dpids.length,
						dv,
						guard: guard.ok ? undefined : guard.reason,
						guardCpu: guard.cpu,
						guardHuman: guard.human,
						cpuWorth,
						userWorth,
						give: await desc(give),
						get: await desc(get),
					};
					rows.push(row);
					fs.appendFileSync(`${out}.jsonl`, JSON.stringify(row) + "\n");
				}
			}
			log(
				`${stats.tried} deals tried, ${stats.rules} within the salary rules, vanilla takes ${stats.vanilla}; the smart CPU then turns down ${stats.guard.now ?? 0} (win now), ${stats.guard.later ?? 0} (building for later), ${stats.guard.rival ?? 0} (rival contender), ${stats.guard.value ?? 0} (a loss by its own read)`,
			);
			const show = (r: any) =>
				`  ${r.cpu} (${(100 * r.cpuTitle).toFixed(1)}%)${r.guard ? ` [smart: no, ${r.guard}]` : ""} CPU ${r.cpuWorth.toFixed(3)} you ${r.userWorth.toFixed(3)} | you give ${r.give} | you get ${r.get}`;
			log("Worst for the CPU team (its own smart read), smart CPU off:");
			for (const r of [...rows]
				.sort((a, b) => a.cpuWorth - b.cpuWorth)
				.slice(0, 12)) {
				log(show(r));
			}
			log("Worst for the CPU team, smart CPU on:");
			for (const r of [...rows]
				.filter((r) => !r.guard)
				.sort((a, b) => a.cpuWorth - b.cpuWorth)
				.slice(0, 12)) {
				log(show(r));
			}
			log("Best for you, smart CPU on:");
			for (const r of [...rows]
				.filter((r) => !r.guard)
				.sort((a, b) => b.userWorth - a.userWorth)
				.slice(0, 8)) {
				log(show(r));
			}
			if (env.HT_MIW) {
				const own = await ownReadValuer(calc);
				const miw: Record<
					string,
					{ n: number; ms: number; found: number; ok: number }
				> = {};
				// The same search asking the team's read of every piece: it
				// must come to the same deal.
				const everyPiece = { evaluate: (s: any) => own.evaluate(s) };
				let differ = 0;
				let last: string | undefined;
				const tally = async (
					key: string,
					f: () => Promise<any>,
					cpuTid: number,
				) => {
					const t0 = performance.now();
					const r = await f();
					if (key.endsWith("every piece")) {
						if (JSON.stringify(r) !== last) {
							differ += 1;
							log(`  differs: ${last} / ${JSON.stringify(r)}`);
						}
					} else {
						last = JSON.stringify(r);
					}
					const ms = performance.now() - t0;
					const m = (miw[key] ??= { n: 0, ms: 0, found: 0, ok: 0 });
					m.n += 1;
					m.ms += ms;
					if (r) {
						m.found += 1;
						const guard = await smartGuardOffer(
							cpuTid,
							userTid,
							{ pids: r[1].pids, dpids: r[1].dpids },
							{ pids: r[0].pids, dpids: r[0].dpids },
						);
						if (guard.ok) {
							m.ok += 1;
						}
					}
				};
				for (const t of teams) {
					if (t.tid === userTid) {
						continue;
					}
					const theirs = (await tradable(t.tid)).slice(0, 3);
					for (const x of theirs) {
						const tt: any = [
							{
								tid: userTid,
								pids: [],
								dpids: [],
								pidsExcluded: [],
								dpidsExcluded: [],
							},
							{
								tid: t.tid,
								pids: [x.pid],
								dpids: [],
								pidsExcluded: [],
								dpidsExcluded: [],
							},
						];
						for (const [key, valuer] of [
							["make it work, vanilla", calc],
							["make it work, own read", own],
							["make it work, own read of every piece", everyPiece],
						] as const) {
							await tally(
								key,
								() =>
									trade.makeItWork(structuredClone(tt), {
										holdUserConstant: false,
										valueChangeCalculator: valuer,
									}),
								t.tid,
							);
						}
					}
					for (const p of mine.slice(0, 3)) {
						const tt: any = [
							{
								tid: userTid,
								pids: [p.pid],
								dpids: [],
								pidsExcluded: [],
								dpidsExcluded: [],
							},
							{
								tid: t.tid,
								pids: [],
								dpids: [],
								pidsExcluded: [],
								dpidsExcluded: [],
							},
						];
						for (const [key, valuer] of [
							["trading block, vanilla", calc],
							["trading block, own read", own],
							["trading block, own read of every piece", everyPiece],
						] as const) {
							await tally(
								key,
								() =>
									trade.makeItWork(structuredClone(tt), {
										holdUserConstant: true,
										maxAssetsToAdd: 5,
										valueChangeCalculator: valuer,
									}),
								t.tid,
							);
						}
					}
				}
				log(
					`own read, a cheap read of each piece against every piece: ${differ} differ`,
				);
				for (const [key, m] of Object.entries(miw)) {
					log(
						`${key}: ${m.n} tries, ${(m.ms / m.n).toFixed(0)} ms each, ${m.found} come back with a deal, ${m.ok} of them the team would take`,
					);
				}
			}
		}
	},
);
