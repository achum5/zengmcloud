import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// PREFLIGHT: the smart CPU on vanilla's rules switched on in a league as the
// owner would - at a trade difficulty of 3.5, with PF_HUMANS teams run as
// league mates' (out of the CPU market), on from PF_ON_DAY days into the
// first season (0: from the start) - and played on PF_SEASONS seasons. Fails
// on any error. Logs, from the moment it goes on: each phase's time; the CPU
// teams' trades, signings, re-signings, releases; the biggest new deals to
// players rated under 50; any roster out of bounds; any trade between CPU
// teams that took a league mate's player; and at each phase, for league
// mates' teams, what "What would make this work" and the trading block come
// back with, whether the team would then take it, and one offer proposed.
//   PF_LEAGUE=<league.json> PF_OUT=<file> [PF_SEASONS=2] [PF_ON_DAY=0] \
//     [PF_HUMANS=4] \
//     npx vitest --run --project basketball src/test/smart/preflight.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.PF_LEAGUE)("preflight", { timeout: 14_400_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const out = env.PF_OUT ?? "/tmp/preflight.txt";
	const lines: string[] = [];
	const log = (x: string) => {
		lines.push(x);
		fs.writeFileSync(out, lines.join("\n") + "\n");
	};
	await loadLeagueFile(env.PF_LEAGUE!);
	const { league, game, phase, draft, freeAgents, trade } =
		await import("../../worker/core/index.ts");
	const { PHASE, PLAYER } = await import("../../common/constants.ts");
	const { idb } = await import("../../worker/db/index.ts");
	const { tradeValueCalculator } =
		await import("../../worker/core/vanillaPlus/index.ts");
	const { ownReadValuer, smartGuardOffer } =
		await import("../../worker/core/smartCpu/humanTrades.ts");
	const { smartHumans } = await import("../../worker/core/smartCpu/signing.ts");
	const { contractOf } =
		await import("../../worker/core/realisticFrontOffice/contractOf.ts");
	const isUntradable = (await import("../../worker/core/trade/isUntradable.ts"))
		.default;
	const onDay = Number(env.PF_ON_DAY ?? 0);
	await league.setGameAttributes({
		spectator: true,
		smartCpu: onDay === 0,
		realisticFrontOffices: false,
		difficulty: Number(env.PF_DIFFICULTY ?? 3.5),
		difficultyTrade: null,
	});
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
	const nHumans = Number(env.PF_HUMANS ?? 4);
	const step = Math.floor(teams.length / nHumans);
	const humans = teams
		.filter((_, i) => i % step === 1)
		.slice(0, nHumans)
		.map((t) => t.tid);
	smartHumans.tids = humans;
	log(
		`${g.get("season")} phase ${g.get("phase")}, smart CPU ${onDay === 0 ? "on from the start" : `on ${onDay} days into the season`}, league mates ${humans.map((t) => abbrev.get(t)).join(" ")}, difficulty ${g.get("difficulty")}`,
	);
	let on = onDay === 0;
	let onEid = on ? -1 : Infinity;
	const eventsOf = async (season: number) =>
		(await idb.merged.events.getAll({ season } as any)) as any[];
	const lastEid = async () =>
		Math.max(-1, ...(await eventsOf(g.get("season"))).map((e) => e.eid));
	const proposed: string[] = [];
	// The trade events of the league mates' own proposals.
	const ours = new Set<number>();
	const problems: string[] = [];

	// A league mate's trade screens, as one of them would use them now.
	const screens = async (label: string) => {
		if (!on) {
			return;
		}
		const ph = g.get("phase");
		if (ph === PHASE.AFTER_TRADE_DEADLINE || ph === PHASE.PLAYOFFS) {
			return;
		}
		const t0 = performance.now();
		const parts: string[] = [];
		for (const [i, h] of humans.slice(0, 2).entries()) {
			await league.setGameAttributes({
				spectator: false,
				userTid: h,
				userTids: humans,
			});
			const tradable = async (tid: number) =>
				(await idb.cache.players.indexGetAll("playersByTid", tid))
					.filter((p) => !isUntradable(p).untradable)
					.sort((a, b) => b.value - a.value);
			const cpus = teams.filter((t) => !humans.includes(t.tid));
			// What would make this work, for three CPU teams' best.
			let miw = 0;
			let miwOk = 0;
			let miwSaid: string | undefined;
			for (const c of cpus.slice(i * 3, i * 3 + 3)) {
				const x = (await tradable(c.tid))[0];
				if (!x) {
					continue;
				}
				await trade.create([
					{ tid: h, pids: [], dpids: [], pidsExcluded: [], dpidsExcluded: [] },
					{
						tid: c.tid,
						pids: [x.pid],
						dpids: [],
						pidsExcluded: [],
						dpidsExcluded: [],
					},
				]);
				const r = await trade.makeItWorkTrade();
				if (!r.changed) {
					miwSaid ??= r.message;
				}
				if (r.changed) {
					miw += 1;
					const tr = await trade.get();
					const guard = await smartGuardOffer(
						tr.teams[1].tid,
						h,
						{ pids: tr.teams[1].pids, dpids: tr.teams[1].dpids },
						{ pids: tr.teams[0].pids, dpids: tr.teams[0].dpids },
					);
					if (guard.ok) {
						miwOk += 1;
					}
				}
			}
			// The trading block, for its best player.
			const mine = (await tradable(h))[0];
			const calc = await tradeValueCalculator();
			const own = await ownReadValuer(calc);
			let offers = 0;
			let offersOk = 0;
			let toPropose: any;
			if (mine) {
				for (const c of cpus) {
					const r = await trade.makeItWork(
						[
							{
								tid: h,
								pids: [mine.pid],
								dpids: [],
								pidsExcluded: [],
								dpidsExcluded: [],
							},
							{
								tid: c.tid,
								pids: [],
								dpids: [],
								pidsExcluded: [],
								dpidsExcluded: [],
							},
						],
						{
							holdUserConstant: true,
							maxAssetsToAdd: 5,
							valueChangeCalculator: own,
						},
					);
					if (!r) {
						continue;
					}
					offers += 1;
					const guard = await smartGuardOffer(
						c.tid,
						h,
						{ pids: r[1].pids, dpids: r[1].dpids },
						{ pids: r[0].pids, dpids: r[0].dpids },
					);
					if (guard.ok) {
						offersOk += 1;
						if (!toPropose && !(await trade.summary(r)).warning) {
							toPropose = r;
						}
					}
				}
			}
			parts.push(
				`${abbrev.get(h)}: make it work ${miwOk}/${miw}${miwSaid ? ` (else ${miwSaid})` : ""}, block ${offersOk}/${offers}`,
			);
			// One offer the team would take, proposed for real (the first
			// league mate only, once a phase).
			if (i === 0 && toPropose) {
				// The trade object is emptied once a trade goes through.
				const pids = [...toPropose[0].pids, ...toPropose[1].pids].sort();
				const what = `${abbrev.get(h)} ${toPropose[0].pids.length}p+${toPropose[0].dpids.length}dp for ${abbrev.get(toPropose[1].tid)} ${toPropose[1].pids.length}p+${toPropose[1].dpids.length}dp`;
				await trade.create(structuredClone(toPropose));
				const r = await trade.propose(false);
				parts.push(
					`proposed ${what}: ${r.accepted ? "accepted" : `refused (${r.message})`}`,
				);
				if (r.accepted) {
					proposed.push(JSON.stringify(pids));
					ours.add(await lastEid());
				} else {
					problems.push(
						`${label}: a block offer refused when proposed: ${r.message}`,
					);
				}
			}
		}
		await league.setGameAttributes({ spectator: true });
		log(
			`  screens ${label}: ${parts.join("; ")} (${((performance.now() - t0) / 1000).toFixed(1)} s)`,
		);
	};

	// Every team's roster within the limits.
	const rosters = async (label: string) => {
		for (const t of teams) {
			const n = (await idb.cache.players.indexGetAll("playersByTid", t.tid))
				.length;
			if (n > g.get("maxRosterSize") || n < g.get("minRosterSize")) {
				problems.push(`${label}: ${t.abbrev} has ${n} players`);
			}
		}
	};

	const turnOn = async () => {
		await league.setGameAttributes({ smartCpu: true });
		on = true;
		onEid = await lastEid();
		log(`smart CPU on: ${g.get("season")} phase ${g.get("phase")}`);
		await screens("as it goes on");
	};

	const startSeason = g.get("season");
	const seasons = Number(env.PF_SEASONS ?? 2);
	const t0 = performance.now();
	let tPhase = performance.now();
	let days = 0;
	let guard = 0;
	await screens(`${startSeason} phase ${g.get("phase")}`);
	while (
		!(
			g.get("season") >= startSeason + seasons &&
			g.get("phase") === PHASE.PRESEASON
		) &&
		guard++ < 10000
	) {
		const ph = g.get("phase");
		if (ph === PHASE.REGULAR_SEASON || ph === PHASE.AFTER_TRADE_DEADLINE) {
			if (!on && days >= onDay) {
				await turnOn();
			}
			const chunk = !on && onDay - days < 7 ? onDay - days : 7;
			await game.play(Math.max(1, chunk), {});
			days += Math.max(1, chunk);
		} else if (ph === PHASE.PLAYOFFS) {
			await game.play(100, {});
		} else if (ph === PHASE.DRAFT) {
			await screens(`${g.get("season")} draft`);
			await draft.runPicks({ type: "untilEnd" }, {});
			if (g.get("phase") === PHASE.DRAFT) {
				await phase.newPhase(PHASE.AFTER_DRAFT, {});
			}
		} else if (ph === PHASE.FREE_AGENCY) {
			await screens(`${g.get("season")} free agency opens`);
			await freeAgents.play(g.get("daysLeft"), {});
			if (g.get("phase") === PHASE.FREE_AGENCY) {
				await phase.newPhase(PHASE.PRESEASON, {});
			}
		} else {
			if (ph === PHASE.RESIGN_PLAYERS) {
				await screens(`${g.get("season")} re-signing`);
			}
			if (ph === PHASE.PRESEASON) {
				await screens(`${g.get("season")} preseason`);
			}
			await phase.newPhase((ph + 1) as any, {});
			if (g.get("phase") === PHASE.REGULAR_SEASON) {
				days = 0;
				await rosters(`${g.get("season")} opening night`);
			}
		}
		if (g.get("phase") !== ph) {
			log(
				`${g.get("season")} phase ${ph} -> ${g.get("phase")}: ${((performance.now() - tPhase) / 60000).toFixed(1)} min`,
			);
			tPhase = performance.now();
		}
	}
	log(
		`to ${g.get("season")} preseason in ${((performance.now() - t0) / 60000).toFixed(1)} min`,
	);

	// What the CPU teams did once it was on.
	const events: any[] = [];
	for (let s = startSeason; s <= g.get("season"); s++) {
		events.push(...(await eventsOf(s)).filter((e) => e.eid > onEid));
	}
	const bySeason = new Map<number, Record<string, number>>();
	for (const e of events) {
		const s = bySeason.get(e.season) ?? {};
		s[e.type] = (s[e.type] ?? 0) + 1;
		bySeason.set(e.season, s);
		if (
			e.type === "trade" &&
			(e.tids ?? []).some((tid: number) => humans.includes(tid)) &&
			!ours.has(e.eid)
		) {
			const names: string[] = [];
			for (const pid of e.pids ?? []) {
				const p = await idb.merged.players.get(pid);
				names.push(p ? `${p.firstName} ${p.lastName}` : `${pid}`);
			}
			problems.push(
				`a trade took a league mate's team: eid ${e.eid}, ${e.season} phase ${e.phase}, ${(e.tids ?? []).map((t: number) => abbrev.get(t)).join("-")}, ai ${JSON.stringify(e.aiTrade ?? null)}, players ${names.join(", ")}, picks ${(e.dpids ?? []).length}, ours by pids ${proposed.includes(JSON.stringify([...(e.pids ?? [])].sort()))}`,
			);
		}
	}
	for (const [s, counts] of [...bySeason].sort((a, b) => a[0] - b[0])) {
		log(
			`${s}: ${["trade", "freeAgent", "reSigned", "release", "draft"]
				.map((k) => `${k} ${counts[k] ?? 0}`)
				.join(", ")}`,
		);
	}
	// The biggest new deals to players rated under 50, since it went on.
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	const cheapRated = players
		.filter((p) => {
			const last = p.transactions?.at(-1);
			return (
				last &&
				(last.type === "freeAgent" || last.type === "reSigned") &&
				last.season >= startSeason &&
				p.ratings.at(-1)!.ovr < 50 &&
				!humans.includes(p.tid)
			);
		})
		.sort((a, b) => contractOf(b).amount - contractOf(a).amount)
		.slice(0, 6);
	log(
		`biggest new deals to players under 50 (cap ${g.get("salaryCap") / 1000}M): ${cheapRated
			.map(
				(p) =>
					`${abbrev.get(p.tid)} ${p.firstName} ${p.lastName} ${g.get("season") - p.born.year}y ${p.ratings.at(-1)!.ovr}/${p.ratings.at(-1)!.pot} $${(contractOf(p).amount / 1000).toFixed(1)}M to ${contractOf(p).exp} (${p.transactions!.at(-1)!.type})`,
			)
			.join("; ")}`,
	);
	log(
		`free agents left: ${(await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)).length}`,
	);
	log(`problems: ${problems.length}`);
	for (const p of problems) {
		log(`  ${p}`);
	}
});
