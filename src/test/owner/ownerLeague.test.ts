import { test } from "vitest";
import { league } from "../../worker/core/index.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import { getDefaultSettings } from "../../worker/views/newLeague.ts";
import { last } from "../../common/utils.ts";
import { currentOutlook } from "../../worker/core/realisticFrontOffice/market.ts";

// OWNER_LEAGUE=<league.json>: the owner's multiplayer league (a BBGM export),
// loaded with the realistic front offices on, to put the offers its human
// teams made to the vanilla CPU to ours.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

const unwrap = (x: unknown) =>
	Array.isArray(x) &&
	x.length > 0 &&
	typeof x[0] === "object" &&
	x[0] &&
	"start" in x[0]
		? (x as { value: unknown }[]).at(-1)!.value
		: x;

export const loadOwnerLeague = async (file: string) => {
	const fs = await importNode("node:fs/promises");
	const data = JSON.parse(await fs.readFile(file, "utf8"));
	const ga = data.gameAttributes;
	const settings: Record<string, unknown> =
		helpers.deepCopy(getDefaultSettings());
	for (const key of Object.keys(settings)) {
		if (Object.hasOwn(ga, key)) {
			settings[key] = unwrap(ga[key]);
		}
	}
	(idb as any).league = undefined;
	await league.createStream(data, {
		confs: unwrap(ga.confs),
		divs: unwrap(ga.divs),
		fromFile: {
			gameAttributes: ga,
			hasRookieContracts: true,
			maxGid: undefined,
			startingSeason: data.startingSeason ?? unwrap(ga.startingSeason),
			teams: data.teams,
			version: data.version,
		},
		getLeagueOptions: undefined,
		keptKeys: new Set([...Object.keys(data), "startingSeason", "version"]),
		lid: 77,
		name: "Owner",
		setLeagueCreationStatus: () => {},
		settings,
		shuffleRosters: false,
		startingSeasonFromInput: undefined,
		teamsFromInput: data.teams,
		tid: 0,
	} as any);
	await league.loadGameAttributes();
};

test.skipIf(!env.OWNER_LEAGUE)(
	"owner's league loads",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const t0 = Date.now();
		await loadOwnerLeague(env.OWNER_LEAGUE!);
		console.log(
			"loaded in",
			(Date.now() - t0) / 1000,
			"s; season",
			g.get("season"),
			"phase",
			g.get("phase"),
			"userTids",
			g.get("userTids"),
			"players",
			(await idb.cache.players.getAll()).length,
		);
		await league.setGameAttributes({ realisticFrontOffices: true });
		const t1 = Date.now();
		const outlook = await currentOutlook();
		console.log("outlook in", (Date.now() - t1) / 1000, "s");
		for (const [tid, o] of outlook.teams) {
			if (o.title[0]! > 0.05) {
				console.log(
					"tid",
					tid,
					"title now",
					o.title[0]!.toFixed(3),
					"strength",
					o.strength[0]!.toFixed(1),
				);
			}
		}
	},
);

// The offers the owner's human teams made to the vanilla CPU, put to both
// the vanilla CPU and ours on draft day in the owner's league.
test.skipIf(!env.OWNER_LEAGUE)(
	"owner's league: human offers, vanilla vs ours",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const { game, phase, trade } = await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		const { ValueChangeCalculator } =
			await import("../../worker/core/team/ValueChangeCalculator.ts");
		const { judgeHumanOffer } =
			await import("../../worker/core/realisticFrontOffice/humanTrades.ts");
		const isUntradable = (
			await import("../../worker/core/trade/isUntradable.ts")
		).default;
		await loadOwnerLeague(env.OWNER_LEAGUE!);
		const conditions = {} as any;
		await league.setGameAttributes({ realisticFrontOffices: false });
		let guard = 0;
		while (g.get("phase") !== PHASE.DRAFT && guard++ < 20) {
			if (g.get("phase") === PHASE.PLAYOFFS) {
				await game.play(100, conditions);
			} else {
				await phase.newPhase((g.get("phase") + 1) as any, conditions);
			}
		}
		console.log("at", g.get("season"), "phase", g.get("phase"));
		const season = g.get("season");
		const humans: number[] = g.get("userTids");
		const abbrev = new Map(
			(await idb.cache.teams.getAll()).map((t) => [t.tid, t.abbrev]),
		);
		const players = async (tid: number) =>
			(await idb.cache.players.indexGetAll("playersByTid", tid)).filter(
				(p) => !isUntradable(p).untradable,
			);
		const picks = async (tid: number) =>
			await idb.cache.draftPicks.indexGetAll("draftPicksByTid", tid);
		const pickLabel = (dp: any) =>
			`${dp.season} R${dp.round} ${abbrev.get(dp.originalTid)}${dp.pick > 0 ? ` #${dp.pick}` : ""}`;
		const pLabel = (p: any) =>
			`${p.firstName[0]}.${p.lastName} ${p.ratings.at(-1).ovr}/${p.ratings.at(-1).pot} ${season - p.born.year}y $${(p.contract.amount / 1000).toFixed(1)}M->${p.contract.exp}`;

		type S = { tid: number; pids: number[]; dpids: number[] };
		const legal = async (cpu: S, human: S) => {
			const s = await trade.summary([
				{ ...human, pidsExcluded: [], dpidsExcluded: [] },
				{ ...cpu, pidsExcluded: [], dpidsExcluded: [] },
			] as any);
			return !s.warning;
		};
		const vcc = new ValueChangeCalculator();
		const vanilla = async (cpu: S, human: S) => {
			g.setWithoutSavingToDB("userTid", human.tid);
			if (!(await legal(cpu, human))) {
				return false;
			}
			const dv = await vcc.evaluate({
				tid: cpu.tid,
				pidsAdd: human.pids,
				pidsRemove: cpu.pids,
				dpidsAdd: human.dpids,
				dpidsRemove: cpu.dpids,
				tradingPartnerTid: human.tid,
			});
			return dv > 0;
		};
		const ours = async (cpu: S, human: S, shop = true) => {
			g.setWithoutSavingToDB("userTid", human.tid);
			if (!(await legal(cpu, human))) {
				return "illegal";
			}
			g.setWithoutSavingToDB("realisticFrontOffices", true);
			try {
				return (await judgeHumanOffer(cpu, human, { shop })).kind;
			} finally {
				g.setWithoutSavingToDB("realisticFrontOffices", false);
			}
		};
		const out: Record<string, unknown[]> = {};
		const log = (key: string, row: Record<string, unknown>) => {
			(out[key] ??= []).push(row);
			console.log(key, JSON.stringify(row));
		};
		const cpuTids = [...abbrev.keys()].filter((tid) => !humans.includes(tid));

		// 1. Draft-day firsts for seconds: the fewest seconds each accepts.
		const seconds = async (tid: number) =>
			(await picks(tid))
				.filter((dp) => dp.round === 2)
				.sort(
					(a, b) =>
						Number(a.season) - Number(b.season) ||
						(a.pick || 99) - (b.pick || 99),
				);
		const richest = (
			await Promise.all(
				humans.map(async (h) => [h, (await seconds(h)).length] as const),
			)
		).sort((a, b) => b[1] - a[1])[0]![0];
		const r2 = await seconds(richest);
		console.log("seconds from", abbrev.get(richest), r2.length);
		for (const c of cpuTids) {
			for (const dp of (await picks(c)).filter(
				(dp) => dp.season === season && dp.round === 1 && dp.pick >= 5,
			)) {
				const row: Record<string, unknown> = {
					cpu: abbrev.get(c),
					pick: pickLabel(dp),
				};
				for (const [name, judge] of [
					["vanilla", vanilla],
					["ours", async (a: S, b: S) => (await ours(a, b, false)) === "yes"],
				] as const) {
					row[name] = null;
					for (let k = 1; k <= Math.min(r2.length, 8); k++) {
						if (
							await judge(
								{ tid: c, pids: [], dpids: [dp.dpid] },
								{
									tid: richest,
									pids: [],
									dpids: r2.slice(0, k).map((x) => x.dpid),
								},
							)
						) {
							row[name] = k;
							break;
						}
					}
				}
				log("firstForSeconds", row);
			}
		}

		// 2. Trading up: a human first and the fewest seconds for an earlier first.
		for (const h of humans) {
			const hs = await seconds(h);
			for (const mine of (await picks(h)).filter(
				(dp) => dp.season === season && dp.round === 1,
			)) {
				for (const c of cpuTids) {
					for (const theirs of (await picks(c)).filter(
						(dp) =>
							dp.season === season &&
							dp.round === 1 &&
							dp.pick < mine.pick &&
							dp.pick >= mine.pick - 12,
					)) {
						const row: Record<string, unknown> = {
							human: abbrev.get(h),
							give: pickLabel(mine),
							cpu: abbrev.get(c),
							get: pickLabel(theirs),
						};
						for (const [name, judge] of [
							["vanilla", vanilla],
							[
								"ours",
								async (a: S, b: S) => (await ours(a, b, false)) === "yes",
							],
						] as const) {
							row[name] = null;
							for (let k = 0; k <= Math.min(hs.length, 5); k++) {
								if (
									await judge(
										{ tid: c, pids: [], dpids: [theirs.dpid] },
										{
											tid: h,
											pids: [],
											dpids: [mine.dpid, ...hs.slice(0, k).map((x) => x.dpid)],
										},
									)
								) {
									row[name] = k;
									break;
								}
							}
						}
						log("tradeUp", row);
					}
				}
			}
		}

		// 3. A far-future first for the human's cheapest player each accepts.
		for (const c of cpuTids) {
			const far = (await picks(c))
				.filter((dp) => dp.round === 1 && dp.originalTid === c)
				.sort((a, b) => Number(b.season) - Number(a.season))[0];
			if (!far) {
				continue;
			}
			for (const h of humans) {
				const roster = (await players(h)).sort((a, b) => a.value - b.value);
				const row: Record<string, unknown> = {
					cpu: abbrev.get(c),
					pick: pickLabel(far),
					human: abbrev.get(h),
				};
				for (const [name, judge] of [
					["vanilla", vanilla],
					["ours", async (a: S, b: S) => (await ours(a, b, false)) === "yes"],
				] as const) {
					row[name] = null;
					for (const p of roster) {
						if (
							await judge(
								{ tid: c, pids: [], dpids: [far.dpid] },
								{ tid: h, pids: [p.pid], dpids: [] },
							)
						) {
							row[name] = pLabel(p);
							break;
						}
					}
				}
				log("farFirst", row);
			}
		}

		// 4. The most each gives for a human's star: add the CPU's most valuable
		// asset it still accepts, up to five.
		const assetsOf = async (tid: number) => [
			...(await players(tid))
				.sort((a, b) => b.value - a.value)
				.slice(0, 10)
				.map((p) => ({ pid: p.pid, label: pLabel(p) })),
			...(await picks(tid))
				.filter((dp) => dp.round === 1)
				.map((dp) => ({ dpid: dp.dpid, label: pickLabel(dp) })),
		];
		const squeeze = async (
			c: number,
			human: S,
			judge: (a: S, b: S) => Promise<boolean>,
		) => {
			const pool = await assetsOf(c);
			const taken: typeof pool = [];
			for (let step = 0; step < 5; step++) {
				let added = false;
				for (const x of pool) {
					if (taken.includes(x)) {
						continue;
					}
					const next = [...taken, x];
					const cpu = {
						tid: c,
						pids: next.flatMap((y) => ("pid" in y ? [y.pid!] : [])),
						dpids: next.flatMap((y) => ("dpid" in y ? [y.dpid!] : [])),
					};
					if (await judge(cpu, human)) {
						taken.push(x);
						added = true;
						break;
					}
				}
				if (!added) {
					break;
				}
			}
			return taken;
		};
		for (const h of humans) {
			const stars = (await players(h))
				.sort((a, b) => b.value - a.value)
				.slice(0, 2);
			for (const star of stars) {
				const human = { tid: h, pids: [star.pid], dpids: [] };
				for (const c of cpuTids) {
					const v = await squeeze(c, human, vanilla);
					if (v.length === 0) {
						continue;
					}
					const cpu = {
						tid: c,
						pids: v.flatMap((y) => ("pid" in y ? [y.pid!] : [])),
						dpids: v.flatMap((y) => ("dpid" in y ? [y.dpid!] : [])),
					};
					log("starHaul", {
						human: abbrev.get(h),
						star: pLabel(star),
						cpu: abbrev.get(c),
						vanillaGives: v.map((x) => x.label),
						oursOnThat: await ours(cpu, human),
					});
				}
			}
		}

		// 5. Salary dumps: the human's costliest older contracts, and the fewest
		// seconds each takes them with (0 = for nothing).
		for (const h of humans) {
			const hs = await seconds(h);
			const dumps = (await players(h))
				.filter((p) => season - p.born.year >= 30 && p.contract.exp > season)
				.sort((a, b) => b.contract.amount - a.contract.amount)
				.slice(0, 2);
			for (const p of dumps) {
				for (const c of cpuTids) {
					const row: Record<string, unknown> = {
						human: abbrev.get(h),
						dump: pLabel(p),
						cpu: abbrev.get(c),
					};
					for (const [name, judge] of [
						["vanilla", vanilla],
						["ours", async (a: S, b: S) => (await ours(a, b, false)) === "yes"],
					] as const) {
						row[name] = null;
						for (let k = 0; k <= Math.min(hs.length, 4); k++) {
							if (
								await judge(
									{ tid: c, pids: [], dpids: [] },
									{
										tid: h,
										pids: [p.pid],
										dpids: hs.slice(0, k).map((x) => x.dpid),
									},
								)
							) {
								row[name] = k;
								break;
							}
						}
					}
					if (row.vanilla !== null || row.ours !== null) {
						log("dump", row);
					}
				}
			}
		}

		if (env.OWNER_OUT) {
			const fs = await importNode("node:fs/promises");
			await fs.writeFile(env.OWNER_OUT, JSON.stringify(out, null, 1));
		}
	},
);

// The owner's league run on with the realistic front offices from the
// conference finals: every CPU trade and notable signing, to read.
test.skipIf(!env.OWNER_LEAGUE || !env.OWNER_SEASONS)(
	"owner's league: a season run by ours",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const { game, phase, draft } = await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		await loadOwnerLeague(env.OWNER_LEAGUE!);
		await league.setGameAttributes({ realisticFrontOffices: true });
		// Nobody at the controls: the human teams are run by the AI too, which
		// keeps the run going (they make no trades - only CPU teams do).
		const humans: number[] = g.get("userTids");
		const { local } = await import("../../worker/util/index.ts");
		local.autoPlayUntil = { season: 9999, phase: 0, start: Date.now() } as any;
		const conditions = {} as any;
		const abbrev = new Map(
			(await idb.cache.teams.getAll()).map((t) => [t.tid, t.abbrev]),
		);
		const season0 = g.get("season");
		const startEid = (
			await idb.merged.events.getAll({ season: season0 })
		).reduce((m: number, e: any) => Math.max(m, e.eid ?? 0), 0);
		const label = async (pid: number) => {
			const p: any =
				(await idb.cache.players.get(pid)) ??
				(await idb.league.get("players", pid));
			if (!p) {
				return `pid ${pid}`;
			}
			const r = p.ratings.at(-1);
			return `${p.firstName[0]}.${p.lastName} ${r.ovr}/${r.pot} ${g.get("season") - p.born.year}y $${(p.contract.amount / 1000).toFixed(1)}M->${p.contract.exp}`;
		};
		const target = g.get("season") + Number(env.OWNER_SEASONS);
		let guard = 0;
		while (
			!(g.get("season") === target && g.get("phase") === PHASE.PLAYOFFS) &&
			guard++ < 200
		) {
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
			} else if (ph === PHASE.FREE_AGENCY) {
				const { freeAgents } = await import("../../worker/core/index.ts");
				await freeAgents.play(g.get("daysLeft"), conditions);
				if (g.get("phase") === PHASE.FREE_AGENCY) {
					await phase.newPhase(PHASE.PRESEASON, conditions);
				}
			} else {
				await phase.newPhase((ph + 1) as any, conditions);
			}
		}
		const events: any[] = [];
		for (let s = season0; s <= g.get("season"); s++) {
			events.push(
				...(await idb.merged.events.getAll({ season: s })).filter(
					(e: any) => e.eid > startEid,
				),
			);
		}
		events.sort((a, b) => a.eid - b.eid);
		const lines: string[] = [];
		for (const e of events as any[]) {
			if (e.type === "trade") {
				const sides = await Promise.all(
					e.teams.map(
						async (t: any, k: number) =>
							`${abbrev.get(e.tids[k])} get ${(await Promise.all(t.assets.map(async (a: any) => ("pid" in a ? await label(a.pid) : `${a.season} R${a.round} ${abbrev.get(a.originalTid) ?? ""}`)))).join(", ") || "nothing"}`,
					),
				);
				lines.push(`${e.season} ph${e.phase} TRADE ${sides.join(" | ")}`);
			} else if (
				(e.type === "freeAgent" || e.type === "reSigned") &&
				e.contract &&
				e.contract.amount >= 0.12 * g.get("salaryCap")
			) {
				lines.push(
					`${e.season} ${e.type} ${abbrev.get(e.tids[0])} ${await label(e.pids[0])}`,
				);
			} else if (e.type === "release" && !humans.includes(e.tids[0])) {
				lines.push(
					`${e.season} release ${abbrev.get(e.tids[0])} ${await label(e.pids[0])}`,
				);
			}
		}
		console.log(lines.join("\n"));
		if (env.OWNER_OUT) {
			const fs = await importNode("node:fs/promises");
			await fs.writeFile(env.OWNER_OUT, lines.join("\n"));
		}
	},
);

// What each CPU team asks for its own first-round picks, by year.
test.skipIf(!env.OWNER_LEAGUE || !env.OWNER_PICKS)(
	"owner's league: pick prices by year",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const { game, phase } = await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		const { load } =
			await import("../../worker/core/realisticFrontOffice/humanTrades.ts");
		const { singlePrice } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { ValueChangeCalculator } =
			await import("../../worker/core/team/ValueChangeCalculator.ts");
		await loadOwnerLeague(env.OWNER_LEAGUE!);
		const conditions = {} as any;
		let guard = 0;
		while (g.get("phase") !== Number(env.OWNER_PICKS) && guard++ < 20) {
			if (g.get("phase") === PHASE.PLAYOFFS) {
				await game.play(100, conditions);
			} else {
				await phase.newPhase((g.get("phase") + 1) as any, conditions);
			}
		}
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		const outlook = await currentOutlook();
		const abbrev = new Map(
			(await idb.cache.teams.getAll()).map((t) => [t.tid, t.abbrev]),
		);
		const vcc = new ValueChangeCalculator();
		const rows: string[] = [];
		for (const tid of abbrev.keys()) {
			if (g.get("userTids").includes(tid)) {
				continue;
			}
			const t = await load(outlook, tid);
			const firsts = t.picks
				.filter((dp: any) => dp.round === 1 && dp.originalTid === tid)
				.sort((a: any, b: any) => a.season - b.season);
			const cells: string[] = [];
			for (const dp of firsts) {
				const ours = singlePrice(outlook, t, "-", { kind: "pick", dp } as any);
				g.setWithoutSavingToDB("userTid", 0);
				const vanilla = -(await vcc.evaluate({
					tid,
					pidsAdd: [],
					pidsRemove: [],
					dpidsAdd: [],
					dpidsRemove: [dp.dpid],
					tradingPartnerTid: 0,
				}));
				cells.push(
					`${dp.season}${dp.pick > 0 ? `#${dp.pick}` : ""} ${(-ours * 1000).toFixed(1)}/${vanilla.toFixed(1)}`,
				);
			}
			rows.push(
				`${abbrev.get(tid)} title ${(outlook.teams.get(tid)!.title[0]! * 100).toFixed(0)}%: ${cells.join("  ")}`,
			);
		}
		console.log(
			`ours (milli-titles) / vanilla, own firsts by year:\n${rows.join("\n")}`,
		);
	},
);

// Why a team's draft-day first is worth what it is to it.
test.skipIf(!env.OWNER_LEAGUE || !env.OWNER_EXPLAIN)(
	"owner's league: explain a pick",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const { game, phase } = await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		const { load } =
			await import("../../worker/core/realisticFrontOffice/humanTrades.ts");
		const { explainDeal } =
			await import("../../worker/core/realisticFrontOffice/explain.ts");
		await loadOwnerLeague(env.OWNER_LEAGUE!);
		const conditions = {} as any;
		let guard = 0;
		while (g.get("phase") !== PHASE.DRAFT && guard++ < 20) {
			if (g.get("phase") === PHASE.PLAYOFFS) {
				await game.play(100, conditions);
			} else {
				await phase.newPhase((g.get("phase") + 1) as any, conditions);
			}
		}
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		const outlook = await currentOutlook();
		const byAbbrev = new Map(
			(await idb.cache.teams.getAll()).map((t) => [t.abbrev, t.tid]),
		);
		for (const ab of env.OWNER_EXPLAIN!.split(",")) {
			const tid = byAbbrev.get(ab)!;
			const t = await load(outlook, tid);
			const other = await load(outlook, tid === 1 ? 2 : 1);
			const dp = t.picks.find(
				(x: any) =>
					x.season === g.get("season") + Number(env.OWNER_EXPLAIN_AHEAD ?? 0) &&
					x.round === 1 &&
					x.originalTid === tid,
			)!;
			const players = t.players;
			console.log(
				ab,
				"roster",
				players.length,
				"under contract next season",
				players.filter((p) => p.contract.exp > g.get("season")).length,
				"payroll",
				t.payroll,
				"pick",
				dp.pick,
			);
			const e = explainDeal(outlook, [
				{
					tid,
					players,
					picks: t.picks,
					tradedAway: t.tradedAway,
					gain: 0,
					margin: 0,
					gives: { players: [], picks: [dp] },
					gets: { players: [], picks: [] },
				},
				{
					tid: other.tid,
					players: other.players,
					picks: other.picks,
					tradedAway: other.tradedAway,
					gain: 0,
					margin: 0,
					gives: { players: [], picks: [] },
					gets: { players: [], picks: [dp] },
				},
			]);
			const s = e.sides[0];
			console.log(
				JSON.stringify({
					ab,
					seasons: e.seasons,
					before: s.before,
					after: s.after,
					titleBefore: s.titleBefore,
					value: s.value,
					money: s.money,
					roomBefore: s.roomBefore,
					roomAfter: s.roomAfter,
					buysBefore: s.buysBefore.slice(0, 3),
					buysAfter: s.buysAfter.slice(0, 3),
					pick: s.gives[0],
				}),
			);
		}
	},
);

// The draft classes already in the league (Draft Scouting), measured the way
// the pick table is (frontOffice.test.ts FO_PICKS): by slot, the overall of
// those worth a roster place 1, 3 and 6 seasons after the draft, and the share
// who are.
test.skipIf(!env.OWNER_LEAGUE || !env.OWNER_CLASSES)(
	"owner's league: known classes by slot",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const { PLAYER } = await import("../../common/constants.ts");
		const { projectRatings } =
			await import("../../worker/core/realisticFrontOffice/projection.ts");
		const { lineMeasure, PATIENCE } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { playerValue, REPLACEMENT_OVR } =
			await import("../../worker/core/realisticFrontOffice/strength.ts");
		await loadOwnerLeague(env.OWNER_LEAGUE!);
		const season = g.get("season");
		const undrafted = await idb.cache.players.indexGetAll(
			"playersByTid",
			PLAYER.UNDRAFTED,
		);
		const out: Record<number, number[][][]> = {};
		for (const year of [
			...new Set(undrafted.map((p) => p.draft.year)),
		].sort()) {
			// A real prospect carries his draft season's ratings already; a
			// generated one his ratings now.
			const sample = undrafted.find((p) => p.draft.year === year)!;
			const ahead = last(sample.ratings).season >= year ? 0 : year - season;
			const years = ahead + 10;
			const board = undrafted
				.filter((p) => p.draft.year === year)
				.map((p, i) => {
					const projection = projectRatings({
						ratings: last(p.ratings),
						age: (ahead === 0 ? year : season) - p.born.year,
						years,
						samples: 16,
						seed: i,
						measure: lineMeasure,
					});
					let score = 0;
					for (let y = ahead; y <= years; y++) {
						const [roleFree, offense] = projection.measures[y]!;
						score +=
							PATIENCE ** (y - ahead) *
							Math.max(
								0,
								playerValue({ roleFree: roleFree!, offense: offense! }),
							);
					}
					return { projection, score };
				})
				.sort((a, b) => b.score - a.score);
			out[year] = board.slice(0, 60).map(({ projection }) =>
				[1, 3, 6].map((y) => {
					const o = projection.ovr[ahead + y]!;
					return [Math.round(o), o >= REPLACEMENT_OVR ? 1 : 0];
				}),
			);
			const fmt = (slots: number[]) =>
				slots
					.map((k) => `#${k + 1} ${out[year]![k]!.map((x) => x[0]).join("/")}`)
					.join("  ");
			console.log(
				`class ${year} (${board.length}): ${fmt([0, 4, 9, 14, 19, 24, 29, 34, 44])}`,
			);
		}
		if (env.OWNER_OUT) {
			const fs = await importNode("node:fs/promises");
			await fs.writeFile(env.OWNER_OUT, JSON.stringify(out));
		}
	},
);
