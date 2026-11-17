import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE OWNER'S LEAGUE UNDER THE SMART CPU, SEASON AFTER SEASON (docs/smart-cpu.md,
// "The owner's league"): every team run by the GM (spectator - the human
// teams too), everything each team did logged for a team-by-team review.
//
//   HIST_LEAGUE=<league.json> HIST_OUT=<dir> HIST_SEASONS=10 \
//     npx vitest --run --project basketball src/test/smart/history.test.ts
//
// Writes <dir>/moves.txt (a line a move), history.jsonl (snapshots each
// preseason, deadline, draft and summer, and every trade, pick, signing,
// re-signing and release), deals.jsonl (each GM trade with both sides' gain)
// and ck.json, the league saved each preseason: run again with HIST_LEAGUE
// set to it and HIST_APPEND=1 to go on. HIST_STOP_CKS=n stops after n saves
// (a long run in fresh processes).
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.HIST_LEAGUE || !env.HIST_OUT)(
	"the owner's league under the smart CPU",
	{ timeout: Number(env.HIST_TIMEOUT ?? 40_000_000) },
	async () => {
		await importNode("fake-indexeddb/auto");
		const { game, phase, draft, freeAgents, league } =
			await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { smartDealWatchers, marketSettings } =
			await import("../../worker/core/smartCpu/market.ts");
		// HIST_MARKET='{"minGain":0.001}', HIST_SMART='{"keenest":12}',
		// HIST_OUTLOOK='{"knownClasses":true}': settings to run with.
		Object.assign(marketSettings, JSON.parse(env.HIST_MARKET ?? "{}"));
		const { smartSettings } =
			await import("../../worker/core/smartCpu/worth.ts");
		Object.assign(smartSettings, JSON.parse(env.HIST_SMART ?? "{}"));
		const { outlookSettings } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		Object.assign(outlookSettings, JSON.parse(env.HIST_OUTLOOK ?? "{}"));
		// HIST_FA='{"stageAsks":true}': free agency's settings.
		const { freeAgencySettings } =
			await import("../../worker/core/realisticFrontOffice/freeAgency.ts");
		Object.assign(freeAgencySettings, JSON.parse(env.HIST_FA ?? "{}"));
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const fs = await importNode("node:fs/promises");
		const out = env.HIST_OUT!;
		await fs.mkdir(out, { recursive: true });
		const append = !!env.HIST_APPEND;
		if (!append) {
			for (const f of ["moves.txt", "history.jsonl", "deals.jsonl"]) {
				await fs.writeFile(`${out}/${f}`, "");
			}
		}
		await loadLeagueFile(env.HIST_LEAGUE!);
		if (env.HIST_SEED) {
			const { makeSeededRandom } = await import("../../common/random.ts");
			Math.random = makeSeededRandom(Number(env.HIST_SEED));
		}
		// Nobody at the controls: every team the GM's, the human teams too.
		await league.setGameAttributes({
			realisticFrontOffices: true,
			smartCpu: true,
			spectator: true,
		});
		const conditions = {} as any;
		const abbrev = new Map(
			(await idb.cache.teams.getAll()).map((t) => [t.tid, t.abbrev]),
		);
		const season0 = g.get("season");
		const startEid = (
			await idb.merged.events.getAll({ season: season0 })
		).reduce((m: number, e: any) => Math.max(m, e.eid ?? 0), 0);
		const line = (s: string) => fs.appendFile(`${out}/moves.txt`, `${s}\n`);
		const hist = (row: Record<string, unknown>) =>
			fs.appendFile(`${out}/history.jsonl`, `${JSON.stringify(row)}\n`);

		// Each GM deal with what both sides reckoned it gained (titles), and
		// what each piece was worth to each side (its use: what holding it
		// does for the team's title chances) and on the market (the price the
		// keenest other teams would pay) - so a review can see why.
		const pending: Promise<unknown>[] = [];
		smartDealWatchers.push((deal: any, view: any) => {
			const not = [deal.seller, deal.buyer];
			const read = (f: () => number) => {
				try {
					const x = f();
					return Number.isFinite(x) ? Math.round(x * 100_000) / 100_000 : null;
				} catch {
					return null;
				}
			};
			const pieces = (h: any) => [
				...h.players.map((p: any) => ({ pid: p.pid, players: [p], picks: [] })),
				...h.picks.map((dp: any) => ({
					dpid: dp.dpid,
					players: [],
					picks: [dp],
				})),
			];
			const side = (tid: number, gives: any, gets: any) => ({
				tid,
				gives: pieces(gives).map(({ players, picks, ...id }) => ({
					...id,
					use: read(() => view.useOfNow(tid, { players, picks })),
					price: read(() => view.priceNow({ players, picks }, not)),
				})),
				gets: pieces(gets).map(({ players, picks, ...id }) => ({
					...id,
					use: read(() => view.useOfNow(tid, { players, picks })),
					price: read(() => view.priceNow({ players, picks }, not)),
				})),
				givesAll: {
					use: read(() => view.useOfNow(tid, gives)),
					price: read(() => view.priceNow(gives, not)),
				},
				getsAll: {
					use: read(() => view.useOfNow(tid, gets)),
					price: read(() => view.priceNow(gets, not)),
				},
				title: view.outlook.teams
					.get(tid)
					?.title.slice(0, 4)
					.map((x: number) => Math.round(1000 * x) / 1000),
			});
			const assets = (h: any) => ({
				pids: h.players.map((p: any) => p.pid),
				dpids: h.picks.map((dp: any) => dp.dpid),
			});
			const picks = (h: any) =>
				h.picks.map((dp: any) => ({
					dpid: dp.dpid,
					season: dp.season,
					round: dp.round,
					originalTid: dp.originalTid,
				}));
			pending.push(
				fs.appendFile(
					`${out}/deals.jsonl`,
					`${JSON.stringify({
						season: g.get("season"),
						phase: g.get("phase"),
						seller: deal.seller,
						buyer: deal.buyer,
						sellerGives: assets(deal.out),
						buyerGives: assets(deal.offer),
						picks: [...picks(deal.out), ...picks(deal.offer)],
						sellerGain: deal.sellerGain,
						buyerGain: deal.buyerGain,
						sellerSide: side(deal.seller, deal.out, deal.offer),
						buyerSide: side(deal.buyer, deal.offer, deal.out),
					})}\n`,
				),
			);
		});

		// Each day of free agency for the free agents asking 8% of the cap or
		// more: his ask
		// before and after the day's move, the teams after him, the offers he
		// had and what he did (fa.jsonl) - why a good one went unsigned.
		const { freeAgencyWatchers } =
			await import("../../worker/core/realisticFrontOffice/freeAgency.ts");
		freeAgencyWatchers.push((day: any) => {
			// The good ones, by what they ask (the cache reads only async).
			const bar = 0.08 * g.get("salaryCap");
			const good = new Set<number>(
				day.priced
					.filter((x: any) => Math.max(x.from, x.to) >= bar)
					.map((x: any) => x.pid),
			);
			if (good.size === 0) {
				return;
			}
			pending.push(
				fs.appendFile(
					`${out}/fa.jsonl`,
					`${JSON.stringify({
						season: g.get("season"),
						phase: g.get("phase"),
						daysLeft: g.get("daysLeft"),
						priced: day.priced.filter((x: any) => good.has(x.pid)),
						offers: day.offers.filter((x: any) => good.has(x.pid)),
						decisions: day.decisions.filter((x: any) => good.has(x.pid)),
					})}\n`,
				),
			);
		});

		// Each GM draft pick: every prospect it weighed, with what he does for
		// the team (use), what the keenest others would pay (price), its
		// worth, and his projected overall - why a pick went where it did
		// (draft.jsonl).
		const { smartDraftWatchers } =
			await import("../../worker/core/smartCpu/draft.ts");
		const { projectPlayer } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		smartDraftWatchers.push((dp: any, worths: any, view: any) => {
			const season = g.get("season");
			const r3 = (x: number) => Math.round(x * 100_000) / 100_000;
			const rows = worths.map(({ p, worth, drafted }: any) => {
				const a = { players: [drafted], picks: [] };
				const proj = projectPlayer(p, season);
				const r = p.ratings.at(-1);
				let use = null;
				let price = null;
				try {
					use = r3(view.useOfNow(dp.tid, a));
					price = r3(view.priceNow(a, [dp.tid]));
				} catch {}
				return {
					pid: p.pid,
					name: `${p.firstName} ${p.lastName}`,
					age: season - p.born.year,
					ovr: r.ovr,
					pot: r.pot,
					value: Math.round(p.value),
					proj: proj.ovr.slice(1, 8).map((x: number) => Math.round(x)),
					boom5: Math.round(proj.boom[5]?.ovr[1] ?? 0),
					use,
					price,
					worth: r3(worth),
				};
			});
			pending.push(
				fs.appendFile(
					`${out}/draft.jsonl`,
					`${JSON.stringify({
						season,
						tid: dp.tid,
						round: dp.round,
						pick: dp.pick,
						title: view.outlook.teams
							.get(dp.tid)
							?.title.slice(0, 4)
							.map((x: number) => Math.round(1000 * x) / 1000),
						rows,
					})}\n`,
				),
			);
		});

		const playerRow = (p: any) => {
			const r = p.ratings.at(-1);
			const c = contractOf(p);
			return {
				pid: p.pid,
				name: `${p.firstName} ${p.lastName}`,
				tid: p.tid,
				age: g.get("season") - p.born.year,
				ovr: r.ovr,
				pot: r.pot,
				pos: r.pos,
				amount: c.amount,
				exp: c.exp,
				inj: p.injury?.gamesRemaining ?? 0,
				draft: p.draft
					? [p.draft.year, p.draft.round, p.draft.pick, p.draft.tid]
					: undefined,
			};
		};
		const snap = async (kind: string) => {
			const season = g.get("season");
			const all = await idb.cache.players.getAll();
			const players =
				kind === "draftClass"
					? all.filter((p: any) => p.tid === -2 && p.draft.year === season)
					: kind === "seasonEnd"
						? []
						: all.filter((p: any) => p.tid >= -1);
			const row: Record<string, unknown> = {
				kind,
				season,
				phase: g.get("phase"),
				players: players.map(playerRow),
			};
			if (kind === "preseason") {
				row.settings = Object.fromEntries(
					[
						"salaryCap",
						"minPayroll",
						"luxuryPayroll",
						"maxContract",
						"minContract",
						"luxuryTax",
						"salaryCapType",
						"numGames",
						"maxRosterSize",
						"minRosterSize",
					].map((k) => [k, g.get(k as any)]),
				);
			}
			if (kind === "draftClass") {
				row.order = (await idb.cache.draftPicks.getAll())
					.filter((dp: any) => dp.season === season)
					.map((dp: any) => ({
						tid: dp.tid,
						originalTid: dp.originalTid,
						round: dp.round,
						pick: dp.pick,
					}));
			}
			if (
				kind === "preseason" ||
				kind === "deadline" ||
				kind === "draftClass"
			) {
				try {
					const outlook: any = await currentOutlook();
					row.outlook = [...outlook.teams.entries()].map(([tid, o]: any) => ({
						tid,
						strength: o.strength
							.slice(0, 4)
							.map((x: number) => Math.round(10 * x) / 10),
						title: o.title
							.slice(0, 4)
							.map((x: number) => Math.round(1000 * x) / 1000),
					}));
				} catch (error) {
					row.outlookError = String(error);
				}
			}
			if (kind === "deadline" || kind === "seasonEnd") {
				const rows: unknown[] = [];
				for (let tid = 0; tid < g.get("numTeams"); tid++) {
					const [ts]: any[] = await idb.merged.teamSeasons.getAll({
						tid,
						season,
					});
					if (ts) {
						rows.push({
							tid,
							won: ts.won,
							lost: ts.lost,
							playoffRoundsWon: ts.playoffRoundsWon,
							luxuryTax: ts.expenses?.luxuryTax,
							minTax: ts.expenses?.minTax,
							salary: ts.expenses?.salary,
						});
					}
				}
				row.teams = rows;
			}
			await hist(row);
		};

		const label = async (pid: number) => {
			const p: any =
				(await idb.cache.players.get(pid)) ??
				(await idb.league.get("players", pid));
			if (!p) {
				return `pid ${pid}`;
			}
			const r = p.ratings.at(-1);
			const c = contractOf(p);
			return `${p.firstName} ${p.lastName} ${r.ovr}/${r.pot} ${g.get("season") - p.born.year}y $${(c.amount / 1000).toFixed(1)}M->${c.exp}`;
		};
		let seenEid = startEid;
		const flush = async () => {
			await Promise.all(pending.splice(0));
			const events: any[] = [];
			for (let s = season0; s <= g.get("season"); s++) {
				events.push(
					...(await idb.merged.events.getAll({ season: s })).filter(
						(e: any) =>
							e.eid > seenEid &&
							["trade", "draft", "freeAgent", "reSigned", "release"].includes(
								e.type,
							),
					),
				);
			}
			events.sort((a, b) => a.eid - b.eid);
			for (const e of events) {
				seenEid = Math.max(seenEid, e.eid);
				await hist({
					kind: "event",
					eid: e.eid,
					type: e.type,
					season: e.season,
					phase: e.phase ?? g.get("phase"),
					tids: e.tids,
					pids: e.pids,
					contract: e.contract,
					how: e.how,
					teams: e.teams,
					text: e.text,
				});
				if (e.type === "trade") {
					const sides = await Promise.all(
						e.teams.map(
							async (t: any, k: number) =>
								`${abbrev.get(e.tids[k])} get ${
									(
										await Promise.all(
											t.assets.map(async (a: any) =>
												"pid" in a
													? await label(a.pid)
													: `${a.season} R${a.round} ${abbrev.get(a.originalTid) ?? ""}`,
											),
										)
									).join(", ") || "nothing"
								}`,
						),
					);
					await line(
						`${e.season} ph${e.phase} TRADE ${sides.join(" | ")}${e.aiTrade ? ` (min gain ${e.aiTrade.dv})` : ""}`,
					);
				} else if (e.type !== "draft") {
					await line(
						`${e.season} ph${e.phase ?? g.get("phase")} ${e.type} ${abbrev.get(e.tids[0])} ${await label(e.pids[0])}`,
					);
				}
			}
			await line(`-- ${g.get("season")} phase ${g.get("phase")}`);
		};

		// The league saved as an export, to go on from in a fresh process.
		const checkpoint = async () => {
			// HIST_NO_CK=1: no saves (a side run that never resumes: the save
			// is a run's biggest use of memory).
			if (env.HIST_NO_CK) {
				return;
			}
			await idb.cache.flush();
			const db: any = idb.league;
			const nested = [
				"teamSeasons",
				"teamStats",
				"gameAttributes",
				"games",
				"liveGamePlayByPlay",
			];
			const data: Record<string, any> = {};
			for (const store of Array.from(db.objectStoreNames as Iterable<string>)) {
				if (!nested.includes(store)) {
					data[store] = await db.getAll(store);
				}
			}
			const group = (rows: any[], key: string) => {
				const m = new Map<number, any[]>();
				for (const r of rows) {
					m.set(r[key], [...(m.get(r[key]) ?? []), r]);
				}
				return m;
			};
			const seasons = group(await db.getAll("teamSeasons"), "tid");
			const tstats = group(await db.getAll("teamStats"), "tid");
			for (const t of data.teams) {
				t.seasons = seasons.get(t.tid) ?? [];
				t.stats = tstats.get(t.tid) ?? [];
			}
			data.gameAttributes = Object.fromEntries(
				(await db.getAll("gameAttributes")).map((r: any) => [r.key, r.value]),
			);
			data.version = db.version;
			data.startingSeason = g.get("startingSeason");
			await fs.writeFile(`${out}/ck.json.tmp`, JSON.stringify(data));
			await fs.rename(`${out}/ck.json.tmp`, `${out}/ck.json`);
		};

		if (!append) {
			await snap("preseason");
		}
		let saves = 0;
		const target =
			Number(env.HIST_UNTIL ?? 0) || season0 + Number(env.HIST_SEASONS ?? 10);
		let guard = 0;
		while (
			!(g.get("season") >= target && g.get("phase") === PHASE.PLAYOFFS) &&
			guard++ < 1000
		) {
			const ph = g.get("phase");
			if (
				ph === PHASE.PLAYOFFS ||
				ph === PHASE.REGULAR_SEASON ||
				ph === PHASE.AFTER_TRADE_DEADLINE
			) {
				await game.play(ph === PHASE.PLAYOFFS ? 100 : 7, conditions);
			} else if (ph === PHASE.DRAFT) {
				await snap("draftClass");
				await draft.runPicks({ type: "untilEnd" }, conditions);
				if (g.get("phase") === PHASE.DRAFT) {
					await phase.newPhase(PHASE.AFTER_DRAFT, conditions);
				}
			} else if (ph === PHASE.FREE_AGENCY) {
				await freeAgents.play(g.get("daysLeft"), conditions);
				if (g.get("phase") === PHASE.FREE_AGENCY) {
					await phase.newPhase(PHASE.PRESEASON, conditions);
				}
			} else {
				await phase.newPhase((ph + 1) as any, conditions);
			}
			await flush();
			const now = g.get("phase");
			if (now !== ph) {
				if (now === PHASE.REGULAR_SEASON) {
					await snap("preseason");
				} else if (now === PHASE.AFTER_TRADE_DEADLINE) {
					await snap("deadline");
				} else if (now === PHASE.RESIGN_PLAYERS) {
					await snap("resign");
				} else if (now === PHASE.FREE_AGENCY) {
					await snap("freeAgency");
				} else if (now === PHASE.DRAFT_LOTTERY) {
					await snap("seasonEnd");
				} else if (now === PHASE.PRESEASON) {
					await checkpoint();
					if (env.HIST_STOP_CKS && ++saves >= Number(env.HIST_STOP_CKS)) {
						return;
					}
				}
			}
		}
		await snap("seasonEnd");
		await line("-- done");
	},
);
