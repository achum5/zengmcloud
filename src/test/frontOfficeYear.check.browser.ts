import { afterAll, assert, test } from "vitest";
import {
	draft,
	freeAgents,
	game,
	league,
	phase,
	season,
} from "../worker/core/index.ts";
import { g, helpers } from "../worker/util/index.ts";
import "../worker/index.ts";
import api from "../worker/api/index.ts";
import { LEAGUE_DATABASE_VERSION, PHASE, PLAYER } from "../common/constants.ts";
import { getDefaultSettings } from "../worker/views/newLeague.ts";
import { last } from "../common/utils.ts";
import { defaultGameAttributes } from "../common/defaultGameAttributes.ts";
import { idb } from "../worker/db/index.ts";
import type { Conditions } from "../common/types.ts";
import { cleanup } from "./smoke-helpers.ts";

// A year of a league with Realistic Front Offices on, in a real browser, with
// one team played by a person: trades offered to CPU teams mid-season, the
// draft pick made by hand, re-signing offers, free agent offers. About eight
// minutes, so not part of the normal run:
//
//   BROWSER_CHECKS=1 npx vitest --run --project browser-basketball \
//     --silent=false src/test/frontOfficeYear.check.browser.ts

const conditions = {} as Conditions;
const times: Record<string, number> = {};
const timed = async (label: string, work: () => Promise<unknown>) => {
	const started = performance.now();
	await work();
	const ms = performance.now() - started;
	times[label] = (times[label] ?? 0) + ms;
	console.log(`[fo-year] ${label}: ${(ms / 1000).toFixed(1)} s`);
};

const tradeTeams = (
	userTid: number,
	userPids: number[],
	otherTid: number,
	otherPids: number[],
) =>
	[
		{
			tid: userTid,
			pids: userPids,
			dpids: [],
			pidsExcluded: [],
			dpidsExcluded: [],
		},
		{
			tid: otherTid,
			pids: otherPids,
			dpids: [],
			pidsExcluded: [],
			dpidsExcluded: [],
		},
	] as Parameters<typeof api.main.createTrade>[0];

const humanTrades = async (userTid: number) => {
	const results: string[] = [];
	const mine = (
		await idb.cache.players.indexGetAll("playersByTid", userTid)
	).sort((a, b) => a.value - b.value);
	for (const otherTid of [3, 11, 19, 27]) {
		const theirs = (
			await idb.cache.players.indexGetAll("playersByTid", otherTid)
		).sort((a, b) => b.value - a.value);
		// Worst for best: a fleece.
		await api.main.createTrade(
			tradeTeams(userTid, [mine[0]!.pid], otherTid, [theirs[0]!.pid]),
		);
		const fleece = await api.main.proposeTrade(false, conditions);
		results.push(`fleece ${otherTid}: ${JSON.stringify(fleece)}`);
		// What would make it work.
		const counter = await api.main.tradeCounterOffer();
		results.push(`counter ${otherTid}: ${JSON.stringify(counter)}`);
		// A middling one-for-one.
		const mid = mine[Math.floor(mine.length / 2)]!;
		const near = theirs.find((p) => p.value <= mid.value) ?? theirs.at(-1)!;
		await api.main.createTrade(
			tradeTeams(userTid, [mid.pid], otherTid, [near.pid]),
		);
		const fair = await api.main.proposeTrade(false, conditions);
		results.push(`fair ${otherTid}: ${JSON.stringify(fair)}`);
	}
	await api.main.clearTrade();
	return results;
};

test(
	"a year with Realistic Front Offices and a team played by a person",
	{ timeout: 60 * 60 * 1000 },
	async () => {
		const settings = { ...getDefaultSettings(), realisticFrontOffices: true };
		await league.createStream(
			{},
			{
				confs: last(defaultGameAttributes.confs).value,
				divs: last(defaultGameAttributes.divs).value,
				fromFile: {
					gameAttributes: undefined,
					hasRookieContracts: true,
					maxGid: undefined,
					startingSeason: undefined,
					teams: undefined,
					version: LEAGUE_DATABASE_VERSION,
				},
				getLeagueOptions: undefined,
				keptKeys: new Set(),
				lid: 0,
				name: "Front offices",
				setLeagueCreationStatus: () => {},
				settings,
				shuffleRosters: false,
				startingSeasonFromInput: "2016",
				teamsFromInput: helpers.addPopRank(helpers.getTeamsDefault()),
				tid: 0,
			},
		);
		await league.loadGameAttributes();
		assert.isTrue(g.get("realisticFrontOffices"));
		const userTid = g.get("userTid");
		const startSeason = g.get("season");
		const log: string[] = [];
		const shown = new Map<number, number>();
		const offered: number[] = [];

		for (let guard = 0; guard < 40; guard++) {
			const ph = g.get("phase");
			if (ph === PHASE.PRESEASON) {
				if (g.get("season") > startSeason) {
					break;
				}
				await timed("to regular season", () =>
					phase.newPhase(PHASE.REGULAR_SEASON, conditions),
				);
			} else if (ph === PHASE.REGULAR_SEASON) {
				const toDeadline = await season.getDaysLeftSchedule("tradeDeadline");
				const half = Math.max(1, Math.floor(toDeadline / 2));
				await timed("regular season to midpoint", () =>
					game.play(half, conditions),
				);
				await timed("human trades", async () => {
					log.push(...(await humanTrades(userTid)));
				});
				await timed("regular season rest", async () =>
					game.play(await season.getDaysLeftSchedule(), conditions),
				);
			} else if (ph === PHASE.AFTER_TRADE_DEADLINE) {
				await timed("regular season rest", async () =>
					game.play(await season.getDaysLeftSchedule(), conditions),
				);
			} else if (ph === PHASE.PLAYOFFS) {
				await timed("playoffs", () => game.play(100, conditions));
			} else if (ph === PHASE.DRAFT_LOTTERY) {
				await timed("to draft", () => phase.newPhase(PHASE.DRAFT, conditions));
			} else if (ph === PHASE.DRAFT) {
				await timed("draft", async () => {
					for (let i = 0; i < 200 && g.get("phase") === PHASE.DRAFT; i++) {
						await draft.runPicks({ type: "untilEnd" }, conditions);
						const [dp] = await draft.getOrder();
						if (!dp) {
							break;
						}
						if (dp.tid === userTid) {
							const pool = (
								await idb.cache.players.indexGetAll(
									"playersByTid",
									PLAYER.UNDRAFTED,
								)
							)
								.filter((p) => p.draft.year === g.get("season"))
								.sort((a, b) => b.value - a.value);
							await api.main.draftUser(pool[0]!.pid, conditions);
							log.push(`drafted ${pool[0]!.pid} at ${dp.round}-${dp.pick}`);
						}
					}
					if (g.get("phase") === PHASE.DRAFT) {
						await phase.newPhase(PHASE.AFTER_DRAFT, conditions);
					}
				});
			} else if (ph === PHASE.AFTER_DRAFT) {
				await timed("to re-signing", () =>
					phase.newPhase(PHASE.RESIGN_PLAYERS, conditions),
				);
				await timed("human re-signing", async () => {
					const negotiations = await idb.cache.negotiations.getAll();
					for (const n of negotiations.filter((n) => n.tid === userTid)) {
						const props = await api.main.getNegotiationProps(n.pid);
						if (typeof props === "string" || !props || !("offer" in props)) {
							log.push(`re-sign ${n.pid}: ${JSON.stringify(props)}`);
							continue;
						}
						const p = await idb.cache.players.get(n.pid);
						const terms = {
							pid: n.pid,
							amount: helpers.roundContract(p!.contract.amount),
							exp: g.get("season") + 3,
						};
						const error = await api.main.makeFreeAgentOffer(terms);
						shown.set(n.pid, (await api.main.getOfferChance(terms)) ?? -1);
						log.push(
							`re-sign offer ${n.pid}: ${error ?? "ok"} (chance shown ${shown.get(n.pid)})`,
						);
					}
					const decided = await api.main.decideReSigningOffers();
					log.push(`re-sign decided: ${JSON.stringify(decided)}`);
					assert.isArray(decided);
					for (const d of decided as { pid: number; chance: number }[]) {
						// The odds the screen showed are the odds rolled.
						assert.closeTo(d.chance, shown.get(d.pid)!, 1e-9);
					}
				});
			} else if (ph === PHASE.RESIGN_PLAYERS) {
				await timed("to free agency", () =>
					phase.newPhase(PHASE.FREE_AGENCY, conditions),
				);
				await timed("human free agent offers", async () => {
					const pool = (
						await idb.cache.players.indexGetAll(
							"playersByTid",
							PLAYER.FREE_AGENT,
						)
					).sort((a, b) => b.value - a.value);
					// The best three at their asking price (beyond the room of most
					// teams), then minimum offers to the next best.
					for (const [i, p] of pool.slice(0, 8).entries()) {
						const props = await api.main.getNegotiationProps(p.pid);
						const terms = {
							pid: p.pid,
							amount: i < 3 ? p.contract.amount : g.get("minContract"),
							exp: g.get("season") + 2,
						};
						const error = await api.main.makeFreeAgentOffer(terms);
						if (error === undefined) {
							offered.push(p.pid);
						}
						log.push(
							`fa offer ${p.pid} (${last(p.ratings).ovr}) at ${terms.amount}: ${error ?? "ok"}; screen ${typeof props === "string" ? props : "ok"}; chance ${await api.main.getOfferChance(terms)}`,
						);
					}
				});
				await timed("free agency", async () => {
					await freeAgents.play(3, conditions);
					const board = (await idb.cache.teams.get(userTid))!.offerBoard ?? [];
					log.push(`board after 3 days: ${JSON.stringify(board)}`);
					await freeAgents.play(g.get("daysLeft"), conditions);
					if (g.get("phase") === PHASE.FREE_AGENCY) {
						await phase.newPhase(PHASE.PRESEASON, conditions);
					}
				});
				const roster = await idb.cache.players.indexGetAll(
					"playersByTid",
					userTid,
				);
				log.push(`user roster after free agency: ${roster.length}`);
				for (const pid of offered) {
					const p = await idb.cache.players.get(pid);
					log.push(
						`offered ${pid}: now tid ${p?.tid} ${p?.contract.amount}/${p?.contract.exp}`,
					);
				}
			} else {
				throw new Error(`Unexpected phase ${ph}`);
			}
		}

		console.log(`[fo-year] log\n${log.join("\n")}`);
		console.log(`[fo-year] times ${JSON.stringify(times)}`);
		assert.strictEqual(g.get("phase"), PHASE.PRESEASON);
		assert.strictEqual(g.get("season"), startSeason + 1);
	},
);

afterAll(cleanup);
