import {
	draft,
	expansionDraft,
	freeAgents,
	game,
	league,
	phase,
	player,
	season,
	team,
} from "../../worker/core/index.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import { PHASE, PLAYER } from "../../common/constants.ts";
import { getDefaultSettings } from "../../worker/views/newLeague.ts";
import { last } from "../../common/utils.ts";
import { defaultGameAttributes } from "../../common/defaultGameAttributes.ts";
import type { Conditions, Player } from "../../common/types.ts";
import {
	playerLine,
	type ValueRatings,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { ARENA_SETTINGS } from "./settings.ts";
import { ensureRetention } from "../../worker/core/realisticFrontOffice/outlook.ts";
import { stayChance } from "../../worker/core/realisticFrontOffice/offers.ts";
import { realisticFrontOfficesOn } from "../../worker/core/realisticFrontOffice/isOn.ts";
import {
	afterCuts,
	currentOutlook,
	loadTeam,
} from "../../worker/core/realisticFrontOffice/market.ts";
import {
	franchiseValue,
	teamStrengths,
} from "../../worker/core/realisticFrontOffice/outlook.ts";
import {
	contractOf,
	draftedAs,
} from "../../worker/core/realisticFrontOffice/contractOf.ts";
import { getNearestContractAmount } from "../../common/contracts.ts";
import { getPayrollSeason } from "../../worker/util/getPayrollSeason.ts";
import { upgradeLegacySnapshotStores } from "../../worker/core/sync/normalizeRecords.ts";

const conditions = {} as Conditions;

// Seconds spent from each phase to the next (the change of phase counted in
// the phase it leaves), all seasons together - layer 3's speed work.
export const phaseClock: Record<number, number> = {};

const cutCheck = async () => {
	const outlook = await currentOutlook();
	const phase = g.get("phase");
	const season = g.get("season");
	const rows: NonNullable<SeasonSnapshot["cutCheck"]> = [];
	for (let tid = 0; tid < g.get("numTeams"); tid++) {
		const t = await loadTeam(outlook, tid);
		const model = afterCuts(outlook, tid, t.players);
		if (model.players === t.players) {
			continue;
		}
		const modelValue = franchiseValue(
			outlook,
			teamStrengths(
				{ ...outlook, deadMoney: model.deadMoney },
				tid,
				model.players,
				t.picks,
			),
		);
		const over = t.players.length - model.players.length;
		let players = [...t.players];
		let dead = outlook.deadMoney;
		let realValue = 0;
		const realCut: number[] = [];
		for (let k = 0; k < over; k++) {
			let best: { p: Player; value: number; dead: typeof dead } | undefined;
			const tried = [...players]
				.sort((a, b) => a.value - b.value || a.pid - b.pid)
				.slice(0, 8);
			for (const p of tried) {
				const free = helpers.justDrafted(draftedAs(p), phase, season);
				const view = free
					? dead
					: new Map(dead).set(tid, [
							...(dead.get(tid) ?? []),
							{ amount: contractOf(p).amount, exp: contractOf(p).exp },
						]);
				const value = franchiseValue(
					outlook,
					teamStrengths(
						{ ...outlook, deadMoney: view },
						tid,
						players.filter((x) => x !== p),
						t.picks,
					),
				);
				if (!best || value > best.value) {
					best = { p, value, dead: view };
				}
			}
			players = players.filter((x) => x !== best!.p);
			dead = best!.dead;
			realValue = best!.value;
			realCut.push(best!.p.pid);
		}
		rows.push({
			tid,
			over,
			modelCut: t.players
				.filter((p) => !model.players.includes(p))
				.map((p) => p.pid),
			realCut,
			diff: realValue - modelValue,
		});
	}
	return rows;
};

const rosterSizesNow = async () => {
	const sizes: number[] = [];
	for (let tid = 0; tid < g.get("numTeams"); tid++) {
		sizes.push(
			(await idb.cache.players.indexGetAll("playersByTid", tid)).length,
		);
	}
	return sizes;
};

// What the end-of-run tables cannot reconstruct: things that only exist at a
// moment in the season (payrolls, cap space, who was a free agent).
export type SeasonSnapshot = {
	season: number;
	// The cap rule as it stood that season - a league's cap can move every year.
	salaryCap: number;
	luxuryPayroll: number;
	// End of the regular season.
	payrolls: number[];
	deadMoney: number[];
	// Trades made with at least a week still to go to the deadline, so the rest
	// went in the final week.
	tradesBeforeDeadlineWeek: number;
	tradesAtDeadline: number;
	// Start of free agency.
	capSpace: number[];
	// Every player whose contract runs out this season as the deadline's final
	// week opens, and what the front offices made of his chance of staying:
	// his chance of taking his team's offer (stay) and of still being with it
	// next season (kept). Front-office leagues only.
	deadlineExpiring?: {
		pid: number;
		tid: number;
		value: number;
		stay: number;
		kept: number;
		probWilling: number;
		// Finishing a rookie deal.
		rookie: boolean;
	}[];
	// ARENA_CUT_CHECK=1: in the preseason, each team over the roster limit -
	// what the trade pricing expects it to be worth once it cuts (afterCuts)
	// against what it is worth after the cuts it would make (cuts.ts).
	cutCheck?: {
		tid: number;
		over: number;
		modelCut: number[];
		realCut: number[];
		diff: number;
	}[];
	// Each team's roster size as the deadline's final week opens and as the
	// regular season ends, by tid.
	rosterSizes?: { deadline: number[]; end: number[] };
	// Every player whose contract ran out this season, as the re-signing phase
	// opened, and where he was once free agency had run its course.
	expiring: {
		pid: number;
		tid: number;
		value: number;
		tidAfter?: number;
		// His chance of being willing to re-sign with his team, by the mood
		// model, as the re-signing phase opened.
		probWilling: number;
		// Finishing a rookie deal.
		rookie: boolean;
		// What he was then: overall and age - and the deal he re-signed for,
		// if he did (share of the cap, years), to set against what the front
		// office expects him to cost.
		ovr?: number;
		age?: number;
		reSigned?: { amount: number; years: number };
	}[];
	// Everyone in the free agent pool as free agency opened, as the front
	// office's value model sees him then (roleFree, offense, usage, overall),
	// and the deal he got by the end of it - what a team's cap room buys.
	freeAgents: {
		pid: number;
		age: number;
		line: [number, number, number, number];
		tidAfter?: number;
		// As a share of that summer's cap.
		amount?: number;
		years?: number;
	}[];
	// Where each team's own first-round pick landed in this season's draft,
	// 0 for the first, by original team.
	draftOrder: number[];
	// Everyone on a roster as the playoffs began: age, overall and value
	// then, whether his contract ran past the season, and whether he had
	// retired by the draft lottery - to set the front office's odds of a
	// player still playing against the game's.
	retirements: {
		age: number;
		ovr: number;
		value: number;
		underContract: boolean;
		retired?: boolean;
	}[];
};

// A random-players league with the owner's settings, built the way the new
// league screen builds one: no file, default teams, the settings passed in at
// creation.
// ARENA_LEAGUE='{"draftType":"noLottery"}': league settings over the owner's.
const arenaSettings = (): Record<string, unknown> => ({
	...ARENA_SETTINGS,
	...JSON.parse((globalThis as any).process?.env?.ARENA_LEAGUE ?? "{}"),
});

export const createArenaLeague = async (lid: number) => {
	const settings: Record<string, unknown> =
		helpers.deepCopy(getDefaultSettings());
	for (const [key, value] of Object.entries(arenaSettings())) {
		if (Object.hasOwn(settings, key)) {
			settings[key] = value;
		}
	}

	await league.createStream({}, {
		confs: last(defaultGameAttributes.confs).value,
		divs: last(defaultGameAttributes.divs).value,
		fromFile: {
			gameAttributes: undefined,
			hasRookieContracts: true,
			maxGid: undefined,
			startingSeason: undefined,
			teams: undefined,
			version: undefined,
		},
		getLeagueOptions: undefined,
		keptKeys: new Set(),
		lid,
		name: "Arena",
		setLeagueCreationStatus: () => {},
		settings,
		shuffleRosters: false,
		startingSeasonFromInput: undefined,
		teamsFromInput: helpers.addPopRank(helpers.getTeamsDefault()),
		tid: 0,
	} as any);
	await league.loadGameAttributes();
	await league.setGameAttributes({
		...arenaSettings(),
		// Every team is run by the CPU.
		spectator: true,
		// Faces are drawn for the UI only and are slow to generate.
		realisticFaces: false,
	} as any);
};

const countTrades = async () =>
	(await idb.merged.events.getAll({ season: g.get("season") })).filter(
		(e) => e.type === "trade",
	).length;

const payrollsAndDeadMoney = async () => {
	const payrolls: number[] = [];
	const deadMoney: number[] = [];
	for (let tid = 0; tid < g.get("numTeams"); tid++) {
		payrolls.push(await team.getPayroll(tid));
		// Contracts of players the team released, until they're cleared once the
		// season is over (see clearPaidReleasedContracts)
		let dead = 0;
		for (const p of await idb.cache.players.indexGetAll(
			"playersByOwedByTid",
			tid,
		)) {
			for (const contract of p.contracts) {
				if (contract.endedEarly?.tid === tid) {
					dead += getNearestContractAmount(contract, getPayrollSeason());
				}
			}
		}
		deadMoney.push(dead);
	}
	return { payrolls, deadMoney };
};

// Auto play, one phase at a time and awaited, which is what league/autoPlay.ts
// does - but driven from here so the harness can look at the league between
// phases. It never sets local.autoPlayUntil, so finalize never re-enters.
export const advanceSeasons = async (
	numSeasons: number,
	options: {
		// Days before the trade deadline to stop at and call onSurvey - or a
		// function of the days to the deadline as the season opens.
		surveyAt?: number[] | ((daysToDeadline: number) => number[]);
		onSurvey?: (daysToDeadline: number) => Promise<void>;
		// Called in the preseason, once ratings have progressed, before any
		// games.
		onPreseason?: () => Promise<void>;
	} = {},
) => {
	const snapshots: SeasonSnapshot[] = [];
	const targetSeason = g.get("season") + numSeasons;
	let snapshot: SeasonSnapshot | undefined;
	let rostered: number[] = [];

	let guard = 0;
	let clocked: { phase: number; started: number } | undefined;
	const clock = () => {
		if (clocked) {
			phaseClock[clocked.phase] =
				(phaseClock[clocked.phase] ?? 0) +
				(performance.now() - clocked.started) / 1000;
		}
		clocked = { phase: g.get("phase"), started: performance.now() };
	};
	while (
		!(g.get("season") === targetSeason && g.get("phase") === PHASE.PRESEASON)
	) {
		clock();
		guard += 1;
		if (guard > numSeasons * 50) {
			throw new Error(
				`Arena stuck at season ${g.get("season")} phase ${g.get("phase")}`,
			);
		}

		const currentPhase = g.get("phase");
		// A league can hold scheduled expansion drafts.
		// Nothing changes phase after one, so this comes first, as in autoPlay.
		if (currentPhase === PHASE.EXPANSION_DRAFT) {
			if (g.get("expansionDraft").phase === "protection") {
				await expansionDraft.start();
			}
			if (g.get("expansionDraft").phase === "draft") {
				await draft.runPicks({ type: "untilEnd" }, conditions);
			}
			continue;
		}

		if (currentPhase === PHASE.PRESEASON) {
			snapshot = {
				season: g.get("season"),
				salaryCap: 0,
				luxuryPayroll: 0,
				payrolls: [],
				deadMoney: [],
				tradesBeforeDeadlineWeek: 0,
				tradesAtDeadline: 0,
				capSpace: [],
				expiring: [],
				freeAgents: [],
				draftOrder: [],
				retirements: [],
			};
			snapshots.push(snapshot);
			if (
				(globalThis as any).process?.env?.ARENA_CUT_CHECK === "1" &&
				realisticFrontOfficesOn()
			) {
				snapshot.cutCheck = await cutCheck();
			}
			await options.onPreseason?.();
			await phase.newPhase(PHASE.REGULAR_SEASON, conditions);
		} else if (currentPhase === PHASE.REGULAR_SEASON) {
			const daysToDeadline = await season.getDaysLeftSchedule("tradeDeadline");
			const surveyAt =
				typeof options.surveyAt === "function"
					? options.surveyAt(daysToDeadline)
					: options.surveyAt;
			// Stops on the way to the deadline: the start of its final week, and
			// any survey points.
			const stops = [...new Set([7, ...(surveyAt ?? [])])]
				.filter((d) => d > 0 && d < daysToDeadline)
				.sort((a, b) => b - a);
			let left = daysToDeadline;
			for (const stop of stops) {
				await game.play(left - stop, conditions);
				left = stop;
				if (stop === 7) {
					snapshot!.tradesBeforeDeadlineWeek = await countTrades();
					snapshot!.rosterSizes = {
						deadline: await rosterSizesNow(),
						end: [],
					};
					if (realisticFrontOfficesOn()) {
						snapshot!.deadlineExpiring = [];
						const view = { retention: new Map<string, number>() };
						for (const p of await idb.cache.players.indexGetAll(
							"playersByTid",
							[0, Infinity],
						)) {
							if (contractOf(p).exp !== g.get("season")) {
								continue;
							}
							await ensureRetention(view, [p], p.tid);
							snapshot!.deadlineExpiring.push({
								pid: p.pid,
								tid: p.tid,
								value: p.value,
								stay: await stayChance(p, p.tid),
								kept: view.retention.get(`${p.pid}:${p.tid}`)!,
								probWilling: (await player.moodInfo(p, p.tid)).probWilling,
								rookie: !!contractOf(p).rookie,
							});
						}
					}
				}
				if (surveyAt?.includes(stop)) {
					await options.onSurvey?.(stop);
				}
			}
			await game.play(await season.getDaysLeftSchedule(), conditions);
			snapshot!.tradesAtDeadline = await countTrades();
		} else if (currentPhase === PHASE.AFTER_TRADE_DEADLINE) {
			await game.play(await season.getDaysLeftSchedule(), conditions);
		} else if (currentPhase === PHASE.PLAYOFFS) {
			if (snapshot!.rosterSizes) {
				snapshot!.rosterSizes.end = await rosterSizesNow();
			}
			Object.assign(snapshot!, await payrollsAndDeadMoney(), {
				salaryCap: g.get("salaryCap"),
				luxuryPayroll: g.get("luxuryPayroll"),
			});
			rostered = (
				await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
			).map((p) => p.pid);
			snapshot!.retirements = [];
			for (const pid of rostered) {
				const p = (await idb.cache.players.get(pid))!;
				snapshot!.retirements.push({
					age: g.get("season") - p.born.year,
					ovr: last(p.ratings).ovr,
					value: p.value,
					underContract: contractOf(p).exp > g.get("season"),
				});
			}
			await game.play(100, conditions);
		} else if (currentPhase === PHASE.DRAFT_LOTTERY) {
			for (const [i, pid] of rostered.entries()) {
				const p = await idb.cache.players.get(pid);
				snapshot!.retirements[i]!.retired = !p || p.tid === PLAYER.RETIRED;
			}
			await phase.newPhase(PHASE.DRAFT, conditions);
		} else if (currentPhase === PHASE.DRAFT) {
			// Where each team's own first-round pick landed.
			for (const dp of await idb.cache.draftPicks.indexGetAll(
				"draftPicksBySeason",
				g.get("season"),
			)) {
				if (dp.round === 1 && dp.pick > 0) {
					snapshot!.draftOrder[dp.originalTid] = dp.pick - 1;
				}
			}
			await draft.runPicks({ type: "untilEnd" }, conditions);
			if (g.get("phase") === PHASE.DRAFT) {
				await phase.newPhase(PHASE.AFTER_DRAFT, conditions);
			}
		} else if (currentPhase === PHASE.AFTER_DRAFT) {
			const players = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);
			snapshot!.expiring = [];
			for (const p of players) {
				if (contractOf(p).exp === g.get("season")) {
					snapshot!.expiring.push({
						pid: p.pid,
						tid: p.tid,
						value: p.value,
						probWilling: (await player.moodInfo(p, p.tid)).probWilling,
						ovr: last(p.ratings).ovr,
						age: g.get("season") - p.born.year,
						rookie: !!contractOf(p).rookie,
					});
				}
			}
			await phase.newPhase(PHASE.RESIGN_PLAYERS, conditions);
			for (const row of snapshot!.expiring) {
				const p = await idb.cache.players.get(row.pid);
				if (p && p.tid === row.tid && contractOf(p).exp > g.get("season")) {
					row.reSigned = {
						amount: contractOf(p).amount / g.get("salaryCap"),
						years: contractOf(p).exp - g.get("season"),
					};
				}
			}
		} else if (currentPhase === PHASE.RESIGN_PLAYERS) {
			await phase.newPhase(PHASE.FREE_AGENCY, conditions);
			// As a share of that summer's cap.
			for (let tid = 0; tid < g.get("numTeams"); tid++) {
				snapshot!.capSpace.push(
					1 - (await team.getPayroll(tid)) / g.get("salaryCap"),
				);
			}
			for (const p of await idb.cache.players.indexGetAll(
				"playersByTid",
				PLAYER.FREE_AGENT,
			)) {
				const r = last(p.ratings);
				const line = playerLine(r as unknown as ValueRatings);
				snapshot!.freeAgents.push({
					pid: p.pid,
					age: g.get("season") - p.born.year,
					line: [line.roleFree, line.offense, line.usage, r.ovr],
				});
			}
		} else if (currentPhase === PHASE.FREE_AGENCY) {
			const cap = g.get("salaryCap");
			const season0 = g.get("season");
			await freeAgents.play(g.get("daysLeft"), conditions);
			for (const row of snapshot!.freeAgents) {
				const p = await idb.cache.players.get(row.pid);
				if (p && p.tid >= 0) {
					row.tidAfter = p.tid;
					row.amount = contractOf(p).amount / cap;
					row.years = contractOf(p).exp - season0;
				}
			}
			if (g.get("phase") === PHASE.FREE_AGENCY) {
				await phase.newPhase(PHASE.PRESEASON, conditions);
			}
			for (const row of snapshot!.expiring) {
				const p = await idb.cache.players.get(row.pid);
				row.tidAfter = p ? p.tid : PLAYER.RETIRED;
			}
		} else {
			throw new Error(`Unexpected phase ${currentPhase}`);
		}
	}
	clock();

	return snapshots;
};

// A league as it stands, carried from one arena run to the next
// (ARENA_EXPORT, ARENA_IMPORT): every record in the database but the box
// scores, a store a line. A long run in parts survives losing one.
const NOT_CARRIED = new Set(["games"]);

const importNode = (name: string) => import(/* @vite-ignore */ name);

// JSON has no Infinity.
const replacer = (_: string, value: unknown) =>
	typeof value === "number" && !Number.isFinite(value)
		? { __number: String(value) }
		: value;
const reviver = (_: string, value: any) =>
	value !== null &&
	typeof value === "object" &&
	Object.keys(value).length === 1 &&
	typeof value.__number === "string"
		? Number(value.__number)
		: value;

export const exportArenaLeague = async (file: string) => {
	await idb.cache.flush();
	const fs = await importNode("node:fs/promises");
	const handle = await fs.open(file, "w");
	try {
		for (const store of idb.league.objectStoreNames) {
			if (NOT_CARRIED.has(store)) {
				continue;
			}
			const records = await idb.league.getAll(store as any);
			await handle.write(`${JSON.stringify({ store, records }, replacer)}\n`);
		}
	} finally {
		await handle.close();
	}
};

export const importArenaLeague = async (file: string) => {
	const fs = await importNode("node:fs/promises");
	const lines = (await fs.readFile(file, "utf8")).split("\n");
	const stores: Record<string, unknown[]> = {};
	for (const line of lines) {
		if (line === "") {
			continue;
		}
		const { store, records } = JSON.parse(line, reviver);
		stores[store] = records;
	}

	// A league carried from before p.contracts (league DB version 81) has
	// players in the old shape and a releasedPlayers store, converted the way
	// the database upgrade converts a league
	upgradeLegacySnapshotStores(stores);

	// Nothing the cache holds of the league it replaces may be written back.
	idb.cache.discardForRestore();
	for (const [store, records] of Object.entries(stores)) {
		if (!idb.league.objectStoreNames.contains(store as any)) {
			continue;
		}
		const tx = idb.league.transaction(store as any, "readwrite");
		await tx.store.clear();
		for (const record of records) {
			tx.store.put(record as any);
		}
		await tx.done;
	}
	// The cache holds a season's worth of some stores, so it is filled for
	// the season carried on before the game attributes are read from it.
	const season = (await idb.league.get("gameAttributes", "season"))?.value;
	await idb.cache.fill(season);
	await league.loadGameAttributes();
};
