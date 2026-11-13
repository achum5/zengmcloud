import { PHASE, PLAYER } from "../../common/constants.ts";
import { getTradeBlockedReason } from "../../common/transactionPhases.ts";
import type {
	Conditions,
	DraftPick,
	Player,
	TradeTeams,
} from "../../common/types.ts";
import { last } from "../../common/utils.ts";
import { choice } from "../../common/random.ts";
import { defaultGameAttributes } from "../../common/defaultGameAttributes.ts";
import {
	contractNegotiation,
	draft,
	freeAgents,
	game,
	league,
	phase,
	player,
	season,
	team,
	trade,
} from "../../worker/core/index.ts";
import {
	getContractDemand,
	getContractSummaryOrThrow,
} from "../../worker/core/contract/index.ts";
import { contractOptionsFor } from "../../worker/core/contractNegotiation/generateContractOptions.ts";
import getBest from "../../worker/core/freeAgents/getBest.ts";
import { getTeamOvrDiffs } from "../../worker/core/draft/runPicks.ts";
import { canBeDrafted } from "../../worker/core/draft/canBeDrafted.ts";
import { dropPlayers } from "../../worker/core/team/checkRosterSizes.ts";
import { ValueChangeCalculator } from "../../worker/core/team/ValueChangeCalculator.ts";
import { tradeValueCalculator } from "../../worker/core/vanillaPlus/index.ts";
import {
	realisticFreeAgencyOn,
	realisticTradesOn,
} from "../../worker/core/realisticFrontOffice/isOn.ts";
import { judgeHumanOffer } from "../../worker/core/realisticFrontOffice/humanTrades.ts";
import {
	decideReSignings,
	makeOffer,
} from "../../worker/core/realisticFrontOffice/freeAgency.ts";
import { offerTerms } from "../../worker/core/realisticFrontOffice/offerScreen.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import { getHardCap } from "../../worker/util/getHardCap.ts";
import { getDefaultSettings } from "../../worker/views/newLeague.ts";
import { ARENA_SETTINGS } from "../arena/settings.ts";
import type {
	BenchContext,
	BenchPolicy,
	ContractOption,
	TradeOffer,
	TradeVerdict,
} from "./policy.ts";

// THE BENCH (yardstick A, docs/smart-cpu.md): a random-players league with
// the owner's settings, one team (or a few) a human team run by a policy
// (policy.ts), every other team today's CPU, played season after season
// through the game's own code. What it answers: how often the policy's team
// wins the title.
//
// The policy teams are user teams (userTids, not spectator), so everything a
// human faces applies to them: the trade difficulty in trade.propose (the CPU
// overvalues its own players and marks down the human's picks), the signing
// difficulty in player mood (who will negotiate, and what he asks), no part
// in the CPU's trades with each other, and - by default - the owner's
// secondary hard cap on human teams at the luxury tax line.
//
// Fallbacks: whatever the policy leaves alone is decided as the game decides
// it for a user team on auto play (newPhaseResignPlayers, freeAgents.autoSign,
// checkRosterSizes, draft.runPicks), but carried out through the human
// flows: a signing goes through the negotiation screen at what the player
// asks this team (his mood's price, up to half again his demand), not at his
// demand as auto play pays, and only a player willing to negotiate with this
// team can be signed. Auto play would also put the team in the CPU's trades
// with each other; a human never is, so the bench never does.

// The owner's league (DBA): the arena's settings plus the rules that matter
// here, spelled out.
export const BENCH_SETTINGS: Record<string, unknown> = {
	...ARENA_SETTINGS,
	difficulty: 3,
	difficultyTrade: 3.5,
	difficultySigning: 2.5,
	salaryCapType: "soft",
	softCapTradeSalaryMatch: 125,
	numGames: 82,
	tradeDeadline: 0.6,
	draftType: "cola",
	maxRosterSize: 18,
	minRosterSize: 10,
	playIn: true,
	playersRefuseToNegotiate: true,
	rookiesCanRefuse: true,
	playerMoodTraits: true,
	realisticFrontOffices: false,
};

const conditions = {} as Conditions;

export type BenchOptions = {
	seed: number;
	seasons: number;
	// The policy's teams.
	tids: number[];
	policy: BenchPolicy;
	// Realistic front offices in place of today's vanilla CPU.
	fo?: boolean;
	// The owner's secondary hard cap on the policy's teams (the luxury tax
	// line), as on the human teams in his league.
	hardCap?: boolean;
	// Days between the stops for the auto play signings in the regular season.
	fallbackEvery?: number;
	// A check on the bench, not a human: the fallbacks sign as auto play
	// itself does - a free agent at his demand whatever his mood
	// (freeAgents.autoSign), an expiring player at his demand
	// (newPhaseResignPlayers) - in place of the negotiation screen.
	autoPlaySigning?: boolean;
	// League settings over BENCH_SETTINGS.
	league?: Record<string, unknown>;
	log?: (line: string) => void;
	// Called with each season's record as it ends, and the trades so far.
	onSeason?: (record: SeasonRecord, trades: TradeRecord[]) => Promise<void>;
};

export const createBenchLeague = async (options: BenchOptions) => {
	const settings: Record<string, unknown> = { ...BENCH_SETTINGS };
	Object.assign(settings, options.league ?? {});
	if (options.fo) {
		settings.realisticFrontOffices = true;
	}
	const initial: Record<string, unknown> =
		helpers.deepCopy(getDefaultSettings());
	for (const [key, value] of Object.entries(settings)) {
		if (Object.hasOwn(initial, key)) {
			initial[key] = value;
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
		lid: options.seed,
		name: "Bench",
		setLeagueCreationStatus: () => {},
		settings: initial,
		shuffleRosters: false,
		startingSeasonFromInput: undefined,
		teamsFromInput: helpers.addPopRank(helpers.getTeamsDefault()),
		tid: options.tids[0]!,
	} as any);
	await league.loadGameAttributes();
	await league.setGameAttributes({
		...settings,
		spectator: false,
		userTid: options.tids[0]!,
		userTids: options.tids,
		// Nobody gets fired.
		gracePeriodEnd: 9999,
		// Faces are drawn for the UI only and are slow to generate.
		realisticFaces: false,
		hardCapUseLuxuryTax: options.hardCap !== false,
		hardCapAmount: 0,
		hardCapTids: options.hardCap !== false ? options.tids : [],
	} as any);
};

// ---------------------------------------------------------------------------
// Records

export type PlayerRecord = {
	pid: number;
	name: string;
	age: number;
	pos: string;
	ovr: number;
	pot: number;
	value: number;
	// Thousands, and the last season.
	amount: number;
	exp: number;
	injury: number;
};

export type PickRecord = {
	dpid: number;
	season: number | "fantasy" | "expansion";
	round: number;
	originalTid: number;
};

export type TradeRecord = {
	season: number;
	phase: number;
	daysToDeadline?: number;
	tid: number;
	partner: number;
	gave: { players: PlayerRecord[]; picks: PickRecord[] };
	got: { players: PlayerRecord[]; picks: PickRecord[] };
	note?: Record<string, unknown>;
};

export type TeamSeasonRecord = {
	tid: number;
	won: number;
	lost: number;
	// -1 missed the playoffs, 0 out in the first round ... rounds = title.
	roundsWon: number;
	// The game's team rating (healthy), as the regular season opens and as
	// the playoffs begin.
	ovrStart: number;
	ovrPlayoffs: number;
};

export type PolicyTeamSeason = TeamSeasonRecord & {
	title: boolean;
	// 1 = best in the league.
	rankStart: number;
	rankPlayoffs: number;
	payroll: number;
	trades: number;
	// Free agents signed, and in the re-signing phase: his players whose
	// contracts ran out, those who wouldn't negotiate, those re-signed.
	signings: number;
	expiring: number;
	unwilling: number;
	reSigned: number;
	// Roster spots the bench had to fill with the game's own AI signing,
	// because no human flow could (playoffs, offer-only free agency).
	forced: number;
};

export type SeasonRecord = {
	season: number;
	rounds: number;
	champion: number;
	teams: TeamSeasonRecord[];
	policy: PolicyTeamSeason[];
	// Seconds: the whole season, the policy's own time, and by phase.
	seconds: number;
	policySeconds: number;
	phaseSeconds: Record<string, number>;
};

const describePlayer = (p: Player): PlayerRecord => {
	const r = last(p.ratings);
	const contract = getContractSummaryOrThrow(p);
	return {
		pid: p.pid,
		name: `${p.firstName} ${p.lastName}`,
		age: g.get("season") - p.born.year,
		pos: r.pos,
		ovr: r.ovr,
		pot: r.pot,
		value: Math.round(p.value * 10) / 10,
		amount: contract.amount,
		exp: contract.exp,
		injury: p.injury.gamesRemaining,
	};
};

const describePick = (dp: DraftPick): PickRecord => ({
	dpid: dp.dpid,
	season: dp.season,
	round: dp.round,
	originalTid: dp.originalTid,
});

const teamRating = async (tid: number) => {
	const players = await idb.cache.players.indexGetAll("playersByTid", tid);
	return team.ovr(
		players.map((p) => {
			const r = last(p.ratings);
			return {
				injury: p.injury,
				value: p.value,
				ratings: { ovr: r.ovr, ovrs: r.ovrs, pos: r.pos },
			};
		}),
	);
};

const allRatings = async () => {
	const out: number[] = [];
	for (let tid = 0; tid < g.get("numTeams"); tid++) {
		out.push(await teamRating(tid));
	}
	return out;
};

const rankOf = (ratings: number[], tid: number) =>
	1 + ratings.filter((x) => x > ratings[tid]!).length;

// ---------------------------------------------------------------------------
// The human flows, for one team.

const actAs = async (tid: number) => {
	if (g.get("userTid") !== tid) {
		await league.setGameAttributes({ userTid: tid });
	}
};

type Tally = {
	trades: number;
	signings: number;
	expiring: number;
	unwilling: number;
	reSigned: number;
	forced: number;
};
const newTally = (): Tally => ({
	trades: 0,
	signings: 0,
	expiring: 0,
	unwilling: 0,
	reSigned: 0,
	forced: 0,
});

export const makeBench = (options: BenchOptions) => {
	const log = options.log ?? (() => {});
	const trades: TradeRecord[] = [];
	const tallies = new Map<number, Tally>(
		options.tids.map((tid) => [tid, newTally()]),
	);
	// Anything a policy does moves this, so a cached trade calculator is never
	// read after the league it priced has changed.
	let version = 0;
	let calculator: { version: number; calc: ValueChangeCalculator } | undefined;
	const calculatorNow = async () => {
		if (!calculator || calculator.version !== version) {
			calculator = { version, calc: await tradeValueCalculator() };
		}
		return calculator.calc;
	};

	const daysToDeadline = async () => {
		if (g.get("phase") !== PHASE.REGULAR_SEASON) {
			return undefined;
		}
		try {
			return await season.getDaysLeftSchedule("tradeDeadline");
		} catch {
			return undefined;
		}
	};

	// What he asks of this team, by length: the negotiation screen
	// (api getNegotiationProps), from his mood's price at the length he wants.
	const askOf = async (
		tid: number,
		p: Player,
	): Promise<ContractOption[] | string> => {
		const mood = await player.moodInfo(p, tid);
		if (!mood.willing) {
			return "He refuses to negotiate with you.";
		}
		const r = last(p.ratings);
		const wantedExp = getContractDemand(p).exp;
		return contractOptionsFor(
			{ amount: mood.contractAmount / 1000, exp: wantedExp },
			player.fuzzRating(r.ovr, r.fuzz),
		).map((o) => ({
			exp: o.exp,
			years: o.years,
			amount: Math.round(o.amount * 1000),
			wanted: o.smallestAmount,
		}));
	};

	const sign = async (
		tid: number,
		pid: number,
		exp?: number,
	): Promise<string | undefined> => {
		await actAs(tid);
		const p = await idb.cache.players.get(pid);
		if (!p || p.tid !== PLAYER.FREE_AGENT) {
			return "He is not a free agent.";
		}
		if (realisticFreeAgencyOn()) {
			const terms = offerTerms(p);
			const term =
				terms.find((x) => x.exp === exp) ??
				terms.find((x) => x.exp === getContractDemand(p).exp) ??
				terms[0]!;
			const error = await makeOffer(tid, {
				pid,
				amount: term.ask,
				exp: term.exp,
			});
			if (error !== undefined) {
				return error;
			}
			version += 1;
			if (g.get("phase") === PHASE.RESIGN_PLAYERS) {
				const decided = await decideReSignings(tid, pid);
				if (!decided.some((row) => row.signed)) {
					return "He turned it down.";
				}
				tallies.get(tid)!.reSigned += 1;
			}
			return undefined;
		}
		let negotiation = await idb.cache.negotiations.get(pid);
		if (negotiation && negotiation.tid !== tid) {
			return "He is negotiating with another team.";
		}
		if (!negotiation) {
			const created = await contractNegotiation.create(pid, false, tid);
			if (typeof created === "string") {
				return created;
			}
			negotiation = created;
		}
		const options = await askOf(tid, p);
		if (typeof options === "string") {
			return options;
		}
		const option =
			exp === undefined
				? options.find((o) => o.wanted)
				: options.find((o) => o.exp === exp);
		if (!option) {
			return "He won't sign for that length.";
		}
		const result = await contractNegotiation.accept({
			negotiation,
			amount: option.amount,
			exp: option.exp,
			dryRun: false,
		});
		if (typeof result === "string") {
			return result;
		}
		await contractNegotiation.afterAccept(tid);
		version += 1;
		if (negotiation.resigning) {
			tallies.get(tid)!.reSigned += 1;
		} else {
			tallies.get(tid)!.signings += 1;
		}
		return undefined;
	};

	const release = async (tid: number, pids: number[]) => {
		for (const pid of pids) {
			const p = await idb.cache.players.get(pid);
			if (!p || p.tid !== tid) {
				continue;
			}
			await player.release(
				p,
				helpers.justDrafted(
					{ draft: p.draft, contract: getContractSummaryOrThrow(p) },
					g.get("phase"),
					g.get("season"),
				),
			);
		}
		await freeAgents.normalizeContractDemands({
			type: "dummyExpiringContracts",
			pids,
		});
		version += 1;
	};

	const teamsOf = (tid: number, offer: TradeOffer): TradeTeams => [
		{
			tid,
			pids: offer.give.pids,
			dpids: offer.give.dpids,
			pidsExcluded: [],
			dpidsExcluded: [],
		},
		{
			tid: offer.get.tid,
			pids: offer.get.pids,
			dpids: offer.get.dpids,
			pidsExcluded: [],
			dpidsExcluded: [],
		},
	];

	const blocked = () =>
		getTradeBlockedReason({
			challengeNoTrades: g.get("challengeNoTrades"),
			gameOver: g.get("gameOver"),
			otherTeamsWantToHire: g.get("otherTeamsWantToHire"),
			phase: g.get("phase"),
			spectator: g.get("spectator"),
			tradesAfterDeadline: g.get("tradesAfterDeadline"),
		});

	// trade.propose's test, step for step, without the trade.
	const tradeVerdict = async (
		tid: number,
		offer: TradeOffer,
	): Promise<TradeVerdict> => {
		await actAs(tid);
		if (blocked()) {
			return { accepted: false, warning: "Trades are closed now." };
		}
		if (g.get("userTids").includes(offer.get.tid)) {
			return { accepted: false, warning: "Another human team." };
		}
		const teams = teamsOf(tid, offer);
		const s = await trade.summary(teams);
		if (s.warning) {
			return { accepted: false, warning: s.warning };
		}
		if (realisticTradesOn()) {
			const verdict = await judgeHumanOffer(teams[1], teams[0]);
			return { accepted: verdict.kind === "yes", verdict: verdict.kind };
		}
		const dv = await (
			await calculatorNow()
		).evaluate({
			tid: offer.get.tid,
			pidsAdd: offer.give.pids,
			pidsRemove: offer.get.pids,
			dpidsAdd: offer.give.dpids,
			dpidsRemove: offer.get.dpids,
			tradingPartnerTid: tid,
		});
		return { accepted: dv > 0, dv };
	};

	const proposeTrade = async (
		tid: number,
		offer: TradeOffer,
		note?: Record<string, unknown>,
	) => {
		await actAs(tid);
		if (g.get("userTids").includes(offer.get.tid)) {
			return { accepted: false, message: "Another human team." };
		}
		const teams = teamsOf(tid, offer);
		const gave = {
			players: (await idb.cache.players.getAll())
				.filter((p) => offer.give.pids.includes(p.pid))
				.map(describePlayer),
			picks: (await idb.cache.draftPicks.getAll())
				.filter((dp) => offer.give.dpids.includes(dp.dpid))
				.map(describePick),
		};
		const got = {
			players: (await idb.cache.players.getAll())
				.filter((p) => offer.get.pids.includes(p.pid))
				.map(describePlayer),
			picks: (await idb.cache.draftPicks.getAll())
				.filter((dp) => offer.get.dpids.includes(dp.dpid))
				.map(describePick),
		};
		const days = await daysToDeadline();
		await trade.create(teams);
		const result = await trade.propose(false);
		version += 1;
		if (result.accepted) {
			tallies.get(tid)!.trades += 1;
			trades.push({
				season: g.get("season"),
				phase: g.get("phase"),
				daysToDeadline: days,
				tid,
				partner: offer.get.tid,
				gave,
				got,
				note,
			});
			const show = (ps: PlayerRecord[]) =>
				ps.map((p) => `#${p.pid} ${p.age}y ${p.ovr}/${p.pot}`).join(", ");
			log(
				`${g.get("season")} trade ${tid} <-> ${offer.get.tid}: gave ${show(gave.players)}${gave.picks.length ? ` +${gave.picks.length} picks` : ""}; got ${show(got.players)}${got.picks.length ? ` +${got.picks.length} picks` : ""}`,
			);
		}
		return { accepted: result.accepted, message: result.message };
	};

	// ---- Fallbacks: the game's auto play for this team, through the human
	// flows.

	// Auto play's own signing: at his demand, no negotiation.
	const signAtDemand = async (tid: number, p: Player) => {
		const resigning = g.get("phase") === PHASE.RESIGN_PLAYERS;
		await player.sign(p, tid, getContractDemand(p), g.get("phase"));
		await idb.cache.players.put(p);
		if (resigning) {
			await contractNegotiation.cancel(p.pid);
			tallies.get(tid)!.reSigned += 1;
		} else {
			tallies.get(tid)!.signings += 1;
		}
		await team.rosterAutoSort(tid);
		version += 1;
		return undefined;
	};

	// One day of freeAgents.autoSign for this team: some days it sits out,
	// otherwise it signs whom getBest picks - among the players who will
	// negotiate with it (a human can't sign the rest), at what they ask it.
	const signingDay = async (tid: number) => {
		const t = await idb.cache.teams.get(tid);
		if (!t) {
			return;
		}
		const probSkip = t.strategy === "rebuilding" ? 0.9 : 0.75;
		if (Math.random() < probSkip) {
			return;
		}
		let pool = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
		).sort((a, b) => b.value - a.value);
		if (!options.autoPlaySigning && !realisticFreeAgencyOn()) {
			const willing: Player[] = [];
			for (const p of pool) {
				if ((await player.moodInfo(p, tid)).willing) {
					willing.push(p);
				}
			}
			pool = willing;
		}
		const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		const payroll = await team.getPayroll(tid);
		for (let tries = 0; tries < 10 && pool.length > 0; tries++) {
			const p = getBest(roster, pool, payroll, getHardCap(tid));
			if (!p) {
				return;
			}
			const error = options.autoPlaySigning
				? await signAtDemand(tid, p)
				: await sign(tid, p.pid);
			if (error === undefined) {
				return;
			}
			pool = pool.filter((x) => x !== p);
		}
	};

	// checkRosterSizes for an AI team: the lowest valued cut when over the
	// limit, the best minimum-contract free agents signed when under it (any
	// free agent will take the minimum from a human).
	const fixRoster = async (tid: number) => {
		const players = await idb.cache.players.indexGetAll("playersByTid", tid);
		const max = g.get("maxRosterSize");
		const min = g.get("minRosterSize");
		if (players.length > max) {
			await dropPlayers(players, players.length - max);
			await freeAgents.normalizeContractDemands({
				type: "dummyExpiringContracts",
				pids: [],
			});
			version += 1;
		} else if (players.length < min) {
			const pool = (
				await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
			)
				.filter((p) => getContractDemand(p).amount === g.get("minContract"))
				.sort((a, b) => b.value - a.value);
			let size = players.length;
			while (size < min) {
				const p = pool.shift();
				if (p && (await sign(tid, p.pid)) === undefined) {
					size += 1;
					continue;
				}
				if (p) {
					continue;
				}
				// No human flow left (signings closed in the playoffs, offers
				// only with realistic front offices): the game's own AI signing.
				const q = await player.genRandomFreeAgent();
				await player.sign(q, tid, getContractDemand(q), g.get("phase"));
				await idb.cache.players.put(q);
				tallies.get(tid)!.forced += 1;
				size += 1;
			}
			version += 1;
		}
		await team.rosterAutoSort(tid);
	};

	// newPhaseResignPlayers for an AI team, decided as it decides (draft picks
	// always; others if willing, the team is worse off without him, and he
	// isn't a cheap player it passes on half the time), signed at his ask.
	const reSign = async (tid: number) => {
		if (realisticFreeAgencyOn()) {
			// The game's default offers stand, decided as free agency opens.
			return;
		}
		await actAs(tid);
		const negotiations = (await idb.cache.negotiations.getAll()).filter(
			(n) => n.tid === tid && n.resigning,
		);
		const players: Player[] = [];
		for (const n of negotiations) {
			const p = await idb.cache.players.get(n.pid);
			if (p && p.tid === PLAYER.FREE_AGENT) {
				players.push(p);
			}
		}
		const isPick = (p: Player) => p.draft.year === g.get("season");
		players.sort(
			(a, b) => Number(isPick(b)) - Number(isPick(a)) || b.value - a.value,
		);
		for (const p of players) {
			if (!isPick(p)) {
				const mood = await player.moodInfo(p, tid);
				if (!mood.willing) {
					continue;
				}
				// Valued on the roster, as the AI values him before he leaves it.
				p.tid = tid;
				await idb.cache.players.put(p);
				let dv: number;
				try {
					dv = await new ValueChangeCalculator().evaluate({
						tid,
						pidsAdd: [],
						pidsRemove: [p.pid],
						dpidsAdd: [],
						dpidsRemove: [],
						tradingPartnerTid: undefined,
					});
				} finally {
					p.tid = PLAYER.FREE_AGENT;
					await idb.cache.players.put(p);
				}
				const skipBadPlayer =
					getContractDemand(p).amount < g.get("minContract") * 2 &&
					Math.random() < 0.5;
				if (!(dv < 0) || skipBadPlayer) {
					continue;
				}
			}
			if (options.autoPlaySigning) {
				await signAtDemand(tid, p);
			} else {
				await sign(tid, p.pid);
			}
		}
	};

	// draft.runPicks's choice for an AI team.
	const draftChoice = async (dp: DraftPick, prospects: Player[]) => {
		const teamPlayers = await idb.cache.players.indexGetAll(
			"playersByTid",
			dp.tid,
		);
		const diffs = getTeamOvrDiffs(teamPlayers, prospects);
		const sorted = prospects;
		return choice(sorted, (p, i) =>
			diffs.length > 0 ? (diffs[i]! + 0.05 * p.value) ** 40 : p.value ** 69,
		).pid;
	};

	const contextFor = (tid: number): BenchContext => ({
		tid,
		roster: () => idb.cache.players.indexGetAll("playersByTid", tid),
		freeAgents: () =>
			idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT),
		picks: () => idb.cache.draftPicks.indexGetAll("draftPicksByTid", tid),
		daysToDeadline,
		tradeVerdict: (offer) => tradeVerdict(tid, offer),
		proposeTrade: (offer, note) => proposeTrade(tid, offer, note),
		contractOptions: async (pid) => {
			const p = await idb.cache.players.get(pid);
			if (!p || p.tid !== PLAYER.FREE_AGENT) {
				return "He is not a free agent.";
			}
			return askOf(tid, p);
		},
		sign: (pid, exp) => sign(tid, pid, exp),
		release: (pids) => release(tid, pids),
		autopilot: {
			signingDay: () => signingDay(tid),
			fixRoster: () => fixRoster(tid),
			reSign: () => reSign(tid),
			draftChoice,
		},
		log,
	});

	return {
		trades,
		tallies,
		contextFor,
		signingDay,
		fixRoster,
		reSign,
		draftChoice,
	};
};

// ---------------------------------------------------------------------------
// The seasons.

const now = () => Date.now() / 1000;

export const runBench = async (options: BenchOptions) => {
	const { policy, tids } = options;
	const log = options.log ?? (() => {});
	const bench = makeBench(options);
	const fallbackEvery = options.fallbackEvery ?? 7;
	const records: SeasonRecord[] = [];
	let policySeconds = 0;
	const timed = async <T>(f: () => Promise<T>) => {
		const t0 = now();
		try {
			return await f();
		} finally {
			policySeconds += now() - t0;
		}
	};
	const each = async (f: (ctx: BenchContext) => Promise<unknown>) => {
		for (const tid of tids) {
			await timed(() => f(bench.contextFor(tid)));
		}
	};
	const fixRosters = async () => {
		for (const tid of tids) {
			await bench.fixRoster(tid);
		}
	};

	// Plays this many days of the regular season or playoffs. game.play stops
	// short when a human team's roster is outside the limits (a death, say);
	// it is fixed and the days played on.
	const daysLeft = async () =>
		g.get("phase") === PHASE.PLAYOFFS ? 0 : season.getDaysLeftSchedule();
	const playDays = async (numDays: number) => {
		let left = numDays;
		let stuck = 0;
		while (left > 0) {
			await fixRosters();
			const before = await daysLeft();
			await game.play(left, conditions);
			const played = before - (await daysLeft());
			left -= played;
			if (
				g.get("phase") !== PHASE.REGULAR_SEASON &&
				g.get("phase") !== PHASE.AFTER_TRADE_DEADLINE
			) {
				return;
			}
			stuck = played > 0 ? 0 : stuck + 1;
			if (stuck > 3) {
				throw new Error(`Bench stuck playing days in ${g.get("season")}`);
			}
		}
	};

	const firstSeason = g.get("season");
	for (let k = 0; k < options.seasons; k++) {
		if (g.get("phase") !== PHASE.PRESEASON) {
			throw new Error(`Bench expects the preseason, not ${g.get("phase")}`);
		}
		const t0 = now();
		const policyAtStart = policySeconds;
		const phaseSeconds: Record<string, number> = {};
		let clock = now();
		const lap = (name: string) => {
			const t = now();
			phaseSeconds[name] = (phaseSeconds[name] ?? 0) + t - clock;
			clock = t;
		};
		const seasonNow = g.get("season");

		// Preseason: the signing day auto play has as free agency ends
		// (newPhasePreseason's autoSign), then the policy.
		if (k > 0 && policy.inSeasonSignings !== false && !policy.freeAgencyDay) {
			for (const tid of tids) {
				await bench.signingDay(tid);
			}
		}
		if (policy.preseason) {
			await each((ctx) => policy.preseason!(ctx));
		}
		await fixRosters();
		const ratingsStart = await allRatings();
		await phase.newPhase(PHASE.REGULAR_SEASON, conditions);
		lap("preseason");

		// Regular season, in stops: the policy's, and every few days for auto
		// play's signings.
		const toDeadline = await season.getDaysLeftSchedule("tradeDeadline");
		const total = await season.getDaysLeftSchedule();
		const stopsAt = new Set<number>();
		const autoSignings = policy.inSeasonSignings !== false;
		if (autoSignings) {
			for (let d = fallbackEvery; d < total; d += fallbackEvery) {
				stopsAt.add(d);
			}
		}
		const policyStops = new Set<number>();
		if (policy.inSeason) {
			for (const x of policy.inSeason.stops?.(toDeadline) ?? []) {
				if (x > 0 && x < toDeadline) {
					policyStops.add(toDeadline - x);
				}
			}
			const every = policy.inSeason.every;
			if (every) {
				for (let d = every; d < total; d += every) {
					policyStops.add(d);
				}
			}
		}
		const stops = [...new Set([...stopsAt, ...policyStops])]
			.filter((d) => d > 0 && d < total)
			.sort((a, b) => a - b);
		let played = 0;
		let lastSigning = 0;
		for (const stop of stops) {
			await playDays(stop - played);
			played = stop;
			if (autoSignings && stopsAt.has(stop)) {
				for (const tid of tids) {
					for (let d = lastSigning; d < stop; d++) {
						await bench.signingDay(tid);
					}
				}
				lastSigning = stop;
			}
			if (policyStops.has(stop)) {
				const days = await (async () => {
					try {
						return g.get("phase") === PHASE.REGULAR_SEASON
							? await season.getDaysLeftSchedule("tradeDeadline")
							: undefined;
					} catch {
						return undefined;
					}
				})();
				await each((ctx) => policy.inSeason!.act(ctx, days));
			}
		}
		await playDays(await season.getDaysLeftSchedule());
		lap("regularSeason");

		// Playoffs.
		if (g.get("phase") !== PHASE.PLAYOFFS) {
			throw new Error(`Bench expects the playoffs, not ${g.get("phase")}`);
		}
		await fixRosters();
		const ratingsPlayoffs = await allRatings();
		const payrolls = new Map<number, number>();
		for (const tid of tids) {
			payrolls.set(tid, await team.getPayroll(tid));
		}
		let guard = 0;
		while (g.get("phase") === PHASE.PLAYOFFS) {
			await game.play(100, conditions);
			guard += 1;
			if (guard > 20) {
				throw new Error("Bench stuck in the playoffs");
			}
		}
		lap("playoffs");

		// The season's results.
		const rounds = g.get("numGamesPlayoffSeries", seasonNow).length;
		const teams: TeamSeasonRecord[] = [];
		for (let tid = 0; tid < g.get("numTeams"); tid++) {
			const ts = await idb.cache.teamSeasons.indexGet(
				"teamSeasonsBySeasonTid",
				[seasonNow, tid],
			);
			teams.push({
				tid,
				won: ts?.won ?? 0,
				lost: ts?.lost ?? 0,
				roundsWon: ts?.playoffRoundsWon ?? -1,
				ovrStart: Math.round(ratingsStart[tid]! * 10) / 10,
				ovrPlayoffs: Math.round(ratingsPlayoffs[tid]! * 10) / 10,
			});
		}
		const champion = teams.find((t) => t.roundsWon === rounds)?.tid ?? -1;

		// Draft.
		if (g.get("phase") === PHASE.DRAFT_LOTTERY) {
			await phase.newPhase(PHASE.DRAFT, conditions);
		}
		if (policy.draftPick) {
			let picks = 0;
			while (g.get("phase") === PHASE.DRAFT) {
				await draft.runPicks({ type: "untilYourNextPick" }, conditions);
				if (g.get("phase") !== PHASE.DRAFT) {
					break;
				}
				const order = await draft.getOrder();
				const dp = order[0];
				if (!dp) {
					await draft.afterPicks(true, conditions);
					break;
				}
				const prospects = (
					await idb.cache.players.indexGetAll("playersByDraftYearRetiredYear", [
						[g.get("season")],
						[g.get("season"), Infinity],
					])
				)
					.filter(canBeDrafted)
					.sort((a, b) => b.value - a.value);
				if (prospects.length === 0) {
					await draft.runPicks({ type: "untilEnd" }, conditions);
					break;
				}
				const ctx = bench.contextFor(dp.tid);
				const pid =
					(await timed(() => policy.draftPick!(ctx, dp, prospects))) ??
					(await bench.draftChoice(dp, prospects));
				await draft.selectPlayer(dp, pid);
				await draft.afterPicks(order.length <= 1, conditions);
				picks += 1;
				if (picks > 100) {
					throw new Error("Bench stuck in the draft");
				}
			}
		} else if (g.get("phase") === PHASE.DRAFT) {
			await draft.runPicks({ type: "untilEnd" }, conditions);
		}
		if (g.get("phase") === PHASE.DRAFT) {
			await phase.newPhase(PHASE.AFTER_DRAFT, conditions);
		}
		lap("draft");

		// Re-signing.
		await phase.newPhase(PHASE.RESIGN_PLAYERS, conditions);
		for (const tid of tids) {
			const tally = bench.tallies.get(tid)!;
			for (const n of await idb.cache.negotiations.getAll()) {
				const p =
					n.tid === tid && n.resigning
						? await idb.cache.players.get(n.pid)
						: undefined;
				if (p) {
					tally.expiring += 1;
					if (!(await player.moodInfo(p, tid)).willing) {
						tally.unwilling += 1;
					}
				}
			}
			if (policy.reSign) {
				const ctx = bench.contextFor(tid);
				const expiring: Player[] = [];
				for (const n of await idb.cache.negotiations.getAll()) {
					if (n.tid === tid && n.resigning) {
						const p = await idb.cache.players.get(n.pid);
						if (p) {
							expiring.push(p);
						}
					}
				}
				const decision = await timed(() => policy.reSign!(ctx, expiring));
				for (const pid of decision?.letGo ?? []) {
					await contractNegotiation.cancel(pid);
				}
			}
			await bench.reSign(tid);
		}
		lap("reSign");

		// Free agency, a day at a time. A policy acts before the CPU teams
		// sign that day, as a human does; auto play's signing day falls in
		// with theirs, which come in random order - before or after them at
		// even odds here.
		await phase.newPhase(PHASE.FREE_AGENCY, conditions);
		guard = 0;
		while (g.get("phase") === PHASE.FREE_AGENCY) {
			const left = g.get("daysLeft");
			const after: number[] = [];
			for (const tid of tids) {
				const done = policy.freeAgencyDay
					? await timed(() =>
							policy.freeAgencyDay!(bench.contextFor(tid), left),
						)
					: false;
				if (!done) {
					if (Math.random() < 0.5) {
						await bench.signingDay(tid);
					} else {
						after.push(tid);
					}
				}
			}
			await freeAgents.play(1, conditions);
			for (const tid of after) {
				await bench.signingDay(tid);
			}
			if (g.get("phase") === PHASE.FREE_AGENCY && g.get("daysLeft") <= 0) {
				await phase.newPhase(PHASE.PRESEASON, conditions);
			}
			guard += 1;
			if (guard > 100) {
				throw new Error("Bench stuck in free agency");
			}
		}
		lap("freeAgency");

		const policyTeams: PolicyTeamSeason[] = tids.map((tid) => {
			const t = teams[tid]!;
			const tally = bench.tallies.get(tid)!;
			const row = {
				...t,
				title: t.roundsWon === rounds,
				rankStart: rankOf(ratingsStart, tid),
				rankPlayoffs: rankOf(ratingsPlayoffs, tid),
				payroll: payrolls.get(tid) ?? 0,
				...tally,
			};
			bench.tallies.set(tid, newTally());
			return row;
		});
		const record: SeasonRecord = {
			season: seasonNow,
			rounds,
			champion,
			teams,
			policy: policyTeams,
			seconds: Math.round((now() - t0) * 10) / 10,
			policySeconds: Math.round((policySeconds - policyAtStart) * 10) / 10,
			phaseSeconds: Object.fromEntries(
				Object.entries(phaseSeconds).map(([key, x]) => [
					key,
					Math.round(x * 10) / 10,
				]),
			),
		};
		records.push(record);
		for (const row of policyTeams) {
			log(
				`${seasonNow} ${policy.name} tid ${row.tid}: ${row.won}-${row.lost}, rank ${row.rankStart}/${row.rankPlayoffs}, rounds won ${row.roundsWon}${row.title ? " TITLE" : ""}, ${row.trades} trades, ${row.signings} signed, re-signed ${row.reSigned} of ${row.expiring} (${row.unwilling} unwilling), payroll ${Math.round(row.payroll / 100) / 10}M; champion ${champion} [${record.seconds} s, policy ${record.policySeconds} s] (${seasonNow - firstSeason + 1}/${options.seasons})`,
			);
		}
		await options.onSeason?.(record, bench.trades);
	}

	return { records, trades: bench.trades };
};
