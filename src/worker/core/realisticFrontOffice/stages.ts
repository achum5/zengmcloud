import { PHASE, PLAYER } from "../../../common/constants.ts";
import type { Player, Team } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import {
	birdTid,
	checkSigning,
	hardCap,
	maxFor,
	midLevelAmount,
	serviceYears,
	type SigningCheck,
} from "./cba.ts";
import { realisticFreeAgencyOn } from "./isOn.ts";

// FREE AGENCY IN STAGES (owner's call, 2026-11-10; docs/free-agency-overhaul.md):
// in place of 30 days, a few stages, each one Play. Nobody signs in the
// negotiation window; the best free agents decide on the first decision day,
// the next on the second, everyone else in the open market, and minimum deals
// fill rosters at camp. On a decision day every team sees the number to beat
// - the leading offer's money and length, not who made it - and makes a final
// offer, sealed. The cap rules are CBA-lite's (cba.ts).
export const stageSettings = {
	// On (owner's call, 2026-11-10). League test, 2 leagues each way over 5
	// seasons against the daily market: good players signed for under 3% of
	// the cap 1 vs 23 (all of the 23 on deals of 3+ years), prices for 55-64s
	// up, contracts shorter, sims about 29% faster; about 2-3 good players a
	// summer still unsigned at camp (they sign in the season, one-year deals).
	on: true,
	// How many free agents, best first as free agency opens, decide on the
	// first decision day, and how many more on the second.
	tier1: 15,
	tier2: 30,
	// How much chance there is in a decision on offers (offers.ts's
	// temperature): sharper than the daily market's, so a clearly better
	// offer wins about 85-90% of the time (owner's call).
	decisionTemperature: 0.05,
	// At camp, waiting is worth this much less to him (offers.ts's units):
	// the season is about to start, and he wants a team.
	campWait: -0.15,
	// A deal for less than this share of his opening ask is short - no more
	// than discountYears: a "prove it" deal, not years of a bargain.
	discountShare: 0.6,
	discountYears: 1,
	// From camp on (and in the season), a deal for less than this share of
	// his opening ask is no longer than discountYears: he takes less only for
	// a season. Without, at camp 28-to-32-year-olds rated 58-60 signed for 2%
	// of the cap for four years.
	lateLongShare: 1,
	// How many free agents a CPU team prices at every length each stage, and
	// how many more offers than it has places it may have out.
	shortlist: 8,
	extraOffers: 3,
	// How many free agents, best first, the market prices and CPU teams look
	// at each stage (the daily market: 20).
	candidates: 60,
	// As free agency opens his ask is the market's, but never below this
	// share of stock's demand for him.
	openAskFloor: 0.5,
};

export const STAGES = [
	"Negotiation window",
	"Decision day 1",
	"Decision day 2",
	"Open market",
	"Training camp",
] as const;

// The stage in which the rest of the free agents decide.
const OPEN_MARKET = 3;

export const stagesOn = () => realisticFreeAgencyOn() && stageSettings.on;

// Whether free agency is running in stages right now.
export const inStages = () =>
	stagesOn() && g.get("phase") === PHASE.FREE_AGENCY && !!g.get("faInStages");

// The stage free agency is in: daysLeft counts the stages down.
export const stageIndex = () =>
	Math.max(0, Math.min(STAGES.length - 1, STAGES.length - g.get("daysLeft")));

export const stageName = () => STAGES[stageIndex()]!;

helpers.setFreeAgencyStageName(() => (inStages() ? stageName() : undefined));

// No Bird rights from a discount deal (owner's call: no signing a player
// cheap and then re-signing him over the cap).
export const hadDiscount = (p: Pick<Player, "contracts">) =>
	stagesOn() && !!p.contracts.at(-1)?.discount;

// The team holding his Bird rights: the one he played for, while he is its
// player to re-sign. As in stock, once free agency opens he is anyone's -
// unless he is restricted: then his team keeps them, to match (owner's call,
// 2026-11-11: no cap holds, so no Bird rights in free agency).
export const ownTid = (
	p: Pick<Player, "stats" | "contracts" | "tid" | "faRestricted">,
) => {
	if (hadDiscount(p)) {
		return undefined;
	}
	if (
		p.tid === PLAYER.FREE_AGENT &&
		g.get("phase") !== PHASE.RESIGN_PLAYERS &&
		!p.faRestricted
	) {
		return undefined;
	}
	return birdTid(p, g.get("season"));
};

// The team that may match another team's offer for him: his own, while he is
// restricted - not at camp, with no stage left to match in.
export const matchingTid = (
	p: Pick<Player, "stats" | "faRestricted" | "contracts" | "tid">,
) =>
	p.faRestricted && stageIndex() < STAGES.length - 1 ? ownTid(p) : undefined;

// Whether he decides at the end of this stage.
export const decidesNow = (p: Pick<Player, "faDecides">) =>
	stageIndex() >= (p.faDecides ?? OPEN_MARKET);

// Whether he signs at the end of this stage with an offer he will take
// rather than wait: on his decision day (1 or 2), and at camp, the last
// stage - the season is about to start, and he wants a team (with an offer
// in hand, a top free agent sat out the season for want of one at his ask).
export const decisionDay = (p: Pick<Player, "faDecides">) =>
	inStages() &&
	((p.faDecides !== undefined &&
		p.faDecides < OPEN_MARKET &&
		stageIndex() === p.faDecides) ||
		stageIndex() === STAGES.length - 1);

// As free agency opens: the stages to come, and when each free agent
// decides (his ask is set once the phase is on: openStageMarket).
export const openStages = async () => {
	const pool = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
	).sort((a, b) => b.valueNoPot - a.valueNoPot || a.pid - b.pid);
	for (const [i, p] of pool.entries()) {
		p.faDecides =
			i < stageSettings.tier1
				? 1
				: i < stageSettings.tier1 + stageSettings.tier2
					? 2
					: OPEN_MARKET;
		delete p.faOpenAsk;
		delete p.faLead;
		delete p.faRenounced;
		delete p.faOfferSheet;
		delete p.faMatch;
		// Restricted, coming off a rookie deal.
		if (p.contracts.at(-1)?.type === "rookie") {
			p.faRestricted = true;
		} else {
			delete p.faRestricted;
		}
		await idb.cache.players.put(p);
	}
	return STAGES.length;
};

// The season a contract signed now starts.
const startSeason = () =>
	g.get("season") + (g.get("phase") <= PHASE.AFTER_TRADE_DEADLINE ? 0 : 1);

export const midLevelUsed = (t: Pick<Team, "midLevelSeason">) =>
	t.midLevelSeason === startSeason();

// Whether a team may sign him for `amount`, and how, by CBA-lite's rules:
// room under the cap, its own player by Bird rights, the mid-level, or the
// minimum.
export const checkFor = ({
	t,
	p,
	payroll,
	amount,
	rosterSize,
}: {
	t: Pick<Team, "tid" | "midLevelSeason">;
	p: Player;
	payroll: number;
	amount: number;
	rosterSize: number;
}): SigningCheck =>
	checkSigning({
		tid: t.tid,
		payroll,
		amount,
		service: serviceYears(p),
		own: ownTid(p) === t.tid,
		midLevelUsed: midLevelUsed(t),
		rosterSize,
	});

// The most a team may pay him a season, if anything.
export const mostFor = ({
	t,
	p,
	payroll,
	rosterSize,
}: {
	t: Pick<Team, "tid" | "midLevelSeason">;
	p: Player;
	payroll: number;
	rosterSize: number;
}) => {
	const minContract = g.get("minContract");
	const may = (amount: number) =>
		checkFor({ t, p, payroll, amount, rosterSize }).ok;
	if (!may(minContract)) {
		return undefined;
	}
	const own = ownTid(p) === t.tid;
	const candidates = [
		minContract,
		maxFor(serviceYears(p)),
		g.get("salaryCap") - payroll,
		hardCap(t.tid) - payroll,
		midLevelAmount(),
	];
	let most = minContract;
	for (const amount of candidates) {
		if (amount > most && may(amount)) {
			most = amount;
		}
	}
	// Room and the mid-level cap below the max: the largest that passes.
	if (!own && most < maxFor(serviceYears(p))) {
		let lo = most;
		let hi = maxFor(serviceYears(p));
		for (let i = 0; i < 12; i++) {
			const mid = (lo + hi) / 2;
			if (may(mid)) {
				lo = mid;
			} else {
				hi = mid;
			}
		}
		most = lo;
	}
	return most;
};

// Training camp, the last stage: asks go all the way to what the market
// pays.
export const atCamp = () => stageIndex() === STAGES.length - 1;

// What waiting for more is worth to a free agent this stage.
export const stageWait = (wait: number) =>
	stageIndex() === STAGES.length - 1 ? wait + stageSettings.campWait : wait;

// The least he takes for a deal this long: a long deal only near his value,
// and from camp on only at his opening ask. Only while he is this summer's
// free agent (his opening ask is gone once he signs).
export const leastForLength = (p: Pick<Player, "faOpenAsk">, years: number) => {
	if (years <= stageSettings.discountYears || p.faOpenAsk === undefined) {
		return 0;
	}
	const phase = g.get("phase");
	if (inStages()) {
		return (
			(atCamp() ? stageSettings.lateLongShare : stageSettings.discountShare) *
			p.faOpenAsk
		);
	}
	return phase >= PHASE.PRESEASON && phase <= PHASE.AFTER_TRADE_DEADLINE
		? stageSettings.lateLongShare * p.faOpenAsk
		: 0;
};

// After a signing by the mid-level exception, the team has used it.
export const noteSigning = async (
	tid: number,
	check: SigningCheck,
): Promise<void> => {
	if (check.ok && check.path === "midLevel") {
		const t = await idb.cache.teams.get(tid);
		if (t) {
			t.midLevelSeason = startSeason();
			await idb.cache.teams.put(t);
		}
	}
};
