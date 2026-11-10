import { PHASE, PLAYER } from "../../../common/constants.ts";
import type { Player, Team } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import {
	apron,
	birdTid,
	checkSigning,
	hardCap,
	maxFor,
	midLevelAmount,
	serviceYears,
	type SigningCheck,
} from "./cba.ts";
import { contractOf } from "./contractOf.ts";
import { realisticFreeAgencyOn } from "./isOn.ts";

// FREE AGENCY IN STAGES (owner's call, 2026-11-10; docs/free-agency-overhaul.md):
// in place of 30 days, a few stages, each one Play. Nobody signs in the
// negotiation window; the best free agents decide on the first decision day,
// the next on the second, everyone else in the open market, and minimum deals
// fill rosters at camp. On a decision day every team sees the number to beat
// - the leading offer's money and length, not who made it - and makes a final
// offer, sealed. The cap rules are CBA-lite's (cba.ts).
export const stageSettings = {
	// Off until it is built out and measured.
	on: false,
	// How many free agents, best first as free agency opens, decide on the
	// first decision day, and how many more on the second.
	tier1: 15,
	tier2: 30,
	// How much chance there is in a decision on offers (offers.ts's
	// temperature): sharper than the daily market's, so a clearly better
	// offer wins about 85-90% of the time (owner's call).
	decisionTemperature: 0.05,
	// His ask never falls below this share of what it was as free agency
	// opened, by stage: it holds through the decision days, then eases.
	floorByStage: [1, 1, 1, 0.75, 0.5],
	// A deal for less than this share of his opening ask is short - no more
	// than discountYears: a "prove it" deal, not years of a bargain.
	discountShare: 0.6,
	discountYears: 2,
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
	stagesOn() && g.get("phase") === PHASE.FREE_AGENCY;

// The stage free agency is in: daysLeft counts the stages down.
export const stageIndex = () =>
	Math.max(0, Math.min(STAGES.length - 1, STAGES.length - g.get("daysLeft")));

export const stageName = () => STAGES[stageIndex()]!;

helpers.setFreeAgencyStageName(() => (inStages() ? stageName() : undefined));

// Whether he decides at the end of this stage.
export const decidesNow = (p: Pick<Player, "faDecides">) =>
	stageIndex() >= (p.faDecides ?? OPEN_MARKET);

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
		await idb.cache.players.put(p);
	}
	return STAGES.length;
};

// The season a contract signed now starts.
const startSeason = () =>
	g.get("season") + (g.get("phase") <= PHASE.AFTER_TRADE_DEADLINE ? 0 : 1);

export const midLevelUsed = (t: Pick<Team, "midLevelSeason">) =>
	t.midLevelSeason === startSeason();

// What a team's unsigned free agents it holds Bird rights to count against
// its cap room: each at his ask (cap holds) - but not `except`.
export const capHolds = (
	tid: number,
	pool: readonly Player[],
	except?: number,
) => {
	let holds = 0;
	for (const p of pool) {
		if (
			p.pid !== except &&
			p.tid === PLAYER.FREE_AGENT &&
			birdTid(p, g.get("season")) === tid
		) {
			holds += contractOf(p).amount;
		}
	}
	return holds;
};

// Whether a team may sign him for `amount`, and how, by CBA-lite's rules:
// its payroll with its cap holds for room, its own player by Bird rights.
export const checkFor = ({
	t,
	p,
	payroll,
	holds,
	amount,
	rosterSize,
}: {
	t: Pick<Team, "tid" | "midLevelSeason">;
	p: Player;
	payroll: number;
	holds: number;
	amount: number;
	rosterSize: number;
}): SigningCheck => {
	const own = birdTid(p, g.get("season")) === t.tid;
	const service = serviceYears(p);
	const args = {
		tid: t.tid,
		amount,
		service,
		own,
		midLevelUsed: midLevelUsed(t),
		rosterSize,
	};
	// Room counts the holds; Bird rights and exceptions go by payroll alone.
	const withHolds = checkSigning({ ...args, payroll: payroll + holds });
	if (withHolds.ok || holds === 0) {
		return withHolds;
	}
	const without = checkSigning({ ...args, payroll });
	return without.ok && without.path !== "room" ? without : withHolds;
};

// The most a team may pay him a season, if anything.
export const mostFor = ({
	t,
	p,
	payroll,
	holds,
	rosterSize,
}: {
	t: Pick<Team, "tid" | "midLevelSeason">;
	p: Player;
	payroll: number;
	holds: number;
	rosterSize: number;
}) => {
	const minContract = g.get("minContract");
	const may = (amount: number) =>
		checkFor({ t, p, payroll, holds, amount, rosterSize }).ok;
	if (!may(minContract)) {
		return undefined;
	}
	const own = birdTid(p, g.get("season")) === t.tid;
	const candidates = [
		minContract,
		maxFor(serviceYears(p)),
		g.get("salaryCap") - payroll - holds,
		hardCap(t.tid) - payroll,
		midLevelAmount(),
		apron() - payroll,
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

// The least he asks for this stage.
// At camp, no more than the mid-level: a team over the cap can still sign
// him.
export const askFloor = (p: Pick<Player, "faOpenAsk">) => {
	const floor = (p.faOpenAsk ?? 0) * stageSettings.floorByStage[stageIndex()]!;
	return stageIndex() === STAGES.length - 1
		? Math.min(floor, midLevelAmount())
		: floor;
};

// The least he takes for a deal this long: a long deal only near his value.
export const leastForLength = (p: Pick<Player, "faOpenAsk">, years: number) =>
	years > stageSettings.discountYears
		? stageSettings.discountShare * (p.faOpenAsk ?? 0)
		: 0;

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
