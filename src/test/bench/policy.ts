import type { DraftPick, Player } from "../../common/types.ts";

// THE BENCH's policy interface (docs/smart-cpu.md, "The bench"): what a bot
// running one team as a human may see and do, and the moments it is asked to.
//
// A policy acts only through what a human can do in the game's screens -
// trades proposed to the CPU and judged by the game's own trade.propose,
// contracts through the negotiation screen at what the player asks this team,
// draft picks, releases. Every moment it leaves alone falls back to what the
// game does for a team on auto play, run through the same human flows (see
// runBench.ts, "Fallbacks").

export type TradeSide = { pids: number[]; dpids: number[] };

export type TradeOffer = {
	// What the policy team gives.
	give: TradeSide;
	// What it asks for, and from whom.
	get: TradeSide & { tid: number };
};

export type TradeVerdict = {
	// Whether trade.propose would accept it now.
	accepted: boolean;
	// The trade screen's warning (salary rule, hard cap), if it blocks it.
	warning?: string;
	// The CPU's value change (vanilla), above 0 to accept.
	dv?: number;
	// The realistic front office's verdict, with that mode on.
	verdict?: string;
};

export type ContractOption = {
	exp: number;
	years: number;
	// Thousands.
	amount: number;
	// The length he wants, at his lowest price.
	wanted: boolean;
};

export type BenchContext = {
	// The team this policy runs.
	tid: number;
	// Anything in the league can be read: the CPU sees everything (owner's
	// rule), and so may the bench's bot. Shortcuts:
	roster: () => Promise<Player[]>;
	freeAgents: () => Promise<Player[]>;
	picks: () => Promise<DraftPick[]>;
	// Days to the trade deadline, or undefined once it is past or out of
	// the regular season.
	daysToDeadline: () => Promise<number | undefined>;

	// Trades. What trade.propose would answer, without making the trade (a
	// human gets the same by proposing and pressing undo).
	tradeVerdict: (offer: TradeOffer) => Promise<TradeVerdict>;
	// Proposes it through trade.propose; made if accepted. `note` is kept with
	// the trade in the run's record.
	proposeTrade: (
		offer: TradeOffer,
		note?: Record<string, unknown>,
	) => Promise<{ accepted: boolean; message: string | null }>;

	// Contracts. What he asks of this team by length (the negotiation
	// screen), or why he won't negotiate.
	contractOptions: (pid: number) => Promise<ContractOption[] | string>;
	// Signs a free agent, or re-signs one of the team's own in the re-signing
	// phase, at his ask for that length (his wanted length by default).
	// Returns an error message, or undefined once signed. With realistic
	// front offices on: an offer at his ask, which he decides on as the game
	// decides human offers.
	sign: (pid: number, exp?: number) => Promise<string | undefined>;
	// Releases players, as the roster screen does.
	release: (pids: number[]) => Promise<void>;

	// The game's own auto play for this team, for policies that take some of
	// a moment and leave the rest: one day's free agent signing, the roster
	// brought within the limits, re-signing whoever is still undecided, the
	// draft choice for a pick.
	autopilot: {
		signingDay: () => Promise<void>;
		fixRoster: () => Promise<void>;
		reSign: () => Promise<void>;
		draftChoice: (dp: DraftPick, prospects: Player[]) => Promise<number>;
	};

	log: (line: string) => void;
};

export type BenchPolicy = {
	name: string;

	// Preseason, after progression, before any games.
	preseason?: (ctx: BenchContext) => Promise<void>;

	// Regular season stops. `stops`: days before the trade deadline to stop
	// at, given the days to it as the season opens; `every`: also every so
	// many days, the whole regular season. `act` gets the days to the deadline
	// (undefined after it).
	inSeason?: {
		stops?: (daysToDeadline: number) => number[];
		every?: number;
		act: (
			ctx: BenchContext,
			daysToDeadline: number | undefined,
		) => Promise<void>;
	};

	// Each of the team's draft picks: a pid from `prospects`, or undefined for
	// the game's own choice.
	draftPick?: (
		ctx: BenchContext,
		dp: DraftPick,
		prospects: Player[],
	) => Promise<number | undefined>;

	// The re-signing phase: `expiring` are the team's players whose contracts
	// ran out (in negotiation). Sign with ctx.sign; return the pids to let go.
	// Anyone left undecided gets the game's auto play decision.
	reSign?: (
		ctx: BenchContext,
		expiring: Player[],
	) => Promise<{ letGo?: number[] } | void>;

	// Each day of free agency, before the CPU teams sign that day. Return true
	// when the policy has done the team's signing for the day; otherwise the
	// game's auto play signs for it too.
	freeAgencyDay?: (ctx: BenchContext, daysLeft: number) => Promise<boolean>;

	// false: no auto play signings in the regular season (the policy signs
	// in its own stops).
	inSeasonSignings?: boolean;
};
