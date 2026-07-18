import type { FaceConfig } from "facesjs";
import type { ReactNode } from "react";
import * as z from "zod";
import type processInputs from "../worker/api/processInputs.ts";
import type * as views from "../worker/views/index.ts";

// Would be nice to make .at(-1) return T but idk how, so use the `last` function instead!
export type NonEmptyArray<T> = [T, ...T[]];

export type Env = {
	bbgmVersion: string;
	enableLogging: boolean;
	heartbeatID: string;
	mobile: boolean;
	useSharedWorker: boolean;
};

declare global {
	interface Window {
		bbgm: any; // Just for debugging
		bbgmVersion: string;
		bugsnagKey: string;
		enableLogging: boolean;
		freestar: any;
		getTheme: () => "dark" | "light";
		getThemeFilename: (theme: "dark" | "light") => string;
		mobile: boolean;
		releaseStage: "unknown" | "development" | "beta" | "production";
		themeCSSLink: HTMLLinkElement;
		useSharedWorker: boolean;
		withGoodUI: () => void;
		withGoodWorker: () => void;
	}

	const process: {
		env: {
			NODE_ENV: "development" | "production" | "test";
			SPORT: "basketball" | "football" | "baseball" | "hockey";
		};
	};
}

type ViewsKeys = keyof typeof views;

export type View<Name extends ViewsKeys> = Exclude<
	Awaited<
		Name extends ViewsKeys
			? ReturnType<(typeof views)[Name]>
			: Record<string, unknown>
	>,
	void | { redirectUrl: string } | { errorMessage: string }
>;

export type ViewInput<T extends keyof typeof processInputs> = Exclude<
	ReturnType<(typeof processInputs)[T]>,
	{ redirectUrl: string }
>;

export type AchievementWhen =
	| "afterAwards"
	| "afterFired"
	| "afterPlayoffs"
	| "afterRegularSeason";

export type Achievement = {
	slug: string;
	name: string;
	category: string;
	desc: string;
	check?: () => Promise<boolean>;
	when?: AchievementWhen;
};

export type AllStarPlayer = {
	injured?: true;
	pid: number;
	tid: number;
	name: string;
};

export type DunkAttempt = {
	toss: string;
	distance: string;
	move1: string;
	move2: string;
};

type DunkResult = {
	// Index of dunk.players
	index: number;

	// Last attempt is the first successful one
	attempts: DunkAttempt[];

	// Undefind until a successful dunk or LOWEST_POSSIBLE_SCORE
	score?: number;
	made: boolean;
};

// Done rack when there are 5 entries here
type ThreeRack = boolean[];

export type ThreeResult = {
	index: number;
	racks: ThreeRack[];
};

export type AllStars = {
	season: number;
	teamNames: [string, string];
	teams: [AllStarPlayer[], AllStarPlayer[]];
	remaining: AllStarPlayer[];
	finalized: boolean; // Refers to if draft is complete or not
	type: GameAttributesLeague["allStarType"];

	// After game is complete
	gid?: number;
	score?: [number, number];
	sPts?: [number, number]; // Only if there was a shootout
	overtimes?: number;
	mvp?: {
		pid: number;
		tid: number;
		name: string;
	};

	dunk?: {
		players: AllStarPlayer[];

		// 2 rounds, plus tiebreaker rounds
		rounds: {
			tiebreaker?: true;
			dunkers: number[]; // Index of dunk.players

			// Default is 2 dunks per player per round, but tiebreaker rounds are 1 dunk per round
			dunks: DunkResult[];
		}[];

		controlling: number[]; // Indexes of dunk.players

		// Index of players array above. Undefined if still in progress
		winner?: number;

		// 2 players each because you can't jump over yourself, but the tallest/shortest player might be a contestant
		pidsTall: [number, number];
		pidsShort: [number, number];
	};

	three?: {
		players: AllStarPlayer[];

		rounds: {
			tiebreaker?: true;
			indexes: number[]; // Index of three.players

			results: ThreeResult[];
		}[];

		// Index of players array above. Undefined if still in progress
		winner?: number;
	};
};

export type CompositeWeights<RatingKey = string> = {
	[key: string]: {
		ratings: (RatingKey | number)[];
		weights?: number[];
		skill?: {
			label: string;
			cutoff?: number;
		};
	};
};

export type Conditions = {
	hostID?: number;
};

export type DraftLotteryResultArray<Completed = boolean> = {
	tid: number;
	originalTid: number;
	chances: number;
	pick: Completed extends true ? number : number | undefined;
	dpid: number;
}[];

export type DraftLotteryResult<Completed = true> = {
	season: number;
	draftType?:
		| Exclude<
				DraftType,
				"random" | "noLottery" | "freeAgents" | "noLotteryReverse"
		  >
		| "dummy";
	rigged?: GameAttributesLeague["riggedLottery"];
	result: DraftLotteryResultArray<Completed>;
} & (
	| {
			draftType: "nba2027";
			// This is so draftTeamHistory can show accurate historical progs for the nba2027 draftType. Values are indexes of the result array, not tids!
			nba2027: {
				restricted1: number[];
				restricted5: number[];
			};
	  }
	| {
			draftType?:
				| Exclude<
						DraftType,
						| "random"
						| "noLottery"
						| "freeAgents"
						| "noLotteryReverse"
						| "nba2027"
				  >
				| "dummy";
			nba2027?: undefined;
	  }
);

export type DraftPickSeason = number | "fantasy" | "expansion";

export type DraftPickWithoutKey = {
	dpid?: number;
	tid: number;
	originalTid: number;
	round: number;
	pick: number; // 0 if not set
	season: DraftPickSeason;
	note?: string;
	noteBool?: 1; // Keep in sync with note - for indexing
};

export type DraftPick = {
	dpid: number;
} & DraftPickWithoutKey;

export type DraftType =
	| "nba1994"
	| "nba2019"
	| "noLottery"
	| "noLotteryReverse"
	| "random"
	| "coinFlip"
	| "randomLottery"
	| "randomLotteryFirst3"
	| "nba1990"
	| "freeAgents"
	| "nhl2017"
	| "nhl2021"
	| "mlb2022"
	| "custom"
	| "cola"
	| "nba2027";

// Key is team ID receiving this asset
// Why store name and extra draft pick info? For performance a bit, but mostly in case old players are deleted in a league, the trade event will still show something reasonable
type TradeEventAsset =
	| {
			pid: number;
			name: string;
			contract: PlayerContract;
			ratingsIndex: number;
			statsIndex: number;
	  }
	| {
			dpid: number;
			season: DraftPickSeason;
			round: number;
			originalTid: number;
	  };

export type TradeEventTeams = [
	{
		assets: TradeEventAsset[];
	},
	{
		assets: TradeEventAsset[];
	},
];

export type DiscriminateUnion<T, K extends keyof T, V extends T[K]> =
	T extends Record<K, V> ? T : never;

export type EventBBGMWithoutKey =
	| {
			type: Exclude<
				LogEventType,
				"sisyphus" | "trade" | "freeAgent" | "reSigned"
			>;
			text: string;
			pids?: number[];
			dpids?: number[];
			tids?: number[];
			season: number;

			// < 10: not very important
			// < 20: somewhat important
			// >= 20: very important
			score?: number;
	  }
	| {
			type: "sisyphus";
			pids: [number];
			tids: number[];
			season: number;
			wonTitle: boolean;

			// For TypeScript, never actually used
			score?: undefined;
			text?: undefined;
			dpids?: undefined;
	  }
	| {
			type: "trade";
			text?: string; // Only legacy will have text
			pids: number[];
			dpids: number[];
			tids: [number, number];
			season: number;

			// These three will only be undefind in legacy events
			phase?: Phase;
			score?: number;
			teams?: TradeEventTeams;

			// For AI-AI trades: what the AI was thinking, recorded at the moment of
			// the deal so trade history can be audited against intent (tiers match
			// event.tids order).
			aiTrade?: {
				initiatorTid: number;
				tiers: [string, string];
				dv: number;
				motivation: string;
			};
	  }
	| {
			type: "freeAgent" | "reSigned";
			text?: string; // Only legacy will have text
			pids: [number];
			tids: [number];
			season: number;

			// These three will only be undefind in legacy events
			phase?: Phase;
			score?: number;
			contract?: PlayerContract;

			// Never defined, just for TypeScript
			dpids?: number[];
	  };

export type EventBBGM = EventBBGMWithoutKey & {
	eid: number;
};

type GameTeam = {
	tid: number;
	players: any[];

	ovr?: number; // Undefined for legacy objects
	won?: number; // Undefined for legacy objects
	lost?: number; // Undefined for legacy objects
	tied?: number; // Undefined for legacy objects or if there are no ties in this sport
	otl?: number; // Undefined for legacy objects or if there are no otls in this sport

	playerFeat?: boolean;
	playoffs?: {
		seed: number;
		won: number;
		lost: number;
	};

	// This stat is guaranteed to be here in all sports. Others maybe not
	pts: number;

	// Defined only if it's a shootout
	sPts?: number;

	// For stats
	[key: string]: any;
};

export type Game = {
	att: number;
	clutchPlays?: string[];
	day?: number; // Only optional for legacy
	finals?: boolean;
	forceWin?: number; // If defined, it's the number of iterations that were used to force the win/tie
	gid: number;
	lost: {
		tid: number;
		pts: number;
		sPts?: number;
	};
	neutralSite?: boolean;
	note?: string;
	noteBool?: 1; // Keep in sync with note - for indexing
	// A "Day in the League" AI recap for the whole league day. Stored on the
	// day's ANCHOR game (the lowest-gid completed game of that season+day) so it
	// has a home in the existing games store - there's no per-day record - and
	// syncs/exports exactly like a game note. Read/written via setNote type:"day".
	dayNote?: string;
	dayNoteBool?: 1; // Keep in sync with dayNote
	numGamesToWinSeries?: number;
	numPeriods?: number; // Optional only for legacy, otherwise it's the number of periods in the game, defined at the start
	numPlayersOnCourt?: number;
	playoffs?: boolean;
	overtimes: number;
	scoringSummary?: any;
	season: number;
	teams: [GameTeam, GameTeam];
	won: {
		tid: number;
		pts: number;
		sPts?: number;
	};
};

// The saved play-by-play stream of a live-simmed game, keyed by gid, so the
// game can be re-watched later exactly as it was first simmed. Stored in its
// own league-DB store and synced to the whole room.
export type LiveGamePlayByPlay = {
	gid: number;
	season: number;
	playByPlay: any[];
};

// One team's line in a contested free-agency roll: its mood-derived odds and
// its band on the 1-100 roll, kept so the result can be shown fully.
export type FaRollTeam = {
	tid: number;
	abbrev: string;
	mood: number; // summed mood components toward this team (the UI number)
	oddsPct: number;
	lo: number; // inclusive 1-100 band
	hi: number;
};

export type FaDayResultItem =
	| {
			type: "contest";
			pid: number;
			name: string;
			round: number;
			teams: FaRollTeam[];
			roll: number; // 1-100
			winnerTid: number;
			amount: number;
			exp: number;
	  }
	| {
			type: "unopposed";
			pid: number;
			name: string;
			round: number;
			tid: number;
			abbrev: string;
			amount: number;
			exp: number;
	  }
	| {
			// The player wouldn't negotiate with the team that ranked him.
			type: "refused";
			pid: number;
			name: string;
			tid: number;
			abbrev: string;
	  }
	| {
			// The team couldn't legally offer the asking price (cap rules).
			type: "ineligible";
			pid: number;
			name: string;
			tid: number;
			abbrev: string;
	  };

// The full, transparent record of one multiplayer free-agency day: every
// board as submitted, and how each claim resolved (odds, roll, winner).
export type FaDayResults = {
	key: string; // `${season}-${daysLeft}`
	season: number;
	daysLeft: number; // before this day simmed
	items: FaDayResultItem[];
	boards: {
		tid: number;
		abbrev: string;
		pids: { pid: number; name: string }[];
	}[];
	at: number;
};

export type GamePlayer = any;

export type GameResults = any;

type GameAttributesNonLeague = { lid: undefined };

export type ScheduledEventGameAttributes = {
	type: "gameAttributes";
	season: number;
	phase: Phase;
	info: Partial<GameAttributesLeague>;
};

export type ScheduledEventTeamInfo = {
	type: "teamInfo";
	season: number;
	phase: Phase;
	info: {
		tid: number;
		abbrev?: string;
		cid?: number;
		colors?: [string, string, string];
		did?: number;
		imgURL?: string;
		imgURLSmall?: string;
		jersey?: string;
		name?: string;
		pop?: number;
		region?: string;
		srID?: string;
		stadiumCapacity?: number;
	};
};

export type ScheduledEventWithoutKey =
	| ScheduledEventTeamInfo
	| ScheduledEventGameAttributes
	| {
			type: "expansionDraft";
			season: number;
			phase: Phase;
			info: {
				// Actually stadiumCapacity is optional
				teams: (ExpansionDraftSetupTeam & {
					tid: number;
					srID?: string;
				})[];
				numProtectedPlayers?: number;
			};
	  }
	| {
			type: "contraction";
			season: number;
			phase: Phase;
			info: {
				tid: number;
			};
	  }
	| {
			type: "unretirePlayer";
			season: number;
			phase: Phase;
			info: {
				pid: number;
			};
	  };

export type ScheduledEvent = ScheduledEventWithoutKey & { id: number };

export type GameAttributeWithHistory<T> = NonEmptyArray<{
	start: number;
	value: T;
}>;

export type ExpansionDraftSetupTeam = {
	abbrev: string;
	region: string;
	name: string;
	imgURL: string | undefined;
	imgURLSmall?: string;
	colors: [string, string, string];
	jersey?: string;
	pop: string;
	stadiumCapacity: string;
	did: string;
	takeControl: boolean;

	// tid is for referencing a disabled current team
	tid?: number;
};

export type NamesLegacy = {
	first: {
		[key: string]: [string, number][];
	};
	last: {
		[key: string]: [string, number][];
	};
};

export type Conf = { cid: number; name: string };
export type Div = { cid: number; did: number; name: string };

export type InjuriesSetting = {
	name: string;
	frequency: number;
	games: number;
}[];

export type TragicDeaths = {
	reason: string;
	frequency: number;
}[];

type FootballOvertime = "suddenDeath" | "exceptFg" | "bothPossess";

export type GameAttributesLeague = {
	aiJerseyRetirement: boolean;
	aiTradesFactor: number;
	allStarGame: number | null;
	allStarNum: number;
	allStarType: "draft" | "byConf" | "top";
	allStarDunk: boolean;
	allStarThree: boolean;
	alwaysShowCountry: boolean;
	autoExpand:
		| {
				phase: "vote";
				abbrevs: string[];
		  }
		| undefined;
	autoExpandProb: number;
	autoExpandNumTeams: number;
	autoExpandMaxNumTeams: number;
	autoExpandGeo: "naFirst" | "naOnly" | "any";
	autoRelocate:
		| {
				phase: "vote";
				tid: number;
				abbrev: string;
				realigned?: number[][];
		  }
		| undefined;
	autoRelocateProb: number;
	autoRelocateGeo: "naFirst" | "naOnly" | "any";
	autoRelocateRealign: boolean;
	autoRelocateRebrand: boolean;
	brotherRate: number;
	budget: boolean;
	challengeNoDraftPicks: boolean;
	challengeNoFreeAgents: boolean;
	challengeNoRatings: boolean;
	hideRatingsOnesDigit: boolean;
	challengeNoTrades: boolean;
	challengeLoseBestPlayer: boolean;
	challengeFiredLuxuryTax: boolean;
	challengeFiredMissPlayoffs: boolean;
	challengeSisyphusMode: boolean;
	challengeThanosMode: number;
	thanosCooldownEnd: number | undefined;
	confs: NonEmptyArray<Conf>;
	daysLeft: number;
	defaultStadiumCapacity: number;
	dh: "all" | "none" | number[];
	difficulty: number;
	difficultyTrade: number | null;
	difficultySigning: number | null;
	divs: NonEmptyArray<Div>;
	draftAges: [number, number];
	draftPickAutoContract: boolean;
	draftPickAutoContractPercent: number;
	draftPickAutoContractRounds: number;
	draftType: DraftType;
	draftLotteryCustomChances: number[];
	draftLotteryCustomNumPicks: number;
	elam: boolean;
	elamASG: boolean;
	elamMinutes: number;
	elamOvertime: boolean;
	elamPoints: number;
	equalizeRegions: boolean;
	fantasyPoints?: "standard" | "ppr" | "halfPpr";
	forceRetireAge: number;
	forceRetireSeasons: number;
	foulsNeededToFoulOut: number;
	foulsUntilBonus: [number, number, number];
	foulRateFactor: number;
	gameOver: boolean;
	gender: "female" | "male";
	goatFormula?: string;
	goatSeasonFormula?: string;
	godMode: boolean;
	godModeInPast: boolean;
	gracePeriodEnd: number;
	groupScheduleSeries: boolean;
	heightFactor: number;
	hideDisabledTeams: boolean;
	hofFactor: number;
	homeCourtAdvantage: number;
	inflationAvg: number;
	inflationMax: number;
	inflationMin: number;
	inflationStd: number;
	injuries?: InjuriesSetting;
	injuryRate: number;
	lid: number;
	lowestDifficulty: number;
	luxuryPayroll: number;
	luxuryTax: number;
	maxContract: number;
	maxContractLength: number;
	maxOvertimes: number | null; // null means infinite overtimes (no ties/shootouts)
	maxOvertimesPlayoffs: number | null; // null means infinite overtimes (no shootouts)
	maxRosterSize: number;
	minContract: number;
	minContractLength: number;
	minPayroll: number;
	minRetireAge: number;
	minRosterSize: number;
	names?: NamesLegacy;
	nextPhase?: Phase;
	numActiveTeams: number;
	numDraftPicksCurrent?: number;
	numDraftRounds: number;
	numGames: number;
	numGamesDiv: number | null;
	numGamesConf: number | null;
	numGamesPlayoffSeries: number[];
	numPeriods: number;
	numPlayersDunk: number;
	numPlayersOnCourt: number;
	numPlayersThree: number;
	numPlayoffByes: number;
	numSeasonsFutureDraftPicks: number;
	numTeams: number;
	numWatchColors: number;
	playIn: boolean;
	playerMoodTraits: boolean;
	pointsFormula: string;
	shootoutRounds: number;
	shootoutRoundsPlayoffs: number;
	spectator: boolean;
	otl: boolean;
	otherTeamsWantToHire: boolean;
	phase: Phase;
	playerBioInfo?: PlayerBioInfo;
	playersRefuseToNegotiate: boolean;
	playoffsByConf: boolean;
	playoffsNumTeamsDiv: number;
	playoffsReseed: boolean;
	quarterLength: number;
	randomDebutsForever?: number;
	realDraftRatings?: "draft" | "rookie";
	realPlayerDeterminism: number;
	repeatSeason:
		| undefined
		| {
				type: "playersAndRosters";
				startingSeason: number;
				players: Record<
					number,
					{
						tid: number;
						contract: PlayerContract;
						injury: PlayerInjury;
					}
				>;
		  }
		| {
				type: "players";
				startingSeason: number;
		  };
	riggedLottery?: (number | null)[];
	rookieContractLengths: number[];
	rookiesCanRefuse: boolean;
	salaryCap: number;
	salaryCapType: "hard" | "none" | "soft";
	hardCapAmount: number;
	hardCapTids: number[];
	hardCapUseLuxuryTax: boolean;
	saveOldBoxScores: {
		pastSeasons: number | "all";
		pastSeasonsType?: "your" | "all";
		note?: "your" | "all";
		playoffs?: "your" | "all";
		finals?: "your" | "all";
		playerFeat?: "your" | "all";
		clutchPlays?: "your" | "all";
		allStar?: "all";
	};
	season: number;
	softCapTradeSalaryMatch: number;
	sonRate: number;
	startingSeason: number;
	stopOnInjury: boolean;
	stopOnInjuryGames: number;
	tiebreakers: (keyof typeof TIEBREAKERS)[];
	teamInfoCache: {
		abbrev: string;
		region: string;
		name: string;
		imgURL: string | undefined;
		imgURLSmall: string | undefined;
		disabled: boolean | undefined;
	}[];
	tradeDeadline: number;
	tradeProposalsSeed: number;
	tragicDeathRate: number;
	tragicDeaths?: TragicDeaths;
	userTid: number;
	userTids: number[];
	weightFactor: number;

	threePointers: boolean;
	threePointTendencyFactor: number;
	threePointAccuracyFactor: number;
	twoPointAccuracyFactor: number;
	ftAccuracyFactor: number;
	blockFactor: number;
	stealFactor: number;
	turnoverFactor: number;
	orbFactor: number;
	pace: number;
	expansionDraft:
		| {
				phase: "setup";
				numPerTeam?: string;
				numProtectedPlayers?: string;
				teams?: ExpansionDraftSetupTeam[];
		  }
		| {
				phase: "protection";
				numPerTeam: number;
				numProtectedPlayers: number;
				expansionTids: number[];
				protectedPids: { [key: number]: number[] };
				allowSwitchTeam: boolean;
		  }
		| {
				phase: "draft";
				numPerTeam: number;
				numPerTeamDrafted: Record<number, number>;
				expansionTids: number[];
				availablePids: number[];
		  };

	passFactor: number;
	rushYdsFactor: number;
	passYdsFactor: number;
	completionFactor: number;
	scrambleFactor: number;
	sackFactor: number;
	fumbleFactor: number;
	intFactor: number;
	fgAccuracyFactor: number;
	fourthDownFactor: number;
	onsideFactor: number;
	onsideRecoveryFactor: number;
	hitFactor: number;
	giveawayFactor: number;
	takeawayFactor: number;
	deflectionFactor: number;
	saveFactor: number;
	assistFactor: number;
	foulFactor: number;
	groundFactor: number;
	lineFactor: number;
	flyFactor: number;
	powerFactor: number;
	throwOutFactor: number;
	strikeFactor: number;
	balkFactor: number;
	wildPitchFactor: number;
	passedBallFactor: number;
	hitByPitchFactor: number;
	swingFactor: number;
	contactFactor: number;
	errorFactor: number;
	neutralSite: "never" | "finals" | "playoffs";
	rpdPot: boolean;
	currencyFormat: [string, "." | ",", string];
	overtimeLength: number;
	overtimeLengthPlayoffs: number | null;
	forceRetireRealPlayers: boolean;
	forceHistoricalRosters: boolean;
	scrimmageTouchbackKickoff: number;
	twoPointConversions: boolean;
	footballOvertime: FootballOvertime;
	footballOvertimePlayoffs: FootballOvertime;
};

type AlwaysWrap = (typeof ALWAYS_WRAP)[number];

export type GameAttributesLeagueWithHistory = Omit<
	GameAttributesLeague,
	AlwaysWrap
> & {
	[T in AlwaysWrap]: GameAttributeWithHistory<GameAttributesLeague[T]>;
};

export type GameAttributes =
	| GameAttributesNonLeague
	| GameAttributesLeagueWithHistory;

export type GameAttributeKey = keyof GameAttributesLeague;

export type GameAttribute<T extends GameAttributeKey> = {
	key: T;
	value: GameAttributesLeagueWithHistory[T];
};

export type League = {
	lid: number;
	name: string;
	tid: number;
	phaseText: string;
	teamName: string;
	teamRegion: string;
	heartbeatID?: string;
	heartbeatTimestamp?: number;
	difficulty?: number;
	starred?: boolean;
	created?: Date;
	lastPlayed?: Date;
	startingSeason?: number;
	season?: number;
	imgURL?: string; // Should contain imgURLSmall if it exists

	// Multiplayer sync (per-league, per-device): the server-timestamp watermark
	// of the last change we've applied from the shared log, so we only catch up
	// on what we missed; the room this league is expected to stay connected to;
	// and a stable id for this device so we skip our own changes across reconnects.
	syncWatermark?: number;
	syncCode?: string;
	syncIsHost?: boolean;
	syncClientId?: string;
	// The room-binding fingerprint: a stable id shared by the room registry doc
	// and every league file legitimately connected to that room. An automatic
	// reconnect is only allowed when they match, so a stale stored session (e.g.
	// a recycled lid) can never silently join a new file to an old room.
	syncLeagueId?: string;
};

export type Locks = {
	drafting: boolean;
	gameSim: boolean;
	newPhase: boolean;
	stopGameSim: boolean;
};

export type LockName = "drafting" | "newPhase" | "gameSim" | "stopGameSim";

export type LogEventType =
	| "achievement"
	| "ageFraud"
	| "award"
	| "changes"
	| "draft"
	| "draftLottery"
	| "error"
	| "freeAgent"
	| "gameAttribute"
	| "gameLost"
	| "gameTied"
	| "gameWon"
	| "hallOfFame"
	| "healed"
	| "healedList"
	| "info"
	| "injured"
	| "injuredList"
	| "madePlayoffs"
	| "newLeague"
	| "newTeam"
	| "playerFeat"
	| "playoffs"
	| "reSigned"
	| "refuseToSign"
	| "release"
	| "retired"
	| "retiredList"
	| "retiredJersey"
	| "screenshot"
	| "sisyphus"
	| "sisyphusTeam"
	| "success"
	| "teamContraction"
	| "teamExpansion"
	| "teamLogo"
	| "teamRelocation"
	| "teamRename"
	| "trade"
	| "tragedy"
	| "upgrade"
	| "luxuryTax"
	| "luxuryTaxDist"
	| "minPayroll";

// https://stackoverflow.com/a/57103940/786644
export type DistributiveOmit<T, K extends keyof T> = T extends any
	? Omit<T, K>
	: never;
export type LogEventSaveOptions = DistributiveOmit<
	EventBBGMWithoutKey,
	"season"
>;

export type OwnerMood = {
	money: number;
	playoffs: number;
	wins: number;
};

export type MessageWithoutKey = {
	mid?: number;
	from: string;
	read: boolean;
	text: string;
	year: number;
	tid?: number;
	subject?: string;
	ownerMoods?: OwnerMood[];
};

export type Message = {
	mid: number;
} & MessageWithoutKey;

export type MenuItemLink = {
	type: "link";
	active?: (pageID?: string, pathname?: string) => boolean;
	league?: true;
	godMode?: true;
	nonLeague?: true;
	commandPalette?: true;
	commandPaletteOnly?: true;
	onClick?: () => undefined | void | false | Promise<undefined | void | false>; // Return false to leave sidebar open
	path?: string | (number | string)[];
	prefix?: ReactNode;
	text:
		| Exclude<ReactNode, null | undefined | number | boolean>
		| {
				side: Exclude<ReactNode, null | undefined | number | boolean>;
				top: Exclude<ReactNode, null | undefined | number | boolean>;
		  };
};

export type MenuItemHeader = {
	type: "header";
	long: string;
	short: string;
	league?: true;
	nonLeague?: true;
	commandPalette?: true;
	commandPaletteOnly?: true;
	children: (MenuItemLink | MenuItemText)[];
};

export type MenuItemText = {
	type: "text";
	text: string;
};

export type MoodComponents = {
	marketSize: number;
	facilities: number;
	teamPerformance: number;
	hype: number;
	loyalty: number;
	trades: number;
	playingTime: number;
	rookieContract: number;
	difficulty: number;
	relatives: number;
	custom?: {
		text: string;
		amount: number;
	}[];
};

export type MoodTrait = "F" | "L" | "$" | "W";

export type Negotiation = {
	pid: number;
	tid: number;
	resigning: boolean;
};

export type Option = {
	id: string;
	label: string;
	url?: string;
	keyboardShortcut?: keyof KeyboardShortcuts["playMenu"];
};

// Which AI site the "Copy → [AI] → Paste" recap buttons open.
export type RecapAIProvider = "claude" | "chatgpt";

export type Options = {
	fullNames?: boolean;
	phaseChangeRedirects: Phase[];
	recapAIProvider?: RecapAIProvider;
	units?: "metric" | "us";
};

type LocalStateUIGameTeam = {
	ovr?: number;
	tid: number;
	playoffs?: {
		seed: number;
		won: number;
		lost: number;
	};
} & (
	| {
			pts: number;
			sPts?: number;
	  }
	| {
			pts?: undefined;
			sPts?: undefined;
	  }
);

type GameAttributesSyncedToUi = (typeof gameAttributesSyncedToUi)[number];
// The simmer's auto-play schedule, shared to the whole room for a live view +
// countdown on every device.
export type SyncedAutoPlay = {
	enabled: boolean;
	// Timestamp (ms) of the next scheduled sim, or undefined if paused/none.
	nextRunAt: number | undefined;
	// Human-readable schedule lines, for a read-only view on other devices.
	rules: string[];
};

// A live-sim broadcast this device is part of, mirrored into UI local state so
// the LiveGame view can drive it. On the broadcaster (isBroadcaster) the view
// heartbeats the cursor; on a follower it locks the page and seeks playback to
// `cursor` so it stays in lockstep with the simmer. Undefined when no broadcast.
export type MpLiveBroadcast = {
	active: boolean;
	gid: number;
	// Display name of whoever is simming (for the follower banner).
	byName: string;
	isBroadcaster: boolean;
	// Which broadcast this is (the simmer's clock, ms). Changes for each new live
	// sim, so a follower can remount for a fresh replay even if it was still
	// sitting on the previous game's final box score.
	startedAt: number;
	// Events the simmer has played so far - a follower seeks its own playback to
	// here. Ignored on the broadcaster (it drives its own playback normally).
	cursor: number;
	paused: boolean;
	gameOver: boolean;
};

// Ready-up state for the header control, for whichever gated stage the league
// is in (draft lottery, draft, re-sign period, free agency). A "step" is a
// pick during the draft, a day during free agency, or a single phase advance;
// a device is "ready through" a step.
export type MpPhaseReady = {
	phase: number;
	// User teams covered by a ready device / total user teams.
	readyTeams: number;
	totalTeams: number;
	// Is THIS device ready for (at least) the next step?
	ready: boolean;
	// The step this device is ready through, if ready.
	myUntilStep: number | undefined;
	nextStep: { number: number; label: string };
	// Draft only: the pick on the clock belongs to a user team (that user
	// drafting is their "ready"; nothing auto-advances).
	onClockUser: boolean;
	// Quick targets ("Until my pick", "Through this round", …).
	waypoints: { step: number; label: string }[];
	// The full "ready through…" list (every remaining pick / free-agency day).
	// Empty for single-step stages.
	options: { step: number; label: string }[];
	// Per-user-team ready status, for the roster popover beside the button.
	teams: { tid: number; name: string; ready: boolean; onClock: boolean }[];
};

export type LocalStateUI = {
	customMenu?: MenuItemHeader;
	email?: string;
	flagOverrides: Record<string, string>;
	gameSimInProgress: boolean;
	games: {
		finals?: boolean;
		forceWin?: number; // Number of iterations - defined means result was forced
		gid: number;
		numPeriods?: number;
		overtimes?: number;
		teams: [LocalStateUIGameTeam, LocalStateUIGameTeam];
	}[];
	fullNames: boolean;
	gold?: boolean;
	keyboardShortcuts: KeyboardShortcutsLocal;
	leagueCreation?: {
		id: string;
		status: string;
	};
	leagueCreationPercent?: {
		id: string;
		percent: number;
	};
	lid?: number;
	liveGameInProgress: boolean;
	// True while connected to a multiplayer sync session - used to hide the
	// multi-team switcher so it feels like single-player.
	mpSyncActive: boolean;
	// Does this device currently hold sim authority (may it advance the league)?
	// Only meaningful while mpSyncActive. Drives the Play-menu / draft locks.
	mpSyncIsHost: boolean;
	// Display name of whoever currently is in charge of simming (undefined = nobody yet).
	mpSyncHostName: string | undefined;
	// True only when this device is connected and has recently proven the cloud
	// room is writable/listenable.
	mpSyncReady: boolean;
	// True when we intend to be synced but aren't connected yet (reconnecting
	// after a refresh, or offline). Simming is paused while this is true.
	mpSyncReconnecting: boolean;
	// The auto-play schedule the SIMMER is running, shared to every device in the
	// room so all users see the same schedule + countdown. Undefined if nobody is
	// auto-playing.
	mpAutoPlay: SyncedAutoPlay | undefined;
	// Live progress while this device is uploading a change to the cloud (chunks
	// done / total). Undefined when idle. Drives the "keep the app open" indicator.
	mpSyncUpload: { done: number; total: number } | undefined;
	// Monotonic counter bumped each time a local change is confirmed uploaded; the
	// header flashes a brief "synced ✓" when it ticks.
	mpSyncUploadOk: number;
	// How many local deltas are durably queued but not yet confirmed in the
	// cloud. Nonzero means "your change is safe locally and will upload
	// automatically" - surfaced in the header so an unuploaded change is never
	// invisible.
	mpPendingUploads: number;
	// Whether the cloud connection is confirmed live (recent verified contact),
	// vs. only nominally connected.
	mpSyncHealthy: boolean;
	// Whether conflict-prone edits (trades, signings, roster/lineup moves) are
	// currently blocked on THIS device because the sim authority is mid-sim or this
	// device is still catching up. Drives the header "simming…" indicator so a
	// blocked action reads as expected, not glitched.
	mpEditsPaused: boolean;
	// Progress while this device is draining a large backlog after being away
	// (entries applied / total to apply). Undefined when caught up or the gap is
	// trivial. Drives the header "catching up …%" progress indicator.
	mpCatchUp: { done: number; total: number } | undefined;
	// The live-sim broadcast this device is part of (simming to the room, or
	// watching the simmer in lockstep). Undefined when none. See MpLiveBroadcast.
	mpLiveBroadcast: MpLiveBroadcast | undefined;
	// Ready-up state (undefined outside a synced gated stage). Drives the header
	// ready control; gated steps only advance once every user team is ready.
	mpPhaseReady: MpPhaseReady | undefined;
	// A league-mate is revealing the draft lottery live; this device replays the
	// reveal in lockstep (revealed = how many picks are shown so far).
	mpLotteryReveal:
		| { season: number; revealed: number; byName: string; startedAt: number }
		| undefined;
	phaseText: string;
	playMenuOptions: Option[];
	popup: boolean;
	recapAIProvider: RecapAIProvider;
	showLeagueTopBar: boolean;
	showNagModal: boolean;
	sidebarOpen: boolean;
	statusText: string;
	units: "metric" | "us";
	username?: string;
	title?: string;
	hideNewWindow: boolean;
	jumpTo: boolean;
	jumpToSeason?: number | "all" | "career";
	dropdownCustomOptions?: Record<string, DropdownOption[]>;
	dropdownCustomURL?: (fields: Record<string, number | string>) => string;
	dropdownView?: string;
	dropdownFields?: {
		[key: string]: number | string;
	};
	moreInfoAbbrev?: string;
	moreInfoSeason?: number;
	moreInfoTid?: number;
	stickyFooterAd: boolean;
	stickyFormButtons: boolean;
} & {
	[Key in Exclude<GameAttributesSyncedToUi, "lid">]: GameAttributesLeague[Key];
};

export type PartialTopMenu = {
	email: string;
	goldCancelled: boolean;
	goldUntil: number;
	mailingList: boolean;
	username: string;
};

export type Phase = -2 | -1 | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8;

export type PhaseReturn = {
	redirect?: {
		url: string;
		text: string;
	};
	updateEvents?: UpdateEvents;
};

export type PlayerContract = {
	amount: number;
	exp: number;
	rookie?: true; // If present, this is a rookie contract. Could be either a rookie scale auto sign, or negotiated.
	rookieResign?: true; // Should only be present during re-signing phase for guys re-signing after rookie contracts, otherwise can't identify if previous contract was a rookie contract cause it's overwritten!
};

export type PlayerFeatWithoutKey = {
	fid?: number;
	pid: number;
	name: string;
	pos: string;
	season: number;
	tid: number;
	oppTid: number;
	playoffs: boolean;
	gid: number;
	stats: any;
	result: "W" | "L" | "T";
	score: string;
	overtimes: number;
	numPeriods: number;
};

export type PlayerFeat = PlayerFeatWithoutKey & {
	fid: number;
};

export type PlayerFiltered = any;

export type PlayerInjury = {
	gamesRemaining: number;
	type: string;
	score?: number;
};

type PlayerSalary = {
	amount: number;
	season: number;
};

// jerseyNumber: string | undefined;
// *Max: [number, number] | null | undefined; - null is for new value, not yet initialized. undefined is for upgraded rows from before this existed
export type PlayerStats = any;

export type RelativeType = "brother" | "father" | "son";

export type Relative = {
	type: RelativeType;
	pid: number;
	name: string;
};

export type MinimalPlayerRatings = {
	ovr: number;
	pot: number;
	fuzz: number;
	pos: string;
	skills: string[];
	season: number;
	ovrs?: any;
	pots?: any;
	injuryIndex?: number;
	hgt: number;
	spd: number;
	endu: number;
	locked?: boolean;
};

export type PlayerAward = {
	season: number;
	type: string;
};

export type PlayerWithoutKey<PlayerRatings = MinimalPlayerRatings> = {
	awards: PlayerAward[];
	born: {
		year: number;
		loc: string;
	};
	college: string;
	contract: PlayerContract & {
		temp?: true; // Used only on import
	};
	customMoodItems?: {
		amount: number;
		text: string;
		tid?: number;
	}[];
	diedYear?: number;
	draft: {
		round: number;
		pick: number;
		tid: number;
		originalTid: number;
		year: number;
		pot: number;
		ovr: number;
		skills: string[];
		dpid?: number;
	};
	face: FaceConfig;
	firstName: string;
	gamesUntilTradable: number;
	hgt: number;
	hof?: 1; // Would rather be boolean, but can't index boolean
	imgURL: string;
	injury: PlayerInjury;
	injuries: {
		season: number;
		games: number;
		type: string;
		ovrDrop?: number;
		potDrop?: number;
	}[];
	jerseyNumber?: string; // Should be undefined only for a player who has never been on a team, or a player signed to his first team before the preseason
	lastName: string;
	moodTraits: MoodTrait[];
	numPlayersTradedAwayNormalized?: Record<number, number>;
	note?: string;
	noteBool?: 1; // Keep in sync with note - for indexing
	numDaysFreeAgent: number;
	pid?: number;
	pos?: string; // Only in players from custom league files
	ptModifier: number;
	ratings: NonEmptyArray<PlayerRatings>;
	real?: boolean;
	relatives: Relative[];
	retiredYear: number;
	rosterOrder: number;
	salaries: PlayerSalary[];
	srID?: string;
	stats: PlayerStats[];
	statsTids: number[];
	tid: number;
	transactions?: (
		| {
				season: number;
				phase: Phase;
				tid: number;
				type: "draft";
				pickNum: number;
		  }
		| {
				season: number;
				phase: Phase;
				tid: number;
				type: "freeAgent";
				eid?: number;
		  }
		| {
				season: number;
				phase: Phase;
				tid: number;
				type: "trade";
				fromTid: number;
				eid?: number;
		  }
		| {
				season: number;
				phase: Phase;
				tid: number;
				type: "godMode";
		  }
		| {
				season: number;
				phase: Phase;
				tid: number;
				type: "import";
		  }
		| {
				season: number;
				phase: Phase;
				tid: number;
				type: "sisyphus";
				fromTid: number;
		  }
	)[]; // Only optional cause I'm worried about upgrades
	value: number;
	valueNoPot: number;
	valueFuzz: number;
	valueNoPotFuzz: number;
	watch?: number;
	weight: number;
	yearsFreeAgent: number;

	// Only for hockey goalies
	numConsecutiveGamesG?: number;

	// Only for baseball pitchers
	pFatigue?: number;
};

export type Player<PlayerRatings = MinimalPlayerRatings> = {
	pid: number;
} & PlayerWithoutKey<PlayerRatings>;

export type PlayerStatType = "per36" | "perGame" | "totals";

export type PlayersPlusOptions = {
	season?: number;
	seasonRange?: [number, number];
	// An arbitrary set of seasons to include (for a selected-rows subtotal). Like
	// seasonRange but non-contiguous; applied to the stat rows that feed
	// careerStats.
	seasons?: number[];
	tid?: number;
	attrs?: string[];
	ratings?: string[];
	stats?: string[];
	playoffs?: boolean;
	regularSeason?: boolean;
	combined?: boolean;
	showNoStats?: boolean;
	showRookies?: boolean;
	showDraftProspectRookieRatings?: boolean;
	showRetired?: boolean;
	fuzz?: boolean;
	oldStats?: boolean;
	numGamesRemaining?: number;
	statType?: PlayerStatType;
	mergeStats?: "none" | "totOnly" | "totAndTeams";
	disableAbbrevsCacheDatabaseAccess?: boolean;
};

export type Race = "asian" | "black" | "brown" | "white";

export type PlayerBioInfo = {
	// This either overwrites a built-in country, or adds a new country
	countries?: Record<
		string,
		{
			// If any of these properties is undefined, fall back to default, then whatever the built-in value is (if it exists)
			first?: Record<string, number>;
			last?: Record<string, number>;
			colleges?: Record<string, number>;
			fractionSkipCollege?: number;
			races?: Record<Race, number>;
			flag?: string;
		}
	>;

	default?: {
		// Applies to all built-in countries, since there is just one global country list to override
		colleges?: Record<string, number>;

		// Applies to all built-in countries except US and Canada, where it's overridden to 0.02 by default
		fractionSkipCollege?: number;

		// Applies to no built-in countries, since they all have built-in defaults
		races?: Record<Race, number>;
	};

	// This specifies which countries (from the built-in database, and supplemented by "data" above)
	frequencies?: Record<string, number>;
};

export type PlayerBioInfoProcessed = {
	countries: Record<
		string,
		{
			first: [string, number][];
			last: [string, number][];
			colleges?: [string, number][];
			fractionSkipCollege?: number;
			races?: [Race, number][];
		}
	>;

	default: {
		colleges: [string, number][];
		fractionSkipCollege: number;
		races: [Race, number][];
	};

	// This specifies which countries (from the built-in database, and supplemented by "data" above)
	frequencies: [string, number][];
};

export type UndoableAction =
	| ({
			type: "sign";
			phase: Phase;
			tid: number;
			eid: number | undefined;
	  } & Pick<
			Player,
			| "numDaysFreeAgent"
			| "numPlayersTradedAwayNormalized"
			| "jerseyNumber"
			| "contract"
			| "salaries"
			| "transactions"
	  >)
	| {
			type: "release";
			tid: number;
	  };

export type Local = {
	autoPlayUntil?: {
		season: number;
		phase: number;

		// Time in milliseconds of the start of auto play
		start: number;
	};
	autoSave: boolean;
	email: string | undefined;
	eightyTwoZeroDraft?: {
		round: number;
		picks: {
			p: Player;
			teamAbbrev: string;
			season: number;
		}[];
		eliteBallKnowerMode: boolean;
		lockTopPlayers: boolean;
		lifelinesUsed: {
			newTeam: boolean;
			newSeason: boolean;
			unlock: boolean;
		};
		currentTeam:
			| ({
					players: {
						p: Player;
						locked: boolean;
					}[];
					season: number;
					seasonInfo?: {
						won: number;
						lost: number;
						tied: number;
						otl: number;
						roundsWonText?: string;
					};
					srID: string;
			  } & Pick<
					Team,
					"abbrev" | "imgURL" | "imgURLSmall" | "name" | "region" | "tid"
			  >)
			| undefined;
	};
	exhibitionGamePlayers?: Record<number, Player>;
	fantasyDraftResults: (Player<any> & {
		prevAbbrev: string | undefined;
		prevTid: number;
	})[];
	goldUntil: number;
	leagueLoaded: boolean;
	liveSimRatingsStatsPopoverPlayers: Record<number, Player> | undefined;
	mailingList: boolean;
	minFractionDiffs:
		| Record<
				number,
				{
					tid: number;
					diff: number;
				}
		  >
		| undefined;
	phaseText: string;
	playerBioInfo?: PlayerBioInfoProcessed;
	playerOvrMean: number;
	playerOvrStd: number;
	playerOvrMeanStdStale: boolean;
	playingUntilEndOfRound: boolean;
	realPlayerActiveSeasons:
		| Record<
				string,
				Record<number, number | undefined> & {
					retiredUntil?: Record<number, number>;
				}
		  >
		| undefined;
	seasonLeaders: SeasonLeaders | undefined;
	statusText: string;
	undoableActions: Record<number, UndoableAction>;
	unviewedSeasonSummary: boolean;
	username: string | undefined;
};

export type PlayoffSeriesTeam = {
	abbrev?: string;
	cid: number;
	colors?: [string, string, string];
	imgURL?: string;
	imgURLSmall?: string;
	pendingPlayIn?: true;
	region?: string;
	regularSeason?: {
		won: number;
		lost: number;
		tied?: number;
		otl?: number;
	};
	seed: number;
	tid: number;
	won: number;

	// pts and sPts are basically only used when there is one game in the series, but they're tracked as the sum of all games for some reason. Beside the one game use case, pts is used in a couple places to identify if a series has started or not. Might be good to improve that some day.
	pts?: number; // undefined means game hasn't happened yet
	sPts?: number; // undefined means game hasn't happened yet or there was no shootout
};

type PlayInMatchup = {
	home: PlayoffSeriesTeam;
	away: PlayoffSeriesTeam;
	gids?: number[];
};

// Each entry is the 2 first round games (7/8 and 9/10) and the 1 game between the loser of the 7/8 game and the winner of the 9/10 game
export type PlayInTournament =
	| [PlayInMatchup, PlayInMatchup]
	| [PlayInMatchup, PlayInMatchup, PlayInMatchup];

export type ByConf = number | false;

export type PlayoffSeries = {
	byConf?: ByConf; // undefined is for upgraded leagues and real players leagues
	currentRound: number;
	season: number;
	series: {
		home: PlayoffSeriesTeam;
		away?: PlayoffSeriesTeam;
		gids?: number[];
	}[][];

	// undefined means no play-in tournament
	playIns?: PlayInTournament[];
};

export type ContractInfo = {
	pid: number;
	firstName: string;
	lastName: string;
	skills: string[];
	pos: string;
	injury: PlayerInjury;
	jerseyNumber: string | undefined;
	amount: number;
	exp: number;
	released: boolean;
	watch: number;
};

export type ReleasedPlayerWithoutKey = {
	rid?: number;
	pid: number;
	tid: number;
	contract: {
		amount: number;
		exp: number;
	};
};

export type ReleasedPlayer = ReleasedPlayerWithoutKey & {
	rid: number;
};

export type ScheduleGameWithoutKey = {
	gid?: number;
	awayTid: number;
	homeTid: number;
	forceWin?: number | "tie"; // either awayTid or homeTid, if defined
	finals?: boolean; // Used for easily checking neutralSite "finals" setting
	day: number; // In the playoffs the values are kind of weird
};

export type ScheduleGame = ScheduleGameWithoutKey & {
	gid: number;
};

export type SortOrder = "asc" | "desc";

export type SortType =
	| "country"
	| "draftPick"
	| "lastTen"
	| "name"
	| "number"
	| "record"
	| "string"
	| "pos";

export type Team = {
	tid: number;
	cid: number;
	did: number;
	region: string;
	name: string;
	abbrev: string;
	imgURL?: string;
	imgURLSmall?: string;
	colors: [string, string, string];
	jersey?: string;
	budget: Record<
		// ticketPrice is in dollars, others are levels
		"ticketPrice" | "scouting" | "coaching" | "health" | "facilities",
		number
	>;
	// initialBudget is for when starting a new league, it can use initialBudget as values for the past 2 seasons when no data exists
	initialBudget: Record<
		"scouting" | "coaching" | "health" | "facilities",
		number
	>;
	strategy: "contending" | "rebuilding";
	depth?:
		| {
				QB: number[];
				RB: number[];
				WR: number[];
				TE: number[];
				OL: number[];
				DL: number[];
				LB: number[];
				CB: number[];
				S: number[];
				K: number[];
				P: number[];
				KR: number[];
				PR: number[];
		  }
		| {
				F: number[];
				D: number[];
				G: number[];
		  }
		| {
				L: number[]; // Lineup
				LP: number[]; // Lineup (no DH)
				D: number[]; // Defense
				DP: number[]; // Defense (no DH)
				P: number[]; // Pitching
		  };
	firstSeasonAfterExpansion?: number;
	srID?: string;

	pop: number;
	stadiumCapacity: number;

	adjustForInflation: boolean;
	disabled: boolean;
	keepRosterSorted: boolean;

	// [regular season, playoffs]
	playThroughInjuries: [number, number];

	// Optional because no upgrade
	autoTicketPrice?: boolean;

	// Optional because no upgrade. Otherwise, would make this empty array by default
	retiredJerseyNumbers?: {
		number: string;
		seasonRetired: number;
		seasonTeamInfo: number;
		pid?: number;
		score?: number;
		text: string;
	}[];

	draftLottery?:
		| {
				type: "cola";
				chances: number;
				optOut?: true;
		  }
		| {
				type: "nba2027";
				restricted1?: true; // True if team got the top pick last year
				restricted5?: 1 | 2; // Number of prior seasons in a row that team got a top 5 pick (2 is max to track, undefined is 0)
		  };

	// Per-team court styling for the basketball live-game graphic. Optional (no
	// upgrade); when absent the court falls back to the team's colors + logo. All
	// fields optional so a partial customization is fine. See common/court.ts.
	court?: CourtStyle;

	// Play-money sportsbook wallet for this team (see worker/core/sportsbook).
	// Purely a fun side feature, completely separate from the real game economy:
	// every preseason each team is granted more virtual "$", which rolls over
	// year to year. Stored on the team record so it syncs to the whole room.
	// Optional (no upgrade); absent means "not initialized yet" (treated as 0).
	sportsbook?: {
		// Current virtual-$ balance.
		balance: number;
		// Open (unsettled) bets. Settled bets move to `history`.
		bets?: SportsbookBet[];
		// Recently settled bets, newest first (bounded).
		history?: SportsbookBet[];
	};
};

// One placed bet in the play-money sportsbook. `stake` is debited when placed;
// on a win the team is credited `stake * decimalOdds` (stake back + profit).
export type SportsbookBet = {
	betID: number;
	// When placed: season + a real-clock timestamp for display/sorting.
	season: number;
	placedAt: number;
	// American odds shown when the bet was placed, and the decimal multiplier
	// used to pay it out (locked in at placement).
	americanOdds: number;
	decimalOdds: number;
	stake: number;
	// Human-readable description of what was bet ("Lakers to win the title",
	// "Warriors -4.5 vs Suns", "LeBron James MVP").
	label: string;
	// What kind of market this is, so settlement knows how to resolve it. For a
	// parlay (see `legs`) this is a copy of the first leg's market and is never
	// used to settle - the legs are.
	market: SportsbookMarket;
	// A parlay: two or more legs combined into one ticket. All legs must win for
	// the bet to win, and the payout compounds (top-level decimalOdds is the
	// product of the legs'). A leg that pushes or voids drops out and the payout
	// is recomputed from the surviving legs at settlement. Absent/empty for an
	// ordinary single (straight) bet.
	legs?: SportsbookBetLeg[];
	// Filled in at settlement. "void" is an administrative refund (stake back,
	// no win/loss either way) for a market that can no longer be resolved at
	// all - e.g. its game's box score was deleted before settlement, or a
	// division/conference/champion couldn't be determined from the league's
	// data. Distinct from "push" (a legitimate tied outcome, like a total
	// landing exactly on the line) even though both refund the stake.
	result?: "won" | "lost" | "push" | "void";
	settledAt?: number;
};

// One leg of a parlay. Its own market + locked-in price; its per-leg outcome is
// filled in at settlement so the UI can show which leg sank the ticket.
export type SportsbookBetLeg = {
	market: SportsbookMarket;
	americanOdds: number;
	decimalOdds: number;
	label: string;
	result?: "won" | "lost" | "push" | "void";
};

// The market a bet belongs to, carrying just enough to settle it later.
export type SportsbookMarket =
	| { type: "gameMoneyline"; gid: number; pickTid: number }
	| { type: "gameSpread"; gid: number; pickTid: number; line: number }
	| { type: "gameTotal"; gid: number; side: "over" | "under"; line: number }
	| { type: "champion"; pickTid: number; season: number }
	| { type: "conf"; pickTid: number; cid: number; season: number }
	| { type: "div"; pickTid: number; did: number; season: number }
	| {
			type: "winTotal";
			pickTid: number;
			side: "over" | "under";
			line: number;
			season: number;
	  }
	| {
			type: "award";
			award: "mvp" | "dpoy" | "roy" | "smoy" | "mip";
			pid: number;
			season: number;
	  }
	| {
			// Makes the All-Star Team (either roster, any role - captain,
			// starter, reserve). See worker/core/allStar.
			type: "allStarTeam";
			pid: number;
			season: number;
	  }
	| {
			// Makes All-League Team `tier` (1 = First Team, 2 = Second, 3 = Third).
			type: "allLeagueTeam";
			pid: number;
			tier: 1 | 2 | 3;
			season: number;
	  }
	| {
			// Makes All-Defensive Team `tier`. Basketball only.
			type: "allDefensiveTeam";
			pid: number;
			tier: 1 | 2 | 3;
			season: number;
	  }
	| {
			// Makes the All-Rookie Team (a single unranked tier).
			type: "allRookieTeam";
			pid: number;
			season: number;
	  }
	| {
			// A single player's stat line in one specific game. "pra"/"pr"/"pa" are
			// the standard combo props (points+rebounds+assists, points+rebounds,
			// points+assists). See worker/core/sportsbook/getGameProps.ts.
			type: "playerProp";
			gid: number;
			pid: number;
			stat: "pts" | "trb" | "ast" | "stl" | "blk" | "tp" | "tov" | "pra" | "pr" | "pa";
			side: "over" | "under";
			line: number;
	  }
	| {
			// Single-outcome "yes" prop: did this player record a double-double /
			// triple-double in this specific game.
			type: "playerMilestone";
			gid: number;
			pid: number;
			milestone: "dd" | "td";
	  }
	| {
			// A team's stat total in one specific game (as opposed to the
			// whole-game combined `gameTotal` market above).
			type: "teamGameProp";
			gid: number;
			tid: number;
			stat: "pts" | "trb" | "ast" | "tp";
			side: "over" | "under";
			line: number;
	  }
	| {
			// Single-outcome "yes" prop on the game itself.
			type: "gameProp";
			gid: number;
			prop: "overtime";
	  };

// How a team's basketball court is drawn in the live-game view. Stored on the
// team record (so it syncs to the whole room) and edited from Manage Teams.
export type CourtStyle = {
	floor?: string; // hardwood tone (hex)
	floorPattern?: "hardwood" | "parquet" | "diagonal" | "chevron" | "solid";
	lines?: string; // court line color (hex)
	paint?: string; // painted key fill (hex); "" / undefined = no paint fill
	apron?: string; // sideline/baseline rail color (hex); default team color 0
	apronText?: string; // rail team-name text color (hex); default team color 1
	logoURL?: string; // center-court logo; default the team's imgURL
	trophyURL?: string; // finals center-court trophy; default the league default
	secondaryLogoURL?: string; // secondary logo shown in each half-court
	sidelineImageURL?: string; // image stretched lengthwise along each sideline
};

export type TeamAttr = keyof Team;

type TeamSeasonPlus = Omit<TeamSeason, "lastTen"> & {
	winp: number;
	revenue: number;
	profit: number;
	salaryPaid: number;
	payroll: number;
	payrollOrSalaryPaid: number;
	lastTen: string;
	streak: string;
	pts: number;
	ptsDefault: number;
	ptsMax: number;
	ptsPct: number;
	avgAge: number | undefined;
	gp: number;
};
export type TeamSeasonAttr = keyof TeamSeasonPlus;

import type {
	TeamStatAttr as TeamStatAttrBaseball,
	TeamStatAttrByPos as TeamStatAttrByPosBaseball,
} from "./types.baseball.ts";
import type { TeamStatAttr as TeamStatAttrBasketball } from "./types.basketball.ts";
import type { TeamStatAttr as TeamStatAttrFootball } from "./types.football.ts";
import type { TeamStatAttr as TeamStatAttrHockey } from "./types.hockey.ts";
import type { TIEBREAKERS } from "./constants.ts";
import type { DropdownOption } from "../ui/hooks/useDropdownOptions.tsx";
import type { LookingForState } from "../ui/views/TradingBlock/useLookingForState.ts";
import type { ALWAYS_WRAP } from "../worker/core/league/loadGameAttributes.ts";
import type {
	KeyboardShortcuts,
	KeyboardShortcutsLocal,
} from "../ui/util/keyboardShortcuts.ts";
import type { gameAttributesSyncedToUi } from "./gameAttributesSyncedToUi.ts";
type TeamStatsPlus = Record<TeamStatAttrBaseball, number> &
	Record<TeamStatAttrByPosBaseball, number[]> &
	Record<TeamStatAttrBasketball, number> &
	Record<TeamStatAttrFootball, number> &
	Record<TeamStatAttrHockey, number> & {
		season: number;
		playoffs: boolean;
	};
export type TeamStatAttr = keyof TeamStatsPlus;

export type TeamFiltered<
	Attrs extends Readonly<TeamAttr[]> | undefined = undefined,
	SeasonAttrs extends Readonly<TeamSeasonAttr[]> | undefined = undefined,
	StatAttrs extends Readonly<TeamStatAttr[]> | undefined = undefined,
	Season extends number | undefined = undefined,
> = (Attrs extends Readonly<TeamAttr[]>
	? Pick<Team, Attrs[number]>
	: Record<string, unknown>) &
	(SeasonAttrs extends Readonly<TeamSeasonAttr[]>
		? {
				seasonAttrs: Season extends number
					? Pick<TeamSeasonPlus, SeasonAttrs[number]>
					: Pick<TeamSeasonPlus, SeasonAttrs[number]>[];
			}
		: Record<string, unknown>) &
	(StatAttrs extends Readonly<TeamStatAttr[]>
		? {
				stats: Season extends number
					? Pick<TeamStatsPlus, StatAttrs[number]> & { playoffs: boolean }
					: (Pick<TeamStatsPlus, StatAttrs[number]> & { playoffs: boolean })[];
			}
		: Record<string, unknown>);

export type TeamBasic = {
	tid: number;
	cid: number;
	did: number;
	region: string;
	name: string;
	abbrev: string;
	pop: number;
	imgURL?: string;
	imgURLSmall?: string;
	colors: [string, string, string];
	jersey?: string;
};

export type TeamStatType = "perGame" | "totals";

export type TeamSeasonWithoutKey = {
	rid?: number;
	tid: number;
	season: number;
	gpHome: number; // Includes playoff games! Used for attendance average
	att: number;
	cash: number;
	won: number;
	lost: number;
	tied: number;
	otl: number;
	wonHome: number;
	lostHome: number;
	tiedHome: number;
	otlHome: number;
	wonAway: number;
	lostAway: number;
	tiedAway: number;
	otlAway: number;
	wonDiv: number;
	lostDiv: number;
	tiedDiv: number;
	otlDiv: number;
	wonConf: number;
	lostConf: number;
	tiedConf: number;
	otlConf: number;
	lastTen: (-1 | 0 | 1 | "OTL")[];
	streak: number;
	playoffRoundsWon: number;
	// -1: didn't make playoffs. 0: lost in first round. ... N: won championship
	hype: number;
	pop: number;
	stadiumCapacity: number;
	revenues: {
		luxuryTaxShare: number;
		merch: number;
		sponsor: number;
		ticket: number;
		nationalTv: number;
		localTv: number;
	};
	expenses: {
		luxuryTax: number;
		minTax: number;
		salary: number;
		coaching: number;
		health: number;
		facilities: number;
		scouting: number;
	};
	// These are cumsums per game, divide by gp for the average
	expenseLevels: {
		coaching: number;
		facilities: number;
		health: number;
		scouting: number;
	};
	payrollEndOfSeason: number;
	ownerMood?: OwnerMood;
	numPlayersTradedAway: number;
	note?: string;
	noteBool?: 1; // Keep in sync with note - for indexing

	// w - clinched play-in tournament
	// x - clinched playoffs
	// y - if byes exist - clinched bye
	// z - clinched #1 seed advantage
	// o - eliminated
	clinchedPlayoffs?: "w" | "x" | "y" | "z" | "o";

	// Value only written here after the end of the season
	avgAge?: number;

	// Start of first game, and end of regular season
	ovrStart?: number;
	ovrEnd?: number;

	// Copied over from Team
	cid: number;
	did: number;
	region: string;
	name: string;
	abbrev: string;
	imgURL?: string;
	imgURLSmall?: string;
	colors: [string, string, string];
	jersey?: string;

	// Only used in historical leagues when realStats="all"
	srID?: string;
};

export type TeamSeason = TeamSeasonWithoutKey & {
	rid: number;
};

// opp stats (except Blk) can be undefined
export type TeamStatsWithoutKey = any;

export type TeamStats = TeamStatsWithoutKey & {
	rid: number;
};

export type TradePickValues = {
	[key: string]: number[] | undefined;
	default: number[];
};
type TradeSummaryTeam = {
	name: string;
	ovrAfter: number;
	ovrBefore: number;
	payrollAfterTrade: number;
	payrollBeforeTrade: number;
	picks: {
		dpid: number;
		season: DraftPickSeason;
		round: number;
		pick: number;
		desc: string;
	}[];
	total: number;
	trade: PlayerFiltered[];
};

export type TradeSummary = {
	teams: [TradeSummaryTeam, TradeSummaryTeam];
	warning: null | string;
	warningAmount?: number;
};

export type TradeTeam = {
	dpids: number[];
	dpidsExcluded: number[];
	pids: number[];
	pidsExcluded: number[];
	tid: number;
	warning?: string | null;
	warningAmount?: number;
};

export type TradeTeams = [TradeTeam, TradeTeam];

export type Trade = {
	rid: 0;
	teams: TradeTeams;
};

export type UpdateEvents = (
	| "account"
	| "allStarDunk"
	| "allStarThree"
	| "firstRun"
	| "g.goatFormula"
	| "g.goatSeasonFormula"
	| "g.tradeProposalsSeed"
	| "gameAttributes"
	| "gameSim"
	| "leagues"
	| "newPhase"
	| "notes"
	| "options"
	| "playerMovement"
	| "playoffs"
	| "savedTrades"
	| "scheduledEvents"
	| "retiredJerseys"
	| "team"
	| "teamFinances"
	| "draftLottery"

	// A follower asking the liveGame view to serve the cached multiplayer
	// broadcast payload (recovery when the navigation carrying it was dropped).
	| "mpLiveBroadcast"

	// This should be used for things that do stuff like "select all players on watch list", not updating the watch property for individual players. crossTabEmit handles that automatically.
	| "watchList"
)[];

export const RealPlayerPhotosSchema = z.record(z.string(), z.string());
export type RealPlayerPhotos = z.infer<typeof RealPlayerPhotosSchema>;

export const IndividualRealTeamInfoSchema = z.object({
	abbrev: z.string().exactOptional(),
	region: z.string().exactOptional(),
	name: z.string().exactOptional(),
	pop: z.number().exactOptional(),
	colors: z.tuple([z.string(), z.string(), z.string()]).exactOptional(),
	imgURL: z.string().exactOptional(),
	imgURLSmall: z.string().exactOptional(),
	jersey: z.string().exactOptional(),
});
export type IndividualRealTeamInfo = z.infer<
	typeof IndividualRealTeamInfoSchema
>;

export const RealTeamInfoSchema = z.record(
	z.string(),
	IndividualRealTeamInfoSchema.extend({
		seasons: z.record(z.number(), IndividualRealTeamInfoSchema).exactOptional(),
	}),
);
export type RealTeamInfo = z.infer<typeof RealTeamInfoSchema>;

export type GetLeagueOptionsReal = {
	type: "real";
	season: number;
	phase: Phase;
	randomDebuts: boolean;
	randomDebutsKeepCurrent: boolean;
	realDraftRatings: "draft" | "rookie";
	realStats: "none" | "lastSeason" | "allActive" | "allActiveHOF" | "all";
	includePlayers: boolean;

	// For callers that need historical team records/players attached to teams
	includeSeasonInfo?: boolean;
	preservePlayerOvrContext?: boolean;
	pidOffset?: number;
};

export type GetLeagueOptions =
	| GetLeagueOptionsReal
	| {
			type: "legends";
			decade:
				| "1950s"
				| "1960s"
				| "1970s"
				| "1980s"
				| "1990s"
				| "2000s"
				| "2010s"
				| "2020s"
				| "all";
	  };

// Would probably be better to have this all at the root, and store one object per (season, t0, t1) but it's awkward to separate t0 and t1 and IndexedDB does not let you make a compound index that includes a multiEntry index, so maybe this is better?
export type HeadToHead = {
	season: number;

	// The keys are team IDs. First should be the lowest of the pair
	regularSeason: Record<
		number,
		Record<
			number,
			{
				won: number;
				lost: number;
				tied: number;
				otl: number;
				pts: number;
				oppPts: number;

				// Needed because we're only storing one record per (tid, tid) pair, and we swap the results when returning the other
				otw: number;
			}
		>
	>;

	playoffs: Record<
		number,
		Record<
			number,
			// This assumes you can only play one playoff series against a given team in a season
			{
				round: number;
				result: "won" | "lost" | undefined;
				won: number;
				lost: number;
				pts: number;
				oppPts: number;
			}
		>
	>;
};

export type GetCopyType = "noCopyCache";

export type SeasonLeaders = {
	season: number;
	age: number;
	regularSeason: Record<string, unknown>;
	playoffs: Record<string, unknown>;
	combined?: Record<string, unknown>;
	ratings: Record<string, unknown>;

	// Optional because we can't compute this for real players leagues without scanning the whole history (slow, tedious) and probably nobody cares
	ratingsFuzz?: Record<string, unknown>;
};

export type SavedTrade = {
	hash: string;
	tid: number;
};

export type SavedTradingBlock = {
	rid: 0;
	dpids: number[];
	pids: number[];
	tid: number;
	offers: {
		dpids: number[];
		pids: number[];
		tid: number;
	}[];
	lookingFor?: LookingForState;
};

export type TeamNum = 0 | 1;
