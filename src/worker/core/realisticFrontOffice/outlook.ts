import {
	COLA_ALPHA,
	COLA_NUM_LOTTERY_PICKS,
	PHASE,
	PLAYER,
} from "../../../common/constants.ts";
import type {
	DraftPick,
	MinimalPlayerRatings,
	Player,
	Team,
} from "../../../common/types.ts";
import { last } from "../../../common/utils.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { marketSalaries, pickSalaries, signingsWith } from "./payroll.ts";
import { pickPlayer } from "./pickValue.ts";
import {
	moodShiftFromTrading,
	shiftRetention,
	type TradedAway,
} from "./tradePenalty.ts";
import {
	BOOM_SHARE,
	projectRatings,
	type RatingsProjection,
} from "./projection.ts";
import getDaysLeftSchedule from "../season/getDaysLeftSchedule.ts";
import { normalCdf, simulateSeason, winProbability } from "./seasonSim.ts";
import { fitLogistic } from "./logistic.ts";
import { clearMarketView, offerSettings, stayChance } from "./offers.ts";
import { buildDeadlineView, type DeadlineView } from "./timing.ts";
import {
	expectedRosterPoints,
	lineSynergy,
	SLOT_MINUTES,
	playerLine,
	typicalSynergy,
	TYPICAL_FORWARD,
	TYPICAL_GUARD,
	valueOverReplacement,
	type StrengthPlayer,
	type ValueRatings,
} from "./strength.ts";
import getRookieContractLength from "../draft/getRookieContractLength.ts";
import { getSlotPickProbsCached } from "../team/pickProbsCache.ts";
import {
	DRAFT_LOTTERY_FACTORS,
	getNumColaLotteryTeams,
	PLAYOFF_FACTORS,
} from "../draft/cola.ts";

// WHERE EVERY TEAM IS HEADED, season by season: each roster projected forward
// with the game's own development, its draft picks turned into the players
// they are expected to become, everything turned into a strength, and every
// season played out to see what that strength is worth. This is the yardstick
// every decision is measured against - a move is good for a team when it
// raises the chances this says it has.

// How many seasons ahead, after this one: long enough to see a prospect
// through his prime and a pick seven drafts away onto the floor.
export const OUTLOOK_YEARS = 10;

// Each season out counts this much of the one before (owner's call: a title
// five years away is worth about 60% of one now).
export const PATIENCE = 0.9;

// A deep run only breaks ties between moves that leave title chances the same.
const PLAYOFF_TIEBREAK = 0.01;

// Projections: enough simulated careers to average out a player's luck.
const PROJECTION_SAMPLES = 24;

// Seasons played out per year of the outlook. The far future is a blur
// anyway, so it gets fewer.
const iterationsFor = (y: number, iterations: number) =>
	y <= 2 ? iterations : Math.ceil(iterations / 3);

// The chance a player retires when the season ends, by the game's own rule
// (player.shouldRetire, basketball): nobody under 34 who is on a roster, and
// after that the older and the less valuable, the likelier - his win shares
// that season, or what his value says they should be if more. Win shares are
// not projected, so this reads them off value alone, and a player who beats
// his value retires a little later than this says. `value` and `ovr` are his
// that season, `maxOvr` the best he has been.
export const retireChance = (
	age: number,
	value: number,
	ovr: number,
	maxOvr: number,
) => {
	const forceRetireAge = g.get("forceRetireAge");
	if (forceRetireAge >= g.get("draftAges")[1] && age >= forceRetireAge) {
		return 1;
	}
	if (age < g.get("minRetireAge") || age <= 33) {
		return 0;
	}
	const pot = 1.1 * value;
	const estWS = (Math.max(pot, 37) - 37) ** 2 / 85.017;
	const logit =
		-1.4134 + (Math.max(age, 18) - 18) ** 2 / 175.19 - 0.971 * estWS;
	const byValue = 1 / (1 + Math.exp(-logit));
	// Aging stars well past their best.
	const fraction = maxOvr > 0 ? ovr / maxOvr : 1;
	const pastBest = fraction < 0.7 ? Math.min(1, 0.2 + (0.7 - fraction)) : 0;
	return byValue + (1 - byValue) * pastBest;
};

// A player's chance of re-signing when his contract runs out is his chance of
// taking his asking price from the team he would be re-signing with, against
// testing the market, as he chooses in the offer market (offers.ts) - so a
// player traded for is priced on how he would feel about his new team. Worked
// out once per outlook and kept here; this is only for a player nobody asked
// about.
const UNKNOWN_RETENTION = 0.5;

export type Curve = { a: number; b: number };

export type TeamOutlook = {
	tid: number;
	// Points per game against an average team, each season.
	strength: number[];
	title: number[];
	playoffs: number[];
};

export type LeagueOutlook = {
	season: number;
	// 1 in the offseason: "this season" is the one about to start.
	offset: number;
	// Games each team has left this season (the whole schedule once it is
	// over).
	remaining: Map<number, number>;
	numTeams: number;
	numRounds: number;
	// The last draft whose picks exist; later drafts' picks are assumed to
	// stay with their teams.
	lastPickSeason: number;
	// Where each team's own pick is expected to land in each season's draft,
	// 0 for the first, from its roster alone - counted smoothly (softPlace).
	// With the lottery (outlookSettings.lottery), where it finishes: the
	// order teams go into the draft in, the worst first.
	draftPlace: Map<number, number[]>;
	// The chance of each pick in a round from each place teams go into the
	// draft in, by round (the game's own lottery odds): with the lottery.
	slotPicks?: readonly (readonly (readonly number[])[])[];
	// In the offseason before this season's lottery: where each team
	// finished, for its pick in it.
	finalPlace?: Map<number, number>;
	// The share of its games each team is expected to win each season, on its
	// roster as it stands, against the league's, drifting back toward the
	// middle seasons out - what its room buys in free agency goes by it
	// (payroll.ts).
	winShare?: Map<number, number[]>;
	// COLA (draftType "cola", with the lottery): the first few picks are
	// drawn among the teams outside the last three playoff rounds, each by
	// the points it has built up, and the rest go by record. A team's own pick
	// is in the draw only while it holds it.
	cola?: {
		points: Map<number, number>;
		meanPoints: number;
		// Points each team adds this season, not yet counted.
		addNow: boolean;
		numLottery: number;
		numPicks: number;
		alpha: number;
		// Where a pick lands from each finish, by its team's share of points.
		matrices: Map<number, number[][]>;
		// Each team's share of points toward each draft's draw, as its path
		// there would leave them (colaPaths): by team, then by draft.
		path?: Map<number, Map<number, number>>;
	};
	// What draftPlace ranks each team by, each season: its strength on its
	// roster alone, and this season, once games are played, the wins it is
	// heading for - with the wins banked and the average strength those come
	// from, and rosters' strengths as priced now (placesAfter).
	placeKeys: Map<number, { points: number[]; keys: number[] }>;
	placeNow?: { won: Map<number, number>; meanNow: number };
	placeBase: Map<string, number[]>;
	// Money each team owes players it has released, and until when.
	deadMoney: Map<number, { amount: number; exp: number }[]>;
	// The first season whose free agency is still to come or under way: from
	// then on, teams refill with their cap room (payroll.ts).
	firstRefillSeason: number;
	// Season lines already worked out, for this outlook's lifetime.
	lines: SeasonLines;
	// The same for picks (pickLineAt); null for a pick worth nothing.
	pickLines: Map<string, PickAt | null>;
	// Each pick's line every season of the outlook, as pickLineAt has it, and
	// each team's picks in the drafts beyond the last that exists.
	pickYears: WeakMap<PickLike, PickYears>;
	futurePicks: Map<number, FuturePickAt[][]>;
	// Chance each player re-signs with each team, by retentionKey.
	retention: Map<string, number>;
	// And his chance of taking its offer at his asking price, which that was
	// worked out from (ensureRetention).
	stays?: Map<string, number>;
	// Each team's roster points each season as it stands, its hype now and
	// its record so far this season: what a move's change in its record is
	// measured against (losingShifts).
	basePoints?: Map<number, number[]>;
	hype?: Map<number, number>;
	records?: Map<number, { won: number; lost: number }>;
	teams: Map<number, TeamOutlook>;
	// Roster points of the average team each season, so a hypothetical roster
	// can be put on the same scale.
	meanPoints: number[];
	// For the arena's version-against-version leagues: how much higher the
	// average team's points are each season at the other injuryRisk, for the
	// teams set to it.
	injuryShift?: number[];
	titleCurves: Curve[];
	playoffCurves: Curve[];
	// For the arena's version-against-version leagues: the same curves at the
	// other measuredUncertainty, for the teams set to it.
	otherCurves?: { title: Curve[]; playoff: Curve[] };
	// Share of this season's games played, 0 in the offseason.
	progress: number;
	// For weighing a deal now against waiting, until the trade deadline passes.
	deadline?: DeadlineView;
	// What each team's record so far is worth to its title chances this
	// season, in points of strength (outlookSettings.recordOdds). Empty when
	// off or out of season.
	recordEdge: Map<number, number>;
	// From the draft lottery to the end of the draft: the class, so the
	// draft's picks are priced by who would go there (classPickAt).
	classBoard?: ClassBoard;
	// The classes of the drafts still to come that are already in the league,
	// by draft (knownClassFor).
	knownClasses?: Map<number, KnownClass>;
	// Before the trade deadline: how much this season's title chances follow
	// playoff strength rather than regular-season strength, 0 to 1 (from the
	// deadline view's fit), and the playoff games a team plays (teamPoints).
	playoffWeight?: number;
	playoffGames?: number;
};

export type ClassBoard = {
	// Bumped whenever it changes, for what is remembered from it.
	version: number;
	season: number;
	// The prospects still there, best first by what each is generally worth.
	prospects: Player[];
	// The draft's picks still to be made, in order, as "round:pick".
	order: string[];
};

// How far a team's true strength may be from our read of it, in points a game,
// outlook year y: surer once games have been played, less sure the further out
// it reaches. `progress` is the share of the season played, for this season.
export const strengthUncertainty = (
	y: number,
	progress: number,
	measured = outlookSettings.measuredUncertainty,
) =>
	measured
		? measuredStrengthUncertainty(y, progress)
		: Math.sqrt((1.5 + 2 * (1 - progress)) ** 2 + (1.6 * y) ** 2);

// As measured in arena leagues (3 leagues x 5 seasons, every team at every
// outlook): how far a team's points a game, the regular season through,
// landed from the outlook's read of it, less the games' own luck in that.
// This season 1.5 points from its start (the first slice's guess was 3.5:
// favourites given 15-30% won 28% of the time, those given over 30% 57%);
// seasons out, a spread growing as a random walk does, MEASURED_PER_SEASON
// in variance a season - 3.6, 4.9 and 6.0 points one, two and three out,
// about the first slice's there.
const MEASURED_NOW = 1.5;
const MEASURED_PER_SEASON = 11;
export const measuredStrengthUncertainty = (y: number, progress: number) =>
	Math.sqrt(
		(MEASURED_NOW * (1 - 0.25 * progress)) ** 2 + MEASURED_PER_SEASON * y,
	);

// The season outlook year y is about.
export const outlookSeason = (
	outlook: Pick<LeagueOutlook, "season" | "offset">,
	y: number,
) => outlook.season + outlook.offset + y;

// Kept for the season they were made in, and dropped once it has passed.
const projections = new Map<string, RatingsProjection>();
let projectionsKey = "";

const seedFor = (pid: number, season: number) =>
	(pid * 2_654_435_761 + season * 40_503) % 2_147_483_647;

// The parts of a player's line that average over simulated careers.
export const lineMeasure = (ratings: MinimalPlayerRatings) => {
	// Where his ratings put him then, not where he plays now.
	const line = playerLine(ratings as unknown as ValueRatings);
	return [line.roleFree, line.offense, line.usage, line.guard, line.forward];
};

export const projectPlayer = (p: Player, season: number) => {
	// Players of another league share pids.
	const forKey = `${g.get("lid")}:${season}`;
	if (forKey !== projectionsKey) {
		projections.clear();
		projectionsKey = forKey;
	}
	const r = last(p.ratings);
	// The fuzz too: a human's read of the player (proposals.ts) is projected
	// from the ratings he sees, which can rate the same.
	const key = `${p.pid}:${season}:${r.season}:${r.ovr}:${r.pot}:${r.fuzz}`;
	let projection = projections.get(key);
	if (!projection) {
		projection = projectRatings({
			ratings: r,
			age: season - p.born.year,
			years: OUTLOOK_YEARS + 1,
			samples: PROJECTION_SAMPLES,
			seed: seedFor(p.pid, season),
			measure: lineMeasure,
		});
		projections.set(key, projection);
	}
	return projection;
};

// One player's line for a season of the outlook: what he plays like, and how
// much of that season we expect to have him.
const computeSeasonLine = (
	p: Player,
	season: number,
	offset: number,
	y: number,
	gamesRemaining: number,
	retention: number,
	boom = false,
): SeasonLine => {
	const index = y + offset;
	if (index === 0) {
		const r = last(p.ratings);
		const out = p.injury.gamesRemaining;
		const availability =
			out <= 0 ? 1 : 1 - Math.min(1, out / Math.max(1, gamesRemaining));
		return {
			...playerLine(r as unknown as ValueRatings, r.pos),
			valueNoPot: p.valueNoPot,
			availability,
			age: season - p.born.year,
		};
	}
	const projection = projectPlayer(p, season);
	const age = season - p.born.year;
	let availability = 1;
	// Every retirement check between now and then: one as each season ends,
	// this season's too unless it is already behind us (the offseason).
	let maxOvr = Math.max(...p.ratings.map((row) => row.ovr));
	for (let c = offset; c < index; c++) {
		const ovr = projection.ovr[c]!;
		maxOvr = Math.max(maxOvr, ovr);
		availability *=
			1 -
			retireChance(age + c, p.value + ovr - projection.ovr[0]!, ovr, maxOvr);
	}
	if (p.contract.exp < season + index) {
		availability *= retention;
	}
	const line = lineFromProjection(projection, index, availability, boom);
	line.age = age + index;
	for (const half of line.halves ?? []) {
		half.age = age + index;
	}
	return line;
};

// A projected player's line `index` seasons on, here this share of it.
const lineFromProjection = (
	projection: RatingsProjection,
	index: number,
	availability: number,
	boom = false,
): SeasonLine => {
	const [roleFree, offense, usage, guard, forward] =
		projection.measures[index]!;
	// One synergy per projected season, shared by all his lines for it, so a
	// lineup is recognised as the same whatever his re-signing odds.
	let synergies = projectedSynergy.get(projection);
	if (!synergies) {
		synergies = [];
		projectedSynergy.set(projection, synergies);
	}
	const synergy = (synergies[index] ??= lineSynergy({
		...(projection.ratings[index] as unknown as ValueRatings),
		ovr: projection.ovr[index]!,
	}));
	const line: SeasonLine = {
		roleFree: roleFree!,
		offense: offense!,
		usage: usage!,
		synergy,
		guard: guard!,
		forward: forward!,
		valueNoPot: projection.ovr[index]!,
		availability,
	};
	// The worse and better half - or with boom, the rest and the best quarter.
	const halves = (boom ? projection.boom : projection.halves)[index];
	const shares = boom ? [1 - BOOM_SHARE, BOOM_SHARE] : [0.5, 0.5];
	if (
		(outlookSettings.upside || teamOutlookSettings.size > 0) &&
		halves &&
		halves.ovr[1] - halves.ovr[0] >= UPSIDE_SPLIT
	) {
		const synergyCache = boom ? projectedBoomSynergy : projectedHalfSynergy;
		let halfSynergies = synergyCache.get(projection);
		if (!halfSynergies) {
			halfSynergies = [];
			synergyCache.set(projection, halfSynergies);
		}
		const pair = (halfSynergies[index] ??= [0, 1].map((k) =>
			lineSynergy({
				...(halves.ratings[k] as unknown as ValueRatings),
				ovr: halves.ovr[k]!,
			}),
		));
		line.halves = [0, 1].map((k) => {
			const [roleFree, offense, usage, guard, forward] = halves.measures[k]!;
			return {
				roleFree: roleFree!,
				offense: offense!,
				usage: usage!,
				synergy: pair[k]!,
				guard: guard!,
				forward: forward!,
				valueNoPot: halves.ovr[k]!,
				availability: availability * shares[k]!,
			};
		});
	}
	return line;
};

// A season of a player's: what he plays like and how likely he is to be
// there - and where his future is uncertain enough, the same for the worse
// and better half of his careers, each half as likely, for the roster to
// play as it would play them (outlookSettings.upside).
export type SeasonLine = StrengthPlayer & {
	availability: number;
	halves?: (StrengthPlayer & { availability: number })[];
};

// The gap between the halves' overalls below which a player is played as one.
const UPSIDE_SPLIT = 4;

// Whether a roster's points count injuries yet to come: by the team's own
// setting when it prices its moves, by the league's (injuryView) where teams
// are set against each other - where they finish, the league's average.
type InjuryMode = "off" | "flat" | "minutes" | "age";
const injuryModeOf = (settings: {
	injuryRisk: boolean;
	injuryByMinutes: boolean;
	injuryByAge: boolean;
}): InjuryMode =>
	!settings.injuryRisk
		? "off"
		: settings.injuryByMinutes
			? "minutes"
			: settings.injuryByAge
				? "age"
				: "flat";
const leagueInjuryMode = () => injuryModeOf(outlookSettings);
const teamInjuryMode = (tid: number) =>
	injuryModeOf({
		injuryRisk: outlookSettingFor(tid, "injuryRisk"),
		injuryByMinutes: outlookSettingFor(tid, "injuryByMinutes"),
		injuryByAge: outlookSettingFor(tid, "injuryByAge"),
	});
// Each player there for INJURY_SHARE fewer of his games at 26, more for
// every year older and fewer for every year younger (injuryByAge).
const byAge = (p: StrengthPlayer) =>
	1 - INJURY_SHARE * INJURY_AGE_RATE ** ((p.age ?? 26) - 26);
// Each place in the pecking order there for its share of games fewer, by its
// minutes (injuryByMinutes).
let byMinutes: number[] | undefined;
const minutesFactors = () =>
	(byMinutes ??= SLOT_MINUTES.map((m) => 1 - INJURY_PER_MINUTES * m));
let injuryView: InjuryMode | undefined;
const injuryFactor = (tid: number) => {
	const mode = injuryView ?? teamInjuryMode(tid);
	return mode === "off"
		? 1
		: mode === "flat"
			? 1 - INJURY_SHARE
			: mode === "age"
				? byAge
				: minutesFactors();
};
const withInjuryView = <T>(view: InjuryMode, work: () => T) => {
	const was = injuryView;
	injuryView = view;
	try {
		return work();
	} finally {
		injuryView = was;
	}
};

// How much higher the average team's points are for this team, at its own
// injuryRisk, than the league's (injuryShift).
const injuryShiftFor = (
	outlook: Pick<LeagueOutlook, "injuryShift">,
	tid: number,
	y: number,
) =>
	outlook.injuryShift && teamInjuryMode(tid) !== leagueInjuryMode()
		? (outlook.injuryShift[y] ?? 0)
		: 0;

// Worse than a minimum free agent.
const belowReplacement = (line: SeasonLine) => valueOverReplacement(line) < 0;

// A player past his contract, as his team would have him: not re-signed if
// worse than a minimum free agent the first season it may let him go
// (wantedOnly), and gone from then on - each half on its own, where his line
// has them: by then the team knows which he turned out to be. Without, the
// team paid a scrub's market salary for years, re-signed at the odds a player
// who would take its offer stays.
type Gone = "none" | "whole" | "worse" | "better";
const goneOf = (line: SeasonLine): Gone => {
	if (!(line.availability > 0)) {
		return "none";
	}
	if (line.halves) {
		const [worse, better] = line.halves.map(belowReplacement);
		return better ? "whole" : worse ? "worse" : "none";
	}
	return belowReplacement(line) ? "whole" : "none";
};
const keptLines = {
	whole: new WeakMap<SeasonLine, SeasonLine>(),
	worse: new WeakMap<SeasonLine, SeasonLine>(),
	better: new WeakMap<SeasonLine, SeasonLine>(),
};
const withoutGone = (line: SeasonLine, gone: Gone) => {
	if (gone === "none" || !(line.availability > 0)) {
		return line;
	}
	let kept = keptLines[gone].get(line);
	if (!kept) {
		if (gone === "whole" || !line.halves) {
			kept =
				gone === "whole"
					? { ...line, availability: 0, halves: undefined }
					: line;
		} else {
			const k = gone === "worse" ? 1 : 0;
			const left = line.halves[k]!;
			kept = {
				...line,
				halves: line.halves.map((half, j) =>
					j === k ? half : { ...half, availability: 0 },
				),
				availability: left.availability,
				valueNoPot: left.valueNoPot,
			};
		}
		keptLines[gone].set(line, kept);
	}
	return kept;
};

// A roster's lines with each split player as his two halves - and, by
// wantedOnly, without those (or the halves) worse than a minimum free
// agent, whose places one takes.
const asPlayed = (tid: number, lines: readonly SeasonLine[]) => {
	const played =
		outlookSettingFor(tid, "upside") && lines.some((line) => line.halves)
			? lines.flatMap((line) => line.halves ?? [line])
			: lines;
	return outlookSettingFor(tid, "wantedOnly") && played.some(belowReplacement)
		? played.filter((line) => !belowReplacement(line))
		: played;
};

const projectedHalfSynergy = new WeakMap<
	RatingsProjection,
	ReturnType<typeof lineSynergy>[][]
>();

const projectedBoomSynergy = new WeakMap<
	RatingsProjection,
	ReturnType<typeof lineSynergy>[][]
>();

const projectedSynergy = new WeakMap<
	RatingsProjection,
	ReturnType<typeof lineSynergy>[]
>();

// A player's lines, by player and re-signing chance, then by what else they
// depend on: his injury, his team's games left, his contract, his overall and
// the value he is ranked by.
type PlayerLines = {
	season: number;
	injury: number;
	gamesRemaining: number;
	exp: number;
	ovr: number;
	valueNoPot: number;
	boom: boolean;
	byYear: (SeasonLine | undefined)[];
};
export type SeasonLines = Map<number, Map<number, PlayerLines[]>>;

const playerLines = (
	lines: SeasonLines,
	p: Player,
	season: number,
	gamesRemaining: number,
	retention: number,
	boom = false,
) => {
	let byRetention = lines.get(p.pid);
	if (!byRetention) {
		byRetention = new Map();
		lines.set(p.pid, byRetention);
	}
	let list = byRetention.get(retention);
	if (!list) {
		list = [];
		byRetention.set(retention, list);
	}
	const injury = p.injury.gamesRemaining;
	const exp = p.contract.exp;
	const ovr = last(p.ratings).ovr;
	const valueNoPot = p.valueNoPot;
	for (const entry of list) {
		if (
			entry.season === season &&
			entry.injury === injury &&
			entry.gamesRemaining === gamesRemaining &&
			entry.exp === exp &&
			entry.ovr === ovr &&
			entry.valueNoPot === valueNoPot &&
			entry.boom === boom
		) {
			return entry;
		}
	}
	const entry = {
		season,
		injury,
		gamesRemaining,
		exp,
		ovr,
		valueNoPot,
		boom,
		byYear: [],
	};
	list.push(entry);
	return entry;
};

export const seasonLine = (
	lines: SeasonLines,
	p: Player,
	season: number,
	offset: number,
	y: number,
	gamesRemaining: number,
	retention: number,
) =>
	(playerLines(lines, p, season, gamesRemaining, retention).byYear[y] ??=
		computeSeasonLine(p, season, offset, y, gamesRemaining, retention));

export const retentionKey = (pid: number, tid: number) => `${pid}:${tid}`;

export const retentionOf = (
	outlook: Pick<LeagueOutlook, "retention">,
	pid: number,
	tid: number,
) => outlook.retention.get(retentionKey(pid, tid)) ?? UNKNOWN_RETENTION;

// Works out, for any of these players not yet asked about, his chance of
// re-signing with this team.
// For the arena, to measure what a behaviour is worth by turning it off.
export const outlookSettings = {
	// A player's chance of re-signing depends on the team: off, every team gets
	// his chance with his current one.
	teamRetention: true,
	// This season's title odds count a team's record, not only its strength:
	// two teams equally good, one with a lucky record and one with an unlucky
	// one, do not have the same chances, and a deal is worth more to the first.
	recordOdds: true,
	// Before the deadline, injured players back for the playoffs count for
	// them (teamPoints).
	playoffInjuries: true,
	// How many drafts out a deal moves the picks of the teams in it
	// (placesAfter); later ones stay where the outlook put them.
	placesAfterDrafts: 99,
	// A pick lands where its team finishes, then the draft lottery moves it
	// (slotPicks); off, by where picks have landed, the lottery in the noise.
	lottery: true,
	// Picks are expected to land back toward the middle of the draft by as
	// much as they have in arena leagues (expectedPlace).
	placeRegression: true,
	// A player's re-sign chance is his chance of still being there after his
	// contract, as measured (KEPT), not of taking his asking price.
	keptCalibration: true,
	// A player whose future is uncertain plays as the worse and the better
	// half of his careers, not as their average (SeasonLine).
	upside: true,
	// And a pick's player as the worse and the better half of what players
	// taken there become (PICK_HALVES), with upside.
	pickUpside: true,
	// Cap room is priced by how good the team is (payroll.ts), not the same
	// for every team.
	roomByRecord: true,
	// A veteran's chance of staying counts how good he is (keptByValue).
	retentionByValue: true,
	// A move's change in the team's record changes its players' mood
	// (losingShifts).
	costsOfLosing: true,
	// Which of the costs of losing count: the record's own part of mood -
	// and, measured and left off, hype's and what the next summer's room buys.
	losingParts: { record: true, hype: false, room: false },
	// Which measurement turns a veteran's chance of taking his team's offer
	// into his chance of staying: 1, KEPT then keptByValue; 2, KEPT_FIT - the
	// better forecast, and the worse one to decide by (see KEPT_FIT).
	retentionFit: 1 as 1 | 2,
	// A pick's player past his rookie deal is let go when his salary buys more
	// as room (letGo). Other players' chance of staying already counts the
	// times their teams let them go (KEPT).
	letGo: true,
	// What payroll past the cap costs a team, in titles a season per cap's
	// worth, and the luxury tax's rate on top past the tax line (payrollCost).
	moneyPerCap: 0.02,
	// And payroll under the cap, past the league's minimum, with what the
	// room is expected to buy; off, money costs only past the cap.
	moneyUnderCap: true,
	// A pick in a draft whose class is already in the league (Draft Scouting)
	// is priced by who would go where it lands (knownClassFor), not by what
	// the place usually yields. Measured and left off: half the teams each
	// way, 5 leagues each way round, 23 titles to 27.
	knownClasses: false,
	// How far a team's true strength may be from the outlook's read of it, as
	// measured against how teams did (measuredStrengthUncertainty) - not the
	// first slice's guess. Half the teams each way, 5 leagues each way round
	// over 5 seasons: 30 titles to 20 (more or as many in every league), 368
	// playoff rounds to 382, 65 seasons under 20 wins to 49 - surer of who
	// can win, contenders push and the rest wait.
	measuredUncertainty: true,
	// COLA: each team's chances toward a draw projected along its own path
	// (colaPaths) - its playoff runs and draw wins cost it chances, as the
	// game charges - not as if every team added the same each season. Half
	// the teams each way, 3 leagues each way round over 5 seasons: as many
	// titles (15 to 15), 241 playoff rounds to 209 (more in every league),
	// and more seasons under 20 wins (29 to 18): a team out of contention
	// builds its chances by staying out of the playoffs, and does.
	colaPath: true,
	// Before the deadline, how much a team expects to learn about itself by
	// then (DeadlineView.revealedMeasured) as its read of a team really moves
	// by then - about 0.7 points a game from the season's start, its own
	// trades aside - not by the first slice's guess, 2.6. Off while it is
	// measured.
	measuredDeadline: false,
	// Games lost to injuries yet to come: everyone is there for this share of
	// games fewer (INJURY_SHARE), as the roster plays without him. Off, only
	// the injuries a team has now count - so a roster leaning on one star was
	// read as better than it played, and a deep one as worse (470
	// team-seasons: -0.10 ± 0.03 points a game for each point the best
	// player's overall stands above the top eight's, +0.12 ± 0.03 for each
	// point the 9th-12th men's do). With them, half that for the star; half
	// the teams each way, 2 leagues each way round over 5 seasons: as many
	// titles (10 to 10), 153 playoff rounds to 147, 12 seasons under 20 wins
	// to 22, and 4 to 6 points more of their games won in the third and
	// fourth seasons. Measured deadline waiting (measuredDeadline) is left
	// off: CPU deals are almost never put off for the deadline.
	injuryRisk: true,
	// Injuries by minutes: each place in the pecking order there for a share
	// of games fewer as it plays (INJURY_PER_MINUTES of its share of the
	// game) - a starter about 10%, the end of the bench almost none - not
	// everyone 8%. Measured and left off: half the teams each way, a league
	// each way round over 5 seasons, 3 titles to 7, 60 playoff rounds to 90,
	// and the read of a team missed by more (2.09 points a game to 1.98).
	injuryByMinutes: false,
	// Injuries by age: the game's injury rate rises 3% a year past 26, and
	// players on a team all season, at 24 minutes a game or more, missed 7.3%
	// of its games at 23 or younger, 8.7% at 24-27, 9.8% at 28-31 and 11.2%
	// at 32 or older - INJURY_AGE_RATE a year. Read with flat injuries, a
	// roster whose top eight were a year older came in 0.19 ± 0.06 points a
	// game worse than read (579 team-seasons). Measured and left off: half
	// the teams each way, 2 leagues each way round over 5 seasons, 8 titles
	// to 12, and 27 seasons under 20 wins to 20.
	injuryByAge: false,
	// A team plays and re-signs only players it wants. One worse than a
	// minimum free agent (valueOverReplacement) never plays ahead of one -
	// the team can always sign one instead - and once his contract is up is
	// not re-signed; and any player past his contract is let go when his
	// salary buys more as room, as a pick's player past his rookie deal
	// (letGo). Without, a projected roster thin in years to come played its
	// worst players, and re-signed every player at his odds of staying: a
	// scrub, or a prospect whose market salary bought more as room, cost his
	// team for a decade - so teams cut him, still paying him, for an older
	// one who would soon retire, and signed and cut players at the end of the
	// bench all season. Measured: half the teams each way, 2 leagues each way
	// round over 5 seasons, 4 times: 23 titles to 17, 0.04 more of its games
	// won by seasons 3-5, and 48 seasons under 20 wins to 63.
	wantedOnly: true,
	// A pick's player, and a young player, as the best quarter of what he may
	// become and the rest (BOOM_SHARE), not as two halves: a star's chance,
	// which a better half averages away. In the owner's league picks 21-25
	// became a 65 or better 11% of the time, 31-35 2% - and a first went for a
	// second, three for a 48. Off while it is measured.
	boom: false,
	// Money short of the league's minimum payroll is free only this season,
	// once games are played: before then, or for seasons ahead, a team that
	// reads itself short nearly always ends up past it (6% of arena teams
	// paid the fine in a season) - it signs and trades for more. Without, a
	// team with room to spare for years bid the most for anyone: a 57 at the
	// maximum for three years. Measured: half the teams each way, 2 leagues
	// each way round over 5 seasons: 14 titles to 6 (7 to 3 in each), games
	// won even; 17 deals near the maximum to 6, all to players 60 or better,
	// and less paid to 50-59s.
	floorNow: true,
};

// Measured in arena leagues: players on a team all season missed 6.5% of its
// games at 12-18 minutes a game, 9.5% at 30-36.
export const INJURY_SHARE = 0.08;
// The same by the share of the game a player is on the floor: 9.5% at 30-36
// minutes a game is 0.14 of 0.69 of the game.
export const INJURY_PER_MINUTES = 0.14;
export const INJURY_AGE_RATE = 1.038;

// For the arena's version-against-version leagues: settings for some teams'
// own rosters, over outlookSettings - the ones a team's pricing alone reads.
export const teamOutlookSettings = new Map<
	number,
	Partial<
		Pick<
			typeof outlookSettings,
			| "upside"
			| "pickUpside"
			| "keptCalibration"
			| "retentionByValue"
			| "retentionFit"
			| "costsOfLosing"
			| "roomByRecord"
			| "letGo"
			| "moneyPerCap"
			| "moneyUnderCap"
			| "knownClasses"
			| "measuredUncertainty"
			| "colaPath"
			| "measuredDeadline"
			| "injuryRisk"
			| "injuryByMinutes"
			| "injuryByAge"
			| "wantedOnly"
			| "boom"
			| "floorNow"
		>
	>
>();
const outlookSettingFor = <
	K extends
		| "upside"
		| "pickUpside"
		| "keptCalibration"
		| "roomByRecord"
		| "retentionByValue"
		| "retentionFit"
		| "costsOfLosing"
		| "letGo"
		| "moneyPerCap"
		| "moneyUnderCap"
		| "knownClasses"
		| "measuredUncertainty"
		| "colaPath"
		| "measuredDeadline"
		| "injuryRisk"
		| "injuryByMinutes"
		| "injuryByAge"
		| "wantedOnly"
		| "boom"
		| "floorNow",
>(
	tid: number,
	key: K,
): (typeof outlookSettings)[K] =>
	teamOutlookSettings.get(tid)?.[key] ?? outlookSettings[key];

// A player's chance of still being with his team the season after his
// contract runs out, by his chance of taking its offer at his asking price
// (stayChance): the team offers what is best for it, often more, and some who
// test the market come back. Measured in 6 arena leagues (1,039 players of
// 50 or better whose contract ran out with the team that had traded for
// them): those given 7% stayed 40% of the time, those given 91% 82% - nearly
// as often by the season the contract ran out as two or more seasons before.
const KEPT: readonly (readonly [number, number])[] = [
	[0, 0.37],
	[0.07, 0.4],
	[0.34, 0.56],
	[0.65, 0.73],
	[0.91, 0.82],
	[1, 0.85],
];

export const keptChance = (stay: number) => tableAt(KEPT, stay);

// KEPT was measured on players of 50 or better that teams had traded for. A
// veteran's team keeps him far more often the better he is: in arena leagues
// under the current market (417 veterans whose contracts ran out, each over
// several runs), those valued under 45 stayed 5% of the time whatever KEPT
// gave them, those of 60 and up 72-93%. Refit on that, by the odds KEPT gives
// and his value: per player, the log-likelihood of what happened went from
// -0.65 to -0.47.
const keptByValue = (kept: number, value: number) => {
	const k = Math.min(0.99, Math.max(0.01, kept));
	const x = -0.101 + 0.858 * Math.log(k / (1 - k)) + 0.812 * ((value - 55) / 5);
	return 1 / (1 + Math.exp(-x));
};

// KEPT_FIT: a veteran's chance of still being with his team the season after
// his contract runs out, by his chance of taking its offer (stay) and his
// value - refit on arena leagues logging what front offices expected as each
// deadline's final week opened against where players were the next season.
// Under the offer market (4 leagues, 1,810 veterans): his team keeps him more
// often the better he is up to a value of about 55, then hardly more - of
// those of 60 and up, those given under 10% to take its offer stayed 21% of
// the time and those given 10-50% 50%, where KEPT and keptByValue had said
// 65-89%. Per player, the log-likelihood of what happened went from -0.581 to
// -0.554.
// logit = a + b stay' + c min(v, 0) + d max(v, 0) + e stay' max(v, 0), with
// stay' the logit of his chance and v = (value - 55) / 5.
// Not used (retentionFit 1): priced by it, half the teams each way, 2
// leagues each way round over 6 seasons, its teams won 2-5% fewer games from
// the fourth season on, 7 titles to 17 and 160 playoff rounds to 200 - they
// sold veterans for picks. Many who leave were let go because they asked for
// more than they were worth to the team; priced as a loss, that undervalues
// the choice of keeping them only when it pays.
const KEPT_FIT = [0.591, 0.256, 0.721, -0.163, 0.128] as const;

const keptFit = (stay: number, value: number) => {
	const [a, b, c, d, e] = KEPT_FIT;
	const s = Math.min(0.999, Math.max(0.001, stay));
	const l = Math.log(s / (1 - s));
	const v = (value - 55) / 5;
	const over = Math.max(0, v);
	return (
		1 /
		(1 + Math.exp(-(a + b * l + c * Math.min(0, v) + d * over + e * l * over)))
	);
};

// His chance of still being with the team after his contract, from his
// chance of taking its offer (KEPT, keptByValue), to the thousandth.
const keptFromStay = (stay: number, p: Player, tid: number) => {
	if (outlookSettingFor(tid, "retentionFit") === 2 && !p.contract.rookie) {
		return Math.round(keptFit(stay, p.value) * 1000) / 1000;
	}
	let kept = outlookSettingFor(tid, "keptCalibration")
		? keptChance(stay)
		: stay;
	// Players finishing rookie deals are kept as KEPT says.
	if (outlookSettingFor(tid, "retentionByValue") && !p.contract.rookie) {
		kept = keptByValue(kept, p.value);
	}
	return Math.round(kept * 1000) / 1000;
};

export const ensureRetention = async (
	outlook: Pick<LeagueOutlook, "retention" | "stays">,
	players: readonly Player[],
	tid: number,
) => {
	for (const p of players) {
		const key = retentionKey(p.pid, tid);
		if (!outlook.retention.has(key)) {
			const asked = !outlookSettings.teamRetention && p.tid >= 0 ? p.tid : tid;
			// His chance of taking his asking price from the team, against
			// testing the market - as he chooses in the offer market - turned
			// into his chance of still being there after (KEPT).
			const stay = await stayChance(p, asked);
			outlook.stays?.set(key, stay);
			outlook.retention.set(key, keptFromStay(stay, p, tid));
		}
	}
};

export type PickLike = Pick<
	DraftPick,
	"originalTid" | "season" | "round" | "pick"
>;

// A pick's player as a roster sees him, and what he is expected to be paid.
type PickAt = {
	line: SeasonLine;
	salary: number;
	// What each of the line's halves is paid, when it has them.
	halfSalaries?: readonly [number, number];
};

// A team's own pick in a draft beyond the last that exists, by its draft.
type FuturePickAt = PickAt & { season: number; round: number };

// A pick's line each season of the outlook (null for none), for the pick as
// it was when they were worked out.
type PickYears = {
	originalTid: number;
	season: PickLike["season"];
	round: number;
	pick: number;
	board: number;
	// Whether the team holding it is the one it came from (COLA's draw).
	own: boolean;
	// Whether it is priced by its draft's class (knownClassFor).
	known: boolean;
	// Whether its COLA draw goes by its team's path (colaPath).
	colaPath: boolean;
	// Whether it is priced with the boom split (outlookSettings.boom).
	boom: boolean;
	byYear: (PickAt | null | undefined)[];
};

const pickYearsOf = (
	outlook: Omit<LeagueOutlook, "teams" | "titleCurves" | "playoffCurves">,
	dp: PickLike,
	holder: number,
) => {
	let entry = outlook.pickYears.get(dp);
	const own = holder === dp.originalTid;
	const known = knownClassFor(outlook, dp, holder) !== undefined;
	const colaPath = colaPathFor(outlook, holder);
	const boom = outlookSettingFor(holder, "boom");
	if (
		!entry ||
		entry.originalTid !== dp.originalTid ||
		entry.season !== dp.season ||
		entry.round !== dp.round ||
		entry.pick !== dp.pick ||
		entry.board !== (outlook.classBoard?.version ?? 0) ||
		entry.own !== own ||
		entry.known !== known ||
		entry.colaPath !== colaPath ||
		entry.boom !== boom
	) {
		entry = {
			originalTid: dp.originalTid,
			season: dp.season,
			round: dp.round,
			pick: dp.pick,
			board: outlook.classBoard?.version ?? 0,
			own,
			known,
			colaPath,
			boom,
			byYear: [],
		};
		outlook.pickYears.set(dp, entry);
	}
	return entry;
};

// The players a team's own picks in drafts beyond the last one that exists
// are expected to have become by outlook year y.
const futurePickLines = (
	outlook: Omit<LeagueOutlook, "teams" | "titleCurves" | "playoffCurves">,
	tid: number,
	y: number,
) => {
	let byYear = outlook.futurePicks.get(tid);
	if (!byYear) {
		byYear = [];
		outlook.futurePicks.set(tid, byYear);
	}
	let lines = byYear[y];
	if (!lines) {
		lines = [];
		const season = outlookSeason(outlook, y);
		for (let d = outlook.lastPickSeason + 1; d < season; d++) {
			for (let round = 1; round <= outlook.numRounds; round++) {
				const at = pickLineAt(
					outlook,
					{ originalTid: tid, season: d, round, pick: 0 },
					y,
				);
				if (at) {
					lines.push({ ...at, season: d, round });
				}
			}
		}
		byYear[y] = lines;
	}
	return lines;
};

// How many places a team finishes from where the outlook expects it to go into
// the draft, typically (root mean square, from where it is expected back
// toward the middle: PLACE_KEPT), by seasons of games still to be played.
// Measured in 4 arena leagues (ARENA_PICK_SURVEY, 58,200 projections) against
// the standings: non-playoff teams by record, then playoff teams. A guess at
// random would be about 9 places off.
const FINISH_SPREAD: readonly (readonly [number, number])[] = [
	[0, 0],
	[0.25, 1.9],
	[0.5, 2.3],
	[0.75, 2.8],
	[1, 4.1],
	[1.5, 4.7],
	[2, 5.6],
	[3, 6.5],
	[4, 7.4],
	[5, 7.8],
];

// Without the lottery: how many places in its round a pick lands from where
// the outlook expects, typically (root mean square), by seasons of games still
// to be played before its draft order is set. Measured in arena leagues
// (ARENA_PICK_SURVEY): every outlook's expected place for every team's own
// first-round pick, against where it landed, the lottery included (35,400 of
// them). A guess at random would be about 12 places off.
const DRAFT_PLACE_SPREAD: readonly (readonly [number, number])[] = [
	[0, 0],
	[0.5, 4.9],
	[0.75, 5],
	[1, 6],
	[1.5, 7.1],
	[2, 7.7],
	[3, 9.2],
	[4, 10.1],
	[5, 11.7],
];

// A table's value this many seasons out, straight between its rows.
const tableAt = (
	table: readonly (readonly [number, number])[],
	seasons: number,
) => {
	const last = table.at(-1)!;
	if (seasons >= last[0]) {
		return last[1];
	}
	let i = 0;
	while (table[i + 1]![0] < seasons) {
		i += 1;
	}
	const [s0, e0] = table[i]!;
	const [s1, e1] = table[i + 1]!;
	return e0 + ((e1 - e0) * (seasons - s0)) / (s1 - s0);
};

const spreadAt = (seasons: number) =>
	tableAt(
		outlookSettings.lottery ? FINISH_SPREAD : DRAFT_PLACE_SPREAD,
		seasons,
	);

// How far a pick may land from where it is expected to, in places in its
// round - none once the draft order is set.
export const draftPlaceSpread = (
	outlook: Pick<LeagueOutlook, "season" | "offset" | "progress">,
	dp: PickLike,
) => {
	if (dp.pick > 0 || typeof dp.season !== "number") {
		return 0;
	}
	const seasons =
		outlook.offset === 0
			? dp.season - outlook.season + 1 - outlook.progress
			: dp.season - outlook.season;
	return spreadAt(Math.max(0, seasons));
};

// Where a pick is expected to land in its round, 0 for the first.
// How much of a team's projected distance from the middle of the draft its
// pick really keeps, by seasons of games still to be played before the draft
// order is set: teams drift back toward the middle - others get better and
// worse too, rosters turn over - more than projecting a roster forward says.
// Measured in 8 arena leagues (ARENA_PICK_SURVEY, 89,400 projections), half
// of them with picks expected back toward the middle like this: the slope of
// where picks landed on where they were projected, both from the middle.
// Leagues differ a lot at long range - 4 seasons out, from 0.05 to 0.80.
// That is with the lottery in the noise; how much of its distance from the
// middle a team's finish keeps (FINISH_KEPT) is far more within a season: the
// lottery, not the standings, moved most picks. Measured against the
// standings in 4 of those leagues; two seasons and more out, less the
// difference the other 4 showed in where picks landed.
const PLACE_KEPT: readonly (readonly [number, number])[] = [
	[0, 1],
	[0.5, 0.84],
	[1, 0.79],
	[1.5, 0.71],
	[2, 0.64],
	[3, 0.55],
	[4, 0.42],
	[5, 0.25],
];
const FINISH_KEPT: readonly (readonly [number, number])[] = [
	[0, 1],
	[0.25, 0.99],
	[0.5, 0.98],
	[0.75, 0.96],
	[1, 0.9],
	[1.5, 0.81],
	[2, 0.74],
	[3, 0.6],
	[4, 0.44],
	[5, 0.27],
];

const placeKept = (seasons: number) =>
	tableAt(outlookSettings.lottery ? FINISH_KEPT : PLACE_KEPT, seasons);

export const expectedPlace = (
	outlook: Pick<
		LeagueOutlook,
		"draftPlace" | "finalPlace" | "season" | "offset" | "numTeams" | "progress"
	>,
	dp: PickLike,
) => {
	if (dp.pick > 0) {
		return dp.pick - 1;
	}
	const final = outlook.finalPlace?.get(dp.originalTid);
	if (final !== undefined && dp.season === outlook.season) {
		return final;
	}
	const middle = (outlook.numTeams - 1) / 2;
	const places = outlook.draftPlace.get(dp.originalTid);
	const yDraft = (dp.season as number) - outlookSeason(outlook, 0);
	const projected = places?.[Math.min(OUTLOOK_YEARS, Math.max(0, yDraft))];
	if (projected === undefined) {
		return middle;
	}
	if (!outlookSettings.placeRegression) {
		return projected;
	}
	const seasons =
		outlook.offset === 0
			? (dp.season as number) - outlook.season + 1 - outlook.progress
			: (dp.season as number) - outlook.season;
	return middle + placeKept(Math.max(0, seasons)) * (projected - middle);
};

// The player one pick is expected to have become by outlook year y, if he has
// been drafted by then: averaged over everywhere in the draft the pick may
// land, and over whether he turns out worth a roster place (his availability;
// pickValue.ts), with what he is expected to be paid. `shift` moves where the
// pick is expected to land, to price how much a misread of that matters.
export const pickLineAt = (
	outlook: Omit<LeagueOutlook, "teams" | "titleCurves" | "playoffCurves">,
	dp: PickLike,
	y: number,
	shift = 0,
	// The team holding it, where that matters (COLA's draw); the team it
	// came from if not given.
	holder?: number,
): PickAt | undefined => {
	const season = outlookSeason(outlook, y);
	if (typeof dp.season !== "number" || dp.season >= season) {
		return undefined;
	}
	// With the boom split, by the holder's own setting.
	const boom =
		holder === undefined
			? outlookSettings.boom
			: outlookSettingFor(holder, "boom");
	const boomKey = boom ? ":B" : "";
	const fromClass = classPickAt(outlook, dp, y, boom);
	if (fromClass !== undefined) {
		return fromClass ?? undefined;
	}
	const known = knownClassFor(outlook, dp, holder);
	const knownKey = known ? `:K${known.season}${boomKey}` : boomKey;
	const n = outlook.numTeams;
	const center = Math.min(
		n - 1,
		Math.max(0, expectedPlace(outlook, dp) + shift),
	);
	const spread = Math.round(10 * draftPlaceSpread(outlook, dp)) / 10;
	const years = season - dp.season;
	// Before the order is set, the lottery moves where the team finishes.
	let lottery = dp.pick > 0 ? undefined : outlook.slotPicks?.[dp.round - 1];
	let lotteryKey = lottery ? ":L" : "";
	if (dp.pick <= 0 && dp.round === 1 && outlook.cola) {
		const ratio =
			holder === undefined || holder === dp.originalTid
				? colaRatio(
						outlook.cola,
						dp,
						outlook.season,
						colaPathFor(outlook, holder ?? dp.originalTid),
					)
				: 0;
		lottery = colaPicks(outlook.cola, outlook.numTeams, ratio);
		lotteryKey = `:C${ratio}`;
	}
	// Priced at the places a tenth apart either side of where it is expected
	// to land, and in between by how far along it is. A cache by the place
	// itself grew with every deal priced; one by the place rounded priced the
	// pick wherever the first deal to round there put it, so what a deal was
	// worth hung on what had been priced before it.
	const scaled = center * PLACE_GRID;
	const k = Math.floor(scaled);
	const t = scaled - k;
	const sumsAt = (at: number) => {
		let bySums = gridSums.get(outlook.pickLines);
		if (!bySums) {
			bySums = new Map();
			gridSums.set(outlook.pickLines, bySums);
		}
		const key = `${dp.round}:${at}:${spread}:${years}${lotteryKey}${knownKey}`;
		let sums = bySums.get(key);
		if (!sums) {
			// Where it lands is the same every season it is priced for.
			let byPlace = landingCache.get(outlook.pickLines);
			if (!byPlace) {
				byPlace = new Map();
				landingCache.set(outlook.pickLines, byPlace);
			}
			const landingKey = `${dp.round}:${at}:${spread}${lotteryKey}`;
			let weights = byPlace.get(landingKey);
			if (!weights) {
				weights = landing(n, at / PLACE_GRID, spread, lottery);
				byPlace.set(landingKey, weights);
			}
			sums = pickSums(outlook, dp.round, weights, years, known, boom);
			bySums.set(key, sums);
		}
		return sums;
	};
	// Remembered by the place itself too, as many as PICK_LINES_KEPT.
	const key = `${dp.round}:${center}:${spread}:${years}${lotteryKey}${knownKey}`;
	let at = outlook.pickLines.get(key);
	if (at === undefined) {
		const lo = sumsAt(k);
		if (t === 0) {
			at = pickFromSums(lo);
		} else {
			const hi = sumsAt(k + 1);
			const sums = new Float64Array(SUMS);
			for (let j = 0; j < SUMS; j++) {
				sums[j] = lo[j]! + t * (hi[j]! - lo[j]!);
			}
			at = pickFromSums(sums);
		}
		if (outlook.pickLines.size >= PICK_LINES_KEPT) {
			outlook.pickLines.clear();
		}
		outlook.pickLines.set(key, at);
	}
	return at ?? undefined;
};

const PICK_LINES_KEPT = 20_000;

// A team's points toward a draft's COLA draw against the average team's,
// rounded to cache on: what it has plus what each season until then adds,
// for the teams outside the last three playoff rounds (those inside lose
// theirs, which is left out).
const colaRatio = (
	cola: NonNullable<LeagueOutlook["cola"]>,
	dp: PickLike,
	season: number,
	path: boolean,
) => {
	if (path) {
		const ratio = cola.path?.get(dp.originalTid)?.get(dp.season as number);
		if (ratio !== undefined) {
			return ratio;
		}
	}
	const added =
		cola.alpha * ((dp.season as number) - season + (cola.addNow ? 1 : 0));
	const mean = cola.meanPoints + added;
	if (mean <= 0) {
		return 1;
	}
	return (
		Math.round((20 * ((cola.points.get(dp.originalTid) ?? 0) + added)) / mean) /
		20
	);
};

// Whether this team prices COLA draws by each team's path (colaPaths).
const colaPathFor = (outlook: Pick<LeagueOutlook, "cola">, tid: number) =>
	outlook.cola?.path !== undefined && outlookSettingFor(tid, "colaPath");

// A team's chance of winning each drawn pick under COLA, by its points against
// the average's in the draw.
const colaWins = (cola: NonNullable<LeagueOutlook["cola"]>, ratio: number) => {
	const others = cola.numLottery - 1;
	const win: number[] = [];
	let left = 1;
	for (let j = 0; j < cola.numPicks; j++) {
		const p = ratio > 0 ? ratio / (ratio + others - j) : 0;
		win.push(left * p);
		left *= 1 - p;
	}
	return { win, left };
};

// Where a pick in the first round lands from each place its team finishes in,
// under COLA, by its team's points against the average's (0 out of the draw):
// its chance of each drawn pick against the other teams in the draw, else
// its place among the rest by record, behind the winners - who come from the
// teams below it about as often as from those above.
const colaPicks = (
	cola: NonNullable<LeagueOutlook["cola"]>,
	n: number,
	ratio: number,
) => {
	let rows = cola.matrices.get(ratio);
	if (rows) {
		return rows;
	}
	const { numLottery, numPicks } = cola;
	const others = numLottery - 1;
	const { win, left } = colaWins(cola, ratio);
	rows = [];
	for (let k = 0; k < n; k++) {
		const row = new Array<number>(n).fill(0);
		if (k >= numLottery || others <= numPicks) {
			row[k] = 1;
		} else {
			for (const [j, p] of win.entries()) {
				row[j]! += p;
			}
			const at = Math.min(n - 1, k + numPicks * (1 - k / others));
			const lo = Math.floor(at);
			row[lo]! += left * (1 - (at - lo));
			if (lo + 1 < n) {
				row[lo + 1]! += left * (at - lo);
			}
		}
		rows.push(row);
	}
	cola.matrices.set(ratio, rows);
	return rows;
};

// A drafted player's chance of re-signing when his rookie deal runs out, for
// pricing a pick by its player (most stay: arena, about 95%).
const ROOKIE_RETENTION = 0.95;

// A prospect as he would be once drafted with this pick: on its rookie deal.
const asDrafted = (p: Player, round: number, slot: number, season: number) => {
	const salary = pickSalaries()({ slot, round, years: 1, ovr: 0 });
	return {
		...p,
		contract: {
			amount: salary,
			exp: season + getRookieContractLength(round),
			rookie: true,
		},
	} as Player;
};

// Whether a pick is priced as one of the class's players (classPickAt).
export const isClassPick = (
	outlook: Pick<LeagueOutlook, "classBoard">,
	dp: PickLike,
) => {
	const board = outlook.classBoard;
	return (
		board !== undefined &&
		dp.season === board.season &&
		dp.pick > 0 &&
		board.order.includes(`${dp.round}:${dp.pick}`)
	);
};

// One of this draft's picks once the class is known and the order set: the
// player who would go there off a board of the class by what each prospect is
// generally worth - undefined for a pick it does not price, null for one with
// nobody left.
const classPickAt = (
	outlook: Omit<LeagueOutlook, "teams" | "titleCurves" | "playoffCurves">,
	dp: PickLike,
	y: number,
	boom = false,
): PickAt | null | undefined => {
	const board = outlook.classBoard;
	if (!board || dp.season !== board.season || dp.pick <= 0) {
		return undefined;
	}
	const k = board.order.indexOf(`${dp.round}:${dp.pick}`);
	if (k < 0) {
		return undefined;
	}
	const key = `class:${board.version}:${dp.round}:${dp.pick}:${y}${boom ? ":B" : ""}`;
	let at = outlook.pickLines.get(key);
	if (at === undefined) {
		const p = board.prospects[k];
		if (!p) {
			at = null;
		} else {
			const slot = (dp.round - 1) * outlook.numTeams + dp.pick - 1;
			const drafted = asDrafted(p, dp.round, slot, board.season);
			const line = computeSeasonLine(
				drafted,
				outlook.season,
				outlook.offset,
				y,
				0,
				ROOKIE_RETENTION,
				boom,
			);
			const years = outlookSeason(outlook, y) - board.season;
			const paid = (half: { availability: number; valueNoPot: number }) =>
				half.availability *
				pickSalaries()({ slot, round: dp.round, years, ovr: half.valueNoPot });
			at = {
				line,
				salary: paid(line),
				halfSalaries: line.halves
					? [paid(line.halves[0]!), paid(line.halves[1]!)]
					: undefined,
			};
		}
		outlook.pickLines.set(key, at);
	}
	return at;
};

// A draft still to come whose class is already in the league - its prospects
// as Draft Scouting shows them, best first by what each is generally worth,
// as the class board ranks them. A pick in it is priced by who would go where
// it lands, like this draft's from the lottery on (classPickAt): a strong
// class's picks are not to be had at the average, nor a weak one's sold at it.
export type KnownClass = {
	season: number;
	prospects: { p: Player; projection: RatingsProjection }[];
	// Per round and seasons after the draft, as slotsFor.
	slots: Map<string, (number[] | undefined)[][]>;
};

// The class a pick is priced by, if any: a draft beyond the one whose order is
// set, with a class in the league, for a team that prices picks so.
const knownClassFor = (
	outlook: Pick<LeagueOutlook, "knownClasses">,
	dp: PickLike,
	holder?: number,
) =>
	typeof dp.season === "number" &&
	outlookSettingFor(holder ?? dp.originalTid, "knownClasses")
		? outlook.knownClasses?.get(dp.season)
		: undefined;

// Prospects in the pool keep their draft-day ratings until they are drafted
// (newPhasePreseason), so each is projected from his draft on - once per
// league, by what he is.
const prospectProjections = new Map<string, RatingsProjection>();
let prospectProjectionsLid: number | undefined;
const projectProspect = (p: Player) => {
	const lid = g.get("lid");
	if (lid !== prospectProjectionsLid || prospectProjections.size > 10_000) {
		prospectProjections.clear();
		prospectProjectionsLid = lid;
	}
	const r = last(p.ratings);
	const draft = p.draft.year;
	const key = `${p.pid}:${draft}:${r.season}:${r.ovr}:${r.pot}:${r.fuzz}`;
	let projection = prospectProjections.get(key);
	if (!projection) {
		projection = projectRatings({
			ratings: r,
			age: draft - p.born.year,
			years: OUTLOOK_YEARS + 1,
			samples: PROJECTION_SAMPLES,
			seed: seedFor(p.pid, draft),
			measure: lineMeasure,
		});
		prospectProjections.set(key, projection);
	}
	return projection;
};

// A prospect's line `years` seasons after his draft (1 his rookie season),
// drafted in this round: there for his rookie deal, then as likely to stay as
// a drafted player is.
const prospectLine = (
	projection: RatingsProjection,
	years: number,
	round: number,
	boom = false,
) =>
	lineFromProjection(
		projection,
		Math.min(projection.ovr.length - 1, years),
		years > getRookieContractLength(round) ? ROOKIE_RETENTION : 1,
		boom,
	);

// The classes of the drafts still to come that are already in the league: from
// the next one whose order is not set to the last with picks.
const buildKnownClasses = async ({
	season,
	numTeams,
	numRounds,
	lastPickSeason,
}: {
	season: number;
	numTeams: number;
	numRounds: number;
	lastPickSeason: number;
}) => {
	const classes = new Map<number, KnownClass>();
	const phase = g.get("phase");
	const first = phase >= PHASE.DRAFT_LOTTERY ? season + 1 : season;
	const prospects = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
	).filter((p) => p.draft.year >= first && p.draft.year <= lastPickSeason);
	const numPicks = numTeams * numRounds;
	for (let draft = first; draft <= lastPickSeason; draft++) {
		// The best by the game's own value are the ones worth projecting.
		const pool = prospects
			.filter((p) => p.draft.year === draft)
			.sort((a, b) => b.value - a.value || a.pid - b.pid)
			.slice(0, 2 * numPicks);
		if (pool.length === 0) {
			continue;
		}
		const ranked = pool
			.map((p) => {
				const projection = projectProspect(p);
				// What he is generally worth, as buildClassBoard: his value over
				// a replacement each season after the draft, each half of his
				// careers as far as it is above one.
				let worth = 0;
				for (let y = 1; y <= OUTLOOK_YEARS + 1; y++) {
					const line = prospectLine(projection, y, 1);
					for (const half of line.halves ?? [line]) {
						worth +=
							0.9 ** (y - 1) *
							half.availability *
							Math.max(0, valueOverReplacement(half));
					}
				}
				return { p, projection, worth };
			})
			.sort((a, b) => b.worth - a.worth || a.p.pid - b.p.pid);
		classes.set(draft, {
			season: draft,
			prospects: ranked.map(({ p, projection }) => ({ p, projection })),
			slots: new Map(),
		});
	}
	return classes;
};

// What the player each place in a round of a known class becomes, `years`
// after its draft, as slotsFor: the prospect who would go there.
const knownSlots = (
	known: KnownClass,
	n: number,
	round: number,
	years: number,
	boom = false,
) => {
	const key = `${n}:${round}:${years}:${g.get("salaryCap")}:${g.get("minContract")}:${g.get("maxContract")}${boom ? ":B" : ""}`;
	let slots = known.slots.get(key);
	if (!slots) {
		const pickSalary = pickSalaries();
		slots = [];
		for (let k = 0; k < n; k++) {
			const slot = (round - 1) * n + k;
			const prospect = known.prospects[slot];
			if (!prospect) {
				slots.push([undefined, undefined, undefined]);
				continue;
			}
			const line = prospectLine(prospect.projection, years, round, boom);
			const rowOf = (x: StrengthPlayer & { availability: number }) => {
				const a = x.availability;
				return a > 0
					? [
							a,
							a * x.roleFree,
							a * x.offense,
							a * x.usage,
							a * x.valueNoPot,
							a * pickSalary({ slot, round, years, ovr: x.valueNoPot }),
						]
					: undefined;
			};
			slots.push([
				rowOf(line),
				line.halves ? rowOf(line.halves[0]!) : undefined,
				line.halves ? rowOf(line.halves[1]!) : undefined,
			]);
		}
		known.slots.set(key, slots);
	}
	return slots;
};

// The draft class, best first by what each prospect is generally worth - his
// value over a replacement each season, for as long as he is expected to be
// around, five seasons out counting about 60% of the next - and the picks
// still to be made. Built from the draft lottery to the end of the draft.
export const buildClassBoard = async (
	outlook: Omit<LeagueOutlook, "teams" | "titleCurves" | "playoffCurves">,
): Promise<ClassBoard | undefined> => {
	const phase = g.get("phase");
	if (phase !== PHASE.DRAFT_LOTTERY && phase !== PHASE.DRAFT) {
		return undefined;
	}
	const season = g.get("season");
	const order = (
		await idb.cache.draftPicks.indexGetAll("draftPicksBySeason", season)
	)
		.filter((dp) => dp.pick > 0)
		.sort((a, b) => a.round - b.round || a.pick - b.pick);
	if (order.length === 0) {
		return undefined;
	}
	const prospects = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
	).filter((p) => p.draft.year === season);
	const worth = new Map<number, number>();
	for (const p of prospects) {
		const drafted = asDrafted(p, 1, outlook.numTeams, season);
		let sum = 0;
		for (let y = 0; y <= OUTLOOK_YEARS; y++) {
			const line = computeSeasonLine(
				drafted,
				outlook.season,
				outlook.offset,
				y,
				0,
				ROOKIE_RETENTION,
			);
			// A season he would be cut is worth nothing, not less: each half
			// of his careers counts as far as it is above a replacement.
			for (const half of line.halves ?? [line]) {
				sum +=
					0.9 ** y *
					half.availability *
					Math.max(0, valueOverReplacement(half));
			}
		}
		worth.set(p.pid, sum);
	}
	return {
		version: ++classBoardVersion,
		season,
		prospects: prospects.sort(
			(a, b) => worth.get(b.pid)! - worth.get(a.pid)! || a.pid - b.pid,
		),
		order: order.map((dp) => `${dp.round}:${dp.pick}`),
	};
};

let classBoardVersion = 0;

// What the player each place in a round becomes, `years` after its draft,
// as pickSums adds them up: for the line, then its worse and its better
// half (if any), the chance he is worth a roster place, then roleFree,
// offense, usage, overall and salary each times that chance - only the
// players worth keeping are paid. The same for every pick and every outlook
// while the league's salaries stand.
const slotPlayers = new Map<string, (number[] | undefined)[][]>();
const slotsFor = (
	n: number,
	numPicks: number,
	round: number,
	years: number,
	boom = false,
) => {
	const key = `${n}:${numPicks}:${round}:${years}:${g.get("salaryCap")}:${g.get("minContract")}:${g.get("maxContract")}${boom ? ":B" : ""}`;
	let slots = slotPlayers.get(key);
	if (!slots) {
		const pickSalary = pickSalaries();
		slots = [];
		for (let k = 0; k < n; k++) {
			const slot = (round - 1) * n + k;
			const p = pickPlayer({ slot, numPicks, years, boom });
			const rowOf = (x: StrengthPlayer & { availability: number }) => {
				const a = x.availability;
				return a > 0
					? [
							a,
							a * x.roleFree,
							a * x.offense,
							a * x.usage,
							a * x.valueNoPot,
							a * pickSalary({ slot, round, years, ovr: x.valueNoPot }),
						]
					: undefined;
			};
			slots.push([
				rowOf(p),
				p.halves ? rowOf(p.halves[0]!) : undefined,
				p.halves ? rowOf(p.halves[1]!) : undefined,
			]);
		}
		slotPlayers.set(key, slots);
	}
	return slots;
};

// The chance of each pick in a round, for a pick expected `center` places in
// (`spread` the error in that), then moved by the lottery - the chance of
// each pick in the round from each place a team finishes.
const landing = (
	n: number,
	center: number,
	spread: number,
	lottery?: readonly (readonly number[])[],
) => {
	// The chance of each place in the round, the ends taking what lies beyond.
	let weights: number[] = [];
	if (spread > 0) {
		for (let k = 0; k < n; k++) {
			const below = k === 0 ? 0 : normalCdf((k - 0.5 - center) / spread);
			const above = k === n - 1 ? 1 : normalCdf((k + 0.5 - center) / spread);
			weights.push(above - below);
		}
	} else {
		for (let k = 0; k < n; k++) {
			weights.push(k === Math.round(center) ? 1 : 0);
		}
	}
	// Then where the lottery takes each finish.
	if (lottery && lottery.length === n) {
		const picks = new Array<number>(n).fill(0);
		for (const [k, w] of weights.entries()) {
			if (w > 0) {
				for (const [pick, p] of lottery[k]!.entries()) {
					picks[pick]! += w * p;
				}
			}
		}
		weights = picks;
	}
	return weights;
};

// Where it lands, and what that adds up to, by pickLines' lifetime: the
// lottery is priced in it.
const landingCache = new WeakMap<object, Map<string, number[]>>();
const gridSums = new WeakMap<object, Map<string, Float64Array>>();

// Picks are priced at this many points to a place, and in between
// (pickLineAt).
const PLACE_GRID = 10;

// What a pick's player adds up to over where it may land: the line, then its
// worse and its better half, each chance, roleFree, offense, usage, overall
// and salary summed by the chance of each pick.
const SUMS = 18;
const pickSums = (
	outlook: Pick<LeagueOutlook, "numTeams" | "numRounds">,
	round: number,
	// The chance of each pick in the round (landing).
	weights: readonly number[],
	years: number,
	// Its draft's class, when it is priced by it.
	known?: KnownClass,
	boom = false,
) => {
	const n = outlook.numTeams;
	const numPicks = outlook.numRounds * n;
	const sums = new Float64Array(SUMS);
	const slots = known
		? knownSlots(known, n, round, years, boom)
		: slotsFor(n, numPicks, round, years, boom);
	for (const [k, w] of weights.entries()) {
		if (w < 1e-4) {
			continue;
		}
		const at = slots[k]!;
		for (let h = 0; h < 3; h++) {
			const row = at[h];
			if (row) {
				for (let j = 0; j < 6; j++) {
					sums[6 * h + j]! += w * row[j]!;
				}
			}
		}
	}
	return sums;
};

// The pick's player, from what he adds up to.
const pickFromSums = (sums: Float64Array): PickAt | null => {
	const lineOf = (at: number): SeasonLine => {
		const kept = sums[at]!;
		const ovr = sums[at + 4]! / kept;
		return {
			roleFree: sums[at + 1]! / kept,
			offense: sums[at + 2]! / kept,
			usage: sums[at + 3]! / kept,
			// Who he is is not known yet: a typical player of his overall.
			synergy: typicalSynergy(ovr),
			guard: TYPICAL_GUARD,
			forward: TYPICAL_FORWARD,
			valueNoPot: ovr,
			availability: kept,
		};
	};
	if (sums[0]! <= 0) {
		return null;
	}
	const line = lineOf(0);
	if (
		sums[6]! > 0 &&
		sums[12]! > 0 &&
		sums[16]! / sums[12]! - sums[10]! / sums[6]! >= UPSIDE_SPLIT
	) {
		line.halves = [lineOf(6), lineOf(12)];
		return {
			line,
			salary: sums[5]!,
			halfSalaries: [sums[11]!, sums[17]!],
		};
	}
	return { line, salary: sums[5]! };
};

// What a player is expected to be paid in a season of the outlook: his
// contract while it runs, then what he would re-sign for - each only as far
// as he is expected to be around.
const salaryIn = (
	p: Player,
	season: number,
	index: number,
	line: { valueNoPot: number; availability: number },
	marketSalary: (value: number) => number,
) => {
	if (index === 0) {
		return p.contract.amount;
	}
	return (
		line.availability *
		(p.contract.exp >= season
			? p.contract.amount
			: marketSalary(line.valueNoPot))
	);
};

// Roster points of these players and picks, each season of the outlook, for a
// team - any team, real or hypothetical (a trade, a signing).
// `trading`, for a deal not yet made: how many players the team trades away
// in it, and its count so far, so the trade penalty's cost is priced.
export const teamPoints = (
	outlook: Omit<LeagueOutlook, "teams" | "titleCurves" | "playoffCurves">,
	tid: number,
	players: readonly Player[],
	picks: readonly PickLike[],
	trading?: { tradedAway: TradedAway; extra: number },
	// For explaining a deal: the cap room each season, as a share of the cap,
	// and the free agents it is expected to buy.
	refill?: (
		y: number,
		room: number,
		signings: readonly { valueNoPot: number; availability: number }[],
	) => void,
	// For the arena's checks: everyone the team is expected to have each
	// season, with the chance he is there.
	roster?: (
		y: number,
		lines: readonly (StrengthPlayer & { availability: number })[],
	) => void,
	// Whom the team lets go after their rookie deals, as decided on another
	// roster of its (letGo) - or false for nobody, for ranking rosters alone.
	known?: LetGoDecisions | false,
	// What a season's points are worth to the franchise, for deciding whom to
	// let go by it; points themselves without.
	worth?: (y: number, points: number) => number,
	// What a season's payroll costs the franchise at this much room, in the
	// same units as `worth` (payrollCost).
	spend?: (
		y: number,
		room: number,
		signings: readonly { valueNoPot: number; availability: number }[],
	) => number,
	// What a change in the team's record does to the players whose contracts
	// run out each season, and to the free agents its room buys (losingShifts).
	losing?: LosingShifts,
) => {
	const retentionFor = (p: Player) => {
		let r = retentionOf(outlook, p.pid, tid);
		if (losing) {
			const e = p.contract.exp - outlookSeason(outlook, 0);
			const mood = e >= 0 ? (losing.mood[e] ?? 0) : 0;
			const stay = outlook.stays?.get(retentionKey(p.pid, tid));
			if (mood !== 0 && stay !== undefined) {
				const s = Math.min(1 - 1e-6, Math.max(1e-6, stay));
				const logit = Math.log(s / (1 - s)) + losing.slope * mood;
				r = keptFromStay(1 / (1 + Math.exp(-logit)), p, tid);
			}
		}
		return trading
			? Math.round(
					shiftRetention(
						r,
						moodShiftFromTrading(
							trading.tradedAway,
							trading.extra,
							p.contract.exp - outlook.season,
						),
					) * 1000,
				) / 1000
			: r;
	};
	const gamesRemaining = outlook.remaining.get(tid) ?? 0;
	const retention = players.map(retentionFor);
	const boom = outlookSettingFor(tid, "boom");
	const byPlayer = players.map((p, i) =>
		playerLines(
			outlook.lines,
			p,
			outlook.season,
			gamesRemaining,
			retention[i]!,
			boom,
		),
	);
	const byPick = picks.map((dp) => pickYearsOf(outlook, dp, tid));
	// League settings, looked up once: nothing here can change them.
	let salaryCap: number | undefined;
	let marketSalary: ((value: number) => number) | undefined;
	const wanted = outlookSettingFor(tid, "wantedOnly");
	// Whom the team lets go, worse than a minimum player, the first season it
	// may (withoutGone).
	const gone: (Gone | undefined)[] = [];
	// Every season's roster, before the team lets anyone go.
	const seasons: SeasonState[] = [];
	for (let y = 0; y <= OUTLOOK_YEARS; y++) {
		const season = outlookSeason(outlook, y);
		const lines = players.map((p, i) => {
			const line = (byPlayer[i]!.byYear[y] ??= computeSeasonLine(
				p,
				outlook.season,
				outlook.offset,
				y,
				gamesRemaining,
				retention[i]!,
				boom,
			));
			if (!wanted || p.contract.exp >= season) {
				return line;
			}
			gone[i] ??= goneOf(line);
			return withoutGone(line, gone[i]);
		});
		// Its picks' players past their rookie deals, whom the team may let go
		// rather than pay that season - each half on its own where a line has
		// them: by then the team knows which he turned out to be.
		const options: LetGoOption[] = [];
		// The players its picks are expected to have become by then, and what
		// they are paid.
		let salaries = 0;
		const pickUpside =
			outlookSettingFor(tid, "upside") && outlookSettingFor(tid, "pickUpside");
		const addPick = (
			at: PickAt,
			draft: unknown,
			round: number,
			key: string,
		) => {
			const halfSalaries =
				at.line.halves && pickUpside ? at.halfSalaries : undefined;
			const salary = halfSalaries
				? halfSalaries[0] + halfSalaries[1]
				: at.salary;
			if (typeof draft === "number") {
				// Cut for nothing straight after the draft, if he is not worth
				// his place - not yet knowing which half he will turn out.
				options.push({
					i: lines.length,
					key: `${key}:drafted`,
					pick: key,
					salary,
				});
			}
			if (
				typeof draft === "number" &&
				season - draft > getRookieContractLength(round)
			) {
				if (halfSalaries) {
					for (const half of [0, 1] as const) {
						options.push({
							i: lines.length,
							half,
							key: `${key}:${half}`,
							pick: key,
							salary: halfSalaries[half],
						});
					}
				} else {
					options.push({ i: lines.length, key, pick: key, salary });
				}
			}
			lines.push(
				at.line.halves && !pickUpside
					? { ...at.line, halves: undefined }
					: at.line,
			);
			salaries += salary;
		};
		for (let i = 0; i < picks.length; i++) {
			const byYear = byPick[i]!.byYear;
			let at = byYear[y];
			if (at === undefined) {
				at = pickLineAt(outlook, picks[i]!, y, 0, tid) ?? null;
				byYear[y] = at;
			}
			if (at) {
				const dp = picks[i]!;
				addPick(
					at,
					dp.season,
					dp.round,
					`${dp.originalTid}:${dp.season}:${dp.round}`,
				);
			}
		}
		for (const at of futurePickLines(outlook, tid, y)) {
			addPick(at, at.season, at.round, `future:${at.season}:${at.round}`);
		}
		const state: SeasonState = { y, lines, options };
		seasons.push(state);
		if (season >= outlook.firstRefillSeason) {
			salaryCap ??= g.get("salaryCap");
			marketSalary ??= marketSalaries();
			let committed = salaries;
			for (let i = 0; i < players.length; i++) {
				const salary = salaryIn(
					players[i]!,
					season,
					y + outlook.offset,
					lines[i]!,
					marketSalary,
				);
				committed += salary;
				// A player past his contract, whom the team may let go rather
				// than re-sign, as a pick's player past his rookie deal
				// (wantedOnly) - each half on its own, paid as he is.
				if (
					wanted &&
					players[i]!.contract.exp < season &&
					salary > 0 &&
					lines[i]!.availability >= VETERAN_OPTION_CHANCE
				) {
					const line = lines[i]!;
					const key = `vet:${players[i]!.pid}`;
					if (line.halves && outlookSettingFor(tid, "upside")) {
						const paid = line.halves.map((half) =>
							half.availability > 0 ? marketSalary!(half.valueNoPot) : 0,
						);
						const total = paid[0]! + paid[1]!;
						for (const half of [0, 1] as const) {
							if (paid[half]! > 0) {
								options.push({
									i,
									half,
									key: `${key}:${half}`,
									pick: key,
									salary: (salary * paid[half]!) / total,
								});
							}
						}
					} else {
						options.push({ i, key, pick: key, salary });
					}
				}
			}
			for (const row of outlook.deadMoney.get(tid) ?? []) {
				if (row.exp >= season) {
					committed += row.amount;
				}
			}
			const duringFreeAgency =
				y === 0 && outlook.offset === 1 && g.get("phase") === PHASE.FREE_AGENCY;
			let winp = outlookSettingFor(tid, "roomByRecord")
				? outlook.winShare?.get(tid)?.[y]
				: undefined;
			if (winp !== undefined && losing) {
				winp = Math.min(1, Math.max(0, winp + (losing.winp[y] ?? 0)));
			}
			state.room = salaryCap - committed;
			state.signingsAt = (room) => signingsWith(room, duringFreeAgency, winp);
		}
	}
	if (known !== false && outlookSettingFor(tid, "letGo")) {
		letGo(
			seasons,
			(all) => expectedRosterPoints(asPlayed(tid, all), injuryFactor(tid)),
			known,
			worth,
			spend,
		);
	}
	const out: number[] = [];
	for (let y = 0; y <= OUTLOOK_YEARS; y++) {
		const { lines, room, signingsAt } = seasons[y]!;
		if (room !== undefined) {
			const signings = signingsAt!(room);
			lines.push(...signings);
			refill?.(y, room / salaryCap!, signings);
		}
		roster?.(y, lines);
		let points =
			seasons[y]!.points ??
			expectedRosterPoints(asPlayed(tid, lines), injuryFactor(tid));
		// This season's playoffs count with everyone back who will be by then
		// (playoffWeight): a star out for the rest of the regular season is
		// still worth his playoffs.
		const weight = outlook.playoffWeight ?? 0;
		if (y === 0 && weight > 0) {
			let playoffLines: typeof lines | undefined;
			for (let i = 0; i < players.length; i++) {
				const out = players[i]!.injury.gamesRemaining;
				if (out <= 0) {
					continue;
				}
				const back =
					1 -
					Math.min(
						1,
						Math.max(0, out - gamesRemaining) /
							Math.max(1, outlook.playoffGames ?? 1),
					);
				if (back !== lines[i]!.availability) {
					playoffLines ??= [...lines];
					playoffLines[i] = { ...lines[i]!, availability: back };
				}
			}
			if (playoffLines) {
				points +=
					weight *
					(expectedRosterPoints(
						asPlayed(tid, playoffLines),
						injuryFactor(tid),
					) -
						points);
			}
		}
		out.push(points);
	}
	return out;
};

type LetGoOption = {
	// His line's place in the roster's lines, and which half, if just one.
	i: number;
	half?: 0 | 1;
	key: string;
	// The pick he was taken with: once he is cut, the later decisions on him
	// are moot.
	pick: string;
	salary: number;
};

// A veteran less likely than this to be there past his contract is not
// worth deciding on (wantedOnly): whichever way, he moves his team little,
// and trying every one cost more than twice as much as the rest of a
// roster's outlook.
const VETERAN_OPTION_CHANCE = 0.2;

// A player past his contract, not a pick's player (wantedOnly).
const isVeteran = (o: LetGoOption) => o.pick.startsWith("vet:");

const optionLine = (line: SeasonLine, o: LetGoOption) =>
	o.half === undefined ? line : line.halves![o.half]!;

// The line without the player, or without that half of him.
const withoutOption = (line: SeasonLine, o: LetGoOption): SeasonLine =>
	o.half === undefined
		? { ...line, availability: 0, halves: undefined }
		: {
				...line,
				halves: line.halves!.map((half, k) =>
					k === o.half ? { ...half, availability: 0 } : half,
				),
			};

// Whom a team lets go after their rookie deals, by pick (and half): decided on
// its roster as it stands (`record`), then the same in a deal's - unless the
// deal brings the pick in or moves the team's room that season. Deciding
// afresh on every roster a deal might leave it with cost more than pricing
// the deal.
export type LetGoDecisions = {
	// Whether he goes, and the team's room that season before it decided.
	decisions: Map<string, { gone: boolean; room: number }>;
	record: boolean;
};

// A deal that changes a team's room the season it decides on a player by
// more than this share of the cap has it decide again.
const LET_GO_ROOM_TOLERANCE = 0.1;

// One season of a team's outlook, as teamPoints works it out: its roster, and
// from the first season it refills, its cap room and what that room buys.
type SeasonState = {
	y: number;
	lines: SeasonLine[];
	// Its picks' players it may let go that season.
	options: LetGoOption[];
	room?: number;
	signingsAt?: (room: number) => readonly SeasonLine[];
	// Its points once the room is filled, when worked out (letGo).
	points?: number;
};

// A roster's points alone, for ranking where teams finish: no picks, and
// nobody let go.
const rosterOnlyPoints = (
	outlook: Parameters<typeof teamPoints>[0],
	tid: number,
	players: readonly Player[],
) =>
	withInjuryView(leagueInjuryMode(), () =>
		teamPoints(
			outlook,
			tid,
			players,
			[],
			undefined,
			undefined,
			undefined,
			false,
		),
	);

// A team keeps a drafted player past his rookie deal only while it is better
// with him than with what his salary buys as cap room, over the seasons ahead
// - decided once, the first season it may let him go, as it is when the time
// comes (freeAgency.ts). A player let go is out of every season's `lines`
// from then on, and his salary is room.
// Without this, every pick's player was kept at what he asks: a mediocre
// one's second contract ate room that buys better, so a bad team was worse
// off for a second first-round pick.
const letGo = (
	seasons: readonly SeasonState[],
	points: (lines: readonly SeasonLine[]) => number,
	known?: LetGoDecisions,
	worth: (y: number, points: number) => number = (_, x) => x,
	spend?: (
		y: number,
		room: number,
		signings: readonly { valueNoPot: number; availability: number }[],
	) => number,
) => {
	const minContract = g.get("minContract");
	const tolerance = LET_GO_ROOM_TOLERANCE * g.get("salaryCap");
	const pointsOf = (s: SeasonState) =>
		(s.points ??= points([...s.lines, ...s.signingsAt!(s.room!)]));
	// The same player in each season from y on.
	const aheadOf = (y: number, key: string) => {
		const ahead: [SeasonState, LetGoOption][] = [];
		for (const s of seasons.slice(y)) {
			const at = s.options.find((x) => x.key === key);
			if (at) {
				ahead.push([s, at]);
			}
		}
		return ahead;
	};
	// Picks whose players were cut.
	const cut = new Set<string>();
	const goFrom = (
		ahead: readonly [SeasonState, LetGoOption][],
		freed: readonly number[] = [],
	) => {
		if (ahead[0]?.[1].half === undefined && ahead[0]) {
			cut.add(ahead[0][1].pick);
		}
		for (const [k, [s, at]] of ahead.entries()) {
			s.lines[at.i] = withoutOption(s.lines[at.i]!, at);
			s.room! += at.salary;
			s.points = freed[k];
		}
	};
	const decided = new Set<string>();
	for (const [y, now] of seasons.entries()) {
		if (now.room === undefined || now.options.length === 0) {
			continue;
		}
		const room = now.room;
		const fresh: LetGoOption[] = [];
		for (const o of now.options) {
			if (decided.has(o.key) || cut.has(o.pick)) {
				continue;
			}
			decided.add(o.key);
			const before = known?.record ? undefined : known?.decisions.get(o.key);
			if (before && Math.abs(before.room - room) <= tolerance) {
				if (before.gone) {
					goFrom(aheadOf(y, o.key));
				}
			} else {
				fresh.push(o);
				if (known?.record) {
					known.decisions.set(o.key, { gone: false, room });
				}
			}
		}
		const best = (room: number) =>
			Math.max(...now.signingsAt!(room).map((x) => x.valueNoPot));
		// What letting him go from this season on is worth, and the team's
		// points each season without him.
		// Over this many seasons from now (for ranking them): the seasons
		// after, where he is gone too, are worked out again.
		const goneGain = (o: LetGoOption, seasonsAhead = Infinity) => {
			const ahead = aheadOf(y, o.key);
			let gain = 0;
			const freed: number[] = [];
			// Every season ahead, by what it is worth to the franchise: on the
			// first few alone, it let young players go who were about to get
			// better. One worth more than his room the first season only gets
			// better - but a drafted player is not decided by that: what he
			// costs is room later on, nor a veteran, paid what he is worth.
			for (const [s, at] of ahead.slice(0, seasonsAhead)) {
				const kept = pointsOf(s);
				const line = s.lines[at.i]!;
				s.lines[at.i] = withoutOption(line, at);
				const after = points([
					...s.lines,
					...s.signingsAt!(s.room! + at.salary),
				]);
				s.lines[at.i] = line;
				freed.push(after);
				if (
					freed.length === 1 &&
					after <= kept &&
					!o.key.endsWith(":drafted") &&
					!isVeteran(o)
				) {
					break;
				}
				gain += worth(s.y, after) - worth(s.y, kept);
				if (spend) {
					gain +=
						spend(s.y, s.room!, s.signingsAt!(s.room!)) -
						spend(s.y, s.room! + at.salary, s.signingsAt!(s.room! + at.salary));
				}
			}
			return { ahead, gain, freed };
		};
		// Tries them in turn, letting go each the team is better without,
		// until two in a row are kept: the rest are better for the money.
		const tryInTurn = (options: readonly LetGoOption[]) => {
			let keptInARow = 0;
			for (const o of options) {
				if (cut.has(o.pick)) {
					continue;
				}
				const { ahead, gain, freed } = goneGain(o);
				if (gain > 0) {
					if (known?.record) {
						known.decisions.set(o.key, { gone: true, room });
					}
					goFrom(ahead, freed);
					keptInARow = 0;
				} else {
					keptInARow += 1;
					if (keptInARow === 2) {
						break;
					}
				}
			}
		};
		// Picks' players: those his salary would buy the most better than
		// him go first; room that buys nobody better is no reason to let him
		// go.
		tryInTurn(
			fresh
				.filter(
					(o) =>
						!isVeteran(o) &&
						(o.salary >= minContract || o.key.endsWith(":drafted")),
				)
				.map((o) => ({
					o,
					surplus:
						optionLine(now.lines[o.i]!, o).valueNoPot -
						best(now.room! + o.salary),
				}))
				.filter((x) => x.surplus < 0)
				.sort((a, b) => a.surplus - b.surplus || a.o.i - b.o.i)
				.map((x) => x.o),
		);
		// Veterans (wantedOnly): by what letting him go is worth the first
		// season, best first - his market salary may be worth more as room
		// than any one player it buys. Decided on the first few seasons
		// alone, a team was better off without players it kept.
		tryInTurn(
			fresh
				.filter((o) => isVeteran(o) && !cut.has(o.pick))
				.map((o) => ({ o, first: goneGain(o, 1).gain }))
				.sort((a, b) => b.first - a.first || a.o.i - b.o.i)
				.map((x) => x.o),
		);
	}
};

// What a team's draft place is ranked by each season, from its strength on
// its roster alone (draftPlace).
const placeKeysOf = (
	outlook: Pick<LeagueOutlook, "placeNow" | "remaining">,
	tid: number,
	points: readonly number[],
) =>
	points.map((x, y) =>
		y === 0 && outlook.placeNow
			? outlook.placeNow.won.get(tid)! +
				(outlook.remaining.get(tid) ?? 0) *
					winProbability(x - outlook.placeNow.meanNow)
			: x,
	);

// Where a team ranked by this key lands among the others (by `changed` keys
// where given, else as the outlook ranked them), 0 for the worst - counted
// smoothly over a win or half a point either way, so a deal that barely
// changes a team cannot jump its pick a whole place and invent a gain.
const PLACE_SOFTNESS_WINS = 1.5;
const PLACE_SOFTNESS_POINTS = 0.5;
const softPlace = (
	outlook: Pick<LeagueOutlook, "placeKeys" | "placeNow">,
	tid: number,
	key: number,
	y: number,
	changed: ReadonlyMap<number, readonly number[]>,
) => {
	const softness =
		y === 0 && outlook.placeNow ? PLACE_SOFTNESS_WINS : PLACE_SOFTNESS_POINTS;
	let place = 0;
	for (const [other, was] of outlook.placeKeys) {
		if (other !== tid) {
			const otherKey = (changed.get(other) ?? was.keys)[y]!;
			place += 1 / (1 + Math.exp(-(key - otherKey) / softness));
		}
	}
	return place;
};

// A team's strength on a roster alone, as priced now, remembered for its
// current roster.
const rosterOnlyNow = (
	outlook: LeagueOutlook,
	tid: number,
	players: readonly Player[],
) => {
	const key = `${tid}:${players
		.map((p) => `${p.pid}.${p.injury.gamesRemaining}`)
		.sort()
		.join()}`;
	let points = outlook.placeBase.get(key);
	if (!points) {
		points = rosterOnlyPoints(outlook, tid, players);
		outlook.placeBase.set(key, points);
	}
	return points;
};

// The outlook with these teams' rosters changed, as a trade changes them:
// their own picks land where the new rosters put them, against everyone
// else's as they stand. Picks are otherwise priced where the outlook was
// built, so a team giving up its best player would price its own pick as if
// it still had him, and a team selling its star the buyer's as if the buyer
// still did not.
export const placesAfter = (
	outlook: LeagueOutlook,
	changes: readonly {
		tid: number;
		before: readonly Player[];
		after: readonly Player[];
	}[],
	// Only as far as the change makes a team better: a signing that makes it
	// worse - a poor player the rotation would play ahead of better ones - is
	// not a way to a better pick. Every team plays to win.
	onlyBetter = false,
): LeagueOutlook => {
	const keys = new Map<number, number[]>();
	for (const { tid, before, after } of changes) {
		const was = outlook.placeKeys.get(tid);
		if (!was) {
			return outlook;
		}
		// The change in strength, both ways priced now, onto what the places
		// were ranked by.
		const from = rosterOnlyNow(outlook, tid, before);
		const to = rosterOnlyPoints(outlook, tid, after);
		keys.set(
			tid,
			placeKeysOf(
				outlook,
				tid,
				was.points.map((x, y) =>
					onlyBetter
						? x + Math.max(0, to[y]! - from[y]!)
						: x + to[y]! - from[y]!,
				),
			),
		);
	}
	const draftPlace = new Map(outlook.draftPlace);
	for (const [tid, own] of keys) {
		const was = outlook.draftPlace.get(tid)!;
		draftPlace.set(
			tid,
			own.map((key, y) =>
				y < outlookSettings.placesAfterDrafts
					? softPlace(outlook, tid, key, y, keys)
					: was[y]!,
			),
		);
	}
	return {
		...outlook,
		draftPlace,
		pickYears: new WeakMap(),
		futurePicks: new Map(),
	};
};

// The same, against an average team.
// THE COSTS OF LOSING (owner's call: only what the game itself charges). A
// player's mood toward his team counts its record - from -2 at a quarter of
// its games won to +2 at three quarters, twice as hard below 0
// (moodComponents) - and its hype, which every game won moves up and every
// game lost moves down (writeTeamStats). Mood decides whether he re-signs, and
// free agents go where they want to win (HAUL_BY_RECORD). Retention and room
// are priced from the team as it stands, so a move that leaves it far worse
// was priced as if its players would feel about it as they do now: a team
// that tore itself down to win 3 games was still counting on re-signing whom
// it kept, and on landing free agents with its room. So a move's change in
// the team's record each season, as a share of games won, is turned into
// what it does to the mood of everyone whose contract runs out after that
// season - priced again only when that comes to a quarter point of mood (29%
// of rosters priced in an arena league). Half the teams each way, 4 leagues
// each way round over 6 seasons: its teams won 29 titles to 19, about as many
// games and playoff rounds (352 to 368), with 56 seasons under 20 wins to 81
// and 8 under 12 to 19. Counting hype too, and what the next summer's room
// buys (losingParts), they shied from losing so much more - 5 seasons under
// 20 wins to 41, in 2 leagues each way - that they won 8 titles to 16.
export type LosingShifts = {
	// Points of mood, by the season a contract runs out after.
	mood: number[];
	// Change in the share of games won its room is priced at, by season.
	winp: number[];
	// What a point of mood does to his chance of taking an offer, in log odds:
	// the offer market's moodWeight / temperature.
	slope: number;
};

// For the arena: rosters priced, and how many priced again for this.
export const losingClock = { priced: 0, repriced: 0 };

// Smaller changes than these are not worth pricing the roster again for.
const LOSING_MOOD_MIN = 0.25;
const LOSING_WINP_MIN = 0.02;

const teamPerformanceMood = (winp: number) => {
	const x = -2 + 8 * (winp - 0.25);
	return Math.min(2, x < 0 ? 2 * x : x);
};

export const losingShifts = (
	outlook: Pick<
		LeagueOutlook,
		"basePoints" | "meanPoints" | "hype" | "records" | "remaining" | "progress"
	>,
	tid: number,
	points: readonly number[],
): LosingShifts | undefined => {
	const base = outlook.basePoints?.get(tid);
	if (!base) {
		return;
	}
	const record = outlook.records?.get(tid);
	const left = outlook.remaining.get(tid) ?? 0;
	// The share of its games it is expected to win, this season's with the
	// games already played.
	const winsAt = (path: readonly number[], y: number) => {
		const wp = winProbability(path[y]! - outlook.meanPoints[y]!);
		if (y === 0 && record && record.won + record.lost > 0) {
			return (record.won + left * wp) / (record.won + record.lost + left);
		}
		return wp;
	};
	const hypeNow = outlook.hype?.get(tid) ?? 0.5;
	const mood: number[] = [];
	const winp: number[] = [];
	let material = false;
	let gained = 0;
	for (let y = 0; y < points.length; y++) {
		// Seasons out, a team's finish drifts back toward the middle as moves
		// it has not made yet come in (FINISH_KEPT) - this one's change too.
		const kept = tableAt(FINISH_KEPT, y + 1 - outlook.progress);
		const was = winsAt(base, y);
		const change = kept * (winsAt(points, y) - was);
		// Hype: 0.01 a game for each point of share of games won it is off
		// 0.55, which builds up, and 0.015 a game for each point it is off
		// the last seasons', which does not.
		gained += 0.01 * 82 * change;
		const hype =
			Math.min(1, Math.max(0, hypeNow + gained + 0.015 * 82 * change)) -
			hypeNow;
		const parts = outlookSettings.losingParts;
		mood[y] =
			(parts.record
				? teamPerformanceMood(Math.min(1, Math.max(0, was + change))) -
					teamPerformanceMood(was)
				: 0) + (parts.hype ? 4 * hype : 0);
		// What free agents see the summer after.
		winp[y + 1] = parts.room ? change : 0;
		if (
			Math.abs(mood[y]!) >= LOSING_MOOD_MIN ||
			Math.abs(winp[y + 1]!) >= LOSING_WINP_MIN
		) {
			material = true;
		}
	}
	if (!material) {
		return;
	}
	return {
		mood,
		winp,
		slope: offerSettings.moodWeight / offerSettings.temperature,
	};
};

export const teamStrengths = (
	outlook: LeagueOutlook,
	tid: number,
	players: readonly Player[],
	picks: readonly PickLike[],
	trading?: { tradedAway: TradedAway; extra: number },
	refill?: Parameters<typeof teamPoints>[5],
	known?: LetGoDecisions,
) => {
	const strengthOf = (y: number, points: number) =>
		points -
		outlook.meanPoints[y]! -
		injuryShiftFor(outlook, tid, y) +
		(y === 0 ? (outlook.recordEdge.get(tid) ?? 0) : 0);
	const perCap = outlookSettingFor(tid, "moneyPerCap");
	const salaryCap = g.get("salaryCap");
	const floor = outlookSettingFor(tid, "moneyUnderCap")
		? g.get("minPayroll") / salaryCap
		: 1;
	// Payroll short of the league's minimum is paid as a fine (at the
	// playoffs), so money spent up to it is free - but only this season once
	// games are played can a team know it will be short (floorNow).
	const floorNow = outlookSettingFor(tid, "floorNow");
	const floorAt = (y: number) =>
		floorNow && floor < 1 && !(outlook.offset === 0 && y === 0) ? 0 : floor;
	const money: number[] = [];
	// With wantedOnly, whom the team lets go is decided on the first pass and
	// kept on the second, for its record's shift - as on a deal's roster
	// (LetGoDecisions): deciding afresh doubled the cost of every veteran.
	const decisions: LetGoDecisions | undefined =
		known ??
		(outlookSettingFor(tid, "wantedOnly")
			? { decisions: new Map(), record: true }
			: undefined);
	const pointsWith = (losing?: LosingShifts) =>
		teamPoints(
			outlook,
			tid,
			players,
			picks,
			trading,
			(y, room, signings) => {
				money[y] = payrollCost(perCap, floorAt(y), y, room, signings);
				refill?.(y, room, signings);
			},
			undefined,
			decisions,
			outlook.titleCurves.length > 0
				? (y, points) => seasonValue(outlook, y, strengthOf(y, points), tid)
				: undefined,
			(y, room, signings) =>
				payrollCost(perCap, floorAt(y), y, room / salaryCap, signings),
			losing,
		);
	let points = pointsWith();
	if (outlookSettingFor(tid, "costsOfLosing")) {
		losingClock.priced += 1;
		const losing = losingShifts(
			outlook,
			tid,
			points.map((x, y) => x - injuryShiftFor(outlook, tid, y)),
		);
		if (losing) {
			losingClock.repriced += 1;
			if (decisions && decisions !== known) {
				decisions.record = false;
			}
			points = pointsWith(losing);
		}
	}
	const strengths = points.map((x, y) => strengthOf(y, x));
	// This season's payroll, as it will stand when the playoffs start and the
	// tax is charged: the tax in full, and the rest of the season's salary
	// past the cap.
	if (outlook.offset === 0 && perCap > 0 && g.get("phase") < PHASE.PLAYOFFS) {
		let payroll = 0;
		for (const p of players) {
			payroll += p.contract.amount;
		}
		for (const row of outlook.deadMoney.get(tid) ?? []) {
			if (row.exp >= outlook.season) {
				payroll += row.amount;
			}
		}
		money[0] = payrollShareCost(
			perCap,
			floor,
			payroll / salaryCap,
			1 - outlook.progress,
		);
	}
	moneyOf.set(strengths, money);
	return strengths;
};

// MONEY (owner's call: a small real cost). A team does what is best for the
// franchise, and money spent is part of that: each season, payroll past the
// league's minimum costs `moneyPerCap` titles per cap's worth, and the luxury
// tax's rate on top past the tax line. Payroll counts the free agents the
// team's room is expected to buy, at their market salaries: room it would
// spend anyway costs nothing more spent on someone else, room it could not
// spend is money kept. With money free, a team over the cap paid its own
// players whatever made them likeliest to stay - the max to 21-year-olds
// rated 50 - and charged only past the cap, a bad team with room it could
// not use gave the max for four years to young players rated 53.
const payrollShareCost = (
	perCap: number,
	// Payroll costs money past this share of the cap.
	floor: number,
	payroll: number,
	left = 1,
) => {
	const taxLine = g.get("luxuryPayroll") / g.get("salaryCap");
	return (
		perCap *
		(left * Math.max(0, payroll - floor) +
			g.get("luxuryTax") * Math.max(0, payroll - taxLine))
	);
};

const payrollCost = (
	perCap: number,
	floor: number,
	y: number,
	room: number,
	signings: readonly { valueNoPot: number; availability: number }[],
) => {
	if (perCap === 0) {
		return 0;
	}
	let payroll = 1 - room;
	if (room > 0 && floor < 1) {
		const salary = marketSalaries();
		const cap = g.get("salaryCap");
		for (const s of signings) {
			payroll += (s.availability * salary(s.valueNoPot)) / cap;
		}
		payroll = Math.min(1, payroll);
	}
	return PATIENCE ** y * payrollShareCost(perCap, floor, payroll);
};

// What each season's payroll costs, for strengths worked out by
// teamStrengths.
const moneyOf = new WeakMap<readonly number[], readonly number[]>();
export const moneyCost = (strengths: readonly number[]) =>
	moneyOf.get(strengths);

// Logistic fit of a probability against strength. Used to price a roster that
// is not any team's actual roster.
export const fitCurve = (points: { x: number; p: number }[]): Curve => {
	const [a, b] = fitLogistic(
		points.map(({ x, p }) => ({ x: [x], p })),
		[0, 0.3],
	);
	return { a: a!, b: b! };
};

export const curveAt = (curve: Curve, x: number) =>
	1 / (1 + Math.exp(-(curve.a + curve.b * x)));

// The game's chance of each pick in each round from each place a team goes
// into the draft in - undefined if it has none to give.
const lotteryOdds = async (numTeams: number, season: number) => {
	try {
		const rounds = [];
		for (let round = 1; round <= g.get("numDraftRounds"); round++) {
			rounds.push(await getSlotPickProbsCached(round, numTeams, season));
		}
		return rounds;
	} catch {
		return undefined;
	}
};

// Where each team stands toward the COLA draw.
const colaState = async (
	teams: readonly Team[],
): Promise<LeagueOutlook["cola"]> => {
	const points = new Map(
		teams.map((t) => [
			t.tid,
			t.draftLottery?.type === "cola" ? t.draftLottery.chances : 0,
		]),
	);
	return {
		points,
		meanPoints:
			[...points.values()].reduce((a, x) => a + x, 0) /
			Math.max(1, points.size),
		addNow: g.get("phase") <= PHASE.PLAYOFFS,
		numLottery: await getNumColaLotteryTeams(),
		numPicks: COLA_NUM_LOTTERY_PICKS,
		alpha: COLA_ALPHA,
		matrices: new Map(),
	};
};

// COLA: each team's points toward each draft's draw, carried along its own
// path there. Each season out of the last three playoff rounds adds the
// game's COLA_ALPHA and each one in them keeps a share (PLAYOFF_FACTORS); a
// team holding its own first loses a share for each drawn pick it wins
// (DRAFT_LOTTERY_FACTORS). Expected points, by the chance of each finish the
// season sims give (`finishes`, by outlook year from `first`); a draw goes by
// a team's points against the average of the teams likely to be in it.
// Priced as if every team added the same each season, a contender that goes
// deep year after year kept the draw odds of one that never does.
export const colaPaths = ({
	cola,
	finishes,
	holds,
	season,
	first,
	lastDraft,
	numPlayoffRounds,
}: {
	cola: NonNullable<LeagueOutlook["cola"]>;
	finishes: Map<number, number[][]>;
	// "tid:season" for each first a team holds of its own.
	holds: Set<string>;
	season: number;
	first: number;
	lastDraft: number;
	numPlayoffRounds: number;
}) => {
	const offset = numPlayoffRounds - PLAYOFF_FACTORS.length + 1;
	const tids = [...cola.points.keys()];
	const points = new Map(cola.points);
	const path = new Map(tids.map((tid) => [tid, new Map<number, number>()]));
	for (let d = season; d <= lastDraft; d++) {
		const inDraw = new Map<number, number>();
		const chance = new Map<number, number>();
		const after = new Map<number, number>();
		for (const tid of tids) {
			const p = points.get(tid)!;
			if (d < first) {
				// This season's playoffs are over, and its points with them.
				inDraw.set(tid, p);
				chance.set(tid, 1);
				after.set(tid, p);
				continue;
			}
			const odds = finishes.get(tid)?.[Math.min(OUTLOOK_YEARS, d - first)];
			let out = 1;
			let kept = 0;
			if (odds) {
				out = 0;
				for (const [i, q] of odds.entries()) {
					// i - 1 series won, -1 out of the playoffs.
					const factor = i === 0 ? undefined : PLAYOFF_FACTORS[i - 1 - offset];
					if (factor === undefined) {
						out += q;
					} else {
						kept += q * factor;
					}
				}
			}
			inDraw.set(tid, p + cola.alpha);
			chance.set(tid, out);
			after.set(tid, p * (out + kept) + cola.alpha * out);
		}
		let weight = 0;
		let total = 0;
		for (const tid of tids) {
			weight += chance.get(tid)!;
			total += chance.get(tid)! * inDraw.get(tid)!;
		}
		const mean = weight > 0 ? total / weight : 0;
		for (const tid of tids) {
			const ratio =
				mean > 0 ? Math.round((20 * inDraw.get(tid)!) / mean) / 20 : 1;
			path.get(tid)!.set(d, ratio);
			let p = after.get(tid)!;
			if (holds.has(`${tid}:${d}`) && ratio > 0) {
				const { win } = colaWins(cola, ratio);
				let lost = 0;
				for (const [j, q] of win.entries()) {
					lost += q * (1 - (DRAFT_LOTTERY_FACTORS[j] ?? 1));
				}
				p -= chance.get(tid)! * lost * inDraw.get(tid)!;
			}
			points.set(tid, Math.max(0, p));
		}
	}
	return path;
};

// In the offseason before this season's draft order is set: where each team
// goes into it, the worst first - the teams that missed the playoffs by
// record, then the rest.
const finalPlaces = async (season: number) => {
	const unset = (
		await idb.cache.draftPicks.indexGetAll("draftPicksBySeason", season)
	).some((dp) => dp.round === 1 && dp.pick <= 0);
	if (!unset) {
		return undefined;
	}
	const rows = (
		await idb.cache.teamSeasons.indexGetAll("teamSeasonsBySeasonTid", [
			[season],
			[season, "Z"],
		])
	).map((ts) => {
		const games = ts.won + ts.lost + (ts.tied ?? 0) + (ts.otl ?? 0);
		return {
			tid: ts.tid,
			playoffs: ts.playoffRoundsWon >= 0,
			winp: games > 0 ? (ts.won + 0.5 * (ts.tied ?? 0)) / games : 0.5,
		};
	});
	rows.sort(
		(a, b) =>
			Number(a.playoffs) - Number(b.playoffs) ||
			a.winp - b.winp ||
			a.tid - b.tid,
	);
	return new Map(rows.map((row, i) => [row.tid, i]));
};

export const buildOutlook = async ({
	iterations = 1000,
}: { iterations?: number } = {}): Promise<LeagueOutlook> => {
	const season = g.get("season");
	const phase = g.get("phase");
	const offset = phase > PHASE.PLAYOFFS ? 1 : 0;
	const numGames = g.get("numGames");
	const inSeason = offset === 0;
	// What players feel about each team, as of now.
	clearMarketView();

	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const records = new Map<number, { won: number; lost: number }>();
	const hype = new Map<number, number>();
	for (const t of teams) {
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
			season,
			t.tid,
		]);
		hype.set(t.tid, ts?.hype ?? 0.5);
		records.set(t.tid, {
			won: inSeason ? (ts?.won ?? 0) : 0,
			lost: inSeason ? (ts?.lost ?? 0) + (ts?.tied ?? 0) + (ts?.otl ?? 0) : 0,
		});
	}
	const deadMoney = new Map<number, { amount: number; exp: number }[]>();
	for (const t of teams) {
		deadMoney.set(
			t.tid,
			(
				await idb.cache.releasedPlayers.indexGetAll(
					"releasedPlayersByTid",
					t.tid,
				)
			).map((rp) => ({ amount: rp.contract.amount, exp: rp.contract.exp })),
		);
	}
	const rosters = new Map<number, Player[]>();
	const picks = new Map<number, DraftPick[]>();
	let lastPickSeason = season;
	for (const t of teams) {
		rosters.set(
			t.tid,
			await idb.cache.players.indexGetAll("playersByTid", t.tid),
		);
		const teamPicks = await idb.cache.draftPicks.indexGetAll(
			"draftPicksByTid",
			t.tid,
		);
		picks.set(t.tid, teamPicks);
		for (const dp of teamPicks) {
			if (typeof dp.season === "number" && dp.season > lastPickSeason) {
				lastPickSeason = dp.season;
			}
		}
	}

	const remaining = new Map<number, number>();
	for (const t of teams) {
		const r = records.get(t.tid)!;
		remaining.set(
			t.tid,
			inSeason ? Math.max(0, numGames - r.won - r.lost) : numGames,
		);
	}
	const progress =
		teams.reduce((sum, t) => sum + (numGames - remaining.get(t.tid)!), 0) /
		Math.max(1, teams.length * numGames);

	const base = {
		season,
		offset,
		remaining,
		numTeams: teams.length,
		numRounds: g.get("numDraftRounds"),
		lastPickSeason,
		draftPlace: new Map<number, number[]>(),
		placeKeys: new Map<number, { points: number[]; keys: number[] }>(),
		placeNow: undefined as LeagueOutlook["placeNow"],
		placeBase: new Map<string, number[]>(),
		deadMoney,
		// During free agency too: room not yet spent is worth what it can still
		// buy, so money paid out this coming season is not free.
		firstRefillSeason: season + 1,
		lines: new Map() as SeasonLines,
		pickLines: new Map<string, PickAt | null>(),
		pickYears: new WeakMap<PickLike, PickYears>(),
		futurePicks: new Map<number, FuturePickAt[][]>(),
		retention: new Map<string, number>(),
		stays: new Map<string, number>(),
		hype,
		records,
		basePoints: undefined as LeagueOutlook["basePoints"],
		meanPoints: [] as number[],
		injuryShift: undefined as LeagueOutlook["injuryShift"],
		progress: inSeason ? progress : 0,
		recordEdge: new Map<number, number>(),
		winShare: undefined as LeagueOutlook["winShare"],
		slotPicks: outlookSettings.lottery
			? await lotteryOdds(teams.length, season)
			: undefined,
		finalPlace: inSeason ? undefined : await finalPlaces(season),
		cola:
			outlookSettings.lottery && g.get("draftType") === "cola"
				? await colaState(teams)
				: undefined,
		knownClasses:
			outlookSettings.knownClasses || teamOutlookSettings.size > 0
				? await buildKnownClasses({
						season,
						numTeams: teams.length,
						numRounds: g.get("numDraftRounds"),
						lastPickSeason,
					})
				: undefined,
	};
	for (const t of teams) {
		await ensureRetention(base, rosters.get(t.tid)!, t.tid);
	}

	// How good each team's roster alone is each season, against the league's,
	// as a share of games won: what its room buys goes by it. Priced first
	// with every team's room worth the league's average, and everything below
	// with it, so a roster priced now and one priced in a deal later are
	// priced alike.
	if (outlookSettings.roomByRecord || teamOutlookSettings.size > 0) {
		const first = new Map(
			teams.map((t) => [
				t.tid,
				rosterOnlyPoints(base, t.tid, rosters.get(t.tid)!),
			]),
		);
		const means = Array.from(
			{ length: OUTLOOK_YEARS + 1 },
			(_, y) =>
				teams.reduce((a, t) => a + first.get(t.tid)![y]!, 0) /
				Math.max(1, teams.length),
		);
		// Seasons out, teams drift back toward the middle as their finishes
		// do (FINISH_KEPT): a bad roster now is not a bad team for good, and
		// its room later is worth more than its roster alone says.
		base.winShare = new Map(
			teams.map((t) => [
				t.tid,
				first
					.get(t.tid)!
					.map(
						(x, y) =>
							0.5 +
							tableAt(FINISH_KEPT, y + 1 - (inSeason ? progress : 0)) *
								(winProbability(x - means[y]!) - 0.5),
					),
			]),
		);
	}

	// Where each team's own pick lands, from its roster alone: worst first -
	// and this season's draft, once games are played, by the record it is
	// heading for, the wins already banked included.
	const rosterOnly = new Map(
		teams.map((t) => [
			t.tid,
			rosterOnlyPoints(base, t.tid, rosters.get(t.tid)!),
		]),
	);
	for (const t of teams) {
		base.draftPlace.set(t.tid, []);
	}
	const meanNow =
		teams.reduce((a, t) => a + rosterOnly.get(t.tid)![0]!, 0) /
		Math.max(1, teams.length);
	if (inSeason) {
		base.placeNow = {
			won: new Map(teams.map((t) => [t.tid, records.get(t.tid)!.won])),
			meanNow,
		};
	}
	for (const t of teams) {
		const points = rosterOnly.get(t.tid)!;
		base.placeKeys.set(t.tid, {
			points,
			keys: placeKeysOf(base, t.tid, points),
		});
	}
	for (const t of teams) {
		base.draftPlace.set(
			t.tid,
			base.placeKeys
				.get(t.tid)!
				.keys.map((key, y) => softPlace(base, t.tid, key, y, new Map())),
		);
	}
	// Picks' lines worked out before then had nowhere to land.
	base.pickYears = new WeakMap();
	base.futurePicks.clear();

	const pointsAt = (view: InjuryMode) =>
		withInjuryView(
			view,
			() =>
				new Map(
					teams.map((t) => [
						t.tid,
						teamPoints(base, t.tid, rosters.get(t.tid)!, picks.get(t.tid)!),
					]),
				),
		);
	const points = pointsAt(leagueInjuryMode());
	base.basePoints = points;
	// Only in the arena's version-against-version leagues: the average team at
	// the other injury setting.
	const otherMode = teams
		.map((t) => teamInjuryMode(t.tid))
		.find((mode) => mode !== leagueInjuryMode());
	if (otherMode) {
		const other = pointsAt(otherMode);
		const meanOf = (map: Map<number, number[]>, y: number) =>
			teams.reduce((a, t) => a + map.get(t.tid)![y]!, 0) /
			Math.max(1, teams.length);
		base.injuryShift = Array.from(
			{ length: OUTLOOK_YEARS + 1 },
			(_, y) => meanOf(other, y) - meanOf(points, y),
		);
	}

	const outlooks = new Map<number, TeamOutlook>();
	for (const t of teams) {
		outlooks.set(t.tid, { tid: t.tid, strength: [], title: [], playoffs: [] });
	}
	const titleCurves: Curve[] = [];
	const playoffCurves: Curve[] = [];
	// Only in the arena's version-against-version leagues.
	const otherCurves = [...teamOutlookSettings.values()].some(
		(x) =>
			x.measuredUncertainty !== undefined &&
			x.measuredUncertainty !== outlookSettings.measuredUncertainty,
	)
		? { title: [] as Curve[], playoff: [] as Curve[] }
		: undefined;
	// Each team's chance of each finish, season by season (colaPaths).
	const finishes = new Map<number, number[][]>(teams.map((t) => [t.tid, []]));

	for (let y = 0; y <= OUTLOOK_YEARS; y++) {
		const mean =
			teams.reduce((a, t) => a + points.get(t.tid)![y]!, 0) /
			Math.max(1, teams.length);
		base.meanPoints.push(mean);

		const strengthOf = (tid: number) => points.get(tid)![y]! - mean;
		const simWith = (measured: boolean) =>
			simulateSeason({
				teams: teams.map((t) => ({
					tid: t.tid,
					cid: t.cid,
					strength: strengthOf(t.tid),
					won: y === 0 && inSeason ? records.get(t.tid)!.won : 0,
					lost: y === 0 && inSeason ? records.get(t.tid)!.lost : 0,
					gamesRemaining: y === 0 ? remaining.get(t.tid)! : numGames,
				})),
				numGamesPlayoffSeries: g.get("numGamesPlayoffSeries", "current"),
				playIn: g.get("playIn"),
				byConference: g.get("playoffsByConf", "current") ?? true,
				uncertainty: strengthUncertainty(
					y,
					y === 0 && inSeason ? progress : 0,
					measured,
				),
				iterations: iterationsFor(y, iterations),
				seed: season * 1009 + y * 101 + Math.round(progress * 1000),
			});
		const sim = simWith(outlookSettings.measuredUncertainty);
		for (const t of teams) {
			finishes.get(t.tid)!.push(sim.get(t.tid)!.rounds);
		}

		for (const t of teams) {
			const o = outlooks.get(t.tid)!;
			const r = sim.get(t.tid)!;
			o.strength.push(strengthOf(t.tid));
			o.title.push(r.title);
			o.playoffs.push(r.playoffs);
		}
		// Wins banked beyond what the team as it stands would have won in the
		// games played: luck, or a roster that has changed. Worth something in
		// the standings; fit on every team's odds, then put in points.
		if (y === 0 && inSeason && outlookSettings.recordOdds) {
			const luck = (tid: number) => {
				const r = records.get(tid)!;
				return r.won - (r.won + r.lost) * winProbability(strengthOf(tid));
			};
			const [, b, c] = fitLogistic(
				teams.map((t) => ({
					x: [strengthOf(t.tid), luck(t.tid)],
					p: sim.get(t.tid)!.title,
				})),
				[0, 0.3, 0],
			);
			if (b! > 0 && c! > 0) {
				for (const t of teams) {
					base.recordEdge.set(t.tid, (c! / b!) * luck(t.tid));
				}
			}
		}
		const x = (tid: number) =>
			strengthOf(tid) + (y === 0 ? (base.recordEdge.get(tid) ?? 0) : 0);
		titleCurves.push(
			fitCurve(teams.map((t) => ({ x: x(t.tid), p: sim.get(t.tid)!.title }))),
		);
		playoffCurves.push(
			fitCurve(
				teams.map((t) => ({
					x: x(t.tid),
					p: sim.get(t.tid)!.playoffs,
				})),
			),
		);
		if (otherCurves) {
			const other = simWith(!outlookSettings.measuredUncertainty);
			otherCurves.title.push(
				fitCurve(
					teams.map((t) => ({ x: x(t.tid), p: other.get(t.tid)!.title })),
				),
			);
			otherCurves.playoff.push(
				fitCurve(
					teams.map((t) => ({ x: x(t.tid), p: other.get(t.tid)!.playoffs })),
				),
			);
		}
	}

	if (
		base.cola &&
		(outlookSettings.colaPath ||
			[...teamOutlookSettings.values()].some((x) => x.colaPath))
	) {
		const holds = new Set<string>();
		for (const list of picks.values()) {
			for (const dp of list) {
				if (dp.round === 1 && dp.tid === dp.originalTid) {
					holds.add(`${dp.originalTid}:${dp.season}`);
				}
			}
		}
		base.cola.path = colaPaths({
			cola: base.cola,
			finishes,
			holds,
			season,
			first: outlookSeason(base, 0),
			lastDraft: Math.max(lastPickSeason, outlookSeason(base, OUTLOOK_YEARS)),
			numPlayoffRounds: g.get("numGamesPlayoffSeries", "current").length,
		});
		// The team's own picks past the last draft that exists were worked
		// out for its points before it had a path, and kept: a deal priced
		// on a view of the outlook with them worked out again (placesAfter)
		// gained or lost what the path moves them.
		base.futurePicks.clear();
	}

	let deadline: DeadlineView | undefined;
	if (phase === PHASE.REGULAR_SEASON) {
		let daysToDeadline: number | undefined;
		try {
			daysToDeadline = await getDaysLeftSchedule("tradeDeadline");
		} catch {
			// No deadline left this season.
		}
		const daysLeft = await getDaysLeftSchedule();
		if (daysToDeadline !== undefined && daysToDeadline > 0 && daysLeft > 0) {
			const uncertaintyAt = (p: number) => 1.5 + 2 * (1 - p);
			const progressAtDeadline =
				progress + (1 - progress) * (daysToDeadline / daysLeft);

			deadline = buildDeadlineView({
				teams: teams.map((t) => ({
					tid: t.tid,
					cid: t.cid,
					strength: outlooks.get(t.tid)!.strength[0]!,
					won: records.get(t.tid)!.won,
					lost: records.get(t.tid)!.lost,
					gamesRemaining: remaining.get(t.tid)!,
				})),
				numGamesPlayoffSeries: g.get("numGamesPlayoffSeries", "current"),
				playIn: g.get("playIn"),
				byConference: g.get("playoffsByConf", "current") ?? true,
				uncertaintyNow: uncertaintyAt(progress),
				uncertaintyAtDeadline: uncertaintyAt(progressAtDeadline),
				measuredNow: measuredStrengthUncertainty(0, progress),
				measuredAtDeadline: measuredStrengthUncertainty(0, progressAtDeadline),
				daysToDeadline,
				daysLeft,
				seed: season * 7919 + Math.round(progress * 1000),
			});
		}
	}

	const playoffWeight =
		deadline &&
		deadline.curve.regular > 0 &&
		deadline.curve.playoff > 0 &&
		outlookSettings.playoffInjuries
			? deadline.curve.playoff /
				(deadline.curve.regular + deadline.curve.playoff)
			: undefined;
	return {
		...base,
		teams: outlooks,
		titleCurves,
		playoffCurves,
		otherCurves,
		deadline,
		classBoard: await buildClassBoard(base),
		playoffWeight,
		// About three quarters of the most a run could last.
		playoffGames: Math.round(
			0.75 *
				g
					.get("numGamesPlayoffSeries", "current")
					.reduce((a: number, x: number) => a + x, 0),
		),
	};
};

// What this season alone is worth at this strength, in the same units as
// franchiseValue.
export const seasonValue = (
	outlook: LeagueOutlook,
	y: number,
	s: number,
	tid?: number,
) => {
	const { title, playoff } = curvesFor(outlook, tid);
	return (
		PATIENCE ** y *
		(curveAt(title[y]!, s) + PLAYOFF_TIEBREAK * curveAt(playoff[y]!, s))
	);
};

// The title and playoff curves a team prices by: its own measuredUncertainty's
// in the arena's version-against-version leagues, otherwise the league's.
export const curvesFor = (
	outlook: Pick<LeagueOutlook, "titleCurves" | "playoffCurves" | "otherCurves">,
	tid?: number,
) =>
	tid !== undefined &&
	outlook.otherCurves &&
	outlookSettingFor(tid, "measuredUncertainty") !==
		outlookSettings.measuredUncertainty
		? outlook.otherCurves
		: { title: outlook.titleCurves, playoff: outlook.playoffCurves };

// The deadline as a team reads it: what it expects to learn by then by its
// own measuredDeadline.
export const deadlineViewFor = (
	outlook: Pick<LeagueOutlook, "deadline">,
	tid: number,
) => {
	const view = outlook.deadline;
	return view &&
		view.revealedMeasured !== undefined &&
		outlookSettingFor(tid, "measuredDeadline")
		? { ...view, revealed: view.revealedMeasured }
		: view;
};

// What a run of seasons at these strengths is worth to a franchise, in titles:
// discounted title chances, plus a sliver for making the playoffs.
export const franchiseValue = (
	outlook: LeagueOutlook,
	strengths: number[],
	tid?: number,
) => {
	const { title, playoff } = curvesFor(outlook, tid);
	let value = 0;
	for (const [y, s] of strengths.entries()) {
		const weight = PATIENCE ** y;
		value +=
			weight *
			(curveAt(title[y]!, s) + PLAYOFF_TIEBREAK * curveAt(playoff[y]!, s));
	}
	for (const x of moneyOf.get(strengths) ?? []) {
		if (x !== undefined) {
			value -= x;
		}
	}
	return value;
};
