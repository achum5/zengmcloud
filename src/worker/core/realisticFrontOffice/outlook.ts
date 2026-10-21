import { PHASE } from "../../../common/constants.ts";
import type {
	DraftPick,
	MinimalPlayerRatings,
	Player,
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
import { projectRatings, type RatingsProjection } from "./projection.ts";
import moodInfo from "../player/moodInfo.ts";
import getDaysLeftSchedule from "../season/getDaysLeftSchedule.ts";
import { normalCdf, simulateSeason, winProbability } from "./seasonSim.ts";
import { fitLogistic } from "./logistic.ts";
import { buildDeadlineView, type DeadlineView } from "./timing.ts";
import {
	expectedRosterPoints,
	getReplacement,
	lineSynergy,
	playerLine,
	type StrengthPlayer,
	type ValueRatings,
} from "./strength.ts";

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

// Still playing that many seasons from now, by age - a stand-in until
// retirement is modelled properly.
const survival = (age: number) =>
	age <= 31
		? 0.99
		: age === 32
			? 0.96
			: age === 33
				? 0.92
				: age === 34
					? 0.86
					: age === 35
						? 0.78
						: age === 36
							? 0.68
							: age === 37
								? 0.58
								: 0.45;

// A player's chance of re-signing when his contract runs out is the game's own
// mood model's (player.moodInfo's probWilling), with the team he would be
// re-signing with - so a player traded for is priced on how he would feel
// about his new team. Worked out once per outlook and kept here; this is only
// for a player nobody asked about.
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
	// 0 for the first, from its roster alone.
	draftPlace: Map<number, number[]>;
	// Money each team owes players it has released, and until when.
	deadMoney: Map<number, { amount: number; exp: number }[]>;
	// The first season whose free agency is still to come: from then on,
	// teams refill with their cap room (payroll.ts).
	firstRefillSeason: number;
	// Season lines already worked out, for this outlook's lifetime.
	lines: SeasonLines;
	// The same for picks (pickLineAt); null for a pick worth nothing.
	pickLines: Map<string, PickAt | null>;
	// Each pick's line every season of the outlook, as pickLineAt has it, and
	// each team's picks in the drafts beyond the last that exists.
	pickYears: WeakMap<PickLike, PickYears>;
	futurePicks: Map<number, PickAt[][]>;
	// Chance each player re-signs with each team, by retentionKey.
	retention: Map<string, number>;
	teams: Map<number, TeamOutlook>;
	// Roster points of the average team each season, so a hypothetical roster
	// can be put on the same scale.
	meanPoints: number[];
	titleCurves: Curve[];
	playoffCurves: Curve[];
	// Share of this season's games played, 0 in the offseason.
	progress: number;
	// For weighing a deal now against waiting, until the trade deadline passes.
	deadline?: DeadlineView;
	// What each team's record so far is worth to its title chances this
	// season, in points of strength (outlookSettings.recordOdds). Empty when
	// off or out of season.
	recordEdge: Map<number, number>;
};

// How far a team's true strength may be from our read of it, in points a game,
// outlook year y: surer once games have been played, less sure the further out
// it reaches. `progress` is the share of the season played, for this season.
export const strengthUncertainty = (y: number, progress: number) =>
	Math.sqrt((1.5 + 2 * (1 - progress)) ** 2 + (1.6 * y) ** 2);

// The season outlook year y is about.
export const outlookSeason = (
	outlook: Pick<LeagueOutlook, "season" | "offset">,
	y: number,
) => outlook.season + outlook.offset + y;

// Kept for the season they were made in, and dropped once it has passed.
const projections = new Map<string, RatingsProjection>();
let projectionsSeason = -Infinity;

const seedFor = (pid: number, season: number) =>
	(pid * 2_654_435_761 + season * 40_503) % 2_147_483_647;

// The parts of a player's line that average over simulated careers.
export const lineMeasure = (ratings: MinimalPlayerRatings) => {
	const line = playerLine(ratings as unknown as ValueRatings);
	return [line.roleFree, line.offense, line.usage];
};

export const projectPlayer = (p: Player, season: number) => {
	if (season !== projectionsSeason) {
		projections.clear();
		projectionsSeason = season;
	}
	const r = last(p.ratings);
	const key = `${p.pid}:${season}:${r.season}:${r.ovr}:${r.pot}`;
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
): StrengthPlayer & { availability: number } => {
	const index = y + offset;
	if (index === 0) {
		const r = last(p.ratings);
		const out = p.injury.gamesRemaining;
		const availability =
			out <= 0 ? 1 : 1 - Math.min(1, out / Math.max(1, gamesRemaining));
		return {
			...playerLine(r as unknown as ValueRatings),
			valueNoPot: p.valueNoPot,
			availability,
		};
	}
	const projection = projectPlayer(p, season);
	const age = season - p.born.year;
	let availability = 1;
	for (let k = 1; k <= index; k++) {
		availability *= survival(age + k);
	}
	if (p.contract.exp < season + index) {
		availability *= retention;
	}
	const [roleFree, offense, usage] = projection.measures[index]!;
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
	return {
		roleFree: roleFree!,
		offense: offense!,
		usage: usage!,
		synergy,
		valueNoPot: projection.ovr[index]!,
		availability,
	};
};

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
	byYear: ((StrengthPlayer & { availability: number }) | undefined)[];
};
export type SeasonLines = Map<number, Map<number, PlayerLines[]>>;

const playerLines = (
	lines: SeasonLines,
	p: Player,
	season: number,
	gamesRemaining: number,
	retention: number,
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
			entry.valueNoPot === valueNoPot
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
	recordOdds: false,
};

export const ensureRetention = async (
	outlook: Pick<LeagueOutlook, "retention">,
	players: readonly Player[],
	tid: number,
) => {
	for (const p of players) {
		const key = retentionKey(p.pid, tid);
		if (!outlook.retention.has(key)) {
			const asked = !outlookSettings.teamRetention && p.tid >= 0 ? p.tid : tid;
			const { probWilling } = await moodInfo(
				p.tid === asked ? p : { ...p, tid: asked },
				asked,
			);
			outlook.retention.set(key, Math.round(probWilling * 1000) / 1000);
		}
	}
};

export type PickLike = Pick<
	DraftPick,
	"originalTid" | "season" | "round" | "pick"
>;

// A pick's player as a roster sees him, and what he is expected to be paid.
type PickAt = {
	line: StrengthPlayer & { availability: number };
	salary: number;
};

// A pick's line each season of the outlook (null for none), for the pick as
// it was when they were worked out.
type PickYears = {
	originalTid: number;
	season: PickLike["season"];
	round: number;
	pick: number;
	byYear: (PickAt | null | undefined)[];
};

const pickYearsOf = (
	outlook: Omit<LeagueOutlook, "teams" | "titleCurves" | "playoffCurves">,
	dp: PickLike,
) => {
	let entry = outlook.pickYears.get(dp);
	if (
		!entry ||
		entry.originalTid !== dp.originalTid ||
		entry.season !== dp.season ||
		entry.round !== dp.round ||
		entry.pick !== dp.pick
	) {
		entry = {
			originalTid: dp.originalTid,
			season: dp.season,
			round: dp.round,
			pick: dp.pick,
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
					lines.push(at);
				}
			}
		}
		byYear[y] = lines;
	}
	return lines;
};

// How many places in its round a pick lands from where the outlook expects,
// typically (root mean square), by seasons of games still to be played before
// its draft order is set. Measured in arena leagues (ARENA_PICK_SURVEY): every
// outlook's expected place for every team's own first-round pick, against
// where it landed, the lottery included (35,400 of them). A guess at random
// would be about 12 places off.
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

const spreadAt = (seasons: number) => {
	const last = DRAFT_PLACE_SPREAD.at(-1)!;
	if (seasons >= last[0]) {
		return last[1];
	}
	let i = 0;
	while (DRAFT_PLACE_SPREAD[i + 1]![0] < seasons) {
		i += 1;
	}
	const [s0, e0] = DRAFT_PLACE_SPREAD[i]!;
	const [s1, e1] = DRAFT_PLACE_SPREAD[i + 1]!;
	return e0 + ((e1 - e0) * (seasons - s0)) / (s1 - s0);
};

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
export const expectedPlace = (
	outlook: Pick<LeagueOutlook, "draftPlace" | "season" | "offset" | "numTeams">,
	dp: PickLike,
) => {
	if (dp.pick > 0) {
		return dp.pick - 1;
	}
	const places = outlook.draftPlace.get(dp.originalTid);
	const yDraft = (dp.season as number) - outlookSeason(outlook, 0);
	return (
		places?.[Math.min(OUTLOOK_YEARS, Math.max(0, yDraft))] ??
		Math.floor(outlook.numTeams / 2)
	);
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
): PickAt | undefined => {
	const season = outlookSeason(outlook, y);
	if (typeof dp.season !== "number" || dp.season >= season) {
		return undefined;
	}
	const n = outlook.numTeams;
	const center = Math.min(
		n - 1,
		Math.max(0, expectedPlace(outlook, dp) + shift),
	);
	const spread = Math.round(10 * draftPlaceSpread(outlook, dp)) / 10;
	const years = season - dp.season;
	const key = `${dp.round}:${Math.round(10 * center) / 10}:${spread}:${years}`;
	let at = outlook.pickLines.get(key);
	if (at === undefined) {
		at = averagePick(outlook, dp.round, center, spread, years);
		outlook.pickLines.set(key, at);
	}
	return at ?? undefined;
};

const averagePick = (
	outlook: Pick<LeagueOutlook, "numTeams" | "numRounds">,
	round: number,
	center: number,
	spread: number,
	years: number,
): PickAt | null => {
	const n = outlook.numTeams;
	const numPicks = outlook.numRounds * n;
	// The chance of each place in the round, the ends taking what lies beyond.
	const weights: number[] = [];
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
	let kept = 0;
	let roleFree = 0;
	let offense = 0;
	let usage = 0;
	let ovr = 0;
	let salary = 0;
	let pickSalary: ReturnType<typeof pickSalaries> | undefined;
	for (const [k, w] of weights.entries()) {
		if (w < 1e-4) {
			continue;
		}
		const slot = (round - 1) * n + k;
		const p = pickPlayer({ slot, numPicks, years });
		const share = w * p.availability;
		if (share <= 0) {
			continue;
		}
		kept += share;
		roleFree += share * p.roleFree;
		offense += share * p.offense;
		usage += share * p.usage;
		ovr += share * p.valueNoPot;
		// Only the players worth keeping are paid.
		pickSalary ??= pickSalaries();
		salary += share * pickSalary({ slot, round, years, ovr: p.valueNoPot });
	}
	if (kept <= 0) {
		return null;
	}
	return {
		line: {
			roleFree: roleFree / kept,
			offense: offense / kept,
			usage: usage / kept,
			// Who he is is not known yet, so no particular skills.
			synergy: getReplacement().synergy,
			valueNoPot: ovr / kept,
			availability: kept,
		},
		salary,
	};
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
) => {
	const retentionFor = (p: Player) => {
		const r = retentionOf(outlook, p.pid, tid);
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
	const byPlayer = players.map((p, i) =>
		playerLines(
			outlook.lines,
			p,
			outlook.season,
			gamesRemaining,
			retention[i]!,
		),
	);
	const byPick = picks.map((dp) => pickYearsOf(outlook, dp));
	// League settings, looked up once: nothing here can change them.
	let salaryCap: number | undefined;
	let marketSalary: ((value: number) => number) | undefined;
	const out: number[] = [];
	for (let y = 0; y <= OUTLOOK_YEARS; y++) {
		const lines = players.map(
			(p, i) =>
				(byPlayer[i]!.byYear[y] ??= computeSeasonLine(
					p,
					outlook.season,
					outlook.offset,
					y,
					gamesRemaining,
					retention[i]!,
				)),
		);
		// The players its picks are expected to have become by then, and what
		// they are paid.
		let salaries = 0;
		for (let i = 0; i < picks.length; i++) {
			const byYear = byPick[i]!.byYear;
			let at = byYear[y];
			if (at === undefined) {
				at = pickLineAt(outlook, picks[i]!, y) ?? null;
				byYear[y] = at;
			}
			if (at) {
				lines.push(at.line);
				salaries += at.salary;
			}
		}
		for (const at of futurePickLines(outlook, tid, y)) {
			lines.push(at.line);
			salaries += at.salary;
		}
		const season = outlookSeason(outlook, y);
		if (season >= outlook.firstRefillSeason) {
			salaryCap ??= g.get("salaryCap");
			marketSalary ??= marketSalaries();
			let committed = salaries;
			for (let i = 0; i < players.length; i++) {
				committed += salaryIn(
					players[i]!,
					season,
					y + outlook.offset,
					lines[i]!,
					marketSalary,
				);
			}
			for (const row of outlook.deadMoney.get(tid) ?? []) {
				if (row.exp >= season) {
					committed += row.amount;
				}
			}
			const signings = signingsWith(salaryCap - committed);
			lines.push(...signings);
			refill?.(y, 1 - committed / salaryCap, signings);
		}
		roster?.(y, lines);
		out.push(expectedRosterPoints(lines));
	}
	return out;
};

// The same, against an average team.
export const teamStrengths = (
	outlook: LeagueOutlook,
	tid: number,
	players: readonly Player[],
	picks: readonly PickLike[],
	trading?: { tradedAway: TradedAway; extra: number },
	refill?: Parameters<typeof teamPoints>[5],
) =>
	teamPoints(outlook, tid, players, picks, trading, refill).map(
		(x, y) =>
			x -
			outlook.meanPoints[y]! +
			(y === 0 ? (outlook.recordEdge.get(tid) ?? 0) : 0),
	);

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

export const buildOutlook = async ({
	iterations = 1000,
}: { iterations?: number } = {}): Promise<LeagueOutlook> => {
	const season = g.get("season");
	const phase = g.get("phase");
	const offset = phase > PHASE.PLAYOFFS ? 1 : 0;
	const numGames = g.get("numGames");
	const inSeason = offset === 0;

	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const records = new Map<number, { won: number; lost: number }>();
	for (const t of teams) {
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
			season,
			t.tid,
		]);
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
		deadMoney,
		firstRefillSeason: season + 1 + (phase === PHASE.FREE_AGENCY ? 1 : 0),
		lines: new Map() as SeasonLines,
		pickLines: new Map<string, PickAt | null>(),
		pickYears: new WeakMap<PickLike, PickYears>(),
		futurePicks: new Map<number, PickAt[][]>(),
		retention: new Map<string, number>(),
		meanPoints: [] as number[],
		progress: inSeason ? progress : 0,
		recordEdge: new Map<number, number>(),
	};
	for (const t of teams) {
		await ensureRetention(base, rosters.get(t.tid)!, t.tid);
	}

	// Where each team's own pick lands, from its roster alone: worst first -
	// and this season's draft, once games are played, by the record it is
	// heading for, the wins already banked included.
	const rosterOnly = new Map(
		teams.map((t) => [t.tid, teamPoints(base, t.tid, rosters.get(t.tid)!, [])]),
	);
	for (const t of teams) {
		base.draftPlace.set(t.tid, []);
	}
	const meanNow =
		teams.reduce((a, t) => a + rosterOnly.get(t.tid)![0]!, 0) /
		Math.max(1, teams.length);
	const finalWins = (tid: number) =>
		records.get(tid)!.won +
		remaining.get(tid)! * winProbability(rosterOnly.get(tid)![0]! - meanNow);
	for (let y = 0; y <= OUTLOOK_YEARS; y++) {
		const order = [...teams].sort((a, b) =>
			y === 0 && inSeason
				? finalWins(a.tid) - finalWins(b.tid)
				: rosterOnly.get(a.tid)![y]! - rosterOnly.get(b.tid)![y]!,
		);
		for (const [i, t] of order.entries()) {
			base.draftPlace.get(t.tid)!.push(i);
		}
	}
	// Picks' lines worked out before then had nowhere to land.
	base.pickYears = new WeakMap();
	base.futurePicks.clear();

	const points = new Map(
		teams.map((t) => [
			t.tid,
			teamPoints(base, t.tid, rosters.get(t.tid)!, picks.get(t.tid)!),
		]),
	);

	const outlooks = new Map<number, TeamOutlook>();
	for (const t of teams) {
		outlooks.set(t.tid, { tid: t.tid, strength: [], title: [], playoffs: [] });
	}
	const titleCurves: Curve[] = [];
	const playoffCurves: Curve[] = [];

	for (let y = 0; y <= OUTLOOK_YEARS; y++) {
		const mean =
			teams.reduce((a, t) => a + points.get(t.tid)![y]!, 0) /
			Math.max(1, teams.length);
		base.meanPoints.push(mean);

		const uncertainty = strengthUncertainty(
			y,
			y === 0 && inSeason ? progress : 0,
		);

		const strengthOf = (tid: number) => points.get(tid)![y]! - mean;
		const sim = simulateSeason({
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
			uncertainty,
			iterations: iterationsFor(y, iterations),
			seed: season * 1009 + y * 101 + Math.round(progress * 1000),
		});

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
				daysToDeadline,
				daysLeft,
				seed: season * 7919 + Math.round(progress * 1000),
			});
		}
	}

	return {
		...base,
		teams: outlooks,
		titleCurves,
		playoffCurves,
		deadline,
	};
};

// What this season alone is worth at this strength, in the same units as
// franchiseValue.
export const seasonValue = (outlook: LeagueOutlook, y: number, s: number) =>
	PATIENCE ** y *
	(curveAt(outlook.titleCurves[y]!, s) +
		PLAYOFF_TIEBREAK * curveAt(outlook.playoffCurves[y]!, s));

// What a run of seasons at these strengths is worth to a franchise, in titles:
// discounted title chances, plus a sliver for making the playoffs.
export const franchiseValue = (outlook: LeagueOutlook, strengths: number[]) => {
	let value = 0;
	for (const [y, s] of strengths.entries()) {
		const weight = PATIENCE ** y;
		value +=
			weight *
			(curveAt(outlook.titleCurves[y]!, s) +
				PLAYOFF_TIEBREAK * curveAt(outlook.playoffCurves[y]!, s));
	}
	return value;
};
