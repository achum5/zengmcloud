import { PHASE } from "../../../common/constants.ts";
import type {
	DraftPick,
	MinimalPlayerRatings,
	Player,
} from "../../../common/types.ts";
import { last } from "../../../common/utils.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { marketSalary, pickSalary, signingsWith } from "./payroll.ts";
import { pickPlayer } from "./pickValue.ts";
import {
	moodShiftFromTrading,
	shiftRetention,
	type TradedAway,
} from "./tradePenalty.ts";
import { projectRatings, type RatingsProjection } from "./projection.ts";
import moodInfo from "../player/moodInfo.ts";
import getDaysLeftSchedule from "../season/getDaysLeftSchedule.ts";
import { simulateSeason } from "./seasonSim.ts";
import { fitLogistic } from "./logistic.ts";
import { buildDeadlineView, type DeadlineView } from "./timing.ts";
import {
	expectedRosterPoints,
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
	lines: Map<string, StrengthPlayer & { availability: number }>;
	// Chance each player re-signs with each team, by retentionKey.
	retention: Map<string, number>;
	teams: Map<number, TeamOutlook>;
	// Roster points of the average team each season, so a hypothetical roster
	// can be put on the same scale.
	meanPoints: number[];
	titleCurves: Curve[];
	playoffCurves: Curve[];
	// For weighing a deal now against waiting, until the trade deadline passes.
	deadline?: DeadlineView;
};

// The season outlook year y is about.
export const outlookSeason = (
	outlook: Pick<LeagueOutlook, "season" | "offset">,
	y: number,
) => outlook.season + outlook.offset + y;

const projections = new Map<string, RatingsProjection>();

const seedFor = (pid: number, season: number) =>
	(pid * 2_654_435_761 + season * 40_503) % 2_147_483_647;

// The parts of a player's line that average over simulated careers.
export const lineMeasure = (ratings: MinimalPlayerRatings) => {
	const line = playerLine(ratings as unknown as ValueRatings);
	return [line.roleFree, line.offense, line.usage];
};

export const projectPlayer = (p: Player, season: number) => {
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
	return {
		...playerLine({
			...(projection.ratings[index] as unknown as ValueRatings),
			ovr: projection.ovr[index]!,
		}),
		roleFree: roleFree!,
		offense: offense!,
		usage: usage!,
		valueNoPot: projection.ovr[index]!,
		availability,
	};
};

export const seasonLine = (
	lines: LeagueOutlook["lines"],
	p: Player,
	season: number,
	offset: number,
	y: number,
	gamesRemaining: number,
	retention: number,
) => {
	const key = `${p.pid}:${season}:${y}:${p.injury.gamesRemaining}:${gamesRemaining}:${p.contract.exp}:${last(p.ratings).ovr}:${retention}`;
	let line = lines.get(key);
	if (!line) {
		line = computeSeasonLine(p, season, offset, y, gamesRemaining, retention);
		lines.set(key, line);
	}
	return line;
};

export const retentionKey = (pid: number, tid: number) => `${pid}:${tid}`;

export const retentionOf = (
	outlook: Pick<LeagueOutlook, "retention">,
	pid: number,
	tid: number,
) => outlook.retention.get(retentionKey(pid, tid)) ?? UNKNOWN_RETENTION;

// Works out, for any of these players not yet asked about, his chance of
// re-signing with this team.
export const ensureRetention = async (
	outlook: Pick<LeagueOutlook, "retention">,
	players: readonly Player[],
	tid: number,
) => {
	for (const p of players) {
		const key = retentionKey(p.pid, tid);
		if (!outlook.retention.has(key)) {
			const { probWilling } = await moodInfo(
				p.tid === tid ? p : { ...p, tid },
				tid,
			);
			outlook.retention.set(key, Math.round(probWilling * 1000) / 1000);
		}
	}
};

// Pick players are the same whoever holds the pick.
const pickLineCache = new Map<
	string,
	StrengthPlayer & { availability: number }
>();

export type PickLike = Pick<
	DraftPick,
	"originalTid" | "season" | "round" | "pick"
>;

// The players a team's picks are expected to have become by outlook year y.
// Picks in drafts beyond the last one that exists are the team's own.
const pickLines = (
	outlook: Omit<LeagueOutlook, "teams" | "titleCurves" | "playoffCurves">,
	tid: number,
	picks: readonly PickLike[],
	y: number,
) => {
	const season = outlookSeason(outlook, y);
	const numPicks = outlook.numRounds * outlook.numTeams;
	const all: PickLike[] = [...picks];
	for (let d = outlook.lastPickSeason + 1; d < season; d++) {
		for (let round = 1; round <= outlook.numRounds; round++) {
			all.push({ originalTid: tid, season: d, round, pick: 0 });
		}
	}
	const lines: (StrengthPlayer & { availability: number })[] = [];
	let salaries = 0;
	for (const dp of all) {
		if (typeof dp.season !== "number" || dp.season >= season) {
			continue;
		}
		let place;
		if (dp.pick > 0) {
			place = dp.pick - 1;
		} else {
			const places = outlook.draftPlace.get(dp.originalTid);
			const yDraft = dp.season - outlookSeason(outlook, 0);
			place =
				places?.[Math.min(OUTLOOK_YEARS, Math.max(0, yDraft))] ??
				Math.floor(outlook.numTeams / 2);
		}
		const slot = (dp.round - 1) * outlook.numTeams + place;
		const years = season - dp.season;
		const key = `${slot}:${numPicks}:${years}`;
		let line = pickLineCache.get(key);
		if (!line) {
			line = { ...pickPlayer({ slot, numPicks, years }), availability: 1 };
			pickLineCache.set(key, line);
		}
		lines.push(line);
		salaries += pickSalary({
			slot,
			round: dp.round,
			years,
			ovr: line.valueNoPot,
		});
	}
	return { lines, salaries };
};

// What a player is expected to be paid in a season of the outlook: his
// contract while it runs, then what he would re-sign for - each only as far
// as he is expected to be around.
const salaryIn = (
	p: Player,
	season: number,
	index: number,
	line: { valueNoPot: number; availability: number },
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
	const out: number[] = [];
	for (let y = 0; y <= OUTLOOK_YEARS; y++) {
		const lines = players.map((p) =>
			seasonLine(
				outlook.lines,
				p,
				outlook.season,
				outlook.offset,
				y,
				outlook.remaining.get(tid) ?? 0,
				retentionFor(p),
			),
		);
		const fromPicks = pickLines(outlook, tid, picks, y);
		lines.push(...fromPicks.lines);
		const season = outlookSeason(outlook, y);
		if (season >= outlook.firstRefillSeason) {
			let committed = fromPicks.salaries;
			for (const [i, p] of players.entries()) {
				committed += salaryIn(p, season, y + outlook.offset, lines[i]!);
			}
			for (const row of outlook.deadMoney.get(tid) ?? []) {
				if (row.exp >= season) {
					committed += row.amount;
				}
			}
			for (const signing of signingsWith(g.get("salaryCap") - committed)) {
				lines.push({ ...signing, availability: 1 });
			}
		}
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
) =>
	teamPoints(outlook, tid, players, picks, trading).map(
		(x, y) => x - outlook.meanPoints[y]!,
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
		lines: new Map<string, StrengthPlayer & { availability: number }>(),
		retention: new Map<string, number>(),
		meanPoints: [] as number[],
	};
	for (const t of teams) {
		await ensureRetention(base, rosters.get(t.tid)!, t.tid);
	}

	// Where each team's own pick lands, from its roster alone: worst first.
	const rosterOnly = new Map(
		teams.map((t) => [t.tid, teamPoints(base, t.tid, rosters.get(t.tid)!, [])]),
	);
	for (const t of teams) {
		base.draftPlace.set(t.tid, []);
	}
	for (let y = 0; y <= OUTLOOK_YEARS; y++) {
		const order = [...teams].sort(
			(a, b) => rosterOnly.get(a.tid)![y]! - rosterOnly.get(b.tid)![y]!,
		);
		for (const [i, t] of order.entries()) {
			base.draftPlace.get(t.tid)!.push(i);
		}
	}

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

		// Our read of a team is surer once games have been played, and less sure
		// the further out it reaches.
		const now = 1.5 + 2 * (1 - (y === 0 && inSeason ? progress : 0));
		const uncertainty = Math.sqrt(now ** 2 + (1.6 * y) ** 2);

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
		titleCurves.push(
			fitCurve(
				teams.map((t) => ({ x: strengthOf(t.tid), p: sim.get(t.tid)!.title })),
			),
		);
		playoffCurves.push(
			fitCurve(
				teams.map((t) => ({
					x: strengthOf(t.tid),
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
