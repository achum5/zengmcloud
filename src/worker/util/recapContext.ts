// WHAT A BEAT WRITER KNOWS BEFORE THE GAME STARTS.
//
// A box score says what happened. A recap worth reading says what it MEANT,
// and for that the writer arrives with the season in his head: the two clubs'
// last meeting, who is on the second night of a back-to-back, whether this
// was a season high, the streak a scorer carried in, the milestone a veteran
// was closing on, who is playing his first game since going down, what the
// win did to the standings, and who is up next.
//
// Every one of those is derivable from the games this season already stored,
// so this module derives them - as pure functions over plain game rows, so a
// test can hand-build a season in a few lines. getDayGamesForRecap wires
// them into the RecapGame the writer reads. Nothing here is written back
// anywhere; it is read fresh every time, so recapping a past day sees the
// season as it stood THAT day, not as it stands now.

// The slice of a stored game these derivations read. Optional everywhere a
// stored game can lack it (legacy rows have no day; a schedule row has no
// score), so the same shape covers both.
export type ContextGameRow = {
	gid: number;
	day?: number;
	playoffs?: boolean;
	won?: { tid: number; pts: number };
	lost?: { tid: number; pts: number };
	teams: { tid: number; pts?: number; players?: any[] }[];
};

export type ContextScheduleRow = {
	gid: number;
	day: number;
	homeTid: number;
	awayTid: number;
};

export type WonLost = { won: number; lost: number };

const dayOf = (g: { day?: number }) => g.day ?? 0;

const completed = (g: ContextGameRow) => !!g.won && !!g.lost;

const involves = (g: ContextGameRow, tid: number) =>
	g.teams[0]?.tid === tid || g.teams[1]?.tid === tid;

// Strictly earlier than the game (gid, day): the order the sim played them.
const playedBefore = (g: ContextGameRow, gid: number, day: number) =>
	dayOf(g) < day || (dayOf(g) === day && g.gid < gid);

const playedAfter = (g: ContextGameRow, gid: number, day: number) =>
	dayOf(g) > day || (dayOf(g) === day && g.gid > gid);

const byPlayed = (a: ContextGameRow, b: ContextGameRow) =>
	dayOf(a) - dayOf(b) || a.gid - b.gid;

// A team's completed games before a given game, oldest first.
export const teamGamesBefore = (
	tid: number,
	gid: number,
	day: number,
	games: readonly ContextGameRow[],
): ContextGameRow[] =>
	games
		.filter(
			(g) => completed(g) && involves(g, tid) && playedBefore(g, gid, day),
		)
		.sort(byPlayed);

const sideOf = (g: ContextGameRow, tid: number) => {
	const home = g.teams[0]?.tid === tid;
	const mine = home ? g.teams[0] : g.teams[1];
	const opp = home ? g.teams[1] : g.teams[0];
	return { home, mine, opp };
};

// ---------------------------------------------------------------- THE SERIES

export type SeasonSeries = {
	// This team's regular-season record against the opponent ENTERING the game.
	won: number;
	lost: number;
	// The most recent meeting, from this team's side.
	last?: {
		won: boolean;
		pts: number;
		oppPts: number;
		day: number;
		home: boolean;
	};
	// Meetings still to come after this one, when the schedule is known.
	left?: number;
};

export const seasonSeries = (
	tid: number,
	oppTid: number,
	gid: number,
	day: number,
	games: readonly ContextGameRow[],
	schedule?: readonly ContextScheduleRow[],
): SeasonSeries | undefined => {
	const prior = games
		.filter(
			(g) =>
				completed(g) &&
				!g.playoffs &&
				involves(g, tid) &&
				involves(g, oppTid) &&
				playedBefore(g, gid, day),
		)
		.sort(byPlayed);
	let left: number | undefined;
	if (schedule) {
		const later = games.filter(
			(g) =>
				!g.playoffs &&
				involves(g, tid) &&
				involves(g, oppTid) &&
				playedAfter(g, gid, day),
		).length;
		const upcoming = schedule.filter(
			(s) =>
				s.day > day &&
				((s.homeTid === tid && s.awayTid === oppTid) ||
					(s.homeTid === oppTid && s.awayTid === tid)),
		).length;
		left = later + upcoming;
	}
	if (prior.length === 0) {
		return left === undefined ? undefined : { won: 0, lost: 0, left };
	}
	const won = prior.filter((g) => g.won!.tid === tid).length;
	const lastGame = prior.at(-1)!;
	const { home, mine, opp } = sideOf(lastGame, tid);
	return {
		won,
		lost: prior.length - won,
		last: {
			won: lastGame.won!.tid === tid,
			pts:
				mine?.pts ??
				(lastGame.won!.tid === tid ? lastGame.won!.pts : lastGame.lost!.pts),
			oppPts:
				opp?.pts ??
				(lastGame.won!.tid === tid ? lastGame.lost!.pts : lastGame.won!.pts),
			day: dayOf(lastGame),
			home,
		},
		left,
	};
};

// ---------------------------------------------------------------- THE SCHEDULE

// Days since the team last played, entering this game. One is a back-to-back.
export const restEntering = (
	tid: number,
	gid: number,
	day: number,
	games: readonly ContextGameRow[],
): { daysSince: number; prevDay: number } | undefined => {
	const prev = teamGamesBefore(tid, gid, day, games).at(-1);
	if (!prev) {
		return undefined;
	}
	return { daysSince: day - dayOf(prev), prevDay: dayOf(prev) };
};

export type NextGame = {
	day: number;
	home: boolean;
	oppTid: number;
};

// The team's next game after this one: a later game already in the log (when
// recapping a past day) or the earliest schedule row, whichever comes first.
export const nextGameFor = (
	tid: number,
	gid: number,
	day: number,
	games: readonly ContextGameRow[],
	schedule: readonly ContextScheduleRow[] = [],
): NextGame | undefined => {
	let best:
		| { day: number; gid: number; home: boolean; oppTid: number }
		| undefined;
	const consider = (cand: {
		day: number;
		gid: number;
		home: boolean;
		oppTid: number;
	}) => {
		if (
			!best ||
			cand.day < best.day ||
			(cand.day === best.day && cand.gid < best.gid)
		) {
			best = cand;
		}
	};
	for (const g of games) {
		if (!involves(g, tid) || !playedAfter(g, gid, day)) {
			continue;
		}
		const { home, opp } = sideOf(g, tid);
		if (opp) {
			consider({ day: dayOf(g), gid: g.gid, home, oppTid: opp.tid });
		}
	}
	for (const s of schedule) {
		if (s.day <= day || (s.homeTid !== tid && s.awayTid !== tid)) {
			continue;
		}
		const home = s.homeTid === tid;
		consider({
			day: s.day,
			gid: s.gid,
			home,
			oppTid: home ? s.awayTid : s.homeTid,
		});
	}
	return best
		? { day: best.day, home: best.home, oppTid: best.oppTid }
		: undefined;
};

// ---------------------------------------------------------------- THE RECORDS

// Home and road regular-season records through a day.
export const homeAwayRecords = (
	tid: number,
	upToDay: number,
	games: readonly ContextGameRow[],
): { home: WonLost; away: WonLost } => {
	const home = { won: 0, lost: 0 };
	const away = { won: 0, lost: 0 };
	for (const g of games) {
		if (
			!completed(g) ||
			g.playoffs ||
			!involves(g, tid) ||
			dayOf(g) > upToDay
		) {
			continue;
		}
		const side = g.teams[0]?.tid === tid ? home : away;
		if (g.won!.tid === tid) {
			side.won += 1;
		} else {
			side.lost += 1;
		}
	}
	return { home, away };
};

// ---------------------------------------------------------------- SEASON HIGHS

export type TeamSeasonHighs = {
	// Games the team had played entering this one; the flags mean nothing
	// early, so the writer checks it.
	priorGames: number;
	// This game's points were the team's most of the season.
	pts: boolean;
	// The winner's margin was its biggest of the season.
	margin: boolean;
	// This game's points were the most ANY team had scored this season.
	leaguePts: boolean;
};

export const teamSeasonHighs = (
	tid: number,
	game: ContextGameRow,
	games: readonly ContextGameRow[],
): TeamSeasonHighs => {
	const day = dayOf(game);
	const { mine } = sideOf(game, tid);
	const pts = mine?.pts ?? 0;
	const won = game.won?.tid === tid;
	const margin =
		won && game.won && game.lost ? game.won.pts - game.lost.pts : 0;

	let priorGames = 0;
	let maxPts = 0;
	let maxMargin = 0;
	let leagueMax = 0;
	let leagueGames = 0;
	for (const g of games) {
		if (!completed(g) || g.playoffs || !playedBefore(g, game.gid, day)) {
			continue;
		}
		if (g.teams[0]?.tid === -1) {
			continue;
		}
		leagueGames += 1;
		leagueMax = Math.max(leagueMax, g.won!.pts);
		if (!involves(g, tid)) {
			continue;
		}
		priorGames += 1;
		const side = sideOf(g, tid).mine;
		maxPts = Math.max(maxPts, side?.pts ?? 0);
		if (g.won!.tid === tid) {
			maxMargin = Math.max(maxMargin, g.won!.pts - g.lost!.pts);
		}
	}
	return {
		priorGames,
		pts: priorGames > 0 && pts > maxPts,
		margin: won && priorGames > 0 && margin > maxMargin,
		leaguePts: leagueGames > 0 && pts > leagueMax,
	};
};

// ---------------------------------------------------------------- THE PLAYER

export type CountingTotals = {
	pts: number;
	reb: number;
	ast: number;
	tp: number;
	stl: number;
	blk: number;
};

export type PlayerEntering = {
	// Games with a box-score line before this one, this phase.
	gp: number;
	// The best single-game figures before this one.
	high: CountingTotals;
	totals: CountingTotals;
	// Consecutive games, ending with the previous one, at each bar.
	streaks: { twenty: number; thirty: number; doubleDouble: number };
};

const lineTotals = (row: Record<string, number>): CountingTotals => ({
	pts: row.pts ?? 0,
	reb: (row.orb ?? 0) + (row.drb ?? 0) + (row.reb ?? 0),
	ast: row.ast ?? 0,
	tp: row.tp ?? 0,
	stl: row.stl ?? 0,
	blk: row.blk ?? 0,
});

const zeroTotals = (): CountingTotals => ({
	pts: 0,
	reb: 0,
	ast: 0,
	tp: 0,
	stl: 0,
	blk: 0,
});

export const addTotals = (
	a: CountingTotals,
	b: CountingTotals,
): CountingTotals => ({
	pts: a.pts + b.pts,
	reb: a.reb + b.reb,
	ast: a.ast + b.ast,
	tp: a.tp + b.tp,
	stl: a.stl + b.stl,
	blk: a.blk + b.blk,
});

const isDoubleDouble = (t: CountingTotals) =>
	[t.pts, t.reb, t.ast, t.stl, t.blk].filter((v) => v >= 10).length >= 2;

export const playerEntering = (
	lines: readonly {
		day: number;
		gid: number;
		playoffs: boolean;
		row: Record<string, number>;
	}[],
	gid: number,
	day: number,
	playoffs: boolean,
): PlayerEntering => {
	const prior = lines
		.filter(
			(l) =>
				l.playoffs === playoffs &&
				(l.day < day || (l.day === day && l.gid < gid)),
		)
		.sort((a, b) => a.day - b.day || a.gid - b.gid);
	const high = zeroTotals();
	let totals = zeroTotals();
	for (const l of prior) {
		const t = lineTotals(l.row);
		totals = addTotals(totals, t);
		for (const key of Object.keys(high) as (keyof CountingTotals)[]) {
			high[key] = Math.max(high[key], t[key]);
		}
	}
	const streaks = { twenty: 0, thirty: 0, doubleDouble: 0 };
	let twentyOpen = true;
	let thirtyOpen = true;
	let ddOpen = true;
	for (let i = prior.length - 1; i >= 0; i--) {
		const t = lineTotals(prior[i]!.row);
		if (twentyOpen && t.pts >= 20) {
			streaks.twenty += 1;
		} else {
			twentyOpen = false;
		}
		if (thirtyOpen && t.pts >= 30) {
			streaks.thirty += 1;
		} else {
			thirtyOpen = false;
		}
		if (ddOpen && isDoubleDouble(t)) {
			streaks.doubleDouble += 1;
		} else {
			ddOpen = false;
		}
		if (!twentyOpen && !thirtyOpen && !ddOpen) {
			break;
		}
	}
	return { gp: prior.length, high, totals, streaks };
};

// The line the box score gives a player, as counting totals.
export const boxTotals = (p: {
	pts: number;
	reb: number;
	ast: number;
	tp: number;
	stl: number;
	blk: number;
}): CountingTotals => ({
	pts: p.pts,
	reb: p.reb,
	ast: p.ast,
	tp: p.tp,
	stl: p.stl,
	blk: p.blk,
});

// ---------------------------------------------------------------- MILESTONES

export type MilestoneStat = "pts" | "reb" | "ast" | "tp";

export type Milestone = {
	scope: "season" | "career";
	stat: MilestoneStat;
	// The round number passed, and the total after this game.
	mark: number;
	total: number;
};

// The largest multiple of `step` at or above `min` that `after` reaches and
// `before` had not.
export const crossedMark = (
	before: number,
	after: number,
	step: number,
	min: number,
): number | undefined => {
	if (after < min || after <= before) {
		return undefined;
	}
	const mark = Math.floor(after / step) * step;
	return mark >= min && mark > before ? mark : undefined;
};

const SEASON_STEPS: Record<MilestoneStat, { step: number; min: number }> = {
	pts: { step: 500, min: 500 },
	reb: { step: 500, min: 500 },
	ast: { step: 500, min: 500 },
	tp: { step: 100, min: 100 },
};

const CAREER_STEPS: Record<MilestoneStat, { step: number; min: number }> = {
	pts: { step: 1000, min: 1000 },
	reb: { step: 1000, min: 1000 },
	ast: { step: 1000, min: 1000 },
	tp: { step: 500, min: 500 },
};

// Points outrank the rest when two marks fall in one game, career outranks
// season, and one milestone per player per game is plenty.
const STAT_ORDER: MilestoneStat[] = ["pts", "reb", "ast", "tp"];

const milestoneIn = (
	scope: Milestone["scope"],
	before: CountingTotals,
	after: CountingTotals,
	steps: Record<MilestoneStat, { step: number; min: number }>,
): Milestone | undefined => {
	for (const stat of STAT_ORDER) {
		const { step, min } = steps[stat];
		const mark = crossedMark(before[stat], after[stat], step, min);
		if (mark !== undefined) {
			return { scope, stat, mark, total: after[stat] };
		}
	}
	return undefined;
};

export const seasonMilestone = (
	before: CountingTotals,
	after: CountingTotals,
) => milestoneIn("season", before, after, SEASON_STEPS);

export const careerMilestone = (
	before: CountingTotals,
	after: CountingTotals,
) => milestoneIn("career", before, after, CAREER_STEPS);

// Regular-season career totals from every season BEFORE the one given, off a
// player's stats rows. The current season is left out because its live row
// includes this game and everything after it; the entering totals cover it.
export const pastSeasonTotals = (
	player: { stats?: any[] },
	season: number,
): CountingTotals & { gp: number } => {
	let out: CountingTotals & { gp: number } = { ...zeroTotals(), gp: 0 };
	for (const row of player.stats ?? []) {
		if (!row || row.playoffs || row.season === season || row.season > season) {
			continue;
		}
		out = { ...addTotals(out, lineTotals(row)), gp: out.gp + (row.gp ?? 0) };
	}
	return out;
};

// ---------------------------------------------------------------- THE RETURN

// A player's first game back: the previous games his team played, walking
// backwards, in which he was on the roster, did not play, and was injured.
// Stops at the first game he played (or was not listed for), so a healthy
// scratch or a mid-season arrival never reads as a return.
export const returnFromAbsence = (
	pid: number,
	tid: number,
	gid: number,
	day: number,
	games: readonly ContextGameRow[],
): { games: number; type: string } | undefined => {
	const prior = teamGamesBefore(tid, gid, day, games);
	let missed = 0;
	let type = "";
	for (let i = prior.length - 1; i >= 0; i--) {
		const side = sideOf(prior[i]!, tid).mine;
		const entry = side?.players?.find((p: any) => p?.pid === pid);
		if (!entry || (entry.min ?? 0) > 0 || !(entry.injury?.gamesRemaining > 0)) {
			break;
		}
		missed += 1;
		if (!type) {
			type = String(entry.injury.type ?? "injury");
		}
	}
	return missed >= 3 ? { games: missed, type } : undefined;
};

// ---------------------------------------------------------------- STANDINGS

export type StandingRow = {
	tid?: number;
	abbrev: string;
	name?: string;
	rank: number;
	won: number;
	lost: number;
	gb: number;
};

export type Standing = {
	conf: string;
	rank: number;
	gb: number;
	teams: number;
	won: number;
	lost: number;
	// When first: the gap to second. When not: the nickname of the leader.
	lead?: number;
	leader?: string;
};

export const standingOf = (
	standings: { confs: { name: string; teams: StandingRow[] }[] } | undefined,
	tid: number,
): Standing | undefined => {
	if (!standings) {
		return undefined;
	}
	for (const conf of standings.confs) {
		const row = conf.teams.find((t) => t.tid === tid);
		if (row) {
			const out: Standing = {
				conf: conf.name,
				rank: row.rank,
				gb: row.gb,
				teams: conf.teams.length,
				won: row.won,
				lost: row.lost,
			};
			const first = conf.teams.find((t) => t.rank === 1);
			const second = conf.teams.find((t) => t.rank === 2);
			if (row.rank === 1 && second) {
				out.lead = second.gb;
			} else if (row.rank > 1 && first?.name) {
				out.leader = first.name;
			}
			return out;
		}
	}
	return undefined;
};
