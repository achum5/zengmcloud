// WHAT THE GAMECAST SAYS ABOUT A GAME BEYOND THE GAME ITSELF: this season's
// meetings between the two teams, and their division standings - both as
// they stood at the tip.
//
// A live game is watched after it has been played: its result is already in
// the games store, the two teams' records and their streaks. None of that
// can show, or the standings would give the ending away. So this game, and
// any meeting after it, is listed without a score, and the two teams'
// records and streaks are taken back to before it.

import { idb } from "../db/index.ts";
import { season as seasonCore } from "../core/index.ts";
import { g, helpers } from "../util/index.ts";
import type { Game, PlayoffSeries } from "../../common/types.ts";
import {
	expectedTotalFromRates,
	leagueAvgGameTotal,
	toHalfPointLine,
	type ScoringRate,
	type TotalsPrior,
} from "../../common/sportsbookOdds.ts";
import { getTotalsPrior } from "../core/sportsbook/gameLines.ts";
import { BASKETBALL_PLAYOFF_TOTAL_FACTOR } from "../../common/getGameSpread.ts";

export type SeriesGame = {
	gid: number;
	day?: number;
	// Box score order, as the game itself: 0 home, 1 away.
	tids: [number, number];
	// The final score, home first - only for a meeting before this one.
	pts?: [number, number];
	// The game being watched.
	now?: boolean;
};

export type StandingsTeam = {
	tid: number;
	// Where it stands in its conference.
	rank: number;
	abbrev: string;
	region: string;
	name: string;
	imgURL?: string;
	imgURLSmall?: string;
	won: number;
	lost: number;
	tied: number;
	otl: number;
	gb: number;
	// Games won (+) or lost (-) in a row.
	streak: number;
};

// One of a team's games: who against, home or away, the score (theirs
// first) - and whether it is the game on the page.
export type RecentGame = {
	gid: number;
	day?: number;
	opp: number;
	oppAbbrev?: string;
	home: boolean;
	pts: [number, number];
	now?: boolean;
};

// A playoff game's bracket, as it stood: each round's series, each team's
// seed and its wins in that series so far - by the tip at a live game (this
// one not counted), that night on its box score. A later round's team only
// once it had won its way there by then.
export type BracketTeam = {
	tid: number;
	seed: number;
	abbrev: string;
	imgURL?: string;
	imgURLSmall?: string;
	won: number;
};
export type BracketSeries = {
	teams: [BracketTeam | undefined, BracketTeam | undefined];
	cid?: number;
	// The series this game is in.
	now?: boolean;
};
export type BracketRound = {
	name: string;
	// Wins that take a series.
	toWin: number;
	series: BracketSeries[];
};

// A play-in game: each team's seed (by tid), and which of the play-in's
// games it is (0 the 7th seed against the 8th, 1 the 9th against the 10th,
// 2 the last one, for the final playoff spot - as PlayInTournament has them).
export type PlayInGame = {
	seeds: Record<number, number>;
	stage: number;
};

export type GamecastInfo = {
	series: SeriesGame[];
	bracket?: BracketRound[];
	playIn?: PlayInGame;
	// The two teams' conferences (one, when they share it), as they stood.
	standings: { name: string; teams: StandingsTeam[] }[];
	// Each team's last few games, latest first.
	recent: { tid: number; games: RecentGame[] }[];
	// The over/under at the tip (see totalAsOf).
	total?: number;
	// Each team's leading scorer going in (see leadersOf) - a live game's
	// only. Box score order: home first.
	leaders?: [GameLeader, GameLeader];
	// Each team's numbers going in, for the pregame show's matchup board (see
	// tapeOf) - a live game's only. Box score order: home first.
	tape?: [TeamTape | undefined, TeamTape | undefined];
};

// A team's numbers going into a game: a game's points for and against,
// rebounds and assists, its three-point shooting, and its last ten.
export type TeamTape = {
	tid: number;
	gp: number;
	pts: number;
	opp: number;
	trb: number;
	ast: number;
	// Undefined with no threes tried.
	tpPct?: number;
	last10: [number, number];
};

// A team's leading scorer going into a game, and his averages then.
export type GameLeader = {
	tid: number;
	pid: number;
	name: string;
	pts: number;
	trb: number;
	ast: number;
	gp: number;
};

type Played = Pick<Game, "gid" | "day" | "playoffs" | "teams">;
type Scheduled = {
	gid: number;
	day?: number;
	homeTid: number;
	awayTid: number;
};

// Before or after the game being watched: by day, then by gid (games are
// numbered as they're scheduled).
const before = (
	a: { day?: number; gid: number },
	b: { day?: number; gid: number },
) =>
	a.day !== undefined && b.day !== undefined && a.day !== b.day
		? a.day < b.day
		: a.gid < b.gid;

export const seriesOf = (
	game: Played,
	played: readonly Played[],
	scheduled: readonly Scheduled[],
	// A finished game's page (the box score): every meeting played so far
	// with its score, this one too - nothing to hold back.
	final = false,
): SeriesGame[] => {
	const tids = [game.teams[0].tid, game.teams[1].tid];
	const between = (a: number, b: number) =>
		(a === tids[0] && b === tids[1]) || (a === tids[1] && b === tids[0]);
	const out: SeriesGame[] = [];
	for (const p of played) {
		if (
			!!p.playoffs !== !!game.playoffs ||
			!between(p.teams[0].tid, p.teams[1].tid)
		) {
			continue;
		}
		const now = p.gid === game.gid;
		out.push({
			gid: p.gid,
			day: p.day,
			tids: [p.teams[0].tid, p.teams[1].tid],
			...(now ? { now: true } : {}),
			...((now && final) || (!now && (final || before(p, game)))
				? { pts: [p.teams[0].pts, p.teams[1].pts] as [number, number] }
				: {}),
		});
	}
	if (!game.playoffs) {
		for (const s of scheduled) {
			if (between(s.homeTid, s.awayTid) && !out.some((x) => x.gid === s.gid)) {
				out.push({ gid: s.gid, day: s.day, tids: [s.homeTid, s.awayTid] });
			}
		}
	}
	// (The game being watched is in the games store already; a follower's
	// device may not have it yet.)
	if (!out.some((x) => x.now)) {
		out.push({
			gid: game.gid,
			day: game.day,
			tids: [game.teams[0].tid, game.teams[1].tid],
			now: true,
		});
	}
	return out.sort((a, b) => (before(a, b) ? -1 : before(b, a) ? 1 : 0));
};

// A team's streak going into a game: its results before it, latest first.
export const streakBefore = (
	tid: number,
	game: Played,
	played: readonly Played[],
): number => {
	const results = played
		.filter(
			(p) =>
				!p.playoffs &&
				before(p, game) &&
				(p.teams[0].tid === tid || p.teams[1].tid === tid),
		)
		.sort((a, b) => (before(a, b) ? 1 : -1))
		.map((p) => {
			const us = p.teams[0].tid === tid ? 0 : 1;
			const diff = p.teams[us].pts - p.teams[us === 0 ? 1 : 0].pts;
			return Math.sign(diff);
		});
	let streak = 0;
	for (const r of results) {
		if (r === 0 || (streak !== 0 && Math.sign(streak) !== r)) {
			break;
		}
		streak += r;
	}
	return streak;
};

// The standings as they stood: as of the tip (the live game - the day's
// games not yet played) or that night (its box score - the day's games
// all in), worked out from the season's games, so an old game's page shows
// the table as it was then, not as it is now. Ties counted half.
export const standingsAsOf = (
	game: Played,
	played: readonly Played[],
	final: boolean,
): Map<number, { won: number; lost: number; tied: number; streak: number }> => {
	const sameDay = (p: Played) =>
		p.day !== undefined && game.day !== undefined && p.day === game.day;
	const counts = (p: Played) =>
		!p.playoffs &&
		(game.playoffs
			? true
			: final
				? p.gid === game.gid || before(p, game) || sameDay(p)
				: before(p, game) && !sameDay(p));
	const games = played
		.filter(counts)
		.sort((a, b) => (before(a, b) ? -1 : before(b, a) ? 1 : 0));
	const out = new Map<
		number,
		{ won: number; lost: number; tied: number; streak: number }
	>();
	const row = (tid: number) => {
		let r = out.get(tid);
		if (!r) {
			r = { won: 0, lost: 0, tied: 0, streak: 0 };
			out.set(tid, r);
		}
		return r;
	};
	for (const p of games) {
		for (const i of [0, 1] as const) {
			const r = row(p.teams[i].tid);
			const diff = p.teams[i].pts - p.teams[i === 0 ? 1 : 0].pts;
			if (diff > 0) {
				r.won += 1;
				r.streak = r.streak > 0 ? r.streak + 1 : 1;
			} else if (diff < 0) {
				r.lost += 1;
				r.streak = r.streak < 0 ? r.streak - 1 : -1;
			} else {
				r.tied += 1;
				r.streak = 0;
			}
		}
	}
	return out;
};

// THE OVER/UNDER AT THE TIP, as the sportsbook set it (see gameLines.ts):
// from each team's regular-season scoring going into the game's day, worked
// out from the season's games - so a live game, or an old one on replay,
// quotes the line it was played under. (The spread needs none of this: the
// sim keeps it on the game - Game.spread.) Undefined when the games can't
// account for a team's record - an old season's box scores deleted - rather
// than quote a line off part of a season.
export const totalAsOf = (
	game: Played,
	played: readonly Played[],
	// Each side's regular-season games going into it (its record), box
	// score order - what the games found have to add up to.
	gpAtTip: [number, number],
	// What the season's totals start from (see getTotalsPrior).
	prior: TotalsPrior,
): number | undefined => {
	const sameDay = (p: Played) =>
		p.day !== undefined && game.day !== undefined && p.day === game.day;
	const totals = new Map<number, ScoringRate>();
	for (const p of played) {
		if (
			p.playoffs ||
			(p as { exhibition?: boolean }).exhibition ||
			!before(p, game) ||
			sameDay(p)
		) {
			continue;
		}
		for (const i of [0, 1] as const) {
			const tid = p.teams[i].tid;
			if (tid < 0) {
				continue;
			}
			let r = totals.get(tid);
			if (!r) {
				r = { gp: 0, pts: 0, oppPts: 0 };
				totals.set(tid, r);
			}
			r.gp += 1;
			r.pts += p.teams[i].pts;
			r.oppPts += p.teams[i === 0 ? 1 : 0].pts;
		}
	}
	// Per game, as the sportsbook reads a team's stats.
	const rates = new Map<number, ScoringRate>();
	for (const [tid, r] of totals) {
		rates.set(tid, { gp: r.gp, pts: r.pts / r.gp, oppPts: r.oppPts / r.gp });
	}
	const none: ScoringRate = { gp: 0, pts: 0, oppPts: 0 };
	const home = rates.get(game.teams[0].tid) ?? none;
	const away = rates.get(game.teams[1].tid) ?? none;
	if (home.gp !== gpAtTip[0] || away.gp !== gpAtTip[1]) {
		return undefined;
	}
	return toHalfPointLine(
		expectedTotalFromRates({
			home,
			away,
			leagueAvgTotal: leagueAvgGameTotal(rates.values(), prior),
			factor:
				game.playoffs && __SPORT === "basketball"
					? BASKETBALL_PLAYOFF_TOTAL_FACTOR
					: 1,
		}),
	);
};

// A team's last few games, latest first: up to and including this one on
// its box score, before it at the tip.
export const recentOf = (
	tid: number,
	game: Played,
	played: readonly Played[],
	final: boolean,
	n = 5,
): RecentGame[] =>
	played
		.filter(
			(p) =>
				(p.teams[0].tid === tid || p.teams[1].tid === tid) &&
				(before(p, game) || (final && p.gid === game.gid)),
		)
		.sort((a, b) => (before(a, b) ? 1 : before(b, a) ? -1 : 0))
		.slice(0, n)
		.map((p) => {
			const us = p.teams[0].tid === tid ? 0 : 1;
			const them = us === 0 ? 1 : 0;
			return {
				gid: p.gid,
				day: p.day,
				opp: p.teams[them].tid,
				home: us === 0,
				pts: [p.teams[us].pts, p.teams[them].pts] as [number, number],
				...(p.gid === game.gid ? { now: true } : {}),
			};
		});

// EACH TEAM'S LEADING SCORER GOING IN, for the key matchup in the pregame
// show: of the men in its box score, the one scoring the most a game in the
// season's games before this one (wherever he played them), of those who
// had played at least half as many as the most of them. A playoff game, in
// the playoffs - or the season's, if either team had played none yet. None
// for the first night (or a team none of whose men had played).
export const leadersOf = (
	game: Played,
	played: readonly Played[],
): [GameLeader, GameLeader] | undefined => {
	const ours = new Set<number>();
	for (const t of game.teams) {
		for (const p of t.players ?? []) {
			ours.add(p.pid);
		}
	}
	const sums = (playoffs: boolean) => {
		const out = new Map<
			number,
			{ pts: number; trb: number; ast: number; gp: number }
		>();
		for (const gm of played) {
			if (
				gm.gid === game.gid ||
				!!gm.playoffs !== playoffs ||
				!before(gm, game)
			) {
				continue;
			}
			for (const t of gm.teams) {
				for (const p of t.players ?? []) {
					if (!ours.has(p.pid) || !((p.min ?? 0) > 0)) {
						continue;
					}
					const s = out.get(p.pid) ?? { pts: 0, trb: 0, ast: 0, gp: 0 };
					s.pts += p.pts ?? 0;
					s.trb += (p.orb ?? 0) + (p.drb ?? 0);
					s.ast += p.ast ?? 0;
					s.gp += 1;
					out.set(p.pid, s);
				}
			}
		}
		return out;
	};
	const pick = (
		stats: ReturnType<typeof sums>,
	): [GameLeader, GameLeader] | undefined => {
		const out: GameLeader[] = [];
		for (const t of game.teams) {
			const men = (t.players ?? []).flatMap((p) => {
				const s = stats.get(p.pid);
				return s && s.gp > 0 ? [{ p, s }] : [];
			});
			const most = Math.max(0, ...men.map((m) => m.s.gp));
			let best: (typeof men)[number] | undefined;
			for (const m of men) {
				if (
					m.s.gp * 2 >= most &&
					(!best || m.s.pts / m.s.gp > best.s.pts / best.s.gp)
				) {
					best = m;
				}
			}
			if (!best) {
				return undefined;
			}
			const { p, s } = best;
			out.push({
				tid: t.tid,
				pid: p.pid,
				name: p.name ?? "",
				pts: s.pts / s.gp,
				trb: s.trb / s.gp,
				ast: s.ast / s.gp,
				gp: s.gp,
			});
		}
		return out.length === 2 ? [out[0]!, out[1]!] : undefined;
	};
	return (game.playoffs ? pick(sums(true)) : undefined) ?? pick(sums(false));
};

// EACH TEAM'S NUMBERS GOING IN, for the matchup board in the pregame show:
// over its games this season before this one (the day's others left out,
// as the standings leave them - see standingsAsOf), the playoffs' in the
// playoffs once it has played one there. Undefined for a team with none.
export const tapeOf = (
	game: Played,
	played: readonly Played[],
): [TeamTape | undefined, TeamTape | undefined] => {
	const sameDay = (p: Played) =>
		p.day !== undefined && game.day !== undefined && p.day === game.day;
	const one = (tid: number): TeamTape | undefined => {
		const mine = (playoffs: boolean) =>
			played
				.filter(
					(p) =>
						p.gid !== game.gid &&
						!!p.playoffs === playoffs &&
						(p.teams[0].tid === tid || p.teams[1].tid === tid) &&
						before(p, game) &&
						!sameDay(p),
				)
				.sort((a, b) => (before(a, b) ? 1 : before(b, a) ? -1 : 0));
		const games =
			(game.playoffs ? mine(true) : []).length > 0 ? mine(true) : mine(false);
		if (games.length === 0) {
			return undefined;
		}
		let pts = 0;
		let opp = 0;
		let trb = 0;
		let ast = 0;
		let tp = 0;
		let tpa = 0;
		const last10: [number, number] = [0, 0];
		games.forEach((p, k) => {
			const us = p.teams[0].tid === tid ? 0 : 1;
			const t = p.teams[us];
			const o = p.teams[us === 0 ? 1 : 0];
			pts += t.pts;
			opp += o.pts;
			trb += (t.orb ?? 0) + (t.drb ?? 0);
			ast += t.ast ?? 0;
			tp += t.tp ?? 0;
			tpa += t.tpa ?? 0;
			if (k < 10 && t.pts !== o.pts) {
				last10[t.pts > o.pts ? 0 : 1] += 1;
			}
		});
		const gp = games.length;
		return {
			tid,
			gp,
			pts: pts / gp,
			opp: opp / gp,
			trb: trb / gp,
			ast: ast / gp,
			...(tpa > 0 ? { tpPct: (100 * tp) / tpa } : {}),
			last10,
		};
	};
	return [one(game.teams[0].tid), one(game.teams[1].tid)];
};

// The bracket as it stood (see BracketRound), from the playoff series and
// the season's games: each series' wins counted from the games themselves,
// so an old game's page, or a live one, shows that night's - not today's.
export const bracketAsOf = (
	game: Played,
	played: readonly Played[],
	ps: Pick<PlayoffSeries, "series">,
	info: {
		toWin: (round: number) => number;
		roundName: (round: number) => string;
		team: (tid: number) => Omit<BracketTeam, "seed" | "won">;
	},
	final: boolean,
): BracketRound[] | undefined => {
	const tids = [game.teams[0].tid, game.teams[1].tid];
	const counted = played.filter(
		(p) => p.playoffs && (before(p, game) || (final && p.gid === game.gid)),
	);
	const winsOver = (a: number, b: number) =>
		counted.filter((p) => {
			const [h, v] = p.teams;
			return (
				(h.tid === a && v.tid === b && h.pts > v.pts) ||
				(v.tid === a && h.tid === b && v.pts > h.pts)
			);
		}).length;
	const rounds: BracketRound[] = [];
	// Who had won their way into each round by then.
	let through: Set<number> | undefined;
	ps.series.forEach((matchups, r) => {
		const toWin = info.toWin(r);
		const next = new Set<number>();
		const series: BracketSeries[] = [];
		for (const m of matchups) {
			const side = (
				x: typeof m.home | undefined,
				y: typeof m.home | undefined,
			) =>
				x && (through === undefined || through.has(x.tid))
					? {
							...info.team(x.tid),
							tid: x.tid,
							seed: x.seed,
							won: y ? winsOver(x.tid, y.tid) : 0,
						}
					: undefined;
			const home = side(m.home, m.away);
			const away = side(m.away, m.home);
			if (!home && !away) {
				continue;
			}
			for (const t of [home, away]) {
				if (t && (t.won >= toWin || (m.away === undefined && t === home))) {
					next.add(t.tid);
				}
			}
			const now =
				m.away !== undefined &&
				tids.includes(m.home.tid) &&
				tids.includes(m.away.tid);
			series.push({
				teams: [home, away],
				cid: m.home.cid,
				...(now ? { now: true } : {}),
			});
		}
		through = next;
		if (series.length > 0) {
			rounds.push({ name: info.roundName(r), toWin, series });
		}
	});
	return rounds.length > 0 ? rounds : undefined;
};

export const gamecastInfo = async (
	game: Game,
	// The box score's: the meetings with their scores, and the standings as
	// they stood that night - not as they stood at the tip.
	final = false,
): Promise<GamecastInfo | undefined> => {
	const tids = [game.teams[0].tid, game.teams[1].tid] as const;
	if (tids[0] < 0 || tids[1] < 0 || (game as any).exhibition) {
		return undefined;
	}
	const season = game.season;
	const played = (await idb.merged.games.getAll({ season })) as Played[];
	const scheduled =
		season === g.get("season") ? await idb.cache.schedule.getAll() : [];
	const series = seriesOf(game, played, scheduled, final);

	const teams = await idb.cache.teams.getAll();
	const teamSeasons = await idb.merged.teamSeasons.getAll({ season });
	const confs = g.get("confs", season);
	const records = standingsAsOf(game, played, final);
	// The two teams' conference(s), home's first - once, if they share it.
	const cids: number[] = [];
	for (const tid of tids) {
		const cid = teamSeasons.find((ts) => ts.tid === tid)?.cid;
		if (cid !== undefined && !cids.includes(cid)) {
			cids.push(cid);
		}
	}
	const standings: GamecastInfo["standings"] = [];
	for (const cid of cids) {
		const rows: StandingsTeam[] = [];
		for (const ts of teamSeasons) {
			if (ts.cid !== cid) {
				continue;
			}
			const t = teams.find((t2) => t2.tid === ts.tid);
			const r = records.get(ts.tid) ?? { won: 0, lost: 0, tied: 0, streak: 0 };
			rows.push({
				tid: ts.tid,
				rank: 0,
				abbrev: ts.abbrev ?? t?.abbrev ?? "",
				region: ts.region ?? t?.region ?? "",
				name: ts.name ?? t?.name ?? "",
				imgURL: ts.imgURL ?? t?.imgURL,
				imgURLSmall: ts.imgURLSmall ?? t?.imgURLSmall,
				won: r.won,
				lost: r.lost,
				tied: r.tied,
				otl: 0,
				gb: 0,
				streak: r.streak,
			});
		}
		const pct = (r: StandingsTeam) => {
			const gp = r.won + r.lost + r.tied;
			return gp > 0 ? (r.won + 0.5 * r.tied) / gp : 0;
		};
		rows.sort((a, b) => pct(b) - pct(a) || b.won - a.won || a.lost - b.lost);
		rows.forEach((r, k) => {
			r.rank = k + 1;
			r.gb = helpers.gb(rows[0]!, r);
		});
		standings.push({
			name: confs.find((c) => c.cid === cid)?.name ?? "Conference",
			teams: rows,
		});
	}

	const abbrevOf = (tid: number) =>
		teamSeasons.find((ts) => ts.tid === tid)?.abbrev ??
		teams.find((t) => t.tid === tid)?.abbrev;
	const recent = tids.map((tid) => ({
		tid,
		games: recentOf(tid, game, played, final).map((gm) => ({
			...gm,
			oppAbbrev: abbrevOf(gm.opp),
		})),
	}));

	// A playoff game: the bracket, not the standings.
	let bracket: BracketRound[] | undefined;
	let playIn: PlayInGame | undefined;
	if (game.playoffs) {
		const ps = await idb.merged.playoffSeries.get(season);
		if (ps) {
			playIn = playInOf(game, ps.playIns);
			const numRounds = g.get("numGamesPlayoffSeries", season).length;
			const byConf = await seasonCore.getPlayoffsByConf(season);
			bracket = bracketAsOf(
				game,
				played,
				ps,
				{
					toWin: (r) => helpers.numGamesToWinRound(r, season),
					roundName: (r) =>
						helpers.upperCaseFirstLetter(
							helpers.playoffRoundName(r, numRounds, byConf),
						),
					team: (tid) => {
						const ts = teamSeasons.find((x) => x.tid === tid);
						const t = teams.find((x) => x.tid === tid);
						return {
							tid,
							abbrev: ts?.abbrev ?? t?.abbrev ?? "",
							imgURL: ts?.imgURL ?? t?.imgURL,
							imgURLSmall: ts?.imgURLSmall ?? t?.imgURLSmall,
						};
					},
				},
				final,
			);
		}
	}

	// The over/under, for a live game's header and a finished game's banner.
	// Its record as it went in: the game's own is written after it, this one
	// counted - in the regular season.
	const gpAtTip = game.teams.map((t) =>
		t.won === undefined || t.lost === undefined
			? -1
			: t.won + t.lost + (t.tied ?? 0) + (t.otl ?? 0) - (game.playoffs ? 0 : 1),
	) as [number, number];
	const total = totalAsOf(
		game,
		played,
		gpAtTip,
		await getTotalsPrior(game.season),
	);
	const leaders = final ? undefined : leadersOf(game, played);
	const tape = final ? undefined : tapeOf(game, played);

	return {
		series,
		standings,
		recent,
		...(bracket ? { bracket } : {}),
		...(playIn ? { playIn } : {}),
		...(total !== undefined ? { total } : {}),
		...(leaders ? { leaders } : {}),
		...(tape && (tape[0] || tape[1]) ? { tape } : {}),
	};
};

// The play-in game this one is, if it is one: the matchup between these
// two teams - this very game, where its games are known.
export const playInOf = (
	game: Pick<Game, "gid" | "teams">,
	playIns: PlayoffSeries["playIns"],
): PlayInGame | undefined => {
	const tids = [game.teams[0].tid, game.teams[1].tid];
	for (const tournament of playIns ?? []) {
		for (const [stage, m] of tournament.entries()) {
			if (
				!tids.includes(m.home.tid) ||
				!tids.includes(m.away.tid) ||
				(m.gids !== undefined &&
					m.gids.length > 0 &&
					!m.gids.includes(game.gid))
			) {
				continue;
			}
			return {
				seeds: { [m.home.tid]: m.home.seed, [m.away.tid]: m.away.seed },
				stage,
			};
		}
	}
	return undefined;
};
