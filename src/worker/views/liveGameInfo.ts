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
import { g, helpers } from "../util/index.ts";
import type { Game } from "../../common/types.ts";

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

export type GamecastInfo = {
	series: SeriesGame[];
	standings: { name: string; teams: StandingsTeam[] }[];
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
			...(now
				? { now: true }
				: before(p, game)
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

export const gamecastInfo = async (
	game: Game,
): Promise<GamecastInfo | undefined> => {
	const tids = [game.teams[0].tid, game.teams[1].tid] as const;
	if (tids[0] < 0 || tids[1] < 0 || (game as any).exhibition) {
		return undefined;
	}
	const season = game.season;
	const played = (await idb.merged.games.getAll({ season })) as Played[];
	const scheduled =
		season === g.get("season") ? await idb.cache.schedule.getAll() : [];
	const series = seriesOf(game, played, scheduled);

	const standings: GamecastInfo["standings"] = [];
	if (!game.playoffs) {
		const teams = await idb.cache.teams.getAll();
		const teamSeasons = await idb.merged.teamSeasons.getAll({ season });
		const divs = g.get("divs", season);
		// The two teams' division(s), home's first.
		const dids: number[] = [];
		for (const tid of tids) {
			const did = teamSeasons.find((ts) => ts.tid === tid)?.did;
			if (did !== undefined && !dids.includes(did)) {
				dids.push(did);
			}
		}
		for (const did of dids) {
			const rows: StandingsTeam[] = [];
			for (const ts of teamSeasons) {
				if (ts.did !== did) {
					continue;
				}
				const t = teams.find((t2) => t2.tid === ts.tid);
				const row: StandingsTeam = {
					tid: ts.tid,
					abbrev: ts.abbrev ?? t?.abbrev ?? "",
					region: ts.region ?? t?.region ?? "",
					name: ts.name ?? t?.name ?? "",
					imgURL: ts.imgURL ?? t?.imgURL,
					imgURLSmall: ts.imgURLSmall ?? t?.imgURLSmall,
					won: ts.won,
					lost: ts.lost,
					tied: ts.tied ?? 0,
					otl: ts.otl ?? 0,
					gb: 0,
					streak: ts.streak ?? 0,
				};
				// This game, taken back off the two teams that played it.
				const i = tids.indexOf(ts.tid as any);
				const thisGame = played.find((p) => p.gid === game.gid);
				if (i >= 0 && thisGame) {
					const us = thisGame.teams[i]!.pts;
					const them = thisGame.teams[i === 0 ? 1 : 0]!.pts;
					if (us > them) {
						row.won = Math.max(0, row.won - 1);
					} else if (us < them) {
						if (game.overtimes > 0 && row.otl > 0) {
							row.otl -= 1;
						} else {
							row.lost = Math.max(0, row.lost - 1);
						}
					} else {
						row.tied = Math.max(0, row.tied - 1);
					}
					row.streak = streakBefore(ts.tid, game, played);
				}
				rows.push(row);
			}
			const pct = (r: StandingsTeam) => {
				const gp = r.won + r.lost + r.tied + r.otl;
				return gp > 0 ? (r.won + 0.5 * r.tied) / gp : 0;
			};
			rows.sort((a, b) => pct(b) - pct(a) || b.won - a.won);
			for (const r of rows) {
				r.gb = rows[0] ? helpers.gb(rows[0], r) : 0;
			}
			standings.push({
				name: divs.find((d) => d.did === did)?.name ?? "Division",
				teams: rows,
			});
		}
	}

	return { series, standings };
};
