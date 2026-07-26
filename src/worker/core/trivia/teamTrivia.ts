import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import { getSearchList, getTriviaPool } from "./pool.ts";

// Team Trivia, ported from ZenGM Grids' team-trivia page: a random
// team-season is drawn and the player works through rounds - name the
// roster, then with hints, then pick each stat leader, guess the win total
// within a window, and pick the playoff finish. All round/scoring flow lives
// in the UI; this builds one round's data bundle.

export type TeamTriviaRoster = {
	pid: number;
	name: string;
	pos: string;
	age: number;
	gp: number;
	jerseyNumber: string | undefined;
	ppg: number;
	rpg: number;
	apg: number;
	spg: number;
	bpg: number;
	// Season totals, for leader determination display.
	pts: number;
	trb: number;
	ast: number;
	stl: number;
	blk: number;
};

export type TeamTriviaRound = {
	season: number;
	team: { tid: number; label: string; abbrev: string };
	roster: TeamTriviaRoster[];
	// pid of the team leader in each stat (by season total).
	leaders: { pts: number; trb: number; ast: number; stl: number; blk: number };
	wins: { actual: number; games: number; window: number };
	// Absent when the season's playoffs haven't happened/finished yet.
	playoffs?: { options: string[]; answerIndex: number };
	searchList: { pid: number; name: string; years: string }[];
};

const round1 = (x: number) => Math.round(x * 10) / 10;

export const generateTeamTriviaRound = async (): Promise<
	TeamTriviaRound | undefined
> => {
	const pool = await getTriviaPool();
	const currentSeason = g.get("season");
	const playoffsDone = g.get("phase") > PHASE.PLAYOFFS;

	// Roster sizes per (season, tid), so only real team-seasons are drawn.
	const rosterCount = new Map<string, number>();
	for (const p of pool.players) {
		for (const r of p.rows) {
			if (r.gp > 0) {
				const key = `${r.season}-${r.tid}`;
				rosterCount.set(key, (rosterCount.get(key) ?? 0) + 1);
			}
		}
	}

	const candidates: { season: number; tid: number }[] = [];
	for (const [key, count] of rosterCount) {
		if (count < 5) {
			continue;
		}
		const [seasonStr, tidStr] = key.split("-");
		const season = Number(seasonStr);
		const tid = Number(tidStr);
		// The current season is only quizzable once its story is finished.
		if (season === currentSeason && !playoffsDone) {
			continue;
		}
		candidates.push({ season, tid });
	}
	if (candidates.length === 0) {
		return undefined;
	}

	// Up to a few draws in case a candidate's team-season row is missing.
	for (let attempt = 0; attempt < 10; attempt++) {
		const { season, tid } =
			candidates[Math.floor(Math.random() * candidates.length)]!;

		const teamSeasons = await idb.getCopies.teamSeasons(
			{ season },
			"noCopyCache",
		);
		const ts = teamSeasons.find((row) => row.tid === tid);
		if (!ts) {
			continue;
		}

		const games = ts.won + ts.lost + (ts.tied ?? 0) + ((ts as any).otl ?? 0);
		if (games <= 0) {
			continue;
		}

		const team = await idb.cache.teams.get(tid);
		const region = ts.region || team?.region || "";
		const name = ts.name || team?.name || "";
		const abbrev = ts.abbrev || team?.abbrev || "???";

		const roster: TeamTriviaRoster[] = [];
		for (const p of pool.players) {
			for (const r of p.rows) {
				if (r.season === season && r.tid === tid && r.gp > 0) {
					roster.push({
						pid: p.pid,
						name: p.name,
						pos: r.pos,
						age: season - p.bornYear,
						gp: r.gp,
						jerseyNumber: r.jerseyNumber,
						ppg: round1(r.pts / r.gp),
						rpg: round1(r.trb / r.gp),
						apg: round1(r.ast / r.gp),
						spg: round1(r.stl / r.gp),
						bpg: round1(r.blk / r.gp),
						pts: r.pts,
						trb: r.trb,
						ast: r.ast,
						stl: r.stl,
						blk: r.blk,
					});
				}
			}
		}
		if (roster.length < 5) {
			continue;
		}
		roster.sort((a, b) => b.pts - a.pts);

		const leaderBy = (key: "pts" | "trb" | "ast" | "stl" | "blk") =>
			roster.reduce((best, p) => (p[key] > best[key] ? p : best)).pid;
		const leaders = {
			pts: leaderBy("pts"),
			trb: leaderBy("trb"),
			ast: leaderBy("ast"),
			stl: leaderBy("stl"),
			blk: leaderBy("blk"),
		};

		// Win-total guess: a window 12.5% of the season wide counts as correct.
		const wins = {
			actual: ts.won,
			games,
			window: Math.max(1, Math.round(games * 0.125)),
		};

		// Playoff finish, from playoffRoundsWon (-1 = missed; numRounds = title).
		let playoffs: TeamTriviaRound["playoffs"];
		const roundsWon = ts.playoffRoundsWon;
		if (roundsWon !== undefined && (season < currentSeason || playoffsDone)) {
			const series = await idb.getCopy.playoffSeries({ season }, "noCopyCache");
			const numRounds =
				series?.series.length ?? g.get("numGamesPlayoffSeries").length;
			if (numRounds > 0) {
				const options = ["Missed the playoffs"];
				for (let i = 0; i < numRounds - 1; i++) {
					options.push(`Lost in ${helpers.ordinal(i + 1)} round`);
				}
				options.push("Lost in the Finals");
				options.push("Won the championship");
				// options index: 0 = missed; 1..numRounds = lost in round i; last = champ
				const answerIndex =
					roundsWon < 0 ? 0 : Math.min(roundsWon + 1, options.length - 1);
				playoffs = { options, answerIndex };
			}
		}

		return {
			season,
			team: { tid, label: `${region} ${name}`, abbrev },
			roster,
			leaders,
			wins,
			playoffs,
			searchList: getSearchList(pool),
		};
	}

	return undefined;
};
