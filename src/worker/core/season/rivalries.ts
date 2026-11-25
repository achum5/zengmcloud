import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { distanceMiles, type Coords } from "./calendarTravel.ts";

// THE LEAGUE'S RIVALRIES, for Rivals Week (see marqueeNights.ts), the
// biggest first. With more than one user team, the user teams against each
// other come first; then every other matchup by how much of a rivalry it
// is: the playoff series the two have played these last seasons (a deeper
// round, a series that went the distance and a recent one counting more),
// the same division, how close their cities are (the same city most), and
// how good both teams are as the season begins (their draw, marqueeDraw.ts).

// Seasons of playoff meetings looked at, and what each season back keeps.
const LOOKBACK = 10;
const FADE = 0.75;

const USERS = 10;
const PLAYOFFS = 1.5;
const SAME_DIVISION = 0.6;
const SAME_CONFERENCE = 0.1;
// The same city (within these miles), and nearby, less and less to none.
const SAME_CITY_MILES = 75;
const SAME_CITY = 1.2;
const NEARBY_MILES = 700;
const NEARBY = 0.6;
const DRAW = 0.6;

// Matchups worth trying, at most.
const MAX_PAIRS = 80;

export const pairKey = (a: number, b: number) =>
	a < b ? `${a}-${b}` : `${b}-${a}`;

// Each matchup's playoff history, by pairKey: for every series the two
// played in the last LOOKBACK seasons, how deep a round it was (the finals
// 1), more if it went the distance, less each season back.
export const playoffHistory = async (season: number) => {
	const out = new Map<string, number>();
	for (let s = season - 1; s >= season - LOOKBACK; s--) {
		const headToHead = await idb.merged.headToHeads.get(s);
		if (!headToHead) {
			continue;
		}
		const numRounds = Math.max(1, g.get("numGamesPlayoffSeries", s).length);
		for (const [t0, row] of Object.entries(headToHead.playoffs)) {
			for (const [t1, info] of Object.entries(row)) {
				// (Not the play-in.)
				if (info.round < 0) {
					continue;
				}
				const toWin = helpers.numGamesToWinRound(info.round, s);
				const full = toWin > 1 && info.won + info.lost >= 2 * toWin - 1;
				const value =
					((info.round + 1) / numRounds) *
					(full ? 1.25 : 1) *
					FADE ** (season - 1 - s);
				const key = pairKey(Number(t0), Number(t1));
				out.set(key, (out.get(key) ?? 0) + value);
			}
		}
	}
	return out;
};

export type RivalryTeam = { tid: number; cid: number; did: number };

export const rankRivalries = ({
	teams,
	locations,
	draw,
	users = [],
	playoffs = new Map(),
}: {
	teams: RivalryTeam[];
	locations: Map<number, Coords | undefined>;
	draw: Map<number, number>;
	users?: number[];
	playoffs?: Map<string, number>;
}): [number, number][] => {
	const userTeams = new Set(users.length > 1 ? users : []);
	const scored: { a: number; b: number; key: string; score: number }[] = [];
	for (const [i, ta] of teams.entries()) {
		for (const tb of teams.slice(i + 1)) {
			const key = pairKey(ta.tid, tb.tid);
			let score = PLAYOFFS * (playoffs.get(key) ?? 0);
			if (userTeams.has(ta.tid) && userTeams.has(tb.tid)) {
				score += USERS;
			}
			if (ta.cid === tb.cid) {
				score += ta.did === tb.did ? SAME_DIVISION : SAME_CONFERENCE;
			}
			const la = locations.get(ta.tid);
			const lb = locations.get(tb.tid);
			if (la && lb) {
				const miles = distanceMiles(la, lb);
				score +=
					miles <= SAME_CITY_MILES
						? SAME_CITY
						: NEARBY * Math.max(0, 1 - miles / NEARBY_MILES);
			}
			const da = draw.get(ta.tid) ?? 0;
			const db = draw.get(tb.tid) ?? 0;
			score += DRAW * (Math.min(da, db) + 0.25 * (da + db));
			scored.push({ a: ta.tid, b: tb.tid, key, score });
		}
	}
	return scored
		.sort((x, y) => y.score - x.score || (x.key < y.key ? -1 : 1))
		.slice(0, MAX_PAIRS)
		.map(({ a, b }) => [a, b]);
};

// Keeps the season's Rivals Week matchups with its teams (TeamSeason.rivals),
// for that week's national games.
export const saveRivals = async (season: number, pairs: [number, number][]) => {
	const rivals = new Map<number, number[]>();
	for (const [a, b] of pairs) {
		rivals.set(a, [...(rivals.get(a) ?? []), b]);
		rivals.set(b, [...(rivals.get(b) ?? []), a]);
	}
	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsBySeasonTid",
		[[season], [season, "Z"]],
	);
	for (const teamSeason of teamSeasons) {
		const own = rivals.get(teamSeason.tid);
		if (own) {
			teamSeason.rivals = own;
		} else if (teamSeason.rivals) {
			delete teamSeason.rivals;
		} else {
			continue;
		}
		await idb.cache.teamSeasons.put(teamSeason);
	}
};
