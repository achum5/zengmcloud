import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { league, team } from "../index.ts";
import { computeCollegeRatings } from "./tournaments.ts";
import { collegeCalendarDays } from "./recruiting.ts";

// THE TOP 25
//
// A preseason poll from rosters and reputation, then a new poll each week of
// the regular season. Like real voters, it leans on records more than a
// power rating would, and early on it still remembers the preseason.

const zScores = (values: Map<number, number>) => {
	const xs = [...values.values()];
	const mean = xs.reduce((a, b) => a + b, 0) / Math.max(1, xs.length);
	const sd =
		Math.sqrt(
			xs.reduce((a, b) => a + (b - mean) ** 2, 0) / Math.max(1, xs.length),
		) || 1;
	return new Map([...values].map(([tid, x]) => [tid, (x - mean) / sd]));
};

const preseasonScores = async () => {
	const scores = new Map<number, number>();
	for (const t of await idb.cache.teams.getAll()) {
		if (t.disabled) {
			continue;
		}
		const players = await idb.cache.players.indexGetAll("playersByTid", t.tid);
		const ovr = team.ovr(
			players.map((p) => ({
				injury: p.injury,
				pid: p.pid,
				value: p.value,
				ratings: {
					ovr: p.ratings.at(-1)!.ovr,
					ovrs: p.ratings.at(-1)!.ovrs,
					pos: p.ratings.at(-1)!.pos,
				},
			})),
		);
		// Reputation counts for a little.
		scores.set(t.tid, ovr + (t.prestige ?? 30) / 20);
	}
	return zScores(scores);
};

const pollFrom = (scores: Map<number, number>) =>
	[...scores]
		.sort((a, b) => b[1] - a[1])
		.slice(0, 25)
		.map(([tid]) => tid);

export const collegePreseasonPoll = async () => {
	const scores = await preseasonScores();
	await league.setGameAttributes({
		collegePolls: {
			season: g.get("season"),
			weeks: [pollFrom(scores)],
			days: 0,
		},
	});
};

// Called after each regular season game day.
export const collegePollDay = async () => {
	const season = g.get("season");
	let polls = g.get("collegePolls");
	if (!polls || polls.season !== season) {
		await collegePreseasonPoll();
		polls = g.get("collegePolls")!;
	}
	const days = polls.days + collegeCalendarDays();
	if (days < 7) {
		await league.setGameAttributes({ collegePolls: { ...polls, days } });
		return;
	}

	const ratings = await computeCollegeRatings(season);
	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsBySeasonTid",
		[[season], [season, "Z"]],
	);
	const preseason = await preseasonScores();
	const record = new Map<number, number>();
	const gamesPlayed = new Map<number, number>();
	for (const ts of teamSeasons) {
		const gp = ts.won + ts.lost;
		record.set(ts.tid, gp > 0 ? ts.won / gp : 0.5);
		gamesPlayed.set(ts.tid, gp);
	}
	const ratingZ = zScores(ratings);
	const recordZ = zScores(record);

	const scores = new Map<number, number>();
	for (const [tid, pre] of preseason) {
		const gp = gamesPlayed.get(tid) ?? 0;
		// Fully on this season's results after about ten games.
		const w = Math.min(1, gp / 10);
		const now = 0.55 * (recordZ.get(tid) ?? 0) + 0.45 * (ratingZ.get(tid) ?? 0);
		scores.set(tid, w * now + (1 - w) * pre);
	}

	await league.setGameAttributes({
		collegePolls: {
			season,
			weeks: [...polls.weeks, pollFrom(scores)],
			days: 0,
		},
	});
};
