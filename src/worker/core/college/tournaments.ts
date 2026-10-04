import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { league, season as seasonCore } from "../index.ts";
import type { Conditions, Game } from "../../../common/types.ts";
import type { CollegeConfTourney } from "../../../common/college.ts";

// The college postseason.
//
// Conference tournaments run after the regular-season schedule: every team in
// each conference, seeded by conference record, single elimination with byes
// for the top seeds when the field isn't a power of two. One round is
// scheduled at a time, because each depends on the last. The winner of each
// is its conference's automatic bid.
//
// The NCAA field is the 31 automatic bids plus at-large teams by power rating
// (a simple rating system: margin of victory adjusted for opponents), seeded
// 1-64 on that rating. The pro playoff bracket seeded 1-64 is exactly the
// NCAA S-curve: the four 1 seeds meet the four 16 seeds, and 1 seeds can't
// meet before the Final Four.

const confStandings = async (season: number) => {
	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsBySeasonTid",
		[[season], [season, "Z"]],
	);
	const byCid = new Map<number, typeof teamSeasons>();
	for (const ts of teamSeasons) {
		const t = await idb.cache.teams.get(ts.tid);
		if (!t || t.disabled) {
			continue;
		}
		const list = byCid.get(t.cid) ?? [];
		list.push(ts);
		byCid.set(t.cid, list);
	}
	const winp = (won: number, lost: number) =>
		won + lost > 0 ? won / (won + lost) : 0;
	const out = new Map<number, number[]>();
	for (const [cid, list] of byCid) {
		list.sort(
			(a, b) =>
				winp(b.wonConf, b.lostConf) - winp(a.wonConf, a.lostConf) ||
				winp(b.won, b.lost) - winp(a.won, a.lost) ||
				Math.random() - 0.5,
		);
		out.set(
			cid,
			list.map((ts) => ts.tid),
		);
	}
	return out;
};

// The most recent game between two teams this season.
const latestGame = (games: Game[], a: number, b: number) => {
	let latest: Game | undefined;
	for (const game of games) {
		const tids = [game.won.tid, game.lost.tid];
		if (tids.includes(a) && tids.includes(b)) {
			if (!latest || game.gid > latest.gid) {
				latest = game;
			}
		}
	}
	return latest;
};

// Called when the regular-season schedule runs out. Schedules the next round
// of conference tournament games and returns true, or returns false once
// every conference has its champion.
export const advanceCollegeConfTourneys = async (conditions?: Conditions) => {
	const season = g.get("season");
	let state: CollegeConfTourney | undefined = g.get("collegeConfTourney");

	if (!state || state.season !== season) {
		const standings = await confStandings(season);
		state = { season, alive: {}, champs: {}, pending: [] };
		for (const [cid, tids] of standings) {
			if (tids.length === 1) {
				state.champs[cid] = tids[0]!;
			} else if (tids.length > 1) {
				state.alive[cid] = tids;
			}
		}
	} else if (state.pending.length > 0) {
		const games = await idb.getCopies.games({ season }, "noCopyCache");
		for (const [home, away, cid] of state.pending) {
			const game = latestGame(games, home, away);
			const loser = game ? game.lost.tid : away;
			const alive = state.alive[cid];
			if (alive) {
				state.alive[cid] = alive.filter((tid) => tid !== loser);
			}
		}
		state.pending = [];
	}

	// Crown champions, and line up the next round everywhere else.
	const matchups: [number, number][] = [];
	for (const [cidString, alive] of Object.entries(state.alive)) {
		const cid = Number(cidString);
		if (alive.length === 1) {
			const champ = alive[0]!;
			state.champs[cid] = champ;
			delete state.alive[cid];
			const t = await idb.cache.teams.get(champ);
			const conf = g.get("confs").find((c) => c.cid === cid);
			if (t && conf) {
				logEvent(
					{
						type: "playoffs",
						text: `The <a href="${helpers.leagueUrl([
							"roster",
							`${t.abbrev}_${t.tid}`,
							season,
						])}">${t.region} ${t.name}</a> won the ${conf.name} tournament.`,
						showNotification: champ === g.get("userTid"),
						tids: [champ],
						score: 10,
					},
					conditions,
				);
			}
			continue;
		}

		// Only as many games as it takes to get down to a power of two; the
		// top seeds wait for the winners.
		const n = alive.length;
		let size = 1;
		while (size * 2 <= n) {
			size *= 2;
		}
		const numGames = size === n ? n / 2 : n - size;
		const playing = alive.slice(n - 2 * numGames);
		for (let i = 0; i < numGames; i++) {
			const home = playing[i]!;
			const away = playing[playing.length - 1 - i]!;
			matchups.push([home, away]);
			state.pending.push([home, away, cid]);
		}
	}

	await league.setGameAttributes({ collegeConfTourney: state });

	if (matchups.length === 0) {
		return false;
	}

	await seasonCore.setSchedule(matchups);
	return true;
};

// Power rating from margin of victory and schedule strength, solved by
// iterating rating = average margin + average opponent rating.
export const computeCollegeRatings = async (season: number) => {
	const games = await idb.getCopies.games({ season }, "noCopyCache");
	const margins = new Map<number, number[]>();
	const opponents = new Map<number, number[]>();
	const add = (tid: number, margin: number, opp: number) => {
		margins.set(tid, [...(margins.get(tid) ?? []), margin]);
		opponents.set(tid, [...(opponents.get(tid) ?? []), opp]);
	};
	for (const game of games) {
		const margin = game.won.pts - game.lost.pts;
		add(game.won.tid, margin, game.lost.tid);
		add(game.lost.tid, -margin, game.won.tid);
	}

	const mean = (xs: number[]) =>
		xs.length > 0 ? xs.reduce((a, b) => a + b, 0) / xs.length : 0;
	const mov = new Map([...margins].map(([tid, xs]) => [tid, mean(xs)]));
	let ratings = new Map(mov);
	for (let iter = 0; iter < 30; iter++) {
		const next = new Map<number, number>();
		for (const [tid, m] of mov) {
			const opps = opponents.get(tid) ?? [];
			next.set(tid, m + mean(opps.map((opp) => ratings.get(opp) ?? 0)));
		}
		// Keep the scale centered on zero.
		const center = mean([...next.values()]);
		for (const [tid, r] of next) {
			next.set(tid, r - center);
		}
		ratings = next;
	}
	return ratings;
};

// The tournament field: automatic bids (tournament champions, or before the
// tournaments are done, each conference's best team by rating) plus the best
// at-large teams by rating, seeded by rating.
export const projectCollegeField = async <
	T extends { tid: number; seasonAttrs: { cid: number } },
>(
	teams: T[],
	numPlayoffTeams: number,
) => {
	const season = g.get("season");
	const ratings = await computeCollegeRatings(season);
	const rating = (t: T) => ratings.get(t.tid) ?? -Infinity;

	const state = g.get("collegeConfTourney");
	const champs =
		state && state.season === season
			? state.champs
			: ({} as Record<number, number>);
	const autobids = new Set<number>();
	const byCid = Map.groupBy(teams, (t) => t.seasonAttrs.cid);
	for (const [cid, list] of byCid) {
		const champ = champs[cid];
		if (champ !== undefined) {
			autobids.add(champ);
		} else {
			const best = [...list].sort((a, b) => rating(b) - rating(a))[0];
			if (best) {
				autobids.add(best.tid);
			}
		}
	}

	const sorted = [...teams].sort((a, b) => rating(b) - rating(a));
	const field = sorted.filter((t) => autobids.has(t.tid));
	const atLarge: T[] = [];
	const out: T[] = [];
	for (const t of sorted) {
		if (autobids.has(t.tid)) {
			continue;
		}
		if (field.length < numPlayoffTeams) {
			field.push(t);
			atLarge.push(t);
		} else {
			out.push(t);
		}
	}
	field.sort((a, b) => rating(b) - rating(a));
	return {
		field,
		autobids,
		lastFourIn: atLarge.slice(-4),
		firstFourOut: out.slice(0, 4),
		rest: out,
	};
};

// The tournament field, best seed first, followed by everyone left out.
export const orderCollegeTournamentField = async <
	T extends { tid: number; seasonAttrs: { cid: number } },
>(
	teams: T[],
	numPlayoffTeams: number,
) => {
	const { field, rest } = await projectCollegeField(teams, numPlayoffTeams);
	return [...field, ...rest];
};

// --- The NIT -----------------------------------------------------------------

// The best 32 teams left out of the NCAA field, by power rating.
export const collegeStartNit = async (
	ncaaTids: number[],
	conditions: Conditions,
) => {
	const season = g.get("season");
	const ratings = await computeCollegeRatings(season);
	const inNcaa = new Set(ncaaTids);
	const teams = (await idb.cache.teams.getAll()).filter(
		(t) => !t.disabled && !inNcaa.has(t.tid),
	);
	const field = teams
		.sort((a, b) => (ratings.get(b.tid) ?? -999) - (ratings.get(a.tid) ?? -999))
		.slice(0, 32)
		.map((t) => t.tid);
	await league.setGameAttributes({
		collegeNit: { season, field, alive: field, pending: [] },
	});
	if (field.includes(g.get("userTid"))) {
		logEvent(
			{
				type: "madePlayoffs",
				text: `You made the NIT.`,
				showNotification: true,
				tids: [g.get("userTid")],
				score: 0,
			},
			conditions,
		);
	}
};

// Each NCAA tournament day: settle the last NIT round, then add the next one
// to the day's games. Returns the NIT games to play.
export const collegeNitDay = async (
	ncaaGamesToday: boolean,
): Promise<[number, number][]> => {
	const season = g.get("season");
	const state = g.get("collegeNit");
	if (!state || state.season !== season || state.champ !== undefined) {
		return [];
	}

	let alive = state.alive;
	if (state.pending.length > 0) {
		const games = await idb.getCopies.games({ season }, "noCopyCache");
		const losers = new Set<number>();
		for (const [home, away] of state.pending) {
			const game = latestGame(games, home, away);
			losers.add(game ? game.lost.tid : away);
		}
		alive = alive.filter((tid) => !losers.has(tid));
	}

	let champ: number | undefined;
	const matchups: [number, number][] = [];
	if (alive.length === 1) {
		champ = alive[0];
		const t =
			champ !== undefined ? await idb.cache.teams.get(champ) : undefined;
		if (t) {
			logEvent({
				type: "playoffs",
				text: `The <a href="${helpers.leagueUrl([
					"roster",
					`${t.abbrev}_${t.tid}`,
					season,
				])}">${t.region} ${t.name}</a> won the NIT.`,
				showNotification: t.tid === g.get("userTid"),
				tids: [t.tid],
				score: 10,
			});
		}
	} else if (ncaaGamesToday) {
		// Best remaining seed hosts the worst.
		for (let i = 0; i < alive.length / 2; i++) {
			matchups.push([alive[i]!, alive[alive.length - 1 - i]!]);
		}
	}

	await league.setGameAttributes({
		collegeNit: { ...state, alive, pending: matchups, champ },
	});
	return matchups;
};
