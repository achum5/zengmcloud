import { choice } from "../../../common/random.ts";
import g from "../../util/g.ts";
import realSchedules from "./realSchedules.basketball.json";
import { teamLocation } from "../../../common/teamLocation.ts";
import {
	addDays,
	dayNumber,
	makeDate,
	seasonStartYear,
	weekdayOf,
} from "../../../common/calendarDates.ts";
import { distanceMiles, type Coords } from "./calendarTravel.ts";

// REAL NBA SEASONS, for a league set up like the NBA: 30 teams, two
// conferences of three five-team divisions, 82 games with 16 in the division
// and 36 more in the conference. Each real team's slot goes to the league team
// nearest it on the map in a matching division (a league of the real teams
// gets them exactly), and the season is moved to the league's year so every
// date keeps its weekday - Christmas is still December 25 and opening night
// still a Tuesday.
//
// The data is only dates and matchups. Seasons that don't fit the format
// exactly are left out: 2011-12 (66 games), 2012-13 (one game never played),
// 2019-20 and 2020-21 (COVID), and 2023-24 on (the NBA Cup's extra games break
// the division and conference counts).

const NUM_GAMES = 82;
const NUM_GAMES_DIV = 16;
const NUM_GAMES_CONF = 36;
const TEAM_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcd";

type RealSchedules = {
	teams: string[];
	seasons: {
		season: string;
		start: string;
		nights: string[];
	}[];
};

// The data's order: East (Atlantic, Central, Southeast), then West
// (Northwest, Pacific, Southwest), five teams each.
const REAL_TEAMS: {
	abbrevs: string[];
	region: string;
}[] = [
	{ abbrevs: ["BOS"], region: "Boston" },
	{ abbrevs: ["BKN", "NJN"], region: "Brooklyn" },
	{ abbrevs: ["NYK", "NYC"], region: "New York" },
	{ abbrevs: ["PHI"], region: "Philadelphia" },
	{ abbrevs: ["TOR"], region: "Toronto" },
	{ abbrevs: ["CHI"], region: "Chicago" },
	{ abbrevs: ["CLE"], region: "Cleveland" },
	{ abbrevs: ["DET"], region: "Detroit" },
	{ abbrevs: ["IND"], region: "Indiana" },
	{ abbrevs: ["MIL"], region: "Milwaukee" },
	{ abbrevs: ["ATL"], region: "Atlanta" },
	{ abbrevs: ["CHA"], region: "Charlotte" },
	{ abbrevs: ["MIA"], region: "Miami" },
	{ abbrevs: ["ORL"], region: "Orlando" },
	{ abbrevs: ["WAS"], region: "Washington" },
	{ abbrevs: ["DEN"], region: "Denver" },
	{ abbrevs: ["MIN"], region: "Minnesota" },
	{ abbrevs: ["OKC"], region: "Oklahoma City" },
	{ abbrevs: ["POR"], region: "Portland" },
	{ abbrevs: ["UTA"], region: "Utah" },
	{ abbrevs: ["GSW", "GS"], region: "Golden State" },
	{ abbrevs: ["LAC"], region: "Los Angeles" },
	{ abbrevs: ["LAL"], region: "Los Angeles" },
	{ abbrevs: ["PHX", "PHO"], region: "Phoenix" },
	{ abbrevs: ["SAC"], region: "Sacramento" },
	{ abbrevs: ["DAL"], region: "Dallas" },
	{ abbrevs: ["HOU"], region: "Houston" },
	{ abbrevs: ["MEM"], region: "Memphis" },
	{ abbrevs: ["NOP", "NOL", "NOH"], region: "New Orleans" },
	{ abbrevs: ["SAS", "SA"], region: "San Antonio" },
];

const NUM_DIVS_PER_CONF = 3;
const NUM_TEAMS_PER_DIV = 5;

export type RealScheduleTeam = {
	tid: number;
	cid: number;
	did: number;
	abbrev?: string;
	// Where it plays (teamLocation.ts), if it's on the map.
	location?: Coords;
};

export type RealScheduleGame = {
	homeTid: number;
	awayTid: number;
	date: string;
};

const permutations = <T>(items: T[]): T[][] => {
	if (items.length <= 1) {
		return [items];
	}
	const out: T[][] = [];
	items.forEach((item, i) => {
		const rest = [...items.slice(0, i), ...items.slice(i + 1)];
		for (const perm of permutations(rest)) {
			out.push([item, ...perm]);
		}
	});
	return out;
};

// How badly a league team fits a real team's slot: an exact abbreviation is a
// perfect fit, otherwise the distance between the two cities. A city off the
// map costs the same everywhere, so it changes nothing.
const slotCost = (t: RealScheduleTeam, slot: number) => {
	const real = REAL_TEAMS[slot]!;
	if (t.abbrev !== undefined && real.abbrevs.includes(t.abbrev)) {
		return -100_000;
	}
	const a = t.location;
	const b = teamLocation({ region: real.region });
	if (!a || !b) {
		return 1000;
	}
	return distanceMiles(a, b);
};

// League tid for each real slot, or undefined if the league isn't shaped
// like the NBA.
export const mapTeamsToRealSlots = (
	teams: RealScheduleTeam[],
): number[] | undefined => {
	if (teams.length !== REAL_TEAMS.length) {
		return;
	}
	const didsByCid = new Map<number, number[]>();
	const teamsByDid = new Map<number, RealScheduleTeam[]>();
	for (const t of teams) {
		const dids = didsByCid.get(t.cid) ?? [];
		if (!dids.includes(t.did)) {
			dids.push(t.did);
		}
		didsByCid.set(t.cid, dids);
		const divTeams = teamsByDid.get(t.did) ?? [];
		divTeams.push(t);
		teamsByDid.set(t.did, divTeams);
	}
	if (
		didsByCid.size !== 2 ||
		Array.from(didsByCid.values()).some(
			(dids) => dids.length !== NUM_DIVS_PER_CONF,
		) ||
		Array.from(teamsByDid.values()).some(
			(divTeams) => divTeams.length !== NUM_TEAMS_PER_DIV,
		)
	) {
		return;
	}

	// Best way to seat each league division in each real division.
	const slotPerms = permutations([0, 1, 2, 3, 4]);
	const divFit = (did: number, realDiv: number) => {
		const divTeams = teamsByDid.get(did)!;
		let best = Infinity;
		let bestPerm = slotPerms[0]!;
		for (const perm of slotPerms) {
			let cost = 0;
			for (let i = 0; i < NUM_TEAMS_PER_DIV; i++) {
				cost += slotCost(divTeams[i]!, realDiv * NUM_TEAMS_PER_DIV + perm[i]!);
			}
			if (cost < best) {
				best = cost;
				bestPerm = perm;
			}
		}
		return { cost: best, perm: bestPerm };
	};

	const cids = Array.from(didsByCid.keys());
	const divPerms = permutations([0, 1, 2]);
	let best:
		| { cost: number; seats: { did: number; realDiv: number }[] }
		| undefined;
	for (const cidOrder of [cids, [...cids].reverse()]) {
		for (const permEast of divPerms) {
			for (const permWest of divPerms) {
				const seats: { did: number; realDiv: number }[] = [];
				const east = didsByCid.get(cidOrder[0]!)!;
				const west = didsByCid.get(cidOrder[1]!)!;
				for (let i = 0; i < NUM_DIVS_PER_CONF; i++) {
					seats.push(
						{ did: east[i]!, realDiv: permEast[i]! },
						{ did: west[i]!, realDiv: NUM_DIVS_PER_CONF + permWest[i]! },
					);
				}
				let cost = 0;
				for (const seat of seats) {
					cost += divFit(seat.did, seat.realDiv).cost;
				}
				if (!best || cost < best.cost) {
					best = { cost, seats };
				}
			}
		}
	}

	const tidBySlot: number[] = [];
	for (const { did, realDiv } of best!.seats) {
		const { perm } = divFit(did, realDiv);
		const divTeams = teamsByDid.get(did)!;
		for (let i = 0; i < NUM_TEAMS_PER_DIV; i++) {
			tidBySlot[realDiv * NUM_TEAMS_PER_DIV + perm[i]!] = divTeams[i]!.tid;
		}
	}
	return tidBySlot;
};

export const realScheduleFits = () =>
	g.get("numGames") === NUM_GAMES &&
	g.get("numGamesDiv") === NUM_GAMES_DIV &&
	g.get("numGamesConf") === NUM_GAMES_CONF;

// The real seasons whose October 1 fell on the same weekday as the league's,
// so day-for-day they line up with its calendar.
export const realSeasonsFor = (season: number) => {
	const { seasons } = realSchedules as RealSchedules;
	const target = makeDate(seasonStartYear(season), 10, 1);
	return seasons.filter((real) => {
		const oct1 = makeDate(Number(real.start.slice(0, 4)), 10, 1);
		return weekdayOf(oct1) === weekdayOf(target);
	});
};

export const getRealSchedule = (
	teams: RealScheduleTeam[],
	season: number,
): RealScheduleGame[] | undefined => {
	if (!realScheduleFits()) {
		return;
	}
	const tidBySlot = mapTeamsToRealSlots(teams);
	if (!tidBySlot) {
		return;
	}
	const candidates = realSeasonsFor(season);
	if (candidates.length === 0) {
		return;
	}
	const real = choice(candidates);

	// Same distance from October 1 in both years.
	const realOct1 = makeDate(Number(real.start.slice(0, 4)), 10, 1);
	const shift =
		dayNumber(makeDate(seasonStartYear(season), 10, 1)) - dayNumber(realOct1);
	const start = addDays(real.start, shift);

	const games: RealScheduleGame[] = [];
	real.nights.forEach((night, i) => {
		const date = addDays(start, i);
		for (let j = 0; j < night.length; j += 2) {
			const home = TEAM_LETTERS.indexOf(night[j]!);
			const away = TEAM_LETTERS.indexOf(night[j + 1]!);
			games.push({
				homeTid: tidBySlot[home]!,
				awayTid: tidBySlot[away]!,
				date,
			});
		}
	});
	return games;
};
