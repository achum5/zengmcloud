import { COLLEGE_CONFERENCES } from "../../../common/collegeSchools.ts";
import type { Conf, Div } from "../../../common/types.ts";
import { randInt, realGauss } from "../../../common/random.ts";
import { helpers } from "../../util/index.ts";

// Teams, conferences and rules for a brand new college league. Every
// conference is one "conference" with a single division of the same name, so
// standings group by conference and the schedule generator sees them.

export const getCollegeTeams = () => {
	const confs: Conf[] = [];
	const divs: Div[] = [];
	const teams: {
		tid: number;
		cid: number;
		did: number;
		region: string;
		name: string;
		abbrev: string;
		colors: [string, string, string];
		pop: number;
		popRank: number;
		stadiumCapacity: number;
		prestige: number;
		state: string;
		collegeFacilities: number;
		collegeCoachYears: number;
		collegePros: number[];
		collegePromiseRep: number;
		collegePrestigeFloor: number;
	}[] = [];

	for (const [cid, conference] of COLLEGE_CONFERENCES.entries()) {
		confs.push({ cid, name: conference.name, abbrev: conference.abbrev });
		divs.push({
			did: cid,
			cid,
			name: conference.name,
			abbrev: conference.abbrev,
		});
		for (const school of conference.schools) {
			teams.push({
				tid: teams.length,
				cid,
				did: cid,
				region: school.region,
				name: school.name,
				abbrev: school.abbrev,
				colors: school.colors,
				// Fan base: drives attendance and hype. Blue bloods fill arenas.
				pop: Math.round((0.3 + school.prestige / 25) * 100) / 100,
				popRank: 0,
				stadiumCapacity: Math.round(2500 + school.prestige * 180),
				prestige: school.prestige,
				state: school.state,
				collegeFacilities: Math.round(
					helpers.bound(school.prestige + realGauss(0, 8), 5, 99),
				),
				collegeCoachYears: randInt(0, 15),
				// Players sent to the pros each of the last five seasons.
				collegePros: Array.from({ length: 5 }, () =>
					Math.round(
						Math.max(0, (school.prestige - 55) / 12 + realGauss(0, 0.6)),
					),
				),
				collegePromiseRep: 0.8,
				collegePrestigeFloor:
					school.prestige >= 85 ? 70 : school.prestige >= 75 ? 50 : 1,
			});
		}
	}

	const byPop = [...teams].sort((a, b) => b.pop - a.pop);
	for (const [i, t] of byPop.entries()) {
		t.popRank = i + 1;
	}

	return { confs, divs, teams };
};

// Rules that differ from the pro game. Halves instead of quarters, five fouls,
// a bigger home court edge, no money, no trades, no All-Star Game.
export const COLLEGE_SETTINGS = {
	college: true,
	numGames: 31,
	numGamesDiv: null,
	numGamesConf: null,
	numPeriods: 2,
	quarterLength: 20,
	foulsNeededToFoulOut: 5,
	foulsUntilBonus: [7, 4, 2],
	homeCourtAdvantage: 1.5,
	// Tuned to real D1 averages: at college talent levels the pro sim would
	// take too few threes, foul too little and miss too many free throws.
	threePointTendencyFactor: 2.6,
	foulRateFactor: 1.45,
	ftAccuracyFactor: 1.15,
	orbFactor: 1.25,
	// The tournament: 64 teams, single elimination.
	numGamesPlayoffSeries: [1, 1, 1, 1, 1, 1],
	numPlayoffByes: 0,
	playIn: false,
	neutralSite: "playoffs",
	allStarGame: null,
	tradeDeadline: 1,
	budget: false,
	salaryCapType: "none",
	// Contracts are NIL deals, in thousands per year.
	minPayroll: 0,
	luxuryPayroll: 1000000000,
	minContract: 5,
	maxContract: 5000,
	minContractLength: 1,
	maxContractLength: 4,
	// No draft: high school recruits arrive through the signing period.
	draftType: "freeAgents",
	numSeasonsFutureDraftPicks: 0,
	playoffsByConf: false,
	playoffsNumTeamsDiv: 0,
	// Slower than the pros: about 68 possessions a game.
	pace: 84,
	minRosterSize: 10,
	maxRosterSize: 15,
} as const;
