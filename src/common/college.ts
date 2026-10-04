// Class years for college leagues. Players get five seasons in five years
// (the NCAA's rule from 2027): freshman through fifth-year senior, no
// redshirts. A player's freshman season is stored as collegeYear0.

export const COLLEGE_CLASSES = ["FR", "SO", "JR", "SR", "5TH"] as const;
export const COLLEGE_SEASONS = COLLEGE_CLASSES.length;

export const collegeYear = (p: { collegeYear0?: number }, season: number) =>
	p.collegeYear0 === undefined ? undefined : season - p.collegeYear0 + 1;

export const collegeClassLabel = (
	p: { collegeYear0?: number },
	season: number,
) => {
	const year = collegeYear(p, season);
	if (year === undefined) {
		return "";
	}
	if (year < 1) {
		return "HS";
	}
	return COLLEGE_CLASSES[Math.min(year, COLLEGE_SEASONS) - 1]!;
};

// Last season he can play.
export const collegeFinalSeason = (p: { collegeYear0?: number }) =>
	p.collegeYear0 === undefined
		? undefined
		: p.collegeYear0 + COLLEGE_SEASONS - 1;

// Phase names where the college calendar differs from the pro one.
export const COLLEGE_PHASE_TEXT: Partial<Record<number, string>> = {
	4: "after playoffs",
	5: "after playoffs",
	6: "after playoffs",
	7: "retention",
	8: "offseason recruiting",
};

// Conference tournament progress, kept in game attributes while they run.
export type CollegeConfTourney = {
	season: number;
	// Seed order: alive[cid][0] is the top remaining seed.
	alive: Record<number, number[]>;
	champs: Record<number, number>;
	// [home, away, cid] for games scheduled but not yet resolved.
	pending: [number, number, number][];
};

// NCAA seeds: the field is seeded 1-64 overall, four teams per seed line.
export const collegeSeedLine = (overallSeed: number) =>
	Math.ceil(overallSeed / 4);

// What a player looks for in a school. Every player weighs all of these, in
// his own proportions; his top three are shown.
export const COLLEGE_PRIORITIES = [
	"prestige",
	"winning",
	"proximity",
	"playingTime",
	"proPotential",
	"nil",
	"conference",
	"coachStability",
	"facilities",
] as const;
export type CollegePriority = (typeof COLLEGE_PRIORITIES)[number];

export const COLLEGE_PRIORITY_LABELS: Record<CollegePriority, string> = {
	prestige: "Prestige",
	winning: "Winning",
	proximity: "Close to home",
	playingTime: "Playing time",
	proPotential: "Pro potential",
	nil: "NIL",
	conference: "Conference",
	coachStability: "Coach stability",
	facilities: "Facilities",
};

// A player's personality, from high school on.
export type CollegeProfile = {
	weights: Record<CollegePriority, number>; // sums to 1
	// How many rounds of haggling he puts up with, 1-5.
	patience: number;
};

export const collegeTopPriorities = (profile: CollegeProfile, n = 3) =>
	[...COLLEGE_PRIORITIES]
		.sort((a, b) => profile.weights[b] - profile.weights[a])
		.slice(0, n);

// NIL talks between one player and one school.
export type CollegeTalks = {
	// Rounds of patience left. At 0 his counter is final.
	patience: number;
	// His standing counteroffer, thousands per year.
	counter?: number;
	// Ended talks with this school.
	walked?: true;
	// Interest permanently lost over lowball offers.
	penalty: number;
};

export type CollegePromiseType =
	| "starter"
	| "minutes"
	| "nilRaise"
	| "noPosition";

export const COLLEGE_PROMISE_LABELS: Record<CollegePromiseType, string> = {
	starter: "Starter",
	minutes: "Minutes",
	nilRaise: "NIL raise",
	noPosition: "No one at his position",
};

export type CollegePromise = {
	type: CollegePromiseType;
	tid: number;
	// The season it's for: his first season for a recruit, next season for a
	// returning player.
	season: number;
	// Minutes per game for "minutes"; the class (draft year) for "noPosition".
	value?: number;
	status?: "kept" | "broken";
};

// Recruiting state, kept on each high school recruit (and transfer portal
// player) while he is being recruited.
export type CollegeRecruiting = {
	stars: number; // 1-5
	rank: number; // national rank in his class
	// The NIL he's looking for, thousands per year. Schools only see a range.
	ask: number;
	askRange: [number, number];
	// Scouting error in his ovr/pot, shrinking as a school spends hours on him.
	fuzz: number;
	// Hours each school has spent on him in total, which is also how well it
	// has scouted him.
	scout: Record<number, number>;
	// Accumulated recruiting effort and resulting interest (0-100ish), only for
	// schools that have engaged.
	effort: Record<number, number>;
	interest: Record<number, number>;
	// Scholarship offers, with the agreed NIL (thousands per year).
	offers: Record<number, number>;
	talks: Record<number, CollegeTalks>;
	// Promises attached to a school's offer.
	promises: Record<number, CollegePromise[]>;
	visits: number[];
	// Hours per week user schools are spending on him.
	hours: Record<number, number>;
	committed?: number;
	// Days he's been recruited. Nobody decides right away.
	days?: number;
	// Transfer portal: the school he left.
	portalFrom?: number;
};

// The offseason for a returning player: his NIL renegotiation and whether
// he's thinking about the transfer portal.
export type CollegeRetention = {
	season: number;
	// NIL he wants next season, thousands per year, and the range schools see.
	demand: number;
	demandRange: [number, number];
	talks?: CollegeTalks;
	// Chance he enters the portal, 0-1, and the reasons behind it.
	risk: number;
	reasons: string[];
	// Settled for the year: new deal agreed (or nothing wanted).
	settled?: true;
};

// The user as coach of his school.
export type CollegeCoach = {
	tid: number;
	// First season at this school, and his contract's final season.
	start: number;
	exp: number;
	// Job offers after the season, from other schools.
	offers?: number[];
};

export type CollegePolls = {
	season: number;
	// Top 25 after each week (the first is the preseason poll), best first.
	weeks: number[][];
	// Game days since the last poll.
	days: number;
};

// The NIT: the best 32 teams left out of the NCAA tournament, single
// elimination alongside it.
export type CollegeNit = {
	season: number;
	// Seed order, best first.
	field: number[];
	alive: number[];
	pending: [number, number][];
	champ?: number;
};

// Points for a recruiting class: stars, counting a school's best signees most.
export const STAR_POINTS = [0, 5, 20, 40, 70, 100];

export const classPoints = (stars: number[]) =>
	[...stars]
		.sort((a, b) => b - a)
		.reduce((total, s, i) => total + STAR_POINTS[s]! * 0.9 ** i, 0);

export const RECRUITING_HOURS_PER_WEEK = 100;
export const RECRUITING_MAX_HOURS = 25;
export const RECRUITING_VISITS = 8;
// Hours of attention it takes to know a player's ratings exactly.
export const SCOUTING_HOURS_FULL = 50;

export const scoutingProgress = (rec: CollegeRecruiting, tid: number) =>
	Math.min(1, (rec.scout[tid] ?? 0) / SCOUTING_HOURS_FULL);

// A rating as a school sees it: a range that narrows to the true value as it
// scouts him.
export const scoutedRange = (
	value: number,
	fuzz: number,
	progress: number,
): [number, number] => {
	const center = value + fuzz * (1 - progress);
	const half = 6 * (1 - progress);
	return [Math.round(center - half), Math.round(center + half)];
};

// Yearly NIL budget (thousands) for a program of this prestige: about $12M at
// the top, under $1M at the bottom - always some room over what a program
// like it pays its returning players.
export const collegeNilBudget = (prestige: number, scale = 1) =>
	Math.round((scale * (450 + 300 * 1.037 ** prestige)) / 10) * 10;

const STATES: Record<string, string> = {
	Alabama: "AL",
	Alaska: "AK",
	Arizona: "AZ",
	Arkansas: "AR",
	California: "CA",
	Colorado: "CO",
	Connecticut: "CT",
	Delaware: "DE",
	"District of Columbia": "DC",
	Florida: "FL",
	Georgia: "GA",
	Hawaii: "HI",
	Idaho: "ID",
	Illinois: "IL",
	Indiana: "IN",
	Iowa: "IA",
	Kansas: "KS",
	Kentucky: "KY",
	Louisiana: "LA",
	Maine: "ME",
	Maryland: "MD",
	Massachusetts: "MA",
	Michigan: "MI",
	Minnesota: "MN",
	Mississippi: "MS",
	Missouri: "MO",
	Montana: "MT",
	Nebraska: "NE",
	Nevada: "NV",
	"New Hampshire": "NH",
	"New Jersey": "NJ",
	"New Mexico": "NM",
	"New York": "NY",
	"North Carolina": "NC",
	"North Dakota": "ND",
	Ohio: "OH",
	Oklahoma: "OK",
	Oregon: "OR",
	Pennsylvania: "PA",
	"Rhode Island": "RI",
	"South Carolina": "SC",
	"South Dakota": "SD",
	Tennessee: "TN",
	Texas: "TX",
	Utah: "UT",
	Vermont: "VT",
	Virginia: "VA",
	Washington: "WA",
	"West Virginia": "WV",
	Wisconsin: "WI",
	Wyoming: "WY",
};

// Census divisions, for "close to home".
const REGIONS: string[][] = [
	["ME", "NH", "VT", "MA", "RI", "CT"],
	["NY", "NJ", "PA"],
	["OH", "IN", "IL", "MI", "WI"],
	["MN", "IA", "MO", "ND", "SD", "NE", "KS"],
	["DE", "MD", "DC", "VA", "WV", "NC", "SC", "GA", "FL"],
	["KY", "TN", "AL", "MS"],
	["AR", "LA", "OK", "TX"],
	["MT", "ID", "WY", "CO", "NM", "AZ", "UT", "NV"],
	["WA", "OR", "CA", "AK", "HI"],
];

// Two-letter home state for a US birthplace like "Texas, USA".
export const homeState = (bornLoc: string) => {
	const parts = bornLoc.split(", ");
	if (parts.at(-1) !== "USA" && parts.length > 1) {
		return undefined;
	}
	const name = parts.length > 1 ? parts.at(-2)! : parts[0]!;
	return STATES[name] ?? (name.length === 2 ? name : undefined);
};

export const sameRegion = (a: string, b: string) =>
	REGIONS.some((region) => region.includes(a) && region.includes(b));
