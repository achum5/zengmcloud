// Class years for college leagues. A player's freshman season is stored as
// collegeYear0; a redshirt year doesn't use up eligibility.

export const COLLEGE_CLASSES = ["FR", "SO", "JR", "SR"] as const;

export const collegeYear = (
	p: { collegeYear0?: number; redshirt?: number },
	season: number,
) => {
	if (p.collegeYear0 === undefined) {
		return undefined;
	}
	let year = season - p.collegeYear0 + 1;
	if (p.redshirt !== undefined && p.redshirt < season) {
		year -= 1;
	}
	return year;
};

export const collegeClassLabel = (
	p: { collegeYear0?: number; redshirt?: number },
	season: number,
) => {
	const year = collegeYear(p, season);
	if (year === undefined) {
		return "";
	}
	if (year < 1) {
		return "HS";
	}
	const label = COLLEGE_CLASSES[Math.min(year, 4) - 1]!;
	return p.redshirt !== undefined && p.redshirt < season
		? `RS ${label}`
		: label;
};

// Last season he can play: four seasons from his freshman year, five with a
// redshirt.
export const collegeFinalSeason = (p: {
	collegeYear0?: number;
	redshirt?: number;
}) =>
	p.collegeYear0 === undefined
		? undefined
		: p.collegeYear0 + 3 + (p.redshirt !== undefined ? 1 : 0);

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

// Recruiting state, kept on each high school recruit (and transfer portal
// player) while he is being recruited.
export type CollegeRecruiting = {
	stars: number; // 1-5
	rank: number; // national rank in his class
	ask: number; // NIL he's looking for, thousands per year
	// Accumulated recruiting effort and resulting interest (0-100ish), only for
	// schools that have engaged.
	effort: Record<number, number>;
	interest: Record<number, number>;
	// Scholarship offers, with the NIL attached (thousands per year).
	offers: Record<number, number>;
	visits: number[];
	// Hours per week user schools are spending on him.
	hours: Record<number, number>;
	committed?: number;
	// Transfer portal: the school he left.
	portalFrom?: number;
};

export const RECRUITING_HOURS_PER_WEEK = 100;
export const RECRUITING_MAX_HOURS = 25;
export const RECRUITING_VISITS = 8;

// Yearly NIL budget (thousands) for a program of this prestige: about $12M at
// the top, a few hundred thousand at the bottom.
export const collegeNilBudget = (prestige: number) =>
	Math.round((150 * 1.045 ** prestige) / 10) * 10;

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

// How a player reacts to an NIL number, for the negotiation.
export const nilReaction = (rec: { ask: number }, amount: number) => {
	const ratio = amount / Math.max(1, rec.ask);
	if (ratio >= 1.15) {
		return "thrilled" as const;
	}
	if (ratio >= 0.95) {
		return "happy" as const;
	}
	if (ratio >= 0.75) {
		return "lukewarm" as const;
	}
	return "insulted" as const;
};
