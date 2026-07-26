import type { TriviaPlayer, TriviaPool } from "./pool.ts";

// Parametric grid criteria: "20,000+ Career Points", "1+ PPG (Season)",
// "20 or fewer PPG (Season)", "Debuted in the 1990s".
//
// The fixed achievement list can only offer the thresholds someone wrote down.
// These are computed from a (stat, operator, number) triple instead, so the
// grid editor can offer a real number box and a </> toggle and every value in
// between just works.
//
// Note this is deliberately NOT the label-regex approach the standalone Grids
// app uses (parse "30+ PPG" back out of its own display string). Our criteria
// are already structured, so carrying the number as a number is both simpler
// and impossible to get wrong on an unusual label.

export type {
	StatOp,
	DecadeMode,
} from "../../../common/triviaCriteriaLabels.ts";
import type { StatOp } from "../../../common/triviaCriteriaLabels.ts";
export {
	decadeLabel,
	statLabel,
} from "../../../common/triviaCriteriaLabels.ts";

export type StatSpec = {
	id: string;
	// Shown in the editor's stat dropdown.
	label: string;
	// Short form used in the generated criterion label ("PPG", "Career Points").
	unit: string;
	scope: "career" | "season";
	// Which TriviaPlayer field to read.
	field:
		| "pts"
		| "trb"
		| "ast"
		| "stl"
		| "blk"
		| "tp"
		| "gp"
		| "min"
		| "seasons";
	// Season specs compare a per-game rate; career specs compare a total.
	perGame: boolean;
	decimals: number;
	defaultValue: number;
	step: number;
};

// A season only counts toward a per-game rate if the player actually played
// enough of it - the same 50-game bar the hand-written season achievements use,
// so a 3-game cameo can't win a scoring title.
export const MIN_SEASON_GP = 50;

export const STAT_SPECS: StatSpec[] = [
	{
		id: "career-pts",
		label: "Career Points",
		unit: "Career Points",
		scope: "career",
		field: "pts",
		perGame: false,
		decimals: 0,
		defaultValue: 20000,
		step: 1000,
	},
	{
		id: "career-trb",
		label: "Career Rebounds",
		unit: "Career Rebounds",
		scope: "career",
		field: "trb",
		perGame: false,
		decimals: 0,
		defaultValue: 10000,
		step: 500,
	},
	{
		id: "career-ast",
		label: "Career Assists",
		unit: "Career Assists",
		scope: "career",
		field: "ast",
		perGame: false,
		decimals: 0,
		defaultValue: 5000,
		step: 500,
	},
	{
		id: "career-stl",
		label: "Career Steals",
		unit: "Career Steals",
		scope: "career",
		field: "stl",
		perGame: false,
		decimals: 0,
		defaultValue: 1500,
		step: 100,
	},
	{
		id: "career-blk",
		label: "Career Blocks",
		unit: "Career Blocks",
		scope: "career",
		field: "blk",
		perGame: false,
		decimals: 0,
		defaultValue: 1500,
		step: 100,
	},
	{
		id: "career-tp",
		label: "Career Threes",
		unit: "Career Threes",
		scope: "career",
		field: "tp",
		perGame: false,
		decimals: 0,
		defaultValue: 1000,
		step: 100,
	},
	{
		id: "career-gp",
		label: "Career Games",
		unit: "Career Games",
		scope: "career",
		field: "gp",
		perGame: false,
		decimals: 0,
		defaultValue: 1000,
		step: 100,
	},
	{
		id: "career-min",
		label: "Career Minutes",
		unit: "Career Minutes",
		scope: "career",
		field: "min",
		perGame: false,
		decimals: 0,
		defaultValue: 30000,
		step: 1000,
	},
	{
		id: "career-seasons",
		label: "Seasons Played",
		unit: "Seasons",
		scope: "career",
		field: "seasons",
		perGame: false,
		decimals: 0,
		defaultValue: 15,
		step: 1,
	},
	{
		id: "season-ppg",
		label: "Points per game",
		unit: "PPG",
		scope: "season",
		field: "pts",
		perGame: true,
		decimals: 1,
		defaultValue: 30,
		step: 1,
	},
	{
		id: "season-rpg",
		label: "Rebounds per game",
		unit: "RPG",
		scope: "season",
		field: "trb",
		perGame: true,
		decimals: 1,
		defaultValue: 12,
		step: 1,
	},
	{
		id: "season-apg",
		label: "Assists per game",
		unit: "APG",
		scope: "season",
		field: "ast",
		perGame: true,
		decimals: 1,
		defaultValue: 10,
		step: 1,
	},
	{
		id: "season-spg",
		label: "Steals per game",
		unit: "SPG",
		scope: "season",
		field: "stl",
		perGame: true,
		decimals: 1,
		defaultValue: 2,
		step: 0.5,
	},
	{
		id: "season-bpg",
		label: "Blocks per game",
		unit: "BPG",
		scope: "season",
		field: "blk",
		perGame: true,
		decimals: 1,
		defaultValue: 2.5,
		step: 0.5,
	},
	{
		id: "season-tpg",
		label: "Threes per game",
		unit: "3PG",
		scope: "season",
		field: "tp",
		perGame: true,
		decimals: 1,
		defaultValue: 3,
		step: 0.5,
	},
	{
		id: "season-mpg",
		label: "Minutes per game",
		unit: "MPG",
		scope: "season",
		field: "min",
		perGame: true,
		decimals: 1,
		defaultValue: 36,
		step: 1,
	},
	{
		id: "season-gp",
		label: "Games in a season",
		unit: "Games (Season)",
		scope: "season",
		field: "gp",
		perGame: false,
		decimals: 0,
		defaultValue: 70,
		step: 1,
	},
];

export const statSpecById = (id: string): StatSpec | undefined =>
	STAT_SPECS.find((s) => s.id === id);

const passes = (actual: number, op: StatOp, value: number) =>
	op === "gte" ? actual >= value : actual <= value;

// Seasons in which this player met a season-scoped threshold. Returned rather
// than a bare boolean so a Team × Season cell can require the player to have
// done it ON that team, which is what makes those cells mean anything.
export const statSeasonsFor = (
	p: TriviaPlayer,
	spec: StatSpec,
	op: StatOp,
	value: number,
): Set<number> => {
	const out = new Set<number>();
	for (const row of p.rows) {
		if (row.gp <= 0) {
			continue;
		}
		if (spec.perGame) {
			if (row.gp < MIN_SEASON_GP) {
				continue;
			}
			const rate = (row[spec.field as "pts"] as number) / row.gp;
			if (passes(rate, op, value)) {
				out.add(row.season);
			}
		} else {
			const total = row[spec.field as "gp"] as number;
			if (passes(total, op, value)) {
				out.add(row.season);
			}
		}
	}
	return out;
};

export const careerStatPasses = (
	p: TriviaPlayer,
	spec: StatSpec,
	op: StatOp,
	value: number,
): boolean => passes(p.tot[spec.field], op, value);

// --- Decades ---------------------------------------------------------------

// Every decade the league has actually reached, for the editor's dropdown -
// offering the 1950s in a league that started in 2025 is just noise.
export const availableDecades = (pool: TriviaPool): number[] => {
	const first = Math.floor(pool.minSeason / 10) * 10;
	const last = Math.floor(pool.maxSeason / 10) * 10;
	const out: number[] = [];
	for (let d = first; d <= last; d += 10) {
		out.push(d);
	}
	return out;
};

export const debutedInDecade = (p: TriviaPlayer, decade: number): boolean =>
	p.firstSeason >= decade && p.firstSeason <= decade + 9;

// Seasons the player logged inside the decade, so "Played in the 1990s" can be
// team-aligned the same way a season achievement is.
export const seasonsInDecade = (
	p: TriviaPlayer,
	decade: number,
): Set<number> => {
	const out = new Set<number>();
	for (const row of p.rows) {
		if (row.gp > 0 && row.season >= decade && row.season <= decade + 9) {
			out.add(row.season);
		}
	}
	return out;
};
