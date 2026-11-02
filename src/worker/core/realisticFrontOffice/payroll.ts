import getRookieContractLength from "../draft/getRookieContractLength.ts";
import getRookieSalaries from "../draft/getRookieSalaries.ts";
import genContract from "../player/genContract.ts";
import { g } from "../../util/index.ts";
import {
	typicalSynergy,
	TYPICAL_FORWARD,
	TYPICAL_GUARD,
	type StrengthPlayer,
} from "./strength.ts";

// HOW TEAMS REFILL. A roster projected forward loses players to age and to
// free agency; in the real league their places do not stay empty, and the
// team with cap room buys someone. Each future season a team's cap room - the
// cap less what it has committed - is expected to bring in what teams with
// that much room actually ended up signing in arena free agencies (HAUL), so
// cap room is worth what it really buys and a contract that eats it costs what
// it blocks.

// By cap room as free agency opens (share of the cap): the chance of signing a
// first and a second free agent better than the minimum, and what they are as
// the value model sees them (roleFree, offense, usage, overall). Read off
// 900 team-summers in 6 arena leagues under the offer market with picks
// priced honestly (src/test/arena, the freeAgents snapshot), averaged over
// rooms within 5% of the cap and smoothed so more room never buys less. Teams
// with room do far better than a signing at that price suggests - most end up
// paying less than they have - and what they get rises smoothly with room:
// there is no special prize for clearing a max slot. Under the offer market
// room buys less than it did before it - a first signing of 60 at 40% of the
// cap, not 65 - and even a team with none lands someone better than the
// minimum three times in four. A half step toward 300 team-summers under the
// table it replaced: with room priced lower fewer teams keep it, and those
// without do better still. And another toward 1,200 team-summers once money
// past the cap cost something and a signing was priced up to the maximum:
// teams bid less for role players, so room buys less again - with none, a
// team landed someone better than the minimum half the time, not three in
// four.
const HAUL: readonly (readonly number[])[] = [
	[0, 0.61, 8.67, 17.77, 0.397, 52.25, 0.35, 8.03, 17.94, 0.378, 49.55],
	[0.05, 0.81, 9.12, 18.77, 0.412, 54.15, 0.5, 8.36, 18.48, 0.388, 50.55],
	[0.1, 0.91, 9.44, 20.49, 0.425, 56.35, 0.66, 8.73, 17.88, 0.394, 51.8],
	[0.15, 0.95, 9.59, 20.81, 0.43, 57.15, 0.72, 8.91, 17.99, 0.4, 52.85],
	[0.2, 0.95, 9.78, 21.32, 0.44, 58.15, 0.74, 8.92, 18.75, 0.402, 53.55],
	[0.25, 0.95, 10.0, 22.41, 0.444, 59.35, 0.78, 9.06, 18.95, 0.407, 54.3],
	[0.3, 0.98, 10.25, 23.2, 0.448, 60.25, 0.84, 9.34, 18.05, 0.414, 54.55],
	[0.35, 0.98, 10.22, 23.82, 0.451, 60.5, 0.89, 9.34, 18.05, 0.414, 54.55],
	[0.4, 0.98, 10.22, 23.82, 0.451, 60.5, 0.89, 9.16, 19.68, 0.416, 54.75],
	[0.5, 0.99, 10.22, 23.82, 0.451, 60.5, 0.95, 9.43, 19.52, 0.42, 55.3],
];

// Rosters are priced thousands of times a day, so these are remembered for
// the league's contract settings of the moment.
let memoKey = "";
const salaryMemo = new Map<number, number>();
let rookieSalaries: number[] = [];
const memo = () => {
	const key = `${g.get("minContract")}:${g.get("maxContract")}:${g.get("salaryCapType")}:${g.get("numActiveTeams")}:${g.get("numDraftRounds")}:${g.get("draftPickAutoContractPercent")}:${g.get("draftPickAutoContractRounds")}`;
	if (key !== memoKey) {
		memoKey = key;
		salaryMemo.clear();
		rookieSalaries = getRookieSalaries();
	}
};

const salaryFor = (value: number) => {
	const v = Math.round(value * 10) / 10;
	let amount = salaryMemo.get(v);
	if (amount === undefined) {
		amount = genContract(
			{ value: v, ratings: [{ pos: "F" }] } as any,
			false,
		).amount;
		salaryMemo.set(v, amount);
	}
	return amount;
};

// What a player of this value would ask for, by the game's own formula.
export const marketSalary = (value: number) => {
	memo();
	return salaryFor(value);
};

// The same for many players at once, with the league's settings looked up
// once - for as long as they cannot change, as while pricing one roster.
export const marketSalaries = () => {
	memo();
	return salaryFor;
};

// The same by how good the team is, in the season the room is for: free
// agents want to win, so a bad team's room buys less - with 30% of the cap, a
// first signing of 56 for a team that won a quarter of its games, 61 for an
// average one, 66 for a good one. Read off 1,380 team-summers in 8 arena
// leagues, by each team's record the season before, rows as HAUL's (fewer:
// fewer summers each); good teams with a lot of room are rare, so its last
// row is its 30% one. A half step toward 1,200 team-summers since, as HAUL:
// with no room, a bad team landed someone better than the minimum 30% of
// the time, not 74%.
const HAUL_BY_RECORD: readonly {
	winp: number;
	rows: readonly (readonly number[])[];
}[] = [
	{
		winp: 0.262,
		rows: [
			[0, 0.52, 8.53, 14.72, 0.39, 49.49, 0.24, 7.97, 15.48, 0.374, 47.59],
			[0.1, 0.85, 9.3, 17.83, 0.414, 54.41, 0.54, 8.52, 16.53, 0.381, 50.13],
			[0.2, 0.89, 9.32, 18.42, 0.42, 55.23, 0.69, 8.59, 17.62, 0.394, 51.75],
			[0.3, 0.96, 9.51, 20.16, 0.427, 56.78, 0.82, 8.59, 17.62, 0.394, 51.75],
			[0.45, 0.96, 9.84, 19.36, 0.437, 57.07, 0.88, 9.43, 16.22, 0.417, 53.92],
		],
	},
	{
		winp: 0.504,
		rows: [
			[0, 0.55, 8.43, 17.64, 0.394, 51.24, 0.28, 8.01, 16.92, 0.378, 48.44],
			[0.1, 0.93, 9.34, 21.51, 0.424, 56.7, 0.71, 8.53, 18.99, 0.39, 51.94],
			[0.2, 0.93, 9.91, 20.88, 0.438, 58.28, 0.71, 8.86, 19.08, 0.398, 53.09],
			[0.3, 1, 10.59, 22.63, 0.444, 60.68, 0.74, 9.2, 18.55, 0.409, 54.74],
			[0.45, 1, 10.41, 23.42, 0.449, 60.68, 0.9, 9.42, 19.59, 0.417, 55.69],
		],
	},
	{
		winp: 0.688,
		rows: [
			[0, 0.72, 8.74, 18.25, 0.401, 53.01, 0.45, 8.07, 18.13, 0.377, 50.02],
			[0.1, 0.99, 9.54, 23.81, 0.438, 58.65, 0.86, 8.91, 19.09, 0.397, 53.18],
			[0.2, 1, 10.26, 23.95, 0.455, 60.91, 0.88, 8.73, 20.22, 0.398, 53.97],
			[0.3, 1, 11.34, 26.3, 0.484, 65.02, 0.98, 9.42, 19.21, 0.408, 55.89],
		],
	},
];

const rowAt = (table: readonly (readonly number[])[], share: number) => {
	const s = Math.min(table.at(-1)![0]!, Math.max(table[0]![0]!, share));
	let i = 0;
	while (i < table.length - 2 && table[i + 1]![0]! < s) {
		i += 1;
	}
	const lo = table[i]!;
	const hi = table[i + 1]!;
	const t = (s - lo[0]!) / (hi[0]! - lo[0]!);
	return lo.map((x, k) => x + t * (hi[k]! - x));
};

const haulAt = (share: number, winp: number | undefined) => {
	if (winp === undefined) {
		return rowAt(HAUL, share);
	}
	const tables = HAUL_BY_RECORD;
	const w = Math.min(tables.at(-1)!.winp, Math.max(tables[0]!.winp, winp));
	let i = 0;
	while (i < tables.length - 2 && tables[i + 1]!.winp < w) {
		i += 1;
	}
	const lo = rowAt(tables[i]!.rows, share);
	const hi = rowAt(tables[i + 1]!.rows, share);
	const t = (w - tables[i]!.winp) / (tables[i + 1]!.winp - tables[i]!.winp);
	return lo.map((x, k) => x + t * (hi[k]! - x));
};

// During free agency, how much of the market the coming season's room was
// measured against is still out there: the quality of the free agents left
// against the quality as free agency opened - set each day of free agency
// (freeAgency.ts) and good for that day only.
let poolLeft: { key: string; share: number } | undefined;

const poolKey = () =>
	`${g.get("lid")}:${g.get("season")}:${g.get("phase")}:${g.get("daysLeft")}`;

export const setFreeAgentPoolLeft = (share: number) => {
	poolLeft = { key: poolKey(), share };
};

// The free agents a team with this much room is expected to sign, each with
// the chance it does. `duringFreeAgency`: room in the free agency under way,
// whose chances shrink as the best free agents sign.
export const signingsWith = (
	room: number,
	duringFreeAgency?: boolean,
	// The share of its games the team is expected to win that season: room
	// priced by it (HAUL_BY_RECORD), or the same for every team (HAUL).
	winp?: number,
): (StrengthPlayer & { availability: number })[] => {
	const h = haulAt(room / g.get("salaryCap"), winp);
	const left =
		duringFreeAgency && poolLeft?.key === poolKey() ? poolLeft.share : 1;
	return [h.slice(1, 6), h.slice(6, 11)].map(
		([chance, roleFree, offense, usage, ovr]) => ({
			roleFree: roleFree!,
			offense: offense!,
			usage: usage!,
			// Who he is is not known yet: a typical player of his overall.
			synergy: typicalSynergy(ovr!),
			guard: TYPICAL_GUARD,
			forward: TYPICAL_FORWARD,
			valueNoPot: ovr!,
			availability: chance! * left,
		}),
	);
};

// What a pick's player is paid in his n-th season after the draft (1 is his
// rookie season): his rookie deal, then what a player of his expected overall
// asks for.
const pickSalaryFor = ({
	slot,
	round,
	years,
	ovr,
}: {
	slot: number;
	round: number;
	years: number;
	ovr: number;
}) => {
	if (years <= getRookieContractLength(round)) {
		return (
			rookieSalaries[Math.min(slot, rookieSalaries.length - 1)] ??
			g.get("minContract")
		);
	}
	return salaryFor(ovr);
};

export const pickSalary = (pick: Parameters<typeof pickSalaryFor>[0]) => {
	memo();
	return pickSalaryFor(pick);
};

// The same for many picks at once, like marketSalaries.
export const pickSalaries = () => {
	memo();
	return pickSalaryFor;
};
