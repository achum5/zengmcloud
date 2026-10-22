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
// without do better still.
const HAUL: readonly (readonly number[])[] = [
	[0, 0.74, 8.63, 18.38, 0.394, 52.4, 0.46, 7.9, 18.38, 0.37, 49.1],
	[0.05, 0.91, 9.32, 18.3, 0.414, 54.9, 0.64, 8.51, 16.91, 0.387, 50.3],
	[0.1, 0.99, 9.63, 18.59, 0.425, 56.2, 0.75, 8.8, 16.16, 0.394, 51.2],
	[0.15, 0.99, 9.69, 19.91, 0.433, 57.3, 0.79, 8.93, 17.66, 0.404, 53.2],
	[0.2, 0.99, 9.83, 22.14, 0.448, 59, 0.83, 8.93, 19.28, 0.407, 54.3],
	[0.25, 0.99, 9.9, 23.91, 0.455, 60.3, 0.91, 9.04, 19.79, 0.416, 55],
	[0.3, 0.99, 10.03, 23.4, 0.453, 60.3, 0.96, 9.18, 19.67, 0.416, 55.1],
	[0.35, 0.99, 10.03, 23.4, 0.453, 60.3, 0.96, 9.18, 19.67, 0.416, 55.1],
	[0.4, 0.99, 10.03, 23.4, 0.453, 60.3, 0.96, 9.18, 19.67, 0.416, 55.1],
	[0.5, 1, 10.03, 23.4, 0.453, 60.3, 1, 9.18, 19.67, 0.416, 55.1],
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
// row is its 30% one.
const HAUL_BY_RECORD: readonly {
	winp: number;
	rows: readonly (readonly number[])[];
}[] = [
	{
		winp: 0.262,
		rows: [
			[0, 0.74, 8.23, 15.01, 0.381, 49.5, 0.39, 7.8, 14.56, 0.369, 47.1],
			[0.1, 1, 9.44, 16.56, 0.422, 55, 0.75, 8.41, 15.89, 0.38, 50.1],
			[0.2, 1, 9.27, 18.1, 0.422, 55.8, 0.87, 8.52, 17.39, 0.4, 51.9],
			[0.3, 1, 9.33, 18.98, 0.429, 56.4, 0.95, 8.52, 17.39, 0.4, 51.9],
			[0.45, 1, 9.99, 17.57, 0.441, 56.9, 1, 9.12, 16.09, 0.412, 53.4],
		],
	},
	{
		winp: 0.504,
		rows: [
			[0, 0.7, 8.36, 17.5, 0.39, 51, 0.42, 7.99, 16.23, 0.377, 48.4],
			[0.1, 1, 9.37, 20.23, 0.42, 56.4, 0.82, 8.54, 17.58, 0.385, 51.3],
			[0.2, 1, 9.63, 23.3, 0.438, 58.8, 0.82, 8.7, 20.66, 0.401, 53.8],
			[0.3, 1, 10.63, 21.38, 0.448, 60.7, 0.84, 8.67, 20.39, 0.404, 54.2],
			[0.45, 1, 10.27, 22.97, 0.458, 60.7, 1, 9.45, 17.42, 0.421, 55.2],
		],
	},
	{
		winp: 0.688,
		rows: [
			[0, 0.8, 8.7, 19.02, 0.401, 53.1, 0.52, 7.95, 18.67, 0.365, 49.4],
			[0.1, 0.98, 9.86, 22.45, 0.439, 58.8, 0.82, 8.85, 18.93, 0.397, 52.8],
			[0.2, 1, 11.2, 20.41, 0.472, 61.4, 0.86, 8.42, 20.5, 0.385, 52.9],
			[0.3, 1, 11.35, 27.63, 0.494, 66.1, 1, 9.51, 18.73, 0.406, 55.9],
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
