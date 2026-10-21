import getRookieContractLength from "../draft/getRookieContractLength.ts";
import getRookieSalaries from "../draft/getRookieSalaries.ts";
import genContract from "../player/genContract.ts";
import { g } from "../../util/index.ts";
import { getReplacement, type StrengthPlayer } from "./strength.ts";

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
// 1,200 team-summers in 8 arena leagues (src/test/arena, the freeAgents
// snapshot), averaged over rooms within 5% of the cap and smoothed so more
// room never buys less. Teams with room do far
// better than a signing at that price suggests - most end up paying less than
// they have - and what they get rises smoothly with room: there is no special
// prize for clearing a max slot.
const HAUL: readonly (readonly number[])[] = [
	[0, 0.28, 8.61, 16.63, 0.387, 51, 0.07, 7.8, 18, 0.355, 48.2],
	[0.05, 0.82, 8.92, 19.23, 0.403, 54.1, 0.28, 8.29, 18.11, 0.381, 50.5],
	[0.1, 0.96, 9.38, 21.48, 0.421, 56.9, 0.52, 8.48, 18.16, 0.384, 51.4],
	[0.15, 0.96, 10.04, 21.73, 0.444, 59.1, 0.67, 8.56, 18.95, 0.388, 52.4],
	[0.2, 0.96, 10.35, 21.96, 0.451, 60.2, 0.73, 8.58, 20.27, 0.393, 53.5],
	[0.25, 0.96, 10.68, 23.23, 0.464, 61.7, 0.73, 9.02, 20.27, 0.401, 54.3],
	[0.3, 0.96, 11.3, 23.27, 0.487, 63.5, 0.73, 9.22, 20.27, 0.406, 54.6],
	[0.35, 0.96, 11.3, 23.92, 0.487, 63.7, 0.81, 9.22, 20.27, 0.407, 54.9],
	[0.4, 0.99, 11.3, 26.3, 0.489, 64.9, 0.82, 9.37, 21.05, 0.416, 56.3],
	[0.5, 0.99, 11.3, 26.9, 0.489, 64.9, 0.86, 9.37, 22.11, 0.419, 56.8],
];

// Rosters are priced thousands of times a day, so these are remembered for
// the league's contract settings of the moment.
let memoKey = "";
const salaryMemo = new Map<number, number>();
let rookieSalaries: number[] = [];
const memo = () => {
	const key = `${g.get("minContract")}:${g.get("maxContract")}:${g.get("salaryCapType")}:${g.get("numActiveTeams")}:${g.get("numDraftRounds")}`;
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

const haulAt = (share: number) => {
	const s = Math.min(HAUL.at(-1)![0]!, Math.max(HAUL[0]![0]!, share));
	let i = 0;
	while (i < HAUL.length - 2 && HAUL[i + 1]![0]! < s) {
		i += 1;
	}
	const lo = HAUL[i]!;
	const hi = HAUL[i + 1]!;
	const t = (s - lo[0]!) / (hi[0]! - lo[0]!);
	return lo.map((x, k) => x + t * (hi[k]! - x));
};

// The free agents a team with this much room is expected to sign, each with
// the chance it does.
export const signingsWith = (
	room: number,
): (StrengthPlayer & { availability: number })[] => {
	const h = haulAt(room / g.get("salaryCap"));
	return [h.slice(1, 6), h.slice(6, 11)].map(
		([chance, roleFree, offense, usage, ovr]) => ({
			roleFree: roleFree!,
			offense: offense!,
			usage: usage!,
			// Who he is is not known yet, so no particular skills.
			synergy: getReplacement().synergy,
			valueNoPot: ovr!,
			availability: chance!,
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
