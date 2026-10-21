import getRookieContractLength from "../draft/getRookieContractLength.ts";
import getRookieSalaries from "../draft/getRookieSalaries.ts";
import genContract from "../player/genContract.ts";
import { g } from "../../util/index.ts";
import { getReplacement, type StrengthPlayer } from "./strength.ts";

// HOW TEAMS REFILL. A roster projected forward loses players to age and to
// free agency; in the real league their places do not stay empty, and the
// team with cap room buys someone. Each future season a team's cap room -
// the cap less what it has committed - is spent in free agency at the prices
// teams actually pay there (PRICE_CURVE, from arena leagues run with these
// front offices), so cap room is worth what it buys and a contract that eats
// it costs what it blocks.

// What a free agent who signs for this share of the cap typically is, as the
// value model sees him: roleFree, offense, usage, overall. Read off 602
// signings in 20 arena free agencies (src/test/arena, the freeAgents
// snapshot), the top two bins merged so better always costs more.
const PRICE_CURVE: readonly (readonly [
	number,
	number,
	number,
	number,
	number,
])[] = [
	[0.009, 7.36, 14.63, 0.34, 43.2],
	[0.021, 7.58, 15.58, 0.354, 45.2],
	[0.044, 7.93, 15.51, 0.363, 46.3],
	[0.077, 8.26, 18.22, 0.382, 50],
	[0.121, 8.19, 19.15, 0.384, 50.6],
	[0.173, 8.77, 18.46, 0.385, 52.6],
	[0.226, 10.14, 20.78, 0.432, 58.1],
	[0.3, 9.8, 25, 0.44, 59.7],
];

// Free agents a team signs in one summer, at most, with its room.
const MAX_SIGNINGS = 2;

// Room smaller than this buys nothing worth counting.
const MIN_SHARE = 0.05;

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

// What a player of this value would ask for, by the game's own formula.
export const marketSalary = (value: number) => {
	memo();
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

const curveAt = (share: number) => {
	const s = Math.min(
		PRICE_CURVE.at(-1)![0],
		Math.max(PRICE_CURVE[0]![0], share),
	);
	let i = 0;
	while (i < PRICE_CURVE.length - 2 && PRICE_CURVE[i + 1]![0] < s) {
		i += 1;
	}
	const lo = PRICE_CURVE[i]!;
	const hi = PRICE_CURVE[i + 1]!;
	const t = (s - lo[0]) / (hi[0] - lo[0]);
	return lo.map((x, k) => x + t * (hi[k]! - x));
};

// The free agents a team with this much room is expected to sign.
export const signingsWith = (room: number): StrengthPlayer[] => {
	const cap = g.get("salaryCap");
	const maxShare = PRICE_CURVE.at(-1)![0];
	const out: StrengthPlayer[] = [];
	let left = room / cap;
	while (out.length < MAX_SIGNINGS && left >= MIN_SHARE) {
		const share = Math.min(left, maxShare);
		const [, roleFree, offense, usage, ovr] = curveAt(share);
		out.push({
			roleFree: roleFree!,
			offense: offense!,
			usage: usage!,
			// Who he is is not known yet, so no particular skills.
			synergy: getReplacement().synergy,
			valueNoPot: ovr!,
		});
		left -= share;
	}
	return out;
};

// What a pick's player is paid in his n-th season after the draft (1 is his
// rookie season): his rookie deal, then what a player of his expected overall
// asks for.
export const pickSalary = ({
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
		memo();
		return (
			rookieSalaries[Math.min(slot, rookieSalaries.length - 1)] ??
			g.get("minContract")
		);
	}
	return marketSalary(ovr);
};
