import type {
	FrontOfficeBreakdown,
	FrontOfficeSide,
} from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import type { LeagueOutlook } from "./outlook.ts";

// IN PLAIN WORDS, for a deal held for bids (windows.ts): why each team makes
// it - where its gain falls in its own calculation - and what it would want
// most in a bid for what it gives: more of what the deal gets it.

export type DealNote = { why: string; wants: string };

// Players this young count with picks, as the future.
const YOUNG = 24;
// A contender: this share of a title or better this season.
const CONTENDER = 0.05;
// Rebuilding: this many points a game worse than average or more.
const REBUILDING = -3;

const total = (xs: readonly number[] | undefined, from = 0, to?: number) =>
	(xs ?? []).slice(from, to).reduce((a, x) => a + x, 0);

const whyFor = (outlook: LeagueOutlook, side: FrontOfficeSide) => {
	const money = total(side.money);
	const rival = total(side.rival);
	// The roster's part: the value with money and the rival taken out.
	const play = side.value.map(
		(x, y) => x - (side.money?.[y] ?? 0) - (side.rival?.[y] ?? 0),
	);
	const now = total(play, 0, 2);
	const later = total(play, 2);
	const o = outlook.teams.get(side.tid);
	const contender = (o?.title[0] ?? 0) >= CONTENDER;
	const rebuilding = (o?.strength[0] ?? 0) <= REBUILDING;
	const best = Math.max(now, later, money, rival);
	if (best <= 0) {
		return "A small gain all round";
	}
	if (best === money) {
		return "Saves money";
	}
	if (best === rival) {
		return "Keeps a player from a rival";
	}
	if (best === now) {
		if (contender) {
			return "A contender adding for a title run";
		}
		return later < 0 ? "Better now, at a cost later" : "Better now and later";
	}
	if (rebuilding) {
		return "Rebuilding, for later";
	}
	return now < 0 ? "Better later, at a cost now" : "Better later and now";
};

const wantsFor = (side: FrontOfficeSide) => {
	let help = 0;
	let future = 0;
	for (const x of side.gets) {
		if (x.kind === "pick" || x.age <= YOUNG) {
			future += 1;
		} else {
			help += 1;
		}
	}
	const salary = (assets: FrontOfficeSide["gets"]) =>
		assets.reduce((a, x) => a + (x.kind === "player" ? x.amount : 0), 0);
	// Cap relief when the money it saves counts for it, not merely moves.
	const relief =
		total(side.money) > 0 &&
		salary(side.gives) - salary(side.gets) > 0.05 * g.get("salaryCap");
	const kinds = [
		...(help > 0 && help >= future ? ["players who help now"] : []),
		...(future > 0 && future >= help ? ["young players and picks"] : []),
		...(relief ? ["cap relief"] : []),
	];
	if (kinds.length === 0) {
		return "The best offer";
	}
	const text = kinds.join(" and ");
	return text[0]!.toUpperCase() + text.slice(1);
};

export const dealNotes = (
	outlook: LeagueOutlook,
	breakdown: FrontOfficeBreakdown,
): [DealNote, DealNote] => [
	{
		why: whyFor(outlook, breakdown.sides[0]),
		wants: wantsFor(breakdown.sides[0]),
	},
	{
		why: whyFor(outlook, breakdown.sides[1]),
		wants: wantsFor(breakdown.sides[1]),
	},
];
