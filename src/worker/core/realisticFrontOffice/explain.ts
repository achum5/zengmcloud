import type {
	DraftPick,
	FrontOfficeAsset,
	FrontOfficeBreakdown,
	FrontOfficeSide,
	Player,
} from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import {
	curveAt,
	draftPlaceSpread,
	expectedPlace,
	OUTLOOK_YEARS,
	outlookSeason,
	pickLineAt,
	retentionOf,
	seasonLine,
	seasonValue,
	teamStrengths,
	type LeagueOutlook,
} from "./outlook.ts";
import type { TradedAway } from "./tradePenalty.ts";

// THE WHOLE CALCULATION behind a deal, for the trade page and the arena's
// audit: each side's strength and title odds season by season without and
// with it, what every asset is expected to be, and a one-line reason.

export type ExplainSide = {
	tid: number;
	players: readonly Player[];
	picks: readonly DraftPick[];
	tradedAway: TradedAway;
	gain: number;
	margin: number;
	gives: { players: readonly Player[]; picks: readonly DraftPick[] };
	gets: { players: readonly Player[]; picks: readonly DraftPick[] };
};

const round = (x: number, places: number) => {
	const f = 10 ** places;
	return Math.round(x * f) / f;
};

const years = Array.from({ length: OUTLOOK_YEARS + 1 }, (_, y) => y);

const playerAsset = (
	outlook: LeagueOutlook,
	p: Player,
	tid: number,
	effect: number[],
): FrontOfficeAsset => {
	const reSign = retentionOf(outlook, p.pid, tid);
	const lines = years.map((y) =>
		seasonLine(
			outlook.lines,
			p,
			outlook.season,
			outlook.offset,
			y,
			outlook.remaining.get(tid) ?? 0,
			reSign,
		),
	);
	return {
		kind: "player",
		pid: p.pid,
		name: `${p.firstName} ${p.lastName}`,
		age: g.get("season") - p.born.year,
		ovr: p.ratings.at(-1)!.ovr,
		amount: p.contract.amount,
		exp: p.contract.exp,
		ovrBySeason: lines.map((line) => round(line.valueNoPot, 0)),
		hereBySeason: lines.map((line) => round(line.availability, 2)),
		reSign,
		effect,
	};
};

const pickAsset = (
	outlook: LeagueOutlook,
	dp: DraftPick,
	effect: number[],
): FrontOfficeAsset => {
	const at = years.map((y) => pickLineAt(outlook, dp, y));
	return {
		kind: "pick",
		dpid: dp.dpid,
		season: dp.season as number,
		round: dp.round,
		originalTid: dp.originalTid,
		place: expectedPlace(outlook, dp),
		spread: round(draftPlaceSpread(outlook, dp), 1),
		ovrBySeason: at.map((x) => (x ? round(x.line.valueNoPot, 0) : 0)),
		hereBySeason: at.map((x) => (x ? round(x.line.availability, 2) : 0)),
		effect,
	};
};

// Where the value comes from, in titles x100: this season, the next two, and
// after.
const reasonFor = (
	outlook: LeagueOutlook,
	value: readonly number[],
	gain: number,
	margin: number,
) => {
	const sum = (from: number, to: number) =>
		value.slice(from, to).reduce((a, x) => a + x, 0);
	const now = sum(0, 1);
	const soon = sum(1, 3);
	const later = sum(3, value.length);
	const fmt = (x: number) => `${x >= 0 ? "+" : ""}${(100 * x).toFixed(2)}`;
	const first = outlookSeason(outlook, 0);
	return `gain ${fmt(gain)} (margin ${(100 * margin).toFixed(2)}): ${fmt(now)} in ${first}, ${fmt(soon)} in ${first + 1}-${first + 2}, ${fmt(later)} after; titles x100`;
};

export const explainDeal = (
	outlook: LeagueOutlook,
	sides: readonly [ExplainSide, ExplainSide],
): FrontOfficeBreakdown => {
	const explained = sides.map((side): FrontOfficeSide => {
		const outP = new Set(side.gives.players.map((p) => p.pid));
		const outD = new Set(side.gives.picks.map((dp) => dp.dpid));
		const playersAfter = [
			...side.players.filter((p) => !outP.has(p.pid)),
			...side.gets.players,
		];
		const picksAfter = [
			...side.picks.filter((dp) => !outD.has(dp.dpid)),
			...side.gets.picks,
		];
		const room = [years.map(() => 0), years.map(() => 0)];
		const buys: [number, number][][][] = [
			years.map(() => []),
			years.map(() => []),
		];
		const refill =
			(k: 0 | 1) =>
			(
				y: number,
				share: number,
				signings: readonly { valueNoPot: number; availability: number }[],
			) => {
				room[k]![y] = round(share, 3);
				buys[k]![y] = signings.map((x) => [
					round(x.valueNoPot, 0),
					round(x.availability, 2),
				]);
			};
		const before = teamStrengths(
			outlook,
			side.tid,
			side.players,
			side.picks,
			undefined,
			refill(0),
		);
		const after = teamStrengths(
			outlook,
			side.tid,
			playersAfter,
			picksAfter,
			{
				tradedAway: side.tradedAway,
				extra: side.gives.players.length,
			},
			refill(1),
		);
		// One asset at a time, against the roster as it stands.
		const effectOf = (
			players: readonly Player[],
			picks: readonly DraftPick[],
		) =>
			teamStrengths(outlook, side.tid, players, picks).map((x, y) =>
				round(x - before[y]!, 2),
			);
		const value = years.map(
			(y) =>
				seasonValue(outlook, y, after[y]!) -
				seasonValue(outlook, y, before[y]!),
		);
		const title = (path: readonly number[]) =>
			path.map((s, y) => round(curveAt(outlook.titleCurves[y]!, s), 4));
		return {
			tid: side.tid,
			gain: round(side.gain, 5),
			margin: round(side.margin, 5),
			before: before.map((x) => round(x, 2)),
			after: after.map((x) => round(x, 2)),
			titleBefore: title(before),
			titleAfter: title(after),
			value: value.map((x) => round(x, 5)),
			roomBefore: room[0]!,
			roomAfter: room[1]!,
			buysBefore: buys[0]!,
			buysAfter: buys[1]!,
			gets: [
				...side.gets.players.map((p) =>
					playerAsset(
						outlook,
						p,
						side.tid,
						effectOf([...side.players, p], side.picks),
					),
				),
				...side.gets.picks.map((dp) =>
					pickAsset(outlook, dp, effectOf(side.players, [...side.picks, dp])),
				),
			],
			gives: [
				...side.gives.players.map((p) =>
					playerAsset(
						outlook,
						p,
						side.tid,
						effectOf(
							side.players.filter((x) => x.pid !== p.pid),
							side.picks,
						),
					),
				),
				...side.gives.picks.map((dp) =>
					pickAsset(
						outlook,
						dp,
						effectOf(
							side.players,
							side.picks.filter((x) => x.dpid !== dp.dpid),
						),
					),
				),
			],
			tradedAway: [...side.tradedAway],
			reason: reasonFor(outlook, value, side.gain, side.margin),
		};
	});
	return {
		seasons: years.map((y) => outlookSeason(outlook, y)),
		sides: [explained[0]!, explained[1]!],
	};
};
