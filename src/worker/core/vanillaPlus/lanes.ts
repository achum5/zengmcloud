import { PHASE } from "../../../common/constants.ts";
import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import type {
	TradePlan,
	TradeValueAsset,
} from "../team/ValueChangeCalculator.ts";
import { ovr as ovrBasketball } from "../team/ovr.basketball.ts";
import { laneWatchers } from "./events.ts";

// LANES (docs/vanilla-plus.md): every team has a plan, as real front offices
// do, and it moves what the team pays for what.
//
//   all-in      a real shot at the title now: pays for proven players, with
//               picks and prospects;
//   playoff     good, not great: upgrades that don't cost its future;
//   retool      aging or stuck in the middle: gets younger without
//               bottoming out;
//   rebuild     bad: collects picks, prospects and expiring deals for its
//               veterans.
//
// Reviewed each offseason (after the playoffs), in the preseason once ratings
// have moved, at mid-season and late in the season, before the deadline.
// Sticky: in the season a team moves a lane only on clear evidence.
export type Lane = "rebuild" | "retool" | "playoff" | "allIn";

const ORDER: Record<Lane, number> = {
	rebuild: 0,
	retool: 1,
	playoff: 2,
	allIn: 3,
};

export const laneSettings = {
	// Shares of the league, best first: all-in, then playoff teams.
	allInShare: 0.2,
	playoffShare: 0.5,
	// Below the playoff teams: the worst this share rebuild, and any whose
	// core (best eight by value) is this young; the rest retool if their core
	// is this old, else try for the playoffs.
	rebuildShare: 0.25,
	youngCoreAge: 24,
	retoolAge: 26.5,
	// In the offseason an all-in team stays all-in while it is in this share.
	allInStays: 0.3,
	// How much of a team's standing is its record, at most, once games are
	// played (the rest its rating).
	recordWeight: 0.6,
};

// Each lane's weighing of an asset: a factor on vanilla's value, as stock's
// two strategies have (team/ValueChangeCalculator.ts sumValues), and how much
// contracts count. All-in is stock's contending with a little more for proven
// players; rebuild is stock's rebuilding; the two between are milder.
const PLANS: Record<Lane, TradePlan> = {
	allIn: {
		factor: (p, futurePick) =>
			futurePick
				? 0.825
				: p.age <= 19
					? 0.8
					: p.age === 20
						? 0.825
						: p.age === 21
							? 0.85
							: p.age === 22
								? 0.875
								: p.age === 23
									? 0.925
									: p.age === 24
										? 0.95
										: p.age <= 31
											? 1.02
											: 1,
		contractsFactor: 0.25,
	},
	playoff: {
		factor: (p, futurePick) =>
			futurePick
				? 0.925
				: p.age <= 20
					? 0.925
					: p.age <= 22
						? 0.95
						: p.age === 23
							? 0.975
							: 1,
		contractsFactor: 0.5,
	},
	retool: {
		factor: (p, futurePick) =>
			futurePick
				? 1.025
				: p.age <= 21
					? 1.025
					: p.age <= 26
						? 1.0125
						: p.age === 29
							? 0.975
							: p.age <= 31 && p.age >= 30
								? 0.95
								: p.age >= 32
									? 0.925
									: 1,
		contractsFactor: 1.25,
	},
	rebuild: {
		factor: (p, futurePick) =>
			futurePick
				? 1.1
				: p.age <= 19
					? 1.075
					: p.age === 20
						? 1.05
						: p.age === 21
							? 1.0375
							: p.age === 22
								? 1.025
								: p.age === 23
									? 1.0125
									: p.age === 27
										? 0.975
										: p.age === 28
											? 0.95
											: p.age >= 29
												? 0.9
												: 1,
		contractsFactor: 2,
	},
};

export const planFor = (lane: Lane | undefined): TradePlan | undefined =>
	lane ? PLANS[lane] : undefined;

// What a team is, for its lane.
type Facts = {
	tid: number;
	ovr: number;
	coreAge: number;
	winPct: number | undefined;
};

const factsOf = async (): Promise<{ facts: Facts[]; progress: number }> => {
	const season = g.get("season");
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	const byTid = new Map<number, Player[]>();
	for (const p of players) {
		const list = byTid.get(p.tid) ?? [];
		list.push(p);
		byTid.set(p.tid, list);
	}
	const facts: Facts[] = [];
	let gp = 0;
	let counted = 0;
	for (const t of await idb.cache.teams.getAll()) {
		if (t.disabled) {
			continue;
		}
		const roster = byTid.get(t.tid) ?? [];
		const ovr = ovrBasketball(
			roster.map((p) => ({
				ratings: { ovr: p.ratings.at(-1)!.ovr, pos: p.ratings.at(-1)!.pos },
			})),
			{},
		);
		const core = [...roster].sort((a, b) => b.value - a.value).slice(0, 8);
		const coreAge =
			core.length > 0
				? core.reduce((s, p) => s + (season - p.born.year), 0) / core.length
				: 26;
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
			season,
			t.tid,
		]);
		const games = ts ? ts.won + ts.lost + (ts.tied ?? 0) + (ts.otl ?? 0) : 0;
		gp += games;
		counted += 1;
		facts.push({
			tid: t.tid,
			ovr,
			coreAge,
			winPct: games > 0 ? (ts!.won + 0.5 * (ts!.tied ?? 0)) / games : undefined,
		});
	}
	const progress =
		g.get("phase") === PHASE.REGULAR_SEASON && counted > 0
			? gp / counted / g.get("numGames")
			: 0;
	return { facts, progress };
};

// When lanes are reviewed: the review this moment belongs to.
const reviewOf = (progress: number) => {
	const season = g.get("season");
	const phase = g.get("phase");
	if (phase < PHASE.REGULAR_SEASON) {
		return { key: `${season}:pre`, inSeason: false };
	}
	if (phase === PHASE.REGULAR_SEASON) {
		if (progress < 0.4) {
			return { key: `${season}:pre`, inSeason: false };
		}
		return {
			key: progress < 0.6 ? `${season}:mid` : `${season}:late`,
			inSeason: true,
		};
	}
	return { key: `${season}:off`, inSeason: false };
};

// The lane its standing puts a team in, before stickiness.
const freshLane = (
	f: Facts,
	rank: number,
	n: number,
	previous: Lane | undefined,
	inSeason: boolean,
): Lane => {
	const s = laneSettings;
	if (rank <= Math.ceil(s.allInShare * n)) {
		return "allIn";
	}
	if (
		!inSeason &&
		previous === "allIn" &&
		rank <= Math.ceil(s.allInStays * n)
	) {
		return "allIn";
	}
	if (rank <= Math.ceil(s.playoffShare * n)) {
		return "playoff";
	}
	// The bottom half: the worst rebuild, and a young team below the middle;
	// an old one retools; the rest try for the playoffs.
	if (rank > n - Math.ceil(s.rebuildShare * n) || f.coreAge <= s.youngCoreAge) {
		return "rebuild";
	}
	if (f.coreAge >= s.retoolAge) {
		return "retool";
	}
	return "playoff";
};

let current: { key: string; lanes: Map<number, Lane> } | undefined;

// Every team's lane at this moment, reviewed if this moment's review has not
// been done.
export const ensureLanes = async (): Promise<Map<number, Lane>> => {
	const { facts, progress } = await factsOf();
	const review = reviewOf(progress);
	const lid = g.get("lid");
	const key = `${lid}:${review.key}`;
	if (current?.key === key) {
		return current.lanes;
	}
	const teams = new Map(
		(await idb.cache.teams.getAll()).map((t) => [t.tid, t]),
	);
	// Standing: the rating's rank, and once games are played its record's too.
	const n = facts.length;
	const byOvr = [...facts].sort((a, b) => b.ovr - a.ovr);
	const ovrRank = new Map(byOvr.map((f, i) => [f.tid, i + 1]));
	const w = review.inSeason ? Math.min(laneSettings.recordWeight, progress) : 0;
	const byRecord = [...facts].sort(
		(a, b) => (b.winPct ?? 0.5) - (a.winPct ?? 0.5),
	);
	const recordRank = new Map(byRecord.map((f, i) => [f.tid, i + 1]));
	const standing = facts
		.map((f) => ({
			f,
			score: (1 - w) * ovrRank.get(f.tid)! + w * recordRank.get(f.tid)!,
		}))
		.sort((a, b) => a.score - b.score);
	const lanes = new Map<number, Lane>();
	for (const [i, { f }] of standing.entries()) {
		const t = teams.get(f.tid)!;
		const previous = t.lane;
		let lane = freshLane(f, i + 1, n, previous, review.inSeason);
		// Sticky in the season: a lane apart, not before late in the season.
		if (
			review.inSeason &&
			previous &&
			Math.abs(ORDER[lane] - ORDER[previous]) < 2 &&
			!review.key.endsWith(":late")
		) {
			lane = previous;
		}
		lanes.set(f.tid, lane);
		if (t.laneReview !== review.key || t.lane !== lane) {
			if (previous !== lane) {
				for (const watch of laneWatchers) {
					watch({ tid: f.tid, from: previous, to: lane, review: review.key });
				}
			}
			t.lane = lane;
			t.laneReview = review.key;
			// Stock's code reads its two strategies.
			t.strategy = ORDER[lane] >= ORDER.playoff ? "contending" : "rebuilding";
			await idb.cache.teams.put(t);
		}
	}
	current = { key, lanes };
	return lanes;
};

// Each lane's weighing, for the calculator (TradeValueAsset re-exported for
// the tweaks).
export type { TradeValueAsset };
