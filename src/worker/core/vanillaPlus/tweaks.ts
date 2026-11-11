import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import type { TradeValueAsset } from "../team/ValueChangeCalculator.ts";
import {
	playerLine,
	rosterPoints,
	type StrengthPlayer,
	type ValueRatings,
} from "../realisticFrontOffice/strength.ts";
import type { Lane } from "./lanes.ts";
import { timingFactor } from "./timing.ts";

// THE TWEAKS (docs/vanilla-plus.md): small, capped multipliers on a player's
// trade value by vanilla's reckoning, for what vanilla can't see.
//
// - Fit: what he adds to this team's lineup (or what losing him takes from
//   it), by the position-aware lineup model (realisticFrontOffice/
//   strength.ts), against what he adds to a typical team's. A team with no
//   real center gets more from a center than most teams do.
// - Aggression: an all-in team pays more again for a player who fills a hole.
// - Production: what his box score says (BPM over the last season and a
//   half) past or short of what his rating usually brings.
//
// All together within totalCap of vanilla's trade value, so vanilla still
// decides. Then timing (timing.ts) scales it by when he helps, for this
// team's lane - and a team giving a player away counts him at no less than
// vanilla's value.
export const tweakSettings = {
	// Fit: this share of how far his fit ratio is from 1, within the cap.
	// The ratio counts this many points a game on both sides, so a bench
	// player's (a few tenths either way) doesn't swing on noise.
	fitShare: 0.5,
	fitCap: 0.15,
	fitShrink: 0.5,
	// Aggression: all-in teams, for a player whose fit ratio is this or more.
	aggressionAt: 1.25,
	aggression: 0.1,
	// Production: per point of BPM past what his rating predicts.
	productionPerPoint: 0.02,
	productionCap: 0.1,
	// Minutes, this season and last, before his box score counts.
	productionMinutes: 500,
	totalCap: 0.25,
};

const clamp = (x: number, cap: number) => Math.max(-cap, Math.min(cap, x));

const lineOf = (p: Player): StrengthPlayer => ({
	...playerLine(p.ratings.at(-1)! as unknown as ValueRatings),
	valueNoPot: p.valueNoPot,
});

// BPM and minutes this season and last, regular season.
const boxOf = (p: Player, season: number) => {
	let min = 0;
	let bpm = 0;
	for (const row of p.stats as any[]) {
		if (
			!row.playoffs &&
			(row.season === season || row.season === season - 1) &&
			row.min > 0 &&
			typeof row.obpm === "number"
		) {
			min += row.min;
			bpm += (row.obpm + row.dbpm) * row.min;
		}
	}
	return { min, bpm: min > 0 ? bpm / min : 0 };
};

export type Tweaks = (
	tid: number,
	asset: TradeValueAsset,
	side: "add" | "remove",
) => number;

// The tweaks as the league stands now: rosters, each team's lineup, and what
// a rating's box score usually is.
export const tweaksFor = async (lanes: Map<number, Lane>): Promise<Tweaks> => {
	const s = tweakSettings;
	const season = g.get("season");
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	const byPid = new Map(players.map((p) => [p.pid, p]));
	const rosters = new Map<number, StrengthPlayer[]>();
	const lines = new Map<number, StrengthPlayer>();
	for (const p of players) {
		const line = lineOf(p);
		lines.set(p.pid, line);
		const list = rosters.get(p.tid) ?? [];
		list.push(line);
		rosters.set(p.tid, list);
	}
	const points = new Map<number, number>();
	for (const [tid, roster] of rosters) {
		points.set(tid, rosterPoints(roster));
	}

	// What he adds to a typical team: the average over every other team.
	const typical = new Map<number, number>();
	const typicalGain = (p: Player) => {
		let out = typical.get(p.pid);
		if (out === undefined) {
			const line = lines.get(p.pid) ?? lineOf(p);
			let sum = 0;
			let n = 0;
			for (const [tid, roster] of rosters) {
				if (tid === p.tid) {
					continue;
				}
				sum += rosterPoints([...roster, line]) - points.get(tid)!;
				n += 1;
			}
			out = n > 0 ? sum / n : 0;
			typical.set(p.pid, out);
		}
		return out;
	};
	// What he adds to this team, or what losing him takes from it.
	const gainTo = (tid: number, p: Player, side: "add" | "remove") => {
		const roster = rosters.get(tid) ?? [];
		const base = points.get(tid) ?? rosterPoints(roster);
		const line = lines.get(p.pid) ?? lineOf(p);
		return side === "add"
			? rosterPoints([...roster, line]) - base
			: base - rosterPoints(roster.filter((x) => x !== line));
	};

	// What a rating's box score usually is: BPM against overall, least
	// squares over everyone with the minutes, weighted by minutes.
	let sw = 0;
	let sx = 0;
	let sy = 0;
	let sxx = 0;
	let sxy = 0;
	for (const p of players) {
		const box = boxOf(p, season);
		if (box.min < s.productionMinutes) {
			continue;
		}
		const x = p.ratings.at(-1)!.ovr;
		const w = box.min;
		sw += w;
		sx += w * x;
		sy += w * box.bpm;
		sxx += w * x * x;
		sxy += w * x * box.bpm;
	}
	const slope =
		sw > 0 && sw * sxx - sx * sx > 0
			? (sw * sxy - sx * sy) / (sw * sxx - sx * sx)
			: 0;
	const intercept = sw > 0 ? (sy - slope * sx) / sw : 0;

	const cache = new Map<string, number>();
	return (tid, asset, side) => {
		if (asset.type !== "player") {
			return 1;
		}
		const key = `${tid}:${asset.pid}:${side}`;
		const cached = cache.get(key);
		if (cached !== undefined) {
			return cached;
		}
		const p = byPid.get(asset.pid);
		let total = 0;
		if (p) {
			// Fit.
			const typicalAdds = Math.max(0, typicalGain(p));
			const fitRatio =
				(Math.max(0, gainTo(tid, p, side)) + s.fitShrink) /
				(typicalAdds + s.fitShrink);
			total += clamp(s.fitShare * (fitRatio - 1), s.fitCap);
			// Aggression.
			if (
				side === "add" &&
				lanes.get(tid) === "allIn" &&
				fitRatio >= s.aggressionAt
			) {
				total += s.aggression;
			}
			// Production.
			const box = boxOf(p, season);
			if (box.min >= s.productionMinutes && sw > 0) {
				const expected = intercept + slope * p.ratings.at(-1)!.ovr;
				total += clamp(
					s.productionPerPoint * (box.bpm - expected),
					s.productionCap,
				);
			}
		}
		// Timing (timing.ts): what he's worth to a team with this lane's
		// horizon, against the market's.
		const own =
			(p ? timingFactor(p, lanes.get(tid)) : 1) *
			(1 + clamp(total, s.totalCap));
		// A team never gives a player away for less than the market would pay
		// for him: it could always shop him instead.
		const factor = side === "remove" ? Math.max(1, own) : own;
		cache.set(key, factor);
		return factor;
	};
};
