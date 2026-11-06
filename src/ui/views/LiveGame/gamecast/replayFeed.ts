// A SAVED REPLAY'S PLAYS, for its box score laid out as a game recap: the
// events run through just as the live game runs them (see LiveGame's
// processToNextPause), so the plays read as the replay shows them - and as
// they go, each man's line at the end of every period, so his box score can
// be shown a quarter at a time.

import processLiveGameEventsBasketball from "../../../util/processLiveGameEvents.basketball.tsx";
import { finishOf } from "../../../util/liveGameWording.basketball.ts";
import { helpers } from "../../../util/helpers.ts";
import type { FeedEntry } from "./playFeed.ts";

// What a box score line counts, quarter by quarter.
export const PERIOD_STATS = [
	"min",
	"pts",
	"fg",
	"fga",
	"tp",
	"tpa",
	"ft",
	"fta",
	"orb",
	"drb",
	"ast",
	"tov",
	"stl",
	"blk",
	"ba",
	"pf",
	"pm",
] as const;

export type Lines = Map<number, Record<string, number>>;

export const linesOf = (boxScore: any): Lines => {
	const m: Lines = new Map();
	for (const t of boxScore.teams) {
		for (const p of t.players) {
			m.set(p.pid, Object.fromEntries(PERIOD_STATS.map((k) => [k, p[k] ?? 0])));
		}
	}
	return m;
};

export const feedFromEvents = (
	events: any[],
	initialBoxScore: any,
	gender: "male" | "female",
): {
	// Oldest first.
	entries: FeedEntry[];
	// Each man's line at the end of each period, the first period first.
	ends: Lines[];
} => {
	const boxScore = helpers.deepCopy(initialBoxScore);
	const queue = events.slice();
	const total = queue.length;
	let overtimes = 0;
	let quarters: string[] = [];
	const entries: FeedEntry[] = [];
	const ends: Lines[] = [];
	let period = 0;
	while (queue.length > 0) {
		const before = boxScore.teams[0].pts + boxScore.teams[1].pts;
		const out = processLiveGameEventsBasketball({
			events: queue,
			boxScore,
			overtimes,
			quarters,
		});
		overtimes = out.overtimes;
		quarters = out.quarters;
		const now = boxScore.teams[0].ptsQtrs?.length ?? 0;
		// A new period begun: the last one's lines as it ended (its last
		// play's points and rebounds counted on the way to this line).
		if (now > period) {
			if (period > 0) {
				ends.push(linesOf(boxScore));
			}
			period = now;
		}
		if (out.text === undefined) {
			continue;
		}
		const e: any = out.event;
		const after = boxScore.teams[0].pts + boxScore.teams[1].pts;
		entries.push({
			key: entries.length,
			type: e?.type,
			pid: typeof e?.pid === "number" ? e.pid : e?.pids?.[0],
			t: out.t,
			text: out.text,
			textOnly: out.textOnly,
			time: boxScore.time ?? "",
			period: now,
			pts: [boxScore.teams[0].pts, boxScore.teams[1].pts],
			scored: after > before ? after - before : undefined,
			finish: e ? finishOf(e, boxScore.gid, gender) : undefined,
			cursor: total - queue.length,
			event: e,
		});
	}
	ends.push(linesOf(boxScore));
	return { entries, ends };
};

// A box score for one period of the game (0 the first): each man's line
// over it, and his team's the sum of theirs.
export const periodBoxScore = (
	boxScore: any,
	ends: Lines[],
	k: number,
): any => {
	const end = ends[k];
	if (!end) {
		return boxScore;
	}
	const start = k > 0 ? ends[k - 1] : undefined;
	const out = helpers.deepCopy(boxScore);
	for (const t of out.teams) {
		const sums: Record<string, number> = {};
		for (const p of t.players) {
			const a = end.get(p.pid);
			const b = start?.get(p.pid);
			for (const key of PERIOD_STATS) {
				const v = (a?.[key] ?? 0) - (b?.[key] ?? 0);
				p[key] = v;
				sums[key] = (sums[key] ?? 0) + v;
			}
		}
		for (const key of PERIOD_STATS) {
			if (key !== "pm") {
				t[key] = sums[key] ?? 0;
			}
		}
	}
	return out;
};
