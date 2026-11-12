import { PHASE } from "../../../common/constants.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import { stayChance } from "../realisticFrontOffice/offers.ts";
import getNumPlayoffTeams from "../season/getNumPlayoffTeams.ts";
import { factsOf, laneSettings } from "./lanes.ts";

// THE CALENDAR (docs/vanilla-plus.md, "A market that never stops", 1): what
// this season is still worth to each team, and how long each player will be
// around to help.
//
// This season is the regular season left to play - the seed race - plus the
// playoffs, and the playoffs count most (owner's call). To a team in
// position for them, more for a better seed; to a team out of the race, only
// the games left. The market's is the league's average, so prices before the
// season stay vanilla's.
//
// A player's seasons past his contract count by the chance he re-signs: the
// free agency model's own (realisticFrontOffice/offers.ts stayChance), with
// the team he's on. A veteran on an expiring deal is worth this season, and
// next to nothing after it to a team that doesn't need this season.
export const calendarSettings = {
	// This season's share that is the regular season; the rest is the
	// playoffs.
	regularSeason: 0.3,
	// The best seed's playoffs are worth this much more than the last one's.
	seedBonus: 0.6,
	// How sure a team's playoff place is, in places of the standings either
	// side of the cut: this many at the start of the season, shrinking to
	// `sureAtEnd` by its end.
	unsure: 4,
	sureAtEnd: 0.75,
	// Refreshed this often, in games played.
	everyGames: 10,
};

export type Calendar = {
	// Share of the regular season still to play: 1 before it starts.
	remaining: number;
	// Each team's playoffs, relative to the league's average (mean 1).
	playoffs: Map<number, number>;
	// Each player's chance of staying past his contract.
	retention: Map<number, number>;
	// The last season of each player's contract.
	exp: Map<number, number>;
};

let cached: { key: string; calendar: Calendar } | undefined;

const logistic = (x: number) => 1 / (1 + Math.exp(-x));

export const calendar = async (): Promise<Calendar> => {
	const s = calendarSettings;
	const { facts, progress } = await factsOf();
	const games = progress * g.get("numGames");
	const key = `${g.get("lid")}:${g.get("season")}:${g.get("phase")}:${Math.floor(games / s.everyGames)}`;
	if (cached?.key === key) {
		return cached.calendar;
	}
	const phase = g.get("phase");
	const remaining =
		phase < PHASE.REGULAR_SEASON || phase > PHASE.PLAYOFFS
			? 1
			: phase === PHASE.REGULAR_SEASON
				? 1 - progress
				: 0;

	// Each team's place: its rating's rank and, once games are played, its
	// record's - as the lanes read it.
	const n = facts.length;
	const w = Math.min(laneSettings.recordWeight, progress);
	const byOvr = [...facts].sort((a, b) => b.ovr - a.ovr);
	const byRecord = [...facts].sort(
		(a, b) => (b.winPct ?? 0.5) - (a.winPct ?? 0.5),
	);
	const place = new Map(
		facts.map((f) => [
			f.tid,
			(1 - w) * (byOvr.indexOf(f) + 1) + w * (byRecord.indexOf(f) + 1),
		]),
	);
	const season = phase > PHASE.PLAYOFFS ? g.get("season") + 1 : g.get("season");
	let cut = Math.round(n * 0.5);
	try {
		cut = (await getNumPlayoffTeams(season)).numPlayoffTeams;
	} catch {
		// Not set up for that season yet: half the league.
	}
	const spread = s.sureAtEnd + (s.unsure - s.sureAtEnd) * (1 - progress);
	const raw = new Map<number, number>();
	for (const [tid, at] of place) {
		const chance = logistic((cut + 0.5 - at) / spread);
		const seed =
			1 +
			s.seedBonus * Math.max(0, Math.min(1, (cut - at) / Math.max(1, cut - 1)));
		raw.set(tid, chance * seed);
	}
	const mean =
		[...raw.values()].reduce((sum, x) => sum + x, 0) / Math.max(1, raw.size);
	const playoffs = new Map(
		[...raw].map(([tid, x]) => [tid, mean > 0 ? x / mean : 1]),
	);

	// Re-signing: each player with his team.
	const retention = new Map<number, number>();
	const exp = new Map<number, number>();
	for (const p of await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	])) {
		const contract = contractOf(p);
		exp.set(p.pid, contract.exp);
		if (contract.exp < g.get("season") + 10) {
			try {
				retention.set(p.pid, await stayChance(p, p.tid));
			} catch {
				retention.set(p.pid, 0.5);
			}
		}
	}

	const out = { remaining, playoffs, retention, exp };
	cached = { key, calendar: out };
	return out;
};

// This season's weight for a team (`playoffs` its relative playoffs; 1 for
// the market): the games left and the playoffs.
export const thisSeason = (c: Calendar, playoffs: number) => {
	const s = calendarSettings;
	return s.regularSeason * c.remaining + (1 - s.regularSeason) * playoffs;
};
