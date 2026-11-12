import { PHASE } from "../../../common/constants.ts";
import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { ovr as ovrBasketball } from "../team/ovr.basketball.ts";
import { g } from "../../util/index.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import { stayChance } from "../realisticFrontOffice/offers.ts";
import { stageSettings } from "../realisticFrontOffice/stages.ts";
import { realisticFreeAgencyOn } from "../realisticFrontOffice/isOn.ts";
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
	// The playoffs: getting there, and a shot at the title on top of it, by
	// how strong the team will be in them against the other playoff teams.
	// `title` weighs the title shot (1: as much as getting there, for an
	// average playoff team); `titleSharpness` is per point of team rating.
	title: 1.5,
	titleSharpness: 0.3,
	// An injury counts as a long one, for refreshing the standings, from this
	// many games out.
	longInjury: 15,
	// PATIENCE ("A market that never stops", 2): what a team needs to come out
	// ahead by, as a share of what it gives up - a lot at the start of the
	// season, when there's time to find better, falling to almost nothing at
	// the deadline, when push comes to shove; less again through the draft and
	// free agency.
	patienceStart: 0.2,
	patienceEnd: 0.01,
	patienceCurve: 2,
	patienceOffseason: 0.06,
	// It holds at the start's until this share of the way to the deadline is
	// left, then falls (1: from the start).
	patienceWindow: 1,
	// A team sure of where it stands - in the playoffs, or out of them - has
	// this share of it: it acts when a deal's worth it, as a contender adding
	// for a seed or a bad team selling to tank. A team on the bubble waits to
	// see (1: every team alike).
	patienceSure: 1,
	// How sure a team's playoff place is, in places of the standings either
	// side of the cut: this many at the start of the season, shrinking to
	// `sureAtEnd` by its end.
	unsure: 4,
	sureAtEnd: 0.75,
	// Refreshed this often, in games played.
	everyGames: 10,
	// A player coming off a rookie deal is a restricted free agent his team
	// can match (realistic free agency): he stays at least this often.
	restrictedStays: 0.9,
};

export type Calendar = {
	// Share of the regular season still to play: 1 before it starts.
	remaining: number;
	// Share of the way to the trade deadline: 1 at it and after.
	toDeadline: number;
	// What a team needs to come out ahead by, as a share of what it gives:
	// the league's, and each team's by how sure it is of its place.
	patience: number;
	patienceOf: Map<number, number>;
	// Games still to play this regular season (the coming one's, out of it).
	gamesLeft: number;
	// Each team's playoffs, relative to the league's average (mean 1).
	playoffs: Map<number, number>;
	// Each player's chance of staying past his contract.
	retention: Map<number, number>;
	// The last season of each player's contract.
	exp: Map<number, number>;
	// Each team's share of the league's title chances this season.
	titleShot: Map<number, number>;
	// Each team's core age (lanes.ts): its best eight now.
	coreAge: Map<number, number>;
};

let cached: { key: string; calendar: Calendar } | undefined;

const logistic = (x: number) => 1 / (1 + Math.exp(-x));

let retentionCached:
	| { key: string; retention: Map<number, number>; exp: Map<number, number> }
	| undefined;

// A team's rating from the players who'll be playing.
const strength = (roster: Player[], out: (p: Player) => boolean) =>
	ovrBasketball(
		roster
			.filter((p) => !out(p))
			.map((p) => ({
				ratings: { ovr: p.ratings.at(-1)!.ovr, pos: p.ratings.at(-1)!.pos },
			})),
		{},
	);

export const calendar = async (): Promise<Calendar> => {
	const s = calendarSettings;
	const { facts, progress } = await factsOf();
	const games = progress * g.get("numGames");
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	// A long injury anywhere changes who's a contender: refreshed then too.
	const injuries = players
		.filter((p) => p.injury.gamesRemaining >= s.longInjury)
		.map((p) => p.pid)
		.sort((a, b) => a - b)
		.join(",");
	const base = `${g.get("lid")}:${g.get("season")}:${g.get("phase")}:${Math.floor(games / s.everyGames)}`;
	// Patience falls by the day: the rest refreshed every couple of games.
	const key = `${base}:${Math.floor(games / 2)}:${injuries}`;
	if (cached?.key === key) {
		return cached.calendar;
	}
	const phase = g.get("phase");
	const remaining =
		phase < PHASE.REGULAR_SEASON || phase > PHASE.PLAYOFFS
			? 1
			: phase === PHASE.PLAYOFFS
				? 0
				: 1 - progress;
	const gamesLeft = Math.round(remaining * g.get("numGames"));
	const toDeadline =
		phase === PHASE.REGULAR_SEASON
			? Math.min(1, progress / Math.max(0.05, g.get("tradeDeadline")))
			: phase < PHASE.REGULAR_SEASON
				? 0
				: 1;
	const patience =
		phase > PHASE.PLAYOFFS
			? s.patienceOffseason
			: s.patienceEnd +
				(s.patienceStart - s.patienceEnd) *
					Math.min(1, (1 - toDeadline) / s.patienceWindow) ** s.patienceCurve;

	// Each team now, from the players back for at least half the games left,
	// and in the playoffs, from those back by then.
	const rosters = Map.groupBy(players, (p) => p.tid);
	const now = new Map<number, number>();
	const inPlayoffs = new Map<number, number>();
	for (const f of facts) {
		const roster = rosters.get(f.tid) ?? [];
		now.set(
			f.tid,
			strength(roster, (p) => p.injury.gamesRemaining > gamesLeft / 2),
		);
		inPlayoffs.set(
			f.tid,
			strength(roster, (p) => p.injury.gamesRemaining > gamesLeft),
		);
	}

	// Each team's place: its rating's rank and, once games are played, its
	// record's - as the lanes read it.
	const n = facts.length;
	const w = Math.min(laneSettings.recordWeight, progress);
	const byOvr = [...facts].sort((a, b) => now.get(b.tid)! - now.get(a.tid)!);
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
	const chance = new Map(
		[...place].map(([tid, at]) => [tid, logistic((cut + 0.5 - at) / spread)]),
	);
	// The title: each team's shot against the others likely to be there, by
	// how strong each will be in the playoffs. A rival's star out for the year
	// raises everyone else's - most for the teams just behind it.
	const top = Math.max(...inPlayoffs.values());
	const pull = new Map(
		[...chance].map(([tid, c]) => [
			tid,
			c * Math.exp(s.titleSharpness * (inPlayoffs.get(tid)! - top)),
		]),
	);
	const pullSum = [...pull.values()].reduce((sum, x) => sum + x, 0);
	const raw = new Map<number, number>();
	const titleShot = new Map<number, number>();
	for (const [tid, c] of chance) {
		const shot = pullSum > 0 ? pull.get(tid)! / pullSum : 0;
		titleShot.set(tid, shot);
		raw.set(tid, c + s.title * shot * cut);
	}
	const mean =
		[...raw.values()].reduce((sum, x) => sum + x, 0) / Math.max(1, raw.size);
	const playoffs = new Map(
		[...raw].map(([tid, x]) => [tid, mean > 0 ? x / mean : 1]),
	);

	// Re-signing: each player with his team (refreshed every `everyGames`
	// only - it's the slow part).
	if (retentionCached?.key !== base) {
		const retention = new Map<number, number>();
		const exp = new Map<number, number>();
		const restricted = stageSettings.on && realisticFreeAgencyOn();
		for (const p of players) {
			const contract = contractOf(p);
			exp.set(p.pid, contract.exp);
			if (contract.exp < g.get("season") + 10) {
				let stay = 0.5;
				try {
					stay = await stayChance(p, p.tid);
				} catch {
					// Unpriced: even odds.
				}
				// The free agency model's odds are an unrestricted free agent's;
				// a restricted one's team can match whatever he's offered.
				if (restricted && contract.rookie) {
					stay = Math.max(stay, s.restrictedStays);
				}
				retention.set(p.pid, stay);
			}
		}
		retentionCached = { key: base, retention, exp };
	}
	const { retention, exp } = retentionCached;

	// Each team's: how sure it is of its place (a chance near 0 or 1) takes
	// patience off.
	const patienceOf = new Map(
		[...chance].map(([tid, p]) => {
			const unsure = 4 * p * (1 - p);
			return [
				tid,
				s.patienceEnd +
					(patience - s.patienceEnd) *
						(s.patienceSure + (1 - s.patienceSure) * unsure),
			];
		}),
	);

	const out = {
		remaining,
		toDeadline,
		patience,
		patienceOf,
		gamesLeft,
		playoffs,
		retention,
		exp,
		titleShot,
		coreAge: new Map(facts.map((f) => [f.tid, f.coreAge])),
	};
	cached = { key, calendar: out };
	return out;
};

// This season's weight for a team (`playoffs` its relative playoffs; 1 for
// the market): the games left and the playoffs - for a player out
// `injured` games, only those he'll be back for.
export const thisSeason = (c: Calendar, playoffs: number, injured = 0) => {
	const s = calendarSettings;
	const games = c.gamesLeft > 0 ? Math.max(0, 1 - injured / c.gamesLeft) : 1;
	const backForPlayoffs = injured <= c.gamesLeft ? 1 : 0;
	return (
		s.regularSeason * c.remaining * games +
		(1 - s.regularSeason) * playoffs * backForPlayoffs
	);
};

// The coming season after this one, for a player out `injured` games: what's
// left of his injury past this season's games (and its playoffs, about a
// tenth of a season) comes out of it.
export const nextSeason = (c: Calendar, injured: number) => {
	const numGames = g.get("numGames");
	const over = injured - c.gamesLeft - numGames / 10;
	return over > 0 ? Math.max(0, 1 - over / numGames) : 1;
};
