import { g } from "../../util/index.ts";
import type { TriviaPlayer, TriviaPool } from "./pool.ts";

// Grid criteria, ported from ZenGM Grids' achievements.ts +
// season-achievements.ts, reduced to the basketball set. Two families:
//
// - CAREER achievements: boolean per player over their whole career
//   ("20,000+ career points", "1st overall pick"). Intersected with a team
//   criterion as a plain AND - no season alignment.
// - SEASON achievements: earned in a specific season ("MVP", "Averaged 30+
//   PPG in a season"). Intersected with a team criterion via a season index,
//   so the honor must have been earned WHILE ON that team (award seasons
//   attach to the player's primary team that year, by minutes; stat-line
//   seasons attach to every team they played for that year).

export type CareerAchievement = {
	id: string;
	label: string;
	test: (p: TriviaPlayer) => boolean;
	// At most one criterion per "family" appears in a grid (e.g. two different
	// draft criteria would make a boring/confusing pairing).
	family?: string;
};

export type SeasonAchievement = {
	id: string;
	label: string;
	// Which seasons this player earned it (empty set = never).
	seasons: (p: TriviaPlayer, ctx: SeasonContext) => Set<number>;
	family?: string;
};

// Precomputed league-wide season context: per-season stat leaders.
export type SeasonContext = {
	leaders: Map<string, Map<number, number>>; // achievementId -> season -> pid
};

// ---------------------------------------------------------------------------
// Career achievements
// ---------------------------------------------------------------------------

const decadeOf = (season: number) => Math.floor(season / 10) * 10;

export const buildCareerAchievements = (
	pool: TriviaPool,
): CareerAchievement[] => {
	const list: CareerAchievement[] = [
		{
			id: "career20kPoints",
			label: "20,000+ Career Points",
			test: (p) => p.tot.pts >= 20000,
			// Shares a family with the generated thresholds for this stat, so a
			// grid never pairs two different point/rebound/... cutoffs.
			family: "careerPoints",
		},
		{
			id: "career10kRebounds",
			label: "10,000+ Career Rebounds",
			test: (p) => p.tot.trb >= 10000,
			// Shares a family with the generated thresholds for this stat, so a
			// grid never pairs two different point/rebound/... cutoffs.
			family: "careerRebounds",
		},
		{
			id: "career5kAssists",
			label: "5,000+ Career Assists",
			test: (p) => p.tot.ast >= 5000,
			// Shares a family with the generated thresholds for this stat, so a
			// grid never pairs two different point/rebound/... cutoffs.
			family: "careerAssists",
		},
		{
			id: "career2kSteals",
			label: "2,000+ Career Steals",
			test: (p) => p.tot.stl >= 2000,
			// Shares a family with the generated thresholds for this stat, so a
			// grid never pairs two different point/rebound/... cutoffs.
			family: "careerSteals",
		},
		{
			id: "career1500Blocks",
			label: "1,500+ Career Blocks",
			test: (p) => p.tot.blk >= 1500,
			// Shares a family with the generated thresholds for this stat, so a
			// grid never pairs two different point/rebound/... cutoffs.
			family: "careerBlocks",
		},
		{
			id: "career2kThrees",
			label: "2,000+ Career Threes",
			test: (p) => p.tot.tp >= 2000,
			// Shares a family with the generated thresholds for this stat, so a
			// grid never pairs two different point/rebound/... cutoffs.
			family: "careerThrees",
		},
		{
			id: "played15PlusSeasons",
			label: "Played 15+ Seasons",
			test: (p) => p.tot.seasons >= 15,
			family: "longevity",
		},
		{
			id: "playedAtAge40Plus",
			label: "Played at Age 40+",
			test: (p) => p.rows.some((r) => r.gp > 0 && r.season - p.bornYear >= 40),
			family: "longevity",
		},
		{
			id: "played5PlusFranchises",
			label: "Played for 5+ Franchises",
			test: (p) => p.teamsPlayed.length >= 5,
		},
		{
			id: "isHallOfFamer",
			label: "Hall of Fame",
			test: (p) => p.hof,
		},
		{
			id: "royLaterMVP",
			label: "ROY Who Later Won MVP",
			test: (p) => {
				const roy = p.awards.find((a) => a.type === "Rookie of the Year");
				if (!roy) {
					return false;
				}
				return p.awards.some(
					(a) => a.type === "Most Valuable Player" && a.season > roy.season,
				);
			},
		},
		{
			id: "isPick1Overall",
			label: "#1 Overall Pick",
			test: (p) => p.draft.round === 1 && p.draft.pick === 1,
			family: "draft",
		},
		{
			id: "isFirstRoundPick",
			label: "1st Round Pick",
			test: (p) => p.draft.round === 1,
			family: "draft",
		},
		{
			id: "isSecondRoundPick",
			label: "2nd Round Pick",
			test: (p) => p.draft.round === 2,
			family: "draft",
		},
		{
			id: "isUndrafted",
			label: "Went Undrafted",
			test: (p) => !p.draft.round || p.draft.round === 0,
			family: "draft",
		},
		{
			id: "draftedTeen",
			label: "Drafted as a Teenager",
			test: (p) => p.draft.round >= 1 && p.draft.year - p.bornYear <= 19,
			family: "draft",
		},
	];

	// Decade achievements, from the league's actual year range.
	const firstDecade = decadeOf(pool.minSeason);
	const lastDecade = decadeOf(pool.maxSeason);
	const numDecades = (lastDecade - firstDecade) / 10 + 1;
	for (let decade = firstDecade; decade <= lastDecade; decade += 10) {
		list.push(
			{
				id: `playedIn${decade}s`,
				label: `Played in the ${decade}s`,
				test: (p) =>
					p.rows.some((r) => r.gp > 0 && decadeOf(r.season) === decade),
				family: `decade${decade}`,
			},
			{
				id: `debutedIn${decade}s`,
				label: `Debuted in the ${decade}s`,
				test: (p) => decadeOf(p.firstSeason) === decade,
				family: `decade${decade}`,
			},
		);
	}
	if (numDecades >= 3) {
		list.push({
			id: "playedInThreeDecades",
			label: "Played in 3+ Decades",
			test: (p) => {
				const decades = new Set<number>();
				for (const r of p.rows) {
					if (r.gp > 0) {
						decades.add(decadeOf(r.season));
					}
				}
				return decades.size >= 3;
			},
			family: "longevity",
		});
	}

	list.push(...buildAdaptiveCareerAchievements(pool));

	return list;
};

// ---------------------------------------------------------------------------
// Adaptive statistical thresholds
// ---------------------------------------------------------------------------
//
// A fixed threshold ("20,000+ Career Points") is a bad fit for an arbitrary
// league: in a young or low-scoring one NOBODY qualifies, so the criterion is
// dropped and the grid loses variety; in a 60-season league half the Hall of
// Fame clears it, so it barely narrows anything. ZenGM Grids solves this by
// defining a LADDER of thresholds per stat and picking the rungs that actually
// discriminate in the league at hand. This does the same.
//
// For each stat we pick the rungs whose qualifier count lands in a target band -
// selective enough to be interesting, common enough to be solvable - and keep at
// most a couple per stat so one category can't flood the grid. Everything is
// emitted in the normal CareerAchievement shape, so grid generation, the custom
// grid builder, and rarity scoring all treat these like any other criterion.

// How many players a generated threshold should match to be worth offering.
// The floor is above the generator's MIN_QUALIFIERS so a chosen rung still has
// room once it's intersected with a team.
const ADAPTIVE_MIN_QUALIFIERS = 10;
// Above this, a criterion is so common it stops being a real constraint. Scaled
// against league size below rather than used raw.
const ADAPTIVE_MAX_SHARE = 0.25;
// At most this many rungs per stat, so (say) points can't supply six criteria.
const ADAPTIVE_PER_STAT = 2;

type Ladder = {
	id: string;
	family: string;
	thresholds: number[];
	label: (n: number) => string;
	value: (p: TriviaPlayer) => number;
};

const CAREER_LADDERS: Ladder[] = [
	{
		id: "pts",
		family: "careerPoints",
		thresholds: [
			3000, 5000, 7500, 10000, 12500, 15000, 17500, 20000, 22500, 25000, 30000,
			35000, 40000,
		],
		label: (n) => `${n.toLocaleString()}+ Career Points`,
		value: (p) => p.tot.pts,
	},
	{
		id: "trb",
		family: "careerRebounds",
		thresholds: [
			500, 1000, 1500, 2000, 3000, 4000, 5000, 6000, 7500, 10000, 12000,
		],
		label: (n) => `${n.toLocaleString()}+ Career Rebounds`,
		value: (p) => p.tot.trb,
	},
	{
		id: "ast",
		family: "careerAssists",
		thresholds: [500, 1000, 1500, 2000, 2500, 3000, 4000, 5000, 6000, 7500],
		label: (n) => `${n.toLocaleString()}+ Career Assists`,
		value: (p) => p.tot.ast,
	},
	{
		id: "stl",
		family: "careerSteals",
		thresholds: [200, 400, 600, 800, 1000, 1250, 1500, 2000],
		label: (n) => `${n.toLocaleString()}+ Career Steals`,
		value: (p) => p.tot.stl,
	},
	{
		id: "blk",
		family: "careerBlocks",
		thresholds: [200, 400, 600, 800, 1000, 1250, 1500, 2000],
		label: (n) => `${n.toLocaleString()}+ Career Blocks`,
		value: (p) => p.tot.blk,
	},
	{
		id: "tp",
		family: "careerThrees",
		thresholds: [100, 200, 300, 500, 750, 1000, 1250, 1500, 2000, 2500],
		label: (n) => `${n.toLocaleString()}+ Career 3PM`,
		value: (p) => p.tot.tp,
	},
];

// Pick the rungs of one ladder that discriminate well in THIS league, spread
// apart so the offered thresholds aren't near-duplicates of each other.
const pickRungs = (
	players: TriviaPlayer[],
	ladder: Ladder,
): { threshold: number; count: number }[] => {
	// The ceiling must sit well ABOVE the floor or the target band collapses to
	// nothing: in a small pool, share * count can land exactly on the minimum,
	// and then no rung is ever viable. Keep a real window in every league size.
	const maxQualifiers = Math.max(
		ADAPTIVE_MIN_QUALIFIERS * 3,
		Math.floor(players.length * ADAPTIVE_MAX_SHARE),
	);

	const viable: { threshold: number; count: number }[] = [];
	for (const threshold of ladder.thresholds) {
		let count = 0;
		for (const p of players) {
			if (ladder.value(p) >= threshold) {
				count += 1;
			}
		}
		// Thresholds only get harder, so once a rung is too sparse every rung
		// above it is too.
		if (count < ADAPTIVE_MIN_QUALIFIERS) {
			break;
		}
		if (count <= maxQualifiers) {
			viable.push({ threshold, count });
		}
	}
	if (viable.length <= ADAPTIVE_PER_STAT) {
		return viable;
	}

	// Spread the picks across the viable range (hardest, and one well below it)
	// instead of taking adjacent rungs that match nearly the same players.
	const picks: { threshold: number; count: number }[] = [];
	for (let i = 0; i < ADAPTIVE_PER_STAT; i += 1) {
		const idx = Math.round(
			((viable.length - 1) * i) / (ADAPTIVE_PER_STAT - 1 || 1),
		);
		const pick = viable[idx]!;
		if (!picks.some((existing) => existing.threshold === pick.threshold)) {
			picks.push(pick);
		}
	}
	return picks;
};

const buildAdaptiveCareerAchievements = (
	pool: TriviaPool,
): CareerAchievement[] => {
	const players = pool.players;
	if (players.length === 0) {
		return [];
	}

	// The fixed criteria above already cover these exact numbers; skip a
	// generated rung that would duplicate one of them verbatim.
	const fixed = new Set([
		"20000|careerPoints",
		"10000|careerRebounds",
		"5000|careerAssists",
		"2000|careerSteals",
		"1500|careerBlocks",
		"2000|careerThrees",
	]);

	const out: CareerAchievement[] = [];
	for (const ladder of CAREER_LADDERS) {
		for (const { threshold } of pickRungs(players, ladder)) {
			if (fixed.has(`${threshold}|${ladder.family}`)) {
				continue;
			}
			out.push({
				id: `adaptive_${ladder.id}_${threshold}`,
				label: ladder.label(threshold),
				test: (p) => ladder.value(p) >= threshold,
				// Same family as the hand-written one, so a grid never pairs two
				// different points thresholds against each other.
				family: ladder.family,
			});
		}
	}
	return out;
};

// ---------------------------------------------------------------------------
// Season achievements
// ---------------------------------------------------------------------------

// Merge a player's stints into one line per season.
export const mergedSeasons = (p: TriviaPlayer) => {
	const bySeason = new Map<
		number,
		{
			gp: number;
			min: number;
			pts: number;
			trb: number;
			ast: number;
			stl: number;
			blk: number;
			tp: number;
			tpa: number;
			fg: number;
			fga: number;
			ft: number;
			fta: number;
		}
	>();
	for (const r of p.rows) {
		let s = bySeason.get(r.season);
		if (!s) {
			s = {
				gp: 0,
				min: 0,
				pts: 0,
				trb: 0,
				ast: 0,
				stl: 0,
				blk: 0,
				tp: 0,
				tpa: 0,
				fg: 0,
				fga: 0,
				ft: 0,
				fta: 0,
			};
			bySeason.set(r.season, s);
		}
		s.gp += r.gp;
		s.min += r.min;
		s.pts += r.pts;
		s.trb += r.trb;
		s.ast += r.ast;
		s.stl += r.stl;
		s.blk += r.blk;
		s.tp += r.tp;
		s.tpa += r.tpa;
		s.fg += r.fg;
		s.fga += r.fga;
		s.ft += r.ft;
		s.fta += r.fta;
	}
	return bySeason;
};

type SeasonLine =
	ReturnType<typeof mergedSeasons> extends Map<number, infer V> ? V : never;

const awardSeasons =
	(match: (type: string) => boolean) =>
	(p: TriviaPlayer): Set<number> => {
		const out = new Set<number>();
		for (const a of p.awards) {
			if (match(a.type)) {
				out.add(a.season);
			}
		}
		return out;
	};

const statSeasons =
	(test: (s: SeasonLine) => boolean) =>
	(p: TriviaPlayer): Set<number> => {
		const out = new Set<number>();
		for (const [season, s] of mergedSeasons(p)) {
			if (test(s)) {
				out.add(season);
			}
		}
		return out;
	};

const leaderSeasons =
	(id: string) =>
	(p: TriviaPlayer, ctx: SeasonContext): Set<number> => {
		const out = new Set<number>();
		const bySeason = ctx.leaders.get(id);
		if (bySeason) {
			for (const [season, pid] of bySeason) {
				if (pid === p.pid) {
					out.add(season);
				}
			}
		}
		return out;
	};

export const SEASON_ACHIEVEMENTS: SeasonAchievement[] = [
	// Awards, matched against BBGM's exact award-type strings.
	{
		id: "AllStar",
		label: "All-Star",
		seasons: awardSeasons((t) => t === "All-Star"),
	},
	{
		id: "MVP",
		label: "MVP",
		seasons: awardSeasons((t) => t === "Most Valuable Player"),
	},
	{
		id: "DPOY",
		label: "Defensive Player of the Year",
		seasons: awardSeasons((t) => t === "Defensive Player of the Year"),
	},
	{
		id: "ROY",
		label: "Rookie of the Year",
		seasons: awardSeasons((t) => t === "Rookie of the Year"),
	},
	{
		id: "SMOY",
		label: "Sixth Man of the Year",
		seasons: awardSeasons((t) => t === "Sixth Man of the Year"),
	},
	{
		id: "MIP",
		label: "Most Improved Player",
		seasons: awardSeasons((t) => t === "Most Improved Player"),
	},
	{
		id: "FinalsMVP",
		label: "Finals MVP",
		seasons: awardSeasons((t) => t === "Finals MVP"),
	},
	{
		id: "AllLeagueAny",
		label: "All-League Team",
		seasons: awardSeasons((t) => t.includes("All-League")),
	},
	{
		id: "AllDefAny",
		label: "All-Defensive Team",
		seasons: awardSeasons((t) => t.includes("All-Defensive")),
	},
	{
		id: "AllRookieAny",
		label: "All-Rookie Team",
		seasons: awardSeasons((t) => t === "All-Rookie Team"),
	},
	{
		id: "Champion",
		label: "Won a Championship",
		seasons: awardSeasons((t) => t === "Won Championship"),
	},
	{
		id: "DunkWinner",
		label: "Dunk Contest Winner",
		seasons: awardSeasons((t) => t === "Slam Dunk Contest Winner"),
	},
	{
		id: "ThreeWinner",
		label: "Three-Point Contest Winner",
		seasons: awardSeasons((t) => t === "Three-Point Contest Winner"),
	},

	// League leaders (computed - BBGM doesn't store these as awards).
	{
		id: "PointsLeader",
		label: "Led League in Scoring",
		seasons: leaderSeasons("PointsLeader"),
		family: "leader",
	},
	{
		id: "ReboundsLeader",
		label: "Led League in Rebounds",
		seasons: leaderSeasons("ReboundsLeader"),
		family: "leader",
	},
	{
		id: "AssistsLeader",
		label: "Led League in Assists",
		seasons: leaderSeasons("AssistsLeader"),
		family: "leader",
	},
	{
		id: "StealsLeader",
		label: "Led League in Steals",
		seasons: leaderSeasons("StealsLeader"),
		family: "leader",
	},
	{
		id: "BlocksLeader",
		label: "Led League in Blocks",
		seasons: leaderSeasons("BlocksLeader"),
		family: "leader",
	},

	// Single-season stat lines (thresholds from the original).
	{
		id: "Season30PPG",
		label: "Averaged 30+ PPG in a Season",
		seasons: statSeasons((s) => s.gp >= 50 && s.pts / s.gp >= 30),
		family: "scoring",
	},
	{
		id: "Season2000Points",
		label: "2,000+ Points in a Season",
		seasons: statSeasons((s) => s.pts >= 2000),
		family: "scoring",
	},
	{
		id: "Season200Threes",
		label: "200+ Threes in a Season",
		seasons: statSeasons((s) => s.tp >= 200),
	},
	{
		id: "Season12RPG",
		label: "Averaged 12+ RPG in a Season",
		seasons: statSeasons((s) => s.gp >= 50 && s.trb / s.gp >= 12),
		family: "rebounding",
	},
	{
		id: "Season10APG",
		label: "Averaged 10+ APG in a Season",
		seasons: statSeasons((s) => s.gp >= 50 && s.ast / s.gp >= 10),
		family: "assisting",
	},
	{
		id: "Season800Rebounds",
		label: "800+ Rebounds in a Season",
		seasons: statSeasons((s) => s.trb >= 800),
		family: "rebounding",
	},
	{
		id: "Season700Assists",
		label: "700+ Assists in a Season",
		seasons: statSeasons((s) => s.ast >= 700),
		family: "assisting",
	},
	{
		id: "Season2SPG",
		label: "Averaged 2+ SPG in a Season",
		seasons: statSeasons((s) => s.gp >= 50 && s.stl / s.gp >= 2),
		family: "steals",
	},
	{
		id: "Season2_5BPG",
		label: "Averaged 2.5+ BPG in a Season",
		seasons: statSeasons((s) => s.gp >= 50 && s.blk / s.gp >= 2.5),
		family: "blocks",
	},
	{
		id: "Season150Steals",
		label: "150+ Steals in a Season",
		seasons: statSeasons((s) => s.stl >= 150),
		family: "steals",
	},
	{
		id: "Season150Blocks",
		label: "150+ Blocks in a Season",
		seasons: statSeasons((s) => s.blk >= 150),
		family: "blocks",
	},
	{
		id: "Season200Stocks",
		label: "200+ Steals + Blocks in a Season",
		seasons: statSeasons((s) => s.stl + s.blk >= 200),
	},
	{
		id: "Season50_40_90",
		label: "50/40/90 Season",
		seasons: statSeasons(
			(s) =>
				s.fga >= 500 &&
				s.tpa >= 125 &&
				s.fta >= 250 &&
				s.fg / s.fga >= 0.5 &&
				s.tp / s.tpa >= 0.4 &&
				s.ft / s.fta >= 0.9,
		),
		family: "shooting",
	},
	{
		id: "Season90FT",
		label: "90%+ FT in a Season (250+ FTA)",
		seasons: statSeasons((s) => s.fta >= 250 && s.ft / s.fta >= 0.9),
		family: "shooting",
	},
	{
		id: "Season50FG",
		label: "50%+ FG in a Season (300+ FGA)",
		seasons: statSeasons((s) => s.fga >= 300 && s.fg / s.fga >= 0.5),
		family: "shooting",
	},
	{
		id: "Season40ThreePct",
		label: "40%+ from Three in a Season (100+ 3PA)",
		seasons: statSeasons((s) => s.tpa >= 100 && s.tp / s.tpa >= 0.4),
		family: "shooting",
	},
	{
		id: "Season70Games",
		label: "Played 70+ Games in a Season",
		seasons: statSeasons((s) => s.gp >= 70),
		family: "durability",
	},
	{
		id: "Season36MPG",
		label: "Averaged 36+ MPG in a Season",
		seasons: statSeasons((s) => s.gp >= 50 && s.min / s.gp >= 36),
		family: "durability",
	},
	{
		id: "Season25_10",
		label: "Averaged 25/10 in a Season",
		seasons: statSeasons(
			(s) => s.gp >= 50 && s.pts / s.gp >= 25 && s.trb / s.gp >= 10,
		),
		family: "combo",
	},
	{
		id: "Season25_5_5",
		label: "Averaged 25/5/5 in a Season",
		seasons: statSeasons(
			(s) =>
				s.gp >= 50 &&
				s.pts / s.gp >= 25 &&
				s.trb / s.gp >= 5 &&
				s.ast / s.gp >= 5,
		),
		family: "combo",
	},
	{
		id: "Season20_10_5",
		label: "Averaged 20/10/5 in a Season",
		seasons: statSeasons(
			(s) =>
				s.gp >= 50 &&
				s.pts / s.gp >= 20 &&
				s.trb / s.gp >= 10 &&
				s.ast / s.gp >= 5,
		),
		family: "combo",
	},
	{
		id: "Season1_1_1",
		label: "1+ SPG, BPG and 3PM/G in a Season",
		seasons: statSeasons(
			(s) =>
				s.gp >= 50 &&
				s.stl / s.gp >= 1 &&
				s.blk / s.gp >= 1 &&
				s.tp / s.gp >= 1,
		),
	},
];

// ---------------------------------------------------------------------------
// League-wide season context (stat leaders)
// ---------------------------------------------------------------------------

// A leader must have played ~70% of the season, like the original (and real
// leaderboards); the highest per-game rate among eligibles wins, ties broken
// by raw total.
export const buildSeasonContext = (pool: TriviaPool): SeasonContext => {
	const numGames = g.get("numGames");
	const minGp = Math.ceil(0.7 * numGames);

	const stats = [
		["PointsLeader", "pts"],
		["ReboundsLeader", "trb"],
		["AssistsLeader", "ast"],
		["StealsLeader", "stl"],
		["BlocksLeader", "blk"],
	] as const;

	// season -> stat -> { pid, rate, total }
	const best = new Map<
		number,
		Map<string, { pid: number; rate: number; total: number }>
	>();

	for (const p of pool.players) {
		for (const [season, s] of mergedSeasons(p)) {
			if (s.gp < minGp) {
				continue;
			}
			let seasonBest = best.get(season);
			if (!seasonBest) {
				seasonBest = new Map();
				best.set(season, seasonBest);
			}
			for (const [id, key] of stats) {
				const total = s[key];
				const rate = total / s.gp;
				const cur = seasonBest.get(id);
				if (
					!cur ||
					rate > cur.rate ||
					(rate === cur.rate && total > cur.total)
				) {
					seasonBest.set(id, { pid: p.pid, rate, total });
				}
			}
		}
	}

	const leaders = new Map<string, Map<number, number>>();
	for (const [id] of stats) {
		leaders.set(id, new Map());
	}
	for (const [season, seasonBest] of best) {
		for (const [id, winner] of seasonBest) {
			leaders.get(id)!.set(season, winner.pid);
		}
	}

	return { leaders };
};

// ---------------------------------------------------------------------------
// Season index: (tid, seasonAchievementId) -> Set<pid>
// ---------------------------------------------------------------------------

// An award season attaches to the player's PRIMARY team that season (most
// minutes), so "Lakers × MVP" means "won MVP as a Laker". Stat-line seasons
// attach to every team the player logged games for that season.
const AWARD_IDS = new Set([
	"AllStar",
	"MVP",
	"DPOY",
	"ROY",
	"SMOY",
	"MIP",
	"FinalsMVP",
	"AllLeagueAny",
	"AllDefAny",
	"AllRookieAny",
	"Champion",
	"DunkWinner",
	"ThreeWinner",
	"PointsLeader",
	"ReboundsLeader",
	"AssistsLeader",
	"StealsLeader",
	"BlocksLeader",
]);

export type SeasonIndex = Map<number, Map<string, Set<number>>>;

export const buildSeasonIndex = (
	pool: TriviaPool,
	ctx: SeasonContext,
): SeasonIndex => {
	const index: SeasonIndex = new Map();

	const add = (tid: number, achId: string, pid: number) => {
		let byAch = index.get(tid);
		if (!byAch) {
			byAch = new Map();
			index.set(tid, byAch);
		}
		let pids = byAch.get(achId);
		if (!pids) {
			pids = new Set();
			byAch.set(achId, pids);
		}
		pids.add(pid);
	};

	for (const p of pool.players) {
		// Primary team (by minutes) and all teams, per season.
		const primary = new Map<number, number>(); // season -> tid
		const teams = new Map<number, Set<number>>(); // season -> tids
		const minutes = new Map<number, Map<number, number>>();
		for (const r of p.rows) {
			if (r.gp <= 0) {
				continue;
			}
			let m = minutes.get(r.season);
			if (!m) {
				m = new Map();
				minutes.set(r.season, m);
			}
			m.set(r.tid, (m.get(r.tid) ?? 0) + r.min);
			let t = teams.get(r.season);
			if (!t) {
				t = new Set();
				teams.set(r.season, t);
			}
			t.add(r.tid);
		}
		for (const [season, m] of minutes) {
			let bestTid = -1;
			let bestMin = -1;
			for (const [tid, min] of m) {
				if (min > bestMin) {
					bestMin = min;
					bestTid = tid;
				}
			}
			primary.set(season, bestTid);
		}

		for (const ach of SEASON_ACHIEVEMENTS) {
			const seasons = ach.seasons(p, ctx);
			for (const season of seasons) {
				if (AWARD_IDS.has(ach.id)) {
					const tid = primary.get(season);
					if (tid !== undefined && tid >= 0) {
						add(tid, ach.id, p.pid);
					}
				} else {
					const tids = teams.get(season);
					if (tids) {
						for (const tid of tids) {
							add(tid, ach.id, p.pid);
						}
					}
				}
			}
		}
	}

	return index;
};
