import { idb } from "../../db/index.ts";
import {
	buildCareerAchievements,
	buildSeasonContext,
	buildSeasonIndex,
	SEASON_ACHIEVEMENTS,
	type CareerAchievement,
	type SeasonIndex,
} from "./criteria.ts";
import { getSearchList, getTriviaPool, type TriviaPool } from "./pool.ts";

// The Grids game (Immaculate Grid style), ported from ZenGM Grids'
// grid-generator.ts + intersection-cache.ts. Every generated grid is
// GUARANTEED solvable: all nine intersections are computed up front and a
// candidate grid is thrown away unless every cell has at least one (early
// attempts: at least three) qualifying player. Custom grids (user-picked
// criteria) skip that guarantee and instead report per-cell counts so the
// builder can show which cells are dead.

export type GridCriterion =
	| { kind: "team"; tid: number; label: string }
	| { kind: "career" | "season"; id: string; label: string };

// What the UI sends to identify a criterion when building a custom grid.
export type GridCriterionRef =
	| { kind: "team"; tid: number }
	| { kind: "career" | "season"; id: string };

export type GridCell = {
	// Eligible pids, and per-pid rarity points (10-100): the more obscure the
	// correct guess, the more it scores. Sent to the UI so guessing is instant.
	pids: number[];
	rarity: Record<number, number>;
};

export type TriviaGridData = {
	rows: GridCriterion[];
	cols: GridCriterion[];
	// Row-major: cells[r * 3 + c].
	cells: GridCell[];
};

const intersect = (a: Set<number>, b: Set<number>): Set<number> => {
	const [small, big] = a.size <= b.size ? [a, b] : [b, a];
	const out = new Set<number>();
	for (const x of small) {
		if (big.has(x)) {
			out.add(x);
		}
	}
	return out;
};

const choice = <T>(arr: T[]): T => arr[Math.floor(Math.random() * arr.length)]!;

// Rarity points for a cell's eligible pool: rank by fame, the biggest name
// scores the floor (10), the most obscure qualifier the ceiling, plus a bonus
// when the pool itself is tiny (a hard cell deserves more even for its star).
const rarityForPool = (pool: TriviaPool, pids: number[]): Record<number, number> => {
	const sorted = [...pids].sort(
		(a, b) => pool.byPid.get(b)!.popularity - pool.byPid.get(a)!.popularity,
	);
	const n = sorted.length;
	const bonus = n <= 10 ? (11 - n) * 2 : 0;
	const out: Record<number, number> = {};
	for (const [i, pid] of sorted.entries()) {
		const base = n > 1 ? 10 + (90 * i) / (n - 1) : 60;
		out[pid] = Math.max(10, Math.min(100, Math.round(base + bonus)));
	}
	return out;
};

type Candidate =
	| { kind: "team"; tid: number; label: string; set: Set<number> }
	| {
			kind: "career" | "season";
			id: string;
			label: string;
			family: string | undefined;
			set: Set<number>;
	  };

// Everything both the random generator and the custom-grid builder need,
// assembled in one pass over the pool.
type Candidates = {
	pool: TriviaPool;
	seasonIndex: SeasonIndex;
	teamCandidates: Candidate[];
	// All achievements with at least 1 qualifier (custom grids may use any);
	// the random generator additionally requires MIN_QUALIFIERS.
	achievementCandidates: Candidate[];
	teamAbbrevs: Map<number, string>;
};

const MIN_QUALIFIERS = 8;

const buildCandidates = async (): Promise<Candidates> => {
	const pool = await getTriviaPool();
	const ctx = buildSeasonContext(pool);
	const seasonIndex: SeasonIndex = buildSeasonIndex(pool, ctx);
	const careerAchievements: CareerAchievement[] = buildCareerAchievements(pool);

	const playersByTeam = new Map<number, Set<number>>();
	for (const p of pool.players) {
		for (const tid of p.teamsPlayed) {
			let set = playersByTeam.get(tid);
			if (!set) {
				set = new Set();
				playersByTeam.set(tid, set);
			}
			set.add(p.pid);
		}
	}

	const teams = await idb.cache.teams.getAll();
	const teamAbbrevs = new Map(teams.map((t) => [t.tid, t.abbrev]));
	const teamCandidates: Candidate[] = teams
		.filter((t) => !t.disabled && (playersByTeam.get(t.tid)?.size ?? 0) >= 12)
		.map((t) => ({
			kind: "team" as const,
			tid: t.tid,
			label: `${t.region} ${t.name}`,
			set: playersByTeam.get(t.tid)!,
		}));

	const achievementCandidates: Candidate[] = [];
	for (const ach of careerAchievements) {
		const set = new Set<number>();
		for (const p of pool.players) {
			if (ach.test(p)) {
				set.add(p.pid);
			}
		}
		if (set.size >= 1) {
			achievementCandidates.push({
				kind: "career",
				id: ach.id,
				label: ach.label,
				family: ach.family,
				set,
			});
		}
	}
	for (const ach of SEASON_ACHIEVEMENTS) {
		const set = new Set<number>();
		for (const p of pool.players) {
			if (ach.seasons(p, ctx).size > 0) {
				set.add(p.pid);
			}
		}
		if (set.size >= 1) {
			achievementCandidates.push({
				kind: "season",
				id: ach.id,
				label: ach.label,
				family: ach.family,
				set,
			});
		}
	}

	return { pool, seasonIndex, teamCandidates, achievementCandidates, teamAbbrevs };
};

// The eligible players for one cell.
const cellPids = (
	seasonIndex: SeasonIndex,
	a: Candidate,
	b: Candidate,
): Set<number> => {
	if (a.kind === "team" && b.kind === "team") {
		return intersect(a.set, b.set);
	}
	if (a.kind !== "team" && b.kind !== "team") {
		// Achievement × achievement: independent AND (career-wide), matching
		// the original's simplified logic.
		return intersect(a.set, b.set);
	}
	const team = (a.kind === "team" ? a : b) as Extract<
		Candidate,
		{ kind: "team" }
	>;
	const ach = (a.kind === "team" ? b : a) as Exclude<
		Candidate,
		{ kind: "team" }
	>;
	if (ach.kind === "season") {
		// Season-aligned: the honor must have been earned ON this team.
		return seasonIndex.get(team.tid)?.get(ach.id) ?? new Set();
	}
	return intersect(team.set, ach.set);
};

const toCriterion = (c: Candidate): GridCriterion =>
	c.kind === "team"
		? { kind: "team", tid: c.tid, label: c.label }
		: { kind: c.kind, id: c.id, label: c.label };

const toGrid = (
	pool: TriviaPool,
	rows: Candidate[],
	cols: Candidate[],
	cellSets: Set<number>[],
): TriviaGridData => ({
	rows: rows.map(toCriterion),
	cols: cols.map(toCriterion),
	cells: cellSets.map((set) => {
		const pids = [...set];
		return { pids, rarity: rarityForPool(pool, pids) };
	}),
});

// Slots per layout: which of the 6 criteria are achievements (by index: rows
// 0-2, cols 3-5). At most 2 achievements per axis, like the original.
const LAYOUTS: number[][] = [
	[5], // 1 achievement
	[5], // (weighted twice)
	[2, 5], // 2 achievements
	[2, 5],
	[4, 5], // 2 achievements, both on cols
	[2, 4, 5], // 3 achievements
];

// Fallback layouts for young leagues. Every LAYOUTS grid contains Team×Team
// cells, which need players who played for BOTH franchises - after only a
// season or two almost nobody has, so those grids can never be filled. These
// stages remove that requirement: first all-achievement columns (every cell
// is Team×Achievement), then a pure achievement×achievement grid.
const LAYOUT_ALL_ACH_COLS = [3, 4, 5];
const LAYOUT_ALL_ACH = [0, 1, 2, 3, 4, 5];

export const generateTriviaGrid = async (): Promise<
	| {
			grid: TriviaGridData;
			searchList: ReturnType<typeof getSearchList>;
	  }
	| undefined
> => {
	const { pool, seasonIndex, teamCandidates, achievementCandidates, teamAbbrevs } =
		await buildCandidates();

	// The random generator only draws from achievements with a healthy pool.
	const richAchievements = achievementCandidates.filter(
		(c) => c.set.size >= MIN_QUALIFIERS,
	);

	// Need enough raw material for SOME stage: team-based grids want a few
	// teams to choose from, the pure-achievement fallback wants six criteria.
	if (teamCandidates.length < 5 && richAchievements.length < 6) {
		return undefined; // brand-new or tiny league
	}

	// --- Assembly with retries ---------------------------------------------
	// Staged: prefer team-heavy grids (the classic look), then grids with no
	// Team×Team cells, then pure achievement grids - so young leagues still
	// get a solvable puzzle instead of nothing.
	const MAX_TRIES = 450;
	for (let attempt = 0; attempt < MAX_TRIES; attempt++) {
		const minCell = attempt % 150 < 100 ? 3 : 1;
		const layout =
			attempt < 150
				? choice(LAYOUTS)
				: attempt < 300
					? LAYOUT_ALL_ACH_COLS
					: LAYOUT_ALL_ACH;
		const achSlots = new Set(richAchievements.length > 0 ? layout : []);

		const picked: Candidate[] = [];
		const usedTids = new Set<number>();
		const usedAchIds = new Set<string>();
		const usedFamilies = new Set<string>();
		let ok = true;
		for (let slot = 0; slot < 6; slot++) {
			if (achSlots.has(slot)) {
				const options = richAchievements.filter(
					(c) =>
						c.kind !== "team" &&
						!usedAchIds.has(c.id) &&
						(c.family === undefined || !usedFamilies.has(c.family)),
				);
				if (options.length === 0) {
					ok = false;
					break;
				}
				const c = choice(options) as Exclude<Candidate, { kind: "team" }>;
				usedAchIds.add(c.id);
				if (c.family !== undefined) {
					usedFamilies.add(c.family);
				}
				picked.push(c);
			} else {
				const options = teamCandidates.filter(
					(c) => c.kind === "team" && !usedTids.has(c.tid),
				);
				if (options.length === 0) {
					ok = false;
					break;
				}
				const c = choice(options) as Extract<Candidate, { kind: "team" }>;
				usedTids.add(c.tid);
				picked.push(c);
			}
		}
		if (!ok) {
			continue;
		}

		const rows = picked.slice(0, 3);
		const cols = picked.slice(3, 6);

		const cellSets: Set<number>[] = [];
		let solvable = true;
		for (const row of rows) {
			for (const col of cols) {
				const pids = cellPids(seasonIndex, row, col);
				if (pids.size < minCell) {
					solvable = false;
					break;
				}
				cellSets.push(pids);
			}
			if (!solvable) {
				break;
			}
		}
		if (!solvable) {
			continue;
		}

		return {
			grid: toGrid(pool, rows, cols, cellSets),
			searchList: getSearchList(pool, teamAbbrevs),
		};
	}

	return undefined;
};

// ---------------------------------------------------------------------------
// Custom grids
// ---------------------------------------------------------------------------

// Every criterion the custom-grid builder can offer, with qualifier counts so
// the picker can sort/filter sensibly.
export const getGridCatalog = async () => {
	const { teamCandidates, achievementCandidates } = await buildCandidates();
	return {
		teams: teamCandidates
			.map((c) => ({
				tid: (c as Extract<Candidate, { kind: "team" }>).tid,
				label: c.label,
				count: c.set.size,
			}))
			.sort((a, b) => a.label.localeCompare(b.label)),
		achievements: achievementCandidates
			.map((c) => ({
				id: (c as Exclude<Candidate, { kind: "team" }>).id,
				kind: c.kind as "career" | "season",
				label: c.label,
				count: c.set.size,
			}))
			.sort((a, b) => a.label.localeCompare(b.label)),
	};
};

// Build a grid from user-picked criteria. No solvability guarantee - the
// cells report their own sizes (a 0-pid cell is a dead cell), and the builder
// UI decides whether to warn or let an impossible masterpiece through.
export const buildCustomGrid = async (input: {
	rows: GridCriterionRef[];
	cols: GridCriterionRef[];
}): Promise<
	| {
			grid: TriviaGridData;
			searchList: ReturnType<typeof getSearchList>;
	  }
	| undefined
> => {
	if (input.rows.length !== 3 || input.cols.length !== 3) {
		return undefined;
	}
	const { pool, seasonIndex, teamCandidates, achievementCandidates, teamAbbrevs } =
		await buildCandidates();

	const find = (ref: GridCriterionRef): Candidate | undefined =>
		ref.kind === "team"
			? teamCandidates.find((c) => c.kind === "team" && c.tid === ref.tid)
			: achievementCandidates.find(
					(c) => c.kind !== "team" && c.id === ref.id,
				);

	const rows: Candidate[] = [];
	const cols: Candidate[] = [];
	for (const ref of input.rows) {
		const c = find(ref);
		if (!c) {
			return undefined;
		}
		rows.push(c);
	}
	for (const ref of input.cols) {
		const c = find(ref);
		if (!c) {
			return undefined;
		}
		cols.push(c);
	}

	const cellSets: Set<number>[] = [];
	for (const row of rows) {
		for (const col of cols) {
			cellSets.push(cellPids(seasonIndex, row, col));
		}
	}

	return {
		grid: toGrid(pool, rows, cols, cellSets),
		searchList: getSearchList(pool, teamAbbrevs),
	};
};

// The little card shown in a solved cell: face + team colors. The cell's team
// (when the cell has one) styles the jersey; otherwise the player's own
// current/most-played team does.
export const getTriviaPlayerCard = async (pid: number, tid?: number) => {
	const p = await idb.getCopy.players({ pid }, "noCopyCache");
	if (!p) {
		return undefined;
	}

	let cardTid = tid;
	if (cardTid === undefined || cardTid < 0) {
		if (p.tid >= 0) {
			cardTid = p.tid;
		} else {
			const minByTid = new Map<number, number>();
			for (const ps of p.stats) {
				if (!ps.playoffs && ps.tid >= 0) {
					minByTid.set(ps.tid, (minByTid.get(ps.tid) ?? 0) + (ps.min ?? 0));
				}
			}
			let best = -1;
			for (const [t, min] of minByTid) {
				if (min > (minByTid.get(best) ?? -1)) {
					best = t;
				}
			}
			cardTid = best;
		}
	}

	const t =
		cardTid !== undefined && cardTid >= 0
			? await idb.cache.teams.get(cardTid)
			: undefined;

	return {
		pid,
		face: p.face,
		imgURL: p.imgURL,
		colors: t?.colors,
		jersey: t?.jersey,
	};
};
