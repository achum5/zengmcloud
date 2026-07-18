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
// attempts: at least three) qualifying player.

export type GridCriterion =
	| { kind: "team"; tid: number; label: string }
	| { kind: "career" | "season"; id: string; label: string };

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

export const generateTriviaGrid = async (): Promise<
	| {
			grid: TriviaGridData;
			searchList: ReturnType<typeof getSearchList>;
	  }
	| undefined
> => {
	const pool = await getTriviaPool();
	const ctx = buildSeasonContext(pool);
	const seasonIndex: SeasonIndex = buildSeasonIndex(pool, ctx);
	const careerAchievements: CareerAchievement[] = buildCareerAchievements(pool);

	// --- Candidate criteria ---------------------------------------------
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
	const teamCandidates: Candidate[] = teams
		.filter((t) => !t.disabled && (playersByTeam.get(t.tid)?.size ?? 0) >= 12)
		.map((t) => ({
			kind: "team" as const,
			tid: t.tid,
			label: `${t.region} ${t.name}`,
			set: playersByTeam.get(t.tid)!,
		}));

	const MIN_QUALIFIERS = 8;
	const achievementCandidates: Candidate[] = [];
	for (const ach of careerAchievements) {
		const set = new Set<number>();
		for (const p of pool.players) {
			if (ach.test(p)) {
				set.add(p.pid);
			}
		}
		if (set.size >= MIN_QUALIFIERS) {
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
		if (set.size >= MIN_QUALIFIERS) {
			achievementCandidates.push({
				kind: "season",
				id: ach.id,
				label: ach.label,
				family: ach.family,
				set,
			});
		}
	}

	if (teamCandidates.length < 5) {
		return undefined; // brand-new or tiny league
	}

	// --- Cell intersection ------------------------------------------------
	const cellPids = (a: Candidate, b: Candidate): Set<number> => {
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

	// --- Assembly with retries ---------------------------------------------
	const MAX_TRIES = 300;
	for (let attempt = 0; attempt < MAX_TRIES; attempt++) {
		const minCell = attempt < 200 ? 3 : 1;
		const achSlots = new Set(
			achievementCandidates.length > 0 ? choice(LAYOUTS) : [],
		);

		const picked: Candidate[] = [];
		const usedTids = new Set<number>();
		const usedAchIds = new Set<string>();
		const usedFamilies = new Set<string>();
		let ok = true;
		for (let slot = 0; slot < 6; slot++) {
			if (achSlots.has(slot)) {
				const options = achievementCandidates.filter(
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
				const pids = cellPids(row, col);
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

		const toCriterion = (c: Candidate): GridCriterion =>
			c.kind === "team"
				? { kind: "team", tid: c.tid, label: c.label }
				: { kind: c.kind, id: c.id, label: c.label };

		return {
			grid: {
				rows: rows.map(toCriterion),
				cols: cols.map(toCriterion),
				cells: cellSets.map((set) => {
					const pids = [...set];
					return { pids, rarity: rarityForPool(pool, pids) };
				}),
			},
			searchList: getSearchList(pool),
		};
	}

	return undefined;
};
