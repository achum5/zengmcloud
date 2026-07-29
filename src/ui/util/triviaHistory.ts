// Per-game history for the trivia games, kept in localStorage.
//
// The games already tracked aggregates (played / best / average), which tells
// you how you're doing but not what you did. A list of finished games is the
// thing worth keeping: it's what makes a good grid or a nasty roster worth
// remembering, and it's what the Game History screen reads.
//
// Deliberately NOT stored in the league database. This is per-device play
// history for a side game, not league state - it should not travel through a
// league export, and it should not sync between multiplayer devices.

export type TriviaGame = "grids" | "team";

export type TriviaHistoryEntry = {
	// Unique per entry. Timestamp plus a counter, because two games finished in
	// the same millisecond would otherwise collide on delete.
	id: string;
	ts: number;
	score: number;
	// What the game was: "2054 Clippers", or a grid's headline criterion.
	label: string;
	// Sublabel: "7/9 solved", "12/15 named".
	detail: string;
	// Present when the game was about one team, for the logo and card color.
	tid?: number;
	season?: number;
	colors?: [string, string, string];
	// How much of the game was completed - 9 of 9 cells, 12 of 15 names. Kept as
	// numbers rather than parsed back out of `detail`, so "did I get everything?"
	// is a comparison and not a string match.
	progress?: { done: number; total: number };
};

// Enough to scroll through a long history without letting localStorage grow
// without bound. The oldest entries fall off the end.
const MAX_ENTRIES = 250;

const keyFor = (game: TriviaGame) => `triviaHistory:${game}`;

const isEntry = (x: any): x is TriviaHistoryEntry =>
	!!x &&
	typeof x.id === "string" &&
	typeof x.ts === "number" &&
	typeof x.score === "number" &&
	typeof x.label === "string";

export const loadHistory = (game: TriviaGame): TriviaHistoryEntry[] => {
	try {
		const raw = localStorage.getItem(keyFor(game));
		if (!raw) {
			return [];
		}
		const parsed = JSON.parse(raw);
		if (!Array.isArray(parsed)) {
			return [];
		}
		return parsed.filter((x) => isEntry(x));
	} catch {
		return [];
	}
};

const save = (game: TriviaGame, entries: TriviaHistoryEntry[]) => {
	try {
		localStorage.setItem(
			keyFor(game),
			JSON.stringify(entries.slice(0, MAX_ENTRIES)),
		);
	} catch {}
};

let counter = 0;

export const makeEntryID = (ts: number): string => {
	counter += 1;
	return `${ts}-${counter}`;
};

// Newest first, so the list renders in the order it's stored.
export const addHistoryEntry = (
	game: TriviaGame,
	entry: Omit<TriviaHistoryEntry, "id" | "ts"> & { ts?: number },
): TriviaHistoryEntry[] => {
	const ts = entry.ts ?? Date.now();
	const full: TriviaHistoryEntry = { ...entry, ts, id: makeEntryID(ts) };
	const next = [full, ...loadHistory(game)].slice(0, MAX_ENTRIES);
	save(game, next);
	return next;
};

export const deleteHistoryEntry = (
	game: TriviaGame,
	id: string,
): TriviaHistoryEntry[] => {
	const next = loadHistory(game).filter((e) => e.id !== id);
	save(game, next);
	return next;
};

export const clearHistory = (game: TriviaGame): TriviaHistoryEntry[] => {
	save(game, []);
	return [];
};

export type HistorySort = "recent" | "best";

// The filter behind the funnel icon: free text over the label, an optional
// team, and the sort. Pure, so it's testable without a DOM.
export const filterHistory = (
	entries: TriviaHistoryEntry[],
	{
		query = "",
		tid,
		sort = "recent",
	}: { query?: string; tid?: number; sort?: HistorySort },
): TriviaHistoryEntry[] => {
	const q = query.trim().toLowerCase();
	const out = entries.filter((e) => {
		if (tid !== undefined && e.tid !== tid) {
			return false;
		}
		if (q && !`${e.label} ${e.detail}`.toLowerCase().includes(q)) {
			return false;
		}
		return true;
	});
	// Ties broken by recency either way, so the order is never arbitrary.
	out.sort((a, b) =>
		sort === "best" ? b.score - a.score || b.ts - a.ts : b.ts - a.ts,
	);
	return out;
};

// Games finished with nothing left on the board.
export const countPerfect = (entries: TriviaHistoryEntry[]): number =>
	entries.filter(
		(e) => e.progress !== undefined && e.progress.done >= e.progress.total,
	).length;

export type HistorySummary = {
	played: number;
	best: number;
	average: number;
};

export const summarize = (entries: TriviaHistoryEntry[]): HistorySummary => {
	if (entries.length === 0) {
		return { played: 0, best: 0, average: 0 };
	}
	let best = -Infinity;
	let total = 0;
	for (const e of entries) {
		best = Math.max(best, e.score);
		total += e.score;
	}
	return {
		played: entries.length,
		best,
		average: Math.round(total / entries.length),
	};
};
