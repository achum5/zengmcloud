// Hint mode for Grids: instead of a text clue, you get six faces, exactly one
// of which fits the cell.
//
// What makes it a real puzzle rather than a giveaway is WHICH five are wrong.
// The best distractors satisfy exactly one of the cell's two criteria and are
// about as famous as the right answer; six random players would be trivially
// solvable by recognising the only star.
//
// But "satisfies exactly one criterion" can run dry - a Celtics x Celtics cell
// has NOBODY who meets one and not the other, and a heavily overlapping pair
// (a team crossed with an achievement most of that team has) leaves only a
// handful. So the draw falls through a ladder of progressively looser pools
// rather than returning a short hand. A hint that deals one face is not a hint,
// it's the answer.
//
// Two invariants hold at every tier, and are what the tests pin down:
//   - exactly one option is correct
//   - no other option is a valid answer for that cell
//
// This runs entirely in the UI off the per-criterion pid lists the grid ships,
// so opening a hint is instant and reshuffling costs nothing.

export type HintOption = {
	pid: number;
	correct: boolean;
};

// Small deterministic PRNG (mulberry32) so a given cell + reshuffle count
// always produces the same six players. Without this, any re-render would deal
// a fresh hand and the "wrong pick" marks would point at the wrong faces.
const makeRng = (seedStr: string) => {
	let h = 2166136261;
	for (let i = 0; i < seedStr.length; i += 1) {
		h ^= seedStr.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	let a = h >>> 0;
	return () => {
		a |= 0;
		a = (a + 0x6d2b79f5) | 0;
		let t = Math.imul(a ^ (a >>> 15), 1 | a);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

const shuffle = <T>(arr: T[], rng: () => number): T[] => {
	const out = [...arr];
	for (let i = out.length - 1; i > 0; i -= 1) {
		const j = Math.floor(rng() * (i + 1));
		[out[i], out[j]] = [out[j]!, out[i]!];
	}
	return out;
};

const sample = <T>(arr: T[], n: number, rng: () => number): T[] =>
	shuffle(arr, rng).slice(0, n);

export const HINT_OPTION_COUNT = 6;

export const buildHintOptions = ({
	cellPids,
	rarity,
	rowPids,
	colPids,
	usedPids,
	popByPid,
	seed,
}: {
	cellPids: number[];
	// pid -> rarity points; lower means a more obvious answer.
	rarity: Record<number, number>;
	rowPids: number[];
	colPids: number[];
	usedPids: Set<number>;
	popByPid: Map<number, number>;
	seed: string;
}): HintOption[] => {
	const rng = makeRng(seed);

	const eligible = cellPids.filter((pid) => !usedPids.has(pid));
	if (eligible.length === 0) {
		return [];
	}

	// The correct answer comes from the most COMMON fifth of the eligible pool.
	// A hint should open the cell up, not hand over an obscure deep cut that
	// happens to be the only one left.
	const byCommonness = [...eligible].sort(
		(a, b) => (rarity[a] ?? 50) - (rarity[b] ?? 50),
	);
	const topCount = Math.max(1, Math.ceil(byCommonness.length * 0.2));
	const correct = byCommonness[Math.floor(rng() * topCount)]!;

	// Anything that would ALSO solve the cell is disqualified as a distractor,
	// at every tier. This is the invariant that keeps "exactly one correct"
	// true no matter how loose the pool the ladder falls through to.
	const cellSet = new Set(cellPids);
	const rowSet = new Set(rowPids);
	const colSet = new Set(colPids);
	const correctPop = popByPid.get(correct) ?? 0;

	const taken = new Set<number>([correct]);
	const usable = (pid: number) =>
		!taken.has(pid) && !cellSet.has(pid) && !usedPids.has(pid);

	// Closest in fame to the correct answer first, so the six read as a set of
	// peers rather than one star and five nobodies.
	const bySimilarity = (pids: number[]) =>
		[...pids].sort(
			(a, b) =>
				Math.abs((popByPid.get(a) ?? 0) - correctPop) -
				Math.abs((popByPid.get(b) ?? 0) - correctPop),
		);

	const need = HINT_OPTION_COUNT - 1;
	const distractors: number[] = [];

	// Draw from a widened band of the most similar candidates rather than the
	// top N exactly, so repeated hints on one cell aren't identical.
	const drawFrom = (pids: number[], count: number) => {
		if (count <= 0) {
			return;
		}
		const band = pids
			.filter((pid) => usable(pid))
			.slice(0, Math.max(count * 3, 12));
		for (const pid of sample(band, count, rng)) {
			distractors.push(pid);
			taken.add(pid);
		}
	};

	// Tier 1, the good ones: satisfies exactly one of the two criteria. Aim for
	// a mix of both kinds, so the wrong answers don't all fail the same
	// criterion - which would itself be a giveaway.
	const rowOnly = bySimilarity(rowPids.filter((pid) => !colSet.has(pid)));
	const colOnly = bySimilarity(colPids.filter((pid) => !rowSet.has(pid)));
	drawFrom(rowOnly, Math.ceil(need / 2));
	drawFrom(colOnly, need - distractors.length);
	drawFrom([...rowOnly, ...colOnly], need - distractors.length);

	// Tier 2: meets BOTH criteria on paper but still isn't an answer - the
	// season-aligned cells ("played for X in a season he was an All-Star") are
	// full of these, and they're the most convincing wrong answers there are.
	if (distractors.length < need) {
		drawFrom(
			bySimilarity(rowPids.filter((pid) => colSet.has(pid))),
			need - distractors.length,
		);
	}

	// Tier 3: anyone at all. Reached when the two criteria are the same or very
	// nearly - a hand of six that includes some obviously-unrelated names is
	// still a hint; a hand of one is not.
	if (distractors.length < need) {
		drawFrom(bySimilarity([...popByPid.keys()]), need - distractors.length);
	}

	const options: HintOption[] = [
		{ pid: correct, correct: true },
		...distractors.map((pid) => ({ pid, correct: false })),
	];
	return shuffle(options, rng);
};
