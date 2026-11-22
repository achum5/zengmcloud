// DRAFT NIGHT'S GRAPHICS (see court3d/DraftBroadcast.tsx): what the
// broadcast puts on the screen around a pick, worked out from the draft class
// as it stood when the pick was made - the next teams up, and the best
// prospects still on the board.
//
// Pure: the worker hands it the class (see worker/util/headlineScene.ts), and
// the same class always gives the same board, so a pick looks the same on
// every device and every time it is watched again.

// Where a pick falls in the draft, as one number: round by round, pick by
// pick. A player never drafted falls after all of them.
export const pickOrder = ({
	round,
	pick,
}: {
	round: number;
	pick: number;
}): number => (round >= 1 && pick >= 1 ? round * 1000 + pick : Infinity);

export type BoardMember = {
	pid: number;
	name: string;
	pos?: string;
	college?: string;
	// Where he went (round 0: he wasn't drafted).
	round: number;
	pick: number;
	// What the scouts made of him going in (see the worker's draftValue): the
	// board is ranked on it.
	value: number;
};

// The big board: every man in the class ranked on what he was thought of
// going in, best first - ties to the earlier pick, then the lower pid, so it
// never depends on the order the class was read in.
export const bigBoard = (
	members: readonly BoardMember[],
): Map<number, number> => {
	const sorted = [...members].sort(
		(a, b) => b.value - a.value || pickOrder(a) - pickOrder(b) || a.pid - b.pid,
	);
	return new Map(sorted.map((m, i) => [m.pid, i + 1]));
};

export type Available = {
	rank: number;
	name: string;
	pos?: string;
	college?: string;
};

// The best still on the board once this pick is in: everybody taken after it,
// or never taken, in big-board order, each with his number on the board.
export const bestAvailable = (
	members: readonly BoardMember[],
	at: { round: number; pick: number },
	n: number,
): Available[] => {
	const board = bigBoard(members);
	const after = pickOrder(at);
	return members
		.filter((m) => pickOrder(m) > after)
		.map((m) => ({
			rank: board.get(m.pid)!,
			name: m.name,
			pos: m.pos,
			college: m.college,
		}))
		.sort((a, b) => a.rank - b.rank)
		.slice(0, n);
};

export type Slot = {
	round: number;
	pick: number;
	tid: number;
};

// The next picks after this one, in order: each slot once, however many ways
// it was heard of (a man taken there, or a pick still to be made).
export const nextPicks = <T extends Slot>(
	slots: readonly T[],
	at: { round: number; pick: number },
	n: number,
): T[] => {
	const after = pickOrder(at);
	const seen = new Set<number>();
	const out: T[] = [];
	for (const s of [...slots].sort((a, b) => pickOrder(a) - pickOrder(b))) {
		const o = pickOrder(s);
		if (o === Infinity || o <= after || seen.has(o)) {
			continue;
		}
		seen.add(o);
		out.push(s);
		if (out.length >= n) {
			break;
		}
	}
	return out;
};

export const ordinal = (n: number): string => {
	const t = n % 100;
	const s =
		t >= 11 && t <= 13 ? "th" : (["th", "st", "nd", "rd"][n % 10] ?? "th");
	return `${n}${s}`;
};

// What the commissioner says at the podium: the pick counted through the
// whole draft, the way it is announced ("the 31st pick" opens round two).
export const announcement = ({
	overall,
	season,
	team,
}: {
	overall: number;
	season: number;
	team: string;
}): string =>
	`With the ${ordinal(overall)} pick in the ${season} draft, the ${team} select`;

// The pick counted through the whole draft: every pick of the rounds before
// it, then this one. A round's size is its last pick anybody knows of.
export const overallPick = (
	slots: readonly { round: number; pick: number }[],
	at: { round: number; pick: number },
): number => {
	const roundSize = new Map<number, number>();
	for (const s of slots) {
		if (s.round >= 1 && s.pick >= 1) {
			roundSize.set(s.round, Math.max(roundSize.get(s.round) ?? 0, s.pick));
		}
	}
	let before = 0;
	for (let r = 1; r < at.round; r++) {
		before += roundSize.get(r) ?? 0;
	}
	return before + at.pick;
};

// The lines that go by under his name on the way to the stage, after his age
// and size: true things only, the most telling first.
export const draftFacts = ({
	relatives,
	rank,
	born,
	via,
}: {
	relatives: readonly { type: string; name: string }[];
	rank: number | undefined;
	born: string | undefined;
	via: string | undefined;
}): string[] => {
	const out: string[] = [];
	for (const r of relatives) {
		if (r.type === "father") {
			out.push(`Son of ${r.name}`);
		} else if (r.type === "brother") {
			out.push(`Brother of ${r.name}`);
		}
	}
	if (rank !== undefined) {
		out.push(
			rank === 1 ? "No. 1 on the big board" : `No. ${rank} on the big board`,
		);
	}
	if (born) {
		out.push(`From ${born}`);
	}
	if (via) {
		out.push(`Pick acquired from ${via}`);
	}
	return out;
};
