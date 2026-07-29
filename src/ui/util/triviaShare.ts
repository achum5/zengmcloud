// The shareable summary of a finished grid.
//
// The block of squares is the whole point of sharing one of these: it says how
// you did without saying a single answer, so it can be posted in a league chat
// where everyone is about to play the same grid. Colors follow the same six
// rarity tiers the board uses, so a wall of red is legibly a harder board than
// a wall of white even to someone who never opens the app.

export const TIER_EMOJI = {
	mythic: "🟥",
	legendary: "🟧",
	epic: "🟨",
	rare: "🟩",
	uncommon: "🟦",
	common: "⬜",
	empty: "⬛",
} as const;

export const tierEmoji = (points: number | undefined): string => {
	if (points === undefined) {
		return TIER_EMOJI.empty;
	}
	if (points >= 90) {
		return TIER_EMOJI.mythic;
	}
	if (points >= 75) {
		return TIER_EMOJI.legendary;
	}
	if (points >= 60) {
		return TIER_EMOJI.epic;
	}
	if (points >= 40) {
		return TIER_EMOJI.rare;
	}
	if (points >= 20) {
		return TIER_EMOJI.uncommon;
	}
	return TIER_EMOJI.common;
};

export const buildGridShareText = ({
	// One entry per cell, in reading order. `undefined` = unsolved.
	points,
	score,
	hintedCount,
}: {
	points: (number | undefined)[];
	score: number;
	hintedCount: number;
}): string => {
	const rows: string[] = [];
	for (let r = 0; r < 3; r++) {
		rows.push([0, 1, 2].map((c) => tierEmoji(points[r * 3 + c])).join(""));
	}
	const solved = points.filter((p) => p !== undefined).length;

	// Hints are disclosed rather than hidden: a nine-cell board solved on hints
	// is a different achievement, and leaving it out makes the number a lie.
	const notes = [`${solved}/9`, `${score} points`];
	if (hintedCount > 0) {
		notes.push(`${hintedCount} hinted`);
	}

	return [
		solved === 9 ? "Immaculate! 🏆 Basketball GM Grids" : "Basketball GM Grids",
		...rows,
		notes.join(" · "),
	].join("\n");
};

// Hand the summary off however the device can. `navigator.share` is the right
// thing on a phone (it opens the real share sheet); the clipboard is the right
// thing everywhere else. Returns what happened so the button can say so.
export const shareOrCopy = async (
	text: string,
): Promise<"shared" | "copied" | "failed"> => {
	if (typeof navigator !== "undefined" && navigator.share) {
		try {
			await navigator.share({ text });
			return "shared";
		} catch {
			// A cancelled share sheet is not a failure - fall through to the
			// clipboard rather than reporting an error the user caused on purpose.
		}
	}
	try {
		await navigator.clipboard.writeText(text);
		return "copied";
	} catch {
		return "failed";
	}
};
