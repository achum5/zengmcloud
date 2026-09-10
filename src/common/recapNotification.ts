// A recap, as a push notification reads it.
//
// The recaps are markdown - a bold headline, an italic deck of one-line
// teasers, then the story in paragraphs - and a notification body is plain
// text with no styling at all. What survives the trip is the headline and the
// line under it; the deck is three fragments of the same night said shorter,
// which on a phone is the same news twice.
//
// Works on a filed recap as well as a generated one, so a league whose notes
// are written by hand or by an AI gets its own words pushed rather than the
// fallback.

const stripInline = (text: string): string =>
	text
		// [Player Name](/l/1/player/123) - the label is the part a reader wants.
		.replaceAll(/\[([^\]]*)]\([^)]*\)/g, "$1")
		.replaceAll(/\*\*([^*]+)\*\*/g, "$1")
		.replaceAll(/\*([^*]+)\*/g, "$1")
		.replaceAll(/[_`]/g, "")
		.replaceAll(/\s+/g, " ")
		.trim();

// A block wrapped in single asterisks, which is how the deck is written.
const isDeck = (block: string): boolean =>
	/^\*[^*]/.test(block) && block.endsWith("*");

const isHeadline = (block: string): boolean =>
	block.startsWith("**") || block.startsWith("#");

// Cut to `max` characters without ending mid-sentence where possible: back up
// to the last sentence that fits, and only fall back to an ellipsis when even
// the first sentence is longer than the budget.
export const trimToSentence = (text: string, max: number): string => {
	if (text.length <= max) {
		return text;
	}
	const head = text.slice(0, max + 1);
	const lastEnd = Math.max(
		head.lastIndexOf(". "),
		head.lastIndexOf("! "),
		head.lastIndexOf("? "),
	);
	if (lastEnd > 0) {
		return text.slice(0, lastEnd + 1);
	}
	const lastSpace = head.lastIndexOf(" ");
	return `${text.slice(0, lastSpace > 0 ? lastSpace : max).trimEnd()}…`;
};

export const MAX_RECAP_NOTIFICATION_CHARS = 300;

// The headline and the first line of the story, as two lines of body text.
// Undefined when the recap has nothing in it - the caller keeps whatever it
// was showing before.
export const recapNotificationBody = (
	recap: string | undefined,
	max = MAX_RECAP_NOTIFICATION_CHARS,
): string | undefined => {
	if (!recap) {
		return undefined;
	}
	const blocks = recap
		.split(/\n\s*\n/)
		.map((block) => block.trim())
		.filter((block) => block.length > 0);
	if (blocks.length === 0) {
		return undefined;
	}

	let headline: string | undefined;
	let rest = blocks;
	if (isHeadline(blocks[0]!)) {
		headline = stripInline(blocks[0]!.replace(/^#+\s*/, ""));
		rest = blocks.slice(1);
	}
	const first = rest.find((block) => !isDeck(block));

	const lines = [
		headline,
		first ? trimToSentence(stripInline(first), max) : undefined,
	]
		.filter((line): line is string => !!line && line.length > 0)
		// A recap whose headline IS its first line (a one-line note) says it once.
		.filter((line, i, all) => all.indexOf(line) === i);

	return lines.length > 0 ? lines.join("\n") : undefined;
};
