// A recap, as a push notification reads it.
//
// The recaps are markdown - a bold headline, an italic deck of one-line
// teasers, then the story in paragraphs - and a notification is a title and a
// plain-text body with no styling at all. What survives the trip is the
// headline and the line under it; the deck is three fragments of the same
// night said shorter, which on a phone is the same news twice.
//
// The two go in the two slots a notification actually has: the HEADLINE IS
// THE TITLE, where a phone renders it bold and never truncates it, and the
// story's first line is the body. Putting both in the body wasted the title
// on a label ("Bye day for the Celtics") and pushed the news down a line.
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

// The first sentence of a line, or the whole line when it is one sentence.
// trimToSentence cannot do this job: it only cuts text that is OVER budget,
// and a short two-sentence note is under it, so the whole thing came back as
// the title and the body was left empty.
const firstSentence = (text: string, max: number): string => {
	const head = text.slice(0, max + 1);
	const end = Math.min(
		...[". ", "! ", "? "]
			.map((mark) => head.indexOf(mark))
			.filter((i) => i > 0)
			.concat(Number.POSITIVE_INFINITY),
	);
	if (Number.isFinite(end)) {
		return text.slice(0, end + 1);
	}
	return trimToSentence(text, max);
};

export const MAX_RECAP_NOTIFICATION_CHARS = 300;

// A headline is short by nature, but a hand-written note's first line need not
// be, and a title that runs on pushes the body off a phone's lock screen.
export const MAX_RECAP_NOTIFICATION_TITLE_CHARS = 90;

export type RecapNotification = {
	// The headline, for the notification's title. Undefined when the recap has
	// no headline at all - the caller keeps whatever title it had.
	title?: string;
	// The first line of the story.
	body?: string;
};

// The two pieces of a recap a notification can carry. Both are undefined when
// the recap is empty, so the caller falls back to what it was showing before.
export const recapNotificationParts = (
	recap: string | undefined,
	max = MAX_RECAP_NOTIFICATION_CHARS,
): RecapNotification => {
	if (!recap) {
		return {};
	}
	const blocks = recap
		.split(/\n\s*\n/)
		.map((block) => block.trim())
		.filter((block) => block.length > 0);
	if (blocks.length === 0) {
		return {};
	}

	let title: string | undefined;
	let rest = blocks;
	if (isHeadline(blocks[0]!)) {
		title = stripInline(blocks[0]!.replace(/^#+\s*/, ""));
		rest = blocks.slice(1);
	}
	const first = rest.find((block) => !isDeck(block));
	let body = first ? trimToSentence(stripInline(first), max) : undefined;

	// A one-line note IS its headline: said once, as the title, with nothing
	// under it.
	if (title !== undefined && body === title) {
		body = undefined;
	}

	// A note with no headline leads with its first SENTENCE instead, so the
	// news still lands in the title rather than under a generic one - and the
	// rest of the line follows as the body, rather than the title being
	// repeated underneath itself.
	if (title === undefined && body !== undefined) {
		title = firstSentence(body, MAX_RECAP_NOTIFICATION_TITLE_CHARS);
		const remainder = body.slice(title.length).trim();
		body = remainder.length > 0 ? remainder : undefined;
	}

	return {
		title:
			title === undefined || title.length === 0
				? undefined
				: trimToSentence(title, MAX_RECAP_NOTIFICATION_TITLE_CHARS),
		body: body === undefined || body.length === 0 ? undefined : body,
	};
};
