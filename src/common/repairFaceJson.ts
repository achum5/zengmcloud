// A faces.js config pasted out of a chat AI arrives broken in a small number of
// predictable ways, and every one of them makes JSON.parse reject the whole
// object with a message about a character position. The user can't act on that,
// and the fix is always mechanical, so do it for them.
//
// Only shapes that cannot change meaning are repaired. Nothing here guesses at
// a value.

// Phone keyboards and chat apps smart-quote a plain " on copy. All four curly
// forms mean the same thing here: a face config's strings are ids, hex colors
// and rgba() - never prose - so none of them can legitimately contain a quote.
const CURLY = /[‘’“”]/g;

// The model was asked for bare JSON but sometimes fences it anyway.
const stripFence = (text: string): string => {
	const fence = /```(?:json)?\s*([\S\s]*?)```/.exec(text);
	return fence?.[1] ?? text;
};

// ...and sometimes writes a sentence before or after it.
const outermostObject = (text: string): string => {
	const start = text.indexOf("{");
	const end = text.lastIndexOf("}");
	return start >= 0 && end > start ? text.slice(start, end + 1) : text;
};

// A raw newline or tab inside a string literal - "Bad control character in
// string literal", which is what a wrapped long value looks like to the parser.
// Collapsed to a single space, since the only thing a break can be here is
// soft wrapping.
const collapseControlChars = (text: string): string => {
	let out = "";
	let inString = false;
	let escaped = false;

	for (const ch of text) {
		if (inString) {
			if (escaped) {
				out += ch;
				escaped = false;
			} else if (ch === "\\") {
				out += ch;
				escaped = true;
			} else if (ch === '"') {
				out += ch;
				inString = false;
			} else if (ch < " ") {
				if (!out.endsWith(" ")) {
					out += " ";
				}
			} else {
				out += ch;
			}
			continue;
		}

		if (ch === '"') {
			inString = true;
		}
		out += ch;
	}

	return out;
};

// A comma before a closing brace or bracket. Written as "comma, then only
// whitespace, then the close" so the commas inside an rgba(0,0,0,0.3) string -
// each followed by a digit - are untouched.
const dropTrailingCommas = (text: string): string =>
	text.replaceAll(/,(\s*[\]}])/g, "$1");

// The repaired text, or the original when it was already fine. Returns text
// rather than an object so the caller can show the user what it's going to
// save.
export const repairFaceJson = (text: string): string =>
	dropTrailingCommas(
		collapseControlChars(outermostObject(stripFence(text)).replace(CURLY, '"')),
	);

// Parse a pasted config, repairing it first if it needs it. undefined when even
// the repaired text isn't JSON - that's a paste of something else entirely, and
// guessing further would be inventing a face.
export const parseFaceJson = (text: string): unknown => {
	try {
		return JSON.parse(text);
	} catch {}

	try {
		return JSON.parse(repairFaceJson(text));
	} catch {}

	return undefined;
};
