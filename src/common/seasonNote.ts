// A player has exactly ONE note, but writeups are produced every season and
// again when they retire. So the note is a stack of headed sections, newest
// first - the most recent thing that happened to them is what you see when the
// note is collapsed, and reading down is reading backwards through the career:
//
//   [2012] Retirement — The quiet exit
//   After fourteen seasons...
//
//   [2012] A farewell tour
//   His last year in Boston...
//
//   [2011] Still the anchor
//   ...
//
// Writing a section that already exists REPLACES it and leaves every other
// section alone, so a batch is safe to re-run after a bad reply. A retirement
// writeup and that same year's season recap are DIFFERENT sections, because a
// player usually retires in a year he also played.
//
// Anything the user typed themselves - text under no header - is preserved and
// kept at the bottom. It is the one thing here that cannot be regenerated.

export type SeasonNoteKind = "season" | "retirement";

// "[2007]" or "[2007] Some headline", optionally marked as the retirement
// writeup. An em dash or a plain hyphen both work, since an AI may emit either.
const HEADER = /^\s*\[(\d{4})]\s*(.*)$/;

// "Retirement — Some headline", or bare "Retirement" when there is no headline.
// The bare form has to match: without it a headline-less retirement section
// round-tripped as a SEASON section named "Retirement", so the next write
// couldn't find it and appended a second block for the same year. Requiring a
// dash OR end-of-string keeps a real headline like "Retirement day" from being
// mistaken for the marker.
const RETIREMENT_PREFIX = /^Retirement(?:\s*[—-]\s*|$)/;

export type SeasonNoteSection = {
	// undefined for text that came before any header (hand-written notes).
	season: number | undefined;
	kind: SeasonNoteKind | undefined;
	headline: string;
	body: string;
};

export const renderSectionHeader = (
	season: number,
	kind: SeasonNoteKind,
	headline: string,
): string => {
	const label =
		kind === "retirement"
			? `Retirement${headline ? ` — ${headline}` : ""}`
			: headline;
	return label ? `[${season}] ${label}` : `[${season}]`;
};

// One header line, split into its parts. Returns undefined for anything that
// isn't a header, so a reader can walk a note line by line.
export const parseSectionHeader = (
	line: string,
): { season: number; kind: SeasonNoteKind; headline: string } | undefined => {
	const match = HEADER.exec(line);
	if (!match) {
		return undefined;
	}
	const rest = (match[2] ?? "").trim();
	const isRetirement = RETIREMENT_PREFIX.test(rest);
	return {
		season: Number.parseInt(match[1]!),
		kind: isRetirement ? "retirement" : "season",
		headline: isRetirement ? rest.replace(RETIREMENT_PREFIX, "") : rest,
	};
};

// How a header should READ on the page, as markdown.
//
// A retirement writeup is an article about a whole career, not an entry in a
// season log, so it gets its headline on its own line and drops the year -
// which is noise anyway, since the writeup spans every season the player
// played. Stored form is unchanged ("[2003] Retirement — Headline"); this is
// purely how it's shown, so re-running a batch still finds and replaces the
// same section.
//
// Season sections keep their [year] label: there, the year is the whole point.
export const displaySectionHeader = (line: string): string => {
	const header = parseSectionHeader(line);
	if (!header || header.kind !== "retirement") {
		return line;
	}
	// The trailing newline becomes a blank line once the note is rejoined, so
	// markdown renders the headline as its own paragraph instead of running it
	// into the first sentence.
	return `**${header.headline || "Retirement"}**\n`;
};

export const parseSeasonNote = (note: string): SeasonNoteSection[] => {
	const sections: SeasonNoteSection[] = [];
	let current: SeasonNoteSection | undefined;

	for (const line of note.split("\n")) {
		const match = HEADER.exec(line);
		if (match) {
			if (current) {
				sections.push(current);
			}
			const rest = (match[2] ?? "").trim();
			const isRetirement = RETIREMENT_PREFIX.test(rest);
			current = {
				season: Number.parseInt(match[1]!),
				kind: isRetirement ? "retirement" : "season",
				headline: isRetirement ? rest.replace(RETIREMENT_PREFIX, "") : rest,
				body: "",
			};
		} else if (current) {
			current.body += (current.body === "" ? "" : "\n") + line;
		} else if (sections.length === 0) {
			sections.push({
				season: undefined,
				kind: undefined,
				headline: "",
				body: line,
			});
		} else {
			sections[0]!.body += `\n${line}`;
		}
	}
	if (current) {
		sections.push(current);
	}

	return sections.map((section) => ({
		...section,
		body: section.body.trim(),
	}));
};

export const renderSeasonNote = (sections: SeasonNoteSection[]): string =>
	sections
		.filter((section) => section.body !== "" || section.headline !== "")
		.map((section) =>
			section.season === undefined || section.kind === undefined
				? section.body
				: [
						renderSectionHeader(section.season, section.kind, section.headline),
						section.body,
					]
						.filter(Boolean)
						.join("\n"),
		)
		.join("\n\n")
		.trim();

// Newest first. Within one year the retirement writeup sits above that year's
// season recap, because retiring is the last thing that happened.
const orderSections = (a: SeasonNoteSection, b: SeasonNoteSection) => {
	const seasonDiff = (b.season ?? 0) - (a.season ?? 0);
	if (seasonDiff !== 0) {
		return seasonDiff;
	}
	const rank = (s: SeasonNoteSection) => (s.kind === "retirement" ? 0 : 1);
	return rank(a) - rank(b);
};

// Add (or replace) one section of a player's note.
export const upsertSeasonNote = (
	existingNote: string | undefined,
	{
		season,
		kind = "season",
		headline = "",
		body,
	}: {
		season: number;
		kind?: SeasonNoteKind;
		headline?: string;
		body: string;
	},
): string => {
	const sections = parseSeasonNote(existingNote ?? "");
	const freeform = sections.filter((section) => section.season === undefined);
	const headed = sections.filter((section) => section.season !== undefined);

	const next: SeasonNoteSection = {
		season,
		kind,
		headline: headline.trim(),
		body: body.trim(),
	};

	const index = headed.findIndex(
		(section) => section.season === season && section.kind === kind,
	);
	if (index >= 0) {
		headed[index] = next;
	} else {
		headed.push(next);
	}

	headed.sort(orderSections);

	return renderSeasonNote([...headed, ...freeform]);
};

// Drop one section, leaving everything else alone. Used to clear a retirement
// writeup off a player who did not actually retire that year - the only way
// that can happen is a misfiled paste, and it would otherwise sit in the note
// forever, since re-running a season only ever replaces the SEASON section.
export const removeSeasonNote = (
	existingNote: string | undefined,
	season: number,
	kind: SeasonNoteKind,
): string => {
	const sections = parseSeasonNote(existingNote ?? "");
	const kept = sections.filter(
		(section) => !(section.season === season && section.kind === kind),
	);
	if (kept.length === sections.length) {
		return existingNote ?? "";
	}
	return renderSeasonNote(kept);
};

// Does this note already have a section for the season? Used to report how much
// of a season is already written.
export const hasSeasonNote = (
	existingNote: string | undefined,
	season: number,
	kind: SeasonNoteKind = "season",
): boolean =>
	parseSeasonNote(existingNote ?? "").some(
		(section) =>
			section.season === season && section.kind === kind && section.body !== "",
	);
