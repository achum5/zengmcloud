// A player has exactly ONE note, but season recaps are written every year. So
// the note is kept as a stack of year-headed sections, newest first:
//
//   [2007]
//   His third straight All-Star year...
//
//   [2006]
//   A breakout...
//
// Writing a year that is already present REPLACES that year's section and
// leaves every other year alone, so a batch can be safely re-run. Anything the
// user typed themselves that isn't under a year header is preserved, pushed
// below the year sections rather than thrown away.

// A line that is exactly a bracketed 4-digit year, e.g. "[2007]".
const YEAR_HEADER = /^\s*\[(\d{4})]\s*$/;

export const seasonNoteHeader = (season: number) => `[${season}]`;

export type SeasonNoteSection = {
	// undefined for text that came before any year header (hand-written notes).
	season: number | undefined;
	body: string;
};

// Split a note into its year sections, in the order they appear.
export const parseSeasonNote = (note: string): SeasonNoteSection[] => {
	const sections: SeasonNoteSection[] = [];
	let current: SeasonNoteSection | undefined;

	for (const line of note.split("\n")) {
		const match = YEAR_HEADER.exec(line);
		if (match) {
			if (current) {
				sections.push(current);
			}
			current = { season: Number.parseInt(match[1]!), body: "" };
		} else if (current) {
			current.body += (current.body === "" ? "" : "\n") + line;
		} else {
			// Preamble: text with no year header above it.
			if (sections.length === 0) {
				sections.push({ season: undefined, body: line });
			} else {
				sections[0]!.body += `\n${line}`;
			}
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
		.filter((section) => section.body !== "" || section.season !== undefined)
		.map((section) =>
			section.season === undefined
				? section.body
				: `${seasonNoteHeader(section.season)}\n${section.body}`,
		)
		.join("\n\n")
		.trim();

// Add (or replace) one season's recap in a player's note.
//
// Year sections sort newest-first so the most recent season is what you see
// when the note is collapsed. Free-form text the user wrote keeps its place at
// the BOTTOM - it is theirs, and it is not about any particular season.
export const upsertSeasonNote = (
	existingNote: string | undefined,
	season: number,
	recap: string,
): string => {
	const body = recap.trim();
	const sections = parseSeasonNote(existingNote ?? "");

	const freeform = sections.filter((section) => section.season === undefined);
	const years = sections.filter((section) => section.season !== undefined);

	const index = years.findIndex((section) => section.season === season);
	if (index >= 0) {
		years[index] = { season, body };
	} else {
		years.push({ season, body });
	}

	years.sort((a, b) => (b.season ?? 0) - (a.season ?? 0));

	return renderSeasonNote([...years, ...freeform]);
};

// Does this note already have a recap for the season? Used to skip work and to
// report how much of a season is already written.
export const hasSeasonNote = (
	existingNote: string | undefined,
	season: number,
): boolean =>
	parseSeasonNote(existingNote ?? "").some(
		(section) => section.season === season && section.body !== "",
	);
