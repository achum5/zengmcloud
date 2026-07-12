// Compact labels for a set of seasons, used by the per-team and selected-rows
// subtotal footers. A single unbroken run reads as a range ("2057" or
// "2057-2058"); anything with a gap collapses to "N seasons" (too many ranges
// is ugly inline), with the full breakdown ("2057-2059, 2067, 2069") available
// as a tooltip / expanded view.

export type SeasonRuns = {
	// Inline label: "2057", "2057-2058", or "5 seasons".
	short: string;
	// Full breakdown, always the explicit ranges: "2057-2059, 2067, 2069".
	full: string;
	// True when short === full (a single run), so callers can skip a redundant
	// tooltip.
	single: boolean;
};

// Group a sorted, de-duped season list into consecutive runs.
const toRuns = (seasons: number[]): [number, number][] => {
	const runs: [number, number][] = [];
	for (const season of seasons) {
		const lastRun = runs.at(-1);
		if (lastRun && season === lastRun[1] + 1) {
			lastRun[1] = season;
		} else {
			runs.push([season, season]);
		}
	}
	return runs;
};

const formatRun = ([start, end]: [number, number]): string =>
	start === end ? `${start}` : `${start}-${end}`;

export const formatSeasonRuns = (seasonsRaw: number[]): SeasonRuns => {
	const seasons = Array.from(new Set(seasonsRaw)).sort((a, b) => a - b);
	if (seasons.length === 0) {
		return { short: "", full: "", single: true };
	}

	const runs = toRuns(seasons);
	const full = runs.map(formatRun).join(", ");

	if (runs.length === 1) {
		return { short: full, full, single: true };
	}

	return { short: `${seasons.length} seasons`, full, single: false };
};
