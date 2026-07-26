// Display labels for the parametric Grids criteria.
//
// In `common` because BOTH sides need them and they must agree exactly: the
// worker stamps the label onto the grid it builds, and the editor has to render
// the same text live as you type a threshold, before any worker round-trip has
// come back. Two copies of this formatting would drift the moment either side
// changed, and the mismatch would only show up mid-edit.

export type StatOp = "gte" | "lte";
export type DecadeMode = "debut" | "played";

// Just the display-facing part of a stat spec, so the UI can format a label
// from what the catalog ships without needing the worker's evaluation code.
export type StatSpecDisplay = {
	id: string;
	unit: string;
	scope: "career" | "season";
	decimals: number;
};

const fmtNumber = (value: number, decimals: number) =>
	decimals > 0
		? value.toFixed(decimals).replace(/\.0+$/, "")
		: Math.round(value).toLocaleString();

// "20,000+ Career Points" / "20 or fewer PPG (Season)".
export const statLabel = (
	spec: StatSpecDisplay,
	op: StatOp,
	value: number,
): string => {
	const n = fmtNumber(value, spec.decimals);
	// `season-gp` already says "(Season)" in its unit, so it doesn't get another.
	const seasonSuffix =
		spec.scope === "season" && spec.id !== "season-gp" ? " (Season)" : "";
	return op === "gte"
		? `${n}+ ${spec.unit}${seasonSuffix}`
		: `${n} or fewer ${spec.unit}${seasonSuffix}`;
};

export const decadeLabel = (mode: DecadeMode, decade: number): string =>
	mode === "debut" ? `Debuted in the ${decade}s` : `Played in the ${decade}s`;
