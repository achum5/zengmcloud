// An injury's ovr/pot drop as a table cell. Coarse ratings mode shows "-" for a
// drop that didn't cost a full tens digit; it sorts between 0 and 1.
export const injuryDropCell = (drop: number | "-" | undefined) =>
	drop === "-" ? { value: "-", sortValue: 0.5 } : drop;
