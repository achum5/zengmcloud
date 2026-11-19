// A TEAM'S BOX SCORE BY THE QUARTER: a little picker by its name (each
// team's its own), and the box score of just the quarter picked.

// What a box score can be cut into: each period's name ("1st" ... "OT"),
// and the box score of just that one (0 the first).
export type BoxPeriods = {
	labels: string[];
	boxOf: (k: number) => any;
};

export type PeriodPick = number | "all";

// The team (and the other one) as the period picked has them.
export const teamsInPeriod = (
	boxScore: any,
	periods: BoxPeriods | undefined,
	pick: PeriodPick,
	tid: number,
): { t: any; o: any } => {
	const box =
		periods && pick !== "all" && pick < periods.labels.length
			? periods.boxOf(pick)
			: boxScore;
	const teams = box.teams as any[];
	const t = teams.find((x) => x.tid === tid) ?? teams[0];
	const o = teams.find((x) => x !== t) ?? teams[1];
	return { t, o };
};

export const PeriodPicker = ({
	periods,
	value,
	onChange,
}: {
	periods: BoxPeriods;
	value: PeriodPick;
	onChange: (pick: PeriodPick) => void;
}) => (
	<select
		className="form-select form-select-sm box-period"
		aria-label="Quarter"
		value={String(value)}
		onChange={(e) => {
			const v = e.target.value;
			onChange(v === "all" ? "all" : Number(v));
		}}
	>
		<option value="all">All Qs</option>
		{periods.labels.map((label, k) => (
			<option key={k} value={k}>
				{label}
			</option>
		))}
	</select>
);
