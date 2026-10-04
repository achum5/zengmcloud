import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import type { Col, DataTableRow } from "../components/DataTable/index.tsx";
import type { View } from "../../common/types.ts";

const Movement = ({
	rank,
	prevRank,
}: {
	rank: number;
	prevRank: number | null | undefined;
}) => {
	if (prevRank === undefined) {
		return null;
	}
	if (prevRank === null) {
		return <span className="text-body-secondary">New</span>;
	}
	const diff = prevRank - rank;
	if (diff === 0) {
		return <span className="text-body-secondary">–</span>;
	}
	return diff > 0 ? (
		<span className="text-success">▲{diff}</span>
	) : (
		<span className="text-danger">▼{-diff}</span>
	);
};

const Top25 = (props: View<"top25">) => {
	useTitleBar({ title: "Top 25" });

	if (!props.college) {
		return <p>The Top 25 is only in college leagues.</p>;
	}
	if (props.rows.length === 0) {
		return <p>The first poll comes out when the season starts.</p>;
	}

	const cols: Col[] = [
		{ title: "#", sortType: "number" },
		{ title: "Team", sortType: "string" },
		{ title: "Conf" },
		{ title: "W-L", sortType: "record" },
		{ title: "Prev", desc: "Last week", sortType: "number" },
		{ title: "" },
	];

	const rows: DataTableRow[] = props.rows.map((r) => ({
		key: r.tid,
		classNames: { "table-info": r.tid === props.userTid },
		data: [
			r.rank,
			<a
				href={helpers.leagueUrl([
					"roster",
					`${r.abbrev}_${r.tid}`,
					props.season,
				])}
			>
				{r.region} {r.name}
			</a>,
			r.conf,
			`${r.won}-${r.lost}`,
			r.prevRank ?? "",
			<Movement rank={r.rank} prevRank={r.prevRank} />,
		],
	}));

	return (
		<>
			<p>{props.week === 0 ? "Preseason" : `Week ${props.week}`}</p>
			<DataTable
				cols={cols}
				defaultSort={[0, "asc"]}
				name="Top25"
				rows={rows}
			/>
		</>
	);
};

export default Top25;
