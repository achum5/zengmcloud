import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import type { Col, DataTableRow } from "../components/DataTable/index.tsx";
import type { View } from "../../common/types.ts";

const RecruitingClasses = (props: View<"recruitingClasses">) => {
	useTitleBar({ title: "Class Rankings" });
	const [which, setWhich] = useState<"current" | "last">("current");

	if (!props.college) {
		return <p>Class rankings are only in college leagues.</p>;
	}

	const list = which === "current" ? props.current : props.last;

	const cols: Col[] = [
		{ title: "#", sortType: "number" },
		{ title: "Team", sortType: "string" },
		{ title: "Players", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "5★", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "4★", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "3★", sortSequence: ["desc", "asc"], sortType: "number" },
		{
			title: "Avg",
			desc: "Average stars",
			sortSequence: ["desc", "asc"],
			sortType: "number",
		},
		{ title: "Points", sortSequence: ["desc", "asc"], sortType: "number" },
	];

	const rows: DataTableRow[] = list.map((r) => ({
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
			r.count,
			r.fiveStars,
			r.fourStars,
			r.threeStars,
			r.avgStars.toFixed(2),
			r.points.toFixed(1),
		],
	}));

	return (
		<>
			<div className="btn-group mb-3">
				<button
					type="button"
					className={`btn btn-sm ${which === "current" ? "btn-primary" : "btn-light-bordered"}`}
					onClick={() => setWhich("current")}
				>
					Commitments
				</button>
				<button
					type="button"
					className={`btn btn-sm ${which === "last" ? "btn-primary" : "btn-light-bordered"}`}
					onClick={() => setWhich("last")}
				>
					Signed last year
				</button>
			</div>
			<DataTable
				key={which}
				cols={cols}
				defaultSort={[0, "asc"]}
				name="RecruitingClasses"
				pagination
				rows={rows}
			/>
		</>
	);
};

export default RecruitingClasses;
