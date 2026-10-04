import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { showNotification } from "../util/showNotification.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import type { Col, DataTableRow } from "../components/DataTable/index.tsx";
import { wrappedPlayerNameLabels } from "../components/PlayerNameLabels.tsx";
import { Height } from "../components/Height.tsx";
import type { View } from "../../common/types.ts";

const WalkOns = (props: View<"walkOns">) => {
	useTitleBar({ title: "Walk-ons" });

	if (!props.college) {
		return <p>Walk-ons are only in college leagues.</p>;
	}

	const full = props.rosterSize >= props.maxRosterSize;

	const cols: Col[] = [
		{ title: "Name", sortType: "name" },
		{ title: "Pos" },
		{ title: "Class" },
		{ title: "Ht", sortType: "number" },
		{ title: "Ovr", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "Pot", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "" },
	];

	const rows: DataTableRow[] = props.players.map((p) => ({
		key: p.pid,
		metadata: {
			type: "player",
			pid: p.pid,
			season: props.season,
			playoffs: "regularSeason",
		},
		data: [
			wrappedPlayerNameLabels({
				pid: p.pid,
				injury: p.injury,
				firstName: p.firstName,
				lastName: p.lastName,
				skills: p.ratings.skills,
			}),
			p.ratings.pos,
			p.classLabel,
			{ value: <Height inches={p.hgt} />, sortValue: p.hgt },
			p.ratings.ovr,
			p.ratings.pot,
			<button
				type="button"
				className="btn btn-xs btn-light-bordered"
				disabled={full}
				onClick={async () => {
					const error = await toWorker("main", "collegeSignWalkOn", p.pid);
					if (error) {
						showNotification({ type: "error", text: error });
					}
				}}
			>
				Sign
			</button>,
		],
	}));

	return (
		<>
			<p>
				Roster: {props.rosterSize}/{props.maxRosterSize}
			</p>
			<DataTable
				cols={cols}
				defaultSort={[4, "desc"]}
				defaultStickyCols={1}
				name="WalkOns"
				pagination
				rows={rows}
			/>
		</>
	);
};

export default WalkOns;
