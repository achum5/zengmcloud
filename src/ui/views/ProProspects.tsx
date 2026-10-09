import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { helpers } from "../util/helpers.ts";
import { downloadFile } from "../util/downloadFile.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { showNotification } from "../util/showNotification.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import type { Col, DataTableRow } from "../components/DataTable/index.tsx";
import { wrappedPlayerNameLabels } from "../components/PlayerNameLabels.tsx";
import type { View } from "../../common/types.ts";

const ProProspects = (props: View<"proProspects">) => {
	useTitleBar({ title: "Pro Prospects" });

	if (!props.college) {
		return <p>Pro prospects are only in college leagues.</p>;
	}

	const { season, players, leagues, linkedLid } = props;
	const seasons: number[] = [];
	for (let s = props.currentSeason; s >= props.startingSeason; s--) {
		seasons.push(s);
	}

	const cols: Col[] = [
		{ title: "Name", sortType: "name" },
		{ title: "Pos" },
		{ title: "School" },
		{ title: "Class" },
		{ title: "Age", sortType: "number" },
		{ title: "Ovr", sortSequence: ["desc", "asc"], sortType: "number" },
		{ title: "Pot", sortSequence: ["desc", "asc"], sortType: "number" },
	];

	const rows: DataTableRow<"player">[] = players.map((p) => ({
		key: p.pid,
		metadata: { type: "player", pid: p.pid, season, playoffs: "regularSeason" },
		data: [
			wrappedPlayerNameLabels({
				pid: p.pid,
				firstName: p.firstName,
				lastName: p.lastName,
				skills: p.skills,
			}),
			p.pos,
			p.tid !== undefined ? (
				<a href={helpers.leagueUrl(["roster", `${p.abbrev}_${p.tid}`, season])}>
					{p.abbrev}
				</a>
			) : (
				""
			),
			p.classLabel,
			p.age,
			p.ovr,
			p.pot,
		],
	}));

	return (
		<>
			<div className="d-flex flex-wrap gap-2 align-items-center mb-3">
				<select
					className="form-select w-auto"
					value={season}
					onChange={(event) => {
						void realtimeUpdate(
							[],
							helpers.leagueUrl(["pro_prospects", event.target.value]),
						);
					}}
				>
					{seasons.map((s) => (
						<option key={s} value={s}>
							{s}
						</option>
					))}
				</select>
				<button
					type="button"
					className="btn btn-light-bordered"
					disabled={players.length === 0}
					onClick={async () => {
						const file = await toWorker(
							"main",
							"collegeExportDraftClass",
							season,
						);
						downloadFile(
							`draft-class-${season}.json`,
							JSON.stringify(file),
							"application/json",
						);
					}}
				>
					Export draft class
				</button>
				<select
					className="form-select w-auto"
					value={linkedLid ?? ""}
					title="Pro league that gets each year's draft class"
					onChange={(event) =>
						void toWorker(
							"main",
							"collegeSetLinkedLeague",
							event.target.value === ""
								? undefined
								: Number(event.target.value),
						)
					}
				>
					<option value="">No linked league</option>
					{leagues.map((l) => (
						<option key={l.lid} value={l.lid}>
							{l.name}
						</option>
					))}
				</select>
				{linkedLid !== undefined ? (
					<button
						type="button"
						className="btn btn-light-bordered"
						disabled={players.length === 0}
						onClick={async () => {
							const error = await toWorker(
								"main",
								"collegeSendToLinkedLeague",
								season,
							);
							if (error) {
								showNotification({ type: "error", text: error });
							}
						}}
					>
						Send to linked league
					</button>
				) : null}
			</div>
			{players.length === 0 ? (
				<p>No one declared for the draft.</p>
			) : (
				<DataTable
					cols={cols}
					defaultSort={[5, "desc"]}
					defaultStickyCols={1}
					name="ProProspects"
					pagination
					rows={rows}
				/>
			)}
		</>
	);
};

export default ProProspects;
