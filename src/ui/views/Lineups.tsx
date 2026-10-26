import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { DataTable } from "../components/DataTable/index.tsx";
import { MoreLinks } from "../components/MoreLinks.tsx";
import { PlusMinus } from "../components/PlusMinus.tsx";
import { getCols } from "../../common/getCols.ts";
import { LineupFaces } from "../components/LineupFaces.tsx";
import type { View } from "../../common/types.ts";

// A team's fives over a season: minutes, possessions, and how they scored and
// defended per 100 possessions.

const MIN_MINUTES = [0, 5, 10, 25, 50, 100];

const per100 = (pts: number, poss: number) =>
	poss > 0 ? (100 * pts) / poss : undefined;

const number = (x: number | undefined) => ({
	value: x === undefined ? "" : x.toFixed(1),
	sortValue: x ?? -Infinity,
});

const shortName = (name: string | undefined) => {
	if (!name) {
		return "?";
	}
	const i = name.indexOf(" ");
	return i > 0 ? `${name[0]}. ${name.slice(i + 1)}` : name;
};

const Lineups = ({
	abbrev,
	colors,
	jersey,
	lineups,
	names,
	numGames,
	pics,
	playoffs,
	season,
	tid,
}: View<"lineups">) => {
	useTitleBar({
		title: "Lineups",
		dropdownView: "lineups",
		dropdownFields: {
			teams: abbrev,
			seasons: season,
			playoffs,
		},
	});
	const [minMinutes, setMinMinutes] = useState(10);

	const cols = getCols([
		"Lineup",
		"stat:gp",
		"stat:min",
		"Poss",
		"stat:pm",
		"stat:ortg",
		"stat:drtg",
		"stat:nrtg",
	]);

	const shown = lineups.filter((line) => line.sec >= minMinutes * 60);

	const rows = shown.map((line) => {
		const ortg = per100(line.pts, line.poss);
		const drtg = per100(line.ptsOpp, line.possOpp);
		const nrtg =
			ortg === undefined || drtg === undefined ? undefined : ortg - drtg;
		const pm = line.pts - line.ptsOpp;
		return {
			key: line.pids.join(","),
			data: [
				{
					value: (
						<LineupFaces
							pids={line.pids}
							pics={pics}
							colors={colors}
							jersey={jersey}
							scale={0.9}
						/>
					),
					searchValue: line.pids.map((pid) => names[pid] ?? "").join(" "),
					sortValue: line.pids.map((pid) => shortName(names[pid])).join(" "),
				},
				line.gp,
				number(line.sec / 60),
				line.poss,
				{ value: <PlusMinus decimalPlaces={0}>{pm}</PlusMinus>, sortValue: pm },
				number(ortg),
				number(drtg),
				{
					value: <PlusMinus>{nrtg}</PlusMinus>,
					sortValue: nrtg ?? -Infinity,
				},
			],
		};
	});

	return (
		<>
			<MoreLinks
				type="team"
				page="lineups"
				abbrev={abbrev}
				tid={tid}
				season={season}
			/>
			<div className="d-flex align-items-center gap-2 mb-2">
				<label htmlFor="lineups-min" className="mb-0">
					Min. minutes
				</label>
				<select
					id="lineups-min"
					className="form-select form-select-sm w-auto"
					value={minMinutes}
					onChange={(event) => {
						setMinMinutes(Number(event.target.value));
					}}
				>
					{MIN_MINUTES.map((x) => (
						<option key={x} value={x}>
							{x}
						</option>
					))}
				</select>
			</div>
			{numGames === 0 || lineups.length === 0 ? (
				<p>No lineups yet.</p>
			) : (
				<DataTable
					cols={cols}
					defaultSort={[2, "desc"]}
					name="Lineups"
					pagination={rows.length > 100}
					rows={rows}
				/>
			)}
		</>
	);
};

export default Lineups;
