import { DataTable } from "../../components/DataTable/index.tsx";
import { getCols } from "../../../common/getCols.ts";
import { injuryDropCell } from "../../util/injuryDropCell.ts";

const colsFor = (days: boolean) =>
	getCols(
		[
			"Year",
			"Type",
			"Games",
			...(days ? ["Days"] : []),
			"Ovr Drop",
			"Pot Drop",
		],
		{
			Type: {
				width: "100%",
			},
		},
	);

const Injuries = ({
	injuries,
	showRatings,
}: {
	injuries: {
		games: number;
		// On the NBA calendar.
		days?: number;
		season: number;
		type: string;
		// "-" in coarse ratings mode: a drop that didn't cost a full tens digit.
		ovrDrop?: number | "-";
		potDrop?: number | "-";
	}[];
	showRatings: boolean;
}) => {
	if (injuries === undefined || injuries.length === 0) {
		return <p>None</p>;
	}

	const total = (key: "ovrDrop" | "potDrop") => {
		let sum: number | undefined;
		let dropped = false;
		for (const injury of injuries) {
			const drop = injury[key];
			if (drop === "-") {
				dropped = true;
			} else if (drop !== undefined) {
				sum = (sum ?? 0) + drop;
			}
		}
		return dropped && !sum ? "-" : sum;
	};

	// Injuries from seasons on the NBA calendar also say their days.
	const days = injuries.some((injury) => injury.days !== undefined);
	const cols = colsFor(days);

	const totals = {
		games: injuries.reduce((sum, injury) => sum + injury.games, 0),
		days: injuries.reduce((sum, injury) => sum + (injury.days ?? 0), 0),
		ovrDrop: total("ovrDrop"),
		potDrop: total("potDrop"),
	};

	return (
		<DataTable
			className="datatable-negative-margin-top mb-3"
			cols={cols}
			defaultSort={[0, "asc"]}
			hideAllControls
			name="Player:Injuries"
			rows={injuries.map((injury, i) => {
				return {
					key: i,
					data: [
						{
							sortValue: i,
							value: injury.season,
						},
						injury.type,
						injury.games,
						...(days ? [injury.days ?? null] : []),
						showRatings ? injuryDropCell(injury.ovrDrop) : null,
						showRatings ? injuryDropCell(injury.potDrop) : null,
					],
				};
			})}
			footer={{
				data: [
					"Total",
					null,
					totals.games,
					...(days ? [totals.days] : []),
					showRatings ? totals.ovrDrop : null,
					showRatings ? totals.potDrop : null,
				],
			}}
		/>
	);
};

export default Injuries;
