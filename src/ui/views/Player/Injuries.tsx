import { DataTable } from "../../components/DataTable/index.tsx";
import { getCols } from "../../../common/getCols.ts";

const cols = getCols(["Year", "Type", "Games", "Ovr Drop", "Pot Drop"], {
	Type: {
		width: "100%",
	},
});

const Injuries = ({
	injuries,
	showRatings,
}: {
	injuries: {
		games: number;
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

	const totals = {
		games: injuries.reduce((sum, injury) => sum + injury.games, 0),
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
						showRatings ? injury.ovrDrop : null,
						showRatings ? injury.potDrop : null,
					],
				};
			})}
			footer={{
				data: [
					"Total",
					null,
					totals.games,
					showRatings ? totals.ovrDrop : null,
					showRatings ? totals.potDrop : null,
				],
			}}
		/>
	);
};

export default Injuries;
