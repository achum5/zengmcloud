import useTitleBar from "../../hooks/useTitleBar.tsx";
import { helpers } from "../../util/helpers.ts";
import { getCols } from "../../../common/getCols.ts";
import { DataTable } from "../../components/DataTable/index.tsx";
import type { View } from "../../../common/types.ts";
import { frivolitiesMenu } from "../Frivolities.tsx";
import GOATFormula from "./GOATFormula.tsx";
import { wrappedPlayerNameLabels } from "../../components/PlayerNameLabels.tsx";
import type { DataTableRow } from "../../components/DataTable/index.tsx";
import { wrappedCurrency } from "../../components/wrappedCurrency.ts";
import { SafeHtml } from "../../components/SafeHtml.tsx";
import { useLocal } from "../../util/local.ts";
import { GoatBreakdown, formatGoatValue } from "./GoatBreakdown.tsx";

export const getValue = (
	obj: any,
	key: View<"most">["extraCols"][number]["key"],
) => {
	return typeof key === "string"
		? obj[key]
		: key.length === 2
			? obj[key[0]][key[1]]
			: obj[key[0]][key[1]][key[2]];
};

const Most = ({
	description,
	extraCols,
	extraProps,
	players,
	stats,
	title,
	type,
}: View<"most">) => {
	useTitleBar({ title, customMenu: frivolitiesMenu });

	const { challengeNoRatings, userTid } = useLocal([
		"challengeNoRatings",
		"userTid",
	]);

	const hasBestSeasonOverride = players.some(
		(p) => p.most?.extra?.bestSeasonOverride !== undefined,
	);

	const superCols = [
		{
			title: "",
			colspan: 7 + extraCols.length,
		},
		{
			title: hasBestSeasonOverride ? "Season Stats" : "Best Season",
			colspan: 2 + stats.length,
		},
		{
			title: "Career Stats",
			colspan: stats.length,
		},
	];

	const cols = getCols([
		"#",
		"Name",
		...extraCols.map((x) => x.colName),
		"Pos",
		"Drafted",
		"Retired",
		"Pick",
		"Peak Ovr",
		"Year",
		"Team",
		...stats.map((stat) => `stat:${stat}`),
		...stats.map((stat) => `stat:${stat}`),
	]);

	const rows: DataTableRow[] = players.map((p, i) => {
		const showRatings = !challengeNoRatings || p.retiredYear !== Infinity;

		const draftPick =
			p.draft.round > 0 ? `${p.draft.round}-${p.draft.pick}` : "";

		const hasBestStats = p.bestStats.season !== undefined;

		return {
			key: i,
			metadata: {
				type: "player",
				pid: p.pid,
				season:
					p.most?.extra?.bestSeasonOverride ??
					p.most?.extra?.season ??
					"career",
				playoffs: "regularSeason",
			},
			data: [
				p.rank,
				wrappedPlayerNameLabels({
					awards: p.awards,
					jerseyNumber: p.jerseyNumber,
					pid: p.pid,
					firstName: p.firstName,
					firstNameShort: p.firstNameShort,
					lastName: p.lastName,
				}),
				...extraCols.map((x) => {
					const value = getValue(p, x.key);
					if (x.colName === "Amount") {
						return wrappedCurrency(value / 1000, "M");
					}
					if (x.colName === "Prog") {
						return helpers.plusMinus(value, 0);
					}
					if (x.colName === "GOAT") {
						return {
							value: (
								<GoatBreakdown
									name={`${p.firstName} ${p.lastName}`}
									pid={p.pid}
									season={p.most?.extra?.bestSeasonOverride}
									value={value}
								/>
							),
							sortValue: value,
							searchValue: formatGoatValue(value),
						};
					}
					if (x.colName.startsWith("stat:")) {
						const stat = x.colName.replace("stat:", "");
						return helpers.roundStat(value, stat);
					}
					if (x.colName === "Team") {
						return (
							<a
								href={helpers.leagueUrl([
									"team_history",
									`${value.abbrev}_${value.tid}`,
								])}
							>
								{value.abbrev}
							</a>
						);
					}
					if (x.colName === "Ovr" && !showRatings) {
						return null;
					}
					return value;
				}),
				p.bestPos,
				p.draft.year,
				p.retiredYear === Infinity ? null : p.retiredYear,
				draftPick,
				showRatings ? p.peakOvr : null,
				p.bestStats.season,
				hasBestStats ? (
					<a
						href={helpers.leagueUrl([
							"roster",
							`${p.bestStats.abbrev}_${p.bestStats.tid}`,
							p.bestStats.season,
						])}
					>
						{p.bestStats.abbrev}
					</a>
				) : null,
				...stats.map((stat) =>
					hasBestStats ? helpers.roundStat(p.bestStats[stat], stat) : null,
				),
				...stats.map((stat) =>
					hasBestStats ? helpers.roundStat(p.careerStats[stat], stat) : null,
				),
			],
			classNames: {
				"table-danger": p.hof,
				"table-success": p.retiredYear === Infinity,
				"table-info": p.statsTids.includes(userTid),
			},
		};
	});

	return (
		<>
			{description ? (
				<p>
					<SafeHtml dirty={description} />
				</p>
			) : null}

			{type === "goat" || type === "goat_season" ? (
				<GOATFormula
					key={type}
					customAwards={extraProps.customAwards}
					formula={extraProps.formula}
					oldAwards={extraProps.oldAwards}
					simpleAwards={extraProps.simpleAwards}
					stats={extraProps.stats}
					type={type === "goat_season" ? "season" : "career"}
				/>
			) : null}

			<p>
				Players who have played for your team are{" "}
				<span className="text-info">highlighted in blue</span>. Active players
				are <span className="text-success">highlighted in green</span>. Hall of
				Famers are <span className="text-danger">highlighted in red</span>.
			</p>

			<DataTable
				cols={cols}
				defaultSort={[0, "asc"]}
				defaultStickyCols={window.mobile ? 0 : 2}
				name={`Most_${type}`}
				rows={rows}
				superCols={superCols}
			/>
		</>
	);
};

export default Most;
