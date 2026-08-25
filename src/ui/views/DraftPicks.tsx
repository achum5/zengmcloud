import { DataTable } from "../components/DataTable/index.tsx";
import { MoreLinks } from "../components/MoreLinks.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { getCols } from "../../common/getCols.ts";
import type { View } from "../../common/types.ts";
import type { DataTableRow } from "../components/DataTable/index.tsx";
import { orderBy } from "../../common/utils.ts";
import Note from "./Player/Note.tsx";
import { useLocal } from "../util/local.ts";
import {
	delayedTeamOvrNote,
	powerRankingIsJustTeamOvr,
	type TeamOvrDisplay,
} from "../../common/teamRatings.ts";

const processRows = ({
	teamOvr,
	draftPicks,
	outgoing,
}: Pick<View<"draftPicks">, "draftPicks"> & {
	teamOvr: TeamOvrDisplay;
	outgoing: boolean;
}) => {
	const rows: DataTableRow[] = orderBy(
		draftPicks,
		[
			"season",
			"round",
			(dp) => (dp.pick > 0 ? dp.pick : (dp.projectedPick ?? 0)),
			"powerRanking",
		],
		["asc", "asc", "asc", "asc"],
	).map((dp) => {
		return {
			key: dp.dpid,
			data: [
				dp.season === "fantasy"
					? {
							value: "Fantasy",
							sortValue: -Infinity,
						}
					: dp.season === "expansion"
						? {
								value: "Expansion",
								sortValue: -Infinity,
							}
						: dp.season,
				dp.round,
				dp.pick > 0 ? (
					dp.pick
				) : dp.projectedPick !== undefined ? (
					<i className="text-body-secondary">{dp.projectedPick}</i>
				) : null,
				dp.originalTid !== dp.tid ? (
					outgoing ? (
						<a href={helpers.leagueUrl(["roster", `${dp.abbrev}_${dp.tid}`])}>
							{dp.abbrev}
						</a>
					) : (
						<a
							href={helpers.leagueUrl([
								"roster",
								`${dp.originalAbbrev}_${dp.originalTid}`,
							])}
						>
							{dp.originalAbbrev}
						</a>
					)
				) : null,
				dp.powerRanking,
				// Today's rating, the one from N seasons ago, or nothing at all -
				// whichever the league's team-ratings settings allow. The column
				// itself is dropped in the "hidden" case, so `null` here only ever
				// covers a team with no recorded rating for the delayed season.
				teamOvr.type === "current"
					? dp.ovr
					: teamOvr.type === "delayed"
						? (dp.ovrDelayed ?? null)
						: null,
				helpers.formatRecord(dp.record),
				dp.avgAge?.toFixed(1),
				dp.trades
					? {
							value: (
								<>
									{dp.originalAbbrev}
									{dp.trades.map((info) => {
										return (
											<>
												{" "}
												→{" "}
												<a
													href={helpers.leagueUrl(["trade_summary", info.eid])}
												>
													{info.abbrev}
												</a>
											</>
										);
									})}
								</>
							),
							searchValue: `${dp.originalAbbrev}${dp.trades.map((info) => ` → ${info.abbrev}`)}`,
							sortValue: dp.trades.length,
						}
					: null,
				{
					value: (
						<Note
							note={dp.note}
							info={{
								type: "draftPick",
								dpid: dp.dpid,
							}}
							infoLink
							xs
						/>
					),
					searchValue: dp.note,
					sortValue: dp.note,
				},
			],
		};
	});

	return rows;
};

export const getDraftPicksColsAndRows = ({
	noGamesYet,
	teamOvr,
	draftPicks,
	draftPicksOutgoing,
}: Pick<View<"draftPicks">, "draftPicks" | "draftPicksOutgoing"> & {
	noGamesYet: boolean;
	teamOvr: TeamOvrDisplay;
}) => {
	const cols = getCols(
		[
			"Year",
			"Draft Round",
			"Draft Pick",
			"Team",
			"Power Ranking",
			"Ovr",
			"Record",
			"AvgAge",
			"Trades",
			"Note",
		],
		{
			"Draft Round": {
				title: "Round",
			},
			"Draft Pick": {
				title: "Pick",
			},
			AvgAge: {
				title: "Avg Age",
			},
			Ovr: {
				// A delayed rating has to SAY which season it is from. An unlabelled
				// old number is worse than none, because it reads as the current one.
				title:
					teamOvr.type === "delayed"
						? `Team Ovr ${teamOvr.season}`
						: "Team Ovr",
				desc:
					teamOvr.type === "delayed"
						? delayedTeamOvrNote(teamOvr.season)
						: undefined,
			},
			Note: {
				classNames: "w-100",
			},
		},
	);

	const rows = processRows({
		teamOvr,
		draftPicks,
		outgoing: false,
	});

	const rowsOutgoing = processRows({
		teamOvr,
		draftPicks: draftPicksOutgoing,
		outgoing: true,
	});

	const dropColumn = (index: number) => {
		cols.splice(index, 1);
		for (const row of [...rows, ...rowsOutgoing]) {
			row.data.splice(index, 1);
		}
	};

	// Hidden means the column goes entirely, rather than a row of blanks under a
	// heading promising a number. Highest index first, so removing one does not
	// move the other.
	const ovrIndex = 5;
	if (teamOvr.type === "hidden") {
		dropColumn(ovrIndex);
	}

	// And the power ranking goes with it before any games are played, because
	// until there are results to fold in, the ranking IS the hidden ratings
	// sorted - a league with team ratings off could read the whole league's
	// pecking order off this page while the Power Rankings page refused to show
	// it. See powerRankingIsJustTeamOvr.
	const powerRankingIndex = 4;
	if (powerRankingIsJustTeamOvr({ display: teamOvr, noGamesYet })) {
		dropColumn(powerRankingIndex);
	}

	return {
		cols,
		rows,
		rowsOutgoing,
	};
};

const DraftPicks = ({
	abbrev,
	draftPicks,
	draftPicksOutgoing,
	noGamesYet,
	teamOvr,
	tid,
}: View<"draftPicks">) => {
	useTitleBar({
		title: "Draft Picks",
		dropdownView: "draft_picks",
		dropdownFields: { teams: abbrev },
	});

	const { draftType } = useLocal(["draftType"]);

	const { rows, rowsOutgoing, cols } = getDraftPicksColsAndRows({
		noGamesYet,
		teamOvr,
		draftPicks,
		draftPicksOutgoing,
	});

	return (
		<>
			<MoreLinks
				type="draft"
				page="draft_picks"
				abbrev={abbrev}
				draftType={draftType}
				tid={tid}
			/>

			<p>
				Projected draft pick numbers are shown in{" "}
				<i className="text-body-secondary">faded italics</i>.
			</p>

			{rows.length > 0 ? (
				<DataTable
					cols={cols}
					defaultSort={[0, "asc"]}
					name="DraftPicks"
					rows={rows}
					title={<h2>Owned picks</h2>}
				/>
			) : (
				<>
					<h2>Owned picks</h2>
					<p>None</p>
				</>
			)}

			{rowsOutgoing.length > 0 ? (
				<DataTable
					cols={cols}
					defaultSort={[0, "asc"]}
					name="DraftPicksOutgoing"
					rows={rowsOutgoing}
					title={<h2>Outgoing picks</h2>}
				/>
			) : (
				<>
					<h2>Outgoing picks</h2>
					<p>None</p>
				</>
			)}
		</>
	);
};

export default DraftPicks;
