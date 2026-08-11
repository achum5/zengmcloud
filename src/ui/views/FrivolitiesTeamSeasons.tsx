import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { getCols } from "../../common/getCols.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import type { View } from "../../common/types.ts";
import { frivolitiesMenu } from "./Frivolities.tsx";
import { getValue } from "./Most/index.tsx";
import { isSport } from "../../common/sportFunctions.ts";
import { wrappedMovOrDiff } from "../components/MovOrDiff.tsx";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import { useLocal } from "../util/local.ts";
import { teamOvrDisplay } from "../../common/teamRatings.ts";

const FrivolitiesTeamSeasons = ({
	description,
	extraCols,
	teamSeasons,
	ties,
	otl,
	title,
	type,
	usePts,
}: View<"frivolitiesTeamSeasons">) => {
	useTitleBar({ title, customMenu: frivolitiesMenu });

	const {
		challengeNoRatings,
		hideTeamRatings,
		season,
		teamRatingsDelaySeasons,
		userTid,
	} = useLocal([
		"challengeNoRatings",
		"hideTeamRatings",
		"season",
		"teamRatingsDelaySeasons",
		"userTid",
	]);

	// Drop the column rather than leaving a permanently blank one. The worker
	// already withholds the value; this is just so the table doesn't carry an
	// empty Ovr header in a league that hides team ratings.
	//
	// With "Team Ratings Delay" on, the column stays: the worker fills it in for
	// every season old enough to be knowable and leaves the recent rows blank,
	// which is the point - you can see how good a team WAS.
	const showOvr =
		teamOvrDisplay({
			challengeNoRatings,
			hideTeamRatings,
			teamRatingsDelaySeasons,
			season,
		}).type !== "hidden";

	const cols = getCols([
		"#",
		"Team",
		"Season",
		"W",
		"L",
		...(otl ? ["OTL"] : []),
		...(ties ? ["T"] : []),
		...(usePts ? ["PTS", "PTS%"] : ["%"]),
		`stat:${isSport("basketball") ? "mov" : "diff"}`,
		...(showOvr ? ["Ovr"] : []),
		...extraCols.map((x) => x.colName),
		"Links",
	]);

	const rows = teamSeasons.map((ts) => {
		const teamRegionAndName = `${ts.region} ${ts.name}`;

		return {
			key: ts.rank,
			data: [
				ts.rank,
				{
					value: (
						<div className="d-flex align-items-center gap-1">
							<TeamLogoInline imgURL={ts.imgURL} imgURLSmall={ts.imgURLSmall} />
							<a
								href={helpers.leagueUrl([
									"roster",
									`${ts.abbrev}_${ts.tid}`,
									ts.season,
								])}
							>
								{teamRegionAndName}
							</a>
						</div>
					),
					searchValue: teamRegionAndName,
					sortValue: teamRegionAndName,
				},
				ts.season,
				ts.won,
				ts.lost,
				...(otl ? [ts.otl] : []),
				...(ties ? [ts.tied] : []),
				...(usePts
					? [Math.round(ts.standingsPts), helpers.roundWinp(ts.ptsPct)]
					: [helpers.roundWinp(ts.winp)]),
				wrappedMovOrDiff(
					isSport("basketball")
						? {
								pts: ts.pts * ts.gp,
								oppPts: ts.oppPts * ts.gp,
								gp: ts.gp,
							}
						: ts,
					isSport("basketball") ? "mov" : "diff",
				),
				...(showOvr ? [ts.ovr] : []),
				...extraCols.map((x) => {
					let value = getValue(ts, x.key);
					if (x.colName === "AvgAge") {
						value = value.toFixed(1);
					}

					if (x.keySort !== undefined) {
						const sortValue = getValue(ts, x.keySort);

						return {
							value,
							sortValue,
						};
					}
					return value;
				}),
				<>
					<a href={helpers.leagueUrl(["standings", ts.season])}>Standings</a> |{" "}
					<a href={helpers.leagueUrl(["playoffs", ts.season])}>Playoffs</a>
				</>,
			],
			classNames: {
				"table-info": ts.tid === userTid,
			},
		};
	});

	return (
		<>
			{description ? <p>{description}</p> : null}

			<DataTable
				cols={cols}
				defaultSort={[0, "asc"]}
				name={`FrivolitiesTeamSeasons_${type}`}
				nonfluid
				rows={rows}
			/>
		</>
	);
};

export default FrivolitiesTeamSeasons;
