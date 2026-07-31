import { useState, type ReactNode } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { getCols } from "../../common/getCols.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import type { View } from "../../common/types.ts";
import { POSITIONS, RATINGS } from "../../common/constants.ts";
import { wrappedMovOrDiff } from "../components/MovOrDiff.tsx";
import { wrappedTeamLogoAndName } from "../components/TeamLogoAndName.tsx";
import { bySport, isSport } from "../../common/sportFunctions.ts";
import { useLocal } from "../util/local.ts";
import { gradeFromRank } from "../../common/teamRatingGrade.ts";
import { showTeamOvr } from "../../common/teamRatings.ts";

const Other = ({
	actualShowHealthy,
	current,
	currentIsBetter,
	healthy,
	same,
}: {
	actualShowHealthy: boolean;
	current: ReactNode;
	// Compared on the underlying RANKS, not on what is rendered - once these are
	// letter grades, comparing them would be a string comparison.
	currentIsBetter: boolean;
	healthy: ReactNode;
	same: boolean;
}) => {
	if (actualShowHealthy || same) {
		return <>{healthy}</>;
	}

	return (
		<span className={currentIsBetter ? "text-success" : "text-danger"}>
			{current}
		</span>
	);
};

const PowerRankings = ({
	confs,
	divs,
	playoffs,
	season,
	teams,
	ties,
	otl,
}: View<"powerRankings">) => {
	const dropdownFields = bySport({
		basketball: { seasons: season, playoffs },
		default: { seasons: season },
	}) as { seasons: number; playoffs: string } | { seasons: number };

	useTitleBar({
		title: "Power Rankings",
		dropdownView: "power_rankings",
		dropdownFields,
	});

	const {
		challengeNoRatings,
		hideRatingsOnesDigit,
		hideTeamRatings,
		season: currentSeason,
		userTid,
	} = useLocal([
		"challengeNoRatings",
		"hideRatingsOnesDigit",
		"hideTeamRatings",
		"season",
		"userTid",
	]);

	// Coarse ratings: floor team ratings to the tens digit for display.
	const coarseOvr = (value: number) =>
		hideRatingsOnesDigit ? Math.floor(value / 10) : value;

	// Team ratings can be hidden by the challenge setting, or by the broader
	// "no visible player ratings" one (a team rating is just its players').
	const showTeamRatings = showTeamOvr({ challengeNoRatings, hideTeamRatings });

	// With no games played there is nothing in a power ranking but the rosters,
	// so every number on the page - the rank, the category grades - is a
	// projection of how good each team is. In a league that hides team ratings
	// that's the whole secret, printed in order. It opens with the season.
	const noGamesYet = teams.every((t) => t.stats.gp === 0);
	const closed = !showTeamRatings && noGamesYet;

	const [showHealthy, setShowHealthy] = useState(true);
	const actualShowHealthy = showHealthy || currentSeason !== season;

	const [otherKeys, otherKeysTitle, otherKeysPrefix] = bySport({
		baseball: [
			POSITIONS.filter((pos) => pos !== "DH"),
			"Position Ranks",
			"pos",
		],
		basketball: [RATINGS, "Rating Ranks", "rating"],
		football: [
			POSITIONS.filter((pos) => pos !== "KR" && pos !== "PR"),
			"Position Ranks",
			"pos",
		],
		hockey: [POSITIONS, "Position Ranks", "pos"],
	});

	const superCols = [
		{
			title: "",
			colspan: 4,
		},
		{
			title: showTeamRatings ? "Team Rating" : "",
			colspan: showTeamRatings ? 2 : 0,
		},
		{
			title: "",
			colspan: 6 + (ties ? 1 : 0) + (otl ? 1 : 0),
		},
		{
			title: (
				<>
					{otherKeysTitle}
					{currentSeason === season ? (
						<a
							className="ms-2"
							href=""
							onClick={(event) => {
								event.preventDefault();
								setShowHealthy((val) => !val);
							}}
						>
							{showHealthy ? "(Show with injuries)" : "(Show without injuries)"}
						</a>
					) : null}
				</>
			),
			colspan: otherKeys.length,
		},
	];

	const colNames = [
		"#",
		"Team",
		"Conference",
		"Division",
		...(showTeamRatings ? ["Current", "Healthy"] : []),
		"W",
		"L",
		...(otl ? ["OTL"] : []),
		...(ties ? ["T"] : []),
		"L10",
		"ATS",
		`stat:${isSport("basketball") ? "mov" : "diff"}`,
		"AvgAge",
		...otherKeys.map((key) => `${otherKeysPrefix}:${key}`),
	];

	const cols = getCols(colNames);

	if (isSport("basketball")) {
		for (const [colName, col] of Iterator.zip([colNames, cols], {
			mode: "strict",
		})) {
			if (colName.startsWith("rating:")) {
				col.sortSequence = ["asc", "desc"];
			}
		}
	}

	const rows = teams.map((t) => {
		const conf = confs.find((conf) => conf.cid === t.seasonAttrs.cid);
		const div = divs.find((div) => div.did === t.seasonAttrs.did);

		return {
			key: t.tid,
			data: [
				t.powerRankings.rank,
				wrappedTeamLogoAndName(
					t,
					helpers.leagueUrl([
						"roster",
						`${t.seasonAttrs.abbrev}_${t.tid}`,
						season,
					]),
				),
				conf ? conf.name.replace(" Conference", "") : null,
				div ? div.name : null,
				...(showTeamRatings
					? [
							t.powerRankings.ovr !== t.powerRankings.ovrCurrent ? (
								<span className="text-danger">
									{coarseOvr(t.powerRankings.ovrCurrent)}
								</span>
							) : (
								coarseOvr(t.powerRankings.ovrCurrent)
							),
							coarseOvr(t.powerRankings.ovr),
						]
					: []),
				t.seasonAttrs.won,
				t.seasonAttrs.lost,
				...(otl ? [t.seasonAttrs.otl] : []),
				...(ties ? [t.seasonAttrs.tied] : []),
				t.seasonAttrs.lastTen,
				t.ats,
				wrappedMovOrDiff(
					isSport("basketball")
						? {
								pts: t.stats.pts * t.stats.gp,
								oppPts: t.stats.oppPts * t.stats.gp,
								gp: t.stats.gp,
							}
						: t.stats,
					isSport("basketball") ? "mov" : "diff",
				),
				t.powerRankings.avgAge?.toFixed(1),
				...otherKeys.map((key) => {
					// Already this team's RANK in the category across the league (1 is
					// best) - the view turns the real ratings into ranks before they get
					// here. A rank IS a percentile position, so grading off it can never
					// disagree with the order the column sorts by.
					const current = t.powerRankings.otherCurrent[key]!;
					const healthy = t.powerRankings.other[key]!;
					const render = (rank: number) =>
						hideTeamRatings ? gradeFromRank(rank, teams.length) : rank;
					return {
						value: (
							<Other
								actualShowHealthy={actualShowHealthy}
								current={render(current)}
								currentIsBetter={current < healthy}
								healthy={render(healthy)}
								same={render(current) === render(healthy)}
							/>
						),
						// Sorting stays on the rank, so a column still orders the whole
						// league rather than lumping every B together.
						searchValue: render(actualShowHealthy ? healthy : current),
						sortValue: actualShowHealthy ? healthy : current,
					};
				}),
			],
			classNames: {
				"table-info": t.tid === userTid,
			},
		};
	});

	if (closed) {
		return <p>Power rankings open once games have been played.</p>;
	}

	return (
		<>
			<p>
				The power ranking is a combination of recent performance, margin of
				victory, and team rating. Team rating is based only on the ratings of
				players on each team.
				{hideTeamRatings
					? " Category grades are each team's percentile rank in that category, in even fifths across the league."
					: ""}
			</p>
			{playoffs === "playoffs" && isSport("basketball") ? (
				<p>
					In the playoffs, rotations get shorter and players play harder, so
					some teams get higher or lower ratings.
				</p>
			) : null}

			<DataTable
				cols={cols}
				defaultSort={[0, "asc"]}
				defaultStickyCols={2}
				name="PowerRankings"
				nonfluid
				rows={rows}
				superCols={superCols}
			/>
		</>
	);
};

export default PowerRankings;
