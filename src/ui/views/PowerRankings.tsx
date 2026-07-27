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
import {
	gradeAgainst,
	summarizeTeamRatings,
} from "../../common/teamRatingGrade.ts";

const Other = ({
	actualShowHealthy,
	current,
	healthy,
	injuriesHurt,
	same,
}: {
	actualShowHealthy: boolean;
	current: ReactNode;
	healthy: ReactNode;
	// From the underlying ratings, not the rendered values - once these are
	// letter grades, comparing them would be a string comparison.
	injuriesHurt: boolean;
	same: boolean;
}) => {
	if (actualShowHealthy || same) {
		return <>{healthy}</>;
	}

	return (
		<span className={injuriesHurt ? "text-danger" : "text-success"}>
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
	const showTeamRatings = !challengeNoRatings && !hideTeamRatings;

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

	// One curve per category, computed across the whole league, so a grade means
	// "where this team sits in this league this season". Healthy and current are
	// curved separately - a league-wide injury wave shouldn't drag every grade
	// down when you're looking at healthy ratings.
	const curves: {
		current: Record<string, { mean: number; stdDev: number }>;
		healthy: Record<string, { mean: number; stdDev: number }>;
	} = { current: {}, healthy: {} };
	if (hideTeamRatings) {
		for (const key of otherKeys) {
			curves.current[key] = summarizeTeamRatings(
				teams.map((t) => t.powerRankings.otherCurrent[key]!),
			);
			curves.healthy[key] = summarizeTeamRatings(
				teams.map((t) => t.powerRankings.other[key]!),
			);
		}
	}

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
					const current = t.powerRankings.otherCurrent[key]!;
					const healthy = t.powerRankings.other[key]!;
					return {
						value: (
							<Other
								actualShowHealthy={actualShowHealthy}
								current={
									hideTeamRatings
										? gradeAgainst(current, curves.current[key]!)
										: coarseOvr(current)
								}
								healthy={
									hideTeamRatings
										? gradeAgainst(healthy, curves.healthy[key]!)
										: coarseOvr(healthy)
								}
								injuriesHurt={healthy > current}
								same={
									hideTeamRatings
										? gradeAgainst(current, curves.current[key]!) ===
											gradeAgainst(healthy, curves.healthy[key]!)
										: coarseOvr(current) === coarseOvr(healthy)
								}
							/>
						),
						// Sorting and searching stay on the underlying number, so a column
						// still orders the whole league rather than lumping every B
						// together. The number itself is never displayed.
						searchValue: hideTeamRatings
							? gradeAgainst(
									actualShowHealthy ? healthy : current,
									actualShowHealthy
										? curves.healthy[key]!
										: curves.current[key]!,
								)
							: actualShowHealthy
								? healthy
								: current,
						sortValue: actualShowHealthy ? healthy : current,
					};
				}),
			],
			classNames: {
				"table-info": t.tid === userTid,
			},
		};
	});

	return (
		<>
			<p>
				The power ranking is a combination of recent performance, margin of
				victory, and team rating. Team rating is based only on the ratings of
				players on each team.
				{hideTeamRatings
					? " Category grades are curved against the rest of the league this season."
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
