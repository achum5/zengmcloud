import { useState } from "react";
import type { View } from "../../../common/types.ts";
import { helpers } from "../../util/helpers.ts";
import { getCols } from "../../../common/getCols.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import { highlightLeaderText, MaybeBold, SeasonLink } from "./common.tsx";
import { expandFieldingStats } from "../../util/expandFieldingStats.baseball.ts";
import { formatStatGameHigh } from "../PlayerStats.tsx";
import { SeasonIcons } from "../../components/SeasonIcons.tsx";
import HideableSection from "../../components/HideableSection.tsx";
import { DataTable } from "../../components/DataTable/index.tsx";
import clsx from "clsx";
import { useSelectedSeasonsFooter } from "./useSelectedSeasonsFooter.ts";
import type { FooterRow } from "../../components/DataTable/Footer.tsx";
import { wrappedTeamAbbrevLink } from "../../components/TeamAbbrevLink.tsx";
import type { SuperCol } from "../../components/DataTable/index.tsx";
import { formatSeasonRuns } from "../../util/formatSeasonRuns.ts";
import type { PlayerTeamStats } from "./usePlayerTeamStats.ts";

const hasStats = (
	careerStats: View<"player">["player"]["careerStats"],
	onlyShowIf: string[] | undefined,
) => {
	// For careerStatPlayoffs gp is undefined if there are no stats rows, ugh
	if (careerStats.gp === 0 || careerStats.gp === undefined) {
		return false;
	}

	if (onlyShowIf !== undefined) {
		for (const stat of onlyShowIf) {
			if (
				careerStats[stat]! > 0 ||
				(Array.isArray(careerStats[stat]) &&
					(careerStats[stat] as any).length > 0)
			) {
				return true;
			}
		}

		return false;
	}

	return true;
};

export const StatsTable = ({
	name,
	onlyShowIf,
	p,
	stats,
	superCols,
	leaders,
	teamStats,
}: {
	name: string;
	onlyShowIf?: string[];
	p: View<"player">["player"];
	stats: string[];
	superCols?: SuperCol[];
	leaders: View<"player">["leaders"];
	teamStats?: PlayerTeamStats;
}) => {
	const hasRegularSeasonStats = hasStats(p.careerStats, onlyShowIf);
	const hasPlayoffStats = hasStats(p.careerStatsPlayoffs, onlyShowIf);

	// Show playoffs by default if that's all we have
	const [playoffs, setPlayoffs] = useState<boolean | "combined">(
		!hasRegularSeasonStats,
	);

	// If game sim means we switch from having no stats to having some stats, make sure we're showing what we have
	if (hasRegularSeasonStats && !hasPlayoffStats && playoffs === true) {
		setPlayoffs(false);
	}
	if (!hasRegularSeasonStats && hasPlayoffStats && playoffs === false) {
		setPlayoffs(true);
	}

	let playerStats = p.stats.filter((ps) => ps.playoffs === playoffs);

	const selection = useSelectedSeasonsFooter(p.pid);

	if (!hasRegularSeasonStats && !hasPlayoffStats) {
		return null;
	}

	const careerStats =
		playoffs === "combined"
			? p.careerStatsCombined
			: playoffs
				? p.careerStatsPlayoffs
				: p.careerStats;

	const cols = getCols([
		"Year",
		"Team",
		"Age",
		...stats.map((stat) =>
			stat === "pos"
				? "Pos"
				: `stat:${stat.endsWith("Max") ? stat.replace("Max", "") : stat}`,
		),
	]);

	if (superCols) {
		superCols = helpers.deepCopy(superCols);

		// No name
		if (superCols[0]) {
			superCols[0].colspan -= 1;
		}
	}

	if (isSport("basketball") && name === "Shot Locations") {
		cols.at(-3)!.title = "M";
		cols.at(-2)!.title = "A";
		cols.at(-1)!.title = "%";
	}

	const isBaseballFielding = isSport("baseball") && name === "Fielding";

	let footer: FooterRow[];
	if (isBaseballFielding) {
		playerStats = expandFieldingStats({
			rows: playerStats,
			stats,
		});

		footer = expandFieldingStats({
			rows: [careerStats],
			stats,
			addDummyPosIndex: true,
		}).map((object, i) => ({
			data: [
				i === 0 ? "Career" : null,
				null,
				null,
				...stats.map((stat) => formatStatGameHigh(object, stat)),
			],
		}));
	} else {
		footer = [
			{
				data: [
					"Career",
					null,
					null,
					...stats.map((stat) => formatStatGameHigh(careerStats, stat)),
				],
			},
		];

		// Per-team career totals (like the team rows basketball-reference shows
		// under a career line). Only when the player suited up for more than one
		// team; each row aggregates that team's seasons in the current
		// regular-season / playoffs / combined mode. Ordered oldest-team-first.
		if (teamStats && teamStats.length > 1) {
			const teamRows = teamStats
				.map((entry) => {
					const rowsForTeam = playerStats.filter((ps) => ps.tid === entry.tid);
					if (rowsForTeam.length === 0) {
						// This team has no rows in the current mode (e.g. never reached the
						// playoffs while on it), so skip its subtotal here.
						return undefined;
					}
					const seasons = rowsForTeam.map((ps) => ps.season);
					const lastRow = rowsForTeam.at(-1)!;
					const teamCareerStats =
						playoffs === "combined"
							? entry.careerStatsCombined
							: playoffs
								? entry.careerStatsPlayoffs
								: entry.careerStats;
					return {
						firstSeason: Math.min(...seasons),
						seasons,
						abbrev: lastRow.abbrev,
						tid: entry.tid,
						lastSeason: Math.max(...seasons),
						teamCareerStats,
					};
				})
				.filter((row) => row !== undefined)
				.sort((a, b) => a.firstSeason - b.firstSeason);

			for (const row of teamRows) {
				const runs = formatSeasonRuns(row.seasons);
				footer.push({
					classNames: "text-body-secondary",
					data: [
						runs.single ? runs.short : { value: <span title={runs.full}>{runs.short}</span> },
						wrappedTeamAbbrevLink({
							abbrev: row.abbrev,
							season: row.lastSeason,
							tid: row.tid,
						}),
						null,
						...stats.map((stat) =>
							formatStatGameHigh(row.teamCareerStats, stat),
						),
					],
				});
			}
		}

		// Selected-rows subtotal: when the user has checked 2+ season rows, sum
		// exactly those seasons (see the checkboxes added to each row below). One
		// checked row is redundant with its own line, so we only show it at 2+.
		if (selection.selected.size >= 2) {
			const selectedSeasons = [...selection.selected];
			const runs = formatSeasonRuns(selectedSeasons);
			const label = (
				<div className="d-flex align-items-center gap-1">
					<button
						type="button"
						className="btn-close btn-close-sm"
						style={{ fontSize: "0.6rem" }}
						title="Clear selection"
						onClick={selection.clear}
					/>
					<span title={runs.single ? undefined : runs.full}>
						{runs.short}
						{selection.status === "error" ? (
							<span className="glyphicon glyphicon-exclamation-sign text-danger ms-1" />
						) : null}
					</span>
				</div>
			);

			const selectedStats = selection.data
				? playoffs === "combined"
					? selection.data.careerStatsCombined
					: playoffs
						? selection.data.careerStatsPlayoffs
						: selection.data.careerStats
				: undefined;

			footer.push({
				classNames: clsx("table-primary", {
					"text-body-secondary": selection.status === "loading" || !selectedStats,
				}),
				data: [
					{ value: label },
					null,
					null,
					...stats.map((stat) =>
						selectedStats ? formatStatGameHigh(selectedStats, stat) : null,
					),
				],
			});
		}
	}

	const leadersType =
		playoffs === "combined"
			? "combined"
			: playoffs === true
				? "playoffs"
				: "regularSeason";

	let hasLeader = false;
	if (leadersType) {
		LEADERS_LOOP: for (const row of Object.values(leaders)) {
			if (row?.attrs.has("age")) {
				hasLeader = true;
				break;
			}

			for (const stat of stats) {
				if (row?.[leadersType].has(stat)) {
					hasLeader = true;
					break LEADERS_LOOP;
				}
			}
		}
	}

	// Let the user click season rows (the normal row highlight) to subtotal them
	// (see the footer above). Pointless with a single season, and n/a for the
	// baseball fielding table (multiple position rows per season).
	const selectableSeasons = new Set(playerStats.map((ps) => ps.season));
	const showSelection = !isBaseballFielding && selectableSeasons.size >= 2;

	// Which season each selectable row belongs to, so a row highlight toggles
	// that season (and every row of a selected season shows highlighted).
	const rowKeyToSeason = new Map<number | string, number>();

	const rows = [];

	let prevSeason;
	for (const [i, ps] of playerStats.entries()) {
		// Add blank rows for gap years if necessary
		if (prevSeason !== undefined && prevSeason < ps.season - 1) {
			const gapSeason = prevSeason + 1;

			rows.push({
				key: `gap-${gapSeason}`,
				data: [
					{
						searchValue: gapSeason,

						// i is used to index other sorts, so we need to fit in between
						sortValue: i - 0.5,

						value: null,
					},
					null,
					null,
					...stats.map(() => null),
				],
				classNames: "table-secondary",
			});
		}

		prevSeason = ps.season;

		const className = ps.hasTot ? "text-body-secondary" : undefined;
		if (showSelection) {
			rowKeyToSeason.set(i, ps.season);
		}

		rows.push({
			key: i,
			data: [
				{
					searchValue: ps.season,
					sortValue: i,
					value: (
						<>
							<SeasonLink
								className={className}
								pid={p.pid}
								season={ps.season}
							/>{" "}
							<SeasonIcons
								season={ps.season}
								awards={p.awards}
								playoffs={playoffs === true}
							/>
						</>
					),
				},
				wrappedTeamAbbrevLink({
					abbrev: ps.abbrev,
					className,
					season: ps.season,
					tid: ps.tid,
				}),
				<MaybeBold bold={leaders[ps.season]?.attrs.has("age")}>
					{ps.age}
				</MaybeBold>,
				...stats.map((stat) => (
					<MaybeBold
						bold={!ps.hasTot && leaders[ps.season]?.[leadersType].has(stat)}
					>
						{formatStatGameHigh(ps, stat)}
					</MaybeBold>
				)),
			],
			classNames: className,
		});
	}

	// Row keys whose season is currently selected, so those rows show the
	// highlight; clicking a row toggles its whole season.
	const selectedRowKeys = new Set<number | string>();
	if (showSelection) {
		for (const [key, season] of rowKeyToSeason) {
			if (selection.selected.has(season)) {
				selectedRowKeys.add(key);
			}
		}
	}
	const rowSelect = showSelection
		? {
				selectedKeys: selectedRowKeys,
				onToggle: (key: number | string) => {
					const season = rowKeyToSeason.get(key);
					if (season !== undefined) {
						selection.toggle(season);
					}
				},
			}
		: undefined;

	return (
		<HideableSection
			title={name}
			description={hasLeader ? highlightLeaderText : null}
		>
			<DataTable
				classNameWrapper="mb-3"
				cols={cols}
				defaultSort={[0, "asc"]}
				defaultStickyCols={2}
				footer={footer}
				hideAllControls
				name={`Player:${name}`}
				rows={rows}
				rowSelect={rowSelect}
				superCols={superCols}
				title={
					<ul className="nav nav-tabs border-bottom-0">
						{hasRegularSeasonStats ? (
							<li className="nav-item">
								<button
									className={clsx("nav-link", {
										active: playoffs === false,
										"border-bottom": playoffs === false,
									})}
									onClick={() => {
										setPlayoffs(false);
									}}
								>
									Regular Season
								</button>
							</li>
						) : null}
						{hasPlayoffStats ? (
							<li className="nav-item">
								<button
									className={clsx("nav-link", {
										active: playoffs === true,
										"border-bottom": playoffs === true,
									})}
									onClick={() => {
										setPlayoffs(true);
									}}
								>
									Playoffs
								</button>
							</li>
						) : null}
						{hasRegularSeasonStats && hasPlayoffStats ? (
							<li className="nav-item">
								<button
									className={clsx("nav-link", {
										active: playoffs === "combined",
										"border-bottom": playoffs === "combined",
									})}
									onClick={() => {
										setPlayoffs("combined");
									}}
								>
									Combined
								</button>
							</li>
						) : null}
					</ul>
				}
			/>
		</HideableSection>
	);
};
