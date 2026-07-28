import useTitleBar from "../hooks/useTitleBar.tsx";
import type { View } from "../../common/types.ts";
import TopStuff from "./Player/TopStuff.tsx";
import { helpers } from "../util/helpers.ts";
import { getCols } from "../../common/getCols.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import { NoGamesMessage } from "./GameLog.tsx";
import type { DataTableRow } from "../components/DataTable/index.tsx";
import { isSport } from "../../common/sportFunctions.ts";
import clsx from "clsx";
import { InjuryIcon } from "../components/InjuryIcon.tsx";
import { useLocal } from "../util/local.ts";
import { HighlightsButton } from "../components/HighlightsButton.tsx";
import {
	gameLogAveragesRow,
	useGameLogSelection,
} from "../util/gameLogAverages.tsx";

type DecisionPlayer = {
	w: number;
	l: number;
	sv: number;
	bs: number;
	hld: number;
	seasonStats: {
		w: number;
		l: number;
		sv: number;
		bs: number;
		hld: number;
	};
};

const baseballDecision = (p: DecisionPlayer) => {
	if (p.w > 0) {
		if (p.bs > 0) {
			return "BW";
		}

		return "W";
	}

	if (p.l > 0) {
		if (p.bs > 0) {
			return "BL";
		}
		if (p.hld > 0) {
			return "HL";
		}
		return "L";
	}

	if (p.sv > 0) {
		return "SV";
	}

	if (p.bs > 0) {
		return "BS";
	}

	if (p.hld > 0) {
		return "H";
	}
};

const baseballDecisionGames = (
	p: DecisionPlayer,
	decision: NonNullable<ReturnType<typeof baseballDecision>>,
) => {
	if (
		decision === "W" ||
		decision === "BW" ||
		decision === "L" ||
		decision === "BL" ||
		decision === "HL"
	) {
		return {
			count: p.seasonStats.w + p.seasonStats.l,
			formatted: helpers.formatRecord({
				won: p.seasonStats.w,
				lost: p.seasonStats.l,
			}),
		};
	} else if (decision === "SV") {
		return {
			count: p.seasonStats.sv,
			formatted: `${p.seasonStats.sv}`,
		};
	} else if (decision === "BS") {
		return {
			count: p.seasonStats.bs,
			formatted: `${p.seasonStats.bs}`,
		};
	} else if (decision === "H") {
		return {
			count: p.seasonStats.hld,
			formatted: `${p.seasonStats.hld}`,
		};
	} else {
		throw new Error("Should never happen");
	}
};

export const BaseballDecision = ({
	className,
	hideRecord,
	p,
	wlColors,
}: {
	className?: string;
	hideRecord: boolean; // Useful for ASG or exhibition
	p: DecisionPlayer;
	wlColors?: boolean;
}) => {
	const decision = baseballDecision(p);
	if (decision !== undefined) {
		const colorClassName = wlColors
			? decision === "W" || decision === "BW"
				? "text-success"
				: decision === "L" || decision === "BL" || decision === "HL"
					? "text-danger"
					: undefined
			: undefined;
		const { formatted } = baseballDecisionGames(p, decision);

		return (
			<span className={clsx(colorClassName, className)}>
				{decision}
				{hideRecord || formatted === undefined ? null : <> ({formatted})</>}
			</span>
		);
	}

	return null;
};

const wrappedBaseballDecision = (p: DecisionPlayer, hideRecord: boolean) => {
	let searchValue;
	let sortValue = ""; // Otherwise it doesn't work if undefined
	const decision = baseballDecision(p);
	if (decision !== undefined) {
		const { count, formatted } = baseballDecisionGames(p, decision);
		sortValue = `${decision}${count + 10000}`;
		searchValue = hideRecord ? decision : `${decision} (${formatted})`;
	}

	return {
		value: <BaseballDecision hideRecord={hideRecord} p={p} />,
		searchValue,
		sortValue,
	};
};

const PlayerGameLog = ({
	bestPos,
	customMenu,
	jerseyNumberInfos,
	noteTeammates,
	numGamesPlayoffSeires,
	player,
	randomDebutsForeverPids,
	retired,
	statSummary,
	teamColors,
	teamJersey,
	teamName,
	teamURL,
	willingToSign,
	gameLog,
	season,
	seasonsWithStats,
	showDecisionColumn,
	stats,
	superCols,
}: View<"playerGameLog">) => {
	useTitleBar({
		title: player.name,
		customMenu,
		dropdownView: "player_game_log",
		dropdownFields: {
			playerProfile: "gameLog",
			seasons: season,
		},
		dropdownCustomOptions: {
			seasons: seasonsWithStats.map((season) => ({
				key: season,
				value: String(season),
			})),
		},
		dropdownCustomURL: (fields) => {
			const parts =
				fields.playerProfile === "gameLog"
					? ["player_game_log", player.pid, fields.seasons]
					: ["player", player.pid];

			return helpers.leagueUrl(parts);
		},
	});

	const { challengeNoRatings, season: currentSeason } = useLocal([
		"challengeNoRatings",
		"season",
	]);
	const showRatings = !challengeNoRatings || retired;

	// Highlight game rows to see their per-game averages at the top of the table.
	// Regular season and playoffs each keep their own selection.
	const regularSeasonSelection = useGameLogSelection();
	const playoffsSelection = useGameLogSelection();

	const cols = getCols([
		"#",
		"Team",
		"@",
		"Opp",
		"Result",
		"Record",
		"",
		...(isSport("baseball") && showDecisionColumn ? ["Decision"] : []),
		...stats.map((stat) => `stat:${stat}`),
	]);

	const makeRow = (game: (typeof gameLog)[number], i: number): DataTableRow => {
		const allStarGame = game.tid === -1 || game.tid === -2;

		return {
			key: i,
			data: [
				i + 1,
				<>
					{game.seed !== undefined ? `${game.seed}. ` : null}
					<a
						href={helpers.leagueUrl([
							"roster",
							`${game.abbrev}_${game.tid}`,
							season,
						])}
					>
						{game.abbrev}
					</a>
				</>,
				game.away ? "@" : "",
				{
					value: (
						<>
							{game.oppSeed !== undefined ? `${game.oppSeed}. ` : null}
							<a
								href={helpers.leagueUrl([
									"roster",
									`${game.oppAbbrev}_${game.oppTid}`,
									season,
								])}
							>
								{game.oppAbbrev}
							</a>
						</>
					),
					sortValue: game.oppAbbrev,
					searchValue: game.oppAbbrev,
				},
				{
					value: (
						<>
							<a
								href={helpers.leagueUrl([
									"game_log",
									game.tid < 0 ? "special" : `${game.abbrev}_${game.tid}`,
									season,
									game.gid,
								])}
							>
								{game.result}
							</a>
							{isSport("basketball") && game.hasReplay ? (
								<HighlightsButton gid={game.gid} pid={player.pid} />
							) : null}
						</>
					),
					sortValue: game.diff,
					searchValue: game.result,
				},
				helpers.formatRecord(game),
				{
					value: <InjuryIcon className="ms-0" injury={game.injury} />,
					sortValue: game.injury.gamesRemaining,
					searchValue: game.injury.gamesRemaining,
					classNames: "text-center",
				},
				...(isSport("baseball") && showDecisionColumn
					? [wrappedBaseballDecision(game.stats as any, allStarGame)]
					: []),
				...stats.map((stat) =>
					game.stats[stat] === undefined
						? undefined
						: helpers.roundStat(game.stats[stat], stat, true),
				),
			],
		};
	};

	const regularSeasonGames = gameLog.filter((game) => !game.playoffs);
	const rowsRegularSeason = regularSeasonGames.map(makeRow);

	const playoffGames = gameLog.filter((game) => game.playoffs);
	const rowsPlayoffs = playoffGames.map(makeRow);

	// The averages row (top of the table) covers whichever games are highlighted;
	// row keys are the index into the corresponding filtered game list. Shown
	// only at 2+ selected, since one game's "average" is just its own line.
	const averagesLeadingRows = (
		selection: ReturnType<typeof useGameLogSelection>,
		games: typeof gameLog,
	) => {
		if (selection.selectedKeys.size < 2) {
			return undefined;
		}
		const selectedGames = [...selection.selectedKeys]
			.map((key) => games[key as number])
			.filter((game) => game !== undefined);
		return gameLogAveragesRow(
			selectedGames as any,
			stats,
			cols.length,
			selection.clear,
		);
	};

	// Add separators to playoff series when there is one more than a single game
	let striped;
	if (numGamesPlayoffSeires.some((numGames) => numGames > 1)) {
		striped = false;

		let prevOppTid;
		let oppTidCounter = -1;
		const classes = [
			"",
			"table-info",
			"table-primary",
			"table-success",
			"table-light",
			"table-danger",
			"table-warning",
			"table-secondary",
			"table-active",
		];
		for (const [i, game] of playoffGames.entries()) {
			if (game.oppTid !== prevOppTid) {
				prevOppTid = game.oppTid;
				oppTidCounter += 1;
			}

			rowsPlayoffs[i]!.classNames = classes[oppTidCounter % classes.length];
		}
	} else {
		striped = true;
	}

	let noGamesMessage;
	if (gameLog.length === 0) {
		noGamesMessage = (
			<NoGamesMessage warnAboutDelete={season < currentSeason} />
		);
	}

	return (
		<>
			<TopStuff
				bestPos={bestPos}
				currentSeason={currentSeason}
				jerseyNumberInfos={jerseyNumberInfos}
				noteTeammates={noteTeammates}
				player={player}
				randomDebutsForeverPids={randomDebutsForeverPids}
				retired={retired}
				season={season}
				showRatings={showRatings}
				statSummary={statSummary}
				teamColors={teamColors}
				teamJersey={teamJersey}
				teamName={teamName}
				teamURL={teamURL}
				willingToSign={willingToSign}
			/>

			{noGamesMessage ? (
				noGamesMessage
			) : (
				<>
					{rowsRegularSeason.length > 0 ? (
						<>
							<DataTable
								cols={cols}
								defaultSort={[0, "asc"]}
								leadingRows={averagesLeadingRows(
									regularSeasonSelection,
									regularSeasonGames,
								)}
								name="PlayerGameLog"
								rows={rowsRegularSeason}
								rowSelect={{
									selectedKeys: regularSeasonSelection.selectedKeys,
									onToggle: regularSeasonSelection.onToggle,
								}}
								superCols={superCols}
							/>
						</>
					) : null}
					{rowsPlayoffs.length > 0 ? (
						<>
							<DataTable
								className={rowsRegularSeason.length > 0 ? "mt-5" : undefined}
								cols={cols}
								defaultSort={[0, "asc"]}
								leadingRows={averagesLeadingRows(
									playoffsSelection,
									playoffGames,
								)}
								name="PlayerGameLogPlayoffs"
								rows={rowsPlayoffs}
								rowSelect={{
									selectedKeys: playoffsSelection.selectedKeys,
									onToggle: playoffsSelection.onToggle,
								}}
								striped={striped}
								superCols={superCols}
								title={<h2>Playoffs</h2>}
							/>
						</>
					) : null}
				</>
			)}
		</>
	);
};

export default PlayerGameLog;
