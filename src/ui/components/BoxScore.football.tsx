import {
	memo,
	Fragment,
	type MouseEvent,
	type ReactNode,
	useState,
} from "react";
import { ResponsiveTableWrapper } from "./ResponsiveTableWrapper.tsx";
import { getCols } from "../../common/getCols.ts";
import { helpers } from "../util/helpers.ts";
import { PLAYER_GAME_STATS } from "../../common/constants.football.ts";
import type { Col, SortBy } from "./DataTable/index.tsx";
import updateSortBys from "./DataTable/updateSortBys.ts";
import { getSortClassName } from "./DataTable/Header.tsx";
import {
	getScoreInfo,
	getScoreInfoOld,
	getText,
	type SportState,
} from "../util/processLiveGameEvents.football.tsx";
import type { PlayByPlayEventScore } from "../../worker/core/GameSim.football/PlayByPlayLogger.ts";
import { formatClock } from "../../common/formatClock.ts";
import { processPlayerStats } from "../util/processPlayerStats.ts";
import { getPeriodName } from "../../common/getPeriodName.ts";
import { filterPlayerStats } from "../../common/filterPlayerStats.ts";

type Team = {
	abbrev: string;
	colors: [string, string, string];
	imgURL: string;
	imgURLSmall: string | undefined;
	name: string;
	region: string;
	players: any[];
	season?: number;
	tid: number;
};

type BoxScore = {
	gid: number;
	season: number;
	scoringSummary: PlayByPlayEventScore[];
	teams: [Team, Team];
	numPeriods?: number;
	exhibition?: boolean;
	shootout?: boolean;
	neutralSite: boolean;
	won?: { name: string };
};

export const StatsHeader = ({
	cols,
	onClick,
	sortBys,
	sortable,
}: {
	cols: Col[];
	onClick: (b: MouseEvent, a: number) => void;
	sortBys: SortBy[];
	sortable: boolean;
}) => {
	return (
		<>
			{cols.map((col, i) => {
				const { desc, title } = col;

				let className: string | undefined;

				if (sortable) {
					className = getSortClassName(sortBys, i);
				}

				return (
					<th
						className={className}
						key={i}
						onClick={(event) => {
							onClick(event, i);
						}}
						title={desc}
					>
						{title}
					</th>
				);
			})}
		</>
	);
};

export const sortByStats = (
	stats: string[],
	seasonStats: string[] | undefined,
	sortBys: SortBy[],
	getValue?: (p: any, stat: string) => number,
) => {
	return (a: any, b: any) => {
		for (const [index, order] of sortBys) {
			let stat = stats[index];
			let statsObject = "processed";
			if (stat === undefined && seasonStats) {
				stat = seasonStats[index - stats.length];
				statsObject = "seasonStats";
			}

			const aValue = getValue?.(a, stat!) ?? a[statsObject][stat!];
			const bValue = getValue?.(b, stat!) ?? b[statsObject][stat!];

			if (bValue !== aValue) {
				const diff = bValue - aValue;
				if (order === "asc") {
					return -diff;
				}
				return diff;
			}
		}
		return 0;
	};
};

const StatsTableIndividual = ({
	Row,
	exhibition,
	season,
	t,
	type,
}: {
	Row: any;
	exhibition?: boolean;
	season: number;
	t: BoxScore["teams"][number];
	type: keyof typeof PLAYER_GAME_STATS;
}) => {
	const stats = PLAYER_GAME_STATS[type].stats;
	const cols = getCols(stats.map((stat) => `stat:${stat}`));

	const [sortBys, setSortBys] = useState(() => {
		return PLAYER_GAME_STATS[type].sortBy.map(
			(stat) => [stats.indexOf(stat), "desc"] as SortBy,
		);
	});

	const onClick = (event: MouseEvent, i: number) => {
		setSortBys(
			(prevSortBys) =>
				updateSortBys({
					cols,
					event,
					i,
					prevSortBys,
				}) ?? [],
		);
	};

	const allStarGame = t.tid === -1 || t.tid === -2;
	const players = t.players
		.map((p) => {
			return {
				...p,
				processed: processPlayerStats(p, stats),
			};
		})
		.filter((p) => filterPlayerStats(p, stats, type))
		.sort(sortByStats(stats, undefined, sortBys));

	const sortable = players.length > 1;
	const highlightCols = sortable
		? sortBys.map((sortBy) => sortBy[0])
		: undefined;

	return (
		<div className="mb-3">
			<ResponsiveTableWrapper>
				<table className="table table-striped table-borderless table-sm table-hover">
					<thead>
						<tr>
							<th colSpan={2}>
								{t.season !== undefined ? `${t.season} ` : null}
								{t.region} {t.name}
							</th>
							<StatsHeader
								cols={cols}
								onClick={onClick}
								sortBys={sortBys}
								sortable={sortable}
							/>
						</tr>
					</thead>
					<tbody>
						{players.map((p, i) => (
							<Row
								allStarGame={allStarGame}
								key={p.pid}
								exhibition={exhibition}
								i={i}
								p={p}
								stats={stats}
								highlightCols={highlightCols}
								season={season}
							/>
						))}
					</tbody>
				</table>
			</ResponsiveTableWrapper>
		</div>
	);
};

const StatsTable = ({
	Row,
	boxScore,
	type,
}: {
	Row: any;
	boxScore: BoxScore;
	type: keyof typeof PLAYER_GAME_STATS;
}) => {
	return (
		<>
			{boxScore.teams.map((t, i) => (
				<StatsTableIndividual
					key={i}
					Row={Row}
					exhibition={boxScore.exhibition}
					season={boxScore.season}
					t={t}
					type={type}
				/>
			))}
		</>
	);
};

const MissSymbol = () => <span className="text-danger">✕ </span>;

// Condenses TD + XP/2P into one event rather than two, and normalizes scoring summary events into consistent format (old style format had the text in it already, new one is just raw metadata from game sim)
const processEvents = (events: PlayByPlayEventScore[], numPeriods: number) => {
	const processedEvents: {
		quarter: string;
		noPoints: boolean;
		score: [number, number];
		scoreType: string | null;
		t: 0 | 1;
		text: ReactNode;
		time: string;
	}[] = [];
	let score: [number, number] = [0, 0];
	let shootout = false;

	for (const event of events) {
		let text: ReactNode | undefined;

		const oldEvent = event as any;
		const isOldFormat = oldEvent.text !== undefined;
		if (isOldFormat) {
			// This is an old format entry, with the text already generated!
			text = oldEvent.text;
		} else {
			// This is a new format entry, with metadata that needs to be turned into text
			text = getText(event, numPeriods);
		}

		if (text === undefined) {
			continue;
		}

		if (!shootout && event.type === "shootoutShot") {
			shootout = true;
			score = [0, 0];
		}

		const otherT = event.t === 0 ? 1 : 0;

		const scoreInfo = isOldFormat
			? getScoreInfoOld(oldEvent.text)
			: getScoreInfo(event);
		if (scoreInfo) {
			const ptsKey = shootout ? "sPts" : "points";
			const pts = scoreInfo[ptsKey] ?? 0;
			if (scoreInfo.type === "SF") {
				// Safety is recorded as part of a play by the team with the ball, so for scoring purposes we need to swap the teams here and below
				score[otherT] += pts;
			} else {
				score[event.t] += pts;
			}

			const prevEvent = processedEvents.at(-1);

			if (
				prevEvent &&
				(scoreInfo.type === "XP" ||
					(scoreInfo.type === "2P" && event.t === prevEvent.t))
			) {
				prevEvent.score = [score[0], score[1]];
				prevEvent.text = (
					<>
						{prevEvent.text}
						<br />
						<span className="text-body-secondary">
							{pts === 0 ? <MissSymbol /> : null}
							{text}
						</span>
					</>
				);
			} else {
				processedEvents.push({
					t: scoreInfo.type === "SF" ? otherT : event.t, // See comment above about safety teams
					quarter: isOldFormat
						? oldEvent.quarter
						: event.quarter <= numPeriods
							? `Q${event.quarter}`
							: `OT${event.quarter - numPeriods}`,
					noPoints: pts === 0,
					time: isOldFormat ? oldEvent.time : formatClock(event.clock),
					text,
					score: helpers.deepCopy(score),
					scoreType: scoreInfo.type,
				});
			}
		}
	}

	return processedEvents;
};

const getCount = (events: PlayByPlayEventScore[]) => {
	return events.length;
};

const ScoringSummary = memo(
	({
		events,
		numPeriods,
		teams,
	}: {
		count: number;
		events: PlayByPlayEventScore[];
		numPeriods: number;
		teams: [Team, Team];
	}) => {
		let prevQuarter: string;

		const processedEvents = processEvents(events, numPeriods);

		if (processedEvents.length === 0) {
			return <p>None</p>;
		}

		return (
			<table className="table table-sm border-bottom align-top-all">
				<tbody>
					{processedEvents.map((event, i) => {
						let quarterHeader: ReactNode = null;
						const currentQuarter =
							event.scoreType === "SH" ? "SH" : event.quarter;
						if (currentQuarter !== prevQuarter) {
							prevQuarter = currentQuarter;

							let quarterText = "???";
							if (currentQuarter.startsWith("OT")) {
								const overtimes = Number.parseInt(
									currentQuarter.replace("OT", ""),
								);
								if (overtimes > 1) {
									quarterText = `${helpers.ordinal(overtimes)} overtime`;
								} else {
									quarterText = "Overtime";
								}
							} else if (currentQuarter === "SH") {
								quarterText = "Shootout";
							} else {
								const quarter = Number.parseInt(
									currentQuarter.replace("Q", ""),
								);
								if (!Number.isNaN(quarter)) {
									quarterText = `${helpers.ordinal(quarter)} ${getPeriodName(
										numPeriods,
									)}`;
								}
							}

							quarterHeader = (
								<tr>
									<td className="text-body-secondary" colSpan={5}>
										{quarterText}
									</td>
								</tr>
							);
						}

						return (
							<Fragment key={i}>
								{quarterHeader}
								<tr>
									<td>{teams[event.t].abbrev}</td>
									<td>{event.scoreType === "SH" ? "FG" : event.scoreType}</td>
									<td>
										{event.score.map((pts, i) => {
											return (
												<Fragment key={i}>
													<span
														className={
															!event.noPoints && event.t === i
																? "fw-bold"
																: event.noPoints && event.t === i
																	? "text-danger"
																	: "text-body-secondary"
														}
													>
														{pts}
													</span>
													{i === 0 ? "-" : null}
												</Fragment>
											);
										})}
									</td>
									<td>{currentQuarter !== "SH" ? event.time : null}</td>
									<td style={{ whiteSpace: "normal" }}>
										{event.noPoints ? <MissSymbol /> : null}
										{event.text}
									</td>
								</tr>
							</Fragment>
						);
					})}
				</tbody>
			</table>
		);
	},
	(prevProps, nextProps) => {
		return prevProps.count === nextProps.count;
	},
);

const BoxScore = ({
	boxScore,
	Row,
}: {
	boxScore: BoxScore;
	// Still accepted (BoxScoreWrapper passes it for every sport), but the live
	// field owns the down, the drive and the play text now, so the box score
	// itself no longer reads it.
	sportState?: SportState;
	Row: any;
}) => {
	return (
		<div className="mb-3">
			<h2>Scoring Summary</h2>
			<ScoringSummary
				key={boxScore.gid}
				count={getCount(boxScore.scoringSummary)}
				events={boxScore.scoringSummary}
				numPeriods={boxScore.numPeriods ?? 4}
				teams={boxScore.teams}
			/>
			{helpers.keys(PLAYER_GAME_STATS).map((type) => {
				const info = PLAYER_GAME_STATS[type];
				return (
					<Fragment key={type}>
						<h2>{info.name}</h2>
						<StatsTable Row={Row} boxScore={boxScore} type={type} />
					</Fragment>
				);
			})}
		</div>
	);
};

export default BoxScore;
