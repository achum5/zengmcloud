// A FINISHED GAME'S PAGE, laid out as a game recap: the score across the
// top with the score by quarter, tabs under it - Recap (the leaders and the
// team stats; the story of the game and how it went; the line, the season
// series and the standings), the box score, the team stats and the game
// flow. Basketball, on a device that has the tabbed layout turned on (the
// same switch as the live game's - see local.gamecastLayout).

import clsx from "clsx";
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { helpers } from "../../../util/helpers.ts";
import { toWorker } from "../../../util/toWorker.ts";
import { realtimeUpdate } from "../../../util/realtimeUpdate.ts";
import { useLocal } from "../../../util/local.ts";
import { linkifyRecap, buildRecapLinks } from "../../../util/linkifyRecap.ts";
import { Markdown } from "../../../components/Markdown.tsx";
import { BoxScore } from "../../../components/BoxScore.tsx";
import {
	GameCharts,
	sideColors,
} from "../../../components/GameCharts.basketball.tsx";
import {
	FourFactors,
	NextButton,
	PlayoffRecord,
	WatchReplayButton,
} from "../../../components/BoxScoreWrapper.tsx";
import Note from "../../Player/Note.tsx";
import { teamsInDisplayOrder } from "../../../util/boxScoreDisplayOrder.ts";
import { roundHalf } from "../../../../common/getGameSpread.ts";
import { formatGameDate } from "../../../../common/calendarDates.ts";
import type { GamecastInfo } from "../../../../worker/views/liveGameInfo.ts";
import {
	Leaders,
	AllPlays,
	FeedContext,
	Linescore,
	Logo,
	LinkButton,
	Switch,
	RecentGames,
	Section,
	Series,
	Standings,
	Bracket,
	TeamStatBars,
} from "./Gamecast.tsx";
import { FlowCharts } from "./FlowCharts.tsx";
import { feedFromEvents, periodBoxScore } from "./replayFeed.ts";
import type { BoxPeriods } from "../../../components/PeriodPicker.tsx";
import {
	feedRows,
	periodLabel,
	type FeedEntry,
	type FeedRow,
} from "./playFeed.ts";
import { playSentence } from "./sentence.ts";
import { ShotChart } from "./ShotChart.tsx";
import { HighlightClip } from "./HighlightClip.tsx";
import { unpackShots, type ShotSpot } from "./shots.ts";
import { GAME_S, winProb } from "../winProb.ts";
import { seriesLine } from "./seriesLine.ts";
import { MatchupBanner, matchupVars } from "../../../components/TeamBanner.tsx";
import { NationalTag } from "../../../components/NationalTag.tsx";
import {
	DOCK_ICONS,
	foldSections,
	RecapSections,
	type DockSection,
} from "./RecapDock.tsx";
import { BoxScorePhone, useIsPhone } from "./BoxScorePhone.tsx";
import { Lineups, OnOff, Rotations, WithStints } from "./LineupViews.tsx";

type Tab = "recap" | "box" | "plays" | "stats" | "flow";

const TABS: [Tab, string][] = [
	["recap", "Recap"],
	["box", "Box Score"],
	["plays", "Play-by-Play"],
	["stats", "Team Stats"],
	["flow", "Game Flow"],
];

const teamUrl = (t: any, season: number) =>
	helpers.leagueUrl(["roster", `${t.abbrev}_${t.tid}`, season]);

// THE SCORE, PINNED: once the game's banner scrolls up under the top bar, a
// slim bar of it held there - each side in its colors, the score, Final.
// (Its class written out whole - see MatchupBanner.)
const ScoreBarSide = ({
	t,
	season,
	shown,
	side,
	children,
}: {
	t: any;
	season: number;
	shown: boolean;
	side: 0 | 1;
	children: ReactNode;
}) => {
	const className = side === 0 ? "rsb-half rsb-0" : "rsb-half rsb-1";
	return t.tid >= 0 ? (
		<a
			className={className}
			href={teamUrl(t, season)}
			tabIndex={shown ? undefined : -1}
		>
			{children}
		</a>
	) : (
		<div className={className}>{children}</div>
	);
};

const ScoreBar = ({ boxScore }: { boxScore: any }) => {
	const [shown, setShown] = useState(false);
	useEffect(() => {
		const el = document.querySelector(".recap-header .mb-banner");
		if (!el || typeof IntersectionObserver === "undefined") {
			return;
		}
		const observer = new IntersectionObserver(
			([e]) => {
				setShown(!!e && !e.isIntersecting && e.boundingClientRect.top < 0);
			},
			{ rootMargin: "-60px 0px 0px 0px" },
		);
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, [boxScore.gid]);
	const [away, home] = teamsInDisplayOrder(boxScore.teams as [any, any]);
	const won = away.pts > home.pts ? 0 : home.pts > away.pts ? 1 : undefined;
	return (
		<div className="recap-scorebar-pin">
			<div
				className={
					shown ? "recap-scorebar recap-scorebar-on" : "recap-scorebar"
				}
				style={matchupVars(away.colors, home.colors)}
				aria-hidden={!shown}
			>
				{/* (Each side to its team.) */}
				<ScoreBarSide t={away} season={boxScore.season} shown={shown} side={0}>
					<Logo t={away} size={26} />
					<span className="rsb-abbrev">{away.abbrev}</span>
					<span className={won === 1 ? "rsb-pts rsb-lost" : "rsb-pts"}>
						{away.pts}
					</span>
				</ScoreBarSide>
				<div className="rsb-mid">{finalText(boxScore)}</div>
				<ScoreBarSide t={home} season={boxScore.season} shown={shown} side={1}>
					<span className={won === 0 ? "rsb-pts rsb-lost" : "rsb-pts"}>
						{home.pts}
					</span>
					<span className="rsb-abbrev">{home.abbrev}</span>
					<Logo t={home} size={26} />
				</ScoreBarSide>
			</div>
		</div>
	);
};

// "Final", "Final/OT", "Final/2OT".
const finalText = (boxScore: any) => {
	const ot = boxScore.overtimes ?? 0;
	return ot === 0 ? "Final" : ot === 1 ? "Final/OT" : `Final/${ot}OT`;
};

// One side of the score across the top: its logo and its name (both to the
// team), its record, its score - the winner's dark, the loser's grey, as a
// scoreboard has them.
const HeaderTeam = ({
	t,
	season,
	won,
	right,
}: {
	t: any;
	season: number;
	won: boolean;
	right?: boolean;
}) => {
	const real = t.tid >= 0;
	const link = (children: ReactNode, className?: string) =>
		real ? (
			<a href={teamUrl(t, season)} className={className}>
				{children}
			</a>
		) : (
			<span className={className}>{children}</span>
		);
	return (
		<div className={clsx("recap-team", { "recap-team-right": right })}>
			{link(<Logo t={t} size={64} />, "recap-team-logo")}
			<div className="recap-team-name">
				{link(
					<>
						{t.region} {t.name}
					</>,
				)}
				{t.won !== undefined && t.lost !== undefined ? (
					<div className="recap-record">{helpers.formatRecord(t)}</div>
				) : null}
			</div>
			<div className={clsx("recap-score", { won })}>{t.pts}</div>
		</div>
	);
};

// The score by quarter, small, without the totals (the big scores are
// either side of it): for the middle of a phone's score line.
const MiniLinescore = ({ boxScore }: { boxScore: any }) => {
	const regulation = boxScore.numPeriods ?? 4;
	const n = Math.max(
		boxScore.teams[0].ptsQtrs?.length ?? 0,
		boxScore.teams[1].ptsQtrs?.length ?? 0,
		regulation,
	);
	const labels = Array.from({ length: n }, (_, i) =>
		i < regulation
			? `Q${i + 1}`
			: i === regulation
				? "OT"
				: `${i - regulation + 1}OT`,
	);
	return (
		<table className="recap-mini">
			<thead>
				<tr>
					<th />
					{labels.map((l) => (
						<th key={l}>{l}</th>
					))}
				</tr>
			</thead>
			<tbody>
				{teamsInDisplayOrder(boxScore.teams as [any, any]).map((t) => (
					<tr key={t.tid}>
						<th>{t.abbrev}</th>
						{labels.map((l, i) => (
							<td key={l}>{t.ptsQtrs?.[i] ?? "-"}</td>
						))}
					</tr>
				))}
			</tbody>
		</table>
	);
};

// A phone's side of it: logo, then the abbreviation over the score over the
// record - all on the one line with the score by quarter between them.
const PhoneTeam = ({
	t,
	season,
	won,
	right,
}: {
	t: any;
	season: number;
	won: boolean;
	right?: boolean;
}) => {
	const body = (
		<>
			<Logo t={t} size={34} />
			<div className="recap-phone-text">
				<div className="fw-bold lh-1">{t.abbrev}</div>
				<div className={clsx("recap-score", { won })}>{t.pts}</div>
				{t.won !== undefined && t.lost !== undefined ? (
					<div className="recap-record lh-1">{helpers.formatRecord(t)}</div>
				) : null}
			</div>
		</>
	);
	return t.tid >= 0 ? (
		<a
			href={teamUrl(t, season)}
			className={clsx("recap-phone-team", { "recap-team-right": right })}
		>
			{body}
		</a>
	) : (
		<div className={clsx("recap-phone-team", { "recap-team-right": right })}>
			{body}
		</div>
	);
};

type NavProps = {
	boxScore: any;
	abbrev?: string;
	tid?: number;
	prevGid?: number;
	nextGid?: number;
	currentGidInList?: boolean;
};

// To the team's game before and after this one, and the replay: under the
// score (the refreshed look: each team's arrows in the banner instead).
const RecapNav = ({
	boxScore,
	abbrev,
	tid,
	prevGid,
	nextGid,
	currentGidInList,
}: NavProps) => {
	const prevUrl = helpers.leagueUrl([
		"game_log",
		abbrev === "special" ? abbrev : `${abbrev}_${tid}`,
		boxScore.season,
		prevGid,
	]);
	const replay =
		!boxScore.exhibition && boxScore.gid >= 0 ? (
			<WatchReplayButton gid={boxScore.gid} />
		) : null;
	const next = (
		<NextButton
			abbrev={abbrev}
			boxScore={boxScore}
			currentGidInList={currentGidInList}
			nextGid={nextGid}
			tid={tid}
		/>
	);
	return (
		<div className="recap-nav">
			<a
				className={clsx("btn btn-light-bordered btn-sm", {
					disabled: prevGid === undefined,
				})}
				href={prevUrl}
				data-no-scroll-reset
			>
				Prev
			</a>
			{replay}
			<div className="recap-next">{next}</div>
		</div>
	);
};

// A TEAM'S LEADERS IN THE GAME, small under its record in the banner: who
// had the most points, rebounds and assists ("PTS T. Jones 33"), each to his
// page.
const LEADER_STATS = [
	["pts", "PTS"],
	["trb", "REB"],
	["ast", "AST"],
] as const;
export const TeamLeaders = ({ t }: { t: any }) => {
	const players = (t.players ?? []) as any[];
	// (Rebounds kept as offensive and defensive.)
	const stat = (p: any, key: string) =>
		key === "trb" ? (p.trb ?? (p.orb ?? 0) + (p.drb ?? 0)) : (p[key] ?? 0);
	const lines = LEADER_STATS.map(([key, label]) => {
		const top = players.reduce<any>(
			(best, p) =>
				best === undefined || stat(p, key) > stat(best, key) ? p : best,
			undefined,
		);
		if (!top || !(stat(top, key) > 0)) {
			return null;
		}
		const words = String(top.name).trim().split(/\s+/);
		const short =
			words.length > 1
				? `${words[0]![0]}. ${words.slice(1).join(" ")}`
				: top.name;
		return (
			<div key={key} className="mb-lead">
				<span className="mb-lead-k">{label}</span>
				<a href={helpers.leagueUrl(["player", top.pid])}>{short}</a>
				<span className="mb-lead-v">{stat(top, key)}</span>
			</div>
		);
	}).filter(Boolean);
	return lines.length > 0 ? <>{lines}</> : null;
};

// A TEAM'S GAMES BEFORE AND AFTER THIS ONE, as two arrows at the foot of its
// half of the banner (where its stripes were): the one after, at its latest
// game this season, sims to its next (as BBGM's "Sim Next").
const TeamArrows = ({
	t,
	boxScore,
	nav,
}: {
	t: any;
	boxScore: any;
	nav: { prev?: number; next?: number } | undefined;
}) => {
	if (t.tid < 0 || !nav) {
		return null;
	}
	const team = `${t.abbrev}_${t.tid}`;
	return (
		<>
			<a
				className={clsx("mb-arrow mb-arrow-back", {
					disabled: nav.prev === undefined,
				})}
				href={
					nav.prev === undefined
						? undefined
						: helpers.leagueUrl(["game_log", team, boxScore.season, nav.prev])
				}
				title="Previous game"
				aria-label="Previous game"
				data-no-scroll-reset
			>
				<span className="tb-chevron" aria-hidden />
			</a>
			<NextButton
				arrow="mb-arrow"
				abbrev={t.abbrev}
				boxScore={boxScore}
				currentGidInList
				nextGid={nav.next}
				tid={t.tid}
			/>
		</>
	);
};

type DaySide = {
	tid: number;
	pts: number;
	won?: number;
	lost?: number;
	tied?: number;
	otl?: number;
	// His side's best game of it, by game score (see gameLog.ts).
	lead?: { pid: number; name: string; pts: number; trb: number; ast: number };
};
type DayGame = {
	gid: number;
	teams: [DaySide, DaySide];
	// The pregame line: the home side's spread (above 0, home favored).
	spread?: number;
};
// "T. Jones" for Tyus Jones.
const shortName = (name: string) => {
	const words = name.trim().split(/\s+/);
	return words.length > 1
		? `${words[0]![0]}. ${words.slice(1).join(" ")}`
		: name;
};

// THE GAME'S DAY, under its banner: the date ("Wed, Nov 4, 2009", or "2009
// Day 45"), and when asked for all of that day's games - each a little score
// (the winner's line bold), this one marked, each to its page - with the
// day's Daily Schedule a tap away.
const DayGames = ({
	date,
	day,
	games,
	gid,
	season,
}: {
	date: string | undefined;
	day: number;
	games: DayGame[];
	gid: number;
	season: number;
}) => {
	const [open, setOpen] = useState(false);
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const label =
		date !== undefined
			? formatGameDate(date, "shortWithYear")
			: `${season} Day ${day}`;
	const dayUrl = helpers.leagueUrl(["daily_schedule", season, day]);
	if (games.length < 2) {
		return (
			<div className="recap-day">
				<a
					className="btn btn-light-bordered btn-sm recap-day-toggle"
					href={dayUrl}
				>
					<span className="recap-day-date">{label}</span>
				</a>
			</div>
		);
	}
	return (
		<div className="recap-day">
			<button
				type="button"
				className="btn btn-light-bordered btn-sm recap-day-toggle"
				aria-expanded={open}
				onClick={() => {
					setOpen(!open);
				}}
			>
				<span className="recap-day-date">{label}</span>
				<span className="recap-day-dot">·</span>
				All games this day
				<span className={open ? "tb-caret recap-day-open" : "tb-caret"} />
			</button>
			{open ? (
				<div className="recap-day-actions">
					<a className="btn btn-light-bordered btn-sm" href={dayUrl}>
						Daily Schedule ›
					</a>
				</div>
			) : null}
			{open ? (
				<div className="recap-day-grid">
					{games.map((gm) => {
						// (Away over home, as the banner has them.)
						const sides = [gm.teams[1], gm.teams[0]];
						const won =
							sides[0]!.pts === sides[1]!.pts
								? -1
								: sides[0]!.pts > sides[1]!.pts
									? 0
									: 1;
						const first = teamInfoCache[sides[0]!.tid];
						return (
							<a
								key={gm.gid}
								className={
									gm.gid === gid ? "recap-day-game current" : "recap-day-game"
								}
								href={helpers.leagueUrl([
									"game_log",
									first ? `${first.abbrev}_${sides[0]!.tid}` : "special",
									season,
									gm.gid,
								])}
								data-no-scroll-reset
							>
								{sides.map((side, i) => {
									const t = teamInfoCache[side.tid];
									return (
										<span
											key={i}
											className={
												won === i ? "recap-day-line won" : "recap-day-line"
											}
										>
											<Logo t={t ?? {}} size={18} />
											<span className="recap-day-abbrev">
												{t?.abbrev ?? "?"}
											</span>
											{side.won !== undefined && side.lost !== undefined ? (
												<span className="recap-day-rec">
													{helpers.formatRecord({
														won: side.won,
														lost: side.lost,
														tied: side.tied,
														otl: side.otl,
													})}
												</span>
											) : null}
											<span className="recap-day-pts">{side.pts}</span>
										</span>
									);
								})}
								{(() => {
									// (Home is the second of the two, as listed.)
									if (typeof gm.spread !== "number") {
										return null;
									}
									const line = roundHalf(gm.spread);
									const fav = teamInfoCache[sides[line > 0 ? 1 : 0]!.tid];
									return (
										<span className="recap-day-spread">
											{line === 0
												? "Line PK"
												: `Line ${fav?.abbrev ?? "?"} -${Math.abs(line)}`}
										</span>
									);
								})()}
								{sides.some((side) => side.lead) ? (
									<span className="recap-day-leads">
										{sides.map((side, i) =>
											side.lead ? (
												<span
													key={i}
													className="recap-day-lead"
													title="Points / rebounds / assists"
												>
													<span className="recap-day-lead-name">
														{shortName(side.lead.name)}
													</span>
													<span className="recap-day-lead-line">
														{side.lead.pts}/{side.lead.trb}/{side.lead.ast}
													</span>
												</span>
											) : null,
										)}
									</span>
								) : null}
							</a>
						);
					})}
				</div>
			) : null}
		</div>
	);
};

const RecapHeader = ({
	boxScore,
	abbrev,
	tid,
	prevGid,
	nextGid,
	currentGidInList,
	teamNav,
	dayGames,
}: NavProps & {
	teamNav?: Record<number, { prev?: number; next?: number }>;
	dayGames?: DayGame[];
}) => {
	const { refreshedLook } = useLocal(["refreshedLook"]);
	const [away, home] = teamsInDisplayOrder(boxScore.teams as [any, any]);
	const winner =
		home.pts > away.pts ? home.tid : away.pts > home.pts ? away.tid : undefined;
	const season = boxScore.season;
	const series = seriesLine(
		boxScore.gamecast,
		!!boxScore.playoffs,
		[away, home],
		true,
	);
	const gameDay: number | undefined =
		typeof boxScore.day === "number" && !boxScore.exhibition
			? boxScore.day
			: undefined;
	// The date goes with the day's games under the banner, where they're
	// shown; otherwise in the banner.
	const dayBar = gameDay !== undefined && dayGames !== undefined;
	const tagDay = dayBar ? undefined : gameDay;
	const inSeries =
		boxScore.numGamesToWinSeries > 1 &&
		!!boxScore.teams[0]?.playoffs &&
		!!boxScore.teams[1]?.playoffs;
	// (To the bracket.)
	const seriesLink = series ? (
		<a className="recap-series" href={helpers.leagueUrl(["playoffs", season])}>
			{series}
		</a>
	) : null;
	return (
		<div className="recap-header">
			{refreshedLook ? (
				<>
					<MatchupBanner
						teams={[away, home]}
						numbers={[away.pts, home.pts]}
						won={winner === away.tid ? 0 : winner === home.tid ? 1 : undefined}
						// (BBGM's own: "DAL leads series 1-0", the team to its page, the
						// record to the bracket.)
						// The season and the day ("2024 Day 45", to that day's
						// schedule), and in the playoffs BBGM's own "DAL leads series
						// 1-0" by it (the team to its page, the record to the bracket).
						tag={
							tagDay !== undefined || inSeries ? (
								<span>
									{tagDay !== undefined ? (
										<a
											className="mb-tag-part"
											href={helpers.leagueUrl([
												"daily_schedule",
												season,
												tagDay,
											])}
										>
											{typeof boxScore.date === "string"
												? formatGameDate(boxScore.date, "shortWithYear")
												: `${season} Day ${tagDay}`}
										</a>
									) : null}
									{tagDay !== undefined && inSeries ? (
										<span className="mb-tag-dot"> · </span>
									) : null}
									{inSeries ? (
										<span className="mb-tag-part">
											<PlayoffRecord
												numGamesToWinSeries={boxScore.numGamesToWinSeries}
												season={season}
												t0={boxScore.teams[0]}
												t1={boxScore.teams[1]}
											/>
										</span>
									) : null}
								</span>
							) : undefined
						}
						status={
							<>
								{/* (A national game: its network, as on the broadcast's own
								    final.) */}
								{boxScore.national ? (
									<NationalTag
										className="mb-national"
										tag={boxScore.national}
									/>
								) : null}
								{finalText(boxScore)}
								{/* (Its replay, if it has one, by it.) */}
								{!boxScore.exhibition && boxScore.gid >= 0 ? (
									<WatchReplayButton gid={boxScore.gid} className="mb-replay" />
								) : null}
							</>
						}
						extra={<MiniLinescore boxScore={boxScore} />}
						navs={
							teamNav
								? [
										<TeamArrows
											key="away"
											t={away}
											boxScore={boxScore}
											nav={teamNav[away.tid]}
										/>,
										<TeamArrows
											key="home"
											t={home}
											boxScore={boxScore}
											nav={teamNav[home.tid]}
										/>,
									]
								: undefined
						}
						hrefs={[
							away.tid >= 0 ? teamUrl(away, season) : undefined,
							home.tid >= 0 ? teamUrl(home, season) : undefined,
						]}
						subs={[
							away.won !== undefined ? helpers.formatRecord(away) : null,
							home.won !== undefined ? helpers.formatRecord(home) : null,
						]}
						leaders={[
							<TeamLeaders key="away" t={away} />,
							<TeamLeaders key="home" t={home} />,
						]}
					/>
					{/* (A phone: under the banner instead - no room in its halves.) */}
					<div className="mb-leaders-row">
						<div className="mb-leaders">
							<TeamLeaders t={away} />
						</div>
						<div className="mb-leaders">
							<TeamLeaders t={home} />
						</div>
					</div>
					{dayBar ? (
						<DayGames
							date={
								typeof boxScore.date === "string" ? boxScore.date : undefined
							}
							day={gameDay}
							games={dayGames}
							gid={boxScore.gid}
							season={season}
						/>
					) : null}
				</>
			) : (
				<>
					<div className="recap-scoreline d-none d-md-flex">
						<HeaderTeam t={away} season={season} won={winner === away.tid} />
						<div className="recap-middle">
							<div className="recap-status">{finalText(boxScore)}</div>
							<Linescore boxScore={boxScore} />
							{seriesLink}
						</div>
						<HeaderTeam
							t={home}
							season={season}
							won={winner === home.tid}
							right
						/>
					</div>
					<div className="recap-phone d-md-none">
						<PhoneTeam t={away} season={season} won={winner === away.tid} />
						<div className="recap-phone-mid">
							<div className="recap-status">{finalText(boxScore)}</div>
							<MiniLinescore boxScore={boxScore} />
							{seriesLink}
						</div>
						<PhoneTeam
							t={home}
							season={season}
							won={winner === home.tid}
							right
						/>
					</div>
				</>
			)}
			{/* (The refreshed look: each team's arrows in the banner, the replay
			    by the status.) */}
			{refreshedLook ? null : (
				<RecapNav
					boxScore={boxScore}
					abbrev={abbrev}
					tid={tid}
					prevGid={prevGid}
					nextGid={nextGid}
					currentGidInList={currentGidInList}
				/>
			)}
		</div>
	);
};

// The story of the game (the game's note - an AI recap, or one written in):
// its first line the headline, the rest under it, names linked.
const RecapStory = ({ boxScore }: { boxScore: any }) => {
	const note: string | undefined = boxScore.note?.trim();
	if (!note) {
		return null;
	}
	const links = buildRecapLinks(boxScore);
	const linked = links.length > 0 ? linkifyRecap(note, links) : note;
	const lines = linked.split("\n");
	const at = lines.findIndex((line) => line.trim() !== "");
	const headline = (at >= 0 ? lines[at]! : linked).replace(/^#+\s*/, "");
	const body =
		at >= 0
			? lines
					.slice(at + 1)
					.join("\n")
					.trim()
			: "";
	return (
		<Section>
			<div className="recap-headline">
				<Markdown>{headline}</Markdown>
			</div>
			{body ? (
				<div className="recap-body">
					<Markdown>{body}</Markdown>
				</div>
			) : null}
		</Section>
	);
};

const signed = (x: number) => (x === 0 ? "PK" : x > 0 ? `+${x}` : `${x}`);

// The pregame line - each side's spread, its chance to win at the tip - and
// how the game went against it.
const Line = ({ boxScore }: { boxScore: any }) => {
	const raw = boxScore.spread;
	if (typeof raw !== "number" || !Number.isFinite(raw)) {
		return null;
	}
	// The predicted home margin (above 0, home favored), as it was quoted.
	const spread = roundHalf(raw);
	// (Over the game as long as it was, where it says.)
	const regS: number =
		boxScore.charts?.periodEnds?.[(boxScore.numPeriods ?? 4) - 1] ?? GAME_S;
	const pHome = winProb({ margin: 0, secs: regS, regS, spread: raw });
	const [away, home] = teamsInDisplayOrder(boxScore.teams as [any, any]);
	const diff = home.pts - away.pts - spread;
	const rows = [
		{ t: away, line: spread, p: 1 - pHome, covered: diff < 0 },
		{ t: home, line: -spread, p: pHome, covered: diff > 0 },
	];
	return (
		<Section title="Line">
			<table className="table table-sm gamecast-table recap-line">
				<thead>
					<tr>
						<th />
						<th>Spread</th>
						<th title="Its chance to win at the tip, from the spread">Win %</th>
						<th>ATS</th>
					</tr>
				</thead>
				<tbody>
					{rows.map(({ t, line, p, covered }) => (
						<tr key={t.tid}>
							<td>
								<div className="d-flex align-items-center gap-2">
									<Logo t={t} size={22} />
									<span className="fw-bold">{t.abbrev}</span>
									{t.won !== undefined ? (
										<span className="text-body-secondary">
											{helpers.formatRecord(t)}
										</span>
									) : null}
								</div>
							</td>
							<td className="fw-bold">{signed(line)}</td>
							<td>{Math.round(100 * p)}%</td>
							<td
								className={clsx({
									"fw-bold text-success": covered,
									"text-body-secondary": !covered,
								})}
							>
								{diff === 0 ? "Push" : covered ? "Covered" : "Lost"}
							</td>
						</tr>
					))}
				</tbody>
			</table>
		</Section>
	);
};

const GameInfo = ({ boxScore }: { boxScore: any }) => {
	if (!boxScore.att && !boxScore.neutralSite) {
		return null;
	}
	return (
		<Section title="Game Info">
			<table className="table table-sm gamecast-table mb-0">
				<tbody>
					{boxScore.att ? (
						<tr>
							<td>Attendance</td>
							<td>{helpers.numberWithCommas(boxScore.att)}</td>
						</tr>
					) : null}
					{boxScore.neutralSite ? (
						<tr>
							<td colSpan={2}>Played at a neutral site</td>
						</tr>
					) : null}
				</tbody>
			</table>
		</Section>
	);
};

export const GameRecap = ({
	boxScore,
	abbrev,
	tid,
	prevGid,
	nextGid,
	currentGidInList,
	Row,
	reactions,
	teamNav,
	dayGames,
}: {
	// Each team's games before and after this one, and all that day's games
	// (see gameLog's teamNav).
	teamNav?: Record<number, { prev?: number; next?: number }>;
	dayGames?: DayGame[];
	boxScore: any;
	abbrev?: string;
	tid?: number;
	prevGid?: number;
	nextGid?: number;
	currentGidInList?: boolean;
	// The box score's row (see GameLog).
	Row: unknown;
	// The posts about the game.
	reactions?: ReactNode;
}) => {
	const [tab, setTab] = useState<Tab>("recap");
	const info: GamecastInfo | undefined = boxScore.gamecast;
	const colors = sideColors(boxScore.teams);
	const { gender, refreshedLook } = useLocal(["gender", "refreshedLook"]);
	// (A phone gets the box score a team at a time - see BoxScorePhone.)
	const isPhone = useIsPhone();

	// A saved replay's plays (see replayFeed): the play-by-play, its
	// highlights, the box score a quarter at a time - and any play, tapped,
	// opened in the replay right there.
	const [replay, setReplay] = useState<
		| {
				events: any[];
				initialBoxScore: any;
				shots: ShotSpot[];
				entries: FeedEntry[];
				rows: FeedRow[];
				ends: ReturnType<typeof feedFromEvents>["ends"];
		  }
		| undefined
	>();
	const gid: number = boxScore.gid;
	const hasReplay = !!boxScore.hasReplay;
	useEffect(() => {
		setReplay(undefined);
		if (!hasReplay) {
			return;
		}
		let live = true;
		(async () => {
			try {
				const got: any = await toWorker("main", "getReplayFeed", gid);
				if (!live || !got) {
					return;
				}
				const f = feedFromEvents(got.events, got.initialBoxScore, gender);
				setReplay({
					events: got.events,
					initialBoxScore: got.initialBoxScore,
					shots: unpackShots(got.shots),
					entries: f.entries,
					rows: feedRows([...f.entries].reverse()),
					ends: f.ends,
				});
			} catch {
				// No plays to show.
			}
		})();
		return () => {
			live = false;
		};
	}, [gid, hasReplay, gender]);
	const [period, setPeriod] = useState<number | "all">("all");
	// Each team's box score by the quarter (the refreshed look: picked by each
	// team's name - see PeriodPicker).
	const boxPeriods = useMemo((): BoxPeriods | undefined => {
		if (!replay || replay.ends.length <= 1) {
			return undefined;
		}
		return {
			labels: replay.ends.map(
				(_, k) =>
					periodLabel(k + 1, boxScore.numPeriods ?? 4, helpers.ordinal) ?? "",
			),
			boxOf: (k) => periodBoxScore(boxScore, replay.ends, k),
		};
	}, [replay, boxScore]);

	const names = useMemo(() => {
		const m = new Map<number, string>();
		for (const t of boxScore.teams) {
			for (const p of t.players ?? []) {
				m.set(p.pid, p.name);
			}
		}
		return m;
	}, [boxScore.teams]);
	const feed = useMemo(
		() => ({
			say: (row: FeedRow) =>
				playSentence({
					events: row.events as any[],
					finish: row.finish,
					name: (pid) => (pid === undefined ? undefined : names.get(pid)),
					teamName: (t) => {
						const team = boxScore.teams[t];
						return team ? `${team.region} ${team.name}` : undefined;
					},
				}),
			// The replay, opened just before the play (before the shot, for
			// one that is a shot and its result).
			open: replay
				? (row: FeedRow) => {
						const start = replay.entries[row.key - 1]?.cursor ?? 0;
						void realtimeUpdate([], helpers.leagueUrl(["live_game"]), {
							gidOneGame: gid,
							playByPlay: replay.events,
							replay: true,
							fromAction: true,
							startAt: start,
						});
					}
				: undefined,
			// Or played right there, under its row.
			clip:
				replay && __SPORT === "basketball"
					? (row: FeedRow) => (
							<HighlightClip
								game={replay}
								first={replay.entries[row.key - 1]?.cursor ?? 0}
								last={row.cursor!}
							/>
						)
					: undefined,
		}),
		[names, replay, gid, boxScore.teams],
	);
	const numPeriods = boxScore.numPeriods ?? 4;
	// Who was on the floor when (rotations, lineups, on/off): from the replay,
	// worked out only once one of them is shown (see WithStints).
	const stints = refreshedLook ? replay : undefined;
	const tabs = TABS.filter(([key]) => key !== "plays" || replay !== undefined);
	const shotChart =
		replay && replay.shots.length > 0 ? (
			<Section title="Shot Chart">
				<ShotChart
					spots={replay.shots}
					teams={boxScore.teams}
					colors={colors}
					names={names}
					plays={replay.entries}
					numPeriods={numPeriods}
					clip={feed.clip}
				/>
			</Section>
		) : null;

	// The page, top to bottom (see RecapDock): what each is called on the menu
	// at the foot of the window, and over it.
	const sections: (DockSection & {
		title?: string | null;
		body: ReactNode;
	})[] = [
		{
			id: "recap",
			label: "Recap",
			icon: DOCK_ICONS.recap,
			// (Its button to the very top of the window: the score banner over
			// the recap too.)
			toTop: true,
			// (Its leaders are in the banner over it.)
			body: (
				<div className="gamecast-narrow recap-centered">
					<RecapStory boxScore={boxScore} />
				</div>
			),
		},
		{
			id: "box",
			label: "Box Score",
			icon: DOCK_ICONS.box,
			body: isPhone ? (
				<BoxScorePhone
					boxScore={boxScore}
					periods={boxPeriods}
					extraViews={stints ? [["rot", "Rotations"]] : undefined}
					extra={(v) =>
						v === "rot" && stints ? (
							<WithStints
								events={stints.events}
								initialBoxScore={stints.initialBoxScore}
							>
								{(s) => <Rotations boxScore={boxScore} stints={s} />}
							</WithStints>
						) : null
					}
				/>
			) : (
				<BoxScore
					boxScore={boxScore}
					periods={boxPeriods}
					Row={Row}
					forceRowUpdate
					sportState={undefined}
				/>
			),
		},
		{
			id: "stats",
			label: "Team Stats",
			icon: DOCK_ICONS.stats,
			body: (
				<div className="gamecast-narrow">
					<TeamStatBars boxScore={boxScore} colors={colors} full />
				</div>
			),
		},
		...(boxScore.charts
			? [
					{
						id: "flow",
						label: "Game Flow",
						icon: DOCK_ICONS.flow,
						body: (
							<>
								<FlowCharts
									boxScore={boxScore}
									colors={colors}
									plays={replay?.rows}
									say={feed.say}
								/>
								{/* (Its rotations, lineups and on/off: only where there is no
								    replay to work out ours from - see the sections below.) */}
								{replay ? null : <GameCharts boxScore={boxScore} restOnly />}
							</>
						),
					},
				]
			: []),
		...(shotChart
			? [
					{
						id: "shots",
						label: "Shot Chart",
						// (The chart has its own heading.)
						title: null,
						icon: DOCK_ICONS.shots,
						body: shotChart,
					},
				]
			: []),
		...(replay
			? [
					{
						id: "plays",
						label: "Plays",
						// (The plays have their own heading.)
						title: null,
						icon: DOCK_ICONS.plays,
						body: (
							<div className="gamecast-narrow">
								<AllPlays
									rows={replay.rows}
									teams={boxScore.teams}
									season={boxScore.season}
									numPeriods={numPeriods}
								/>
							</div>
						),
					},
				]
			: []),
		...(stints
			? [
					{
						id: "lineups",
						label: "Lineups",
						icon: DOCK_ICONS.lineups,
						body: (
							<WithStints
								events={stints.events}
								initialBoxScore={stints.initialBoxScore}
							>
								{(s) => <Lineups boxScore={boxScore} stints={s} />}
							</WithStints>
						),
					},
					{
						id: "onoff",
						label: "On/Off",
						title: "On/Off Impact",
						icon: DOCK_ICONS.onoff,
						body: (
							<WithStints
								events={stints.events}
								initialBoxScore={stints.initialBoxScore}
							>
								{(s) => <OnOff boxScore={boxScore} stints={s} />}
							</WithStints>
						),
					},
				]
			: []),
		{
			id: "info",
			label: "More",
			title: "More",
			icon: DOCK_ICONS.info,
			body: (
				<>
					<div className="recap-two">
						<div>
							<Line boxScore={boxScore} />
							<GameInfo boxScore={boxScore} />
						</div>
						<div>
							{info ? (
								<>
									<Series boxScore={boxScore} info={info} />
									{info.bracket ? (
										<Bracket boxScore={boxScore} info={info} final />
									) : (
										<Standings boxScore={boxScore} info={info} final />
									)}
									<RecentGames boxScore={boxScore} info={info} />
								</>
							) : null}
						</div>
					</div>
					{reactions}
					{boxScore.exhibition ? null : (
						<div className="mt-3">
							<Note
								key={boxScore.gid}
								initialNote={boxScore.note}
								info={{ type: "game", gid: boxScore.gid }}
								infoLink
								editOnly
								autoLink={buildRecapLinks(boxScore)}
							/>
						</div>
					)}
				</>
			),
		},
	];

	return (
		<FeedContext.Provider value={feed}>
			<div className="gamecast recap recap-game">
				{/* (Over the game's banner: pinned already by the time it shows -
				    the banner scrolled away - not partway down the page.) */}
				{refreshedLook ? <ScoreBar boxScore={boxScore} /> : null}
				<RecapHeader
					boxScore={boxScore}
					abbrev={abbrev}
					tid={tid}
					prevGid={prevGid}
					nextGid={nextGid}
					currentGidInList={currentGidInList}
					teamNav={teamNav}
					dayGames={dayGames}
				/>
				{refreshedLook ? (
					<>
						<RecapSections
							sections={foldSections(
								// (No heading over each - the menu says where you are -
								// and the rest of it, the line and the standings, at the
								// foot of the page: see below.)
								sections
									.filter((x) => x.id !== "info")
									.map((x) => ({ ...x, title: null })),
								[
									["lineups", "onoff", "On/Off Impact"],
									// (The shot chart under the game flow, its button's:
									// it has its own heading.)
									["flow", "shots", null],
								],
								{ stats: "Stats", shots: "Shots" },
							)}
						/>
						{/* (The line, the series, the standings and the rest: at the
						    foot of the page, after the game itself.) */}
						<div className="recap-section recap-foot">
							{sections.find((x) => x.id === "info")?.body}
						</div>
					</>
				) : (
					<>
						<div className="gamecast-tabs" role="tablist">
							{tabs.map(([key, label]) => (
								<button
									key={key}
									type="button"
									role="tab"
									aria-selected={tab === key}
									className={clsx({ active: tab === key })}
									onClick={() => setTab(key)}
								>
									{label}
								</button>
							))}
						</div>
						<div className="gamecast-content">
							{tab === "recap" ? (
								<>
									<div className="recap-grid">
										<div className="recap-mid">
											<RecapStory boxScore={boxScore} />
											{boxScore.charts ? (
												<Section title="Game Flow">
													<FlowCharts
														boxScore={boxScore}
														colors={colors}
														plays={replay?.rows}
														say={feed.say}
													/>
												</Section>
											) : null}
											{shotChart}
										</div>
										<div className="recap-left">
											<Section
												title="Game Leaders"
												action={
													<LinkButton onClick={() => setTab("box")}>
														Box score
													</LinkButton>
												}
											>
												<Leaders boxScore={boxScore} />
											</Section>
											<Section
												title="Team Stats"
												action={
													<LinkButton onClick={() => setTab("stats")}>
														Full team stats
													</LinkButton>
												}
											>
												<TeamStatBars boxScore={boxScore} colors={colors} />
											</Section>
										</div>
										<div className="recap-right">
											<Line boxScore={boxScore} />
											{info ? (
												<>
													<Series boxScore={boxScore} info={info} />
													{info.bracket ? (
														<Bracket boxScore={boxScore} info={info} final />
													) : (
														<Standings boxScore={boxScore} info={info} final />
													)}
													<RecentGames boxScore={boxScore} info={info} />
												</>
											) : null}
											<GameInfo boxScore={boxScore} />
										</div>
									</div>
									{reactions}
								</>
							) : tab === "box" ? (
								<>
									{replay && replay.ends.length > 1 ? (
										<div className="mb-2">
											<Switch
												options={[
													["all", "Game"] as [number | "all", string],
													...replay.ends.map(
														(_, k) =>
															[
																k,
																periodLabel(
																	k + 1,
																	numPeriods,
																	helpers.ordinal,
																) ?? "",
															] as [number | "all", string],
													),
												]}
												value={period}
												onChange={setPeriod}
											/>
										</div>
									) : null}
									<BoxScore
										key={String(period)}
										boxScore={
											replay && period !== "all"
												? periodBoxScore(boxScore, replay.ends, period)
												: boxScore
										}
										Row={Row}
										forceRowUpdate
										sportState={undefined}
									/>
								</>
							) : tab === "plays" && replay ? (
								<div className="gamecast-narrow">
									{shotChart}
									<AllPlays
										rows={replay.rows}
										teams={boxScore.teams}
										season={boxScore.season}
										numPeriods={numPeriods}
									/>
								</div>
							) : tab === "stats" ? (
								<div className="gamecast-narrow">
									<Section title="Team Stats">
										<TeamStatBars boxScore={boxScore} colors={colors} full />
									</Section>
									<Section title="Four Factors">
										<FourFactors teams={teamsInDisplayOrder(boxScore.teams)} />
									</Section>
								</div>
							) : (
								<>
									{boxScore.charts ? (
										<Section title="Game Flow">
											<FlowCharts
												boxScore={boxScore}
												colors={colors}
												plays={replay?.rows}
												say={feed.say}
											/>
										</Section>
									) : null}
									<GameCharts boxScore={boxScore} restOnly />
								</>
							)}
							{boxScore.exhibition ? null : (
								<div className="mt-3">
									<Note
										key={boxScore.gid}
										initialNote={boxScore.note}
										info={{ type: "game", gid: boxScore.gid }}
										infoLink
										editOnly
										autoLink={buildRecapLinks(boxScore)}
									/>
								</div>
							)}
						</div>
					</>
				)}
			</div>
		</FeedContext.Provider>
	);
};
