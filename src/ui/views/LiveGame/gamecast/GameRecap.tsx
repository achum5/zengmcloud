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
	WatchReplayButton,
} from "../../../components/BoxScoreWrapper.tsx";
import Note from "../../Player/Note.tsx";
import { teamsInDisplayOrder } from "../../../util/boxScoreDisplayOrder.ts";
import { roundHalf } from "../../../../common/getGameSpread.ts";
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
import { MatchupBanner } from "../../../components/TeamBanner.tsx";
import { DOCK_ICONS, RecapDock, type DockSection } from "./RecapDock.tsx";
import { BoxScorePhone, useIsPhone } from "./BoxScorePhone.tsx";
import { Lineups, OnOff, Rotations } from "./LineupViews.tsx";
import { stintsFromEvents } from "./stints.ts";

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

// A team's top scorer, for the banner: "T. Jones 38 PTS", to his page.
const topScorer = (t: any) => {
	const top = ((t.players ?? []) as any[]).reduce<any>(
		(best, p) => (best === undefined || p.pts > best.pts ? p : best),
		undefined,
	);
	if (!top || !(top.pts > 0)) {
		return null;
	}
	const words = String(top.name).trim().split(/\s+/);
	const short =
		words.length > 1
			? `${words[0]![0]}. ${words.slice(1).join(" ")}`
			: top.name;
	return (
		<a href={helpers.leagueUrl(["player", top.pid])}>
			{short} <span className="mb-leader-pts">{top.pts} PTS</span>
		</a>
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
// score - or, compact (arrows, a play mark), in the team's strip over it
// (the refreshed look's game log).
export const RecapNav = ({
	boxScore,
	abbrev,
	tid,
	prevGid,
	nextGid,
	currentGidInList,
	compact,
}: NavProps & { compact?: boolean }) => {
	const prevUrl = helpers.leagueUrl([
		"game_log",
		abbrev === "special" ? abbrev : `${abbrev}_${tid}`,
		boxScore.season,
		prevGid,
	]);
	const replay =
		!boxScore.exhibition && boxScore.gid >= 0 ? (
			<WatchReplayButton gid={boxScore.gid} compact={compact} />
		) : null;
	const next = (
		<NextButton
			abbrev={abbrev}
			boxScore={boxScore}
			currentGidInList={currentGidInList}
			nextGid={nextGid}
			tid={tid}
			compact={compact}
		/>
	);
	if (compact) {
		return (
			<div className="tb-actions">
				{replay}
				<div className="btn-group">
					<a
						className={clsx("btn btn-light-bordered btn-sm", {
							disabled: prevGid === undefined,
						})}
						href={prevUrl}
						title="Previous game"
						aria-label="Previous game"
						data-no-scroll-reset
					>
						<span className="glyphicon glyphicon-menu-left" />
					</a>
					{next}
				</div>
			</div>
		);
	}
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

const RecapHeader = ({
	boxScore,
	abbrev,
	tid,
	prevGid,
	nextGid,
	currentGidInList,
	navElsewhere,
}: NavProps & { navElsewhere?: boolean }) => {
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
						tag={
							series ? (
								<a href={helpers.leagueUrl(["playoffs", season])}>{series}</a>
							) : undefined
						}
						status={finalText(boxScore)}
						extra={<MiniLinescore boxScore={boxScore} />}
						hrefs={[
							away.tid >= 0 ? teamUrl(away, season) : undefined,
							home.tid >= 0 ? teamUrl(home, season) : undefined,
						]}
						subs={[
							away.won !== undefined ? helpers.formatRecord(away) : null,
							home.won !== undefined ? helpers.formatRecord(home) : null,
						]}
						leaders={[topScorer(away), topScorer(home)]}
					/>
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
			{navElsewhere ? null : (
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
	navElsewhere,
}: {
	boxScore: any;
	abbrev?: string;
	tid?: number;
	prevGid?: number;
	nextGid?: number;
	currentGidInList?: boolean;
	// The game before/after and the replay are shown elsewhere (the team's
	// strip over the score - see RecapNav).
	navElsewhere?: boolean;
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
	// Who was on the floor when (rotations, lineups, on/off), from the replay.
	const stints = useMemo(() => {
		if (!replay || !refreshedLook) {
			return undefined;
		}
		try {
			return stintsFromEvents(replay.events, replay.initialBoxScore);
		} catch {
			return undefined;
		}
	}, [replay, refreshedLook]);
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
			id: "box",
			label: "Box Score",
			icon: DOCK_ICONS.box,
			body: isPhone ? (
				<BoxScorePhone
					boxScore={
						replay && period !== "all"
							? periodBoxScore(boxScore, replay.ends, period)
							: boxScore
					}
					extraViews={stints ? [["rot", "Rotations"]] : undefined}
					extra={(v) =>
						v === "rot" && stints ? (
							<Rotations boxScore={boxScore} stints={stints} />
						) : null
					}
				/>
			) : (
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
			),
		},
		{
			id: "recap",
			label: "Recap",
			icon: DOCK_ICONS.recap,
			body: (
				<div className="recap-two">
					<div>
						<RecapStory boxScore={boxScore} />
					</div>
					<div>
						<Section title="Game Leaders">
							<Leaders boxScore={boxScore} />
						</Section>
					</div>
				</div>
			),
		},
		{
			id: "stats",
			label: "Team Stats",
			icon: DOCK_ICONS.stats,
			body: (
				<div className="recap-two">
					<div>
						<TeamStatBars boxScore={boxScore} colors={colors} full />
					</div>
					<div>
						<Section title="Four Factors">
							<FourFactors teams={teamsInDisplayOrder(boxScore.teams)} />
						</Section>
					</div>
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
								<GameCharts boxScore={boxScore} restOnly />
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
						body: <Lineups boxScore={boxScore} stints={stints} />,
					},
					{
						id: "onoff",
						label: "On/Off",
						title: "On/Off Impact",
						icon: DOCK_ICONS.onoff,
						body: <OnOff boxScore={boxScore} stints={stints} />,
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
			<div className="gamecast recap">
				<RecapHeader
					boxScore={boxScore}
					abbrev={abbrev}
					tid={tid}
					prevGid={prevGid}
					nextGid={nextGid}
					currentGidInList={currentGidInList}
					navElsewhere={navElsewhere}
				/>
				{refreshedLook ? (
					<>
						<div className="recap-sections">
							{sections.map((x) => (
								<section
									key={x.id}
									id={`recap-${x.id}`}
									className="recap-section"
								>
									{x.title === null ? null : (
										<h2 className="recap-section-title">
											{x.title ?? x.label}
										</h2>
									)}
									{x.body}
								</section>
							))}
						</div>
						<RecapDock
							sections={sections}
							quarter={
								replay && replay.ends.length > 1 ? (
									<select
										className="form-select form-select-sm"
										aria-label="Quarter"
										value={String(period)}
										onChange={(e) => {
											const v = e.target.value;
											setPeriod(v === "all" ? "all" : Number(v));
											document.getElementById("recap-box")?.scrollIntoView({
												behavior: "smooth",
												block: "start",
											});
										}}
									>
										<option value="all">All Qs</option>
										{replay.ends.map((_, k) => (
											<option key={k} value={k}>
												{periodLabel(k + 1, numPeriods, helpers.ordinal)}
											</option>
										))}
									</select>
								) : undefined
							}
						/>
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
