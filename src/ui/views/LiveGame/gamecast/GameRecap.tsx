// A FINISHED GAME'S PAGE, laid out as a game recap: the score across the
// top with the score by quarter, tabs under it - Recap (the leaders and the
// team stats; the story of the game and how it went; the line, the season
// series and the standings), the box score, the team stats and the game
// flow. Basketball, on a device that has the tabbed layout turned on (the
// same switch as the live game's - see local.gamecastLayout).

import clsx from "clsx";
import { useState, type ReactNode } from "react";
import { helpers } from "../../../util/helpers.ts";
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
	Linescore,
	Logo,
	LinkButton,
	RecentGames,
	Section,
	Series,
	Standings,
	TeamStatBars,
} from "./Gamecast.tsx";
import { FlowCharts } from "./FlowCharts.tsx";
import { winProb } from "../winProb.ts";

type Tab = "recap" | "box" | "stats" | "flow";

const TABS: [Tab, string][] = [
	["recap", "Recap"],
	["box", "Box Score"],
	["stats", "Team Stats"],
	["flow", "Game Flow"],
];

const teamUrl = (t: any, season: number) =>
	helpers.leagueUrl(["roster", `${t.abbrev}_${t.tid}`, season]);

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

const RecapHeader = ({
	boxScore,
	abbrev,
	tid,
	prevGid,
	nextGid,
	currentGidInList,
}: {
	boxScore: any;
	abbrev?: string;
	tid?: number;
	prevGid?: number;
	nextGid?: number;
	currentGidInList?: boolean;
}) => {
	const [away, home] = teamsInDisplayOrder(boxScore.teams as [any, any]);
	const winner =
		home.pts > away.pts ? home.tid : away.pts > home.pts ? away.tid : undefined;
	const season = boxScore.season;
	return (
		<div className="recap-header">
			<div className="recap-scoreline d-none d-md-flex">
				<HeaderTeam t={away} season={season} won={winner === away.tid} />
				<div className="recap-middle">
					<div className="recap-status">{finalText(boxScore)}</div>
					<Linescore boxScore={boxScore} />
				</div>
				<HeaderTeam t={home} season={season} won={winner === home.tid} right />
			</div>
			<div className="recap-phone d-md-none">
				<PhoneTeam t={away} season={season} won={winner === away.tid} />
				<div className="recap-phone-mid">
					<div className="recap-status">{finalText(boxScore)}</div>
					<MiniLinescore boxScore={boxScore} />
				</div>
				<PhoneTeam t={home} season={season} won={winner === home.tid} right />
			</div>
			<div className="recap-nav">
				<a
					className={clsx("btn btn-light-bordered btn-sm", {
						disabled: prevGid === undefined,
					})}
					href={helpers.leagueUrl([
						"game_log",
						abbrev === "special" ? abbrev : `${abbrev}_${tid}`,
						season,
						prevGid,
					])}
					data-no-scroll-reset
				>
					Prev
				</a>
				{!boxScore.exhibition && boxScore.gid >= 0 ? (
					<WatchReplayButton gid={boxScore.gid} />
				) : null}
				<div className="recap-next">
					<NextButton
						abbrev={abbrev}
						boxScore={boxScore}
						currentGidInList={currentGidInList}
						nextGid={nextGid}
						tid={tid}
					/>
				</div>
			</div>
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
	const pHome = winProb({ margin: 0, left: 1, spread: raw });
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
}: {
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

	return (
		<div className="gamecast recap">
			<RecapHeader
				boxScore={boxScore}
				abbrev={abbrev}
				tid={tid}
				prevGid={prevGid}
				nextGid={nextGid}
				currentGidInList={currentGidInList}
			/>
			<div className="gamecast-tabs" role="tablist">
				{TABS.map(([key, label]) => (
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
										<FlowCharts boxScore={boxScore} colors={colors} />
									</Section>
								) : null}
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
										<Standings boxScore={boxScore} info={info} final />
										<RecentGames boxScore={boxScore} info={info} />
									</>
								) : null}
								<GameInfo boxScore={boxScore} />
							</div>
						</div>
						{reactions}
					</>
				) : tab === "box" ? (
					<BoxScore
						boxScore={boxScore}
						Row={Row}
						forceRowUpdate={false}
						sportState={undefined}
					/>
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
								<FlowCharts boxScore={boxScore} colors={colors} />
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
		</div>
	);
};
