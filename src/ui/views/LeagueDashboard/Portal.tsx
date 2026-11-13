import clsx from "clsx";
import { useCallback, useEffect, useRef, useState } from "react";
import { PHASE } from "../../../common/constants.ts";
import type { HeadlineSceneSpec } from "../../../common/headlineScene.ts";
import { categories, types } from "../../../common/transactionInfo.ts";
import type { View } from "../../../common/types.ts";
import { GameHighlightsButton } from "../../components/GameHighlightsButton.tsx";
import { NewsBlock } from "../../components/NewsBlock.tsx";
import { NextPrevButtons } from "../../components/NextPrevButtons.tsx";
import { PlayoffMatchup } from "../../components/PlayoffMatchup.tsx";
import { SafeHtml } from "../../components/SafeHtml.tsx";
import useTitleBar from "../../hooks/useTitleBar.tsx";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { toWorker } from "../../util/toWorker.ts";
import { HeadlineScene } from "../LiveGame/court3d/HeadlineScene.tsx";
import HeadlinePlay from "./HeadlinePlay.tsx";
import Leaders from "./Leaders.tsx";
import Standings from "./Standings.tsx";
import StartingLineup from "./StartingLineup.tsx";
import TeamStats from "./TeamStats.tsx";

// The new interface's league home, laid out as a hub: the inbox down the
// left; the league's news, the next game, the latest scores and the next two
// weeks in the middle; the schedule and the standings on the right. Built
// from the game's usual tables, cards and links. One column on a phone,
// what's next first.

type Props = View<"leagueDashboard">;
type TeamInfo = Props["teams"][number];

const Logo = ({ t, size = 20 }: { t: TeamInfo | undefined; size?: number }) =>
	t?.imgURLSmall || t?.imgURL ? (
		<img
			src={t.imgURLSmall ?? t.imgURL}
			alt=""
			width={size}
			height={size}
			style={{ objectFit: "contain" }}
		/>
	) : null;

const rosterUrl = (t: TeamInfo | undefined, tid: number) =>
	helpers.leagueUrl(["roster", `${t?.abbrev ?? tid}_${tid}`]);

const WL = ({ pts, oppPts }: { pts: number; oppPts: number }) => (
	<span className={pts > oppPts ? "text-success" : "text-danger"}>
		{pts > oppPts ? "W" : "L"} {pts}-{oppPts}
	</span>
);

const Messages = ({
	hubMessages,
	teamNews,
	phase,
	numRoster,
	maxRosterSize,
	minRosterSize,
}: Pick<
	Props,
	"hubMessages" | "teamNews" | "numRoster" | "maxRosterSize" | "minRosterSize"
> & { phase: number }) => {
	const [filter, setFilter] = useState<"all" | "tasks" | "unread">("all");

	const tasks: { text: string; href: string }[] = [];
	if (numRoster > maxRosterSize) {
		tasks.push({
			text: `Roster over the limit (${numRoster}/${maxRosterSize})`,
			href: helpers.leagueUrl(["roster"]),
		});
	} else if (
		numRoster < minRosterSize &&
		phase >= PHASE.REGULAR_SEASON &&
		phase <= PHASE.PLAYOFFS
	) {
		tasks.push({
			text: `Roster under the minimum (${numRoster}/${minRosterSize})`,
			href: helpers.leagueUrl(["free_agents"]),
		});
	}
	if (phase === PHASE.RESIGN_PLAYERS) {
		tasks.push({
			text: "Re-sign your players",
			href: helpers.leagueUrl(["negotiation"]),
		});
	} else if (
		phase === PHASE.DRAFT ||
		phase === PHASE.FANTASY_DRAFT ||
		phase === PHASE.EXPANSION_DRAFT
	) {
		tasks.push({ text: "The draft is on", href: helpers.leagueUrl(["draft"]) });
	} else if (phase === PHASE.FREE_AGENCY) {
		tasks.push({
			text: "Free agency is open",
			href: helpers.leagueUrl(["free_agents"]),
		});
	}
	const unread = hubMessages.filter((m) => !m.read);
	const shownMessages = filter === "all" ? hubMessages : unread;

	return (
		<>
			<div className="d-flex align-items-center flex-wrap gap-2 mb-2">
				<h2 className="mb-0 me-auto">Messages</h2>
				<div className="btn-group btn-group-sm" role="group">
					{(
						[
							["all", "All"],
							["tasks", `Tasks (${tasks.length + unread.length})`],
							["unread", `Unread (${unread.length})`],
						] as const
					).map(([key, label]) => (
						<button
							key={key}
							type="button"
							className={clsx(
								"btn",
								filter === key ? "btn-secondary" : "btn-light-bordered",
							)}
							onClick={() => setFilter(key)}
						>
							{label}
						</button>
					))}
				</div>
			</div>
			<table className="table table-striped table-borderless table-sm mb-1">
				<tbody>
					{filter !== "unread"
						? tasks.map((task) => (
								<tr key={task.href} className="fw-bold">
									<td>
										<span className="badge badge-news bg-warning text-dark">
											To do
										</span>
									</td>
									<td className="text-wrap">
										<a href={task.href}>{task.text}</a>
									</td>
								</tr>
							))
						: null}
					{shownMessages.map((m) => (
						<tr key={`m${m.mid}`} className={clsx(!m.read && "fw-bold")}>
							<td>
								<span className="badge badge-news bg-secondary">{m.from}</span>
							</td>
							<td className="text-wrap">
								<a href={helpers.leagueUrl(["message", m.mid])}>
									{m.subject || m.preview}
								</a>
							</td>
						</tr>
					))}
					{filter === "all"
						? teamNews.map((event) => {
								const info = types[event.type];
								return (
									<tr key={`e${event.eid}`}>
										<td>
											<span
												className={clsx(
													"badge badge-news",
													info
														? categories[info.category].className
														: "bg-secondary",
												)}
											>
												{info?.text ?? event.type}
											</span>
										</td>
										<td className="text-wrap">
											<SafeHtml dirty={event.text} />
										</td>
									</tr>
								);
							})
						: null}
				</tbody>
			</table>
			{filter !== "all" && tasks.length + unread.length === 0 ? (
				<p className="text-body-secondary">Nothing waiting.</p>
			) : null}
			<a href={helpers.leagueUrl(["inbox"])}>» Inbox</a>
		</>
	);
};

// A story's picture where it has no play to show: the teams it is about.
const StoryPoster = ({
	event,
	teams,
}: {
	event: Props["events"][number];
	teams: Props["teams"];
}) => {
	const tids =
		event.tid !== undefined ? [event.tid] : (event.tids ?? []).slice(0, 2);
	return (
		<div className="ni-stage-box mb-3">
			<div className="ni-stage bg-body-tertiary border rounded">
				{tids.map((tid) => (
					<Logo key={tid} t={teams[tid]} size={96} />
				))}
			</div>
		</div>
	);
};

// How long a story with nothing to play stays up, and how long one waits
// after its play has finished, before the next comes on (ms).
const STORY_MS = 9000;
const AFTER_PLAY_MS = 2500;
const PLAY_MOST_MS = 40000;

// The league's top stories, one at a time, moving on by themselves, once
// round - a story about a game whose replay is saved plays its moment on the
// 3D court right there, and a draft pick walks out in his new team's cap.
// Stepping through them by hand stops the moving on.
const TopStories = ({
	events,
	teams,
	season,
	userTid,
}: Pick<Props, "events" | "teams"> & {
	season: number;
	userTid: number;
}) => {
	const { gameSimInProgress } = useLocal(["gameSimInProgress"]);
	const [eid, setEid] = useState<number | undefined>();
	const [auto, setAuto] = useState(true);
	const eids = events.map((event) => event.eid);
	const event = events.find((e) => e.eid === eid) ?? events[0];

	// On screen, in a tab being looked at: nothing plays or moves on unseen.
	const block = useRef<HTMLDivElement>(null);
	const [seen, setSeen] = useState(false);
	useEffect(() => {
		const el = block.current;
		if (!el || typeof IntersectionObserver === "undefined") {
			setSeen(true);
			return;
		}
		let onScreen = false;
		const update = () => setSeen(onScreen && !document.hidden);
		const io = new IntersectionObserver(
			(entries) => {
				onScreen = entries.some((entry) => entry.isIntersecting);
				update();
			},
			{ threshold: 0.4 },
		);
		io.observe(el);
		document.addEventListener("visibilitychange", update);
		return () => {
			io.disconnect();
			document.removeEventListener("visibilitychange", update);
		};
	}, []);

	// The game the story is about (the one its text links to), if its replay
	// is saved: its play can be shown.
	const gidText = event?.text.match(/\/game_log\/[^"/]+\/\d+\/(\d+)/)?.[1];
	const gid = gidText === undefined ? undefined : Number(gidText);
	const shownEid = event?.eid;
	const [play, setPlay] = useState<{ eid: number; gid: number } | undefined>();
	useEffect(() => {
		if (gid === undefined || shownEid === undefined) {
			return;
		}
		let live = true;
		(async () => {
			try {
				if (live && (await toWorker("main", "hasLiveGameReplay", gid))) {
					setPlay({ eid: shownEid, gid });
				}
			} catch {
				// Nothing to play.
			}
		})();
		return () => {
			live = false;
		};
	}, [gid, shownEid]);
	// (Not while games are being simmed: the court waits its turn.)
	const playing =
		play !== undefined && play.eid === shownEid && seen && !gameSimInProgress
			? play
			: undefined;
	// Once it has started, a story's play stays up - scrolled away and back,
	// it is not staged all over again.
	const [started, setStarted] = useState<number | undefined>();
	useEffect(() => {
		if (playing) {
			setStarted(playing.eid);
		}
	}, [playing]);
	const showPlay =
		play !== undefined && play.eid === shownEid && started === shownEid;

	// A story with a scene of its own in the 3D building (see HeadlineScene):
	// a draft pick's walk-out. Not one there turned out to be no scene of.
	const scenePid = event?.type === "draft" ? event.pids?.[0] : undefined;
	const [noScene, setNoScene] = useState<number[]>([]);
	const scene: HeadlineSceneSpec | undefined =
		__SPORT === "basketball" &&
		scenePid !== undefined &&
		shownEid !== undefined &&
		!noScene.includes(shownEid)
			? { type: "draftPick", pid: scenePid }
			: undefined;

	const eidsRef = useRef(eids);
	eidsRef.current = eids;
	// On to the next story - and once round them all, back on the first to
	// stay (a phone is not kept drawing the court for as long as the page is
	// open).
	const shownRef = useRef(shownEid);
	shownRef.current = shownEid;
	const next = useCallback(() => {
		const list = eidsRef.current;
		const i = list.indexOf(shownRef.current ?? list[0]!);
		if (i + 1 >= list.length) {
			setAuto(false);
			setEid(list[0]);
		} else {
			setEid(list[i + 1]);
		}
	}, []);
	const [played, setPlayed] = useState<number | undefined>();
	const waitsOnPlay =
		((play !== undefined && play.eid === shownEid) || scene !== undefined) &&
		played !== shownEid;
	useEffect(() => {
		if (!auto || !seen || gameSimInProgress || eids.length < 2) {
			return;
		}
		// (A play is waited for, but not for ever.)
		const id = window.setTimeout(
			next,
			waitsOnPlay
				? PLAY_MOST_MS
				: played === shownEid
					? AFTER_PLAY_MS
					: STORY_MS,
		);
		return () => window.clearTimeout(id);
	}, [
		auto,
		seen,
		gameSimInProgress,
		eids.length,
		waitsOnPlay,
		played,
		shownEid,
		next,
	]);

	return (
		<div className="mb-3" ref={block}>
			<div className="d-flex align-items-center mb-2">
				<h2 className="mb-0 me-auto">League Headlines</h2>
				{events.length > 1 ? (
					<NextPrevButtons
						currentItem={event?.eid}
						items={eids}
						onChange={(newEid) => {
							setAuto(false);
							setEid(newEid);
						}}
					/>
				) : null}
			</div>
			{event && showPlay && play ? (
				<HeadlinePlay
					key={play.eid}
					gid={play.gid}
					pid={event.pids?.[0]}
					fallback={<StoryPoster event={event} teams={teams} />}
					onDone={() => setPlayed(play.eid)}
					onNone={() => setPlay(undefined)}
				/>
			) : event && scene ? (
				<div className="mb-3">
					<HeadlineScene
						key={event.eid}
						scene={scene}
						playing={seen && !gameSimInProgress}
						onEnd={() => setPlayed(event.eid)}
						onNone={() => setNoScene((eids) => [...eids, event.eid])}
					/>
				</div>
			) : event ? (
				<StoryPoster event={event} teams={teams} />
			) : null}
			{event ? (
				<div className="fs-5">
					<NewsBlock
						event={event}
						season={season}
						teams={teams}
						userTid={userTid}
					/>
				</div>
			) : (
				<p className="text-body-secondary">No news yet.</p>
			)}
			<a href={helpers.leagueUrl(["news"])}>» News Feed</a>
		</div>
	);
};

// Where a GM goes next when there is no game to play.
const PHASE_LINKS: Partial<Record<number, { text: string; url: string[] }>> = {
	[PHASE.PRESEASON]: { text: "Roster", url: ["roster"] },
	[PHASE.DRAFT_LOTTERY]: { text: "Draft Scouting", url: ["draft_scouting"] },
	[PHASE.DRAFT]: { text: "Draft", url: ["draft"] },
	[PHASE.AFTER_DRAFT]: { text: "Draft History", url: ["draft_history"] },
	[PHASE.RESIGN_PLAYERS]: { text: "Re-sign Players", url: ["negotiation"] },
	[PHASE.FREE_AGENCY]: { text: "Free Agents", url: ["free_agents"] },
};

const NextGame = ({
	upcoming,
	played,
	nextDay,
	teams,
	rank,
	conf,
	won,
	lost,
	series,
	seriesTitle,
	showPlayoffSeries,
	numGamesToWinSeries,
	playoffRoundsWon,
	roundsWonText,
	phase,
	season,
	userTid,
}: Pick<
	Props,
	| "upcoming"
	| "played"
	| "nextDay"
	| "teams"
	| "won"
	| "lost"
	| "conf"
	| "series"
	| "seriesTitle"
	| "showPlayoffSeries"
	| "numGamesToWinSeries"
	| "playoffRoundsWon"
	| "roundsWonText"
> & {
	rank: number | undefined;
	phase: number;
	season: number;
	userTid: number;
}) => {
	const next = upcoming[0];
	const opp = next ? teams[next.opp] : undefined;
	const form = played.slice(-5);
	const inSeries = showPlayoffSeries && !!series;
	const phaseLink = PHASE_LINKS[phase];

	return (
		<>
			{inSeries ? (
				<>
					<h2>{seriesTitle || "Playoffs"}</h2>
					<PlayoffMatchup
						expandTeamNames
						numGamesToWinSeries={numGamesToWinSeries}
						season={season}
						series={series}
						userTid={userTid}
					/>
					<div className="mt-1 mb-3">
						<a href={helpers.leagueUrl(["playoffs"])}>» Playoffs</a>
					</div>
				</>
			) : null}
			{next && opp ? (
				<>
					<h2>Next Game</h2>
					<div className="d-flex align-items-center gap-2 mb-2">
						<a href={rosterUrl(opp, next.opp)}>
							<Logo t={opp} size={48} />
						</a>
						<div>
							<div className="fs-5">
								{next.home ? "vs" : "@"}{" "}
								<a href={rosterUrl(opp, next.opp)}>{opp.region}</a>
							</div>
							<div className="text-body-secondary">
								Day {next.day}
								{nextDay !== undefined && next.day > nextDay
									? `, in ${next.day - nextDay} days`
									: null}
							</div>
						</div>
					</div>
				</>
			) : inSeries ? null : (
				<h2>Season</h2>
			)}
			<div>
				Record: {won}-{lost}
				{playoffRoundsWon < 0 && rank !== undefined
					? `, ${helpers.ordinal(rank)} in ${conf?.name ?? "the league"}`
					: null}
			</div>
			{playoffRoundsWon >= 0 ? <div>{roundsWonText}</div> : null}
			{form.length > 0 ? (
				<div className="mb-1">
					Last {form.length}:{" "}
					{form.map((game) => (
						<b
							key={game.gid}
							className={clsx(
								"me-1",
								game.pts > game.oppPts ? "text-success" : "text-danger",
							)}
							title={`${game.pts}-${game.oppPts}`}
						>
							{game.pts > game.oppPts ? "W" : "L"}
						</b>
					))}
				</div>
			) : null}
			{next || !phaseLink ? (
				<a href={helpers.leagueUrl(["schedule"])}>» Schedule</a>
			) : (
				<a href={helpers.leagueUrl(phaseLink.url as any)}>» {phaseLink.text}</a>
			)}
		</>
	);
};

// College leagues: the program in place of finances, as on the classic
// dashboard.
const Program = ({
	collegeNilBudget,
	collegePrestige,
	collegeCoachExp,
	collegeSeat,
	payroll,
}: Pick<
	Props,
	"collegePrestige" | "collegeCoachExp" | "collegeSeat" | "payroll"
> & { collegeNilBudget: number }) => (
	<>
		<h2>Program</h2>
		<div className="mb-3">
			Prestige: {collegePrestige}
			<br />
			NIL payroll: {helpers.formatCurrency(payroll / 1000, "M")}
			<br />
			NIL budget: {helpers.formatCurrency(collegeNilBudget / 1000, "M")}
			<br />
			{collegeCoachExp !== undefined ? (
				<>
					Your contract thru {collegeCoachExp}
					<br />
				</>
			) : null}
			{collegeSeat ? (
				<>
					Job security:{" "}
					<span
						className={
							collegeSeat === "Hot"
								? "text-danger"
								: collegeSeat === "Warm"
									? "text-warning"
									: "text-success"
						}
					>
						{collegeSeat === "Hot"
							? "Hot seat"
							: collegeSeat === "Warm"
								? "Warm seat"
								: "Secure"}
					</span>
					<br />
				</>
			) : null}
			<a href={helpers.leagueUrl(["recruiting"])}>» Recruiting</a>
		</div>
	</>
);

const Finances = ({
	att,
	revenue,
	profit,
	cash,
	payroll,
}: Pick<Props, "att" | "revenue" | "profit" | "cash" | "payroll">) => {
	const { luxuryPayroll, salaryCap, salaryCapType } = useLocal([
		"luxuryPayroll",
		"salaryCap",
		"salaryCapType",
	]);
	return (
		<>
			<h2>Finances</h2>
			<div className="mb-3">
				Avg Attendance: {helpers.numberWithCommas(Math.round(att))}
				<br />
				Revenue (YTD): {helpers.formatCurrency(revenue, "M")}
				<br />
				Profit (YTD): {helpers.formatCurrency(profit, "M")}
				<br />
				Cash: {helpers.formatCurrency(cash / 1000, "M")}
				<br />
				Payroll: {helpers.formatCurrency(payroll / 1000, "M")}
				<br />
				{salaryCapType === "none" ? (
					<>Luxury Tax: {helpers.formatCurrency(luxuryPayroll / 1000, "M")}</>
				) : (
					<>Salary Cap: {helpers.formatCurrency(salaryCap / 1000, "M")}</>
				)}
				<br />
				<a href={helpers.leagueUrl(["team_finances"])}>» Team Finances</a>
			</div>
		</>
	);
};

const LatestScores = ({
	latestScores,
	teams,
	userTid,
}: Pick<Props, "latestScores" | "teams"> & { userTid: number }) => {
	// Any game with a replay saved can be watched as its highlights.
	const anyReplay = latestScores.some((game) => game.replay);
	return (
		<>
			<h2>Latest Scores</h2>
			{latestScores.length > 0 ? (
				<table className="table table-striped table-borderless table-sm mb-1">
					<tbody>
						{latestScores.map((game) => {
							const homeWon = game.home.pts > game.away.pts;
							return (
								<tr
									key={game.gid}
									className={clsx(
										(game.home.tid === userTid || game.away.tid === userTid) &&
											"table-info",
									)}
								>
									<td className={clsx(!homeWon && "fw-bold")}>
										<Logo t={teams[game.away.tid]} size={16} />{" "}
										{teams[game.away.tid]?.abbrev}
									</td>
									<td className={clsx("text-end", !homeWon && "fw-bold")}>
										{game.away.pts}
									</td>
									<td className={clsx(homeWon && "fw-bold")}>
										@ <Logo t={teams[game.home.tid]} size={16} />{" "}
										{teams[game.home.tid]?.abbrev}
									</td>
									<td className={clsx("text-end", homeWon && "fw-bold")}>
										<a
											href={helpers.leagueUrl(["game_log", "_", "_", game.gid])}
										>
											{game.home.pts}
										</a>
									</td>
									{anyReplay ? (
										<td className="text-end">
											{game.replay ? (
												<GameHighlightsButton gid={game.gid} />
											) : null}
										</td>
									) : null}
								</tr>
							);
						})}
					</tbody>
				</table>
			) : (
				<p className="text-body-secondary">No games played yet.</p>
			)}
			<a href={helpers.leagueUrl(["daily_schedule"])}>» Daily Schedule</a>
		</>
	);
};

// The days around today, with games, results and the season's key dates. A
// grid of two weeks, or a list of the busy days on a phone.
const Calendar = ({
	upcoming,
	played,
	keyDates,
	nextDay,
	teams,
}: Pick<Props, "upcoming" | "played" | "keyDates" | "nextDay" | "teams">) => {
	if (nextDay === undefined) {
		return null;
	}
	const start = Math.max(1, nextDay - 3);
	const days = Array.from({ length: 14 }, (_, j) => start + j);
	const playedBy = new Map(played.map((game) => [game.day, game]));
	const upcomingBy = new Map(upcoming.map((game) => [game.day, game]));
	const keyBy = new Map(keyDates.map((k) => [k.day, k.type]));
	const busy = days.filter(
		(d) => playedBy.has(d) || upcomingBy.has(d) || keyBy.has(d),
	);
	if (busy.length === 0) {
		return null;
	}

	const cell = (day: number) => {
		const done = playedBy.get(day);
		const next = upcomingBy.get(day);
		const key = keyBy.get(day);
		return (
			<>
				{done ? (
					<div>
						{done.home ? "" : "@"}
						{teams[done.opp]?.abbrev} <WL pts={done.pts} oppPts={done.oppPts} />
					</div>
				) : null}
				{next ? (
					<div>
						<Logo t={teams[next.opp]} size={16} /> {next.home ? "vs" : "@"}{" "}
						{teams[next.opp]?.abbrev}
					</div>
				) : null}
				{key ? (
					<div className="text-body-secondary">
						{key === "tradeDeadline" ? "Trade deadline" : "All-Star Game"}
					</div>
				) : null}
			</>
		);
	};

	return (
		<div className="mb-3">
			<h2>Calendar</h2>
			<table className="table table-bordered table-sm ni-calendar d-none d-sm-table mb-1">
				<tbody>
					{[days.slice(0, 7), days.slice(7)].map((week, i) => (
						<tr key={i}>
							{week.map((day) => (
								<td key={day} className={clsx(day === nextDay && "table-info")}>
									<div className="text-body-secondary small">Day {day}</div>
									{cell(day)}
								</td>
							))}
						</tr>
					))}
				</tbody>
			</table>
			<table className="table table-striped table-borderless table-sm d-sm-none mb-1">
				<tbody>
					{busy.map((day) => (
						<tr key={day} className={clsx(day === nextDay && "table-info")}>
							<td className="text-body-secondary text-nowrap">Day {day}</td>
							<td>{cell(day)}</td>
						</tr>
					))}
				</tbody>
			</table>
		</div>
	);
};

const Schedule = ({
	upcoming,
	played,
	teams,
}: Pick<Props, "upcoming" | "played" | "teams">) => {
	const rows = [
		...played.slice(-3).map((game) => ({ ...game, done: true })),
		...upcoming.slice(0, 7).map((game) => ({
			...game,
			done: false,
			gid: undefined,
			pts: 0,
			oppPts: 0,
		})),
	];
	if (rows.length === 0) {
		return null;
	}
	// Phones get the same days from the calendar's list.
	return (
		<div className="mb-3 d-none d-sm-block">
			<h2>Schedule</h2>
			<table className="table table-striped table-borderless table-sm mb-1">
				<tbody>
					{rows.map((game) => (
						<tr key={`${game.done}${game.day}`}>
							<td className="text-body-secondary">Day {game.day}</td>
							<td>
								{game.home ? "vs" : "@"} <Logo t={teams[game.opp]} size={16} />{" "}
								<a href={rosterUrl(teams[game.opp], game.opp)}>
									{teams[game.opp]?.abbrev}
								</a>
							</td>
							<td className="text-end">
								{game.done && game.gid !== undefined ? (
									<a href={helpers.leagueUrl(["game_log", "_", "_", game.gid])}>
										<WL pts={game.pts} oppPts={game.oppPts} />
									</a>
								) : null}
							</td>
						</tr>
					))}
				</tbody>
			</table>
			<a href={helpers.leagueUrl(["schedule"])}>» Full Schedule</a>
		</div>
	);
};

const Portal = (props: Props) => {
	useTitleBar({ title: `${props.region} ${props.name}` });
	const { phase, season, userTid } = useLocal(["phase", "season", "userTid"]);
	const rank = props.confOrAllTeams.find((t) => t.tid === userTid)?.rank;

	return (
		<div className="row">
			<div className="col-xl-5 col-lg-12 order-1 order-xl-2 mb-3">
				<TopStories
					events={props.events}
					teams={props.teams}
					season={season}
					userTid={userTid}
				/>
				<div className="row">
					<div className="col-sm-6 mb-3">
						<NextGame
							upcoming={props.upcoming}
							played={props.played}
							nextDay={props.nextDay}
							teams={props.teams}
							rank={rank}
							conf={props.conf}
							won={props.won}
							lost={props.lost}
							series={props.series}
							seriesTitle={props.seriesTitle}
							showPlayoffSeries={props.showPlayoffSeries}
							numGamesToWinSeries={props.numGamesToWinSeries}
							playoffRoundsWon={props.playoffRoundsWon}
							roundsWonText={props.roundsWonText}
							phase={phase}
							season={season}
							userTid={userTid}
						/>
					</div>
					<div className="col-sm-6 mb-3">
						<LatestScores
							latestScores={props.latestScores}
							teams={props.teams}
							userTid={userTid}
						/>
					</div>
				</div>
				<Calendar
					upcoming={props.upcoming}
					played={props.played}
					keyDates={props.keyDates}
					nextDay={props.nextDay}
					teams={props.teams}
				/>
			</div>
			<div className="col-xl-4 col-lg-6 order-2 order-xl-1 mb-3">
				<Messages
					hubMessages={props.hubMessages}
					teamNews={props.teamNews}
					phase={phase}
					numRoster={props.numRoster}
					maxRosterSize={props.maxRosterSize}
					minRosterSize={props.minRosterSize}
				/>
				<div className="mt-3">
					<Leaders
						leagueLeaders={props.leagueLeaders}
						teamLeaders={props.teamLeaders}
					/>
				</div>
			</div>
			<div className="col-xl-3 col-lg-6 order-3 mb-3">
				<Schedule
					upcoming={props.upcoming}
					played={props.played}
					teams={props.teams}
				/>
				<Standings
					confOrAllTeams={props.confOrAllTeams}
					maxPlayoffSeed={props.maxPlayoffSeed}
					maxPlayoffSeedNoPlayIn={props.maxPlayoffSeedNoPlayIn}
					playoffsByConf={props.playoffsByConf}
					pointsFormula={props.pointsFormula}
					usePts={props.usePts}
					userTid={userTid}
				/>
				<div className="row mt-3">
					<div className="col-6 col-lg-12">
						<TeamStats teamStats={props.teamStats} />
					</div>
					<div className="col-6 col-lg-12">
						{props.collegeNilBudget === undefined ? (
							<Finances
								att={props.att}
								revenue={props.revenue}
								profit={props.profit}
								cash={props.cash}
								payroll={props.payroll}
							/>
						) : (
							<Program
								collegeNilBudget={props.collegeNilBudget}
								collegePrestige={props.collegePrestige}
								collegeCoachExp={props.collegeCoachExp}
								collegeSeat={props.collegeSeat}
								payroll={props.payroll}
							/>
						)}
					</div>
				</div>
			</div>
			<div className="col-12 order-4 mb-3">
				<StartingLineup
					numPlayersOnCourt={props.numPlayersOnCourt}
					starters={props.starters}
					startersStats={props.startersStats}
				/>
			</div>
		</div>
	);
};

export default Portal;
