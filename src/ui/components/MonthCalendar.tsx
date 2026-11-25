import clsx from "clsx";
import {
	useEffect,
	useRef,
	useState,
	type CSSProperties,
	type ReactNode,
} from "react";
import {
	addDays,
	formatGameDate,
	formatMonth,
	makeDate,
	weekdayOf,
} from "../../common/calendarDates.ts";
import type { LocalStateUI } from "../../common/types.ts";
import type {
	CalendarGame,
	CalendarKeyDate,
} from "../../worker/views/calendar.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { bannerColors, useAllTeamColors } from "./TeamBanner.tsx";
import { NationalTag } from "./NationalTag.tsx";

// A MONTH OF THE NBA CALENDAR, Sunday to Saturday. For a team, its games as
// the scores bar has them (see TeamDay); for the league, how many games each
// night. Full size on the Schedule page, compact on Daily Schedule and the
// dashboard.

type TeamInfo = LocalStateUI["teamInfoCache"][number];

const WEEKDAYS = [
	"Sunday",
	"Monday",
	"Tuesday",
	"Wednesday",
	"Thursday",
	"Friday",
	"Saturday",
];

const monthOf = (date: string) => date.slice(0, 7);

const firstOfMonth = (month: string) => `${month}-01`;

const nextMonth = (month: string) =>
	monthOf(
		makeDate(Number(month.slice(0, 4)), Number(month.slice(5, 7)) + 1, 1),
	);

// Every month from the first night to the last.
const monthsBetween = (dates: string[]) => {
	if (dates.length === 0) {
		return [];
	}
	let first = dates[0]!;
	let last = first;
	for (const date of dates) {
		if (date < first) {
			first = date;
		}
		if (date > last) {
			last = date;
		}
	}
	const months = [monthOf(first)];
	while (months.at(-1)! < monthOf(last)) {
		months.push(nextMonth(months.at(-1)!));
	}
	return months;
};

// The month of the chosen night or today, or else the end of a season
// that's over and the start of one from the past.
const defaultMonth = (
	months: string[],
	anchor: string | undefined,
	currentSeason: boolean,
) => {
	if (anchor !== undefined && months.includes(monthOf(anchor))) {
		return monthOf(anchor);
	}
	return currentSeason ? months.at(-1) : months[0];
};

// Sunday to Saturday, enough weeks to cover the month.
const weeksOf = (month: string) => {
	const first = firstOfMonth(month);
	const last = addDays(firstOfMonth(nextMonth(month)), -1);
	const start = addDays(first, -weekdayOf(first));
	const end = addDays(last, 6 - weekdayOf(last));
	const weeks: string[][] = [];
	for (let date = start; date <= end; date = addDays(date, 7)) {
		weeks.push(Array.from({ length: 7 }, (_, i) => addDays(date, i)));
	}
	return weeks;
};

// A team's logo on a white disc, so a dark logo shows on a dark night and any
// logo on the team's color.
const Crest = ({
	children,
	t,
}: {
	children?: ReactNode;
	t: TeamInfo | undefined;
}) => {
	const url = t?.imgURLSmall ?? t?.imgURL;
	return (
		<span className="lc-crest">
			{url ? (
				<img src={url} alt="" />
			) : (
				<span className="lc-crest-abbrev">{t?.abbrev}</span>
			)}
			{children}
		</span>
	);
};

// [on a phone, wider]
const KEY_DATE_LABELS: Record<CalendarKeyDate["type"], [string, string]> = {
	tradeDeadline: ["Deadline", "Trade deadline"],
	allStar: ["All-Star", "All-Star Game"],
	openingNight: ["Opening", "Opening Night"],
	christmas: ["Christmas", "Christmas Day"],
	mlkDay: ["MLK Day", "MLK Day"],
	rivalsWeek: ["Rivals", "Rivals Week"],
};

const KeyDateLabel = ({
	compact,
	keyDate,
	season,
}: {
	compact: boolean;
	keyDate: CalendarKeyDate;
	season: number;
}) => {
	const [short, long] = KEY_DATE_LABELS[keyDate.type];
	const label = compact ? (
		short
	) : (
		<>
			<span className="d-md-none">{short}</span>
			<span className="d-none d-md-inline">{long}</span>
		</>
	);
	return (
		<div className="lc-key" title={long}>
			{keyDate.gid !== undefined ? (
				<a
					href={helpers.leagueUrl(["game_log", "special", season, keyDate.gid])}
				>
					{label}
				</a>
			) : (
				label
			)}
		</div>
	);
};

// The favorite's line, "ATL -4.5".
const lineText = (abbrev: string | undefined, spread: number) =>
	`${abbrev ?? ""} -${Math.abs(spread).toLocaleString("en-US", {
		maximumFractionDigits: 1,
	})}`;

// A TEAM'S DAY, in the scores bar's look: the date on a band across the top,
// with OT, the playoff game and the network there; under it the opponent, vs
// or @, and the score - or before tip-off, the line. A game played green if
// the team won, red if it lost; a day with no game just the date, faded.
const TeamDay = ({
	compact,
	date,
	game,
	keyDate,
	nightDay,
	playoffGame,
	season,
	spread,
	teams,
	tid,
}: {
	compact: boolean;
	date: string;
	game: CalendarGame | undefined;
	keyDate: CalendarKeyDate | undefined;
	// The league's night, to open on a day off.
	nightDay: number | undefined;
	playoffGame: number | undefined;
	season: number;
	// The next game's line, home's (see TopBarDay).
	spread: number | undefined;
	teams: TeamInfo[];
	tid: number;
}) => {
	const dayNum = Number(date.slice(8));

	if (!game) {
		return (
			<>
				{nightDay !== undefined ? (
					<a
						className="lc-cover"
						href={helpers.leagueUrl(["daily_schedule", season, nightDay])}
						aria-label={`Games on ${formatGameDate(date, "full")}`}
					/>
				) : null}
				<div className="lc-d-head">
					<span className="lc-d-num">{dayNum}</span>
				</div>
				{keyDate ? (
					<KeyDateLabel compact={compact} keyDate={keyDate} season={season} />
				) : null}
			</>
		);
	}

	const home = game.homeTid === tid;
	const opp = home ? game.awayTid : game.homeTid;
	const final = game.homePts !== undefined && game.awayPts !== undefined;
	const pts = home ? game.homePts : game.awayPts;
	const oppPts = home ? game.awayPts : game.homePts;
	const won = final && pts! > oppPts!;
	const href = final
		? helpers.leagueUrl([
				"game_log",
				helpers.getTeamUrlPart(tid),
				season,
				game.gid,
			])
		: game.day !== undefined
			? helpers.leagueUrl(["daily_schedule", season, game.day])
			: game.planned
				? helpers.leagueUrl(["playoffs", season])
				: undefined;
	const t = teams[opp];
	const logo = t?.imgURLSmall ?? t?.imgURL;
	const ot =
		game.overtimes !== undefined && game.overtimes > 0
			? `${game.overtimes > 1 ? game.overtimes : ""}OT`
			: undefined;
	const tag = game.playIn
		? compact
			? "PI"
			: "Play-In"
		: playoffGame !== undefined
			? `G${playoffGame}${game.ifNecessary ? "*" : ""}`
			: undefined;
	const favorite =
		final || spread === undefined || spread === 0
			? undefined
			: spread > 0
				? game.homeTid
				: game.awayTid;

	const label = `${home ? "vs" : "@"} ${t?.region ?? ""} ${t?.name ?? ""}${
		final ? `, ${won ? "W" : "L"} ${pts}-${oppPts}${ot ? ` ${ot}` : ""}` : ""
	}${game.ifNecessary ? " (if necessary)" : ""}${
		game.national ? `, on ${game.national.network}` : ""
	}`;

	// The whole day is the game's link, laid over it, under the opponent's
	// logo and abbreviation, which open its roster.
	return (
		<>
			{href ? (
				<a className="lc-cover" href={href} title={label} aria-label={label} />
			) : null}
			<div className="lc-d-head">
				<span className="lc-d-num">{dayNum}</span>
				{keyDate ? (
					<span className="lc-d-key">{KEY_DATE_LABELS[keyDate.type][1]}</span>
				) : null}
				<span className="lc-d-status">
					{ot ? <span className="lc-d-ot">{ot}</span> : null}
					{tag ? <span className="lc-d-tag">{tag}</span> : null}
					{game.national ? (
						<NationalTag className="lc-d-national" tag={game.national} />
					) : null}
				</span>
			</div>
			<div className="lc-d-body">
				<div className="lc-d-opp">
					<a
						className="lc-d-team"
						href={helpers.leagueUrl([
							"roster",
							`${t?.abbrev ?? opp}_${opp}`,
							season,
						])}
						title={`${t?.region ?? ""} ${t?.name ?? ""} roster`}
					>
						<span className="lc-d-at">{home ? "vs" : "@"}</span>
						<span className="lc-d-logo">
							{logo ? <img src={logo} alt="" /> : null}
						</span>
						<span className="lc-d-abbrev">{t?.abbrev}</span>
					</a>
				</div>
				{final ? (
					<span className="lc-d-final">
						{pts}-{oppPts}
					</span>
				) : favorite !== undefined ? (
					<span className="lc-d-line" title="Point spread">
						{lineText(teams[favorite]?.abbrev, spread!)}
					</span>
				) : null}
			</div>
		</>
	);
};

const LeagueNight = ({
	compact,
	count,
	day,
	maybe,
	mine,
	planned,
	season,
	teams,
	userTid,
}: {
	compact: boolean;
	count: number;
	day: number | undefined;
	// Games that may not be needed.
	maybe: number;
	mine: CalendarGame | undefined;
	// Playoff games on the plan, not yet on the schedule.
	planned: boolean;
	season: number;
	teams: TeamInfo[];
	userTid: number;
}) => {
	const opp =
		mine === undefined
			? undefined
			: mine.homeTid === userTid
				? mine.awayTid
				: mine.homeTid;
	return (
		<a
			className="lc-game"
			href={
				day !== undefined
					? helpers.leagueUrl(["daily_schedule", season, day])
					: planned
						? helpers.leagueUrl(["playoffs", season])
						: undefined
			}
			title={maybe > 0 ? `${maybe} if necessary` : undefined}
		>
			<span className="lc-count">
				{count}
				{compact ? null : (
					<span className="d-none d-md-inline">
						{" "}
						{helpers.plural("game", count)}
					</span>
				)}
				{maybe > 0 ? "*" : null}
			</span>
			{opp !== undefined ? <Crest t={teams[opp]} /> : null}
		</a>
	);
};

// The league's night the plain way, as Daily Schedule's strip of days has
// it: the date, how many games, and the user's team's little logo in the
// corner the nights it plays.
const PlainNight = ({
	date,
	night,
	season,
	userLogo,
}: {
	date: string;
	night:
		| {
				count: number;
				day: number | undefined;
				maybe: number;
				mine: CalendarGame | undefined;
				planned: boolean;
		  }
		| undefined;
	season: number;
	userLogo: string | undefined;
}) => {
	const inner = (
		<>
			<span className="lc-p-num">{Number(date.slice(8))}</span>
			<span className="lc-p-games">
				{night
					? `${night.count}${night.maybe > 0 ? "*" : ""} ${helpers.plural("game", night.count)}`
					: null}
			</span>
			{night?.mine && userLogo ? (
				<img className="lc-p-logo" src={userLogo} alt="" />
			) : null}
		</>
	);
	const href =
		night?.day !== undefined
			? helpers.leagueUrl(["daily_schedule", season, night.day])
			: night?.planned
				? helpers.leagueUrl(["playoffs", season])
				: undefined;
	return href ? (
		<a
			className="lc-p"
			href={href}
			title={
				night && night.maybe > 0 ? `${night.maybe} if necessary` : undefined
			}
		>
			{inner}
		</a>
	) : (
		<span className="lc-p">{inner}</span>
	);
};

export const MonthCalendar = ({
	className,
	compact = false,
	games,
	keyDates,
	monthChips = false,
	nightDays,
	plain = false,
	season,
	selected,
	tid,
	today,
}: {
	className?: string;
	compact?: boolean;
	games: CalendarGame[];
	keyDates: CalendarKeyDate[];
	// Each night's day, so a team's nights off open the league's games too.
	nightDays?: Record<string, number>;
	// Month buttons along the top, with a team's record in each.
	monthChips?: boolean;
	// The league's nights the plain way (see PlainNight).
	plain?: boolean;
	season: number;
	// The night a page is showing, like Daily Schedule's day.
	selected?: string;
	tid: number | "all";
	today: string | undefined;
}) => {
	const {
		games: localGames,
		season: currentSeason,
		teamInfoCache,
		userTid,
	} = useLocal(["games", "season", "teamInfoCache", "userTid"]);

	// The next game's line, as the scores bar has it.
	const spreads = new Map<number, number>();
	for (const game of localGames) {
		if (game.spread !== undefined) {
			spreads.set(game.gid, game.spread);
		}
	}

	const months = monthsBetween([
		...games.map((game) => game.date),
		...keyDates.map((keyDate) => keyDate.date),
	]);
	const anchor = selected ?? today;
	const startMonth = defaultMonth(months, anchor, season === currentSeason);
	const [chosenMonth, setChosenMonth] = useState(startMonth);

	// A new season starts from its own default month. Choosing another night
	// takes the calendar to its month, and so does today moving into the next
	// month, if the calendar was showing today.
	const prev = useRef({ season, anchor });
	useEffect(() => {
		const prevAnchor = prev.current.anchor;
		if (season !== prev.current.season) {
			setChosenMonth(startMonth);
		} else if (
			anchor !== undefined &&
			prevAnchor !== undefined &&
			monthOf(anchor) !== monthOf(prevAnchor)
		) {
			setChosenMonth((month) =>
				selected !== undefined || month === monthOf(prevAnchor)
					? monthOf(anchor)
					: month,
			);
		}
		prev.current = { season, anchor };
	}, [anchor, season, selected, startMonth]);

	// The league's, the user's road nights in his team's color (so no "@" is
	// needed), his home nights plain.
	const allColors = useAllTeamColors();
	const colors = bannerColors(allColors?.[userTid]);

	const month =
		chosenMonth !== undefined && months.includes(chosenMonth)
			? chosenMonth
			: startMonth;

	if (month === undefined) {
		return null;
	}

	// (A night's first: the deadline over Rivals Week.)
	const keyByDate = new Map<string, CalendarKeyDate>();
	for (const keyDate of keyDates) {
		if (!keyByDate.has(keyDate.date)) {
			keyByDate.set(keyDate.date, keyDate);
		}
	}

	// A team's own nights, results by month, back-to-backs and playoff game
	// numbers.
	const teamGames =
		typeof tid === "number"
			? games.filter((game) => game.homeTid === tid || game.awayTid === tid)
			: [];
	const teamByDate = new Map(teamGames.map((game) => [game.date, game]));
	const playoffGameNumbers = new Map<number, number>();
	const playoffCounts = new Map<number, number>();
	const records = new Map<string, { won: number; lost: number }>();
	for (const game of teamGames) {
		const home = game.homeTid === tid;
		const opp = home ? game.awayTid : game.homeTid;
		if (game.playoffs && !game.playIn) {
			const n = (playoffCounts.get(opp) ?? 0) + 1;
			playoffCounts.set(opp, n);
			playoffGameNumbers.set(game.gid, n);
		}
		if (game.homePts !== undefined && game.awayPts !== undefined) {
			const record = records.get(monthOf(game.date)) ?? { won: 0, lost: 0 };
			const pts = home ? game.homePts : game.awayPts;
			const oppPts = home ? game.awayPts : game.homePts;
			if (pts > oppPts) {
				record.won += 1;
			} else {
				record.lost += 1;
			}
			records.set(monthOf(game.date), record);
		}
	}

	// The league's nights: how many games, and the user's team's.
	const nights = new Map<
		string,
		{
			count: number;
			day: number | undefined;
			maybe: number;
			mine: CalendarGame | undefined;
			planned: boolean;
		}
	>();
	if (tid === "all") {
		for (const game of games) {
			const night = nights.get(game.date) ?? {
				count: 0,
				day: game.day,
				maybe: 0,
				mine: undefined,
				planned: false,
			};
			night.count += 1;
			if (game.ifNecessary) {
				night.maybe += 1;
			}
			if (game.planned) {
				night.planned = true;
			}
			if (game.homeTid === userTid || game.awayTid === userTid) {
				night.mine = game;
			}
			nights.set(game.date, night);
		}
	}

	const userInfo = teamInfoCache[userTid];
	const userLogo = userInfo?.imgURLSmall ?? userInfo?.imgURL;

	const monthIndex = months.indexOf(month);
	const todayMonth = today !== undefined ? monthOf(today) : undefined;
	const isTeam = typeof tid === "number";

	return (
		<div
			className={clsx(
				"lc",
				compact && "lc-compact",
				isTeam ? "lc-team" : "lc-league",
				plain && !isTeam && "lc-plain",
				className,
			)}
			style={
				{
					"--lc-team-bg": colors.bg,
					"--lc-team-fg": colors.fg,
				} as CSSProperties
			}
		>
			{/* The month between its arrows, itself a menu of the season's months. */}
			<div className="lc-head">
				<div className="lc-nav">
					<button
						type="button"
						className="lc-arrow"
						disabled={monthIndex <= 0}
						onClick={() => setChosenMonth(months[monthIndex - 1])}
						aria-label="Previous month"
					>
						<svg viewBox="0 0 16 16" width="16" height="16" aria-hidden>
							<path
								d="M10 3 5 8l5 5"
								fill="none"
								stroke="currentColor"
								strokeWidth="2"
								strokeLinecap="round"
								strokeLinejoin="round"
							/>
						</svg>
					</button>
					<div className="lc-title" role="heading" aria-level={compact ? 3 : 2}>
						<select
							className="lc-title-select"
							value={month}
							onChange={(event) => setChosenMonth(event.target.value)}
							aria-label="Month"
						>
							{months.map((m) => (
								<option key={m} value={m}>
									{formatMonth(firstOfMonth(m))}
								</option>
							))}
						</select>
					</div>
					<button
						type="button"
						className="lc-arrow"
						disabled={monthIndex >= months.length - 1}
						onClick={() => setChosenMonth(months[monthIndex + 1])}
						aria-label="Next month"
					>
						<svg viewBox="0 0 16 16" width="16" height="16" aria-hidden>
							<path
								d="m6 3 5 5-5 5"
								fill="none"
								stroke="currentColor"
								strokeWidth="2"
								strokeLinecap="round"
								strokeLinejoin="round"
							/>
						</svg>
					</button>
				</div>
				{todayMonth !== undefined &&
				todayMonth !== month &&
				months.includes(todayMonth) ? (
					<button
						type="button"
						className="lc-today-button"
						onClick={() => setChosenMonth(todayMonth)}
					>
						Today
					</button>
				) : null}
			</div>

			{monthChips ? (
				<div className="lc-months">
					{months.map((m) => {
						const record = records.get(m);
						return (
							<button
								key={m}
								type="button"
								className={clsx("lc-month", m === month && "active")}
								onClick={() => setChosenMonth(m)}
							>
								<span className="lc-month-name">
									{formatMonth(firstOfMonth(m), true)}
								</span>
								<span className="lc-month-record">
									{record ? `${record.won}-${record.lost}` : "\u00a0"}
								</span>
							</button>
						);
					})}
				</div>
			) : null}

			<table className="lc-grid">
				<thead>
					<tr>
						{WEEKDAYS.map((name) => (
							<th key={name}>
								{compact ? (
									name[0]
								) : (
									<>
										<span className="d-md-none">{name.slice(0, 3)}</span>
										<span className="d-none d-md-inline">{name}</span>
									</>
								)}
							</th>
						))}
					</tr>
				</thead>
				<tbody>
					{weeksOf(month).map((week) => (
						<tr key={week[0]}>
							{week.map((date) => {
								if (monthOf(date) !== month) {
									return <td key={date} className="lc-out" />;
								}
								const game = isTeam ? teamByDate.get(date) : undefined;
								const night = nights.get(date);
								const keyDate = keyByDate.get(date);
								if (plain && !isTeam) {
									return (
										<td
											key={date}
											className={clsx(
												night ? "lc-night" : "lc-off",
												date === today && "lc-today",
												date === selected && "lc-selected",
											)}
											title={formatGameDate(date, "full")}
										>
											<PlainNight
												date={date}
												night={night}
												season={season}
												userLogo={userLogo}
											/>
										</td>
									);
								}
								if (isTeam) {
									const won =
										game?.homePts !== undefined && game.awayPts !== undefined
											? game.homeTid === tid
												? game.homePts > game.awayPts
												: game.awayPts > game.homePts
											: undefined;
									return (
										<td
											key={date}
											className={clsx(
												game ? "lc-d lc-d-game" : "lc-d lc-d-off",
												won === true && "lc-d-won",
												won === false && "lc-d-lost",
												game?.ifNecessary && "lc-maybe",
												date === today && "lc-today",
												date === selected && "lc-selected",
											)}
											title={formatGameDate(date, "full")}
										>
											<TeamDay
												compact={compact}
												date={date}
												game={game}
												keyDate={keyDate}
												nightDay={nightDays?.[date]}
												playoffGame={
													game ? playoffGameNumbers.get(game.gid) : undefined
												}
												season={season}
												spread={game ? spreads.get(game.gid) : undefined}
												teams={teamInfoCache}
												tid={tid}
											/>
										</td>
									);
								}
								return (
									<td
										key={date}
										className={clsx(
											night && "lc-night",
											night?.mine?.awayTid === userTid && "lc-road",
											date === today && "lc-today",
											date === selected && "lc-selected",
										)}
										title={formatGameDate(date, "full")}
									>
										<div className="lc-day-head">
											<span className="lc-num">{Number(date.slice(8))}</span>
										</div>
										{night ? (
											<LeagueNight
												compact={compact}
												count={night.count}
												day={night.day}
												maybe={night.maybe}
												mine={night.mine}
												planned={night.planned}
												season={season}
												teams={teamInfoCache}
												userTid={userTid}
											/>
										) : null}
										{keyDate ? (
											<KeyDateLabel
												compact={compact}
												keyDate={keyDate}
												season={season}
											/>
										) : null}
									</td>
								);
							})}
						</tr>
					))}
				</tbody>
			</table>
		</div>
	);
};
