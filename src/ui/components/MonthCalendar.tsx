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

// A MONTH OF THE NBA CALENDAR, Sunday to Saturday. For a team, its games -
// home nights shaded, results, back-to-backs, playoff games; for the league,
// how many games each night, the user's team's road nights in its color. Full size on the
// Calendar and Schedule pages, compact on Daily Schedule and the dashboard.

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

// The opponent's logo on a white disc, so a dark logo shows on a dark night
// and any logo on the team's color.
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

// A win or a loss, as a little coin, green or red.
const Coin = ({ className, won }: { className: string; won: boolean }) => (
	<span className={clsx("lc-wl", className, won ? "lc-w" : "lc-l")}>
		{won ? "W" : "L"}
	</span>
);

const KeyDateLabel = ({
	compact,
	keyDate,
	season,
}: {
	compact: boolean;
	keyDate: CalendarKeyDate;
	season: number;
}) => {
	const [short, long] =
		keyDate.type === "tradeDeadline"
			? ["Deadline", "Trade deadline"]
			: ["All-Star", "All-Star Game"];
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

// What's in a team's night's corner: Play-In, the playoff game's number, or
// a back-to-back.
const teamTag = ({
	b2b,
	compact,
	game,
	playoffGame,
}: {
	b2b: boolean;
	compact: boolean;
	game: CalendarGame;
	playoffGame: number | undefined;
}) =>
	game.playIn ? (
		<span className="lc-tag lc-tag-playoffs" title="Play-In">
			{compact ? "PI" : "Play-In"}
		</span>
	) : playoffGame !== undefined ? (
		<span
			className="lc-tag lc-tag-playoffs"
			title={game.ifNecessary ? "If necessary" : undefined}
		>
			G{playoffGame}
			{game.ifNecessary ? "*" : null}
		</span>
	) : b2b && !compact ? (
		<span className="lc-tag" title="Back-to-back">
			B2B
		</span>
	) : null;

const TeamNight = ({
	compact,
	game,
	season,
	teams,
	tid,
}: {
	compact: boolean;
	game: CalendarGame;
	season: number;
	teams: TeamInfo[];
	tid: number;
}) => {
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
	const ot =
		game.overtimes !== undefined && game.overtimes > 0
			? `${game.overtimes > 1 ? game.overtimes : ""}OT`
			: undefined;

	const label = `${home ? "vs" : "@"} ${t?.region ?? ""} ${t?.name ?? ""}${
		final ? `, ${won ? "W" : "L"} ${pts}-${oppPts}${ot ? ` ${ot}` : ""}` : ""
	}${game.ifNecessary ? " (if necessary)" : ""}`;

	// The whole night is the game's link, laid over it, under the
	// opponent's logo and abbreviation, which open its roster.
	return (
		<div className="lc-game">
			{href ? (
				<a className="lc-cover" href={href} title={label} aria-label={label} />
			) : null}
			<span className="lc-matchup">
				<a
					className="lc-team-link"
					href={helpers.leagueUrl([
						"roster",
						`${t?.abbrev ?? opp}_${opp}`,
						season,
					])}
					title={`${t?.region ?? ""} ${t?.name ?? ""} roster`}
				>
					{/* (On the crest where there's no room for more: phones and compact.) */}
					<Crest t={t}>
						{final ? <Coin className="lc-badge" won={won} /> : null}
					</Crest>
					{compact ? null : (
						<span className="lc-opp-abbrev">
							<span className="lc-at">{home ? "vs" : "@"}</span>
							{t?.abbrev}
						</span>
					)}
				</a>
			</span>
			{final ? (
				<span className="lc-result">
					<Coin className="lc-coin" won={won} />
					<span className="lc-score">
						{pts}-{oppPts}
						{ot ? <span className="lc-ot">{ot}</span> : null}
					</span>
				</span>
			) : compact ? null : game.ifNecessary ? (
				<span className="lc-note">If necessary</span>
			) : game.national ? (
				<NationalTag className="lc-national" tag={game.national} />
			) : null}
		</div>
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

export const MonthCalendar = ({
	className,
	compact = false,
	games,
	keyDates,
	monthChips = false,
	season,
	selected,
	tid,
	today,
}: {
	className?: string;
	compact?: boolean;
	games: CalendarGame[];
	keyDates: CalendarKeyDate[];
	// Month buttons along the top, with a team's record in each.
	monthChips?: boolean;
	season: number;
	// The night a page is showing, like Daily Schedule's day.
	selected?: string;
	tid: number | "all";
	today: string | undefined;
}) => {
	const {
		season: currentSeason,
		teamInfoCache,
		userTid,
	} = useLocal(["season", "teamInfoCache", "userTid"]);

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

	// A team's home nights in its colors, the way team sites fill them in;
	// the league's, the user's road nights in his team's color (so no "@" is
	// needed), his home nights plain.
	const allColors = useAllTeamColors();
	const colors = bannerColors(allColors?.[tid === "all" ? userTid : tid]);

	const month =
		chosenMonth !== undefined && months.includes(chosenMonth)
			? chosenMonth
			: startMonth;

	if (month === undefined) {
		return null;
	}

	const keyByDate = new Map(keyDates.map((keyDate) => [keyDate.date, keyDate]));

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

	const monthIndex = months.indexOf(month);
	const todayMonth = today !== undefined ? monthOf(today) : undefined;
	const isTeam = typeof tid === "number";

	return (
		<div
			className={clsx(
				"lc",
				compact && "lc-compact",
				isTeam ? "lc-team" : "lc-league",
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
								return (
									<td
										key={date}
										className={clsx(
											game && (game.homeTid === tid ? "lc-home" : "lc-away"),
											game?.ifNecessary && "lc-maybe",
											night && "lc-night",
											night?.mine?.awayTid === userTid && "lc-road",
											date === today && "lc-today",
											date === selected && "lc-selected",
										)}
										title={formatGameDate(date, "full")}
									>
										<div className="lc-day-head">
											<span className="lc-num">{Number(date.slice(8))}</span>
											{game && isTeam
												? teamTag({
														b2b: teamByDate.has(addDays(date, -1)),
														compact,
														game,
														playoffGame: playoffGameNumbers.get(game.gid),
													})
												: null}
										</div>
										{game && isTeam ? (
											<TeamNight
												compact={compact}
												game={game}
												season={season}
												teams={teamInfoCache}
												tid={tid}
											/>
										) : null}
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

			{isTeam && !compact ? (
				<div className="lc-legend">
					<span>
						<span className="lc-swatch lc-swatch-home" />
						Home
					</span>
					<span>
						<span className="lc-swatch lc-swatch-away" />
						Away
					</span>
					{todayMonth !== undefined ? (
						<span>
							<span className="lc-swatch lc-swatch-today" />
							Today
						</span>
					) : null}
				</div>
			) : null}
		</div>
	);
};
