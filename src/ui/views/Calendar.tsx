import clsx from "clsx";
import { useEffect, useRef, useState } from "react";
import {
	addDays,
	formatGameDate,
	formatMonth,
	makeDate,
	weekdayOf,
} from "../../common/calendarDates.ts";
import type { LocalStateUI, View } from "../../common/types.ts";
import { MoreLinks } from "../components/MoreLinks.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";

// THE LEAGUE'S CALENDAR: a month at a time, Sunday to Saturday. For a team,
// its games - home nights shaded, results, back-to-backs, playoff games; for
// the league, how many games each night.

type Props = View<"calendar">;
type Game = Props["games"][number];
type KeyDate = Props["keyDates"][number];
type TeamInfo = LocalStateUI["teamInfoCache"][number];

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

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

// Today's month, or else the end of a season that's over and the start of
// one from the past.
const defaultMonth = (
	months: string[],
	today: string | undefined,
	currentSeason: boolean,
) => {
	if (today !== undefined && months.includes(monthOf(today))) {
		return monthOf(today);
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

const Logo = ({ t }: { t: TeamInfo | undefined }) => {
	const url = t?.imgURLSmall ?? t?.imgURL;
	return url ? (
		<img className="lc-logo" src={url} alt="" />
	) : (
		<span className="lc-abbrev">{t?.abbrev}</span>
	);
};

const KeyDateLabel = ({
	keyDate,
	season,
}: {
	keyDate: KeyDate;
	season: number;
}) => {
	const [short, long] =
		keyDate.type === "tradeDeadline"
			? ["Deadline", "Trade deadline"]
			: ["All-Star", "All-Star Game"];
	const label = (
		<>
			<span className="d-md-none">{short}</span>
			<span className="d-none d-md-inline">{long}</span>
		</>
	);
	return (
		<div className="lc-key">
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

const TeamNight = ({
	b2b,
	game,
	playoffGame,
	season,
	teams,
	tid,
}: {
	b2b: boolean;
	game: Game;
	playoffGame: number | undefined;
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
			: undefined;
	const t = teams[opp];

	return (
		<>
			{playoffGame !== undefined ? (
				<span className="lc-tag lc-tag-playoffs">G{playoffGame}</span>
			) : b2b ? (
				<span className="lc-tag" title="Back-to-back">
					B2B
				</span>
			) : null}
			<a
				className="lc-game"
				href={href}
				title={`${home ? "vs" : "@"} ${t?.region ?? ""} ${t?.name ?? ""}`}
			>
				<span className="lc-opp">
					<span className="lc-at">{home ? "" : "@"}</span>
					<Logo t={t} />
					<span className="lc-opp-abbrev d-none d-md-inline">{t?.abbrev}</span>
				</span>
				{final ? (
					<span
						className={clsx("lc-result", won ? "text-success" : "text-danger")}
					>
						{won ? "W" : "L"}
						<span className="d-none d-md-inline">
							{" "}
							{pts}-{oppPts}
							{game.overtimes !== undefined && game.overtimes > 0
								? ` ${game.overtimes > 1 ? game.overtimes : ""}OT`
								: null}
						</span>
					</span>
				) : null}
			</a>
		</>
	);
};

const LeagueNight = ({
	count,
	day,
	mine,
	season,
	teams,
	userTid,
}: {
	count: number;
	day: number | undefined;
	mine: Game | undefined;
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
					: undefined
			}
		>
			<span className="lc-count">
				{count}
				<span className="d-none d-md-inline">
					{" "}
					{helpers.plural("game", count)}
				</span>
			</span>
			{opp !== undefined ? (
				<span className="lc-opp lc-opp-small">
					<span className="lc-at">{mine!.homeTid === userTid ? "" : "@"}</span>
					<Logo t={teams[opp]} />
				</span>
			) : null}
		</a>
	);
};

const Calendar = ({
	games,
	keyDates,
	nbaCalendar,
	season,
	tid,
	today,
	undated,
}: Props) => {
	useTitleBar({
		title: "Calendar",
		dropdown: {
			fields: [
				{ key: "team", options: { type: "teams", all: true }, value: tid },
				{ key: "season", options: { type: "seasons" }, value: season },
			],
			view: "calendar",
		},
	});

	const {
		season: currentSeason,
		teamInfoCache,
		userTid,
	} = useLocal(["season", "teamInfoCache", "userTid"]);

	const months = monthsBetween([
		...games.map((game) => game.date),
		...keyDates.map((keyDate) => keyDate.date),
	]);
	const isCurrentSeason = season === currentSeason;
	const startMonth = defaultMonth(months, today, isCurrentSeason);
	const [chosenMonth, setChosenMonth] = useState(startMonth);

	// A new season starts from its own default month, and today moving into
	// the next month takes the calendar along if it was showing today.
	const prev = useRef({ season, today });
	useEffect(() => {
		const prevToday = prev.current.today;
		if (season !== prev.current.season) {
			setChosenMonth(startMonth);
		} else if (
			today !== undefined &&
			prevToday !== undefined &&
			monthOf(today) !== monthOf(prevToday)
		) {
			setChosenMonth((month) =>
				month === monthOf(prevToday) ? monthOf(today) : month,
			);
		}
		prev.current = { season, today };
	}, [season, startMonth, today]);

	const month =
		chosenMonth !== undefined && months.includes(chosenMonth)
			? chosenMonth
			: startMonth;

	if (month === undefined) {
		return (
			<>
				{typeof tid === "number" ? (
					<MoreLinks type="team" page="calendar" tid={tid} season={season} />
				) : null}
				<p>
					{!nbaCalendar
						? "This league isn't on the NBA calendar."
						: !isCurrentSeason
							? "No dates this season."
							: undated
								? "Dates start next season."
								: "No games scheduled yet."}
				</p>
			</>
		);
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
		if (game.playoffs) {
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
		{ count: number; day: number | undefined; mine: Game | undefined }
	>();
	if (tid === "all") {
		for (const game of games) {
			const night = nights.get(game.date) ?? {
				count: 0,
				day: game.day,
				mine: undefined,
			};
			night.count += 1;
			if (game.homeTid === userTid || game.awayTid === userTid) {
				night.mine = game;
			}
			nights.set(game.date, night);
		}
	}

	const monthIndex = months.indexOf(month);
	const todayMonth = today !== undefined ? monthOf(today) : undefined;

	return (
		<>
			{typeof tid === "number" ? (
				<MoreLinks type="team" page="calendar" tid={tid} season={season} />
			) : (
				<MoreLinks type="schedule" page="calendar" />
			)}

			<div className="lc">
				<div className="lc-head">
					<button
						type="button"
						className="btn btn-light btn-sm lc-arrow"
						disabled={monthIndex <= 0}
						onClick={() => setChosenMonth(months[monthIndex - 1])}
						aria-label="Previous month"
					>
						‹
					</button>
					<h2 className="lc-title">{formatMonth(firstOfMonth(month))}</h2>
					<button
						type="button"
						className="btn btn-light btn-sm lc-arrow"
						disabled={monthIndex >= months.length - 1}
						onClick={() => setChosenMonth(months[monthIndex + 1])}
						aria-label="Next month"
					>
						›
					</button>
					{todayMonth !== undefined &&
					todayMonth !== month &&
					months.includes(todayMonth) ? (
						<button
							type="button"
							className="btn btn-outline-secondary btn-sm ms-auto"
							onClick={() => setChosenMonth(todayMonth)}
						>
							Today
						</button>
					) : null}
				</div>

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
								{formatMonth(firstOfMonth(m), true)}
								{record ? (
									<span className="lc-month-record">
										{record.won}-{record.lost}
									</span>
								) : null}
							</button>
						);
					})}
				</div>

				<table className="lc-grid">
					<thead>
						<tr>
							{WEEKDAYS.map((name) => (
								<th key={name}>
									{name[0]}
									<span className="d-none d-sm-inline">{name.slice(1)}</span>
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
									const game = teamByDate.get(date);
									const night = nights.get(date);
									const keyDate = keyByDate.get(date);
									const home = game !== undefined && game.homeTid === tid;
									return (
										<td
											key={date}
											className={clsx(
												home && "lc-home",
												night?.mine && "lc-mine",
												date === today && "lc-today",
											)}
											title={formatGameDate(date, "full")}
										>
											<span className="lc-num">{Number(date.slice(8))}</span>
											{game && typeof tid === "number" ? (
												<TeamNight
													b2b={teamByDate.has(addDays(date, -1))}
													game={game}
													playoffGame={playoffGameNumbers.get(game.gid)}
													season={season}
													teams={teamInfoCache}
													tid={tid}
												/>
											) : null}
											{night ? (
												<LeagueNight
													count={night.count}
													day={night.day}
													mine={night.mine}
													season={season}
													teams={teamInfoCache}
													userTid={userTid}
												/>
											) : null}
											{keyDate ? (
												<KeyDateLabel keyDate={keyDate} season={season} />
											) : null}
										</td>
									);
								})}
							</tr>
						))}
					</tbody>
				</table>
			</div>
		</>
	);
};

export default Calendar;
