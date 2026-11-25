import clsx from "clsx";
import {
	useEffect,
	useLayoutEffect,
	useRef,
	useState,
	type ReactNode,
} from "react";
import {
	addDays,
	formatMonth,
	weekdayOf,
} from "../../../common/calendarDates.ts";
import type {
	CalendarGame,
	CalendarKeyDate,
} from "../../../worker/views/calendar.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { MonthCalendar } from "../MonthCalendar.tsx";

// THE NIGHTS ACROSS THE TOP, as NBA.com and ESPN have them: the month and a
// calendar to jump anywhere on the left, then a week of nights, the one shown
// filled in - swiped a week at a time, or paged with the arrows. Without the
// NBA calendar there are no dates, so it is a run of day numbers instead.

export type Night = {
	day: number;
	date: string | undefined;
	numGames: number;
	playoffs: boolean;
	deadline: boolean;
	// The user's team's game that night.
	mine?: { home: boolean; opp: number };
};

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const SPAN = 7;

const dayUrl = (season: number, day: number) =>
	helpers.leagueUrl(["daily_schedule", season, day]);

// Under the date: how many games.
const Games = ({ night }: { night: Night | undefined }) => (
	<span className="lh-week-games">
		{night && night.numGames > 0
			? `${night.numGames} ${night.numGames === 1 ? "game" : "games"}`
			: null}
	</span>
);

// The nights the user's team plays: its little logo in the corner.
const Mine = ({ night }: { night: Night | undefined }) => {
	const { teamInfoCache, userTid } = useLocal(["teamInfoCache", "userTid"]);
	if (!night?.mine) {
		return null;
	}
	const info = teamInfoCache[userTid];
	const logo = info?.imgURLSmall ?? info?.imgURL;
	return logo ? <img className="lh-week-logo" src={logo} alt="" /> : null;
};

const useNightTitle = () => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	return (night: Night) => {
		const games = `${night.numGames} ${night.numGames === 1 ? "game" : "games"}`;
		if (!night.mine) {
			return games;
		}
		const opp = teamInfoCache[night.mine.opp];
		return `${games}, yours ${night.mine.home ? "vs" : "@"} ${opp?.region ?? ""} ${opp?.name ?? ""}`;
	};
};

const Arrow = ({
	dir,
	disabled,
	onClick,
}: {
	dir: "prev" | "next";
	disabled?: boolean;
	onClick: () => void;
}) => (
	<button
		type="button"
		className="lh-week-arrow"
		disabled={disabled}
		onClick={onClick}
		aria-label={dir === "prev" ? "Earlier" : "Later"}
	>
		<svg viewBox="0 0 16 16" width="16" height="16" aria-hidden>
			<path
				d={dir === "prev" ? "M10 3 5 8l5 5" : "m6 3 5 5-5 5"}
				fill="none"
				stroke="currentColor"
				strokeWidth="2"
				strokeLinecap="round"
				strokeLinejoin="round"
			/>
		</svg>
	</button>
);

// Pages of seven nights side by side, a swipe (or an arrow) a page: which
// one is showing, and the way to another. `page`: the shown night's, gone
// back to whenever another night is shown.
const usePager = (count: number, page: number, day: number) => {
	const ref = useRef<HTMLDivElement>(null);
	const [shown, setShown] = useState(page);
	useLayoutEffect(() => {
		const track = ref.current;
		if (track) {
			track.scrollLeft = page * track.clientWidth;
		}
		setShown(page);
	}, [page, count, day]);
	const onScroll = () => {
		const track = ref.current;
		if (track && track.clientWidth > 0) {
			setShown(Math.round(track.scrollLeft / track.clientWidth));
		}
	};
	const go = (by: number) => {
		const track = ref.current;
		if (track) {
			track.scrollBy({ left: by * track.clientWidth, behavior: "smooth" });
		}
	};
	return { go, onScroll, ref, shown };
};

const Track = ({
	children,
	count,
	pager,
}: {
	children: ReactNode;
	count: number;
	pager: ReturnType<typeof usePager>;
}) => (
	<div className="lh-week-days">
		<Arrow
			dir="prev"
			disabled={pager.shown <= 0}
			onClick={() => pager.go(-1)}
		/>
		<div className="lh-week-track" ref={pager.ref} onScroll={pager.onScroll}>
			{children}
		</div>
		<Arrow
			dir="next"
			disabled={pager.shown >= count - 1}
			onClick={() => pager.go(1)}
		/>
	</div>
);

// The month grid, under the calendar button.
const MonthDropdown = ({
	calendar,
	date,
	label,
	season,
}: {
	calendar: {
		games: CalendarGame[];
		keyDates: CalendarKeyDate[];
		nightDays?: Record<string, number>;
		today: string | undefined;
	};
	date: string | undefined;
	label: string;
	season: number;
}) => {
	const [open, setOpen] = useState(false);
	const ref = useRef<HTMLDivElement>(null);
	useEffect(() => {
		if (!open) {
			return;
		}
		const close = (event: MouseEvent) => {
			if (ref.current && !ref.current.contains(event.target as Node)) {
				setOpen(false);
			}
		};
		document.addEventListener("mousedown", close);
		return () => {
			document.removeEventListener("mousedown", close);
		};
	}, [open]);
	// (A night picked in it: closed again.)
	useEffect(() => {
		setOpen(false);
	}, [date]);

	return (
		<div className="lh-month-wrap" ref={ref}>
			<button
				type="button"
				className="lh-month-button"
				aria-expanded={open}
				title="Calendar"
				onClick={() => setOpen((x) => !x)}
			>
				<span className="lh-month-label">{label}</span>
				<svg
					className="lh-month-icon"
					viewBox="0 0 16 16"
					width="16"
					height="16"
					aria-hidden
				>
					<rect
						x="1.5"
						y="2.5"
						width="13"
						height="12"
						rx="1.5"
						fill="none"
						stroke="currentColor"
					/>
					<path d="M1.5 6h13" stroke="currentColor" />
					<path d="M5 1v3M11 1v3" stroke="currentColor" strokeLinecap="round" />
					<rect x="4" y="8" width="2" height="2" fill="currentColor" />
					<rect x="7" y="8" width="2" height="2" fill="currentColor" />
					<rect x="10" y="8" width="2" height="2" fill="currentColor" />
					<rect x="4" y="11" width="2" height="2" fill="currentColor" />
				</svg>
				<span className="lh-caret" />
			</button>
			{open ? (
				<div className="lh-month-pop">
					<MonthCalendar
						compact
						games={calendar.games}
						keyDates={calendar.keyDates}
						nightDays={calendar.nightDays}
						season={season}
						selected={date}
						tid="all"
						today={calendar.today}
					/>
				</div>
			) : null}
		</div>
	);
};

// Sunday to Saturday.
const weekOf = (d: string) => addDays(d, -weekdayOf(d));

const DatedStrip = ({
	calendar,
	date,
	day,
	nights,
	season,
}: {
	calendar: {
		games: CalendarGame[];
		keyDates: CalendarKeyDate[];
		nightDays?: Record<string, number>;
		today: string | undefined;
	};
	date: string;
	day: number;
	nights: Night[];
	season: number;
}) => {
	// Each date's night - the one with games, where a trade deadline shares
	// its date with them.
	const byDate = new Map<string, Night>();
	for (const night of nights) {
		if (night.date === undefined) {
			continue;
		}
		const have = byDate.get(night.date);
		if (!have || (have.numGames === 0 && night.numGames > 0)) {
			byDate.set(night.date, night);
		}
	}

	// Every week of the season, the shown night's to start.
	const dated = nights.flatMap((night) =>
		night.date === undefined ? [] : [night.date],
	);
	const first = weekOf(dated.reduce((a, b) => (b < a ? b : a), date));
	const last = weekOf(dated.reduce((a, b) => (b > a ? b : a), date));
	const weeks: string[] = [];
	for (let week = first; week <= last; week = addDays(week, SPAN)) {
		weeks.push(week);
	}
	const pager = usePager(weeks.length, weeks.indexOf(weekOf(date)), day);
	const nightTitle = useNightTitle();

	// The month of the week in view: the shown night's, if it's in it.
	const inView = weeks[pager.shown] ?? weekOf(date);
	const monthDate =
		weekOf(date) === inView ? date : addDays(inView, Math.floor(SPAN / 2));

	return (
		<div className="lh-week">
			<MonthDropdown
				calendar={calendar}
				date={date}
				label={formatMonth(monthDate)}
				season={season}
			/>
			<Track count={weeks.length} pager={pager}>
				{weeks.map((week) => (
					<div key={week} className="lh-week-page">
						{Array.from({ length: SPAN }, (_, i) => {
							const d = addDays(week, i);
							const night = byDate.get(d);
							const selected = night?.day === day;
							const today = calendar.today === d;
							const inner = (
								<>
									<span className="lh-week-top">{WEEKDAYS[weekdayOf(d)]}</span>
									<span className="lh-week-num">{Number(d.slice(8, 10))}</span>
									<Games night={night} />
									<Mine night={night} />
								</>
							);
							// (A day still to come with no games is a day to open too.)
							return night ? (
								<a
									key={d}
									className={clsx(
										"lh-week-day",
										night.numGames === 0 && "lh-week-off",
										{ selected, today },
									)}
									href={dayUrl(season, night.day)}
									title={night.numGames > 0 ? nightTitle(night) : "No games"}
								>
									{inner}
								</a>
							) : (
								<span
									key={d}
									className={clsx("lh-week-day lh-week-off", { today })}
								>
									{inner}
								</span>
							);
						})}
					</div>
				))}
			</Track>
		</div>
	);
};

const DayNumberStrip = ({
	day,
	nights,
	season,
	todayDay,
}: {
	day: number;
	nights: Night[];
	season: number;
	todayDay: number | undefined;
}) => {
	const list = nights.filter((night) => night.numGames > 0);
	const index = Math.max(
		0,
		list.findIndex((night) => night.day === day),
	);
	// Seven nights a page, counted back from the last, where the season is
	// in the playoffs (the first page the short one).
	const short = list.length % SPAN;
	const pages: Night[][] = short > 0 ? [list.slice(0, short)] : [];
	for (let i = short; i < list.length; i += SPAN) {
		pages.push(list.slice(i, i + SPAN));
	}
	const pager = usePager(
		pages.length,
		index < short
			? 0
			: Math.floor((index - short) / SPAN) + (short > 0 ? 1 : 0),
		day,
	);
	const playoffs = (pages[pager.shown] ?? pages[0])?.[0]?.playoffs;
	const nightTitle = useNightTitle();

	return (
		<div className="lh-week">
			<div className="lh-month-wrap">
				<span className="lh-month-button lh-month-static">
					<span className="lh-month-label">
						{season} {playoffs ? "playoffs" : "season"}
					</span>
				</span>
			</div>
			<Track count={pages.length} pager={pager}>
				{pages.map((page) => (
					<div key={page[0]!.day} className="lh-week-page">
						{Array.from({ length: SPAN - page.length }, (_, i) => (
							<span key={`pad${i}`} className="lh-week-day lh-week-pad" />
						))}
						{page.map((night) => (
							<a
								key={night.day}
								className={clsx("lh-week-day", {
									selected: night.day === day,
									today: night.day === todayDay,
								})}
								href={dayUrl(season, night.day)}
								title={nightTitle(night)}
							>
								<span className="lh-week-top">Day</span>
								<span className="lh-week-num">{night.day}</span>
								<Games night={night} />
								<Mine night={night} />
							</a>
						))}
					</div>
				))}
			</Track>
		</div>
	);
};

export const WeekStrip = ({
	calendar,
	date,
	day,
	nights,
	season,
	todayDay,
}: {
	calendar:
		| {
				games: CalendarGame[];
				keyDates: CalendarKeyDate[];
				today: string | undefined;
		  }
		| undefined;
	date: string | undefined;
	day: number;
	nights: Night[];
	season: number;
	todayDay: number | undefined;
}) => {
	if (nights.length === 0) {
		return null;
	}
	if (calendar && date !== undefined) {
		return (
			<DatedStrip
				calendar={calendar}
				date={date}
				day={day}
				nights={nights}
				season={season}
			/>
		);
	}
	return (
		<DayNumberStrip
			day={day}
			nights={nights}
			season={season}
			todayDay={todayDay}
		/>
	);
};
