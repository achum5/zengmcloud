import clsx from "clsx";
import { useEffect, useRef, useState } from "react";
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
import { MonthCalendar } from "../MonthCalendar.tsx";

// THE NIGHTS ACROSS THE TOP, as NBA.com and ESPN have them: the month and a
// calendar to jump anywhere on the left, then a week of nights, the one shown
// underlined. Without the NBA calendar there are no dates, so it is a run of
// day numbers instead.

export type Night = {
	day: number;
	date: string | undefined;
	numGames: number;
	playoffs: boolean;
	deadline: boolean;
};

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const SPAN = 7;

const dayUrl = (season: number, day: number) =>
	helpers.leagueUrl(["daily_schedule", season, day]);

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
		{dir === "prev" ? "‹" : "›"}
	</button>
);

// The month grid, under the calendar button.
const MonthDropdown = ({
	calendar,
	date,
	season,
}: {
	calendar: {
		games: CalendarGame[];
		keyDates: CalendarKeyDate[];
		today: string | undefined;
	};
	date: string | undefined;
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
				<span className="lh-month-label">
					{date ? formatMonth(date) : null}
				</span>
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
		today: string | undefined;
	};
	date: string;
	day: number;
	nights: Night[];
	season: number;
}) => {
	// The week shown, Sunday first: the shown night's, until the arrows move
	// it.
	const weekOf = (d: string) => addDays(d, -weekdayOf(d));
	const [start, setStart] = useState(() => weekOf(date));
	useEffect(() => {
		setStart(weekOf(date));
	}, [date]);

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
	const dated = nights.filter((night) => night.date !== undefined);
	const first = dated[0]?.date;
	const last = dated.at(-1)?.date;

	const dates = Array.from({ length: SPAN }, (_, i) => addDays(start, i));
	return (
		<div className="lh-week">
			<MonthDropdown calendar={calendar} date={date} season={season} />
			<div className="lh-week-days">
				<Arrow
					dir="prev"
					disabled={first !== undefined && start <= first}
					onClick={() => setStart((s) => addDays(s, -SPAN))}
				/>
				{dates.map((d) => {
					const night = byDate.get(d);
					const selected = night?.day === day;
					const today = calendar.today === d;
					const inner = (
						<>
							<span className="lh-week-top">{WEEKDAYS[weekdayOf(d)]}</span>
							<span className="lh-week-num">{Number(d.slice(8, 10))}</span>
						</>
					);
					return night && night.numGames > 0 ? (
						<a
							key={d}
							className={clsx("lh-week-day", { selected, today })}
							href={dayUrl(season, night.day)}
							title={`${night.numGames} ${night.numGames === 1 ? "game" : "games"}`}
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
				<Arrow
					dir="next"
					disabled={last !== undefined && addDays(start, SPAN - 1) >= last}
					onClick={() => setStart((s) => addDays(s, SPAN))}
				/>
			</div>
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
	// Seven nights with the shown one in the middle, until the arrows move
	// them.
	const startFor = (i: number) =>
		Math.max(0, Math.min(i - Math.floor(SPAN / 2), list.length - SPAN));
	const [start, setStart] = useState(() => startFor(index));
	useEffect(() => {
		setStart(startFor(index));
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [index, list.length]);

	const shown = list.slice(start, start + SPAN);
	const playoffs = list[index]?.playoffs;
	return (
		<div className="lh-week">
			<div className="lh-month-wrap">
				<span className="lh-month-button lh-month-static">
					<span className="lh-month-label">
						{season} {playoffs ? "playoffs" : "season"}
					</span>
				</span>
			</div>
			<div className="lh-week-days">
				<Arrow
					dir="prev"
					disabled={start <= 0}
					onClick={() => setStart((s) => Math.max(0, s - SPAN))}
				/>
				{shown.map((night) => (
					<a
						key={night.day}
						className={clsx("lh-week-day", {
							selected: night.day === day,
							today: night.day === todayDay,
						})}
						href={dayUrl(season, night.day)}
						title={`${night.numGames} ${night.numGames === 1 ? "game" : "games"}`}
					>
						<span className="lh-week-top">Day</span>
						<span className="lh-week-num">{night.day}</span>
					</a>
				))}
				{Array.from({ length: SPAN - shown.length }, (_, i) => (
					<span key={`pad${i}`} className="lh-week-day lh-week-pad" />
				))}
				<Arrow
					dir="next"
					disabled={start + SPAN >= list.length}
					onClick={() =>
						setStart((s) => Math.min(Math.max(0, list.length - SPAN), s + SPAN))
					}
				/>
			</div>
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
