import clsx from "clsx";
import { memo, useEffect, useLayoutEffect, useRef, useState } from "react";
import { useLocal } from "../../util/local.ts";
import { helpers } from "../../util/helpers.ts";
import { ScoreBox } from "../ScoreBox/index.tsx";
import { isAllStarGame } from "../../../common/specialGames.ts";
import {
	addDays,
	formatGameDate,
	formatMonth,
	weekdayOf,
} from "../../../common/calendarDates.ts";
import type { LocalStateUI } from "../../../common/types.ts";
import { PHASE } from "../../../common/constants.ts";

// The strip of recent scores under the navbar. Shown or hidden from Global
// Settings (Scores Bar) rather than by a chevron pinned to its own right edge -
// that button cost every browser a permanent reservation at the end of the
// strip, and on iOS it was reserved twice over, which read as a dead gap after
// the last score.
//
// On the NBA calendar it goes night by night instead: every night of the
// season so far, today, and the coming nights, each with its weekday and
// date, and the team's game that night if it has one.

type BarGame = LocalStateUI["games"][number];

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

// How many nights past today to show. The worker sends the games in them.
const NIGHTS_AHEAD = 5;

// A night as a page off a desk calendar, like today's date in the navbar.
const DayStub = ({
	date,
	game,
	today,
}: {
	date: string;
	game: boolean;
	today: string | undefined;
}) => (
	<div
		className={clsx(
			"ltb-stub",
			game && "ltb-stub-game",
			date === today && "ltb-stub-today",
			today !== undefined && date > today && "ltb-stub-future",
		)}
		title={`${date === today ? "Today, " : ""}${formatGameDate(date, "full")}`}
	>
		<span className="ltb-stub-weekday">{WEEKDAYS[weekdayOf(date)]}</span>
		<span className="ltb-stub-day">{Number(date.slice(8))}</span>
		<span className="ltb-stub-month">{formatMonth(date, true)}</span>
	</div>
);

// Every night from the first game to the coming nights, with the team's game
// on each. In the playoffs the coming nights are the team's series' planned
// ones; with none (between rounds, or out), they stop at the next game rather
// than showing nights off that aren't.
const nightsOf = (
	games: BarGame[],
	today: string | undefined,
	playoffs: boolean,
) => {
	const byDate = new Map<string, BarGame>();
	let first: string | undefined;
	let last: string | undefined;
	let upcoming = false;
	let planned = false;
	for (const game of games) {
		if (game.date === undefined) {
			continue;
		}
		if (game.planned) {
			planned = true;
		}
		byDate.set(game.date, game);
		if (first === undefined || game.date < first) {
			first = game.date;
		}
		if (last === undefined || game.date > last) {
			last = game.date;
		}
		if (game.teams[0].pts === undefined) {
			upcoming = true;
		}
	}
	if (first === undefined || last === undefined) {
		return [];
	}
	if (today !== undefined) {
		const end =
			upcoming && (!playoffs || planned) ? addDays(today, NIGHTS_AHEAD) : today;
		if (end > last) {
			last = end;
		}
	}
	const nights: { date: string; game: BarGame | undefined }[] = [];
	for (let date = first; date <= last; date = addDays(date, 1)) {
		nights.push({ date, game: byDate.get(date) });
	}
	return nights;
};

export const LeagueTopBar = memo(() => {
	const {
		calendarDate,
		games,
		lid,
		liveGameInProgress,
		mpLiveGames,
		phase,
		showLeagueTopBar,
	} = useLocal([
		"calendarDate",
		"games",
		"lid",
		"liveGameInProgress",
		"mpLiveGames",
		"phase",
		"showLeagueTopBar",
	]);

	// (A league mate's game being played live: its score as it stands.)
	const livePtsOf = (game: BarGame) =>
		game.teams[0].pts === undefined
			? mpLiveGames.find((live) => live.gid === game.gid)?.score?.pts
			: undefined;

	const [wrapperElement, setWrapperElement] = useState<HTMLDivElement | null>(
		null,
	);

	const prevGames = useRef<typeof games>([]);

	const games2: typeof games = [];

	const contentRef = useRef<HTMLDivElement>(null);
	const prevContentWidth = useRef(0);

	// The wrapper is flex-row-reverse, so scrollLeft is 0 when scrolled all the way to the right and negative when scrolled to the left. That means the browser keeps it scrolled to the right automatically when games are added or the wrapper is resized. But if the user has scrolled to the left, we need to adjust for new games, otherwise the visible games would shift to the left.
	useLayoutEffect(() => {
		const contentWidth = contentRef.current?.offsetWidth ?? 0;
		const diff = contentWidth - prevContentWidth.current;
		prevContentWidth.current = contentWidth;

		if (!wrapperElement || diff === 0) {
			return;
		}

		const FUDGE_FACTOR = 50; // Off by a few pixels? That's fine!
		wrapperElement.scrollTo({
			left:
				wrapperElement.scrollLeft >= -FUDGE_FACTOR
					? 0
					: wrapperElement.scrollLeft - diff,
		});
	});

	// On the NBA calendar, today stays in view when the coming nights fill a
	// narrow screen, so a phone opens on today rather than next week.
	const dated = games.some((game) => game.date !== undefined);
	useLayoutEffect(() => {
		if (!wrapperElement || !dated) {
			return;
		}
		const showToday = () => {
			const todayElement = wrapperElement.querySelector(".ltb-stub-today");
			if (!todayElement) {
				return;
			}
			const wrapperLeft = wrapperElement.getBoundingClientRect().left;
			const todayLeft = todayElement.getBoundingClientRect().left;
			if (todayLeft < wrapperLeft) {
				wrapperElement.scrollBy({ left: todayLeft - wrapperLeft - 8 });
			}
		};
		showToday();
		window.addEventListener("resize", showToday);
		return () => window.removeEventListener("resize", showToday);
	}, [calendarDate, dated, wrapperElement]);

	useEffect(() => {
		if (!wrapperElement || !showLeagueTopBar) {
			return;
		}

		const handleWheel = (event: WheelEvent) => {
			if (
				!wrapperElement ||
				wrapperElement.scrollWidth <= wrapperElement.clientWidth ||
				event.altKey ||
				event.ctrlKey ||
				event.metaKey ||
				event.shiftKey
			) {
				return;
			}

			// We're scrolling within the bar, not within the whole page
			event.preventDefault();

			const leagueTopBarPosition = wrapperElement.scrollLeft;

			wrapperElement.scrollTo({
				// Normal mouse wheels are just deltaY, but trackpads (such as on Mac) can include both, and I think there's no way to tell if this event came from a device supporting two dimensional scrolling or not.
				left: leagueTopBarPosition + 2 * (event.deltaX + event.deltaY),
			});
		};

		wrapperElement.addEventListener("wheel", handleWheel, { passive: false });

		return () => {
			wrapperElement.removeEventListener("wheel", handleWheel);
		};
	}, [showLeagueTopBar, wrapperElement]);

	// If you take control of an expansion team after the season, the ASG is the only game, and it looks weird to show just it
	const onlyAllStarGame =
		games.length === 1 &&
		isAllStarGame(games[0]!.teams[0].tid, games[0]!.teams[1].tid);

	// Turned off for this device, or nothing worth showing: keep the spacer so
	// the page below sits where it always did, and render no strip at all.
	if (
		!showLeagueTopBar ||
		lid === undefined ||
		games.length === 0 ||
		onlyAllStarGame
	) {
		return <div className="mt-2" />;
	}

	// Don't show any new games if liveGameInProgress
	if (!liveGameInProgress) {
		prevGames.current = games;
	}

	// Show only the first upcoming game
	for (const game of prevGames.current) {
		games2.push(game);
		if (game.teams[0].pts === undefined) {
			break;
		}
	}

	const nights = dated
		? nightsOf(prevGames.current, calendarDate, phase === PHASE.PLAYOFFS)
		: [];

	return (
		<div
			className="league-top-bar flex-shrink-0 d-flex overflow-auto small-scrollbar flex-row-reverse mt-2"
			ref={setWrapperElement}
		>
			<div className="d-flex flex-shrink-0 ps-1" ref={contentRef}>
				{nights.length > 0
					? nights.map(({ date, game }) => (
							<div
								key={date}
								className={clsx(
									"ltb-night me-2",
									date === calendarDate && "ltb-night-today",
								)}
							>
								<DayStub date={date} game={!!game} today={calendarDate} />
								{game ? (
									<div
										className={clsx(game.ifNecessary && "ltb-maybe")}
										title={game.ifNecessary ? "If necessary" : undefined}
									>
										<ScoreBox game={game} livePts={livePtsOf(game)} small />
									</div>
								) : null}
							</div>
						))
					: games2.map((game) => (
							<ScoreBox
								key={game.gid}
								className="me-2"
								game={game}
								livePts={livePtsOf(game)}
								small
							/>
						))}
				{games2.length > 0 ? (
					<>
						<a
							className="btn btn-light-bordered d-flex align-items-center me-2 px-1"
							style={{ height: 56 }}
							href={helpers.leagueUrl(["daily_schedule", "yesterday"])}
							title="Yesterday's games"
						>
							<span className="glyphicon glyphicon-menu-left" />
						</a>
						<a
							className="btn btn-light-bordered d-flex align-items-center me-2 px-1"
							style={{ height: 56 }}
							href={helpers.leagueUrl(["daily_schedule", "today"])}
							title="Today's games"
						>
							<span className="glyphicon glyphicon-menu-right" />
						</a>
					</>
				) : null}
			</div>
		</div>
	);
});
