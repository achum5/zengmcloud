import { memo, useEffect, useLayoutEffect, useRef, useState } from "react";
import { useLocal } from "../../util/local.ts";
import { helpers } from "../../util/helpers.ts";
import { ScoreBox } from "../ScoreBox/index.tsx";
import { isAllStarGame } from "../../../common/specialGames.ts";
import { addDays } from "../../../common/calendarDates.ts";
import type { LocalStateUI } from "../../../common/types.ts";
import { TopBarDay } from "./TopBarDay.tsx";

// The strip of recent scores under the navbar. Shown or hidden from Global
// Settings (Scores Bar) rather than by a chevron pinned to its own right edge -
// that button cost every browser a permanent reservation at the end of the
// strip, and on iOS it was reserved twice over, which read as a dead gap after
// the last score.
//
// On the NBA calendar it's the team's calendar in one row instead, kept to
// the classic bar's ways: every day of the season so far through the team's
// next game, from opening night (the team playing or not), bye days too,
// each with the team's game that day if it has one (TopBarDay.tsx), opening
// on its right end with the same two buttons beside it.

type BarGame = LocalStateUI["games"][number];

// Every day from opening night (or the first game, if earlier) to the last
// game shown (the next one, as on the classic bar), with the team's game on
// each - through today, while a game is still to come.
const nightsOf = (
	games: BarGame[],
	today: string | undefined,
	openingNight: string | undefined,
) => {
	const byDate = new Map<string, BarGame>();
	let first: string | undefined;
	let last: string | undefined;
	let upcoming = false;
	for (const game of games) {
		if (game.date === undefined) {
			continue;
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
	if (openingNight !== undefined && openingNight < first) {
		first = openingNight;
	}
	if (upcoming && today !== undefined && today > last) {
		last = today;
	}
	const nights: { date: string; game: BarGame | undefined }[] = [];
	for (let date = first; date <= last; date = addDays(date, 1)) {
		nights.push({ date, game: byDate.get(date) });
	}
	return nights;
};

// The buttons at the bar's right end: yesterday's and today's games.
const DayButtons = ({ height }: { height: number }) => (
	<>
		<a
			className="btn btn-light-bordered d-flex align-items-center me-2 px-1"
			style={{ height }}
			href={helpers.leagueUrl(["daily_schedule", "yesterday"])}
			title="Yesterday's games"
		>
			<span className="glyphicon glyphicon-menu-left" />
		</a>
		<a
			className="btn btn-light-bordered d-flex align-items-center me-2 px-1"
			style={{ height }}
			href={helpers.leagueUrl(["daily_schedule", "today"])}
			title="Today's games"
		>
			<span className="glyphicon glyphicon-menu-right" />
		</a>
	</>
);

export const LeagueTopBar = memo(() => {
	const {
		calendarDate,
		calendarOpeningNight,
		games,
		lid,
		liveGameInProgress,
		mpLiveGames,
		showLeagueTopBar,
	} = useLocal([
		"calendarDate",
		"calendarOpeningNight",
		"games",
		"lid",
		"liveGameInProgress",
		"mpLiveGames",
		"showLeagueTopBar",
	]);

	// (A league mate's game being played live: as it stands.)
	const liveOf = (game: BarGame) =>
		game.teams[0].pts === undefined
			? mpLiveGames.find((live) => live.gid === game.gid)
			: undefined;
	const livePtsOf = (game: BarGame) => liveOf(game)?.score?.pts;

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

	const dated = games.some((game) => game.date !== undefined);

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

	if (!dated) {
		return (
			<div
				className="league-top-bar flex-shrink-0 d-flex overflow-auto small-scrollbar flex-row-reverse mt-2"
				ref={setWrapperElement}
			>
				<div className="d-flex flex-shrink-0 ps-1" ref={contentRef}>
					{games2.map((game) => (
						<ScoreBox
							key={game.gid}
							className="me-2"
							game={game}
							livePts={livePtsOf(game)}
							small
						/>
					))}
					{games2.length > 0 ? <DayButtons height={56} /> : null}
				</div>
			</div>
		);
	}

	const nights = nightsOf(games2, calendarDate, calendarOpeningNight);

	// (The buttons sit beside the scrolling days, not inside them: pinned
	// inside with position: sticky, iPhones sometimes left them blank.)
	return (
		<div className="tbd-bar d-flex">
			<div
				className="league-top-bar league-top-bar-calendar d-flex overflow-auto small-scrollbar flex-row-reverse"
				ref={setWrapperElement}
			>
				<div className="d-flex flex-shrink-0 ps-1" ref={contentRef}>
					<div className="tbd-track d-flex flex-shrink-0">
						{nights.map(({ date, game }) => (
							<TopBarDay
								key={date}
								date={date}
								game={game}
								live={game ? liveOf(game) : undefined}
								today={calendarDate}
							/>
						))}
					</div>
				</div>
			</div>
			<div className="tbd-buttons d-flex align-items-center ps-2">
				<DayButtons height={64} />
			</div>
		</div>
	);
});
