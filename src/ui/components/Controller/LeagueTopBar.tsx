import { memo, useCallback, useEffect, useRef, useState } from "react";
import { useLocal } from "../../util/local.ts";
import { helpers } from "../../util/helpers.ts";
import { ScoreBox } from "../ScoreBox/index.tsx";
import { emitter } from "../Modal.tsx";

// The strip of recent scores under the navbar. Shown or hidden from Global
// Settings (Scores Bar) rather than by a chevron pinned to its own right edge -
// that button cost every browser a permanent reservation at the end of the
// strip, and on iOS it was reserved twice over, which read as a dead gap after
// the last score.

export const LeagueTopBar = memo(() => {
	const { games, lid, liveGameInProgress, showLeagueTopBar } = useLocal([
		"games",
		"lid",
		"liveGameInProgress",
		"showLeagueTopBar",
	]);

	const keepScrollToRightRef = useRef(true);

	const [wrapperElement, setWrapperElement] = useState<HTMLDivElement | null>(
		null,
	);

	const prevGames = useRef<typeof games>([]);

	const games2: typeof games = [];

	const keepScrolledToRightIfNecessary = useCallback(() => {
		if (
			keepScrollToRightRef.current &&
			wrapperElement &&
			wrapperElement.scrollLeft + wrapperElement.offsetWidth <
				wrapperElement.scrollWidth
		) {
			wrapperElement.scrollTo({
				left: wrapperElement.scrollWidth,
			});
		}
	}, [wrapperElement]);

	useEffect(() => {
		return emitter.on("keepScrollToRight", keepScrolledToRightIfNecessary);
	}, [keepScrolledToRightIfNecessary]);

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

		// This triggers for wheel scrolling and click scrolling
		const handleScroll = () => {
			if (
				!wrapperElement ||
				wrapperElement.scrollWidth <= wrapperElement.clientWidth
			) {
				return;
			}

			// Keep track of if we're scrolled to the right or not
			const FUDGE_FACTOR = 50; // Off by a few pixels? That's fine!
			keepScrollToRightRef.current =
				wrapperElement.scrollLeft + wrapperElement.offsetWidth >=
				wrapperElement.scrollWidth - FUDGE_FACTOR;
		};

		wrapperElement.addEventListener("wheel", handleWheel, { passive: false });
		wrapperElement.addEventListener("scroll", handleScroll, { passive: true });

		// This works better than the global "resize" event because it also handles when the div size changes due to other reasons, like the window's scrollbar appearing or disappearing
		const resizeObserver = new ResizeObserver(keepScrolledToRightIfNecessary);
		resizeObserver.observe(wrapperElement);

		return () => {
			wrapperElement.removeEventListener("wheel", handleWheel);
			wrapperElement.removeEventListener("scroll", handleScroll);
			resizeObserver.disconnect();
		};
	}, [keepScrolledToRightIfNecessary, showLeagueTopBar, wrapperElement]);

	// If you take control of an expansion team after the season, the ASG is the only game, and it looks weird to show just it
	const onlyAllStarGame =
		games.length === 1 &&
		games[0]!.teams[0].tid === -1 &&
		games[0]!.teams[1].tid === -2;

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

	// In a new season, start scrolled to right
	if (games2.length <= 1) {
		keepScrollToRightRef.current = true;
	}

	// Keep scrolled to the right, if something besides a scroll event has moved us away (i.e. a game was simmed and added to the list)
	keepScrolledToRightIfNecessary();

	return (
		<div
			className="league-top-bar flex-shrink-0 d-flex overflow-auto small-scrollbar flex-row ps-1 mt-2"
			ref={(element) => {
				// Shit is wild, if I just do ref={setWrapperElement} it somehow breaks scrolling to the right, idk why
				setWrapperElement(element);
			}}
		>
			{games2.map((game, i) => (
				<ScoreBox
					key={game.gid}
					className={`me-2${i === 0 ? " ms-auto" : ""}`}
					game={game}
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
	);
});
