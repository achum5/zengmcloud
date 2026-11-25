import clsx from "clsx";
import { useEffect, useState } from "react";
import { Nav, Navbar } from "react-bootstrap";
import {
	formatGameDate,
	formatMonth,
	formatSeasonSpan,
	weekdayName,
} from "../../../common/calendarDates.ts";
import { PHASE } from "../../../common/constants.ts";
import { helpers } from "../../util/helpers.ts";
import { localActions, useLocal } from "../../util/local.ts";
import { useSimTimes } from "../../util/simTimes.ts";
import { useViewData } from "../../util/viewManager.tsx";
import DropdownLinks from "../DropdownLinks.tsx";
import LogoAndText from "../LogoAndText.tsx";
import PlayMenu from "../PlayMenu.tsx";
import AutoPlayCountdown from "./AutoPlayCountdown.tsx";
import PhaseReadyControl from "./PhaseReadyControl.tsx";
import SyncUploadIndicator from "./SyncUploadIndicator.tsx";
import SyncStatusDot from "./SyncStatusDot.tsx";
import SyncPausedIndicator from "./SyncPausedIndicator.tsx";
import SyncCatchUpIndicator from "./SyncCatchUpIndicator.tsx";
import { LiveScoresStrip } from "../LiveGames.tsx";
import { menuItems } from "../../util/menuItems.tsx";
import type { Phase } from "../../../common/types.ts";
import type { LeagueUrlParts } from "../../router/types.ts";

// Today's date, as a page off a desk calendar: the weekday on its band, the
// day, the month.
const DateTile = ({ date }: { date: string }) => (
	<span className="league-date-tile" aria-hidden="true">
		<span className="league-date-tile-weekday">
			{weekdayName(date).slice(0, 3)}
		</span>
		<span className="league-date-tile-day">{Number(date.slice(8))}</span>
		<span className="league-date-tile-month">{formatMonth(date, true)}</span>
	</span>
);

const PhaseStatusBlock = () => {
	const {
		calendarDate,
		liveGameInProgress,
		phase,
		phaseText,
		season,
		statusText,
	} = useLocal([
		"calendarDate",
		"liveGameInProgress",
		"phase",
		"phaseText",
		"season",
		"statusText",
	]);

	// On the NBA calendar, today's date leads, as a desk calendar's page, and
	// the season is named the NBA's way ("2025-26"). The date holds still while a live
	// game is on screen: the sim is already past it, and the next night - or
	// none, once the Finals are over - would give the result away.
	const [date, setDate] = useState(calendarDate);
	// When auto play sims today, on this device's clock.
	const simTime = useSimTimes().date(date);
	useEffect(() => {
		if (!liveGameInProgress) {
			setDate(calendarDate);
		}
	}, [calendarDate, liveGameInProgress]);

	const urls: Record<Phase, LeagueUrlParts> = {
		[PHASE.EXPANSION_DRAFT]: ["draft"],
		[PHASE.FANTASY_DRAFT]: ["draft"],
		[PHASE.PRESEASON]: ["roster"],
		[PHASE.REGULAR_SEASON]: ["roster"],
		[PHASE.AFTER_TRADE_DEADLINE]: ["roster"],
		[PHASE.PLAYOFFS]: ["playoffs"],
		// Hack because we don't know repeatSeason and draftType, see updatePhase
		[PHASE.DRAFT_LOTTERY]: phaseText.includes("after playoffs")
			? ["draft_scouting"]
			: ["draft_lottery"],
		[PHASE.DRAFT]: ["draft"],
		[PHASE.AFTER_DRAFT]: ["draft_history"],
		[PHASE.RESIGN_PLAYERS]: ["negotiation"],
		[PHASE.FREE_AGENCY]: ["free_agents"],
	};
	const urlParts = urls[phase];

	if (date !== undefined) {
		const seasonPrefix = `${season} `;
		// The season the NBA's way over its phase: "2025-26" / "Regular season".
		const [seasonName, phaseName] = phaseText.startsWith(seasonPrefix)
			? [
					formatSeasonSpan(season),
					helpers.upperCaseFirstLetter(phaseText.slice(seasonPrefix.length)),
				]
			: [phaseText, undefined];
		// The tile opens the night's games; the words, the phase's page. (Only
		// the inner box clips, so the tile and its hover keep the bar's full
		// height: the outer one is 16px shorter and cut the tile's edges off.)
		return (
			<div
				className="dropdown-links navbar-nav flex-shrink-1 text-nowrap"
				style={{ minWidth: 0 }}
			>
				<div
					className="nav-item d-flex align-items-center overflow-hidden"
					style={{ padding: "4px 0 4px 16px" }}
				>
					<a
						href={helpers.leagueUrl(["daily_schedule"])}
						className={clsx("league-date-tile-link", simTime && "sim-time-day")}
						title={`Daily Schedule: ${formatGameDate(date, "full")}`}
						data-sim-time={simTime}
						aria-label="Daily Schedule"
					>
						<DateTile date={date} />
					</a>
					<a
						href={helpers.leagueUrl(urlParts)}
						className="nav-link p-0 overflow-hidden"
						style={{ lineHeight: 1.35 }}
					>
						{liveGameInProgress ? (
							<>
								Live game
								<br />
								in progress
							</>
						) : (
							<>
								<span className="fw-semibold">{seasonName}</span>
								<br />
								{statusText === "Idle" ? phaseName : statusText}
							</>
						)}
						<AutoPlayCountdown />
						<SyncUploadIndicator />
					</a>
				</div>
			</div>
		);
	}

	// Hide phase and status, to prevent revealing that the playoffs has ended, thus spoiling a 3-0/3-1/3-2 finals
	// game. This is needed because game sim happens before the results are displayed in liveGame.
	//
	// The auto-play countdown is NOT one of those: it is a clock, it says nothing
	// about the phase or anyone's result, and how long you have before the next
	// auto-sim is worth knowing precisely while a live game is on screen. It
	// freezes itself for the duration so it cannot leak by disappearing - see
	// AutoPlayCountdown.
	const text = (
		<>
			{liveGameInProgress ? "Live game" : phaseText}
			<br />
			{liveGameInProgress ? "in progress" : statusText}
			<AutoPlayCountdown />
			<SyncUploadIndicator />
		</>
	);

	return (
		<div className="dropdown-links navbar-nav flex-shrink-1 overflow-hidden text-nowrap">
			<div className="nav-item">
				<a
					href={helpers.leagueUrl(urlParts)}
					className="nav-link"
					style={{
						lineHeight: 1.35,
						padding: "9px 0 8px 16px",
					}}
				>
					{text}
				</a>
			</div>
		</div>
	);
};

export const NavBar = ({ updating }: { updating: boolean }) => {
	const {
		lid,
		godMode,
		gold,
		mpLiveGames,
		sidebarOpen,
		spectator,
		playMenuOptions,
		popup,
	} = useLocal([
		"lid",
		"godMode",
		"gold",
		"mpLiveGames",
		"sidebarOpen",
		"spectator",
		"playMenuOptions",
		"popup",
	]);
	const viewInfo = useViewData();

	// Checking lid too helps with some flicker
	const inLeague = viewInfo?.inLeague && lid !== undefined;

	// (Games being played live in the room: a row of their scores under the
	// bar, still part of the sticky header.)
	const liveScores = inLeague && mpLiveGames.length > 0;

	if (popup) {
		return <div />;
	}

	return (
		<Navbar
			bg="light"
			expand="sm"
			sticky="top"
			className={clsx(
				"navbar-border",
				liveScores ? "flex-wrap pb-0 pb-xl-2" : "flex-nowrap",
			)}
		>
			<div className="container-fluid flex-nowrap">
				<button
					className="navbar-toggler me-2 d-block"
					onClick={() => {
						localActions.setSidebarOpen(!sidebarOpen);
					}}
					type="button"
					aria-label="Toggle navigation"
				>
					<span className="navbar-toggler-icon" />
				</button>
				<LogoAndText gold={gold} inLeague={inLeague} updating={updating} />
				{inLeague ? (
					<Nav navbar>
						<PlayMenu
							lid={lid}
							spectator={spectator}
							options={playMenuOptions}
						/>
					</Nav>
				) : null}
				{inLeague ? <PhaseStatusBlock /> : null}
				{inLeague ? <SyncStatusDot /> : null}
				{inLeague ? <SyncCatchUpIndicator /> : null}
				{inLeague ? <SyncPausedIndicator /> : null}
				{liveScores ? (
					<LiveScoresStrip className="live-scores-inline d-none d-xl-flex ms-3" />
				) : null}
				<div className="flex-grow-1" />
				{inLeague ? <PhaseReadyControl /> : null}
				<div className="d-none d-sm-flex">
					<DropdownLinks
						godMode={godMode}
						inLeague={inLeague}
						lid={lid}
						menuItems={menuItems.filter(
							(menuItem) => !menuItem.commandPaletteOnly,
						)}
					/>
				</div>
				{inLeague ? (
					<Nav navbar>
						<Nav.Link
							href={helpers.leagueUrl(["sportsbook"])}
							aria-label="Sportsbook"
							title="Sportsbook"
							className="fw-bold"
						>
							$
						</Nav.Link>
					</Nav>
				) : null}
			</div>
			{liveScores ? (
				<LiveScoresStrip className="live-scores-row d-flex d-xl-none" />
			) : null}
		</Navbar>
	);
};
