import clsx from "clsx";
import { memo, type ReactNode } from "react";
import { useLocal } from "../../util/local.ts";
import { helpers } from "../../util/helpers.ts";
import { TeamLogoInline } from "../TeamLogoInline.tsx";
import { NationalTag } from "../NationalTag.tsx";
import getWinner from "../../../common/getWinner.ts";
import {
	isAllStarGame,
	isTradeDeadline,
} from "../../../common/specialGames.ts";
import { formatGameDate, weekdayOf } from "../../../common/calendarDates.ts";
import type { LocalStateUI } from "../../../common/types.ts";

// ONE DAY OF THE SCORES BAR on the NBA calendar: the strip is the team's
// calendar laid out in a single row, every day, bye days too. Like the team
// calendar (components/MonthCalendar.tsx) - the day on top ("SAT 11/15"),
// then the opponent ("@" on the road) and the result as a W or L coin with
// the score, home nights filled black, today outlined red.

type BarGame = LocalStateUI["games"][number];

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

// The favorite and its line, "BOS -4.5" - or "PK".
const lineText = (game: BarGame, abbrevOf: (tid: number) => string) => {
	const spread = game.spread;
	if (spread === undefined) {
		return;
	}
	if (spread === 0) {
		return "PK";
	}
	const favorite = spread > 0 ? game.teams[0].tid : game.teams[1].tid;
	return `${abbrevOf(favorite)} -${Math.abs(spread).toLocaleString("en-US", {
		maximumFractionDigits: 1,
	})}`;
};

export const TopBarDay = memo(
	({
		date,
		game,
		livePts,
		today,
	}: {
		date: string;
		game: BarGame | undefined;
		// A game being played live: the score as it stands (home, away).
		livePts?: [number, number];
		today: string | undefined;
	}) => {
		const { season, teamInfoCache, userTid } = useLocal([
			"season",
			"teamInfoCache",
			"userTid",
		]);

		const isToday = date === today;
		const past = today !== undefined && date < today;
		const head = (
			<div className="tbd-head">
				<span className="tbd-weekday">{WEEKDAYS[weekdayOf(date)]}</span>
				<span className="tbd-num">
					{Number(date.slice(5, 7))}/{Number(date.slice(8))}
				</span>
			</div>
		);
		const title = `${isToday ? "Today, " : ""}${formatGameDate(date, "full")}`;

		if (!game) {
			return (
				<div
					className={clsx(
						"tbd tbd-off",
						isToday && "tbd-today",
						past && "tbd-past",
					)}
					title={title}
				>
					{head}
					<div className="tbd-body" />
				</div>
			);
		}

		const abbrevOf = (tid: number) => teamInfoCache[tid]?.abbrev ?? "";
		const allStar = isAllStarGame(game.teams[0].tid, game.teams[1].tid);
		const deadline = isTradeDeadline(game.teams[0].tid, game.teams[1].tid);
		const userIndex = game.teams[1].tid === userTid ? 1 : 0;
		const home = !allStar && !deadline && userIndex === 0;
		const opp = game.teams[userIndex === 0 ? 1 : 0];
		const winner = getWinner(game.teams);
		const final = winner !== undefined;
		const live = !final && livePts !== undefined;

		const href = final
			? helpers.leagueUrl([
					"game_log",
					allStar
						? "special"
						: `${abbrevOf(game.teams[userIndex].tid)}_${game.teams[userIndex].tid}`,
					season,
					game.gid,
				])
			: helpers.leagueUrl(["schedule"]);

		let matchup: ReactNode;
		if (allStar || deadline) {
			matchup = (
				<span className="tbd-special">{allStar ? "All-Star" : "Deadline"}</span>
			);
		} else {
			const branding = teamInfoCache[opp.tid];
			matchup = (
				<>
					<span className="tbd-at">{home ? "vs" : "@"}</span>
					<TeamLogoInline
						imgURL={branding?.imgURL}
						imgURLSmall={branding?.imgURLSmall}
						includePlaceholderIfNoLogo
						size={16}
						className="tbd-logo"
					/>
					<span className="tbd-abbrev">{branding?.abbrev}</span>
				</>
			);
		}

		let result: ReactNode = null;
		if (final) {
			const pts = game.teams[userIndex].pts!;
			const oppPts = game.teams[userIndex === 0 ? 1 : 0].pts!;
			const wl = allStar
				? undefined
				: winner === -1
					? "T"
					: winner === userIndex
						? "W"
						: "L";
			const overtimes = helpers.overtimeText(game.overtimes, game.numPeriods);
			result = (
				<>
					{wl ? (
						<span
							className={clsx(
								"tbd-wl",
								wl === "W" ? "tbd-w" : wl === "L" ? "tbd-l" : "tbd-t",
							)}
						>
							{wl}
						</span>
					) : null}
					<span className="tbd-score">
						{allStar
							? `${game.teams[1].pts}-${game.teams[0].pts}`
							: `${pts}-${oppPts}`}
						{overtimes ? <span className="tbd-ot"> {overtimes}</span> : null}
					</span>
				</>
			);
		} else if (live) {
			result = (
				<span className="tbd-live">
					<span className="tbd-live-dot" />
					{livePts[userIndex]}-{livePts[userIndex === 0 ? 1 : 0]}
				</span>
			);
		} else if (game.ifNecessary) {
			result = <span className="tbd-note">If nec.</span>;
		} else if (game.national) {
			result = <NationalTag className="tbd-national" tag={game.national} />;
		} else {
			const line = lineText(game, abbrevOf);
			if (line) {
				result = (
					<span className="tbd-note" title="Point spread">
						{line}
					</span>
				);
			}
		}

		return (
			<a
				className={clsx(
					"tbd",
					home && "tbd-home",
					isToday && "tbd-today",
					past && "tbd-past",
					game.ifNecessary && "tbd-maybe",
				)}
				href={href}
				title={`${title}${
					allStar || deadline
						? ""
						: `, ${home ? "vs" : "@"} ${teamInfoCache[opp.tid]?.region ?? ""} ${
								teamInfoCache[opp.tid]?.name ?? ""
							}`
				}${game.ifNecessary ? " (if necessary)" : ""}`}
			>
				{head}
				{/* (A game still to come with nothing under it sits centered.) */}
				<div className="tbd-body">
					<div className="tbd-matchup">{matchup}</div>
					{result ? <div className="tbd-result">{result}</div> : null}
				</div>
			</a>
		);
	},
);
