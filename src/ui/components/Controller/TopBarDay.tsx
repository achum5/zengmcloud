import clsx from "clsx";
import { memo, type ReactNode } from "react";
import { useLocal } from "../../util/local.ts";
import { helpers } from "../../util/helpers.ts";
import { TeamLogoInline } from "../TeamLogoInline.tsx";
import { NationalTag } from "../NationalTag.tsx";
import { statusLines } from "../LiveGames.tsx";
import getWinner from "../../../common/getWinner.ts";
import {
	isAllStarGame,
	isTradeDeadline,
} from "../../../common/specialGames.ts";
import { formatGameDate, weekdayOf } from "../../../common/calendarDates.ts";
import type { LocalStateUI, MpLiveGame } from "../../../common/types.ts";

// ONE DAY OF THE SCORES BAR on the NBA calendar: the strip is the team's
// calendar laid out in a single row, every day, bye days too. Each game is a
// scoreboard cell like the networks': the day and where the game stands on
// top ("SAT 11/15", the W or L coin once it's over, the quarter and clock while
// it's live, the network on a national game), then both teams on their own
// rows, away over home, logo, abbreviation, score. Every game cell plain,
// today outlined red.

type BarGame = LocalStateUI["games"][number];

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

// The favorite's line, "-4.5", for its row.
const lineText = (spread: number) =>
	`-${Math.abs(spread).toLocaleString("en-US", {
		maximumFractionDigits: 1,
	})}`;

export const TopBarDay = memo(
	({
		date,
		game,
		live,
		today,
	}: {
		date: string;
		game: BarGame | undefined;
		// A league mate's game being played live, as it stands.
		live?: MpLiveGame;
		today: string | undefined;
	}) => {
		const { season, teamInfoCache, userTid } = useLocal([
			"season",
			"teamInfoCache",
			"userTid",
		]);

		const isToday = date === today;
		const past = today !== undefined && date < today;
		const weekday = WEEKDAYS[weekdayOf(date)];
		const dayNum = `${Number(date.slice(5, 7))}/${Number(date.slice(8))}`;
		const title = `${isToday ? "Today, " : ""}${formatGameDate(date, "full")}`;

		// A day off: a slim faded column, the weekday over the date.
		if (!game) {
			return (
				<div
					className={clsx(
						"tbd tbd-off",
						isToday && "tbd-today",
						past && "tbd-past",
					)}
					title={`${title}, no game`}
				>
					<span className="tbd-off-weekday">{weekday}</span>
					<span className="tbd-off-num">{Number(date.slice(8))}</span>
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
		const livePts = !final ? live?.score?.pts : undefined;

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

		// Where the game stands, on the right of the day's head.
		let status: ReactNode = null;
		if (final) {
			const wl = allStar
				? undefined
				: winner === -1
					? "T"
					: winner === userIndex
						? "W"
						: "L";
			const overtimes = helpers.overtimeText(game.overtimes, game.numPeriods);
			status = (
				<>
					{overtimes ? <span className="tbd-ot">{overtimes}</span> : null}
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
				</>
			);
		} else if (live) {
			// (Live, the clock takes the date's place - it's tonight - as on
			// the networks' scoreboards, the network still on the right.)
			status = game.national ? (
				<NationalTag className="tbd-national" tag={game.national} />
			) : null;
		} else if (game.ifNecessary) {
			status = <span className="tbd-note">If nec.</span>;
		} else if (game.national) {
			status = <NationalTag className="tbd-national" tag={game.national} />;
		}

		let body: ReactNode;
		if (allStar || deadline) {
			body = (
				<div className="tbd-special">
					<span>{allStar ? "All-Star" : "Deadline"}</span>
					{final && allStar ? (
						<span className="tbd-special-score">
							{game.teams[1].pts}-{game.teams[0].pts}
						</span>
					) : null}
				</div>
			);
		} else {
			// Away over home, as on a scoreboard. (The logo and abbreviation
			// open the team's roster; the rest of the day, its game.)
			body = ([1, 0] as const).map((i) => {
				const t = game.teams[i];
				const other = game.teams[i === 0 ? 1 : 0];
				const branding = teamInfoCache[t.tid];
				const pts = final ? t.pts : livePts?.[i];
				const otherPts = final ? other.pts : livePts?.[i === 0 ? 1 : 0];
				const trailing =
					pts !== undefined && otherPts !== undefined && pts < otherPts;
				// (Before tip-off, the favorite's line where its score will be.)
				const spread = game.spread;
				const line =
					!final &&
					livePts === undefined &&
					spread !== undefined &&
					spread !== 0 &&
					spread > 0 === (i === 0)
						? lineText(spread)
						: undefined;
				return (
					<div
						key={i}
						className={clsx(
							"tbd-row",
							trailing && "tbd-row-trailing",
							final && winner === i && "tbd-row-won",
						)}
					>
						<a
							className="tbd-team"
							href={helpers.leagueUrl([
								"roster",
								`${abbrevOf(t.tid)}_${t.tid}`,
								season,
							])}
							title={`${branding?.region ?? ""} ${branding?.name ?? ""} roster`}
						>
							<TeamLogoInline
								imgURL={branding?.imgURL}
								imgURLSmall={branding?.imgURLSmall}
								includePlaceholderIfNoLogo
								size={16}
								className="tbd-logo"
							/>
							<span className="tbd-abbrev">{branding?.abbrev}</span>
						</a>
						{pts !== undefined ? (
							<span className="tbd-pts">{pts}</span>
						) : line ? (
							<span className="tbd-line" title="Point spread">
								{line}
							</span>
						) : null}
					</div>
				);
			});
		}

		const label = `${title}${
			allStar || deadline
				? ""
				: `, ${home ? "vs" : "@"} ${teamInfoCache[opp.tid]?.region ?? ""} ${
						teamInfoCache[opp.tid]?.name ?? ""
					}`
		}${game.ifNecessary ? " (if necessary)" : ""}`;

		// The whole day is the game's link, laid over it, under the teams'.
		return (
			<div
				className={clsx(
					"tbd tbd-link",
					isToday && "tbd-today",
					past && "tbd-past",
					livePts !== undefined && "tbd-is-live",
					game.ifNecessary && "tbd-maybe",
				)}
			>
				<a className="tbd-cover" href={href} title={label} aria-label={label} />
				<div className="tbd-head">
					{live && !final ? (
						<span className="tbd-live">{statusLines(live).join(" · ")}</span>
					) : (
						<>
							<span className="tbd-weekday">{weekday}</span>
							<span className="tbd-num">{dayNum}</span>
						</>
					)}
					<span className="tbd-status">{status}</span>
				</div>
				<div className="tbd-body">{body}</div>
			</div>
		);
	},
);
