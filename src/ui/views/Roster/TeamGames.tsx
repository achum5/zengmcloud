import { useEffect, useState } from "react";
import { toWorker } from "../../util/toWorker.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { TeamLogoInline } from "../../components/TeamLogoInline.tsx";
import { PHASE } from "../../../common/constants.ts";
import type { View } from "../../../common/types.ts";
import { MonthCalendar } from "../../components/MonthCalendar.tsx";

type Side = { tid: number; pts?: number };
type Played = {
	gid: number;
	teams: [Side, Side];
	overtimes?: number;
	playoffs?: boolean;
};
type Upcoming = { gid: number; teams: [Side, Side] };

// (A view's data asked of the worker as its own page asks for it.)
const load = async (
	viewId: "calendar" | "gameLog" | "schedule",
	params: object,
) => {
	const got = await toWorker("main", "runBefore", {
		viewId,
		params,
		ctxBBGM: {},
		updateEvents: new Set(["firstRun"] as const),
		prevOutput: {},
		prevInputs: {},
	});
	const data = got?.data;
	return data && !data.errorMessage && !data.redirectUrl ? data : undefined;
};

// A TEAM'S SEASON, GAME BY GAME (its page's schedule, the refreshed look):
// each game a little tile - who, home or away, the score, the record after
// it - to the game's own page; this season's games still to come after them,
// greyed. Any season the team page is on. A season on the NBA calendar is
// its month calendar instead, each game on its date.
export const TeamGames = ({
	tid,
	abbrev,
	season,
}: {
	tid: number;
	abbrev: string;
	season: number;
}) => {
	const {
		phase,
		season: currentSeason,
		teamInfoCache,
	} = useLocal(["phase", "season", "teamInfoCache"]);
	const [games, setGames] = useState<
		| {
				played: Played[];
				upcoming: Upcoming[];
				calendar?: View<"calendar">;
		  }
		| undefined
	>();
	const current = season === currentSeason;
	useEffect(() => {
		let live = true;
		setGames(undefined);
		(async () => {
			try {
				const team = `${abbrev}_${tid}`;
				const calendar = (await load("calendar", {
					team,
					season: String(season),
				})) as View<"calendar"> | undefined;
				if (calendar && calendar.games.length > 0 && !calendar.undated) {
					if (live) {
						setGames({ played: [], upcoming: [], calendar });
					}
					return;
				}
				let played: Played[] = [];
				let upcoming: Upcoming[] = [];
				if (current) {
					const data = await load("schedule", { team });
					played = (data?.completed ?? []) as Played[];
					upcoming = (data?.upcoming ?? []) as Upcoming[];
				} else {
					const data = await load("gameLog", {
						team,
						season: String(season),
					});
					played = (data?.gamesList?.games ?? []) as Played[];
				}
				if (live) {
					setGames({
						// (Oldest first; the All-Star Game left out.)
						played: played
							.filter((gm) => gm.teams.some((t: Side) => t.tid === tid))
							.sort((a, b) => a.gid - b.gid),
						// (Not the trade deadline or the All-Star Game - in the
						// schedule, but no game of its.)
						upcoming: upcoming.filter((gm) =>
							gm.teams.some((t: Side) => t.tid === tid),
						),
					});
				}
			} catch {
				if (live) {
					setGames({ played: [], upcoming: [] });
				}
			}
		})();
		return () => {
			live = false;
		};
	}, [abbrev, current, season, tid]);

	if (!games) {
		return <div className="hub-wait" />;
	}
	if (games.calendar) {
		return (
			<MonthCalendar
				games={games.calendar.games}
				keyDates={games.calendar.keyDates}
				monthChips
				season={season}
				tid={tid}
				today={games.calendar.today}
			/>
		);
	}
	if (games.played.length === 0 && games.upcoming.length === 0) {
		return <p className="text-body-secondary">No games.</p>;
	}

	const tile = (gm: Played | Upcoming, n: number, record?: string) => {
		const us = gm.teams.findIndex((t) => t.tid === tid);
		const me = gm.teams[us];
		const opp = gm.teams[us === 0 ? 1 : 0];
		if (!me || !opp) {
			return null;
		}
		const home = us === 0;
		const t = teamInfoCache[opp.tid];
		const played = me.pts !== undefined && opp.pts !== undefined;
		const won = played && me.pts! > opp.pts!;
		const tied = played && me.pts === opp.pts;
		const ot = (gm as Played).overtimes ?? 0;
		const top = (
			<span className="gt-top">
				<span className="gt-num">{n}</span>
				<span className="gt-at">{home ? "vs" : "@"}</span>
				<TeamLogoInline
					imgURL={t?.imgURL}
					imgURLSmall={t?.imgURLSmall}
					size={18}
				/>
				<span className="gt-opp">{t?.abbrev ?? "?"}</span>
				{ot > 0 ? (
					<span className="gt-ot">{ot === 1 ? "OT" : `${ot}OT`}</span>
				) : null}
			</span>
		);
		if (!played) {
			return (
				<div key={gm.gid} className="gt gt-later">
					{top}
					<span className="gt-bottom">&nbsp;</span>
				</div>
			);
		}
		return (
			<a
				key={gm.gid}
				className="gt"
				href={helpers.leagueUrl([
					"game_log",
					`${abbrev}_${tid}`,
					season,
					gm.gid,
				])}
			>
				{top}
				<span className="gt-bottom">
					<span
						className={tied ? "gt-res" : won ? "gt-res gt-w" : "gt-res gt-l"}
					>
						{tied ? "T" : won ? "W" : "L"}
					</span>
					<span className="gt-score">
						{me.pts}-{opp.pts}
					</span>
					{record ? <span className="gt-rec">{record}</span> : null}
				</span>
			</a>
		);
	};

	// The record after each game, regular season and playoffs apart.
	const part = (playoffs: boolean) => {
		let w = 0;
		let l = 0;
		const played = games.played.filter((gm) => !!gm.playoffs === playoffs);
		const tiles = played.map((gm, i) => {
			const us = gm.teams.findIndex((t) => t.tid === tid);
			const me = gm.teams[us]?.pts ?? 0;
			const them = gm.teams[us === 0 ? 1 : 0]?.pts ?? 0;
			if (me > them) {
				w += 1;
			} else if (me < them) {
				l += 1;
			}
			return tile(gm, i + 1, `${w}-${l}`);
		});
		// (This season's still to come - the regular season's, or the
		// playoffs' once they are on.)
		const later =
			current && playoffs === phase >= PHASE.PLAYOFFS ? games.upcoming : [];
		return [...tiles, ...later.map((gm, i) => tile(gm, played.length + i + 1))];
	};
	const regular = part(false);
	const playoffs = part(true);

	return (
		<>
			{regular.length > 0 ? <div className="gt-grid">{regular}</div> : null}
			{playoffs.length > 0 ? (
				<>
					<h3 className="recap-subtitle mt-3">Playoffs</h3>
					<div className="gt-grid">{playoffs}</div>
				</>
			) : null}
		</>
	);
};
