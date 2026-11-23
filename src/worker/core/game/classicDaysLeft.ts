import { PHASE } from "../../../common/constants.ts";
import { isTradeDeadline } from "../../../common/specialGames.ts";
import type { ScheduleGame } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import getDaysLeftSchedule from "../season/getDaysLeftSchedule.ts";
import getSchedule from "../season/getSchedule.ts";
import { ticksThrough } from "./calendarTicks.ts";
import { storedClassicDays } from "./classicDays.ts";

// DAYS LEFT AT THE CLASSIC PACE: getDaysLeftSchedule's count, but on a
// calendar the days a classic league would have left (calendarTicks.ts), not
// the nights - so a stretch of so many days before the trade deadline is as
// long, and meets as often, as in a classic league. A calendar has about two
// nights to a classic day; counted in nights, the CPU's last days of trading
// before the deadline were half as many.

// From the schedule still to play, the league's regular-season games played
// so far and the season's classic days (undefined: not on a calendar, the
// schedule's own days). As getDaysLeftSchedule: to the trade deadline, the
// days before it; with no target, the days to the end of the season. The
// All-Star Game and the deadline are a day each, as in a classic league.
export const classicDaysLeftFrom = ({
	schedule,
	played,
	classicDays,
	target,
}: {
	schedule: readonly Pick<ScheduleGame, "homeTid" | "awayTid" | "day">[];
	played: number;
	classicDays: number | undefined;
	target?: "tradeDeadline";
}) => {
	const first = schedule[0];
	if (!first) {
		if (target !== undefined) {
			throw new Error(`No "${target}" left on the schedule`);
		}
		return 0;
	}
	let end = Infinity;
	if (target === "tradeDeadline") {
		const deadline = schedule.find((game) =>
			isTradeDeadline(game.homeTid, game.awayTid),
		);
		if (!deadline) {
			throw new Error(`No "${target}" left on the schedule`);
		}
		end = deadline.day;
	}
	if (classicDays === undefined) {
		return end === Infinity
			? schedule.at(-1)!.day - first.day + 1
			: end - first.day;
	}
	let remaining = 0;
	let before = 0;
	const ownDays = new Set<number>();
	for (const game of schedule) {
		if (game.homeTid >= 0 && game.awayTid >= 0) {
			remaining += 1;
			if (game.day < end) {
				before += 1;
			}
		} else if (game.day < end || end === Infinity) {
			ownDays.add(game.day);
		}
	}
	const total = played + remaining;
	return (
		ticksThrough(played + before, total, classicDays) -
		ticksThrough(played, total, classicDays) +
		ownDays.size
	);
};

// This season's, as it stands (throws as getDaysLeftSchedule does).
const classicDaysLeft = async (target?: "tradeDeadline") => {
	const phase = g.get("phase");
	const classicDays = storedClassicDays();
	if (
		classicDays === undefined ||
		(phase !== PHASE.REGULAR_SEASON && phase !== PHASE.AFTER_TRADE_DEADLINE)
	) {
		return getDaysLeftSchedule(target);
	}
	const schedule = await getSchedule();
	if (!schedule.some((game) => game.date !== undefined)) {
		return getDaysLeftSchedule(target);
	}
	let played = 0;
	for (const game of await idb.cache.games.getAll()) {
		if (
			game.season === g.get("season") &&
			!game.playoffs &&
			game.teams[0].tid >= 0 &&
			game.teams[1].tid >= 0
		) {
			played += 1;
		}
	}
	return classicDaysLeftFrom({ schedule, played, classicDays, target });
};

export default classicDaysLeft;
