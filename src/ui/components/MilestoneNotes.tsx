import { helpers } from "../util/helpers.ts";
import type {
	ChaseRow,
	PaceRow,
	WatchStat,
} from "../../worker/util/milestoneWatch.ts";

// The words of the Milestone Watch, for its page and a player's.

export const STAT_TITLES: Record<WatchStat, string> = {
	pts: "Points",
	trb: "Rebounds",
	ast: "Assists",
	stl: "Steals",
	blk: "Blocks",
	tp: "3-Pointers",
};

export const STAT_ABBREVS: Record<WatchStat, string> = {
	pts: "PTS",
	trb: "REB",
	ast: "AST",
	stl: "STL",
	blk: "BLK",
	tp: "3PM",
};

export const STAT_WORDS: Record<WatchStat, string> = {
	pts: "points",
	trb: "rebounds",
	ast: "assists",
	stl: "steals",
	blk: "blocks",
	tp: "3-pointers",
};

export const n = (x: number) => helpers.numberWithCommas(x);

export const playerUrl = (pid: number) => helpers.leagueUrl(["player", pid]);

// Player Stats' career totals, sorted by the stat.
export const careerLeadersUrl = (stat: WatchStat) =>
	`${helpers.leagueUrl(["player_stats", "all", "career", "totals"])}?sort=${stat}`;

// "41 to pass Name for 2nd", or the lead the man on top has.
export const chaseNote = (
	row: Pick<ChaseRow, "total" | "rank" | "next" | "behind">,
) => {
	if (row.next) {
		return (
			<>
				{n(row.next.total - row.total + 1)} to pass{" "}
				<a href={playerUrl(row.next.pid)}>{row.next.name}</a> for{" "}
				{helpers.ordinal(row.rank - 1)}
			</>
		);
	}
	if (row.behind) {
		return (
			<>
				Leads <a href={playerUrl(row.behind.pid)}>{row.behind.name}</a> by{" "}
				{n(row.total - row.behind.total)}
			</>
		);
	}
	return "All-time leader";
};

export const gamesAway = (games: number | undefined) =>
	games === undefined
		? undefined
		: games <= 1
			? "Could come next game"
			: `About ${games} games away`;

export const paceNote = (row: PaceRow) => {
	const record = (
		<>
			{n(row.record.total)},{" "}
			<a href={playerUrl(row.record.pid)}>{row.record.name}</a> (
			{row.record.season})
		</>
	);
	return row.place === 1 ? (
		row.record.pid === row.pid ? (
			<>
				Would break own record: {n(row.record.total)} ({row.record.season})
			</>
		) : (
			<>Would break the record: {record}</>
		)
	) : (
		<>
			Would be the {helpers.ordinal(row.place)} best season ever. Record:{" "}
			{record}
		</>
	);
};
