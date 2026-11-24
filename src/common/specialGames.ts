// The All-Star Game and the trade deadline are stored in the schedule as games between these placeholder tids, as [homeTid, awayTid]
export const ALL_STAR_GAME_TIDS = [-1, -2] as const;
export const TRADE_DEADLINE_TIDS = [-3, -3] as const;
// On the NBA calendar, a date with no games at all (Election Day, Christmas
// Eve, the All-Star break, the gaps in the playoffs) still is a day the
// league passes through: it's a schedule row of its own, like the deadline.
export const OFF_DAY_TIDS = [-4, -4] as const;

export const isAllStarGame = (homeTid: number, awayTid: number) =>
	homeTid === ALL_STAR_GAME_TIDS[0] && awayTid === ALL_STAR_GAME_TIDS[1];

export const isTradeDeadline = (homeTid: number, awayTid: number) =>
	homeTid === TRADE_DEADLINE_TIDS[0] && awayTid === TRADE_DEADLINE_TIDS[1];

export const isOffDay = (homeTid: number, awayTid: number) =>
	homeTid === OFF_DAY_TIDS[0] && awayTid === OFF_DAY_TIDS[1];

// A schedule row that's a day with no game on it: the trade deadline or an
// off day.
export const isNoGameDay = (homeTid: number, awayTid: number) =>
	isTradeDeadline(homeTid, awayTid) || isOffDay(homeTid, awayTid);

// tid of either All-Star team
export const isAllStarTid = (tid: number) =>
	tid === ALL_STAR_GAME_TIDS[0] || tid === ALL_STAR_GAME_TIDS[1];
