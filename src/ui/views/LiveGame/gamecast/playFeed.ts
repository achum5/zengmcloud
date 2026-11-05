// THE PLAYS, AS THE GAMECAST LISTS THEM. The live sim writes a line for every
// event; the feed turns those lines into rows the way a broadcast's play list
// does: each row headed with what happened ("Missed 3PT", "Rebound"), and a
// shot one row however many lines it took - the attempt opens it, and the
// result (made, missed, blocked, fouled) finishes the same row, so the row is
// there the moment he lets it go and turns into the result when it lands.

// What a line is, for the feed: its event's type, who it belongs to, when,
// and the score once it was played. (Newest first, as the live game keeps
// them.)
export type FeedEntry = {
	key: number;
	type?: string;
	t: 0 | 1 | undefined;
	// The man whose play it is (the first man on, for a substitution).
	pid?: number;
	text: unknown;
	textOnly: boolean;
	time: string;
	// The period it was played in, 1 up (overtimes go on past the regulation
	// periods).
	period?: number;
	// The score after it, box score order (0 home, 1 away).
	pts?: [number, number];
	// It scored: the points it put up.
	scored?: number;
};

export type FeedRow = {
	// The first line's key - a shot keeps its row from attempt to result.
	key: number;
	title: string;
	t: 0 | 1 | undefined;
	pid?: number;
	// The lines it was put together from, oldest first.
	texts: unknown[];
	textOnly: boolean;
	time: string;
	period?: number;
	pts?: [number, number];
	scored?: number;
	// A shot still in the air: its row is open for the result.
	pending: boolean;
};

const ATTEMPTS = new Set([
	"fgaAtRim",
	"fgaLowPost",
	"fgaMidRange",
	"fgaTp",
	"fgaTpFake",
	"fgaPutBack",
	"fgaTipIn",
]);

const THREES = new Set(["fgaTp", "fgaTpFake"]);

// What finishes a shot's row: the ball going in or not, the block, or the
// foul on the shooter (whose free throws are rows of their own).
const RESULTS = new Set([
	"fgAtRim",
	"fgAtRimAndOne",
	"fgLowPost",
	"fgLowPostAndOne",
	"fgMidRange",
	"fgMidRangeAndOne",
	"tp",
	"tpAndOne",
	"fgPutBack",
	"fgPutBackAndOne",
	"fgTipIn",
	"fgTipInAndOne",
	"missAtRim",
	"missLowPost",
	"missMidRange",
	"missTp",
	"missPutBack",
	"missTipIn",
	"blkAtRim",
	"blkLowPost",
	"blkMidRange",
	"blkTp",
	"blkPutBack",
	"blkTipIn",
	"pfFG",
	"pfTP",
]);

export const isAttempt = (type: string | undefined) =>
	type !== undefined && ATTEMPTS.has(type);

// The heading of a row: what happened, in a word or two. (The attempt's type
// says whether it was a three, so a result is headed knowing what it
// finished.)
export const playTitle = (
	type: string | undefined,
	attempt?: string,
): string => {
	const three =
		attempt !== undefined
			? THREES.has(attempt)
			: type === "tp" || type === "tpAndOne" || type === "missTp";
	if (type === undefined) {
		return "";
	}
	if (ATTEMPTS.has(type)) {
		return THREES.has(type) ? "3PT Attempt" : "Shot";
	}
	if (type.endsWith("AndOne")) {
		return three ? "And-One 3PT" : "And-One";
	}
	if (type.startsWith("fg") || type === "tp") {
		return three ? "Made 3PT" : "Made FG";
	}
	if (type.startsWith("miss") && type !== "missFt") {
		return three ? "Missed 3PT" : "Missed FG";
	}
	if (type.startsWith("blk")) {
		return "Blocked Shot";
	}
	switch (type) {
		case "pfFG":
		case "pfTP":
			return "Shooting Foul";
		case "pfNonShooting":
		case "pfBonus":
			return "Foul";
		case "ft":
			return "Made FT";
		case "missFt":
			return "Missed FT";
		case "orb":
			return "Offensive Rebound";
		case "drb":
			return "Defensive Rebound";
		case "tov":
			return "Turnover";
		case "stl":
			return "Steal";
		case "sub":
			return "Substitution";
		case "timeout":
			return "Timeout";
		case "jumpBall":
			return "Jump Ball";
		case "outOfBounds":
			return "Out of Bounds";
		case "foulOut":
			return "Fouled Out";
		case "injury":
			return "Injury";
		case "endOfPeriod":
			return "Buzzer";
		case "shootoutTeam":
		case "shootoutShot":
			return "Shootout";
		default:
			return "";
	}
};

// The rows, newest first, from the lines, newest first. A shot's attempt and
// its result are one row, keyed by the attempt, headed and scored by the
// result, and belonging to the result's team and man (a block is the blocker's play,
// a shooting foul the fouler's - and a make or a miss is the shooter's own,
// the same team as the attempt).
export const feedRows = (entries: readonly FeedEntry[]): FeedRow[] => {
	const rows: FeedRow[] = [];
	// Oldest first, so an attempt is always met before its result.
	let open: FeedRow | undefined;
	let openType: string | undefined;
	for (let i = entries.length - 1; i >= 0; i--) {
		const e = entries[i]!;
		if (open && e.type !== undefined && RESULTS.has(e.type)) {
			open.title = playTitle(e.type, openType);
			open.t = e.t;
			open.pid = e.pid ?? open.pid;
			open.texts.push(e.text);
			open.time = e.time || open.time;
			open.pts = e.pts ?? open.pts;
			open.scored = e.scored;
			open.pending = false;
			open = undefined;
			openType = undefined;
			continue;
		}
		const row: FeedRow = {
			key: e.key,
			title: playTitle(e.type),
			t: e.t,
			pid: e.pid,
			texts: [e.text],
			textOnly: e.textOnly,
			time: e.time,
			period: e.period,
			pts: e.pts,
			scored: e.scored,
			pending: isAttempt(e.type),
		};
		// (A line that isn't the attempt's result - there is always one, but
		// just in case - leaves the attempt as it was.)
		if (open) {
			open.pending = false;
		}
		open = row.pending ? row : undefined;
		openType = row.pending ? e.type : undefined;
		rows.push(row);
	}
	return rows.reverse();
};

// "2nd", "OT", "2OT": the period a row was played in, as a broadcast says it.
export const periodLabel = (
	period: number | undefined,
	numPeriods: number,
	ordinal: (n: number) => string,
): string | undefined => {
	if (period === undefined || period < 1) {
		return undefined;
	}
	if (period <= numPeriods) {
		return ordinal(period);
	}
	const ot = period - numPeriods;
	return ot === 1 ? "OT" : `${ot}OT`;
};
