import { parseScoreBug, scoreBugPictureIds } from "./scoreBug.ts";
import type { NationalBroadcast, NationalSlate } from "./types.ts";
import { addDays, daysBetween, nthWeekday } from "./calendarDates.ts";

// NATIONAL BROADCASTS (see NationalBroadcast): which game of a night a
// national network has, and so whose score bug it carries.

export const NIGHTS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
export const SLATES: NationalSlate[] = [
	"openingNight",
	"christmas",
	"mlkDay",
	"rivalsWeek",
];
const MAX_NETWORKS = 12;

export const parseNationalBroadcasts = (
	raw: unknown,
): NationalBroadcast[] | string => {
	if (!Array.isArray(raw) || raw.length > MAX_NETWORKS) {
		return `Expected a list of networks (at most ${MAX_NETWORKS}).`;
	}
	const out: NationalBroadcast[] = [];
	for (const [i, n] of raw.entries()) {
		if (typeof n !== "object" || n === null || Array.isArray(n)) {
			return `Network ${i + 1} should be an object.`;
		}
		const r = n as Record<string, unknown>;
		const label =
			typeof r.network === "string" ? r.network : `Network ${i + 1}`;
		for (const key of Object.keys(r)) {
			if (
				![
					"network",
					"nights",
					"slate",
					"finals",
					"logo",
					"logoDark",
					"scoreBug",
				].includes(key)
			) {
				return `${label}: unknown field "${key}".`;
			}
		}
		if (typeof r.network !== "string" || r.network.trim() === "") {
			return `${label}: "network" should be its name.`;
		}
		if (
			r.nights !== undefined &&
			(!Array.isArray(r.nights) ||
				r.nights.some((d) => typeof d !== "string" || !NIGHTS.includes(d)))
		) {
			return `${label}: "nights" should be a list of ${NIGHTS.join(", ")}.`;
		}
		if (r.slate !== undefined && !SLATES.includes(r.slate as NationalSlate)) {
			return `${label}: "slate" should be one of ${SLATES.join(", ")}.`;
		}
		if (r.finals !== undefined && typeof r.finals !== "boolean") {
			return `${label}: "finals" should be true or false.`;
		}
		for (const key of ["logo", "logoDark"] as const) {
			if (r[key] !== undefined && typeof r[key] !== "string") {
				return `${label}: "${key}" should be a picture's link.`;
			}
		}
		const bug = parseScoreBug(r.scoreBug);
		if (typeof bug === "string") {
			return `${label}: ${bug}`;
		}
		out.push({
			network: r.network,
			...(r.nights !== undefined ? { nights: r.nights as string[] } : {}),
			...(r.slate !== undefined ? { slate: r.slate as NationalSlate } : {}),
			...(r.finals !== undefined ? { finals: r.finals } : {}),
			...(r.logo !== undefined ? { logo: r.logo } : {}),
			...(r.logoDark !== undefined ? { logoDark: r.logoDark } : {}),
			scoreBug: bug,
		});
	}
	return out;
};

// The pictures a network keeps in the league: its bug's, and its logos.
export const nationalPictureIds = (n: NationalBroadcast) => [
	...scoreBugPictureIds(n.scoreBug),
	...[n.logo, n.logoDark].flatMap((logo) =>
		logo?.startsWith("pic:") ? [logo.slice(4)] : [],
	),
];

// The night of the week a game is on (0 Sunday): by its date, or with no
// calendar, counted from opening night (`openingDay`), a Tuesday as in the
// NBA.
export const nightOf = (
	game: { date?: string; day?: number },
	openingDay: number | undefined,
): number | undefined => {
	if (game.date !== undefined) {
		const d = new Date(`${game.date}T12:00:00Z`);
		return Number.isNaN(d.getTime()) ? undefined : d.getUTCDay();
	}
	if (game.day === undefined || openingDay === undefined) {
		return undefined;
	}
	return (((2 + game.day - openingDay) % 7) + 7) % 7;
};

// The games a team plays before its record counts on its own - till then,
// how it was rated as the season began carries the rest.
const RATED_GAMES = 10;

// A team's standing going into a night: its record, leaning on its rating
// (`rated`, 0 the league's worst to 1 its best) early in the season.
export const strengthOf = (
	record: { won: number; lost: number; tied?: number } | undefined,
	rated: number,
) => {
	const won = (record?.won ?? 0) + 0.5 * (record?.tied ?? 0);
	const gp = (record?.won ?? 0) + (record?.lost ?? 0) + (record?.tied ?? 0);
	return (won + RATED_GAMES * rated) / (gp + RATED_GAMES);
};

// THE NIGHT'S BIGGEST GAME: the one between the two best teams going in
// (the most strength between them - the earliest game, if even). `games`:
// the night's.
export const biggestGame = <T extends { gid: number; tids: [number, number] }>(
	games: T[],
	strength: (tid: number) => number,
): T | undefined => {
	let best: T | undefined;
	let bestScore = -Infinity;
	for (const game of [...games].sort((a, b) => a.gid - b.gid)) {
		const score = strength(game.tids[0]) + strength(game.tids[1]);
		if (score > bestScore + 1e-9) {
			best = game;
			bestScore = score;
		}
	}
	return best;
};

// WHOSE GAME IT IS. A regular season game: the network of its night, if
// it's the night's biggest (`biggest`) - the first listed with that night.
// A playoff game: the Finals' network in the Finals; otherwise its
// series', shared out among the networks with nights.
export const networkFor = (
	networks: NationalBroadcast[],
	game: {
		season: number;
		playoffs?: boolean;
		finals?: boolean;
		tids: [number, number];
	},
	night: number | undefined,
	biggest: boolean,
): NationalBroadcast | undefined => {
	if (game.playoffs) {
		// (A slate's network has only its nights.)
		const weekly = networks.filter((n) => n.slate === undefined);
		if (game.finals) {
			const finals = weekly.find((n) => n.finals);
			if (finals) {
				return finals;
			}
		}
		// (A network once, however many of its nights have their own bug.)
		const shared = weekly.filter(
			(n, i) =>
				(n.nights?.length ?? 0) > 0 &&
				weekly.findIndex(
					(m) => m.network === n.network && (m.nights?.length ?? 0) > 0,
				) === i,
		);
		if (shared.length === 0) {
			return undefined;
		}
		const [a, b] = [...game.tids].sort((x, y) => x - y) as [number, number];
		const k = Math.abs(a * 31 + b * 17 + game.season) % shared.length;
		return shared[k];
	}
	if (!biggest || night === undefined) {
		return undefined;
	}
	return nightNetworks(networks, night, undefined)[0];
};

// THE NBA CALENDAR'S SPECIAL NIGHT a date is, if any (see NationalSlate):
// `openingDate`, the season's first night.
export const slateOf = (
	date: string,
	openingDate: string | undefined,
): NationalSlate | undefined => {
	if (date === openingDate) {
		return "openingNight";
	}
	if (date.slice(5) === "12-25") {
		return "christmas";
	}
	const mlk = nthWeekday(Number(date.slice(0, 4)), 1, 1, 3);
	if (date === mlk) {
		return "mlkDay";
	}
	const after = daysBetween(mlk, date);
	if (after >= 1 && after <= 5) {
		return "rivalsWeek";
	}
};

// HOW MANY OF A NIGHT'S GAMES ARE NATIONAL, as in the NBA's TV deal from
// 2025-26: Monday's games on Peacock, NBC's Tuesday and ESPN's Wednesday
// doubleheaders all season, Prime's Thursday ones once football is over
// (mid-January), Friday's Prime doubleheader with ESPN's beside it from
// January, a Saturday game (two from January: Prime's afternoon, ABC's
// night), and Sundays from February (ABC's afternoon, NBC's night; just
// NBC's in April). Opening week, a doubleheader every night; the night
// before Thanksgiving, three; opening night and Christmas, every game; MLK
// Day, four; Rivals Week, eleven over its five nights. With no calendar,
// one game a night as ever. `night`: the weekday, 0 Sunday.
export const nationalGamesOn = (
	date: string | undefined,
	night: number | undefined,
	openingDate: string | undefined,
	numGames: number,
) => {
	if (date === undefined || night === undefined) {
		return Math.min(1, numGames);
	}
	const slate = slateOf(date, openingDate);
	let count;
	if (slate === "openingNight" || slate === "christmas") {
		count = numGames;
	} else if (slate === "mlkDay") {
		count = 4;
	} else if (slate === "rivalsWeek") {
		count = night === 5 ? 3 : 2;
	} else {
		// Month and day as a number, 1015 for October 15.
		const md = Number(date.slice(5, 7)) * 100 + Number(date.slice(8, 10));
		const from = (start: number) => md >= start && md < 700;
		count = [
			from(401) ? 1 : from(201) ? 2 : 0,
			2,
			2,
			2,
			from(115) ? 2 : 0,
			from(101) ? 4 : 2,
			from(101) && !from(401) ? 2 : 1,
		][night]!;
		if (openingDate !== undefined) {
			const into = daysBetween(openingDate, date);
			if (into >= 1 && into <= 6) {
				count = Math.max(count, 2);
			}
		}
		const thanksgiving = nthWeekday(Number(date.slice(0, 4)), 11, 4, 4);
		if (date === addDays(thanksgiving, -1)) {
			count = 3;
		}
	}
	return Math.min(count, numGames);
};

// WHO CARRIES A NIGHT'S NATIONAL GAMES: its slate's networks, if it's one of
// the special nights and some network has it; otherwise its weekday's. A
// special night nobody has, on a weekday nobody has either, is the league's
// networks' in turn.
export const nightNetworks = (
	networks: NationalBroadcast[],
	night: number | undefined,
	slate: NationalSlate | undefined,
): NationalBroadcast[] => {
	if (slate !== undefined) {
		const own = networks.filter((n) => n.slate === slate);
		if (own.length > 0) {
			return own;
		}
	}
	const weekly = networks.filter(
		(n) =>
			n.slate === undefined &&
			night !== undefined &&
			n.nights?.includes(NIGHTS[night]!),
	);
	if (weekly.length > 0 || slate === undefined) {
		return weekly;
	}
	return networks.filter(
		(n, i) =>
			n.slate === undefined &&
			(n.nights?.length ?? 0) > 0 &&
			networks.findIndex(
				(m) =>
					m.network === n.network &&
					m.slate === undefined &&
					(m.nights?.length ?? 0) > 0,
			) === i,
	);
};

// A NIGHT'S NATIONAL GAMES, by gid: the biggest of its games (see
// biggestGame; in Rivals Week, the division rivals' first), as many as the
// night has (nationalGamesOn), shared out among its networks
// (nightNetworks) in their order - the first ones the bigger games.
export const nationalNight = <
	T extends { gid: number; tids: [number, number] },
>(
	networks: NationalBroadcast[],
	games: T[],
	when: {
		date: string | undefined;
		night: number | undefined;
		openingDate: string | undefined;
	},
	strength: (tid: number) => number,
	rivals?: (a: number, b: number) => boolean,
): Map<number, NationalBroadcast> => {
	const out = new Map<number, NationalBroadcast>();
	const slate =
		when.date !== undefined ? slateOf(when.date, when.openingDate) : undefined;
	const carriers = nightNetworks(networks, when.night, slate);
	const count = nationalGamesOn(
		when.date,
		when.night,
		when.openingDate,
		games.length,
	);
	if (carriers.length === 0 || count === 0) {
		return out;
	}
	const rival = (game: T) =>
		slate === "rivalsWeek" && rivals?.(game.tids[0], game.tids[1]) ? 1 : 0;
	const score = (game: T) => strength(game.tids[0]) + strength(game.tids[1]);
	const ranked = [...games].sort(
		(a, b) =>
			rival(b) - rival(a) ||
			(Math.abs(score(b) - score(a)) > 1e-9 ? score(b) - score(a) : 0) ||
			a.gid - b.gid,
	);
	let i = 0;
	for (const [k, n] of carriers.entries()) {
		const share = Math.ceil((count - i) / (carriers.length - k));
		for (let j = 0; j < share && i < count; j++) {
			out.set(ranked[i]!.gid, n);
			i += 1;
		}
	}
	return out;
};
