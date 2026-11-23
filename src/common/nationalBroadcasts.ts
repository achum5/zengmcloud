import { parseScoreBug, scoreBugPictureIds } from "./scoreBug.ts";
import type { NationalBroadcast } from "./types.ts";

// NATIONAL BROADCASTS (see NationalBroadcast): which game of a night a
// national network has, and so whose score bug it carries.

export const NIGHTS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
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
		if (game.finals) {
			const finals = networks.find((n) => n.finals);
			if (finals) {
				return finals;
			}
		}
		// (A network once, however many of its nights have their own bug.)
		const shared = networks.filter(
			(n, i) =>
				(n.nights?.length ?? 0) > 0 &&
				networks.findIndex(
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
	return networks.find((n) => n.nights?.includes(NIGHTS[night]!));
};
