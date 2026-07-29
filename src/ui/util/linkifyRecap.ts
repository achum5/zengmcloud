import { helpers } from "./helpers.ts";
import {
	displaySectionHeader,
	parseSectionHeader,
} from "../../common/seasonNote.ts";

export type RecapLink = { name: string; href: string };

// Build the name→link map for a game recap, scoped STRICTLY to the two teams in
// this game and their rosters (the box-score players), so a name only ever links
// to the right player/team in this game - never a same-named player elsewhere.
export const buildRecapLinks = (boxScore: any): RecapLink[] => {
	const entries: RecapLink[] = [];
	const teams = Array.isArray(boxScore?.teams) ? boxScore.teams : [];
	for (const t of teams) {
		if (typeof t?.tid !== "number" || t.tid < 0) {
			continue;
		}
		const teamHref = helpers.leagueUrl([
			"roster",
			`${t.abbrev}_${t.tid}`,
			boxScore.season,
		]);
		// Full "Region Name" first (longest), then the nickname and region alone.
		for (const name of [`${t.region} ${t.name}`, t.name, t.region]) {
			if (typeof name === "string" && name.trim() !== "") {
				entries.push({ name: name.trim(), href: teamHref });
			}
		}
		for (const p of Array.isArray(t.players) ? t.players : []) {
			if (typeof p?.pid === "number" && p.pid >= 0 && p.name) {
				entries.push({
					name: String(p.name),
					href: helpers.leagueUrl(["player", p.pid]),
				});
			}
		}
	}
	return entries;
};

// Same name→link map as buildRecapLinks, but for a raw game record (e.g. on the
// Daily Schedule) whose teams carry only tid + box-score players, not the
// enriched region/name/abbrev the box-score page has. Team branding is resolved
// via each team's per-season `branding` (set for past seasons) or the caller's
// current-season lookup (teamInfoCache).
export const buildRecapLinksForGame = (
	game: { season: number; teams: any[] },
	teamInfo: (
		tid: number,
	) => { abbrev?: string; region?: string; name?: string } | undefined,
): RecapLink[] => {
	const entries: RecapLink[] = [];
	for (const t of Array.isArray(game?.teams) ? game.teams : []) {
		if (typeof t?.tid !== "number" || t.tid < 0) {
			continue;
		}
		const info = t.branding ?? teamInfo(t.tid);
		if (info?.abbrev) {
			const teamHref = helpers.leagueUrl([
				"roster",
				`${info.abbrev}_${t.tid}`,
				game.season,
			]);
			const region = info.region ?? "";
			const name = info.name ?? "";
			for (const label of [`${region} ${name}`, name, region]) {
				if (label.trim() !== "") {
					entries.push({ name: label.trim(), href: teamHref });
				}
			}
		}
		for (const p of Array.isArray(t.players) ? t.players : []) {
			if (typeof p?.pid === "number" && p.pid >= 0 && p.name) {
				entries.push({
					name: String(p.name),
					href: helpers.leagueUrl(["player", p.pid]),
				});
			}
		}
	}
	return entries;
};

export type TeamInfoCache = {
	abbrev?: string;
	region?: string;
	name?: string;
	disabled?: boolean;
}[];

// Every league team's name pointing at its roster for one season. The tid is in
// the URL, so a franchise that has since been renamed still resolves - only the
// name being matched in the text has to be current.
const teamLinks = (
	teamInfoCache: TeamInfoCache,
	season: number | undefined,
): RecapLink[] => {
	const entries: RecapLink[] = [];
	// teamInfoCache is indexed by tid.
	for (let tid = 0; tid < teamInfoCache.length; tid++) {
		const info = teamInfoCache[tid];
		if (!info?.abbrev) {
			continue;
		}
		const href = helpers.leagueUrl(
			season === undefined
				? ["roster", `${info.abbrev}_${tid}`]
				: ["roster", `${info.abbrev}_${tid}`, season],
		);
		const region = info.region ?? "";
		const name = info.name ?? "";
		// Full "Region Name" first (longest), then the nickname and region alone.
		for (const label of [`${region} ${name}`, name, region]) {
			if (label.trim() !== "") {
				entries.push({ name: label.trim(), href });
			}
		}
	}
	return entries;
};

// The name→link map for a TEAM-SEASON recap note (the AI season writeups). Links
// every league team's name (to its roster) plus that season's roster players (to
// their pages). Team names are unique so they can be league-wide; players are
// scoped to this team's season roster to avoid mislinking a same-named player.
export const buildTeamSeasonRecapLinks = ({
	season,
	players,
	teamInfoCache,
}: {
	season: number;
	players: { pid?: number; firstName?: string; lastName?: string }[];
	teamInfoCache: TeamInfoCache;
}): RecapLink[] => {
	const entries: RecapLink[] = teamLinks(teamInfoCache, season);

	for (const p of players ?? []) {
		if (typeof p?.pid === "number" && p.pid >= 0) {
			const full = `${p.firstName ?? ""} ${p.lastName ?? ""}`.trim();
			if (full) {
				entries.push({
					name: full,
					href: helpers.leagueUrl(["player", p.pid]),
				});
			}
		}
	}

	return entries;
};

const escapeRegex = (s: string): string =>
	s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// A placeholder that won't appear in recap text and can't be re-matched by a
// later (shorter) name, so real numbers never collide and links never nest.
const TOKEN_OPEN = "@@recapLink";
const TOKEN_CLOSE = "@@";

// Wrap each known team/player name in the recap markdown with a link to its
// page. Preserves existing **bold** (a bolded name becomes a bold link) and
// won't double-link: longer names are matched first and parked in a placeholder.
export const linkifyRecap = (text: string, entries: RecapLink[]): string => {
	if (!text || entries.length === 0) {
		return text;
	}

	// Dedupe by name (first href wins); longest name first.
	const seen = new Set<string>();
	const unique = entries
		.filter((e) => {
			if (!e.name || seen.has(e.name)) {
				return false;
			}
			seen.add(e.name);
			return true;
		})
		.sort((a, b) => b.name.length - a.name.length);

	const placeholders: string[] = [];
	let out = text;
	for (const { name, href } of unique) {
		const escaped = escapeRegex(name);
		// "**Name**" (keep the bold) or a standalone "Name" (word-bounded).
		const re = new RegExp(
			`\\*\\*${escaped}\\*\\*|(?<![\\w])${escaped}(?![\\w])`,
			"g",
		);
		out = out.replace(re, (match) => {
			const link = `[${name}](${href})`;
			const token = `${TOKEN_OPEN}${placeholders.length}${TOKEN_CLOSE}`;
			placeholders.push(match.startsWith("**") ? `**${link}**` : link);
			return token;
		});
	}
	return out.replace(
		new RegExp(`${TOKEN_OPEN}(\\d+)${TOKEN_CLOSE}`, "g"),
		(_, i) => placeholders[Number(i)]!,
	);
};

// A PLAYER note is not one piece of writing - it's a stack of "[YYYY]" sections,
// one per season of a career. A team named in the 2001 section means that team
// in 2001, so linking the whole note against a single year would send every
// mention to the wrong page. Each section is linked against its own year
// instead, and the links land on that season's pages.

export const linkifySeasonNote = (
	text: string,
	linksFor: (season: number | undefined) => RecapLink[],
): string => {
	if (!text) {
		return text;
	}

	const out: string[] = [];
	let season: number | undefined;
	let chunk: string[] = [];

	const flush = () => {
		if (chunk.length > 0) {
			out.push(linkifyRecap(chunk.join("\n"), linksFor(season)));
			chunk = [];
		}
	};

	for (const line of text.split("\n")) {
		const header = parseSectionHeader(line);
		if (header) {
			flush();
			season = header.season;
			// A header is a label, not prose, so it never gets links written into
			// it - but it does get rewritten for display (a retirement writeup
			// shows as a standalone headline rather than a dated log entry).
			out.push(displaySectionHeader(line));
		} else {
			chunk.push(line);
		}
	}
	flush();

	return out.join("\n");
};

// Season-aware links for a player's note: every team, plus the people he
// actually played with that year. Teammates are scoped per season rather than
// looked up league-wide, so a common name can't link to the wrong player - the
// same rule the game and team-season recaps follow.
//
// Built once per note and memoized per season, since a long career has a
// section for every year and they mostly repeat the same names.
export const buildPlayerNoteLinks = (
	teamInfoCache: TeamInfoCache,
	teammatesBySeason?: {
		season: number;
		players: { pid: number; name: string }[];
	}[],
) => {
	const bySeason = new Map<number, { pid: number; name: string }[]>();
	for (const row of teammatesBySeason ?? []) {
		bySeason.set(row.season, row.players);
	}

	const cache = new Map<number | undefined, RecapLink[]>();
	return (season: number | undefined): RecapLink[] => {
		const existing = cache.get(season);
		if (existing) {
			return existing;
		}
		const entries = teamLinks(teamInfoCache, season);
		if (season !== undefined) {
			for (const { pid, name } of bySeason.get(season) ?? []) {
				if (name.trim() !== "") {
					entries.push({
						name: name.trim(),
						href: helpers.leagueUrl(["player", pid]),
					});
				}
			}
		}
		cache.set(season, entries);
		return entries;
	};
};
