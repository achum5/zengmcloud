import { helpers } from "./helpers.ts";

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
	teamInfoCache: {
		abbrev?: string;
		region?: string;
		name?: string;
		disabled?: boolean;
	}[];
}): RecapLink[] => {
	const entries: RecapLink[] = [];

	// teamInfoCache is indexed by tid.
	for (let tid = 0; tid < teamInfoCache.length; tid++) {
		const info = teamInfoCache[tid];
		if (!info?.abbrev) {
			continue;
		}
		const href = helpers.leagueUrl(["roster", `${info.abbrev}_${tid}`, season]);
		const region = info.region ?? "";
		const name = info.name ?? "";
		for (const label of [`${region} ${name}`, name, region]) {
			if (label.trim() !== "") {
				entries.push({ name: label.trim(), href });
			}
		}
	}

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
