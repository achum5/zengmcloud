import { helpers } from "./helpers.ts";
import {
	displaySectionHeader,
	displaySectionHeaderWithoutSeason,
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

// One completed game, reduced to what sentence-level linking needs: every name
// that could identify it in recap prose (both teams' region/nickname forms and
// every player who appeared), and its box score URL. See RecapBanner - a recap
// sentence whose names all point at ONE game gets a hover-underline link there.
export type SentenceGame = { href: string; names: string[] };

export const buildSentenceGamesForDay = (
	games: { gid: number; season: number; teams: any[] }[],
	teamInfo: (
		tid: number,
	) => { abbrev?: string; region?: string; name?: string } | undefined,
): SentenceGame[] => {
	const out: SentenceGame[] = [];
	for (const game of games) {
		if (typeof game?.gid !== "number") {
			continue;
		}
		const names: string[] = [];
		let href: string | undefined;
		for (const t of Array.isArray(game.teams) ? game.teams : []) {
			if (typeof t?.tid !== "number") {
				continue;
			}
			const info = t.tid >= 0 ? (t.branding ?? teamInfo(t.tid)) : undefined;
			// Any real team's abbrev anchors the URL; the All-Star Game's roster
			// tids are negative and use the "special" slug like everywhere else.
			if (href === undefined) {
				href = helpers.leagueUrl([
					"game_log",
					info?.abbrev ? `${info.abbrev}_${t.tid}` : "special",
					game.season,
					game.gid,
				]);
			}
			for (const label of [
				`${info?.region ?? ""} ${info?.name ?? ""}`,
				info?.name ?? "",
				info?.region ?? "",
			]) {
				if (label.trim() !== "") {
					names.push(label.trim());
				}
			}
			for (const p of Array.isArray(t.players) ? t.players : []) {
				if (typeof p?.pid === "number" && p.pid >= 0 && p.name) {
					names.push(String(p.name));
				}
			}
		}
		if (href !== undefined && names.length > 0) {
			out.push({ href, names });
		}
	}
	return out;
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

// Split a paragraph into sentences (kept) and the boundaries between them
// (kept too, as plain text, so nothing is lost in reassembly). Boundaries are a
// sentence end followed by a capital-ish opener - so "10.9 seconds" and other
// decimals never split - plus the "·" separator the day recap's sub-headline
// uses between its blurbs. Used by the Markdown renderer to offer each sentence
// for a link of its own.
export const splitSentences = (
	text: string,
): { text: string; boundary: boolean }[] => {
	const parts = text.split(/(\s*·\s*|(?<=[.!?])\s+(?=["'(A-Z]))/);
	const out: { text: string; boundary: boolean }[] = [];
	for (const [i, part] of parts.entries()) {
		if (part !== undefined && part !== "") {
			out.push({ text: part, boundary: i % 2 === 1 });
		}
	}
	return out;
};

// Which games does this run of prose name? Every game name (team or player)
// found in it votes for its game. Matching runs on the prose, so markdown links
// are stripped to their labels first.
const mdToPlain = (s: string): string =>
	s.replace(/\[([^\]]+)]\([^)]*\)/g, "$1");

const matchedGameHrefs = (text: string, games: SentenceGame[]): string[] => {
	const plain = mdToPlain(text);
	const hrefs: string[] = [];
	for (const game of games) {
		const mentioned = game.names.some((name) =>
			new RegExp(String.raw`(?<![\w])${escapeRegex(name)}(?![\w])`).test(plain),
		);
		if (mentioned && !hrefs.includes(game.href)) {
			hrefs.push(game.href);
		}
	}
	return hrefs;
};

// A run of prose naming exactly one game resolves to it; naming none or several
// resolves to nothing rather than to a guess.
export const resolveSentenceGame = (
	sentence: string,
	games: SentenceGame[],
): string | undefined => {
	const hrefs = matchedGameHrefs(sentence, games);
	return hrefs.length === 1 ? hrefs[0] : undefined;
};

// CLAUSES, for the sentences that cover several games at once.
//
// Half a day recap is round-ups - "the Lakers beat the Kings 99-94, the 76ers
// blew out the Heat 119-97, and the Thunder took down the Hawks 89-84" - and a
// whole-sentence rule can only shrug at those, because they belong to no single
// game. Each CLAUSE does belong to one, so an ambiguous sentence is cut at its
// top-level commas, semicolons, colons and "and"s and each piece resolved on
// its own.
//
// Top-level: a comma inside "(sprained knee, out ~13 games)" is part of one
// player's aside, not a new clause, and cutting there would leave "out ~13
// games)" hanging as its own unlinked fragment.
const splitClauses = (text: string): { text: string; boundary: boolean }[] => {
	const out: { text: string; boundary: boolean }[] = [];
	let depth = 0;
	let start = 0;
	let i = 0;

	const cut = (end: number, boundaryLength: number) => {
		if (end > start) {
			out.push({ text: text.slice(start, end), boundary: false });
		}
		if (boundaryLength > 0) {
			out.push({
				text: text.slice(end, end + boundaryLength),
				boundary: true,
			});
		}
		start = end + boundaryLength;
	};

	while (i < text.length) {
		const char = text[i]!;
		if (char === "(" || char === "[") {
			depth += 1;
		} else if (char === ")" || char === "]") {
			depth = Math.max(0, depth - 1);
		} else if (depth === 0) {
			let length = 0;
			if (char === "," || char === ";" || char === ":") {
				length = 1;
				while (text[i + length] === " ") {
					length += 1;
				}
			} else if (text.startsWith(" and ", i)) {
				length = 5;
			}
			if (length > 0) {
				cut(i, length);
				i += length;
				continue;
			}
		}
		i += 1;
	}
	cut(text.length, 0);
	return out;
};

// KEEPING THE MARKDOWN INTACT ACROSS A CUT.
//
// These cuts run through raw markdown, so a span that WRAPS several pieces -
// the sub-headline is one italic run of "·"-separated blurbs - would be left
// with its opening delimiter in the first piece and its closing one in the
// last, and neither would pair up. Both ends then render as literal asterisks
// on screen with nothing italic between them, which is exactly what a day
// recap started doing when sentence links arrived.
//
// So a span left open at a cut is closed there and reopened on the other side:
// "*A · B · C*" becomes "*A* · *B* · *C*", which renders identically and
// leaves every piece independently linkable.
const EMPHASIS = ["**", "__", "*", "_", "`"];

const rebalanceMarkdown = <T extends { text: string }>(segments: T[]): T[] => {
	const open: string[] = [];
	return segments.map((segment) => {
		const prefix = open.join("");
		const { text } = segment;
		for (let i = 0; i < text.length; i++) {
			const delimiter = EMPHASIS.find((d) => text.startsWith(d, i));
			if (delimiter === undefined) {
				continue;
			}
			if (open.at(-1) === delimiter) {
				open.pop();
			} else {
				open.push(delimiter);
			}
			i += delimiter.length - 1;
		}
		const suffix = [...open].reverse().join("");
		return { ...segment, text: `${prefix}${text}${suffix}` };
	});
};

// LINKS ARE OPAQUE TO EVERY CUT AND EVERY COUNT IN THIS MODULE.
//
// A link's label and URL are full of characters that mean something else in
// prose: "[O.J. Mayo](...)" carries a sentence boundary inside its own label
// (period, space, capital), and a roster URL like /roster/GSW_7/2009 carries
// what reads as an emphasis underscore. Splitting through the middle of a link
// printed its halves as raw markdown on the page, and counting its underscore
// had the rebalancer "closing" an italic nobody opened - a stray _ at the end
// of every paragraph with a team link in it. So links are swapped for
// private-use-character placeholders (which no recap text contains) before any splitting,
// and swapped back at the very end - and for name-matching in between, since
// the names inside links are exactly what identifies a game.
const protectLinks = (
	text: string,
): { masked: string; restore: (s: string) => string } => {
	const links: string[] = [];
	const masked = text.replace(/\[[^\]]*]\([^)]*\)/g, (m) => {
		links.push(m);
		return `\uE000${links.length - 1}\uE001`;
	});
	return {
		masked,
		restore: (s) =>
			s.replace(/\uE000(\d+)\uE001/g, (_, i) => links[Number(i)]!),
	};
};

// Every linkable piece of a recap, in order, with the boundaries between them
// kept as plain text so the prose reassembles exactly as written.
//
// A sentence that names one game wins the whole sentence - the best outcome,
// and the common one. Only a sentence naming SEVERAL is broken into clauses,
// so "Tyson Chandler had 31 points, 9 rebounds, and 6 assists as the Bulls beat
// the Clippers" stays one link rather than fragmenting at its stat commas.
export const linkRecapSegments = (
	text: string,
	games: SentenceGame[],
): { text: string; href?: string }[] => {
	const { masked, restore } = protectLinks(text);
	const out: { text: string; href?: string }[] = [];

	for (const sentence of splitSentences(masked)) {
		if (sentence.boundary) {
			out.push({ text: sentence.text });
			continue;
		}

		const hrefs = matchedGameHrefs(restore(sentence.text), games);
		if (hrefs.length === 1) {
			out.push({ text: sentence.text, href: hrefs[0] });
			continue;
		}
		if (hrefs.length === 0) {
			out.push({ text: sentence.text });
			continue;
		}

		for (const clause of splitClauses(sentence.text)) {
			out.push(
				clause.boundary
					? { text: clause.text }
					: {
							text: clause.text,
							href: resolveSentenceGame(restore(clause.text), games),
						},
			);
		}
	}

	return rebalanceMarkdown(out).map((seg) => ({
		...seg,
		text: restore(seg.text),
	}));
};

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
	// Drop the "[YYYY]" labels. The headers still have to be PARSED - they are
	// what scopes each section's links to the right year - so they are hidden at
	// render rather than stripped from the text beforehand.
	hideSeasonLabels = false,
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
			out.push(
				hideSeasonLabels
					? displaySectionHeaderWithoutSeason(line)
					: displaySectionHeader(line),
			);
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
