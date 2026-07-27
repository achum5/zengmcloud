import type {
	RecapPlayer,
	RecapPlayerBatch,
} from "../../worker/util/getPlayerRecapData.ts";
import { stripOuterCodeFence } from "./stripOuterCodeFence.ts";

// The league-wide PLAYER season recap: one short piece of writing per player
// per season, filed into that player's own note under a [year] heading.
//
// The goal is to humanize the league - after a few seasons a player's note
// reads as a career with a shape to it, not a stat line. That only works if the
// AI can see the whole arc, so every player carries their full history into the
// prompt: stats and complete ratings for every season, transactions, awards,
// feats and injuries.
//
// Data is packed DENSELY (short labelled rows, not prose) because a batch is
// dozens of full careers and every wasted token is reply room taken away from
// the last players in the batch.

const INSTRUCTIONS = `You are a basketball writer producing per-player season recaps for a fictional league. Write a recap of the LISTED SEASON for EACH player below.

Length: judge it by how much there is to say. A deep-bench player who barely played might get one sentence. A star, or anyone with a real story that year (a breakout, a collapse, an injury, a trade, an award, a title run, a contract year, a rookie debut, a last season), can get up to two short paragraphs. Most players land in between. Never pad a nothing season into paragraphs.

Each player's data is their career UP TO AND INCLUDING this season: stats by season, full ratings by season (so you can see skills develop or erode), transactions, awards, statistical feats, and injuries. Use that history to give the season meaning — a 19 ppg year reads differently as a breakout, a career year, or the start of a decline. Write as if the season has just ended and nobody knows what happens next.

Write about them as people with careers. Do not dump the data back — weave the numbers that matter into the prose.

Follow these rules EXACTLY:
- Put your ENTIRE reply inside ONE fenced code block: open with a line of exactly \`\`\`markdown, then all the recaps, then a final line of exactly \`\`\`. Nothing before or after the fence — no preamble, no summary.
- Begin every player's recap with a line containing ONLY this marker: <!--player:ID--> (replace ID with that player's number, shown as "PLAYER <ID>" below). This is how each recap is filed to the correct player — never omit it, never change it.
- The line straight after the marker is the HEADING, in this exact form: [YEAR] followed by a short headline, e.g. "[2004] A leap in Sacramento". Use the LISTED SEASON as the year. The headline is a few words, title-style, no ending period, no bold. Make it specific to that player's year ("Hurt again", "The last dance") — never generic like "Season Recap".
- Then a blank line, then the recap itself as plain prose. No stat table, no bullet lists.
- Include EVERY player listed, in the order given. Do not skip anyone, and do not merge players.
- Put exactly one blank line between players.`;

const one = (x: number) => (Math.round(x * 10) / 10).toFixed(1);

const pct = (made: number, attempted: number) =>
	attempted > 0 ? `${Math.round((made / attempted) * 1000) / 10}%` : "-";

// A season's stat line, per game, in a fixed compact order.
const statLine = (s: RecapPlayer["stats"][number]) => {
	const perGame = (v: number) => (s.gp > 0 ? one(v / s.gp) : "0.0");
	return [
		`${s.season}${s.playoffs ? "p" : ""}`,
		s.abbrev,
		`age${s.age}`,
		`${s.gp}g`,
		`${perGame(s.min)}m`,
		`${perGame(s.pts)}p`,
		`${perGame(s.trb)}r`,
		`${perGame(s.ast)}a`,
		`${perGame(s.stl)}s`,
		`${perGame(s.blk)}b`,
		`${perGame(s.tov)}to`,
		`fg${pct(s.fg, s.fga)}`,
		`3p${pct(s.tp, s.tpa)}`,
		`ft${pct(s.ft, s.fta)}`,
		s.per !== undefined ? `per${one(s.per)}` : undefined,
	]
		.filter(Boolean)
		.join(" ");
};

const ratingLine = (r: RecapPlayer["ratings"][number]) => {
	const subs = Object.entries(r.ratings)
		.map(([key, value]) => `${key}${value}`)
		.join(" ");
	return `${r.season} age${r.age} ${r.pos} ovr${r.ovr} pot${r.pot}${
		subs ? ` | ${subs}` : ""
	}`;
};

const playerBlock = (p: RecapPlayer, season: number): string => {
	const lines: string[] = [];
	lines.push(`PLAYER <${p.pid}>`);

	const where =
		p.teamAbbrevs.length > 0 ? p.teamAbbrevs.join(" / ") : "no team";
	lines.push(`${p.name} — ${p.pos}, age ${p.age} in ${season}, ${where}`);

	const bio: string[] = [];
	if (p.born.loc) {
		bio.push(`from ${p.born.loc}`);
	}
	if (p.draft.year) {
		bio.push(
			p.draft.round > 0
				? `drafted ${p.draft.year} rd${p.draft.round} pk${p.draft.pick}${
						p.draft.abbrev ? ` by ${p.draft.abbrev}` : ""
					}`
				: `undrafted (${p.draft.year})`,
		);
	}
	if (p.retiredYear !== undefined) {
		bio.push(`retired ${p.retiredYear}`);
	}
	if (p.hof) {
		bio.push("Hall of Fame");
	}
	if (p.contract) {
		bio.push(
			`contract $${(p.contract.amount / 1000).toFixed(1)}M through ${p.contract.exp}`,
		);
	}
	if (p.injury) {
		bio.push(`injured: ${p.injury.type} (${p.injury.gamesRemaining}g)`);
	}
	if (bio.length > 0) {
		lines.push(bio.join("; "));
	}

	const reg = p.stats.filter((s) => !s.playoffs);
	const post = p.stats.filter((s) => s.playoffs);
	const thisSeason = reg.filter((s) => s.season === season);
	if (thisSeason.length > 0) {
		lines.push("THIS SEASON:");
		for (const s of thisSeason) {
			lines.push(`  ${statLine(s)}`);
		}
		for (const s of post.filter((x) => x.season === season)) {
			lines.push(`  ${statLine(s)}`);
		}
	} else {
		lines.push("THIS SEASON: did not play");
	}

	if (reg.length > 0) {
		lines.push("CAREER (regular season):");
		for (const s of reg) {
			lines.push(`  ${statLine(s)}`);
		}
	}
	if (post.length > 0) {
		lines.push("CAREER (playoffs):");
		for (const s of post) {
			lines.push(`  ${statLine(s)}`);
		}
	}

	if (p.ratings.length > 0) {
		lines.push("RATINGS BY SEASON:");
		for (const r of p.ratings) {
			lines.push(`  ${ratingLine(r)}`);
		}
	}

	if (p.awards.length > 0) {
		lines.push(
			`AWARDS: ${p.awards.map((a) => `${a.season} ${a.type}`).join("; ")}`,
		);
	}
	if (p.transactions.length > 0) {
		lines.push("TRANSACTIONS:");
		for (const t of p.transactions) {
			lines.push(`  ${t}`);
		}
	}
	if (p.feats.length > 0) {
		lines.push(
			`FEATS: ${p.feats.map((f) => `${f.season} ${f.text}`).join("; ")}`,
		);
	}
	if (p.injuries.length > 0) {
		lines.push(
			`INJURY HISTORY: ${p.injuries
				.map((i) => `${i.season} ${i.type} (${i.games}g)`)
				.join("; ")}`,
		);
	}

	return lines.join("\n");
};

export const buildPlayerRecapPrompt = (data: RecapPlayerBatch): string => {
	const header = [
		INSTRUCTIONS,
		"",
		`LISTED SEASON: ${data.season}`,
		`This is batch ${data.batchIndex + 1} of ${data.batchCount} for this season (${data.players.length} players in this batch, ${data.totalPlayers} in the league).`,
		"",
		"=== PLAYERS ===",
	].join("\n");

	return [header, ...data.players.map((p) => playerBlock(p, data.season))].join(
		"\n\n",
	);
};

// Pull each player's recap out of the AI's reply, keyed by pid. Everything from
// one marker up to the next belongs to that player: the first non-empty line is
// the headline, the rest is the body.
export type ParsedPlayerRecap = {
	headline: string;
	body: string;
};

// Strip the decoration an AI reaches for on a heading even when told not to.
// The AI writes "[2004] A leap in Sacramento". The year is dropped here and
// re-added from the season being written, so a wrong or missing year in the
// reply can never end up in the note.
const cleanHeadline = (line: string) =>
	line
		.replace(/^#+\s*/, "")
		.replaceAll("**", "")
		.replace(/^\s*\[\s*\d{4}\s*]\s*/, "")
		.replace(/^\[|]$/g, "")
		.replace(/[.:]\s*$/, "")
		.trim();

export const parsePlayerRecaps = (
	rawText: string,
): Map<number, ParsedPlayerRecap> => {
	const text = stripOuterCodeFence(rawText);
	const out = new Map<number, ParsedPlayerRecap>();

	const re = /<!--\s*player:\s*(\d+)\s*-->/g;
	const markers: { pid: number; start: number; end: number }[] = [];
	let match = re.exec(text);
	while (match !== null) {
		markers.push({
			pid: Number.parseInt(match[1]!),
			start: match.index,
			end: match.index + match[0].length,
		});
		match = re.exec(text);
	}

	for (const [i, marker] of markers.entries()) {
		const bodyEnd = markers[i + 1]?.start ?? text.length;
		const chunk = text.slice(marker.end, bodyEnd).trim();
		if (chunk === "") {
			continue;
		}

		const lines = chunk.split("\n");
		const headline = cleanHeadline(lines[0] ?? "");
		const body = lines.slice(1).join("\n").trim();

		// If the AI ignored the headline instruction and went straight into prose,
		// keep the whole thing as the body rather than eating its first line.
		if (body === "" || headline.length > 80) {
			out.set(marker.pid, { headline: "", body: chunk });
		} else {
			out.set(marker.pid, { headline, body });
		}
	}

	return out;
};
