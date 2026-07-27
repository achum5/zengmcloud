import type {
	RetiredPlayer,
	RetiredPlayersData,
	RetiredSeasonLine,
	RetiredStatLine,
} from "../../worker/util/getRetiredPlayersForRecap.ts";
import { stripOuterCodeFence } from "./stripOuterCodeFence.ts";
import { FICTIONAL_LEAGUE_NOTICE } from "./fictionalLeagueNotice.ts";

// Instructions for the retirement writeups. The key idea: length must follow
// the career. A 20-year Hall of Famer earns a full retrospective; a fringe role
// player gets a paragraph; an undrafted player who never logged a game gets a
// sentence or two. The AI is told this explicitly.
const INSTRUCTIONS = `You are an expert basketball writer. Write a retirement writeup for EACH player listed below — the kind of career retrospective published when a player hangs it up.

${FICTIONAL_LEAGUE_NOTICE}

CRITICAL — scale the length to the career. Do not give everyone the same treatment:
- Hall of Famers and decorated stars: a full retrospective (several paragraphs) — the arc of the career, peak, signature seasons, accolades, legacy, where they rank.
- Solid long-tenured players: a couple of tight paragraphs.
- Role players / journeymen: a short paragraph.
- Players who barely played, and especially undrafted players who never logged a single game: one or two honest sentences. Do not invent a career that isn't there.

Use the data provided: career and playoff stat lines (full box score and advanced metrics), the season-by-season arc with each season's team result, every team, the draft slot, age, college/country, peak rating, awards and rings. Lean on the advanced stats and team results to judge how good each season actually was. Do NOT list the raw data back — write prose.

Follow these rules EXACTLY:
- Put your ENTIRE reply inside ONE fenced code block so it can be copied in a single click: open with a line of exactly \`\`\`markdown, then all the writeups, then a final line of exactly \`\`\`. Nothing before or after the fence — no preamble, no closing summary.
- Inside the fence, write GitHub-flavored Markdown only, with no text outside the per-player writeups.
- Begin every player's writeup with a line containing ONLY this marker: <!--player:ID--> (replace ID with that player's number, shown as "PLAYER <ID>" below). This is how each writeup is filed to the correct player — never omit it, never change it.
- The line straight after the marker is a HEADLINE: a few words, title-style, no ending period, no bold, no brackets, and no year (the year is added automatically). Make it about how this career is remembered ("The quiet exit", "Sixteen years, one team").
- Then a blank line, then the writeup at the length the career warrants.
- Bold the player's name on first mention. Put exactly one blank line between players.`;

const heightText = (inches: number | undefined): string | undefined => {
	if (typeof inches !== "number" || inches <= 0) {
		return undefined;
	}
	return `${Math.floor(inches / 12)}'${inches % 12}"`;
};

const has = (line: RetiredStatLine, key: string): boolean =>
	typeof line[key] === "number";

// The full box + advanced stat line as one compact string, skipping any stat
// that isn't present.
const fullStatText = (line: RetiredStatLine): string => {
	const parts: string[] = [];

	parts.push(`${line.pts ?? 0}/${line.trb ?? 0}/${line.ast ?? 0}`);

	const usage: string[] = [];
	if (has(line, "min")) {
		usage.push(`${line.min} MPG`);
	}
	usage.push(`${line.gp} G`);
	parts.push(usage.join(", "));

	const shooting: string[] = [];
	if (has(line, "fg")) {
		shooting.push(
			`FG ${line.fg}-${line.fga}${has(line, "fgp") ? ` (${line.fgp}%)` : ""}`,
		);
	}
	if (has(line, "tp")) {
		shooting.push(
			`3P ${line.tp}-${line.tpa}${has(line, "tpp") ? ` (${line.tpp}%)` : ""}`,
		);
	}
	if (has(line, "ft")) {
		shooting.push(
			`FT ${line.ft}-${line.fta}${has(line, "ftp") ? ` (${line.ftp}%)` : ""}`,
		);
	}
	if (shooting.length > 0) {
		parts.push(shooting.join(", "));
	}

	const box: string[] = [];
	for (const [key, label] of [
		["orb", "ORB"],
		["drb", "DRB"],
		["stl", "STL"],
		["blk", "BLK"],
		["tov", "TO"],
		["pf", "PF"],
	] as const) {
		if (has(line, key)) {
			box.push(`${label} ${line[key]}`);
		}
	}
	if (box.length > 0) {
		parts.push(box.join(" "));
	}

	const adv: string[] = [];
	for (const [key, label] of [
		["per", "PER"],
		["tsp", "TS%"],
		["usgp", "USG%"],
		["ortg", "ORtg"],
		["drtg", "DRtg"],
		["ows", "OWS"],
		["dws", "DWS"],
		["ws", "WS"],
		["ws48", "WS/48"],
		["obpm", "OBPM"],
		["dbpm", "DBPM"],
		["bpm", "BPM"],
		["vorp", "VORP"],
		["ewa", "EWA"],
		["pm", "+/-"],
	] as const) {
		if (has(line, key)) {
			adv.push(`${label} ${line[key]}`);
		}
	}
	if (adv.length > 0) {
		parts.push(adv.join(", "));
	}

	return parts.join(" · ");
};

const seasonText = (s: RetiredSeasonLine): string => {
	const teams = s.teams
		.map((t) => (t.result ? `${t.abbrev} (${t.result})` : t.abbrev))
		.join("/");
	const age = typeof s.age === "number" ? `, age ${s.age}` : "";
	return `${s.season} ${teams}${age}: ${fullStatText(s.stats)}`;
};

const draftText = (p: RetiredPlayer): string => {
	if (!p.draft) {
		return "Draft: unknown";
	}
	if (p.draft.undrafted) {
		return `Draft: undrafted (${p.draft.year})`;
	}
	return `Draft: ${p.draft.year}, round ${p.draft.round}, pick ${p.draft.pick}`;
};

const playerBlock = (p: RetiredPlayer): string => {
	const bio = [
		p.pos,
		typeof p.ageAtRetirement === "number"
			? `retired at ${p.ageAtRetirement}`
			: undefined,
		heightText(p.heightIn),
		p.college ? `college: ${p.college}` : undefined,
		p.country ? p.country : undefined,
		typeof p.peakOvr === "number" ? `peak ${p.peakOvr} ovr` : undefined,
	]
		.filter(Boolean)
		.join(", ");

	const lines = [
		`### PLAYER ${p.pid}: ${p.name}${p.hof ? " — HALL OF FAMER" : ""}`,
		bio,
		draftText(p),
	];

	if (p.neverPlayed) {
		lines.push("Never played a game in the league.");
	} else {
		lines.push(
			`Career: ${p.seasonsPlayed} season${
				p.seasonsPlayed === 1 ? "" : "s"
			}, ${p.totalGP} games${
				p.firstSeason && p.lastSeason
					? ` (${p.firstSeason}–${p.lastSeason})`
					: ""
			}${p.rings > 0 ? `, ${p.rings} championship${p.rings === 1 ? "" : "s"}` : ""}.`,
		);
		if (p.career) {
			lines.push(`Career per game: ${fullStatText(p.career)}`);
		}
		if (p.playoffs) {
			lines.push(`Playoffs per game: ${fullStatText(p.playoffs)}`);
		}
		if (p.teams.length > 0) {
			lines.push(
				`Teams: ${p.teams
					.map((t) =>
						t.from === t.to
							? `${t.abbrev} (${t.from})`
							: `${t.abbrev} (${t.from}–${t.to})`,
					)
					.join(", ")}`,
			);
		}
		if (p.awards.length > 0) {
			lines.push(
				`Awards: ${p.awards
					.map((a) => (a.count > 1 ? `${a.type} ×${a.count}` : a.type))
					.join("; ")}`,
			);
		}
		if (p.bySeason.length > 0) {
			lines.push(
				"Season by season:",
				...p.bySeason.map((s) => `- ${seasonText(s)}`),
			);
		}
	}

	return lines.filter(Boolean).join("\n");
};

// The full prompt: instructions + every retired player's career data.
export const buildRetiredRecapPrompt = (data: RetiredPlayersData): string => {
	const blocks = data.players.map(playerBlock).join("\n\n");
	return `${INSTRUCTIONS}

---

${data.players.length} player${
		data.players.length === 1 ? "" : "s"
	} retired after the ${data.season} season:

${blocks}`;
};

// Split a pasted AI response into { pid → writeup markdown } by its markers.
export const parseRetiredRecaps = (rawText: string): Map<number, string> => {
	const text = stripOuterCodeFence(rawText);
	const result = new Map<number, string>();
	const re = /<!--\s*player:\s*(\d+)\s*-->/g;
	const markers = [...text.matchAll(re)];

	for (let i = 0; i < markers.length; i++) {
		const marker = markers[i]!;
		const pid = Number(marker[1]);
		const start = marker.index + marker[0].length;
		const end = i + 1 < markers.length ? markers[i + 1]!.index : text.length;
		const recap = text.slice(start, end).trim();
		if (recap) {
			result.set(pid, recap);
		}
	}

	return result;
};
