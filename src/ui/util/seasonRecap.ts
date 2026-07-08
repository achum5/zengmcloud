import type {
	RecapSeasonData,
	RecapSeasonPlayer,
	RecapSeasonTeam,
} from "../../worker/util/getSeasonRecapData.ts";
import { stripOuterCodeFence } from "./stripOuterCodeFence.ts";

// Instructions for the season-in-review. Kept as one editable constant so the
// brief can change without touching the data-baking below.
const INSTRUCTIONS = `You are an expert basketball writer producing a league-wide season in review. Write a season recap for EACH team listed below.

You are given a lot of data per team: the team's record and playoff result, its seed, its key players' season and postseason lines (with ages, ratings, and any awards), the franchise's history (championships, playoff appearances, recent seasons), and the transactions that shaped the team. Use whatever tells the best story — how the season met or defied expectations given the roster and moves, breakout or declining players, the franchise's arc, playoff runs or collapses, and how the offseason set the team up. Do NOT dump the raw data back.

IMPORTANT — how to treat the transactions: each team lists "Offseason moves" and "In-season moves". The OFFSEASON MOVES are the signings, re-signings, draft picks, and trades that BUILT this season's roster — treat them as THIS season's offseason (they are what the team did to prepare for this year). The IN-SEASON MOVES are trades and cuts made during the season itself. Weave both into the narrative where they matter.

Follow these rules EXACTLY:
- Put your ENTIRE reply inside ONE fenced code block so it can be copied in a single click: open with a line of exactly \`\`\`markdown, then all the recaps, then a final line of exactly \`\`\`. Nothing before or after the fence — no preamble, no closing summary.
- Inside the fence, write GitHub-flavored Markdown only, with no text outside the per-team recaps.
- Begin every team's recap with a line containing ONLY this marker: <!--team:ID--> (replace ID with that team's number, shown as "TEAM <ID>" below). This is how each recap is filed to the correct team — never omit it, never change it.
- After the marker, lead with a bold one-line headline, then 2–4 tight paragraphs.
- Weave the notable numbers into the prose; do not paste a stat table. Bold standout players with **name**.
- Put exactly one blank line between teams.`;

const record = (t: RecapSeasonTeam): string => {
	const parts = [`${t.won}-${t.lost}`];
	if (t.otl) {
		parts.push(`${t.otl} OTL`);
	}
	if (t.tied) {
		parts.push(`${t.tied} T`);
	}
	return parts.join(", ");
};

const playerLine = (p: RecapSeasonPlayer): string => {
	const tags = [
		p.pos,
		typeof p.age === "number" ? `age ${p.age}` : undefined,
		typeof p.ovr === "number" && typeof p.pot === "number"
			? `${p.ovr}/${p.pot} ovr/pot`
			: undefined,
	]
		.filter(Boolean)
		.join(", ");
	const head = `- ${p.name}${tags ? ` (${tags})` : ""}: ${p.pts}/${p.trb}/${p.ast} on ${p.fgp}% FG, ${p.tpp}% 3P, ${p.ftp}% FT (${p.stl} STL, ${p.blk} BLK, ${p.tov} TO${
		typeof p.per === "number" ? `, ${p.per} PER` : ""
	}, ${p.min} MPG over ${p.gp} G)`;
	const lines = [head];
	if (p.playoff) {
		lines.push(
			`    · Playoffs: ${p.playoff.pts}/${p.playoff.trb}/${p.playoff.ast} over ${p.playoff.gp} G`,
		);
	}
	if (p.awards && p.awards.length > 0) {
		lines.push(`    · Awards: ${p.awards.join(", ")}`);
	}
	return lines.join("\n");
};

const franchiseBlock = (t: RecapSeasonTeam): string => {
	const f = t.franchise;
	const bits = [
		`${f.championships} title${f.championships === 1 ? "" : "s"}`,
		f.lastChampionship ? `last in ${f.lastChampionship}` : "none yet",
		`${f.playoffAppearances} playoff appearances`,
		`${f.finalsAppearances} finals`,
		`all-time ${f.totalWon}-${f.totalLost}`,
	];
	const lines = [`Franchise: ${bits.join(", ")}.`];
	if (f.recent.length > 0) {
		const recent = f.recent
			.map((r) => `${r.season}: ${r.won}-${r.lost} (${r.result})`)
			.join("; ");
		lines.push(`Recent seasons: ${recent}`);
	}
	return lines.join("\n");
};

const teamBlock = (t: RecapSeasonTeam): string => {
	const header = [`### TEAM ${t.tid}: ${t.region} ${t.name} (${t.abbrev})`];

	const summary = [`Record: ${record(t)}`];
	if (typeof t.seed === "number") {
		summary.push(`#${t.seed} seed`);
	}
	summary.push(t.madePlayoffs ? t.playoffResult : "missed playoffs");
	if (typeof t.ptsPerGame === "number") {
		summary.push(
			`${t.ptsPerGame} PPG / ${t.oppPtsPerGame ?? "?"} allowed`,
		);
	}

	const lines = [header[0]!, summary.join(" · "), franchiseBlock(t)];

	if (t.offseasonMoves.length > 0) {
		lines.push(
			"",
			"Offseason moves (built this season's roster):",
			...t.offseasonMoves.map((m) => `- ${m}`),
		);
	}
	if (t.inSeasonMoves.length > 0) {
		lines.push("", "In-season moves:", ...t.inSeasonMoves.map((m) => `- ${m}`));
	}

	if (t.players.length > 0) {
		lines.push("", "Key players:", ...t.players.map(playerLine));
	}

	return lines.join("\n");
};

const leagueHeader = (data: RecapSeasonData): string => {
	const lines: string[] = [];
	if (data.champ) {
		lines.push(
			`Champion: ${data.champ.region} ${data.champ.name} (${data.champ.abbrev})`,
		);
	}
	if (data.runnerUp) {
		lines.push(
			`Runner-up: ${data.runnerUp.region} ${data.runnerUp.name} (${data.runnerUp.abbrev})`,
		);
	}
	if (data.awards.length > 0) {
		lines.push(
			`Awards: ${data.awards
				.map(
					(a) =>
						`${a.label} — ${a.player}${a.abbrev ? ` (${a.abbrev})` : ""}`,
				)
				.join("; ")}`,
		);
	}
	return lines.join("\n");
};

// The full prompt: instructions + league context + every team's data.
export const buildSeasonRecapPrompt = (data: RecapSeasonData): string => {
	const header = leagueHeader(data);
	const blocks = data.teams.map(teamBlock).join("\n\n");
	return `${INSTRUCTIONS}

---

${data.season} season in review — ${data.teams.length} team${
		data.teams.length === 1 ? "" : "s"
	} to recap.
${header ? `\n${header}\n` : ""}
${blocks}`;
};

// Split a pasted AI response into { tid → recap markdown } by its team markers.
export const parseSeasonRecaps = (rawText: string): Map<number, string> => {
	const text = stripOuterCodeFence(rawText);
	const result = new Map<number, string>();
	const re = /<!--\s*team:\s*(\d+)\s*-->/g;
	const markers = [...text.matchAll(re)];

	for (let i = 0; i < markers.length; i++) {
		const marker = markers[i]!;
		const tid = Number(marker[1]);
		const start = marker.index + marker[0].length;
		const end = i + 1 < markers.length ? markers[i + 1]!.index : text.length;
		const recap = text.slice(start, end).trim();
		if (recap) {
			result.set(tid, recap);
		}
	}

	return result;
};
