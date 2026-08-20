import type {
	RecapSeasonData,
	RecapSeasonPlayer,
	RecapSeasonTeam,
} from "../../worker/util/getSeasonRecapData.ts";
import { stripOuterCodeFence } from "./stripOuterCodeFence.ts";
import { FICTIONAL_LEAGUE_NOTICE } from "./fictionalLeagueNotice.ts";

// Instructions for the season-in-review. Kept as one editable constant so the
// brief can change without touching the data-baking below.
const INSTRUCTIONS = `You are an expert basketball writer producing a league-wide season in review. Write a season recap for EACH team listed below.

${FICTIONAL_LEAGUE_NOTICE}

You are given a lot of data per team: the franchise's history (championships, playoff appearances, recent seasons); every transaction that shaped the roster — draft picks, re-signings, free-agent signings, trades, releases — each one tagged in brackets with the part of the calendar it happened in and listed OLDEST FIRST; who left and who arrived versus last season; the team's end-of-season payroll this year and last against the league's salary cap; the record, seed, and points scored and allowed; the exact playoff series results (opponent and games won-lost each round — use these for how far a series went; never guess the number of games); and the key players' season and postseason lines, with ages, ratings, salaries, awards, their own transactions, and any major injury history (50+ games missed, with the season). Use whatever tells the best story — how the season met or defied expectations given the roster and moves, breakout or declining players, the franchise's arc, playoff runs or collapses, and how the offseason set the team up. Do NOT dump the raw data back.

IMPORTANT — tell the story in chronological order, exactly how the data is laid out per team: (1) the OFFSEASON MOVES that BUILT this year's roster (the prior offseason — signings, re-signings, draft picks, trades made BEFORE the season), then (2) the SEASON itself — the regular-season record and how it played out, the IN-SEASON MOVES (trades/cuts/signings made during the year), and (3) the PLAYOFFS. The offseason moves are last summer's build-up that set this team up; weave them in as the season's starting point.

READ THE MOVES AS A SEQUENCE, not as a list. They are in the order they happened, so a move and whatever it made possible sit next to each other. Before you characterize any move, read the ones around it in the same window and check what happened to the payroll. A trade that brings back little, or a veteran cut loose, is often what paid for a signing a few lines later; a big signing usually has something that cleared room for it. The phase tags tell you the order within an offseason — draft, then re-signings, then free agency — and within a season, before or after the trade deadline.

ACCURACY — these matter more than style:
- Every fact must come from the data below. Do not invent trades, signings, contract terms, injuries, quotes, or games.
- Do NOT assert WHY a team made a move — its intentions, its negotiations, its front office's thinking — unless the data says so. State what happened, in what order, and what it cost, and let that speak.
- Do NOT call a move a giveaway, a fleecing, a mistake, or a salary dump unless the data supports it. Whether a team got something back is a question about the whole window of moves, not about one line of it.
- Only a player marked "(retired)" retired. Everyone else who left is playing somewhere else.
- If a team's move list says earlier moves are not shown, do not describe its offseason as if the list were complete.

Follow these rules EXACTLY:
- Put your ENTIRE reply inside ONE fenced code block so it can be copied in a single click: open with a line of exactly \`\`\`markdown, then all the recaps, then a final line of exactly \`\`\`. Nothing before or after the fence — no preamble, no closing summary.
- Inside the fence, write GitHub-flavored Markdown only, with no text outside the per-team recaps.
- Begin every team's recap with a line containing ONLY this marker: <!--team:ID--> (replace ID with that team's number, shown as "TEAM <ID>" below). This is how each recap is filed to the correct team — never omit it, never change it.
- After the marker, lead with a bold one-line headline, then 2–4 tight paragraphs.
- Weave the notable numbers into the prose; do not paste a stat table. Bold standout players with **name**.
- Never state a player's rating number. Ratings are scouting information for you — read them to know how good a player is and describe it in basketball terms, never as "a 78 overall". Statistics and records are fine to quote.
- Put exactly one blank line between teams.`;

// Salaries and payrolls come through in thousands of dollars.
const millions = (thousands: number): string =>
	`$${Math.round(thousands / 100) / 10}M`;

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
		typeof p.salary === "number" ? millions(p.salary) : undefined,
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
	if (p.transactions && p.transactions.length > 0) {
		for (const move of p.transactions) {
			lines.push(`    · Move: ${move}`);
		}
	}
	if (p.majorInjuries && p.majorInjuries.length > 0) {
		for (const inj of p.majorInjuries) {
			lines.push(
				`    · Injury history: ${inj.type}, missed ${inj.games} games (${inj.season})`,
			);
		}
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

// A capped move list must say so, or the recap reads it as the whole offseason.
const omitted = (count: number | undefined): string =>
	count ? ` (${count} earlier ones not shown)` : "";

const teamBlock = (t: RecapSeasonTeam): string => {
	// Laid out chronologically so the recap reads in order: who they are →
	// the prior offseason that built this year's team → the season → the playoffs.
	const lines = [
		`### TEAM ${t.tid}: ${t.region} ${t.name} (${t.abbrev})`,
		franchiseBlock(t),
	];

	// 1) The prior offseason — what built this year's roster (before the season).
	if (t.offseasonMoves.length > 0) {
		lines.push(
			"",
			`Offseason moves that built this season's roster (BEFORE the season), oldest first${omitted(
				t.offseasonMovesOmitted,
			)}:`,
			...t.offseasonMoves.map((m) => `- ${m}`),
		);
	}

	// Who actually turned over, so the shape of the roster change doesn't have to
	// be reconstructed from the wording of every individual move.
	if (t.departed.length > 0 || t.arrived.length > 0) {
		lines.push("", "Roster turnover vs last season:");
		if (t.departed.length > 0) {
			lines.push(`- Gone: ${t.departed.join(", ")}`);
		}
		if (t.arrived.length > 0) {
			lines.push(`- New: ${t.arrived.join(", ")}`);
		}
	}

	const payroll: string[] = [];
	if (typeof t.payroll === "number") {
		payroll.push(`${millions(t.payroll)} this season`);
	}
	if (typeof t.priorPayroll === "number") {
		payroll.push(`${millions(t.priorPayroll)} last season`);
	}
	if (payroll.length > 0) {
		lines.push("", `Payroll: ${payroll.join(", ")}`);
	}

	// 2) The season itself, ending at the playoff result.
	const summary = [`Record: ${record(t)}`];
	if (typeof t.seed === "number") {
		summary.push(`#${t.seed} seed`);
	}
	summary.push(t.madePlayoffs ? t.playoffResult : "missed playoffs");
	if (typeof t.ptsPerGame === "number") {
		summary.push(`${t.ptsPerGame} PPG / ${t.oppPtsPerGame ?? "?"} allowed`);
	}
	lines.push("", `The season: ${summary.join(" · ")}`);

	if (t.playoffSeriesResults.length > 0) {
		const seriesStr = t.playoffSeriesResults
			.map(
				(s) =>
					`Round ${s.round}: ${s.win ? "beat" : "lost to"} ${s.opp} ${s.won}-${s.lost}`,
			)
			.join("; ");
		lines.push(`Playoff series: ${seriesStr}`);
	}

	if (t.inSeasonMoves.length > 0) {
		lines.push(
			"",
			`In-season moves, oldest first${omitted(t.inSeasonMovesOmitted)}:`,
			...t.inSeasonMoves.map((m) => `- ${m}`),
		);
	}

	if (t.players.length > 0) {
		lines.push("", "Key players:", ...t.players.map(playerLine));
	}

	return lines.join("\n");
};

const leagueHeader = (data: RecapSeasonData): string => {
	const lines: string[] = [];
	if (typeof data.salaryCap === "number") {
		const bits = [`salary cap ${millions(data.salaryCap)}`];
		if (typeof data.luxuryTax === "number") {
			bits.push(`luxury tax ${millions(data.luxuryTax)}`);
		}
		if (typeof data.minPayroll === "number") {
			bits.push(`minimum payroll ${millions(data.minPayroll)}`);
		}
		lines.push(`League money: ${bits.join(", ")}`);
	}
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
					(a) => `${a.label} — ${a.player}${a.abbrev ? ` (${a.abbrev})` : ""}`,
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
	} to recap, best record first.
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
