import type {
	RecapSeasonData,
	RecapSeasonPlayer,
	RecapSeasonTeam,
} from "../../worker/util/getSeasonRecapData.ts";
import { stripOuterCodeFence } from "./stripOuterCodeFence.ts";
import { FICTIONAL_LEAGUE_NOTICE } from "./fictionalLeagueNotice.ts";

// Instructions for the season-in-review. Kept as one editable constant so the
// brief can change without touching the data-baking below.
const INSTRUCTIONS = `You are a veteran basketball writer producing the season-in-review for a league. Write a season recap for EACH team listed below.

${FICTIONAL_LEAGUE_NOTICE}

WHAT A GOOD RECAP IS. Find the story of this team's year and tell it. Every season has a throughline: a roster that outplayed its talent or wasted it, a hot start that collapsed, a late surge into the playoffs, a star's breakout, a trade that turned the year, a drought ended, a title defended, a rebuild that finally showed life, a contender that lost its best player for two months. Lead with that story, not with the win-loss record, and build every paragraph around it. A recap that walks through "they signed X, they went 41-41, they lost in the first round" is a failure even when every fact in it is right.

LENGTH. Scale it to the story. The champion, the finalists, the biggest risers and fallers, a drought ended or a collapse get 4-5 paragraphs. A middling season with nothing unusual gets 2-3 tight ones. Never pad.

THE DATA, and what it is for. Each team block gives you these already worked out. Use them: they are the sentences that make a recap worth reading, and every one is checkable.
- EXPECTATIONS: how the roster ranked in strength on opening night and at the end of the season, against where its record ranked. A roster that ranked 22nd and finished with the 6th-best record overachieved; one that ranked 3rd and finished 15th underachieved. This is often the spine of the story. Never quote the strength ranks as ratings or scores; say what they mean ("a roster few rated", "the most talented team in the conference").
- LAST SEASON: last year's record and result, and the swing in wins.
- HOW IT UNFOLDED: the record in each third of the season, the longest winning and losing streaks, the record in close games, the last ten, the biggest win and the worst loss. Use them to give the season a shape: when it turned, when it fell apart, how it finished.
- LEAGUE RANKS: record, offence (points scored), defence (points allowed) and scoring margin, each ranked across the league; the conference and division finish; home and road records.
- FRANCHISE: titles, recent seasons, and any streak or drought this season extended or ended.
- MOVES: every transaction in the order it happened, tagged with the part of the calendar.
- KEY PLAYERS: each one's season line and his line LAST season (so you can see who broke out and who declined), whether he was a rookie, games he missed injured THIS season, where he ranked in the league, how he finished in the award races, his salary and his own moves.
The LEAGUE block above the teams carries every team's record and playoff result, the league leaders and the award races, so you can place each team in the league it actually played in.

CHRONOLOGY. Tell the year in order: the offseason that built the roster, then the season, then the playoffs. But tell it as one story, not as three labelled sections. Do not narrate every transaction. Name the moves that shaped the season (ones involving players who mattered, or that cleared the money for one) and skip the roster filler.

READ THE MOVES AS A SEQUENCE, not as a list. They are in the order they happened, so a move and whatever it made possible sit next to each other. Before you characterize any move, read the ones around it in the same window and check what happened to the payroll. A trade that brings back little, or a veteran cut loose, is often what paid for a signing a few lines later; a big signing usually has something that cleared room for it. The phase tags give the order within an offseason (draft, then re-signings, then free agency) and within a season (before or after the trade deadline).

ACCURACY. These matter more than style:
- Every fact must come from the data below. Do not invent trades, signings, contract terms, injuries, quotes, games or streaks.
- Do NOT assert WHY a team made a move (its intentions, its negotiations, its front office's thinking) unless the data says so. State what happened, in what order, and what it cost, and let that speak.
- Do NOT call a move a giveaway, a fleecing, a mistake or a salary dump unless the data supports it. Whether a team got something back is a question about the whole window of moves, not about one line of it.
- Tie a stretch of the season to an injury or a trade only when the dates line up in the data. Otherwise state both facts and let the reader connect them.
- Only a player marked "(retired)" retired. Everyone else who left is playing somewhere else.
- If a team's move list says earlier moves are not shown, do not describe its offseason as if the list were complete.
- Use the playoff series results for how far each series went. Never guess the number of games.

STYLE.
- Write like someone who watched the season happen: confident and specific. No hedging ("appears to", "seemingly", "it seems"). If something isn't in the data, leave it out.
- Do not open every recap the same way. Never start with the team's name followed by its record.
- Name teams in full the way a writer would ("the Toronto Raptors", "Toronto") using the names in the LEAGUE block, never by abbreviation.
- Weave the numbers into the prose; do not paste a stat table or bullet lists. Bold a standout player's name with **name** the first time it appears, and keep the bolding light.
- Never state a player's rating number. Ratings are scouting information for you: read them to know how good a player is and describe it in basketball terms, never as "a 78 overall". Statistics, records and league ranks are fine to quote.

Follow these rules EXACTLY:
- Put your ENTIRE reply inside ONE fenced code block so it can be copied in a single click: open with a line of exactly \`\`\`markdown, then all the recaps, then a final line of exactly \`\`\`. Nothing before or after the fence: no preamble, no closing summary.
- Inside the fence, write GitHub-flavored Markdown only, with no text outside the per-team recaps.
- Begin every team's recap with a line containing ONLY this marker: <!--team:ID--> (replace ID with that team's number, shown as "TEAM <ID>" below). This is how each recap is filed to the correct team. Never omit it, never change it.
- After the marker, lead with a bold one-line headline that names the story, then the paragraphs.
- Include EVERY team listed below, in the order given. Give each one the room its story needs; do not shorten later teams to fit.
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

const ordinal = (n: number) => {
	const rem100 = n % 100;
	if (rem100 >= 11 && rem100 <= 13) {
		return `${n}th`;
	}
	return `${n}${["th", "st", "nd", "rd"][n % 10] ?? "th"}`;
};

const signed = (n: number) => (n > 0 ? `+${n}` : `${n}`);

const playerLine = (p: RecapSeasonPlayer): string => {
	const tags = [
		p.pos,
		typeof p.age === "number" ? `age ${p.age}` : undefined,
		p.rookie ? "rookie" : undefined,
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
	if (p.prior) {
		lines.push(
			`    · Last season: ${p.prior.pts}/${p.prior.trb}/${p.prior.ast} over ${p.prior.gp} G for ${p.prior.abbrev}`,
		);
	}
	if (p.injuredThisSeason) {
		lines.push(
			`    · Missed ${p.injuredThisSeason.games} games injured this season (${p.injuredThisSeason.types.join(", ")})`,
		);
	}
	if (p.leagueRanks && p.leagueRanks.length > 0) {
		lines.push(`    · League: ${p.leagueRanks.join(", ")}`);
	}
	if (p.awardFinishes && p.awardFinishes.length > 0) {
		lines.push(`    · Award races: ${p.awardFinishes.join("; ")}`);
	}
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
	if (t.streaks && t.streaks.length > 0) {
		lines.push(`This season in franchise history: ${t.streaks.join("; ")}`);
	}
	return lines.join("\n");
};

// A capped move list must say so, or the recap reads it as the whole offseason.
const omitted = (count: number | undefined): string =>
	count ? ` (${count} earlier ones not shown)` : "";

const standingLine = (t: RecapSeasonTeam): string | undefined => {
	const bits: string[] = [];
	if (t.conf) {
		bits.push(
			`${t.conf}${typeof t.confRank === "number" ? ` (${ordinal(t.confRank)})` : ""}`,
		);
	}
	if (t.div) {
		bits.push(
			`${t.div} division${typeof t.divRank === "number" ? ` (${ordinal(t.divRank)})` : ""}`,
		);
	}
	return bits.length > 0 ? `Standing: ${bits.join(", ")}` : undefined;
};

const expectationsLine = (
	t: RecapSeasonTeam,
	numTeams: number,
): string | undefined => {
	const r = t.rosterRank;
	if (!r || !t.ranks) {
		return undefined;
	}
	const bits: string[] = [];
	if (typeof r.start === "number") {
		bits.push(
			`roster strength ranked ${ordinal(r.start)} of ${numTeams} on opening night`,
		);
	}
	if (typeof r.end === "number") {
		bits.push(`${ordinal(r.end)} by the end of the regular season`);
	}
	bits.push(`finished with the ${ordinal(t.ranks.record)}-best record`);
	return `Expectations: ${bits.join(", ")}`;
};

const ranksLine = (t: RecapSeasonTeam): string | undefined => {
	if (!t.ranks) {
		return undefined;
	}
	const parts = [
		`offence ${ordinal(t.ranks.offense)}${typeof t.ptsPerGame === "number" ? ` (${t.ptsPerGame} PPG)` : ""}`,
		`defence ${ordinal(t.ranks.defense)}${typeof t.oppPtsPerGame === "number" ? ` (${t.oppPtsPerGame} allowed)` : ""}`,
		`margin ${ordinal(t.ranks.mov)}${typeof t.mov === "number" ? ` (${signed(t.mov)})` : ""}`,
	];
	const splits = [
		t.home ? `home ${t.home}` : undefined,
		t.away ? `road ${t.away}` : undefined,
		typeof t.avgAge === "number" ? `average age ${t.avgAge}` : undefined,
	].filter(Boolean);
	return `League ranks: ${parts.join(", ")}${splits.length > 0 ? ` · ${splits.join(", ")}` : ""}`;
};

const shapeLine = (t: RecapSeasonTeam): string | undefined => {
	const sh = t.shape;
	if (!sh) {
		return undefined;
	}
	const bits = [
		sh.stretches.map((x) => `${x.label} ${x.won}-${x.lost}`).join(", "),
		`longest winning streak ${sh.longestWinStreak}, longest losing streak ${sh.longestLosingStreak}`,
		`close games (5 points or fewer) ${sh.close.won}-${sh.close.lost}`,
		sh.lastTen ? `last ten ${sh.lastTen.won}-${sh.lastTen.lost}` : undefined,
		sh.biggestWin ? `biggest win ${sh.biggestWin}` : undefined,
		sh.worstLoss ? `worst loss ${sh.worstLoss}` : undefined,
	].filter(Boolean);
	return `How it unfolded: ${bits.join("; ")}`;
};

const teamBlock = (t: RecapSeasonTeam, numTeams: number): string => {
	// Laid out chronologically so the recap reads in order: who they are →
	// the prior offseason that built this year's team → the season → the playoffs.
	const lines = [
		`### TEAM ${t.tid}: ${t.region} ${t.name} (${t.abbrev})`,
		franchiseBlock(t),
	];

	if (t.lastSeason) {
		const swing = t.won - t.lastSeason.won;
		lines.push(
			`Last season: ${t.lastSeason.won}-${t.lastSeason.lost}${
				t.lastSeason.result ? `, ${t.lastSeason.result}` : ""
			} (${swing === 0 ? "same win total" : `${signed(swing)} wins this season`})`,
		);
	}

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
	lines.push("", `The season: ${summary.join(" · ")}`);
	for (const line of [
		expectationsLine(t, numTeams),
		standingLine(t),
		ranksLine(t),
		shapeLine(t),
	]) {
		if (line) {
			lines.push(line);
		}
	}

	if (t.inSeasonMoves.length > 0) {
		lines.push(
			"",
			`In-season moves, oldest first${omitted(t.inSeasonMovesOmitted)}:`,
			...t.inSeasonMoves.map((m) => `- ${m}`),
		);
	}

	// 3) The playoffs.
	if (t.playoffSeriesResults.length > 0) {
		const seriesStr = t.playoffSeriesResults
			.map(
				(s) =>
					`Round ${s.round}: ${s.win ? "beat" : "lost to"} ${s.opp} ${s.won}-${s.lost}`,
			)
			.join("; ");
		lines.push("", `Playoff series: ${seriesStr}`);
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

	const standings = data.standings ?? [];
	if (standings.length > 0) {
		lines.push("", "STANDINGS (best record first):");
		const byConf = new Map<string, typeof standings>();
		for (const row of standings) {
			const key = row.conf ?? "";
			byConf.set(key, [...(byConf.get(key) ?? []), row]);
		}
		for (const [conf, rows] of byConf) {
			if (conf) {
				lines.push(conf);
			}
			for (const row of rows) {
				lines.push(
					`  ${row.abbrev} = ${row.region} ${row.name}: ${row.won}-${row.lost}, ${row.result}`,
				);
			}
		}
	}

	const leaders = data.leaders ?? [];
	if (leaders.length > 0) {
		lines.push("", "LEAGUE LEADERS (per game, qualified players):");
		for (const row of leaders) {
			lines.push(
				`  ${row.label}: ${row.players
					.map((p, i) => `${i + 1}. ${p.name} ${p.abbrev} ${p.value}`)
					.join(", ")}`,
			);
		}
	}

	const races = data.awardRaces ?? [];
	if (races.length > 0) {
		lines.push("", "AWARD RACES (finishing order):");
		for (const race of races) {
			lines.push(
				`  ${race.name}: ${race.players
					.map((p, i) => `${i + 1}. ${p.name} ${p.abbrev}`)
					.join(", ")}`,
			);
		}
	}
	return lines.join("\n");
};

// The full prompt: instructions + league context + the teams to write.
export const buildSeasonRecapPrompt = (data: RecapSeasonData): string => {
	const header = leagueHeader(data);
	const numTeams = data.numTeams ?? data.teams.length;
	const blocks = data.teams.map((t) => teamBlock(t, numTeams)).join("\n\n");
	const scope =
		data.teams.length < numTeams
			? `${data.teams.length} of the league's ${numTeams} teams to recap.`
			: `${data.teams.length} team${data.teams.length === 1 ? "" : "s"} to recap.`;
	return `${INSTRUCTIONS}

---

${data.season} season in review. ${scope}
${header ? `\n=== LEAGUE ${data.season} ===\n${header}\n` : ""}
=== TEAMS ===

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
