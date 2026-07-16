import type {
	RecapAverages,
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "../../worker/util/getDayGamesForRecap.ts";
import { stripOuterCodeFence } from "./stripOuterCodeFence.ts";

// The instructions half of the prompt. Kept as a single editable constant so it
// can be swapped for a different writing brief without touching the data-baking
// logic below.
const INSTRUCTIONS = `You are an expert basketball beat writer. Write TWO things: (1) a short "Day in the League" front-page recap of the whole slate, and (2) a lively, ESPN-style recap for EACH game listed below.

You are given far more data than you need — box scores, what each player was averaging ENTERING the game (this game not included), past-season career averages, team records and streaks, quarter-by-quarter scoring, each team's last 10 games, injuries (who's out and who got hurt), the pregame betting line (who was favored and by how many), and (in the playoffs) the series and bracket state, or (in the play-in tournament) the play-in stakes. The games may span several league days (each is labeled with its day) — treat each game's data as of the day it was played, and don't frame games from different days as one night's slate. Use whatever makes the best story: momentum swings by quarter, how a performance compares to a player's norms, records and streaks, injury impact, playoff stakes and series context. If a game is labeled a Play-In Tournament game, frame it as one — it is a single win-or-go-home (or win-and-in) game, not a playoff series, so lean into the stated stakes (a playoff berth on the line, elimination looming). Do NOT list the raw data back.

The pregame betting line is CONTEXT ONLY — use it to judge how surprising the result was (a big underdog winning is an upset; a favorite rolling is chalk) and let that shape the tone. NEVER mention the spread, betting line, odds, "favored", "underdog", "pick'em", or "cover" in the recap itself. Convey the magnitude through the basketball, not the betting.

ACCURACY IS THE TOP PRIORITY — a single wrong claim ruins the recap, so never trade it for a flashier line:
- Every number, name, and event must be exactly what the data below says. Never round up, inflate, embellish, or invent players, teams, injuries, milestones, or moments that aren't in the data.
- Statistical milestones ("double-double", "triple-double", etc.) are counted ONLY from points, rebounds, assists, steals, and blocks, and a category counts only at 10 or more. Two such categories at 10+ is a double-double; three is a triple-double; four a quadruple-double; five a quintuple-double (which essentially never happens). Each player line already states the milestone it qualifies for in brackets (e.g. "[triple-double: PTS, REB, AST]"); a line with no such tag did NOT record a double-double. Use exactly that — never upgrade it.
- Do NOT describe a player as "near", "flirting with", "almost", "on the verge of", or "-caliber" for a milestone unless the numbers are genuinely one basket/rebound/etc. away in the missing category. A player with 3 steals and 5 blocks is nowhere near a quintuple-double; 21/13/10 with 3 steals and 5 blocks is simply a triple-double. 19 points, 14 assists and 3 rebounds is a double-double, not "triple-double-caliber".
- When in doubt, state the line plainly and move on. An accurate, unglamorous sentence always beats an impressive false one.

Follow these rules EXACTLY:
- Put your ENTIRE reply inside ONE fenced code block so it can be copied in a single click: open with a line of exactly \`\`\`markdown, then the day recap and all the game recaps, then a final line of exactly \`\`\`. Nothing before or after the fence — no preamble, no closing summary.
- Inside the fence, write GitHub-flavored Markdown only, with no text outside the day recap and the per-game recaps.
- START with the DAY-IN-THE-LEAGUE recap: a line containing ONLY the marker <!--day-->, then a bold one-line headline, then a SHORT article of 2–3 tight paragraphs recapping the day across the whole league — the marquee results, the best individual performances, upsets, notable streaks, and any standings or playoff/series implications. It's the front-page story of the day, high-level and punchy, NOT a game-by-game rundown. Draw only on the games below. Never omit or change the <!--day--> marker.
- THEN write the per-game recaps. Begin every game recap with a line containing ONLY this marker: <!--game:ID--> (replace ID with that game's number, shown as "GAME <ID>" below). This is how each recap is filed to the correct game — never omit it, never change it.
- After a game's marker, lead with a bold one-line headline, then 2–4 tight paragraphs.
- Weave the notable numbers into the prose; do not paste a stat table. Bold standout players with **name**.
- Put exactly one blank line between the day recap and the first game, and between games.`;

// Strip any HTML tags (ZenGM's clutch-play strings contain <a> links).
const stripHtml = (s: string): string =>
	s
		.replace(/<[^>]*>/g, "")
		.replace(/\s+/g, " ")
		.trim();

const avg = (a: RecapAverages): string =>
	`${a.pts}/${a.reb}/${a.ast} on ${a.fgp}% FG, ${a.tpp}% 3P, ${a.ftp}% FT (${a.stl} STL, ${a.blk} BLK, ${a.tov} TO, ${a.min} MPG over ${a.gp} G)`;

const injuryTag = (p: RecapPlayer): string => {
	if (!p.injury) {
		return "";
	}
	if (p.injury.newThisGame) {
		return ` [left injured: ${p.injury.type}, out ~${p.injury.gamesRemaining}]`;
	}
	if (p.injury.playingThrough) {
		return ` [played through: ${p.injury.type}]`;
	}
	return "";
};

// Precisely label the "double-double" family so the AI never has to count it
// (and can't inflate it): a category counts ONLY at 10+, across the five
// double-eligible stats. 2 categories → double-double, 3 → triple-double,
// 4 → quadruple-double, 5 → quintuple-double. Nothing below 2 gets a tag, so a
// line with no tag provably had no double-double - the AI can rely on that
// instead of eyeballing "near a triple-double" from the raw numbers.
const doubleTag = (p: RecapPlayer): string => {
	const cats: [string, number][] = [
		["PTS", p.pts],
		["REB", p.reb],
		["AST", p.ast],
		["STL", p.stl],
		["BLK", p.blk],
	];
	const hit = cats.filter(([, v]) => (v ?? 0) >= 10).map(([k]) => k);
	const name =
		hit.length >= 5
			? "quintuple-double"
			: hit.length === 4
				? "quadruple-double"
				: hit.length === 3
					? "triple-double"
					: hit.length === 2
						? "double-double"
						: undefined;
	return name ? ` [${name}: ${hit.join(", ")}]` : "";
};

const playerLine = (p: RecapPlayer): string => {
	const lines = [
		`- ${p.name}: ${p.pts} PTS, ${p.reb} REB, ${p.ast} AST, ${p.stl} STL, ${p.blk} BLK, ${p.tov} TO (${p.fg}/${p.fga} FG, ${p.tp}/${p.tpa} 3P, ${p.ft}/${p.fta} FT, ${p.min} min)${doubleTag(p)}${injuryTag(p)}`,
	];
	if (p.seasonAvg) {
		lines.push(`    · Season avg entering this game: ${avg(p.seasonAvg)}`);
	}
	if (p.playoffAvg) {
		lines.push(`    · Playoff avg entering this game: ${avg(p.playoffAvg)}`);
	}
	if (p.career && p.career.length > 0) {
		const career = p.career
			.map((c) => {
				const tag = [
					c.teams && c.teams.length > 0 ? c.teams.join("/") : undefined,
					typeof c.age === "number" ? `age ${c.age}` : undefined,
				]
					.filter(Boolean)
					.join(", ");
				return `${c.season}${tag ? ` (${tag})` : ""}: ${c.pts}/${c.reb}/${c.ast}, ${c.fgp}% FG (${c.gp} G)`;
			})
			.join("; ");
		lines.push(`    · Career by season (past seasons): ${career}`);
	}
	return lines.join("\n");
};

const last10Line = (t: RecapTeam): string | undefined => {
	if (!t.last10 || t.last10.length === 0) {
		return undefined;
	}
	let won = 0;
	let lost = 0;
	for (const g of t.last10) {
		if (g.won) {
			won += 1;
		} else {
			lost += 1;
		}
	}
	const games = t.last10
		.map(
			(g) =>
				`${g.won ? "W" : "L"} ${g.home ? "vs" : "@"} ${g.opp} ${g.pts}-${g.oppPts}`,
		)
		.join(", ");
	return `Last 10 (${won}-${lost}): ${games}`;
};

const teamBlock = (t: RecapTeam): string => {
	const header = [`${t.abbrev} — ${t.region} ${t.name} (${t.pts} pts`];
	if (t.record) {
		header.push(`, ${t.record.won}-${t.record.lost}`);
	}
	if (t.streak && t.streak.count > 0) {
		header.push(`, ${t.streak.won ? "W" : "L"}${t.streak.count}`);
	}
	if (typeof t.seed === "number") {
		header.push(`, #${t.seed} seed`);
	}
	header.push("):");

	const lines = [header.join("")];
	if (t.ptsQtrs && t.ptsQtrs.length > 0) {
		lines.push(`By quarter: ${t.ptsQtrs.join(" | ")}`);
	}
	const l10 = last10Line(t);
	if (l10) {
		lines.push(l10);
	}
	if (t.injuries && t.injuries.length > 0) {
		lines.push(
			`Out (injury): ${t.injuries
				.map((i) => `${i.name} (${i.type}, ~${i.gamesRemaining} out)`)
				.join(", ")}`,
		);
	}
	lines.push(...t.players.map(playerLine));
	return lines.join("\n");
};

const seriesLine = (game: RecapGame): string | undefined => {
	const s = game.series;
	if (!s) {
		return undefined;
	}
	const leader =
		s.homeWon === s.awayWon
			? `series tied ${s.homeWon}-${s.awayWon}`
			: s.homeWon > s.awayWon
				? `${s.homeAbbrev} leads ${s.homeWon}-${s.awayWon}`
				: `${s.awayAbbrev} leads ${s.awayWon}-${s.homeWon}`;
	const seeds =
		typeof s.homeSeed === "number" && typeof s.awaySeed === "number"
			? ` — #${s.awaySeed} ${s.awayAbbrev} at #${s.homeSeed} ${s.homeAbbrev}`
			: "";
	return `Playoffs — Round ${s.round} of ${s.numRounds}${seeds} (before this game: ${leader})`;
};

// A one-line stakes description for a play-in tournament game. This is a
// single win-or-go-home (or advance) game, NOT a series - spell out what each
// team is playing for so the recap frames it as a play-in game.
const playInLine = (game: RecapGame): string | undefined => {
	const p = game.playIn;
	if (!p) {
		return undefined;
	}
	const seeds =
		typeof p.homeSeed === "number" && typeof p.awaySeed === "number"
			? ` — #${p.awaySeed} ${p.awayAbbrev} at #${p.homeSeed} ${p.homeAbbrev}`
			: "";
	const prize =
		typeof p.prizeSeed === "number"
			? `the #${p.prizeSeed} seed`
			: "a playoff spot";
	let stakes: string;
	if (p.kind === "seed7v8") {
		stakes = `Win-and-in: the winner clinches ${prize}; the loser drops to the final play-in game (still alive).`;
	} else if (p.kind === "seed9v10") {
		stakes = `Win-or-go-home: the winner advances to the final play-in game; the loser is eliminated.`;
	} else {
		stakes = `Win-or-go-home for the last playoff spot: the winner clinches ${prize}; the loser is eliminated.`;
	}
	return `Play-In Tournament${seeds}. ${stakes}`;
};

// The pregame betting line, so the recap can frame the result against
// expectations (an upset, or chalk holding up). Undefined if we have no spread.
const spreadLine = (game: RecapGame): string | undefined => {
	const s = game.spread;
	if (!s) {
		return undefined;
	}
	if (s.points === 0) {
		return "Pregame line: pick'em (evenly matched)";
	}
	const fav = game.teams.find((t) => t.tid === s.favTid);
	const favName = fav ? `${fav.region} ${fav.name}` : "the favorite";
	return `Pregame line: ${favName} favored by ${s.points}`;
};

const gameBlock = (game: RecapGame): string => {
	// teams[0] is the home team in ZenGM; list the visitor first ("away @ home").
	const [home, away] = game.teams;
	const winner = game.teams.find((t) => t.tid === game.winnerTid);
	const ot =
		game.overtimes > 0
			? ` (${game.overtimes === 1 ? "OT" : `${game.overtimes}OT`})`
			: "";

	const lines = [
		`### GAME ${game.gid} (League day ${game.day}): ${away.region} ${away.name} @ ${home.region} ${home.name}${ot}`,
		`Final: ${away.abbrev} ${away.pts}, ${home.abbrev} ${home.pts}${
			winner ? ` — ${winner.region} ${winner.name} win` : ""
		}`,
	];

	const series = seriesLine(game);
	if (series) {
		lines.push(series);
	}

	const playIn = playInLine(game);
	if (playIn) {
		lines.push(playIn);
	}

	const spread = spreadLine(game);
	if (spread) {
		lines.push(spread);
	}

	lines.push("", teamBlock(away), "", teamBlock(home));

	if (game.clutchPlays.length > 0) {
		lines.push(
			"",
			"Notable plays:",
			...game.clutchPlays.map((c) => `- ${stripHtml(c)}`),
		);
	}

	return lines.join("\n");
};

// The full prompt: instructions + every game's data, ready for the clipboard.
export const buildRecapPrompt = (
	games: RecapGame[],
	dayLabel: string,
): string => {
	const blocks = games.map(gameBlock).join("\n\n");
	return `${INSTRUCTIONS}

---

${games.length} game${games.length === 1 ? "" : "s"} to recap (${dayLabel}):

${blocks}`;
};

// Split a pasted AI response into the day recap plus { gid → recap markdown } by
// its markers. Everything between one marker and the next belongs to it. Handles
// both <!--day--> (the whole-day recap) and <!--game:ID--> markers, in any order.
export const parseRecaps = (
	rawText: string,
): { dayRecap: string | undefined; games: Map<number, string> } => {
	const text = stripOuterCodeFence(rawText);
	const games = new Map<number, string>();
	let dayRecap: string | undefined;
	const re = /<!--\s*(?:game:\s*(\d+)|day)\s*-->/g;
	const markers = [...text.matchAll(re)];

	for (let i = 0; i < markers.length; i++) {
		const marker = markers[i]!;
		const start = marker.index + marker[0].length;
		const end = i + 1 < markers.length ? markers[i + 1]!.index : text.length;
		const body = text.slice(start, end).trim();
		if (!body) {
			continue;
		}
		if (marker[1] !== undefined) {
			games.set(Number(marker[1]), body);
		} else {
			dayRecap = body;
		}
	}

	return { dayRecap, games };
};
