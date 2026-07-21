import type {
	RecapAverages,
	RecapDaySlate,
	RecapDayStandings,
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "../../worker/util/getDayGamesForRecap.ts";
import { stripOuterCodeFence } from "./stripOuterCodeFence.ts";

// The instructions half of the prompt. Kept as a single editable constant so it
// can be swapped for a different writing brief without touching the data-baking
// logic below.
const INSTRUCTIONS = `You are an expert basketball beat writer. Write a short "Day in the League" front-page recap for EACH league day requested below, plus a lively, ESPN-style recap for EACH game listed below.

THIS IS A FICTIONAL LEAGUE — USE ONLY THE DATA BELOW, NEVER REAL-WORLD KNOWLEDGE. Player and team names may coincide with real people and franchises, but they are NOT them and share no history. A player has no real-world team, hometown, college, draft position, championships, awards, signature moves, nicknames, rivalries, relationships, or reputation — only what the data below states. Do NOT reference or imply anything about a player or team from outside this data: e.g., do not associate Paul Pierce with the Celtics, assume a player's playing style or position, invoke a real-world rivalry, or call anyone the "real-life" anything. Every team a player has played for, every number, and every storyline must come solely from the data provided. Write as if these people and teams exist only within this league and nowhere else.

You are given far more data than you need — box scores, what each player was averaging ENTERING the game (this game not included), past-season career averages, team records and streaks, quarter-by-quarter scoring, each team's last 10 games, injuries (who's out and who got hurt), the pregame betting line (who was favored and by how many), and (in the playoffs) the series and bracket state, or (in the play-in tournament) the play-in stakes. The games may span several league days (each is labeled with its day) — treat each game's data as of the day it was played, and don't frame games from different days as one night's slate. Use whatever makes the best story: momentum swings by quarter, how a performance compares to a player's norms, records and streaks, injury impact, playoff stakes and series context. If a game is labeled a Play-In Tournament game, frame it as one — it is a single win-or-go-home (or win-and-in) game, not a playoff series, so lean into the stated stakes (a playoff berth on the line, elimination looming). If a game is labeled the ALL-STAR GAME, frame it as a fun midseason exhibition — no records, standings, or playoff stakes — and, using ONLY the data in that block, also cover the All-Star Game MVP and the Slam Dunk Contest and Three-Point Contest results (winner, and the contestants named) as part of that same recap. Do NOT list the raw data back.

The pregame betting line is CONTEXT ONLY — use it to judge how surprising the result was (a big underdog winning is an upset; a favorite winning comfortably is unsurprising) and let that shape the tone. NEVER mention the spread, betting line, odds, "favored", "underdog", "pick'em", "chalk", or "cover" in the recap itself. Convey the magnitude through the basketball, not the betting.

ACCURACY IS THE TOP PRIORITY — a single wrong claim ruins the recap, so never trade it for a flashier line:
- Every number, name, and event must be exactly what the data below says. Never round up, inflate, embellish, or invent players, teams, injuries, milestones, or moments that aren't in the data.
- Statistical milestones ("double-double", "triple-double", etc.) are counted ONLY from points, rebounds, assists, steals, and blocks, and a category counts only at 10 or more. Two such categories at 10+ is a double-double; three is a triple-double; four a quadruple-double; five a quintuple-double (which essentially never happens). Each player line already states the milestone it qualifies for in brackets (e.g. "[triple-double: PTS, REB, AST]"); a line with no such tag did NOT record a double-double. Use exactly that — never upgrade it.
- Do NOT describe a player as "near", "flirting with", "almost", "on the verge of", or "-caliber" for a milestone unless the numbers are genuinely one basket/rebound/etc. away in the missing category. A player with 3 steals and 5 blocks is nowhere near a quintuple-double; 21/13/10 with 3 steals and 5 blocks is simply a triple-double. 19 points, 14 assists and 3 rebounds is a double-double, not "triple-double-caliber".
- When in doubt, state the line plainly and move on. An accurate, unglamorous sentence always beats an impressive false one.

Follow these rules EXACTLY:
- Put your ENTIRE reply inside ONE fenced code block so it can be copied in a single click: open with a line of exactly \`\`\`markdown, then the day recaps (if any) and all the game recaps, then a final line of exactly \`\`\`. Nothing before or after the fence — no preamble, no closing summary.
- Inside the fence, write GitHub-flavored Markdown only, with no text outside the day recaps and the per-game recaps.
- FIRST, the DAY-IN-THE-LEAGUE recaps: if a "Day recaps needed" line below lists any league days, write one recap for EACH of those days, oldest first. For each, output a line containing ONLY the marker <!--day:DAY--> (replace DAY with that league day's number, exactly as listed), then a bold one-line headline, then a SHORT article of 2–3 tight paragraphs on THAT day's biggest stories across the league — the marquee results, the best individual performances, upsets, notable streaks, and any standings or playoff/series implications. It's the front page for that one day, high-level and punchy, NOT a game-by-game rundown, and it must draw ONLY on that day's data: its detailed game blocks below (each labeled with its league day) or, for a day whose games are not detailed below, the compact results listed for it under "Results for day recaps". For playoff-race, seeding, and conference-lead context, use the LEAGUE STANDINGS provided for that same day (and ONLY that day's standings) - never state a record or standing that the data doesn't show. Never omit or change a <!--day:DAY--> marker. If no days are listed, skip this and write only game recaps.
- THEN write the per-game recaps. Begin every game recap with a line containing ONLY this marker: <!--game:ID--> (replace ID with that game's number, shown as "GAME <ID>" below). This is how each recap is filed to the correct game — never omit it, never change it.
- After a game's marker, lead with a bold one-line headline, then 2–4 tight paragraphs.
- HEADLINES (both the day recaps and the game recaps): write them like a real newspaper's sports front page, not a template. Lead each headline with the single most specific, concrete thing that actually happened — name the player and the number, or the result and why it mattered — drawn from that day's/game's data. Each headline must be genuinely distinct: do NOT reuse a structure, opening, or pet phrase across headlines, and do NOT default to the same "big line + as the league does X" shape every time. Ban tired sportswriter filler and mood-words that carry no information — including "chalk", "chaos", "crumbles", "business as usual", "statement (win/night)", "split the night/league", "across the map/league", and cutesy alliteration. A plain, specific headline ("Payton's 38 sinks the Spurs") always beats a punchy generic one. Vary it day to day and game to game the way a human writer naturally would.
- Weave the notable numbers into the prose; do not paste a stat table. Bold standout players with **name**.
- Put exactly one blank line between recaps (between each day recap, and between games).`;

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
	// The series length is customizable per round — state it explicitly (with how
	// many wins clinch it) so the recap never assumes best-of-7.
	let format = "";
	if (typeof s.bestOf === "number" && s.bestOf > 0) {
		format =
			s.bestOf === 1
				? " — single game"
				: ` — best-of-${s.bestOf} (first to ${Math.floor(s.bestOf / 2) + 1} wins)`;
	}
	return `Playoffs — Round ${s.round} of ${s.numRounds}${format}${seeds} (before this game: ${leader})`;
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

// The All-Star Game plus the weekend's contests, as one block. The game is a
// fun exhibition (no records/streaks/spread - those are stripped upstream), so
// this leads with that framing, then the box score, MVP, and the dunk and
// three-point contest results.
const allStarBlock = (game: RecapGame): string => {
	const as = game.allStar!;
	// teams[0] is home in ZenGM; both All-Star squads resolve to region
	// "All-Stars" with name "1"/"2", so region+name is an unambiguous label.
	const [home, away] = game.teams;
	const winner = game.teams.find((t) => t.tid === game.winnerTid);
	const ot =
		game.overtimes > 0
			? ` (${game.overtimes === 1 ? "OT" : `${game.overtimes}OT`})`
			: "";

	const lines = [
		`### GAME ${game.gid} (League day ${game.day}): ALL-STAR GAME${ot}`,
		"This is the ALL-STAR GAME — a fun midseason exhibition between two squads of All-Stars. There are NO standings, records, streaks, or playoff stakes; do not frame it as a competitive result.",
		`Final: ${away.region} ${away.name} ${away.pts}, ${home.region} ${home.name} ${home.pts}${
			winner ? ` — ${winner.region} ${winner.name} win` : ""
		}`,
	];

	if (as.mvp) {
		lines.push(`All-Star Game MVP: ${as.mvp}`);
	}
	if (as.dunk) {
		lines.push(
			`Slam Dunk Contest: ${as.dunk.winner ? `${as.dunk.winner} won` : "held"}${
				as.dunk.players.length > 0
					? ` (contestants: ${as.dunk.players.join(", ")})`
					: ""
			}`,
		);
	}
	if (as.three) {
		lines.push(
			`Three-Point Contest: ${
				as.three.winner ? `${as.three.winner} won` : "held"
			}${
				as.three.players.length > 0
					? ` (contestants: ${as.three.players.join(", ")})`
					: ""
			}`,
		);
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

const gameBlock = (game: RecapGame): string => {
	if (game.allStar) {
		return allStarBlock(game);
	}

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

// A compact results slate for a day whose games aren't detailed below (already
// game-recapped), so the AI still has material for that day's recap.
const daySlateBlock = (slate: RecapDaySlate): string => {
	const lines = [`League day ${slate.day}:`];
	for (const g of slate.games) {
		const top = [
			g.topAway ? `${g.topAway.name} ${g.topAway.pts}` : undefined,
			g.topHome ? `${g.topHome.name} ${g.topHome.pts}` : undefined,
		]
			.filter(Boolean)
			.join(", ");
		lines.push(
			`- ${g.away} ${g.awayPts} @ ${g.home} ${g.homePts} (${g.winner} win)${
				top ? ` — leading scorers: ${top}` : ""
			}`,
		);
	}
	return lines.join("\n");
};

const gbText = (gb: number): string =>
	Number.isInteger(gb) ? String(gb) : gb.toFixed(1);

// The full standings, split by conference, as of one day - so a day recap can
// talk about that day's playoff picture accurately.
const standingsBlock = (s: RecapDayStandings): string => {
	const lines = [`Standings as of league day ${s.day}:`];
	for (const conf of s.confs) {
		lines.push("", `${conf.name}:`);
		for (const t of conf.teams) {
			lines.push(
				`${t.rank}. ${t.region} ${t.name} (${t.abbrev}) ${t.won}-${t.lost}${
					t.gb > 0 ? ` — ${gbText(t.gb)} GB` : ""
				}`,
			);
		}
	}
	return lines.join("\n");
};

// The full prompt: instructions + which days need a whole-day recap + the
// standings as of each of those days + every game's data, ready for the
// clipboard. `dayRecapDays` are the league days this run should backfill a "Day
// in the League" recap for (oldest first); empty means game recaps only.
// `daySlates` give compact results for any of those days whose games aren't in
// the detailed blocks (already game-recapped). `standingsByDay` is the
// conference standings as of each day recap day.
export const buildRecapPrompt = (
	games: RecapGame[],
	dayLabel: string,
	dayRecapDays: number[] = [],
	daySlates: RecapDaySlate[] = [],
	standingsByDay: RecapDayStandings[] = [],
): string => {
	const blocks = games.map(gameBlock).join("\n\n");
	const dayLine =
		dayRecapDays.length > 0
			? `Day recaps needed (oldest first): ${dayRecapDays.join(", ")}`
			: "Day recaps needed: none";
	const slateSection =
		daySlates.length > 0
			? `\n\nResults for day recaps whose games aren't detailed below (use these to write those days' recaps):\n\n${daySlates
					.map(daySlateBlock)
					.join("\n\n")}`
			: "";
	const standingsSection =
		standingsByDay.length > 0
			? `\n\nLEAGUE STANDINGS by conference, as of each day a recap is needed (use the matching day's standings for that day's recap):\n\n${standingsByDay
					.map(standingsBlock)
					.join("\n\n")}`
			: "";
	return `${INSTRUCTIONS}

---

${dayLine}${slateSection}${standingsSection}

${games.length} game${games.length === 1 ? "" : "s"} to recap (${dayLabel}):

${blocks}`;
};

// Split a pasted AI response into the per-day recaps plus { gid → recap markdown }
// by its markers. Everything between one marker and the next belongs to it.
// Handles <!--day:DAY--> (a whole-day recap, keyed by league day) and
// <!--game:ID--> markers, in any order.
export const parseRecaps = (
	rawText: string,
): { dayRecaps: Map<number, string>; games: Map<number, string> } => {
	const text = stripOuterCodeFence(rawText);
	const games = new Map<number, string>();
	const dayRecaps = new Map<number, string>();
	const re = /<!--\s*(?:game:\s*(\d+)|day:\s*(\d+))\s*-->/g;
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
		} else if (marker[2] !== undefined) {
			dayRecaps.set(Number(marker[2]), body);
		}
	}

	return { dayRecaps, games };
};
