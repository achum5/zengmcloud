import type {
	RecapDraftInfo,
	RecapPlayer,
	RecapPlayerBatch,
	RecapRetirement,
} from "../../worker/util/getPlayerRecapData.ts";
import { stripOuterCodeFence } from "./stripOuterCodeFence.ts";
import { FICTIONAL_LEAGUE_NOTICE } from "./fictionalLeagueNotice.ts";

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

${FICTIONAL_LEAGUE_NOTICE}

Length: judge it by how much there is to say. A deep-bench player who barely played might get one sentence. A star, or anyone with a real story that year (a breakout, a collapse, an injury, a trade, an award, a title run, a contract year, a rookie debut, a last season), can get up to two short paragraphs. Most players land in between. Never pad a nothing season into paragraphs.

Each player's data is their career UP TO AND INCLUDING this season: stats by season, full ratings by season (so you can see skills develop or erode), transactions, awards, statistical feats, and injuries. Anything he missed time with THIS season is listed separately as INJURIES THIS SEASON with the games lost — if it's there, it is part of the story, and a year cut short by injury should never read as a quiet decline. Use that history to give the season meaning — a 19 ppg year reads differently as a breakout, a career year, or the start of a decline. Write as if the season has just ended and nobody knows what happens next.

Every stat line carries the team's record and how that team's year ended, and the league standings for this season are listed above the players. Use that context where it makes the recap better: 24 ppg on a 19-63 team is a different story from 24 ppg on a title winner, and a role player's year is often best told through what his team was chasing. Keep the focus on the PLAYER — team context is there to give his season stakes, not to become a team recap.

Players drafted this season have a DRAFTED block: where they went, how that team just finished, and the roster they are joining. For those rookies, say something about the landing spot — the role waiting for them, who they sit behind or alongside, whether the fit is natural or awkward, what the team appears to need. Judge it from the roster given; do not invent teammates.

Write about them as people with careers. Do not dump the data back — weave the numbers that matter into the prose.

RETIRING PLAYERS GET TWO PIECES. A player marked RETIRING AFTER THIS SEASON has just played his last season, and his block carries his career totals. Write his season recap exactly like everyone else's, and then a SECOND, separate piece: the retirement writeup, the kind of career retrospective published when a player hangs it up. Scale that one to the career, and do not give everyone the same treatment:
- Hall of Famers and decorated stars: a full retrospective, several paragraphs — the arc, the peak, the signature seasons, the accolades, how he is remembered.
- Solid long-tenured players: a couple of tight paragraphs.
- Role players and journeymen: a short paragraph.
- Players who barely played, and especially undrafted players who never logged a single game: one or two honest sentences. Do not invent a career that isn't there.

Follow these rules EXACTLY:
- Put your ENTIRE reply inside ONE fenced code block: open with a line of exactly \`\`\`markdown, then all the recaps, then a final line of exactly \`\`\`. Nothing before or after the fence — no preamble, no summary.
- Begin every player's recap with a line containing ONLY this marker: <!--player:ID--> (replace ID with that player's number, shown as "PLAYER <ID>" below). This is how each recap is filed to the correct player — never omit it, never change it.
- Straight after a <!--player:ID--> marker, write the season recap as plain prose. NO headline, NO title, NO heading line, no bold lead-in, no year — start with the first sentence of the recap itself. No stat table, no bullet lists.
- For a RETIRING player only, add the retirement writeup after his season recap under a DIFFERENT marker line: <!--retired:ID--> (same ID). This one DOES get a headline: the line straight after the marker is a few words, title-style, no ending period, no bold, no brackets and no year, about how the CAREER is remembered ("The quiet exit", "Sixteen years, one team"). Then a blank line, then the writeup.
- Include EVERY player listed, in the order given. Do not skip anyone, and do not merge players.
- Put exactly one blank line between pieces.`;

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
		// What the team did that year, so the career reads with stakes attached.
		s.teamResult ? `[${s.teamResult}]` : undefined,
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

// Where a rookie landed and what he walked into. Only present for the season's
// own draft class, so it costs nothing for everyone else.
const draftBlock = (d: RecapDraftInfo): string[] => {
	const lines: string[] = [];
	lines.push(
		`DRAFTED: rd${d.round} pk${d.pick}${
			d.overall !== undefined ? ` (#${d.overall} overall)` : ""
		} by ${d.abbrev}${d.teamResult ? ` — ${d.abbrev} were ${d.teamResult}` : ""}`,
	);
	if (d.roster.length > 0) {
		lines.push(`  Roster joining (best first):`);
		for (const spot of d.roster) {
			lines.push(
				`    ${spot.name} ${spot.pos} age${spot.age} ovr${spot.ovr} pot${spot.pot}`,
			);
		}
	}
	return lines;
};

// The career totals a retrospective leans on. Summed in the worker, because an
// AI asked to add up eighteen season rows itself gets them wrong.
const retirementBlock = (r: RecapRetirement): string[] => {
	const totals = (line: Record<string, number> | undefined) =>
		line
			? `${one(line.pts ?? 0)}p ${one(line.trb ?? 0)}r ${one(line.ast ?? 0)}a ${one(line.stl ?? 0)}s ${one(line.blk ?? 0)}b ${one(line.min ?? 0)}m fg${line.fgp ?? 0}% 3p${line.tpp ?? 0}% ft${line.ftp ?? 0}% over ${line.gp ?? 0}g`
			: undefined;

	const lines = [
		`RETIRING AFTER THIS SEASON — age ${r.ageAtRetirement}, ${r.seasonsPlayed} season${
			r.seasonsPlayed === 1 ? "" : "s"
		}${
			r.firstSeason !== undefined && r.lastSeason !== undefined
				? ` (${r.firstSeason}-${r.lastSeason})`
				: ""
		}, ${r.totalGP} career games, peak ovr ${r.peakOvr}${
			r.rings > 0 ? `, ${r.rings} championship${r.rings === 1 ? "" : "s"}` : ""
		}`,
	];

	const career = totals(r.career);
	if (career) {
		lines.push(`  Career per game: ${career}`);
	}
	const playoffs = totals(r.playoffs);
	if (playoffs) {
		lines.push(`  Playoffs per game: ${playoffs}`);
	}
	if (r.teams.length > 0) {
		lines.push(
			`  Teams: ${r.teams
				.map(
					(t) =>
						`${t.abbrev} (${t.from === t.to ? t.from : `${t.from}-${t.to}`}, ${t.gp}g)`,
				)
				.join(", ")}`,
		);
	}
	if (r.totalGP === 0) {
		lines.push("  Never played a game.");
	}

	return lines;
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

	// Called out separately as well as in the career list below. For a
	// fifteen-year veteran the year being written is three entries buried in
	// thirty, and a season shaped by injuries is exactly the season most likely
	// to be recapped as a quiet decline instead.
	const injuriesThisSeason = p.injuries.filter((i) => i.season === season);
	if (injuriesThisSeason.length > 0) {
		const games = injuriesThisSeason.reduce((sum, i) => sum + i.games, 0);
		lines.push(
			`INJURIES THIS SEASON: ${injuriesThisSeason
				.map((i) => `${i.type} (${i.games}g)`)
				.join("; ")} — ${games} games missed`,
		);
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

	if (p.draftInfo) {
		lines.push(...draftBlock(p.draftInfo));
	}

	if (p.retiring) {
		lines.push(...retirementBlock(p.retiring));
	}

	return lines.join("\n");
};

// The league picture for the season being written. Sent ONCE for the whole
// batch rather than repeated per player, which is what makes it affordable.
const leagueBlock = (data: RecapPlayerBatch): string[] => {
	const teams = data.leagueTeams ?? [];
	if (teams.length === 0) {
		return [];
	}

	const lines = [`=== LEAGUE ${data.season} ===`];
	if (data.champion) {
		lines.push(`Champion: ${data.champion}`);
	}

	const byConf = new Map<string, typeof teams>();
	for (const team of teams) {
		const key = team.conf ?? "";
		byConf.set(key, [...(byConf.get(key) ?? []), team]);
	}

	for (const [conf, group] of byConf) {
		if (conf) {
			lines.push(conf);
		}
		for (const team of group) {
			lines.push(`  ${team.abbrev} ${team.won}-${team.lost}, ${team.result}`);
		}
	}

	return lines;
};

export const buildPlayerRecapPrompt = (data: RecapPlayerBatch): string => {
	const header = [
		INSTRUCTIONS,
		"",
		`LISTED SEASON: ${data.season}`,
		`This is batch ${data.batchIndex + 1} of ${data.batchCount} for this season (${data.players.length} players in this batch, ${data.totalPlayers} in the league).`,
		"",
		...leagueBlock(data),
		"",
		"=== PLAYERS ===",
	].join("\n");

	return [header, ...data.players.map((p) => playerBlock(p, data.season))].join(
		"\n\n",
	);
};

// Pull each piece out of the AI's reply. Everything from one marker up to the
// next is that piece's prose. There is no headline - the section is identified
// by its year alone, because an AI-written headline on top of every season is
// the most conspicuously machine-made thing in the note.
//
// The marker also says WHICH section of the note it belongs in - a season recap
// or a retirement writeup - so the two can never be filed as each other. They
// used to share one marker, and a reply pasted into the wrong button filed
// forty season recaps as retirement writeups with nothing to catch it.
export type ParsedPlayerRecap = {
	pid: number;
	kind: "season" | "retirement";
	// Only retirement writeups get one. A season recap is headed by its year
	// alone - an AI headline on every season is the most conspicuously
	// machine-made thing in a note - but a career retrospective is the one piece
	// that reads like an article and wants a title.
	headline: string;
	body: string;
};

// Strip the decoration an AI reaches for on a heading even when told not to,
// and drop any year it puts there - the year is supplied from the season being
// written, so a wrong one in the reply can never reach the note.
const cleanHeadline = (line: string) =>
	line
		.replace(/^#+\s*/, "")
		.replaceAll("**", "")
		.replace(/^\s*\[\s*\d{4}\s*]\s*/, "")
		.replace(/^\[|]$/g, "")
		.replace(/[.:]\s*$/, "")
		.trim();

// For SEASON recaps, told not to write a heading, an AI still sometimes writes
// one. Drop a leading line that is clearly a title - a bracketed year, a
// markdown heading, or a short bolded line - rather than letting it open the
// prose.
const HEADING_LINE = /^\s*(?:#{1,6}\s+|\[\s*\d{4}\s*]|\*\*[^*]{1,80}\*\*\s*$)/;

const stripHeadingLine = (chunk: string): string => {
	const lines = chunk.split("\n");
	const first = lines[0] ?? "";
	if (lines.length > 1 && HEADING_LINE.test(first)) {
		return lines.slice(1).join("\n").trim();
	}
	return chunk;
};

export const parsePlayerRecaps = (rawText: string): ParsedPlayerRecap[] => {
	const text = stripOuterCodeFence(rawText);
	const out: ParsedPlayerRecap[] = [];

	const re = /<!--\s*(player|retired):\s*(\d+)\s*-->/g;
	const markers: {
		pid: number;
		kind: "season" | "retirement";
		start: number;
		end: number;
	}[] = [];
	let match = re.exec(text);
	while (match !== null) {
		markers.push({
			pid: Number.parseInt(match[2]!),
			kind: match[1] === "retired" ? "retirement" : "season",
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

		let headline = "";
		let body: string;
		if (marker.kind === "retirement") {
			const lines = chunk.split("\n");
			const first = cleanHeadline(lines[0] ?? "");
			const rest = lines.slice(1).join("\n").trim();
			// If it ignored the instruction and went straight into prose, keep the
			// whole thing rather than eating its first sentence.
			if (rest === "" || first.length > 80) {
				body = chunk;
			} else {
				headline = first;
				body = rest;
			}
		} else {
			body = stripHeadingLine(chunk);
		}
		if (body === "") {
			continue;
		}
		const parsed: ParsedPlayerRecap = {
			pid: marker.pid,
			kind: marker.kind,
			headline,
			body,
		};

		// A repeated marker is the AI restating itself; last one wins, matching
		// how re-running a season replaces rather than duplicates.
		const existing = out.findIndex(
			(x) => x.pid === parsed.pid && x.kind === parsed.kind,
		);
		if (existing >= 0) {
			out[existing] = parsed;
		} else {
			out.push(parsed);
		}
	}

	return out;
};
