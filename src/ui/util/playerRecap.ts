import type {
	RecapDraftInfo,
	RecapPlayer,
	RecapPlayerBatch,
	RecapProspect,
	RecapRetirement,
	RecapTeamPlayer,
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

The LEAGUE block above the players carries this season's standings, each team's rotation (so you know who a player's teammates were and where he sat in the pecking order), the league leaders and league-average per game in every major category (so you can say where a season actually ranked instead of guessing), and the award races in finishing order. Use them. "Second in the league in rebounding", "the only other man on the roster averaging double figures", "finished fourth in MVP voting" are the sentences that make a recap worth reading, and they are all checkable from that block — so never invent one. A player's own block also lists his AWARD FINISH where he placed in a race, and SEASON HIGHS, his best single game in each category that year.

Every stat line carries the team's record and how that team's year ended, and the league standings for this season are listed above the players. Use that context where it makes the recap better: 24 ppg on a 19-63 team is a different story from 24 ppg on a title winner, and a role player's year is often best told through what his team was chasing. Keep the focus on the PLAYER — team context is there to give his season stakes, not to become a team recap.

EVERY TRANSACTION IS DATED THE SAME WAY, so you always know exactly when a player changed teams. A move marked "(for YYYY)" was made in the offseason and takes effect that year: "2002 free agency (for 2003): signed with LAL" is a player who spent the whole of 2002 elsewhere and pulls on a Los Angeles jersey for the first time in 2003. Everything else — a trade in the regular season, a signing at the deadline — happened inside the season it is dated to, and his stat lines will show both teams that year. That is how a player got to the team he is playing for, and it is one of the most useful things you have.

Ratings are scouting information for YOU, not material for the page. Never print a rating number and never refer to one — no "a 78 three-point rating", no "his overall climbed to 71", no "peaked at 84", no grades or tiers derived from them. Read them to know what a player is good at, what he cannot do, and how that changed year to year, then say it the way a writer would: an elite finisher, no handle to speak of, a jumper that finally came around, legs that went at 33. The same goes for any teammate's or draft pick's ratings.

Write like someone who watched these seasons happen. Everything in front of you is settled fact — what he averaged, when he was hurt, the night he went for 51, which team he was on and how he got there — so state it plainly and with confidence. Never hedge about it: no "appears to have", no "presumably", no "it seems he was traded at some point". If something genuinely isn't in the data, leave it out; a recap is never improved by guessing out loud.

Write about them as people with careers. Do not dump the data back — weave the numbers that matter into the prose.

These are rendered as Markdown, so use it where it earns its place: **bold** a player's name the first time it appears and bold the numbers a reader should take away from the piece, *italics* for the occasional bit of emphasis. Name teams in full the way a writer would — "the Toronto Raptors", "Toronto" — using the region and nickname given in the LEAGUE block, never the abbreviation, because names get turned into links to that season's team page and "TOR" does not. Keep it light: a paragraph with everything bolded reads worse than one with nothing bolded.

RETIRING PLAYERS GET TWO PIECES. A player marked RETIRING AFTER THIS SEASON has just played his last season, and his block carries his career totals. Write his season recap exactly like everyone else's, and then a SECOND, separate piece: the retirement writeup, the kind of career retrospective published when a player hangs it up. Scale that one to the career, and do not give everyone the same treatment:
- Hall of Famers and decorated stars: a full retrospective, several paragraphs — the arc, the peak, the signature seasons, the accolades, how he is remembered.
- Solid long-tenured players: a couple of tight paragraphs.
- Role players and journeymen: a short paragraph.
- Players who barely played, and especially undrafted players who never logged a single game: one or two honest sentences. Do not invent a career that isn't there.

Follow these rules EXACTLY:
- Put your ENTIRE reply inside ONE fenced code block: open with a line of exactly \`\`\`markdown, then all the recaps, then a final line of exactly \`\`\`. Nothing before or after the fence — no preamble, no summary.
- The FIRST line inside the fence must be the season stamp given below, copied exactly. It is how the reply is checked against the season it was written for; without it nothing can be filed.
- Begin every player's recap with a line containing ONLY this marker: <!--player:ID--> (replace ID with that player's number, shown as "PLAYER <ID>" below). This is how each recap is filed to the correct player — never omit it, never change it.
- Straight after a <!--player:ID--> marker, write the season recap as plain prose. NO headline, NO title, NO heading line, no bold lead-in, no year — start with the first sentence of the recap itself. No stat table, no bullet lists.
- For a RETIRING player only, add the retirement writeup after his season recap under a DIFFERENT marker line: <!--retired:ID--> (same ID). This one DOES get a headline: the line straight after the marker is a few words, title-style, no ending period, no bold, no brackets and no year, about how the CAREER is remembered ("The quiet exit", "Sixteen years, one team"). Then a blank line, then the writeup.
- Never state a rating number. Statistics (points, rebounds, percentages, records, league ranks, award finishes) are fine to quote; ratings are not.
- Include EVERY player listed, in the order given. Do not skip anyone, and do not merge players.
- Put exactly one blank line between pieces.`;

const one = (x: number) => (Math.round(x * 10) / 10).toFixed(1);

const ordinal = (n: number) => {
	const rem100 = n % 100;
	if (rem100 >= 11 && rem100 <= 13) {
		return `${n}th`;
	}
	return `${n}${["th", "st", "nd", "rd"][n % 10] ?? "th"}`;
};

const pct = (made: number, attempted: number) =>
	attempted > 0 ? `${Math.round((made / attempted) * 1000) / 10}%` : "-";

// Height in inches to the way anyone would write it.
const height = (inches: number) =>
	inches > 0 ? `${Math.floor(inches / 12)}'${inches % 12}"` : undefined;

// A season's stat line, per game, in a fixed compact order.
const statLine = (s: RecapPlayer["stats"][number]) => {
	const perGame = (v: number) => (s.gp > 0 ? one(v / s.gp) : "0.0");
	return [
		`${s.season}${s.playoffs ? "p" : ""}`,
		s.abbrev,
		`age${s.age}`,
		// Games started alongside games played, because moving into (or out of) a
		// starting lineup is one of the most common shapes a season has.
		s.gs === undefined ? `${s.gp}g` : `${s.gp}g/${s.gs}gs`,
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
const draftBlock = (d: RecapDraftInfo, season: number): string[] => {
	const lines: string[] = [];
	lines.push(
		`DRAFTED: rd${d.round} pk${d.pick}${
			d.overall !== undefined ? ` (#${d.overall} overall)` : ""
		} by ${d.abbrev}${d.teamResult ? ` — ${d.abbrev} were ${d.teamResult}` : ""}`,
		`  Not yet played: the ${season} draft is held after the ${season} season ends, so his first season is ${season + 1}.`,
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

// A player in NEXT season's draft class. He has no stats and no league history
// - the ratings ARE the report, which is why the full set goes out.
const prospectBlock = (d: RecapProspect, season: number): string[] => {
	const subs = Object.entries(d.ratings)
		.map(([key, value]) => `${key}${value}`)
		.join(" ");
	return [
		// Deliberately does NOT say what year it currently is. The report is read
		// at the draft, so telling the writer it is the season before invites
		// "he is a year away" framing that is stale by the time anyone sees it.
		`PROSPECT — coming out in the ${d.draftYear} draft, held at the end of the ${d.draftYear} season. He has never played in this league; nobody knows yet where he will go.`,
		`  scouting: ovr${d.ovr} pot${d.pot}${subs ? ` | ${subs}` : ""}`,
	];
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

	// A prospect has no team and no ratings row for this season, so the usual
	// header came out as "Name — , age 20 in 2000, no team". A member of this
	// season's own draft class hasn't played either, so "no team" is wrong for
	// him too even when he has just been picked.
	const where = p.prospect
		? `${p.prospect.draftYear} draft class`
		: p.draft.year === season
			? `${season} draft class`
			: p.teamAbbrevs.length > 0
				? p.teamAbbrevs.join(" / ")
				: "no team";
	const pos = p.prospect ? p.prospect.pos : p.pos;
	// A prospect's report is read at his DRAFT, not on the day it was filed - a
	// class is scouted the season before, so by the time anyone opens the report
	// he is a year older than he was when it was written. Stating the age he is
	// now made every report read as though the draft were happening a year early
	// ("he's eighteen years old" on a card showing 19). Give the age he will be
	// when he comes out, and label it, so there is nothing to misread.
	if (p.prospect) {
		const ageAtDraft = p.age + Math.max(0, p.prospect.draftYear - season);
		lines.push(
			`${p.name} — ${pos}, age ${ageAtDraft} at the ${p.prospect.draftYear} draft, ${where}`,
		);
	} else {
		lines.push(`${p.name} — ${pos}, age ${p.age} in ${season}, ${where}`);
	}

	const bio: string[] = [];
	const size = [height(p.hgt), p.weight > 0 ? `${p.weight} lbs` : undefined]
		.filter(Boolean)
		.join(", ");
	if (size) {
		bio.push(size);
	}
	if (p.born.loc) {
		bio.push(`from ${p.born.loc}`);
	}
	if (p.prospect) {
		// Where he actually went is a fact from the future - the report is written
		// the season BEFORE his draft. Backfilling an old year, that pick is
		// already in the database, and printing it turns a scouting report into a
		// summary of what happened.
		if (p.prospect.college) {
			bio.push(p.prospect.college);
		}
	} else if (p.draft.year) {
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
	} else if (p.prospect) {
		// Nothing to recap - he isn't in the league and won't be for another year.
		// The PROSPECT block further down is the whole of his entry.
		lines.push(
			`THIS SEASON: not in the league — eligible for the ${p.prospect.draftYear} draft`,
		);
	} else if (p.draftInfo) {
		// He was drafted at the END of this season, so there is no season to have
		// missed. Saying "did not play" here invited the AI to write it up as
		// something that went wrong.
		lines.push(
			`THIS SEASON: not in the league yet — drafted at the end of ${season}, first season is ${season + 1}`,
		);
	} else if (p.draft.year === season) {
		// Same class, nobody called his name. He is still part of this draft's
		// story, and "did not play" would read as a season that went wrong.
		lines.push(
			`THIS SEASON: not in the league — went UNDRAFTED in the ${season} draft, held at the end of the ${season} season. No team has him.`,
		);
	} else {
		lines.push("THIS SEASON: did not play");
	}

	if (p.seasonHighs) {
		// "SEASON HIGHS (single game): 28pts 11trb 11ast" was read, reasonably, as
		// one 28-11-11 night — and written up as a triple-double that never
		// happened. Each of those is his best in that category across the whole
		// season, and they are usually three different nights.
		//
		// So group them by the game they actually came from: bests that share a
		// game are stated as one line (a real triple-double is then sayable), and
		// everything else stands alone as what it is.
		const byGame = new Map<string, string[]>();
		for (const [stat, high] of Object.entries(p.seasonHighs)) {
			// No gid (an old or imported league) means we can't prove two of these
			// shared a night, so they never get grouped.
			const key = high.gid === undefined ? `alone:${stat}` : `game:${high.gid}`;
			const list = byGame.get(key) ?? [];
			list.push(`${high.value} ${stat}`);
			byGame.set(key, list);
		}

		const parts = [...byGame.values()].map((group) =>
			group.length > 1 ? `${group.join(" and ")} in one game` : group[0]!,
		);
		if (parts.length > 0) {
			lines.push(
				`SEASON BESTS (one entry per category, each his best single game in THAT category; entries are from different games except where one says "in one game"): ${parts.join(
					"; ",
				)}`,
			);
		}
	}

	if (p.awardFinishes.length > 0) {
		lines.push(
			`AWARD FINISH: ${p.awardFinishes
				.map((a) => `${a.name} ${ordinal(a.rank)}`)
				.join("; ")}`,
		);
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
		lines.push(...draftBlock(p.draftInfo, season));
	}

	if (p.prospect) {
		lines.push(...prospectBlock(p.prospect, season));
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
			// Full name alongside the abbreviation the stat lines use, so a recap can
			// name the team the way a writer would instead of inferring a city from
			// three letters.
			const label = [team.region, team.name].filter(Boolean).join(" ");
			lines.push(
				`  ${team.abbrev}${label ? ` = ${label}` : ""} ${team.won}-${team.lost}, ${team.result}`,
			);
			for (const spot of team.roster) {
				lines.push(`    ${rosterLine(spot)}`);
			}
		}
	}

	if (data.leaders.length > 0) {
		lines.push("", "LEAGUE LEADERS (per game, qualified players):");
		for (const row of data.leaders) {
			const board = row.players
				.map((p, i) => `${i + 1}. ${p.name} ${p.abbrev} ${one(p.value)}`)
				.join(", ");
			lines.push(`  ${row.label} (avg ${one(row.leagueAvg)}): ${board}`);
		}
	}

	if (data.awardRaces.length > 0) {
		lines.push("", "AWARD RACES (finishing order):");
		for (const race of data.awardRaces) {
			lines.push(
				`  ${race.name}: ${race.players
					.map((p, i) => `${i + 1}. ${p.name} ${p.abbrev}`)
					.join(", ")}`,
			);
		}
	}

	return lines;
};

// A teammate as the writer needs him: enough to see who the team leaned on.
const rosterLine = (p: RecapTeamPlayer) =>
	`${p.name} ${p.pos} age${p.age} ${p.gp}g ${one(p.min)}m ${one(p.pts)}p ${one(p.trb)}r ${one(p.ast)}a`;

// Stamped into the reply so a batch written for one season can't be filed into
// another. Every player carries the same pid whatever year it is, so nothing
// about a 2000 reply looks wrong when it lands in 2001 - the recaps just quietly
// attach to the wrong year on forty players' pages, and there is no way to tell
// afterward which ones came from where.
export const seasonStamp = (season: number) => `<!--season:${season}-->`;

const SEASON_STAMP_RE = /<!--\s*season:\s*(\d{4})\s*-->/;

export const parseRecapSeason = (rawText: string): number | undefined => {
	const match = SEASON_STAMP_RE.exec(stripOuterCodeFence(rawText));
	return match ? Number.parseInt(match[1]!) : undefined;
};

// Next year's draft class gets its own prompt, not a paragraph inside the
// season-recap one.
//
// They are a different job: no stats, no team, no season to recap, and a
// scouting report is long where a recap for a deep-bench player is one
// sentence. Sharing a prompt meant the recap rules and the scouting rules each
// had to carve out an exception for the other, and every prospect carried the
// whole league standings block it has no use for. Separating them makes each
// prompt say one thing, and leaves the reply room for the reports.
const PROSPECT_INSTRUCTIONS = `You are a scout filing reports on next year's draft class for a fictional basketball league.

${FICTIONAL_LEAGUE_NOTICE}

Every player below is in NEXT year's draft class. He has never played a game in this league, has no stats, and nobody has seen him do anything yet. His draft has not happened, so where he goes and who takes him are unknown to you; project them, never state them. There is no season to recap and no absence of one to remark on.

Write it as of the draft he is coming out in, not as of today. The class is scouted a season ahead, so his header gives the age he will be WHEN HE IS DRAFTED - that is the only age you may use, and the only one a reader will see beside the report. Never work out or mention how old he is right now, never say what year it currently is, and never describe the draft as being a year away.

Write the report a scouting department would file on him as he comes out: several paragraphs, and give every one of them real content. Frame, body and athleticism. What he does on offence — how he scores, from where, whether he can shoot it, whether he can create, how he handles it, how he passes. What he does defensively, and whether he can guard his position. Feel for the game, motor, durability. Then the honest part: what has to improve, what may never come, what kind of player he projects as and what kind of role fits him. Say where you think he goes in the draft and why. A short report is a failure here; this is the only thing ever written about him before he arrives.

Everything you say has to come from the ratings in his block and nothing else. They are your entire scouting file, and the length of the report comes from reading them carefully — a big man who cannot shoot, a guard with no handle, a phenomenal athlete with no feel are all right there in the numbers.

Never print a rating number and never refer to one — no "a 78 three-point rating", no "his overall is 71", no grades or tiers derived from them. Read them to know what he can and cannot do, then say it the way a scout would: an elite finisher, no handle to speak of, a jumper that needs rebuilding, feet that will struggle on the perimeter.

Write with the confidence of someone who has watched him play, and without hedging about what the file says. Where you are uncertain, be uncertain about the PROJECTION — how high his ceiling is, whether the shot comes around — not about the facts in front of you.

These are rendered as Markdown, so use it where it earns its place: **bold** his name the first time it appears, *italics* for the occasional bit of emphasis. Keep it light.

Follow these rules EXACTLY:
- Put your ENTIRE reply inside ONE fenced code block: open with a line of exactly \`\`\`markdown, then all the reports, then a final line of exactly \`\`\`. Nothing before or after the fence — no preamble, no summary.
- The FIRST line inside the fence must be the season stamp given below, copied exactly. It is how the reply is checked against the season it was written for; without it nothing can be filed.
- Begin every report with a line containing ONLY this marker: <!--player:ID--> (replace ID with that player's number, shown as "PLAYER <ID>" below). This is how each report is filed to the correct player — never omit it, never change it.
- Straight after the marker, write the report as plain prose. NO headline, NO title, NO heading line, no bold lead-in, no year — start with the first sentence. No bullet lists, no ratings table.
- Include EVERY player listed, in the order given. Do not skip anyone, and do not merge players.
- Put exactly one blank line between reports.`;

// This season's own draft class, written after the draft has been held. Also
// its own prompt rather than a corner of the season recap: every one of these
// players has zero stats and zero league history, so the recap prompt's whole
// apparatus - stat lines, league leaders, "how did his year go" - applies to
// none of them, and the two DRAFTED paragraphs it needed were an exception
// carved out for a group that was never going to fit.
//
// Unlike the prospects pass this one KEEPS the league block: where a pick
// landed is the story, and naming the team properly and knowing what kind of
// season it just had are what make the piece worth reading.
const DRAFT_PICK_INSTRUCTIONS = `You are a basketball writer covering the draft for a fictional league. Write a piece on EACH member of the listed season's draft class below.

${FICTIONAL_LEAGUE_NOTICE}

THE DRAFT IS HELD AFTER THE SEASON ENDS. Every player below belongs to the LISTED SEASON's draft class, which means the draft has just taken place and his first season in the league is the one AFTER the listed season. He has never played a game here. He has not missed anything and nothing has gone wrong — never write that he "did not play this season", never treat the absence of stats as a fact about him, and never recap a season for him. Every piece is forward-looking.

DRAFTED PLAYERS. A player with a DRAFTED block was picked, and that block gives his round and pick, the drafting team, that team's just-finished season and the roster he is joining. Write where he went and what he is walking into: the role waiting for him, who he sits behind or alongside, whether the fit is natural or awkward, what the team appears to need, whether the pick was value where it came or a reach. Judge it from the roster given; never invent teammates. Then what he projects as — the player he could become, what has to improve, what may never come.

UNDRAFTED PLAYERS. A player marked UNDRAFTED went unpicked, and that is part of this draft's story too. Say honestly why nobody called his name and what, if anything, is worth a look — a skill that plays, a body that doesn't, an age problem. Keep these short: a paragraph, often less. Do not manufacture a prospect out of a player nobody wanted.

Length: scale it to the pick. A top selection can carry two or three paragraphs. A late first-rounder gets one solid one. Second-rounders and undrafted players get a paragraph or less. Never pad.

Ratings are scouting information for YOU, not material for the page. Never print a rating number and never refer to one — no "a 78 three-point rating", no "his overall is 71", no grades or tiers derived from them. Read them to know what he can and cannot do, then say it the way a writer would: an elite finisher, no handle to speak of, a jumper that needs rebuilding, feet that will struggle on the perimeter. The same goes for the ratings of anyone already on the roster he is joining.

The LEAGUE block above carries this season's standings and every team's rotation, so you know exactly what kind of team just spent a pick on him. Use it. Where you are uncertain, be uncertain about the PROJECTION — how good he gets, whether the fit works — not about the facts in front of you.

These are rendered as Markdown, so use it where it earns its place: **bold** his name the first time it appears, *italics* for the occasional bit of emphasis. Name teams in full the way a writer would — "the Toronto Raptors", "Toronto" — using the region and nickname given in the LEAGUE block, never the abbreviation, because names get turned into links to that season's team page and "TOR" does not. Keep it light.

Follow these rules EXACTLY:
- Put your ENTIRE reply inside ONE fenced code block: open with a line of exactly \`\`\`markdown, then all the pieces, then a final line of exactly \`\`\`. Nothing before or after the fence — no preamble, no summary.
- The FIRST line inside the fence must be the season stamp given below, copied exactly. It is how the reply is checked against the season it was written for; without it nothing can be filed.
- Begin every piece with a line containing ONLY this marker: <!--player:ID--> (replace ID with that player's number, shown as "PLAYER <ID>" below). This is how each piece is filed to the correct player — never omit it, never change it.
- Straight after the marker, write the piece as plain prose. NO headline, NO title, NO heading line, no bold lead-in, no year — start with the first sentence. No bullet lists, no ratings table.
- Include EVERY player listed, in the order given. Do not skip anyone, and do not merge players.
- Put exactly one blank line between pieces.`;

export const buildPlayerRecapPrompt = (data: RecapPlayerBatch): string => {
	// A prospect batch has no league to describe: no standings, no leaders, no
	// award races, and no teams to name. Sending them the league block would be
	// pure token cost taken off the reports. The draft-pick batch is the
	// opposite - the landing spot is the entire point - so it keeps it.
	const prospects = data.filter === "prospects";
	const draftPicks = data.filter === "draftPicks";

	const instructions = prospects
		? PROSPECT_INSTRUCTIONS
		: draftPicks
			? DRAFT_PICK_INSTRUCTIONS
			: INSTRUCTIONS;

	const scope = prospects
		? `This is batch ${data.batchIndex + 1} of ${data.batchCount} of the ${data.season + 1} draft class (${data.players.length} prospects in this batch, ${data.totalPlayers} in the class).`
		: draftPicks
			? `This is batch ${data.batchIndex + 1} of ${data.batchCount} of the ${data.season} draft class (${data.players.length} players in this batch, ${data.totalPlayers} in the class).`
			: `This is batch ${data.batchIndex + 1} of ${data.batchCount} for this season (${data.players.length} players in this batch, ${data.totalPlayers} in the league).`;

	const header = [
		instructions,
		"",
		`SEASON STAMP (copy as the first line inside the fence): ${seasonStamp(data.season)}`,
		"",
		`LISTED SEASON: ${data.season}`,
		scope,
		"",
		...(prospects ? [] : leagueBlock(data)),
		"",
		prospects
			? "=== PROSPECTS ==="
			: draftPicks
				? `=== ${data.season} DRAFT CLASS ===`
				: "=== PLAYERS ===",
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
