import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import { finishScores } from "./recapFinish.ts";

// DOES THE RECAP SAY ANYTHING THAT ISN'T TRUE?
//
// getAutoRecap writes prose from a box score, and every number it prints is
// meant to be a number that box score contains. Nothing enforced that. The
// builders are individually careful, but they are thousands of lines of string
// templates: one wrong field, one stat attributed to the wrong man, one
// differential subtracted the wrong way round, and the recap states it with
// complete confidence and no test notices.
//
// So read the finished prose back and hold every claim against the game it came
// from - the way a copy desk checks a story rather than the way a unit test
// checks a function. This is deliberately a reader, not a writer: it knows
// nothing about which builder produced which sentence, so it keeps working when
// the phrasing changes, and it covers builders nobody thought to test.
//
// Checked over 600 engine-simmed recaps (recapCorpus.test.ts) and, on the
// fixtures, in CI.

export type RecapViolation = {
	kind: string;
	detail: string;
	sentence: string;
};

// The headline has no full stop, so a paragraph break has to count as one:
// otherwise the headline and the lede read as one sentence and a name in
// the headline is credited with the lede's numbers.
// "six", "sixth", "6", "6th" - the number a claim carries, spelled out or
// not, cardinal or ordinal.
const NUMBER_WORDS: Record<string, number> = {
	one: 1,
	first: 1,
	two: 2,
	second: 2,
	three: 3,
	third: 3,
	four: 4,
	fourth: 4,
	five: 5,
	fifth: 5,
	six: 6,
	sixth: 6,
	seven: 7,
	seventh: 7,
	eight: 8,
	eighth: 8,
	nine: 9,
	ninth: 9,
	ten: 10,
	tenth: 10,
	eleven: 11,
	eleventh: 11,
	twelve: 12,
	twelfth: 12,
	thirteen: 13,
	thirteenth: 13,
	fourteen: 14,
	fourteenth: 14,
	fifteen: 15,
	fifteenth: 15,
};
const wordToNumber = (t: string): number | undefined => {
	const lower = t.toLowerCase();
	if (NUMBER_WORDS[lower] !== undefined) {
		return NUMBER_WORDS[lower];
	}
	const m = /^(\d+)(?:st|nd|rd|th)?$/.exec(lower);
	return m ? Number(m[1]) : undefined;
};

const splitSentences = (text: string): string[] =>
	text
		.replaceAll("**", "")
		.replaceAll(/([^!.?])\n\n/g, "$1. ")
		.replaceAll("\n", " ")
		.split(/(?<=[!.?])\s+/)
		.map((s) => s.trim())
		.filter(Boolean);

const teamTotal = (t: RecapTeam, key: keyof RecapPlayer): number => {
	let sum = 0;
	for (const p of t.players) {
		const v = p[key];
		if (typeof v === "number") {
			sum += v;
		}
	}
	return sum;
};

// A number followed by one of these words is that stat. Every one is a counting
// stat a player line carries, so a mismatch is a real error rather than a
// phrasing difference.
const COUNTING: [keyof RecapPlayer, string][] = [
	["pts", "points"],
	["reb", "rebounds"],
	["ast", "assists"],
	["stl", "steals"],
	["blk", "blocks"],
	["tov", "turnovers"],
];

// A "NN-NN" pair is only the FINAL when nothing in the sentence marks it as a
// period, a run, a rebounding edge or a season record. Those all share the
// shape and none of them is wrong.
const NOT_A_FINAL =
	/quarter|after one|at the break|halftime|half|first|second|third|fourth|\brun\b|stretch|outscor|closed|opened|took over|settled it|broke it open|spurt|glass|boards|rebound|free throw|from the line|threes|from deep|assists|pushed|improved|moved|fell to|dropped to|climbed|meeting|season series|this season|at home|on the road|away from home|own building|bench|reserves|to go\b|\bleft\b|to play|lead change|tied at|as many as|straight points|two minutes|for good|ahead for the last time|went in front|ahead \d|up \d+-\d+|within|cut it|back to|as close as|made it \d|buzzer|no time|remaining/i;

// A count in one of these sentences is a season or career total, a season
// high being quoted, or a bench total - not a line from tonight's box score.
const SEASON_TALK =
	/\bcareer\b|-point mark|-rebound mark|-assist mark|for the season|on the season|this season|of the season|season high/i;
const BENCH_TALK = /\bbench\b|\breserves?\b/i;

// A points figure in one of these sentences is a betting margin, not a stat.
const SPREAD_TALK =
	/the line|wrong side|favou?r|underdog|no chance|getting \d|spread|\bbooks\b/i;

const RESULT_VERB =
	/beat|topped|handled|downed|routed|edged|held off|took down|stunned|shocked|knocked off|upset|survived|outlast|blew out|rolled|cruised|crushed|trounced|got past|pulled away|defeat|\bwin\b|\bwon\b/i;

export const verifyRecap = (
	recap: string,
	game: RecapGame,
): RecapViolation[] => {
	const out: RecapViolation[] = [];
	const add = (kind: string, detail: string, sentence: string) => {
		out.push({ kind, detail, sentence });
	};

	const [home, away] = game.teams;
	const winner = game.winnerTid === home.tid ? home : away;
	const loser = game.winnerTid === home.tid ? away : home;
	const byName = new Map<string, RecapPlayer>();
	for (const t of game.teams) {
		for (const p of t.players) {
			byName.set(p.name, p);
		}
	}
	const names = [...byName.keys()];
	const finalPts = new Set([winner.pts, loser.pts]);

	const text = recap.replaceAll("**", "");

	// A sentence that opens "He ..." or "His ..." belongs to the man the
	// sentence before it was about - the recap turns a repeated subject into
	// a pronoun, and "He was gone by the end, fouling out with 15 points" is
	// still his 15 points.
	let chainOwner: string | undefined;
	const lastNameIn = (text: string): string | undefined => {
		let best = -1;
		let who: string | undefined;
		for (const nm of names) {
			const at = text.lastIndexOf(nm);
			if (at > best) {
				best = at;
				who = nm;
			}
		}
		return who;
	};
	for (const sentence of splitSentences(recap)) {
		const prevOwner = chainOwner;
		const pronounLed = /^(?:He|His)\b/.test(sentence);
		chainOwner = lastNameIn(sentence) ?? (pronounLed ? chainOwner : undefined);
		// --- the final score -------------------------------------------------
		for (const m of sentence.matchAll(/\b(\d{2,3})-(\d{2,3})\b/g)) {
			// "(10-3)" is a record, not a score.
			if (sentence[m.index - 1] === "(") {
				continue;
			}
			const around = sentence.slice(
				Math.max(0, m.index - 60),
				m.index + m[0].length + 40,
			);
			if (NOT_A_FINAL.test(around)) {
				continue;
			}
			if (!RESULT_VERB.test(sentence.slice(0, m.index))) {
				continue;
			}
			const a = Number(m[1]);
			const b = Number(m[2]);
			const tie = winner.pts === loser.pts;
			if (!finalPts.has(a) || !finalPts.has(b) || (a === b && !tie)) {
				add(
					"final score",
					`said ${a}-${b}, real ${winner.pts}-${loser.pts}`,
					sentence,
				);
			}
		}

		// --- a player's counting stats ---------------------------------------
		for (const [key, word] of COUNTING) {
			const totals = new Set([teamTotal(home, key), teamTotal(away, key)]);
			const combined = teamTotal(home, key) + teamTotal(away, key);
			for (const m of sentence.matchAll(
				new RegExp(String.raw`(?<![\d.])(\d+) ${word}\b`, "g"),
			)) {
				const n = Number(m[1]);
				const before = sentence.slice(0, m.index);
				// "came in averaging 12 points a game" is his season line.
				if (/averag\w*\s*$/i.test(before)) {
					continue;
				}
				// "scored the next 10 points" is a stretch of the finish, and "the
				// last 12 points" an overtime run: both checked against the log
				// below.
				if (/\bthe (?:next|last)\s*$/.test(before)) {
					continue;
				}
				// "7 points the wrong side of the line", "favored by 6" - a
				// margin against the pregame line, not anything in the box score.
				if (SPREAD_TALK.test(sentence)) {
					continue;
				}
				// "passed 10,000 career points", "went past 500 points on the
				// season", "had not scored more than 31 this season" - totals and
				// highs, not tonight's line.
				if (SEASON_TALK.test(sentence)) {
					continue;
				}
				// "22 points from Evan Hayes" - the owner follows the number, so
				// the nearest PRECEDING name is the wrong man.
				// ...but only a "from" inside THIS clause: "21 points and 4
				// steals as the Celtics beat the Pacers despite 29 points from
				// Trey Foster" credits the 21 to the man before it, not to
				// Foster. A stat list ("33 points and 8 assists from X") is one
				// clause; "as", "despite", "but" and their like start another.
				const after = sentence.slice(m.index + m[0].length);
				const clauseEnd = after.search(
					/\b(?:as|despite|but|while|though|although|when|after|before)\b|[,;] (?:and |but )?the\b/,
				);
				const reach = clauseEnd === -1 ? after : after.slice(0, clauseEnd);
				const fromMatch = /\b(?:for|from)\s+/.exec(reach);
				let owner: string | undefined;
				if (fromMatch) {
					const tail = after.slice(fromMatch.index + fromMatch[0].length);
					owner = names.find((nm) => tail.startsWith(nm));
				}
				if (owner === undefined) {
					let best = -1;
					for (const nm of names) {
						const at = before.lastIndexOf(nm);
						if (at > best) {
							best = at;
							owner = nm;
						}
					}
					if (best < 0) {
						owner = undefined;
					}
				}
				if (owner === undefined && pronounLed) {
					owner = prevOwner;
				}
				if (owner === undefined) {
					// "got 48 points from the bench" - a bench total, which the
					// box score only has when starters are marked.
					if (BENCH_TALK.test(sentence)) {
						continue;
					}
					if (!totals.has(n) && n !== combined) {
						add(
							`unattributed ${word}`,
							`${n} ${word} is neither a team total nor anyone's line`,
							sentence,
						);
					}
					continue;
				}
				const actual = byName.get(owner)![key];
				if (actual !== n && !totals.has(n)) {
					add(
						`player ${word}`,
						`${owner} credited ${n} ${word}, real ${actual}`,
						sentence,
					);
				}
			}
		}

		// --- "N-of-M" has to be a split somebody actually shot ----------------
		const splits = new Set<string>();
		for (const t of game.teams) {
			splits.add(`${teamTotal(t, "tp")}-${teamTotal(t, "tpa")}`);
			splits.add(`${teamTotal(t, "fg")}-${teamTotal(t, "fga")}`);
			splits.add(`${teamTotal(t, "ft")}-${teamTotal(t, "fta")}`);
			for (const p of t.players) {
				splits.add(`${p.tp}-${p.tpa}`);
				splits.add(`${p.fg}-${p.fga}`);
				splits.add(`${p.ft}-${p.fta}`);
			}
		}
		// "4-of-7", and "hitting 4 of 7 threes".
		for (const m of sentence.matchAll(
			/(?<![\d.])(\d+)(?:-of-| of (?=\d+ (?:threes|three-pointers|free throws|shots)\b))(\d+)/g,
		)) {
			if (!splits.has(`${m[1]}-${m[2]}`)) {
				add("shooting split", `${m[1]}-of-${m[2]} matches nothing`, sentence);
			}
		}
		// "hitting all 4 of his threes" is 4-of-4.
		for (const m of sentence.matchAll(
			/\ball (\d+) of his (?:threes|three-pointers|free throws|shots)\b/g,
		)) {
			if (!splits.has(`${m[1]}-${m[1]}`)) {
				add("shooting split", `${m[1]}-of-${m[1]} matches nothing`, sentence);
			}
		}

		// --- plus-minus, with him and without -------------------------------
		//
		// "The Kings outscored the Bulls by 27 with Isaiah Ellis on the floor
		// and were outscored by 24 without him": his plus-minus, and his
		// team's margin less it.
		for (const m of sentence.matchAll(
			/by (\d+) with (.+?) on the floor and were outscored by (\d+) without him|^(.+?) was \+(\d+) in \d+ minutes; .* were outscored by (\d+) when he sat|won (.+?)'s? \d+ minutes by (\d+) and lost the rest by (\d+)/g,
		)) {
			const who = m[2] ?? m[4] ?? m[7];
			const on = Number(m[1] ?? m[5] ?? m[8]);
			const without = Number(m[3] ?? m[6] ?? m[9]);
			// Every man of that name: two teams can carry one.
			const fits = game.teams.flatMap((team) => {
				const opp = game.teams.find((t) => t !== team)!;
				return team.players.filter(
					(p) =>
						p.name === who &&
						p.pm === on &&
						p.pm - (team.pts - opp.pts) === without,
				);
			});
			if (fits.length === 0) {
				const p = who === undefined ? undefined : byName.get(who);
				add(
					"plus-minus",
					`said +${on} and -${without} for ${who}, real ${p?.pm ?? "none"}`,
					sentence,
				);
			}
		}

		// --- percentages ---------------------------------------------------
		// "shot just 37.8% as a team", "52.4% from the field", "far better
		// than the 44.8% he had managed on the season": a team's or a man's
		// shooting tonight, or a man's coming in. None of them was checked.
		{
			const pct = (made: number, tried: number) =>
				tried > 0 ? (100 * made) / tried : undefined;
			const ok: number[] = [];
			for (const t of game.teams) {
				ok.push(
					...[
						pct(teamTotal(t, "fg"), teamTotal(t, "fga")),
						pct(teamTotal(t, "tp"), teamTotal(t, "tpa")),
						pct(teamTotal(t, "ft"), teamTotal(t, "fta")),
					].filter((x): x is number => x !== undefined),
				);
				for (const pl of t.players) {
					ok.push(
						...[
							pct(pl.fg, pl.fga),
							pct(pl.tp, pl.tpa),
							pct(pl.ft, pl.fta),
							pl.seasonAvg?.fgp,
							pl.seasonAvg?.tpp,
							pl.seasonAvg?.ftp,
							pl.playoffAvg?.fgp,
							pl.playoffAvg?.tpp,
							pl.playoffAvg?.ftp,
						].filter((x): x is number => x !== undefined),
					);
				}
			}
			for (const m of sentence.matchAll(/(?<![\d.])(\d+(?:\.\d+)?)%/g)) {
				const said = Number(m[1]);
				if (!ok.some((x) => Math.abs(x - said) <= 0.051)) {
					add("percentage", `${said}% matches no shooting line`, sentence);
				}
			}
		}

		// --- assisted baskets ------------------------------------------------
		for (const m of sentence.matchAll(
			/assisted on (\d+) of their (\d+) baskets/g,
		)) {
			const ast = Number(m[1]);
			const fg = Number(m[2]);
			if (
				!game.teams.some(
					(t) => teamTotal(t, "ast") === ast && teamTotal(t, "fg") === fg,
				)
			) {
				add("assists", `${ast} of ${fg} baskets matches no team`, sentence);
			}
		}

		// --- threes ----------------------------------------------------------
		//
		// "They knocked down 18 threes", "made 7 more threes than", "13 threes
		// to 4", "on 6 three-pointers": a team's makes or attempts, a man's
		// makes, or the gap between the teams. "Missed 22 of their 29 threes"
		// is one team's misses and attempts, exactly.
		{
			const teamTp = game.teams.map((t) => teamTotal(t, "tp"));
			const teamTpa = game.teams.map((t) => teamTotal(t, "tpa"));
			const ok = new Set<number>([
				...teamTp,
				...teamTpa,
				Math.abs(teamTp[0]! - teamTp[1]!),
				...game.teams.flatMap((t) => t.players.map((pl) => pl.tp)),
			]);
			for (const m of sentence.matchAll(
				// Not the attempts of "hitting 4 of 7 threes": that is a split,
				// held to one above.
				/(?<![\d.-])(?<!\d of )(\d+) (?:more )?(?:threes|three-pointers)\b/g,
			)) {
				if (SEASON_TALK.test(sentence) || /averag/.test(sentence)) {
					continue;
				}
				if (!ok.has(Number(m[1]))) {
					add("threes", `${m[1]} threes matches nothing`, sentence);
				}
			}
			for (const m of sentence.matchAll(
				/missed (\d+) of their (\d+) threes/g,
			)) {
				const missed = Number(m[1]);
				const tried = Number(m[2]);
				if (
					!game.teams.some(
						(_t, i) =>
							teamTpa[i] === tried && teamTpa[i]! - teamTp[i]! === missed,
					)
				) {
					add(
						"threes",
						`missed ${missed} of ${tried} matches no team`,
						sentence,
					);
				}
			}
		}

		// --- a named quarter's score -----------------------------------------
		//
		// The pair right before the quarter word ("a 36-22 first quarter") or
		// the nearest one after it, but never across a clause: "jumped out to
		// a 36-22 first quarter and put together a 12-0 run in the second"
		// was read as a 12-0 first quarter.
		for (const m of sentence.matchAll(
			/(?:(\d+)-(\d+) )?(first|second|third|fourth) quarter(?:[^,.;]{0,25}?(\d+)-(\d+))?/g,
		)) {
			const said1 = m[1] ?? m[4];
			const said2 = m[2] ?? m[5];
			if (said1 === undefined || said2 === undefined) {
				continue;
			}
			const idx = { first: 0, second: 1, third: 2, fourth: 3 }[
				m[3] as "first" | "second" | "third" | "fourth"
			];
			const hq = home.ptsQtrs ?? [];
			const aq = away.ptsQtrs ?? [];
			if (idx < hq.length && idx < aq.length) {
				const said = new Set([Number(said1), Number(said2)]);
				if (!said.has(hq[idx]!) || !said.has(aq[idx]!)) {
					add(
						"quarter score",
						`${m[3]} said ${said1}-${said2}, real ${hq[idx]}-${aq[idx]}`,
						sentence,
					);
				}
			}
		}

		// --- tied at the half ------------------------------------------------
		//
		// "A-B at halftime" is held to the quarters further down; "tied at 61
		// at the break" has one number and needs its own reading.
		{
			const hq = home.ptsQtrs ?? [];
			const aq = away.ptsQtrs ?? [];
			const reg = hq.length - (game.overtimes ?? 0);
			if (reg >= 2 && reg % 2 === 0 && aq.length >= reg) {
				const sum = (q: number[]) =>
					q.slice(0, reg / 2).reduce((acc, x) => acc + x, 0);
				for (const m of sentence.matchAll(
					/\btied at (\d+) (?:at halftime|at the half\b|at the break)/g,
				)) {
					const n = Number(m[1]);
					if (n !== sum(hq) || n !== sum(aq)) {
						add(
							"halftime score",
							`said tied at ${n}, real ${sum(hq)}-${sum(aq)}`,
							sentence,
						);
					}
				}
			}
			// "...and regulation ended tied at 107".
			if (reg >= 1 && aq.length >= reg) {
				const regTotal = (q: number[]) =>
					q.slice(0, reg).reduce((acc, x) => acc + x, 0);
				for (const m of sentence.matchAll(
					/\bregulation ended tied at (\d+)/g,
				)) {
					const n = Number(m[1]);
					if (n !== regTotal(hq) || n !== regTotal(aq)) {
						add(
							"regulation score",
							`said tied at ${n}, real ${regTotal(hq)}-${regTotal(aq)}`,
							sentence,
						);
					}
				}
			}
		}

		// --- the score going into a quarter ----------------------------------
		for (const m of sentence.matchAll(
			/(?:(\d+)-(\d+)|(?:[Ll]evel|[Tt]ied) at (\d+)) going into the (second|third|fourth)\b/g,
		)) {
			const idx = { second: 1, third: 2, fourth: 3 }[
				m[4] as "second" | "third" | "fourth"
			];
			const hq = home.ptsQtrs ?? [];
			const aq = away.ptsQtrs ?? [];
			if (idx <= hq.length && idx <= aq.length) {
				const h = hq.slice(0, idx).reduce((a, b) => a + b, 0);
				const a = aq.slice(0, idx).reduce((x, y) => x + y, 0);
				const said1 = Number(m[1] ?? m[3]);
				const said2 = Number(m[2] ?? m[3]);
				const said = new Set([said1, said2]);
				if (!said.has(h) || !said.has(a)) {
					add(
						"score entering a quarter",
						`said ${said1}-${said2}, real ${h}-${a}`,
						sentence,
					);
				}
			}
		}

		// --- the margin going into a quarter --------------------------------
		// "They were 12 up going into the fourth", "It was a 2-point game
		// entering the fourth", "took a 10-point lead into the fourth":
		// told as a margin, held to the quarters all the same.
		for (const m of sentence.matchAll(
			/(\d+) (?:up|down) going into the (second|third|fourth)\b|an? (\d+)-point (?:game|lead) (?:entering|going into|into) the (second|third|fourth)\b|[Gg]oing into the (second|third|fourth), the [A-Z][\w ]*? were (?:up|down) (\d+)\b/g,
		)) {
			const said = Number(m[1] ?? m[3] ?? m[6]);
			const word = (m[2] ?? m[4] ?? m[5]) as "second" | "third" | "fourth";
			const idx = { second: 1, third: 2, fourth: 3 }[word];
			const hq = home.ptsQtrs ?? [];
			const aq = away.ptsQtrs ?? [];
			if (idx <= hq.length && idx <= aq.length) {
				const h = hq.slice(0, idx).reduce((a, b) => a + b, 0);
				const a = aq.slice(0, idx).reduce((x, y) => x + y, 0);
				if (said !== Math.abs(h - a)) {
					add(
						"margin entering a quarter",
						`said ${said}, real ${Math.abs(h - a)}`,
						sentence,
					);
				}
			}
		}
	}

	// --- how it unfolded, against the sim's own log --------------------------
	const flow = game.flow;
	if (flow) {
		const claim = (re: RegExp, ok: (n: number) => boolean, kind: string) => {
			for (const m of text.matchAll(re)) {
				const n = Number(m.slice(1).find((g) => g !== undefined));
				if (!ok(n)) {
					add(kind, `said ${n}`, m[0]);
				}
			}
		};
		claim(/(\d+) lead changes/g, (n) => n === flow.leadChanges, "lead changes");
		claim(
			/changed hands (\d+) times/g,
			(n) => n === flow.leadChanges,
			"lead changes",
		);
		claim(/(\d+) ties?\b/g, (n) => n === flow.ties, "ties");
		claim(/as many as (\d+)/g, (n) => flow.maxLead.includes(n), "biggest lead");
		claim(
			/lead reached (\d+)|up by (\d+) at their biggest|had led by (\d+)|lead that reached (\d+)/g,
			(n) => flow.maxLead.includes(n),
			"biggest lead",
		);
		claim(
			/up by (\d+) at one stage/g,
			(n) => flow.maxLead.includes(n),
			"biggest lead",
		);
		claim(
			/stretched it to (\d+)|(?:let|blew) an? (\d+)-point lead/g,
			(n) => flow.maxLead.includes(n),
			"biggest lead",
		);
		claim(/led by (\d+)\./g, (n) => flow.maxLead.includes(n), "biggest lead");
		claim(
			/led by more than (\d+)|biggest lead either way was (\d+)/g,
			(n) => n === Math.max(flow.maxLead[0], flow.maxLead[1]),
			"biggest lead",
		);
		claim(/(\d+)-0 run/g, (n) => n === flow.run?.pts, "run");
		claim(/ran off (\d+) straight/g, (n) => n === flow.run?.pts, "run");
		claim(
			/scored (\d+) unanswered|scored the last (\d+) points/g,
			(n) => n === flow.run?.pts,
			"run",
		);
		// "ran off 14 straight points in the first": the run began and ended
		// in that period. One that began with no time left in the first was
		// all second quarter.
		const RUN_PERIOD_WORDS = ["first", "second", "third", "fourth"];
		const run = flow.run;
		if (run) {
			for (const m of text.matchAll(
				/(?:straight points|unanswered|-0 (?:run|burst)) in (?:the (first|second|third|fourth)\b|overtime)/g,
			)) {
				const period = m[1]
					? RUN_PERIOD_WORDS.indexOf(m[1]) + 1
					: (game.teams[0].ptsQtrs?.length ?? 0) - (game.overtimes ?? 0) + 1;
				if (
					run.period !== period ||
					(run.endPeriod !== undefined && run.endPeriod !== period)
				) {
					add(
						"run",
						`run in period ${run.period}-${run.endPeriod ?? "?"}`,
						m[0],
					);
				}
			}
		}
		// "led by as many as 18 in the second": that side's biggest lead, in
		// that period.
		const PERIOD_WORDS = ["first", "second", "third", "fourth"];
		for (const m of text.matchAll(
			/as many as (\d+) in (?:the (first|second|third|fourth)\b|overtime)/g,
		)) {
			const n = Number(m[1]);
			const period = m[2]
				? PERIOD_WORDS.indexOf(m[2]) + 1
				: (game.teams[0].ptsQtrs?.length ?? 0) - (game.overtimes ?? 0) + 1;
			const at = flow.maxLeadAt;
			if (
				at &&
				!([0, 1] as const).some(
					(s) => flow.maxLead[s] === n && at[s] === period,
				)
			) {
				add("biggest lead", `said ${n} in period ${period}`, m[0]);
			}
		}
		// "Tied at 61 at the break" is the halftime score, checked above.
		claim(
			/(?<!regulation ended )tied at (\d+)\b(?! (?:at (?:the break|halftime|the half\b)|going into))/g,
			(n) => n === flow.lastTie?.pts,
			"last tie",
		);
		claim(
			/last tie came at (\d+)/g,
			(n) => n === flow.lastTie?.pts,
			"last tie",
		);
		// --- the closing sequence, against the scores the log passed through -
		//
		// A sentence about the finish carries a clock ("with 47.7 seconds
		// left", "at the buzzer"), and every score in it has to be one the
		// closing stretch actually reached - either order, since the sentence
		// puts whichever side it is about first.
		const { pairs, ties } = finishScores(game);
		const CLOCK =
			/seconds? (?:left|to go|to play)|\d:\d\d (?:left|to go|to play)|at the buzzer|no time left|two minutes/;
		for (const sentence of splitSentences(recap)) {
			if (!CLOCK.test(sentence)) {
				continue;
			}
			for (const m of sentence.matchAll(/\b(\d{1,3})-(\d{1,3})\b/g)) {
				if (sentence[m.index - 1] === "(") {
					continue;
				}
				// A rebounding edge or a run sharing the sentence is its own
				// number.
				const lead = sentence.slice(Math.max(0, m.index - 30), m.index);
				if (
					/glass|boards|rebound|run\b|assist|three|line|turnover/i.test(lead)
				) {
					continue;
				}
				const a = Number(m[1]);
				const b = Number(m[2]);
				const isFinal = finalPts.has(a) && finalPts.has(b) && a !== b;
				if (!pairs.has(`${a}-${b}`) && !isFinal) {
					add("finish score", `said ${a}-${b}`, sentence);
				}
			}
			// "scored the next 10 points": one side's points in a row, from a
			// score by the other side (or the start of the log) on.
			for (const m of sentence.matchAll(/the next (\d+) points/g)) {
				const n = Number(m[1]);
				const log = game.flow?.finish ?? [];
				let ok = false;
				for (let i = 0; i < log.length && !ok; i++) {
					if (i > 0 && log[i - 1]!.side === log[i]!.side) {
						continue;
					}
					let sum = 0;
					for (
						let j = i;
						j < log.length && log[j]!.side === log[i]!.side;
						j++
					) {
						sum += log[j]!.pts;
						if (sum === n) {
							ok = true;
						}
					}
				}
				if (!ok) {
					add("finish run", `said the next ${n} points`, sentence);
				}
			}
			for (const m of sentence.matchAll(/tied it at (\d+)/g)) {
				const n = Number(m[1]);
				if (!ties.has(n)) {
					add("finish tie", `said tied at ${n}`, sentence);
				}
			}
			for (const m of sentence.matchAll(/within (\d+)\b/g)) {
				const n = Number(m[1]);
				const ok = [...pairs].some((p) => {
					const [x, y] = p.split("-").map(Number);
					return Math.abs(x! - y!) === n;
				});
				if (!ok) {
					add("finish margin", `said within ${n}`, sentence);
				}
			}
		}

		const two = flow.late?.find((m) => m.clock === 120);
		for (const m of text.matchAll(
			/(?:were up|led) (\d+)-(\d+) with two minutes/g,
		)) {
			const said = new Set([Number(m[1]), Number(m[2])]);
			if (!two || !said.has(two.pts[0]) || !said.has(two.pts[1])) {
				add("late score", `said ${m[1]}-${m[2]}`, m[0]);
			}
		}

		// How far down the winners were: the log's biggest lead for the losers,
		// or what they led by at a quarter break.
		{
			const loserSide = game.teams[0].tid === winner.tid ? 1 : 0;
			const wq = winner.ptsQtrs ?? [];
			const lq = loser.ptsQtrs ?? [];
			const deficits = new Set<number>([flow.maxLead[loserSide]]);
			let wc = 0;
			let lc = 0;
			for (let i = 0; i < Math.min(wq.length, lq.length); i++) {
				wc += wq[i]!;
				lc += lq[i]!;
				deficits.add(lc - wc);
			}
			for (const m of text.matchAll(
				/(?:erased an? |spotted [^.]*?an? )(\d+)-point|came from (\d+) down|[Dd]own (\d+), |fell behind by (\d+)|from (\d+) down\b/g,
			)) {
				const n = Number(m.slice(1).find((g) => g !== undefined));
				if (!deficits.has(n)) {
					add("deficit", `said ${n} down`, m[0]);
				}
			}
		}

		// Never behind, against the log. Leading at the end of every quarter
		// is not it: a team ten down midway through the first and ahead after
		// it did not lead wire to wire. And "never got back in front" or "did
		// not trail again" says the losers HAD been in front.
		const loserPeak = flow.maxLead[game.teams[0].tid === winner.tid ? 1 : 0];
		for (const m of text.matchAll(
			/wire to wire|without ever trailing|never trailed(?! again)|start to finish|in front from the opening tip/g,
		)) {
			if (loserPeak > 0) {
				add("never trailed", `the losers led by ${loserPeak}`, m[0]);
			}
		}
		for (const m of text.matchAll(
			/(?:never|could not) get back in front|never got back in front|did not trail again|never trailed again/g,
		)) {
			if (loserPeak === 0) {
				add("back in front", "the losers never led", m[0]);
			}
		}
	}

	// --- the record in parentheses, and the score at the break ---------------
	for (const t of game.teams) {
		const rec = t.record;
		for (const m of text.matchAll(
			new RegExp(String.raw`\b[Tt]he ${t.name} \((\d+)-(\d+)\)`, "g"),
		)) {
			if (!rec || Number(m[1]) !== rec.won || Number(m[2]) !== rec.lost) {
				add(
					"record",
					`said ${m[1]}-${m[2]}, real ${rec ? `${rec.won}-${rec.lost}` : "unknown"}`,
					m[0],
				);
			}
		}
	}
	{
		const hq = home.ptsQtrs ?? [];
		const aq = away.ptsQtrs ?? [];
		const reg = hq.length - (game.overtimes ?? 0);
		if (reg >= 2 && reg % 2 === 0 && aq.length >= reg) {
			const halfN = reg / 2;
			const sum = (q: number[], from: number, to: number) =>
				q.slice(from, to).reduce((acc, x) => acc + x, 0);
			const ok = new Set<string>();
			for (const [x, y] of [
				[sum(hq, 0, halfN), sum(aq, 0, halfN)],
				[sum(hq, halfN, reg), sum(aq, halfN, reg)],
			]) {
				ok.add(`${x}-${y}`);
				ok.add(`${y}-${x}`);
			}
			for (const sentence of splitSentences(recap)) {
				if (!/halftime|at the break|the half\b|second half/.test(sentence)) {
					continue;
				}
				for (const m of sentence.matchAll(/\b(\d{1,3})-(\d{1,3})\b/g)) {
					if (sentence[m.index - 1] === "(") {
						continue;
					}
					// A rebounding edge, a run or a quarter in the same sentence
					// is its own number.
					const lead = sentence.slice(Math.max(0, m.index - 30), m.index);
					if (
						/glass|boards|rebound|run\b|assist|three|line|turnover|quarter|first|third|fourth/i.test(
							lead,
						)
					) {
						continue;
					}
					// "a 12-0 run spanning halftime" names the run after it.
					if (/^ (?:run|burst)\b/.test(sentence.slice(m.index + m[0].length))) {
						continue;
					}
					const said = `${m[1]}-${m[2]}`;
					const isFinal =
						finalPts.has(Number(m[1])) && finalPts.has(Number(m[2]));
					if (!ok.has(said) && !isFinal) {
						add("halftime score", `said ${said}`, sentence);
					}
				}
			}
		}
	}

	// --- a team high ------------------------------------------------------------
	// "Amen Dunn had a team-high 16 points": his 16, and nobody on his side
	// with more.
	for (const sentence of splitSentences(recap)) {
		for (const m of sentence.matchAll(/\bteam-high (\d+)\b/g)) {
			const who = lastNameIn(sentence.slice(0, m.index));
			const p = who ? byName.get(who) : undefined;
			const team = p
				? game.teams.find((t) => t.players.includes(p))
				: undefined;
			if (!p || !team) {
				continue;
			}
			const n = Number(m[1]);
			const top = Math.max(...team.players.map((x) => x.pts));
			if (n !== p.pts || p.pts < top) {
				add(
					"team high",
					`said ${who} ${n}, his ${p.pts}, team high ${top}`,
					sentence,
				);
			}
		}
	}

	// --- an average he carried in -----------------------------------------------
	// "came in averaging 17.3 points a game", "his 17.3 average": the season
	// line, or in the playoffs the postseason one, whichever he had.
	for (const sentence of splitSentences(recap)) {
		for (const m of sentence.matchAll(
			/\b(?:came in averaging|had been averaging) (\d+(?:\.\d)?) points a game|\bhis (\d+(?:\.\d)?)(?:-point)? average\b/g,
		)) {
			const who = lastNameIn(sentence.slice(0, m.index));
			const p = who ? byName.get(who) : undefined;
			if (!p) {
				continue;
			}
			const n = Number(m[1] ?? m[2]);
			const lines = [p.seasonAvg?.pts, p.playoffAvg?.pts].filter(
				(x): x is number => x !== undefined,
			);
			if (lines.length > 0 && !lines.some((x) => Math.abs(x - n) < 0.051)) {
				add("average", `said ${who} ${n}, real ${lines.join("/")}`, sentence);
			}
		}
	}

	// --- what a man carried in: streaks, counts, highs ------------------------
	//
	// "Has not been held under 20 in eight games", "his sixth 30-point game
	// of the season", "had not scored more than 31 in a game this season" -
	// all read off the entering context, and all held against it here.
	{
		let owner: string | undefined;
		for (const sentence of splitSentences(recap)) {
			const pronounLed = /^(?:He|His|It was his|That is|Make it)\b/.test(
				sentence,
			);
			const named = lastNameIn(sentence);
			owner = named ?? (pronounLed ? owner : undefined);
			const p = owner ? byName.get(owner) : undefined;
			const e = p?.entering;
			if (!e) {
				continue;
			}
			const num = (t: string) => wordToNumber(t);
			let m: RegExpExecArray | null;
			// Twenty-point streaks.
			m =
				/not been held under 20 in (\w+) games|(\w+) straight games of 20 or more|the (\w+) game in a row [^.]* has reached 20|Make it (\w+) in a row over 20/.exec(
					sentence,
				);
			if (m) {
				const n = num(m[1] ?? m[2] ?? m[3] ?? m[4] ?? "");
				if (n !== undefined && n !== e.streaks.twenty + 1) {
					add(
						"streak of 20",
						`said ${n}, real ${e.streaks.twenty + 1}`,
						sentence,
					);
				}
			}
			m = /(\w+) straight games? of 30|30-plus in (\w+) straight/.exec(
				sentence,
			);
			if (m) {
				const n = num(m[1] ?? m[2] ?? "");
				if (n !== undefined && n !== e.streaks.thirty + 1) {
					add(
						"streak of 30",
						`said ${n}, real ${e.streaks.thirty + 1}`,
						sentence,
					);
				}
			}
			m =
				/double-double in (\w+) straight games|(\w+) double-doubles in a row|(\w+) straight double-double/.exec(
					sentence,
				);
			if (m) {
				const n = num(m[1] ?? m[2] ?? m[3] ?? "");
				if (n !== undefined && n !== e.streaks.doubleDouble + 1) {
					add(
						"double-double streak",
						`said ${n}, real ${e.streaks.doubleDouble + 1}`,
						sentence,
					);
				}
			}
			// Counts of big games.
			m =
				/(\w+) (30|40)-point games? (?:of the season|this season)|(\w+) games of (30|40) or more|scored (30|40) or more (\w+) times/.exec(
					sentence,
				);
			if (m && e.counts) {
				const n = num(m[1] ?? m[3] ?? m[6] ?? "");
				const bar = m[2] ?? m[4] ?? m[5];
				const real = (bar === "40" ? e.counts.forty : e.counts.thirty) + 1;
				if (n !== undefined && n !== real) {
					add(`count of ${bar}`, `said ${n}, real ${real}`, sentence);
				}
			}
			// The previous scoring high - not his best against tonight's
			// opponent, which is its own claim below.
			m =
				/had not scored more than (\d+)\b(?! in \w+ earlier games against)|season high of (\d+)|previous best was (\d+)|had never gone past (\d+)|previous season high was (\d+)(?!\d| (?:rebounds|assists))/.exec(
					sentence,
				);
			if (m) {
				const n = Number(m[1] ?? m[2] ?? m[3] ?? m[4] ?? m[5]);
				if (n !== e.high.pts) {
					add("season high", `said ${n}, real ${e.high.pts}`, sentence);
				}
			}
			// "His previous season high was 13 assists."
			m = /previous season high was (\d+) (rebounds|assists)/.exec(sentence);
			if (m) {
				const n = Number(m[1]);
				const real = m[2] === "rebounds" ? e.high.reb : e.high.ast;
				if (n !== real) {
					add("season high", `said ${n} ${m[2]}, real ${real}`, sentence);
				}
			}
			// "...had not scored more than 17 in two earlier games against the
			// Suns."
			m = /had not scored more than (\d+) in (\w+) earlier games against/.exec(
				sentence,
			);
			const vs = p?.vsOpponent;
			// "...had not managed more than 7 points against the Warriors this
			// season", "The Pistons had held Corey King to 9 or fewer in every
			// earlier meeting."
			const best =
				/had not managed more than (\d+) points? against|to (\d+) or fewer in every earlier meeting/.exec(
					sentence,
				);
			if (best && vs) {
				const n = Number(best[1] ?? best[2]);
				if (n !== vs.bestPts) {
					add(
						"against this opponent",
						`said ${n}, real ${vs.bestPts}`,
						sentence,
					);
				}
			}
			if (m && vs) {
				const games = num(m[2]!);
				if (Number(m[1]) !== vs.bestPts || games !== vs.games) {
					add(
						"against this opponent",
						`said ${m[1]} in ${m[2]}, real ${vs.bestPts} in ${vs.games}`,
						sentence,
					);
				}
			}
		}
	}

	// --- no calendar ---------------------------------------------------------
	// The schedule has an order but no calendar: nothing says how many days
	// apart two games were, so "tomorrow", "in three days", "the second night
	// of a back-to-back" and "four days' rest" are all invented. A title run's
	// "back-to-back championships" is seasons, not days, and stays allowed.
	for (const m of text.matchAll(
		/\b(?:tomorrow|in (?:two|three|four|five|six|seven|\d+) days|days' rest|days of rest|a day off|days off|the night before|two nights|(?:on|of) (?:a|the) back-to-back|back end of a back-to-back)\b/gi,
	)) {
		add("calendar", `text claims schedule timing: "${m[0]}"`, m[0]!);
	}

	// --- claims about the whole game -----------------------------------------
	const saidOt = /\bovertime\b|\(OT\)|\dOT|extra period/.test(text);
	if (saidOt !== game.overtimes > 0) {
		add(
			"overtime",
			`text says ${saidOt}, game had ${game.overtimes}`,
			"(whole recap)",
		);
	}
	// How many: "forced a second overtime" needs a second, "in double
	// overtime" exactly two, "(2OT)" the same.
	const OT_ORDINALS = ["second", "third", "fourth", "fifth"];
	for (const m of text.matchAll(
		/\bforced an? (second|third|fourth|fifth) overtime\b/g,
	)) {
		const n = OT_ORDINALS.indexOf(m[1]!) + 2;
		if (n > game.overtimes) {
			add(
				"overtime",
				`said a ${m[1]} overtime, game had ${game.overtimes}`,
				m[0],
			);
		}
	}
	for (const m of text.matchAll(/\b(double|triple) overtime\b|\((\d)OT\)/g)) {
		const n = m[2] ? Number(m[2]) : m[1] === "double" ? 2 : 3;
		if (n !== game.overtimes) {
			add("overtime", `said ${m[0]}, game had ${game.overtimes}`, m[0]);
		}
	}
	// SERIES CLAIMS. A playoff recap states the bracket constantly - "Game 3
	// of the Finals", "take a 2-1 series lead", "closing out the Magic in
	// five", "#8 seed", "two wins from the title" - and every one of those is
	// checkable against the series payload the recap was written from: wins
	// entering, the winner, the length of the series, the seeds.
	const series = game.series;
	if (series && typeof series.bestOf === "number" && series.bestOf > 1) {
		const winnerIsHome = winner.abbrev === series.homeAbbrev;
		const wAfter = (winnerIsHome ? series.homeWon : series.awayWon) + 1;
		const lAfter = winnerIsHome ? series.awayWon : series.homeWon;
		const gameNo = series.homeWon + series.awayWon + 1;
		const need = Math.floor(series.bestOf / 2) + 1;

		// "Game N of the <round>" is this game; "Game N is tomorrow" the next.
		for (const m of text.matchAll(/Game (\d+) of the/g)) {
			if (Number(m[1]) !== gameNo) {
				add(
					"series game number",
					`text says Game ${m[1]}, series is at Game ${gameNo}`,
					m[0]!,
				);
			}
		}
		// Every OTHER game number in the piece is the next one: a recap looks
		// forward ("Game 5 is tomorrow", "a win in Game 5 ends it", "Game 7
		// decides it") and never further than that. Checked by the number
		// rather than by the phrasing, so a new way of saying it cannot slip
		// past the desk - which an earlier version, pinned to "Game N is
		// tomorrow", would have let through.
		for (const m of text.matchAll(/Game (\d+)(?! of the)/g)) {
			const said = Number(m[1]);
			if (said !== gameNo && said !== gameNo + 1) {
				add(
					"series next game",
					`text says Game ${said}, this is Game ${gameNo} and next is Game ${gameNo + 1}`,
					m[0]!,
				);
			}
		}

		// Any small A-B pair said in series context must be the series count
		// after this game, in some order. Quarter and game scores are two
		// digits, so single digits capped at the series length keep them out.
		const seriesPairs = [
			// Context before the pair: "cut the series deficit to 2-1".
			/(?:series (?:lead|deficit|edge)?[^.]{0,20}?|lead[^.]{0,15} series |the series |pulled? even[^.]{0,20} at |squared it[^.]{0,10} at |to the brink, |go(?:es)? up |grabbed a )(\d)-(\d)/g,
			// Pair before the keyword: "take a 3-1 series lead".
			/(\d)-(\d) (?:series lead|series deficit|edge (?:over|on)|lead (?:on|over) the)/g,
		];
		for (const m of seriesPairs.flatMap((re) => [...text.matchAll(re)])) {
			const a = Number(m[1]);
			const b = Number(m[2]);
			if (a > need || b > need) {
				continue;
			}
			const said = [a, b].sort().join("-");
			const real = [wAfter, lAfter].sort().join("-");
			if (said !== real) {
				add(
					"series score",
					`text says ${a}-${b}, series is ${wAfter}-${lAfter}`,
					m[0]!,
				);
			}
		}

		// A clinch "in five" is the games played.
		for (const m of text.matchAll(
			/(?:clos(?:e|ed|ing) out|finish(?:ed|es)? off|puts? out|sees? off)[^.]{0,40}? in (four|five|six|seven)\b/g,
		)) {
			const said = wordToNumber(m[1]!);
			if (wAfter >= need && said !== undefined && said !== gameNo) {
				add(
					"series length",
					`text says in ${m[1]}, series went ${gameNo}`,
					m[0]!,
				);
			}
		}

		// A seed is one of the two teams' seeds.
		const seeds = new Set(
			[series.homeSeed, series.awaySeed].filter(
				(seed) => typeof seed === "number",
			),
		);
		if (seeds.size > 0) {
			// "#8", "No. 8", "the eighth-seeded Thunder", "top-seeded".
			for (const m of text.matchAll(
				/(?:#|\bNo\. )(\d+)(?! seed of)|\b(top|[a-z]+)-seeded\b/g,
			)) {
				const said = m[1]
					? Number(m[1])
					: m[2] === "top"
						? 1
						: wordToNumber(m[2]!);
				if (said !== undefined && !seeds.has(said)) {
					add(
						"series seed",
						`text says #${said}, series seeds are ${[...seeds].join(", ")}`,
						m[0]!,
					);
				}
			}
		}

		// "N wins from the title" belongs to one of the two sides' remaining
		// counts after tonight.
		for (const m of text.matchAll(
			/(one|two|three|four|\d) (?:more )?(?:wins?|loss(?:es)?) from (?:the title|the next round|going out|putting out)/g,
		)) {
			const said = wordToNumber(m[1]!);
			const remaining = new Set([need - wAfter, need - lAfter]);
			if (said !== undefined && !remaining.has(said)) {
				add(
					"series wins remaining",
					`text says ${said}, remaining counts are ${[...remaining].join(", ")}`,
					m[0]!,
				);
			}
		}

		// FRANCHISE TITLE HISTORY. The clinch line reaches into the past -
		// "first in franchise history", "first since 2019", "back-to-back" -
		// and the payload it was written from rides along, so hold it to it.
		const th = winner.titleHistory;
		if (th) {
			if (
				/first championship in franchise history/i.test(text) &&
				th.titles.length > 0
			) {
				add(
					"franchise first title",
					`text claims a first title, franchise won in ${th.titles.join(", ")}`,
					"first championship in franchise history",
				);
			}
			for (const m of text.matchAll(
				/first (?:championship|title|one)?\s?since (\d{4})/g,
			)) {
				if (th.titles.at(-1) !== Number(m[1])) {
					add(
						"franchise last title year",
						`text says first since ${m[1]}, last title was ${th.titles.at(-1) ?? "never"}`,
						m[0]!,
					);
				}
			}
			if (
				/back-to-back championships/i.test(text) &&
				th.titles.at(-1) !== th.season - 1
			) {
				add(
					"back-to-back claim",
					`text claims back-to-back, last title was ${th.titles.at(-1) ?? "never"} and this is ${th.season}`,
					"back-to-back championships",
				);
			}
			for (const m of text.matchAll(
				/the (\w+) championship in franchise history/g,
			)) {
				const said = wordToNumber(m[1]!);
				if (said !== undefined && said !== th.titles.length + 1) {
					add(
						"franchise title count",
						`text says title #${said}, this is #${th.titles.length + 1}`,
						m[0]!,
					);
				}
			}
		}
	}

	if (
		// "never trailed AGAIN" is the finish of a comeback, not wire to wire.
		/never trailed(?! again)|wire to wire|led from|in front from the opening tip|start to finish/i.test(
			text,
		) &&
		// "series deficit" is the bracket, not the scoreboard: a team can cut
		// its series deficit in a game it led wire to wire, and one did.
		/comeback|erased|rallied|stormed back|came from \d|(?<!series )deficit|down \d+ (?:at|after)/i.test(
			text,
		)
	) {
		add(
			"wire-to-wire and a comeback",
			"the same game cannot be both",
			"(whole recap)",
		);
	}

	return out;
};
