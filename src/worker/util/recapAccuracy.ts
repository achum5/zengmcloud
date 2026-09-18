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
	/beat|topped|handled|downed|routed|edged|held off|took down|stunned|shocked|knocked off|upset|survived|outlast|blew out|rolled|cruised|got past|pulled away|defeat|\bwin\b|\bwon\b/i;

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
		for (const m of sentence.matchAll(/(?<![\d.])(\d+)-of-(\d+)/g)) {
			if (!splits.has(`${m[1]}-${m[2]}`)) {
				add("shooting split", `${m[1]}-of-${m[2]} matches nothing`, sentence);
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
		claim(/led by (\d+)\./g, (n) => flow.maxLead.includes(n), "biggest lead");
		claim(
			/led by more than (\d+)|biggest lead either way was (\d+)/g,
			(n) => n === Math.max(flow.maxLead[0], flow.maxLead[1]),
			"biggest lead",
		);
		claim(/(\d+)-0 run/g, (n) => n === flow.run?.pts, "run");
		claim(/ran off (\d+) straight/g, (n) => n === flow.run?.pts, "run");
		claim(/tied at (\d+)/g, (n) => n === flow.lastTie?.pts, "last tie");
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
				/(\w+) (30|40)-point games? (?:of the season|this season)|(\w+) games of (30|40) or more/.exec(
					sentence,
				);
			if (m && e.counts) {
				const n = num(m[1] ?? m[3] ?? "");
				const bar = m[2] ?? m[4];
				const real = (bar === "40" ? e.counts.forty : e.counts.thirty) + 1;
				if (n !== undefined && n !== real) {
					add(`count of ${bar}`, `said ${n}, real ${real}`, sentence);
				}
			}
			// The previous scoring high.
			m =
				/had not scored more than (\d+)|season high of (\d+)|previous best was (\d+)|had never gone past (\d+)/.exec(
					sentence,
				);
			if (m) {
				const n = Number(m[1] ?? m[2] ?? m[3] ?? m[4]);
				if (n !== e.high.pts) {
					add("season high", `said ${n}, real ${e.high.pts}`, sentence);
				}
			}
		}
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
			for (const m of text.matchAll(/#(\d+)(?! seed of)/g)) {
				if (!seeds.has(Number(m[1]))) {
					add(
						"series seed",
						`text says #${m[1]}, series seeds are ${[...seeds].join(", ")}`,
						m[0]!,
					);
				}
			}
		}

		// "N wins from the title" belongs to one of the two sides' remaining
		// counts after tonight.
		for (const m of text.matchAll(
			/(one|two|three|four|\d) (?:more )?wins? from (?:the title|the next round|going out|putting out)/g,
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
