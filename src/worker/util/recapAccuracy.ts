import type {
	RecapDayStandings,
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import { finishScores, withSettledLead } from "./recapFinish.ts";
import { analyzeShape, comebackSize } from "./getAutoRecap.ts";
import { clockLeft } from "../../common/gameFlow.ts";
import { expandSecondReferences, surnameOf } from "./recapNames.ts";

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

// A sentence on the clock - "with 47.7 seconds left", "with two minutes to
// play" - is about the closing stretch, and its scores are held to that.
const FINISH_CLOCK =
	/seconds? (?:left|to go|to play)|\d:\d\d (?:left|to go|to play)|at the buzzer|no time left|two minutes/;

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
	/quarter|after one|at the break|halftime|half|first|second|third|fourth|\brun\b|stretch|outscor|closed|opened|took over|settled it|broke it open|spurt|glass|boards|rebound|free throw|from the line|threes|from deep|assists|pushed|improved|moved|fell to|dropped to|climbed|meeting|season series|this season|at home|on the road|away from home|own building|bench|reserves|to go\b|\bleft\b|to play|lead change|tied at|as many as|straight points|two minutes|for good|ahead for the last time|went in front|ahead \d|up \d+-\d+|within|cut it|back to|as close as|made it \d|buzzer|no time|remaining|overtime|extra period/i;

// A count in one of these sentences is a season or career total, a season
// high being quoted, or a bench total - not a line from tonight's box score.
const SEASON_TALK =
	/\bcareer\b(?!-high)|-point mark|-rebound mark|-assist mark|for the season|on the season|this season|of the season|season high/i;
const BENCH_TALK = /\bbench\b|\breserves?\b/i;

// A points figure in one of these sentences is a betting margin, not a stat.
const SPREAD_TALK =
	/the line|wrong side|favou?r|underdog|no chance|getting \d|spread|\bbooks\b/i;

const RESULT_VERB =
	/beat|topped|handled|downed|routed|edged|held off|took down|stunned|shocked|knocked off|upset|survived|outlast|blew out|rolled|cruised|crushed|trounced|got past|pulled away|defeat|\bwin\b|\bwon\b/i;

// A headline names a man by his surname, the first time he is named at all:
// "Jackson has 26 points, 14 rebounds" is the one Jackson in the game.
const wholeNamesInHeadline = (text: string, names: string[]): string => {
	const nl = text.indexOf("\n");
	const head = nl < 0 ? text : text.slice(0, nl);
	if (!head.startsWith("**")) {
		return text;
	}
	const bySurname = new Map<string, string[]>();
	for (const n of names) {
		const sur = surnameOf(n);
		if (sur) {
			bySurname.set(sur, [...(bySurname.get(sur) ?? []), n]);
		}
	}
	let out = head;
	for (const [sur, whole] of bySurname) {
		const full = whole[0]!;
		if (whole.length !== 1 || full === sur) {
			continue;
		}
		const esc = sur.replaceAll(/[$()*+.?[\\\]^{|}]/g, String.raw`\$&`);
		out = out.replaceAll(
			new RegExp(String.raw`\b${esc}\b`, "g"),
			(m, at: number) =>
				out.slice(at + sur.length - full.length, at + sur.length) === full
					? m
					: full,
		);
	}
	return nl < 0 ? out : out + text.slice(nl);
};

export const verifyRecap = (
	written: string,
	game: RecapGame,
): RecapViolation[] => {
	// "Ingram's layup tied it" is Lonnie Ingram's layup: the recap writes a
	// man's surname after his first mention, and the checks below go by
	// whole names.
	const recap = expandSecondReferences(
		wholeNamesInHeadline(
			written,
			game.teams.flatMap((t) => t.players.map((p) => p.name)),
		),
		game.teams.flatMap((t) => [
			...t.players.map((p) => p.name),
			...(t.injuries ?? []).map((p) => p.name),
		]),
		game.teams.map((t) => ({ name: t.name ?? "", region: t.region ?? "" })),
	);
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
	// "28 points in just 26 minutes": his minutes, the nearest man before it.
	for (const sentence of splitSentences(recap)) {
		for (const m of sentence.matchAll(/\bin (?:just|only) (\d+) minutes/g)) {
			const who = lastNameIn(sentence.slice(0, m.index));
			const p = who ? byName.get(who) : undefined;
			if (p && p.min !== Number(m[1])) {
				add("minutes", `said ${m[1]}, real ${p.min} for ${who}`, sentence);
			}
		}
	}
	for (const sentence of splitSentences(recap)) {
		const prevOwner = chainOwner;
		const pronounLed = /^(?:He|His|She|Her)\b/.test(sentence);
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
				// "came in averaging 12 points a game" is his season line, and
				// "kept well under their 105 points a game" a team's.
				if (
					/averag\w*\s*$/i.test(before) ||
					/^ (?:a|per) game\b/.test(sentence.slice(m.index + m[0].length))
				) {
					continue;
				}
				// "scored the next 10 points" is a stretch of the finish, and "the
				// last 12 points" an overtime run: both checked against the log
				// below.
				if (/\bthe (?:next|last)\s*$/.test(before)) {
					continue;
				}
				// "came back from 17 points down" is the deficit, held to the
				// game's own.
				if (
					word === "points" &&
					/^ down\b/.test(sentence.slice(m.index + m[0].length))
				) {
					const deficit = comebackSize(analyzeShape(game));
					if (n !== deficit) {
						add("deficit", `said ${n} down, real ${deficit}`, sentence);
					}
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
					// "...from rookie Jalen Green", "...from 36-year-old Vince
					// Carter".
					const tail = after
						.slice(fromMatch.index + fromMatch[0].length)
						.replace(/^(?:rookie |\d+-year-old )/, "");
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
			/\ball (\d+) of (?:his|her) (?:threes|three-pointers|free throws|shots)\b/g,
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
		{
			const hq = home.ptsQtrs ?? [];
			const aq = away.ptsQtrs ?? [];
			const IDX: Record<string, number> = {
				first: 0,
				second: 1,
				third: 2,
				fourth: 3,
			};
			const lastIdx = hq.length - (game.overtimes ?? 0) - 1;
			const hold = (word: string, said1: string, said2: string) => {
				const idx = word === "final" || word === "last" ? lastIdx : IDX[word]!;
				if (idx < 0 || idx >= hq.length || idx >= aq.length) {
					return;
				}
				const said = new Set([Number(said1), Number(said2)]);
				if (!said.has(hq[idx]!) || !said.has(aq[idx]!)) {
					add(
						"quarter score",
						`${word} said ${said1}-${said2}, real ${hq[idx]}-${aq[idx]}`,
						sentence,
					);
				}
			};
			// "a 36-22 first quarter", or the pair that follows the quarter's
			// name ("took the fourth quarter 39-27", "The third quarter settled
			// it: 27-16") - but never one that belongs to something else: "in
			// the first quarter and still led 67-52 at halftime", "in the fourth
			// quarter on the way to a 101-94 win", a record in parentheses.
			for (const m of sentence.matchAll(
				// Not "quarters": "a 19-0 run spanning the first and second
				// quarters and led 49-41 at halftime" names no one quarter's score.
				/(?:(\d+)-(\d+) )?(first|second|third|fourth) quarter(?!s)(?:([^,.;]{0,25}?)(\d+)-(\d+))?/g,
			)) {
				const after =
					m[4] !== undefined &&
					!/\b(?:led|lead|up|ahead|trailed|halftime|half|win|beat|top|topped|way to|after|going|into|deficit|down|run)\b|\(/.test(
						m[4],
					);
				// The final score is the final score, wherever it sits.
				const isFinal =
					m[1] === undefined &&
					finalPts.has(Number(m[5])) &&
					finalPts.has(Number(m[6])) &&
					m[5] !== m[6];
				const said1 = m[1] ?? (after && !isFinal ? m[5] : undefined);
				const said2 = m[2] ?? (after && !isFinal ? m[6] : undefined);
				if (said1 !== undefined && said2 !== undefined) {
					hold(m[3]!, said1, said2);
				}
			}
			// "outscored the Hawks 33-22 in the third quarter".
			for (const m of sentence.matchAll(
				/(\d+)-(\d+) in the (first|second|third|fourth)\b/g,
			)) {
				hold(m[3]!, m[1]!, m[2]!);
			}
			// "won the second 25-16", "took the final quarter 36-21".
			for (const m of sentence.matchAll(
				/\b(?:won|took|take|taking|winning|had) the (first|second|third|fourth|final|last)(?: quarter)?,? (\d+)-(\d+)/g,
			)) {
				hold(m[1]!, m[2]!, m[3]!);
			}
			// "...going into the fourth, where the Bucks won the quarter 30-19",
			// and "outscored them 30-19 the rest of the way" after the score
			// through three: the last quarter of regulation.
			if ((game.overtimes ?? 0) === 0) {
				for (const m of sentence.matchAll(
					/(?:going into the fourth|after three quarters)[^.]*?(?:won the quarter (\d+)-(\d+)|(\d+)-(\d+) the rest of the way)/g,
				)) {
					hold("last", m[1] ?? m[3]!, m[2] ?? m[4]!);
				}
			}
			// "led 31-18 after one quarter", "83-75 after three quarters": the
			// running score when that quarter ended.
			const AFTER: Record<string, number> = { one: 1, two: 2, three: 3 };
			for (const m of sentence.matchAll(
				// "...through three quarters", and "with a quarter to play",
				// which is three of four.
				/(\d+)-(\d+)(?: lead)? (?:(?:after|through) (one|two|three) quarters?\b|with a quarter to play|(out of the first quarter))/g,
			)) {
				const n = m[4]
					? 1
					: m[3] === undefined
						? hq.length - (game.overtimes ?? 0) - 1
						: AFTER[m[3]]!;
				if (n < 1) {
					continue;
				}
				if (n > hq.length || n > aq.length) {
					continue;
				}
				const h = hq.slice(0, n).reduce((x, y) => x + y, 0);
				const a = aq.slice(0, n).reduce((x, y) => x + y, 0);
				const said = new Set([Number(m[1]), Number(m[2])]);
				if (!said.has(h) || !said.has(a)) {
					add(
						"score after a quarter",
						`said ${m[1]}-${m[2]} after ${m[3] ?? "three"}, real ${h}-${a}`,
						sentence,
					);
				}
			}
		}

		// --- the extra period's own score -----------------------------------
		// "The Magic won overtime 18-11", "outscored the Nets 12-2 in
		// overtime": every overtime period, summed.
		{
			const hq = home.ptsQtrs ?? [];
			const aq = away.ptsQtrs ?? [];
			const ot = game.overtimes ?? 0;
			if (ot > 0 && hq.length === aq.length && hq.length > ot) {
				const sum = (q: number[]) => q.slice(-ot).reduce((x, y) => x + y, 0);
				for (const m of sentence.matchAll(
					// Not "beat the Bucks 127-124 in overtime", which is the final.
					/won (?:overtime|the overtimes) (\d+)-(\d+)|outscored [^.]*? (\d+)-(\d+) in (?:overtime|the overtimes|the extra period)/g,
				)) {
					const said = new Set([Number(m[1] ?? m[3]), Number(m[2] ?? m[4])]);
					if (!said.has(sum(hq)) || !said.has(sum(aq))) {
						add(
							"overtime score",
							`said ${m[1] ?? m[3]}-${m[2] ?? m[4]}, real ${sum(hq)}-${sum(aq)}`,
							sentence,
						);
					}
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
			// "80-78 going into the fourth", and "still in front, 80-78, going
			// into the fourth".
			/(?:(\d+)-(\d+),?|(?:[Ll]evel|[Tt]ied) at (\d+)) going into the (second|third|fourth)\b/g,
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
			/(\d+) (?:up|down) going into the (second|third|fourth)\b|an? (\d+)-point (?:game|lead) (?:entering|going into|into) the (second|third|fourth)\b|[Gg]oing into the (second|third|fourth), the [A-Z][\w ]*? were (?:up|down) (\d+)\b|(?:lead was|[Ii]t was|That was) down to (\d+) going into the (second|third|fourth)\b|lead (?:had grown|was out) to (\d+) going into the (second|third|fourth)\b|\bwas still (\d+) going into the (second|third|fourth)\b|trailed by (a point|two) going into the (second|third|fourth)\b/g,
		)) {
			const said =
				m[13] === "a point"
					? 1
					: m[13] === "two"
						? 2
						: Number(m[1] ?? m[3] ?? m[6] ?? m[7] ?? m[9] ?? m[11]);
			const word = (m[2] ?? m[4] ?? m[5] ?? m[8] ?? m[10] ?? m[12] ?? m[14]) as
				| "second"
				| "third"
				| "fourth";
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
			/lead (?:had )?reached (\d+)|up by (\d+) at their biggest|had led by (\d+)|lead that reached (\d+)|pushed the lead to (\d+)/g,
			(n) => flow.maxLead.includes(n),
			"biggest lead",
		);
		claim(
			/up by (\d+) at one stage/g,
			(n) => flow.maxLead.includes(n),
			"biggest lead",
		);
		claim(
			/stretched it to (\d+)\b(?!-)|(?:let|blew) an? (\d+)-point lead/g,
			(n) => flow.maxLead.includes(n),
			"biggest lead",
		);
		claim(/led by (\d+)\./g, (n) => flow.maxLead.includes(n), "biggest lead");
		claim(
			/led by more than (\d+)|biggest lead either way was (\d+)/g,
			(n) => n === Math.max(flow.maxLead[0], flow.maxLead[1]),
			"biggest lead",
		);
		// The runs the score log found (recapRuns.ts), on top of the flow's
		// one unanswered run.
		const logRuns = game.runs?.runs ?? [];
		const straight = (n: number) =>
			n === flow.run?.pts || logRuns.some((r) => r.opp === 0 && r.pts === n);
		claim(/(\d+)-0 run/g, straight, "run");
		// "a 31-6 run": one the log has, both numbers.
		for (const m of text.matchAll(/\b(\d+)-([1-9]\d*) (?:run|burst)\b/g)) {
			const a = Number(m[1]);
			const b = Number(m[2]);
			if (!logRuns.some((r) => r.pts === a && r.opp === b)) {
				add(
					"run",
					`said ${a}-${b}, log has ${logRuns.map((r) => `${r.pts}-${r.opp}`).join(", ")}`,
					m[0],
				);
			}
		}
		// "scored 20 of the last 21 points": a run the game ended on.
		for (const m of text.matchAll(/(\d+) of the last (\d+) points/g)) {
			const a = Number(m[1]);
			const b = Number(m[2]);
			if (
				!logRuns.some((r) => r.closed && r.pts === a && r.pts + r.opp === b)
			) {
				add("run", `said ${a} of the last ${b}`, m[0]);
			}
		}
		// "...a 31-6 run in the second quarter, 12 of the points from Devin
		// Nash": that run's top scorer, with that many.
		for (const m of text.matchAll(
			/(\d+)-(\d+) run[^.]*?, (all|\w+) of (?:the points|them) from ([^,.]+?)[,.]/g,
		)) {
			const a = Number(m[1]);
			const b = Number(m[2]);
			const r = logRuns.find((x) => x.pts === a && x.opp === b);
			const who = byName.get(m[4]!.trim());
			const said = m[3] === "all" ? r?.pts : wordToNumber(m[3]!);
			if (!r || !who || r.top?.pid !== who.pid || r.top?.pts !== said) {
				add("run scorer", `said ${m[3]} of ${a}-${b} from ${m[4]}`, m[0]);
			}
		}
		// "The Knicks did not make a field goal over the final 5:00": a dry
		// spell the log has, to the buzzer.
		for (const m of text.matchAll(
			/(?:did not make a field goal over the final|went the last) ([\d.:]+(?: seconds)?)(?: without a field goal)?/g,
		)) {
			const ok = (game.runs?.droughts ?? []).some(
				(d) => d.kind === "fg" && d.toEnd && clockLeft(d.from.clock) === m[1],
			);
			if (!ok) {
				add("drought", `said ${m[1]}`, m[0]);
			}
		}
		// "The Spurs were up 120-116 with two minutes left. The Nuggets
		// outscored them 13-6 from there": from the two-minute score to the
		// final.
		const twoMin = flow.late?.find((m) => m.clock === 120);
		const all = splitSentences(recap);
		for (let i = 1; i < all.length; i++) {
			const m = /outscored (?:them|the [\w ]+?) (\d+)-(\d+) from there/.exec(
				all[i]!,
			);
			if (!m || !/two minutes/.test(all[i - 1]!)) {
				continue;
			}
			const wSide = game.teams[0].tid === winner.tid ? 0 : 1;
			const real = twoMin
				? `${winner.pts - twoMin.pts[wSide]!}-${loser.pts - twoMin.pts[1 - wSide]!}`
				: undefined;
			if (real !== `${m[1]}-${m[2]}`) {
				add("last two minutes", `said ${m[1]}-${m[2]}, real ${real}`, all[i]!);
			}
		}
		claim(/ran off (\d+) straight/g, straight, "run");
		claim(/(?:scored|reeled off) (\d+) straight points/g, straight, "run");
		// "...scored 15 straight points, eight of them from Devin Nash": the
		// run's top scorer and his share of it, as the log has them.
		for (const m of text.matchAll(
			/(\d+) straight points[^.]*?, (all|\w+) of them from ([^,.]+?)[,.]/g,
		)) {
			const who = byName.get(m[3]!.trim());
			const run = flow.run;
			const n = Number(m[1]);
			const said = m[2] === "all" ? n : wordToNumber(m[2]!);
			const fromLog = logRuns.some(
				(r) =>
					r.opp === 0 &&
					r.pts === n &&
					r.top?.pid === who?.pid &&
					r.top?.pts === said,
			);
			if (
				!fromLog &&
				(!who ||
					!run ||
					run.pts !== n ||
					run.topPid !== who.pid ||
					run.topPts !== said ||
					said === undefined)
			) {
				add(
					"run scorer",
					`said ${m[2]} from ${m[3]}, the log has ${run?.topPts} from pid ${run?.topPid}`,
					m[0],
				);
			}
		}
		// "...with Zeke Foster scoring 14 of his 25 points in the quarter":
		// his night, and a quarter in which he led his side with that many.
		for (const m of text.matchAll(
			/with ([^,.]+?) scoring (?:(\d+) of his (\d+)|all (\d+) of his points)\b/g,
		)) {
			const who = byName.get(m[1]!.trim());
			const part = Number(m[2] ?? m[4]);
			const whole = Number(m[3] ?? m[4]);
			const top = flow.periodTop ?? [];
			let led = false;
			for (let i = 0; i + 1 < top.length; i += 2) {
				if (who && top[i] === who.pid && top[i + 1] === part) {
					led = true;
				}
			}
			if (!who || who.pts !== whole || !led) {
				add("quarter scorer", `said ${part} of ${whole} for ${m[1]}`, m[0]);
			}
		}
		claim(/scored (\d+) unanswered/g, straight, "run");
		claim(
			/scored the last (\d+) points/g,
			(n) =>
				n === flow.run?.pts ||
				logRuns.some((r) => r.closed && r.opp === 0 && r.pts === n),
			"run",
		);
		// "ran off 14 straight points in the first": the run began and ended
		// in that period. One that began with no time left in the first was
		// all second quarter.
		const RUN_PERIOD_WORDS = ["first", "second", "third", "fourth"];
		const run = flow.run;
		if (run || logRuns.length > 0) {
			for (const m of text.matchAll(
				/(?:(\d+) straight points|(\d+) unanswered|(\d+)-(\d+) (?:run|burst))(?:, [^,.]+,)? in (?:the (first|second|third|fourth)\b|overtime)/g,
			)) {
				const period = m[5]
					? RUN_PERIOD_WORDS.indexOf(m[5]) + 1
					: (game.teams[0].ptsQtrs?.length ?? 0) - (game.overtimes ?? 0) + 1;
				const pts = Number(m[1] ?? m[2] ?? m[3]);
				const opp = Number(m[4] ?? 0);
				const inLog = logRuns.some(
					(r) =>
						r.pts === pts &&
						r.opp === opp &&
						r.from.period === period &&
						r.to.period === period,
				);
				if (
					!inLog &&
					(!run ||
						run.pts !== pts ||
						opp !== 0 ||
						run.period !== period ||
						(run.endPeriod !== undefined && run.endPeriod !== period))
				) {
					add(
						"run",
						`run in period ${run?.period}-${run?.endPeriod ?? "?"}`,
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
		// "Tied at 61 at the break" is the halftime score, checked above, and
		// "tied at 110 with two minutes to play" is a score on the clock,
		// checked with the finish below.
		for (const sentence of splitSentences(text)) {
			if (FINISH_CLOCK.test(sentence)) {
				continue;
			}
			// "With the score tied at 28, Miami went on a 17-4 run" is where a
			// run started, not the last tie: held to the runs instead.
			const runFrom = /^With the score tied at (\d+), /.exec(sentence);
			for (const m of sentence.matchAll(
				/(?<!regulation ended )tied at (\d+)\b(?! (?:at (?:the break|halftime|the half\b)|going into))/g,
			)) {
				const n = Number(m[1]);
				if (
					runFrom &&
					Number(runFrom[1]) === n &&
					/\brun\b|scored \w+ of the/.test(sentence) &&
					(game.runs?.runs ?? []).some(
						(r) => r.from.score[0] === n && r.from.score[1] === n,
					)
				) {
					continue;
				}
				if (n !== flow.lastTie?.pts) {
					add("last tie", `said ${m[1]}`, m[0]);
				}
			}
		}
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
		for (const sentence of splitSentences(recap)) {
			if (!FINISH_CLOCK.test(sentence)) {
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
					/glass|boards|rebound|run\b|assist|three|line|turnover|outscored/i.test(
						lead,
					)
				) {
					continue;
				}
				// "...with 38.6 seconds left, closing out the series 4-2": the
				// series, checked with the series claims.
				if (/series(?: at)? $|series lead $/.test(lead)) {
					continue;
				}
				// "closed on an 18-6 run": a run, checked with the runs.
				if (/^ (?:run|burst)\b/.test(sentence.slice(m.index + m[0].length))) {
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
			for (const m of sentence.matchAll(/tied (?:it )?at (\d+)/g)) {
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
					// "led 31-18 after one quarter and 51-50 at halftime", "82-79
					// after three quarters", "won the second 25-16", "outscored
					// them 20-16 the rest of the way": each is held to its own
					// quarter above.
					if (
						/^(?: lead)?,? (?:(?:after|through) (?:one|two|three) quarters?|with a quarter to play|out of the first quarter|going into|in the (?:first|second|third|fourth)|the rest of the way|(?:first|second|third|fourth) quarter)\b/.test(
							sentence.slice(m.index + m[0].length),
						) ||
						/\b(?:won|took|take|taking|winning|had) the (?:first|second|third|fourth|final|last)(?: quarter)?,? $|\bwon the quarter(?: anyway,)? $/.test(
							sentence.slice(0, m.index),
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
			/\b(?:came in averaging|had been averaging) (\d+(?:\.\d)?) points a game|\b(?:his|her) (\d+(?:\.\d)?)(?:-point)? average\b/g,
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

	// --- "a season-high 32 points" ---------------------------------------------
	// Some man named in the sentence had that figure tonight, more than he
	// had had in any game this season before it.
	for (const sentence of splitSentences(recap)) {
		for (const m of sentence.matchAll(
			/\bseason-high (\d+) (points|rebounds|assists)\b/g,
		)) {
			const n = Number(m[1]);
			const key =
				m[2] === "points" ? "pts" : m[2] === "rebounds" ? "reb" : "ast";
			const owns = names
				.filter((nm) => sentence.includes(nm))
				.map((nm) => byName.get(nm)!)
				.some((p) => p[key] === n && (p.entering?.high[key] ?? -1) < n);
			if (!owns) {
				add("season high", `no one named had ${n} ${m[2]} as a high`, sentence);
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
			const pronounLed =
				/^(?:He|His|She|Her|It was his|It was her|That is|Make it)\b/.test(
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

	// --- "went in front for good with 1:52 left" ------------------------------
	// The basket that put the winners ahead for good, at the time the log
	// says - and only when the log can say: a tie in the first quarter was
	// broken by a basket it never kept, and the first one it did keep was
	// being called the go-ahead.
	// (A log with no lead change recorded at all says nothing either way.)
	if (game.flow?.lastLead) {
		const settled = withSettledLead(game).flow?.lastLead;
		const at = settled ? clockLeft(settled.clock) : undefined;
		for (const sentence of splitSentences(recap)) {
			if (
				!/for good|for the last time|never trailed again|(?:ahead|in front) to stay/.test(
					sentence,
				)
			) {
				continue;
			}
			const clocks = [
				...sentence.matchAll(
					/\bwith (\d+:\d\d|\d+(?:\.\d)? seconds?) (?:left|to go|to play)/g,
				),
			].map((m) => m[1]);
			// "...tied it at 108 with 1:22 to play before Thompson's
			// three-pointer 15 seconds later put the Magic ahead for good":
			// the go-ahead is the clock named, less the seconds.
			const later = /\b(\d+) seconds later\b/.exec(sentence);
			const first = clocks[0];
			if (later && first !== undefined && settled) {
				const secs = first.includes(":")
					? Number(first.split(":")[0]) * 60 + Number(first.split(":")[1])
					: Number.parseFloat(first);
				// The clock is printed to the second above a minute.
				if (Math.abs(secs - Number(later[1]) - settled.clock) <= 1.6) {
					continue;
				}
			}
			if (clocks.length > 0 && (at === undefined || !clocks.includes(at))) {
				add(
					"go-ahead for good",
					`said ${clocks.join("/")}, the log has ${at ?? "no settled lead"}`,
					sentence,
				);
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

	// --- careers: highs, firsts, debuts, moves -------------------------------
	//
	// "a career-high 34 points", "It was Devin Nash's first career
	// triple-double", "made his Nuggets debut", "the third pick in the 2025
	// draft" - each held against the career the builder read (recapCareer).
	{
		const UNIT_KEY: Record<string, "pts" | "reb" | "ast" | "tp"> = {
			points: "pts",
			rebounds: "reb",
			assists: "ast",
			"3-pointers": "tp",
		};
		let owner: string | undefined;
		for (const sentence of splitSentences(recap)) {
			const pronounLed = /^(?:He|His|She|Her|It was (?:his|her))\b/.test(
				sentence,
			);
			const named = names.filter((nm) => sentence.includes(nm));
			owner = lastNameIn(sentence) ?? (pronounLed ? owner : undefined);
			for (const m of sentence.matchAll(
				/\bcareer-high (\d+) (points|rebounds|assists|3-pointers)\b/g,
			)) {
				const n = Number(m[1]);
				const key = UNIT_KEY[m[2]!]!;
				const owns = named
					.map((nm) => byName.get(nm)!)
					.some((p) => p[key] === n && !!p.past && p.past.high[key] < n);
				if (!owns) {
					add("career high", `no one named had ${n} ${m[2]} as one`, sentence);
				}
			}
			let m =
				/The (\d+) (points|rebounds|assists|3-pointers) were a career high for (.+?)\.$/.exec(
					sentence,
				);
			if (m) {
				const p = byName.get(m[3]!);
				const key = UNIT_KEY[m[2]!]!;
				const n = Number(m[1]);
				if (!p || p[key] !== n || !p.past || p.past.high[key] >= n) {
					add("career high", `said ${n} ${m[2]} for ${m[3]}`, sentence);
				}
			}
			const p = owner ? byName.get(owner) : undefined;
			m =
				/set a career high with (\d+) (points|rebounds|assists|3-pointers)/.exec(
					sentence,
				);
			if (m) {
				const key = UNIT_KEY[m[2]!]!;
				const n = Number(m[1]);
				if (!p?.past || p[key] !== n || p.past.high[key] >= n) {
					add("career high", `said ${n} ${m[2]} for ${owner}`, sentence);
				}
			}
			m =
				/career high of (\d+)|previous career high was (\d+)|had never scored more than (\d+)/.exec(
					sentence,
				);
			if (m) {
				const n = Number(m[1] ?? m[2] ?? m[3]);
				if (!p?.past || p.past.high.pts !== n || p.pts <= n) {
					add(
						"career high",
						`said ${n}, real ${p?.past?.high.pts ?? "unknown"}`,
						sentence,
					);
				}
			}
			const doubles = p
				? [p.pts, p.reb, p.ast, p.stl, p.blk].filter((v) => v >= 10).length
				: 0;
			if (
				/first (?:career )?triple-double|first triple-double of (?:his|her) career/.test(
					sentence,
				)
			) {
				if (!p?.past || p.past.td !== 0 || doubles < 3) {
					add("first triple-double", `not one for ${owner}`, sentence);
				}
			}
			if (/first career double-double/.test(sentence)) {
				if (!p?.past || p.past.dd !== 0 || doubles < 2) {
					add("first double-double", `not one for ${owner}`, sentence);
				}
			}
			m = /first (30|40)-point game|had never scored (30|40) in a game/.exec(
				sentence,
			);
			if (m) {
				const bar = Number(m[1] ?? m[2]);
				if (!p?.past || p.past.high.pts >= bar || p.pts < bar) {
					add(`first ${bar}`, `not one for ${owner}`, sentence);
				}
			}
			if (
				/making (?:his|her) debut|first game in the league|made (?:his|her) debut|in (?:his|her) debut/.test(
					sentence,
				)
			) {
				if (p?.past?.gp !== 0) {
					add("debut", `not ${owner}'s first game`, sentence);
				}
			}
			m = /made (?:his|her) (.+?) debut/.exec(sentence);
			if (m && p) {
				const t = game.teams.find((x) => x.players.includes(p));
				if (!p.newTeam || t?.name !== m[1]) {
					add("team debut", `not ${owner}'s first game for ${m[1]}`, sentence);
				}
				const from = /from the (.+?) in a trade/.exec(sentence);
				if (
					from &&
					(p.newTeam?.via !== "trade" || p.newTeam.name !== from[1])
				) {
					add("trade", `not traded from ${from[1]}`, sentence);
				}
				if (/as a free agent/.test(sentence) && p.newTeam?.via !== "signing") {
					add("signing", `${owner} was not signed`, sentence);
				}
			}
			m =
				/facing the (.+?) for the first time since they traded|first game against the (.+?) since leaving/.exec(
					sentence,
				);
			if (m) {
				const club = m[1] ?? m[2];
				if (
					!p?.formerTeam ||
					p.formerTeam.name !== club ||
					(m[1] !== undefined && p.formerTeam.via !== "trade")
				) {
					add("former team", `${owner} and the ${club}`, sentence);
				}
			}
			m =
				/the (top|\w+) pick in the (\d+) draft|a second-round pick in the (\d+) draft/.exec(
					sentence,
				);
			if (m) {
				const d = p?.draft;
				const pickNo = m[1] === "top" ? 1 : wordToNumber(m[1] ?? "");
				const ok =
					m[1] !== undefined
						? d?.round === 1 && d.pick === pickNo && d.year === Number(m[2])
						: d?.round === 2 && d.year === Number(m[3]);
				if (!ok) {
					add("draft", `${owner} was not ${m[0]}`, sentence);
				}
			}
		}
	}

	// --- the offensive glass -----------------------------------------------
	{
		const sumOf = (t: RecapTeam, key: "orb") =>
			t.players.every((p) => p[key] !== undefined)
				? t.players.reduce((n, p) => n + (p[key] ?? 0), 0)
				: undefined;
		const pairOk = (key: "orb", a: number, b: number) => {
			const h = sumOf(home, key);
			const v = sumOf(away, key);
			return (h === a && v === b) || (h === b && v === a);
		};
		for (const sentence of splitSentences(recap)) {
			for (const m of sentence.matchAll(
				/(\d+) offensive rebounds to [^.]*?'s? (\d+)/g,
			)) {
				if (!pairOk("orb", Number(m[1]), Number(m[2]))) {
					add("offensive rebounds", `said ${m[1]} to ${m[2]}`, sentence);
				}
			}
		}
	}

	// --- the race and the trip ---------------------------------------------
	{
		const subjectOf = (sentence: string): RecapTeam | undefined => {
			let best = Infinity;
			let who: RecapTeam | undefined;
			for (const t of game.teams) {
				for (const label of [t.name, t.region]) {
					const at = label ? sentence.indexOf(label) : -1;
					if (at >= 0 && at < best) {
						best = at;
						who = t;
					}
				}
			}
			return who;
		};
		let last: RecapTeam | undefined;
		for (const sentence of splitSentences(recap)) {
			// "They opened a six-game trip." is the team the sentence before
			// it was about - whoever else it goes on to name ("They took both
			// meetings with the Clippers").
			const t = /^(?:They|It)\b/.test(sentence) ? last : subjectOf(sentence);
			last = t;
			const r = t?.race;
			const claim = (ok: boolean, kind: string) => {
				if (!ok) {
					add(kind, `not so for ${t?.name ?? "anyone"}`, sentence);
				}
			};
			if (/clinched a playoff spot/.test(sentence)) {
				claim(r?.clinched === "playoffs", "clinch");
			}
			if (/first team to clinch a playoff spot/.test(sentence)) {
				claim(!!r?.firstToClinch, "first to clinch");
			}
			if (/clinched the best record/.test(sentence)) {
				claim(r?.clinched === "top", "best record");
			}
			if (/clinched at least a play-in spot/.test(sentence)) {
				claim(r?.clinched === "playIn", "play-in");
			}
			if (/eliminated from playoff contention/.test(sentence)) {
				claim(!!r?.eliminated, "eliminated");
			}
			let m = /first team to (\d+) wins/.exec(sentence);
			if (m) {
				claim(r?.firstTo === Number(m[1]), "first to");
			}
			const st = t?.stretch;
			m = /opened an? (\w+)-game (trip|homestand)/.exec(sentence);
			if (m) {
				claim(
					!!st &&
						st.game === 1 &&
						st.games === wordToNumber(m[1]!) &&
						st.home === (m[2] === "homestand"),
					"trip",
				);
			}
			m = /finished (?:its|their) (\w+)-game (trip|homestand) (\d+)-(\d+)/.exec(
				sentence,
			);
			if (m) {
				claim(
					!!st &&
						st.game === st.games &&
						st.games === wordToNumber(m[1]!) &&
						st.home === (m[2] === "homestand") &&
						st.won === Number(m[3]) &&
						st.lost === Number(m[4]),
					"trip",
				);
			}
			m =
				/(?:is|are) (\d+)-(\d+) on (?:its|their) (\w+)-game (trip|homestand)/.exec(
					sentence,
				);
			if (m) {
				claim(
					!!st &&
						st.games === wordToNumber(m[3]!) &&
						st.home === (m[4] === "homestand") &&
						st.won === Number(m[1]) &&
						st.lost === Number(m[2]),
					"trip",
				);
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

// THE NIGHT, read back the same way. Each item of "Around the league" is
// held against its own game, as a recap of it under its final; the night's
// figures, streaks and records against the games and the standings they came
// from.
export const verifyNight = (
	written: string,
	games: RecapGame[],
	standings?: RecapDayStandings,
): RecapViolation[] => {
	const out: RecapViolation[] = [];
	const add = (kind: string, detail: string, sentence: string) => {
		out.push({ kind, detail, sentence });
	};
	const nickOf = (t: RecapTeam) => t.name || t.region;
	const headerOf = (g: RecapGame) => {
		const [home, away] = g.teams;
		const W = g.winnerTid === home.tid ? home : away;
		const L = W === home ? away : home;
		return `${nickOf(W)} ${W.pts}, ${nickOf(L)} ${L.pts}`;
	};
	const rows = new Map<
		string,
		{ won: number; lost: number; streak?: { won: boolean; count: number } }
	>();
	for (const c of standings?.confs ?? []) {
		for (const t of c.teams) {
			rows.set(t.name, t);
		}
	}
	const blocks = written.split("\n\n");
	for (const block of blocks) {
		const title = /^### (.+)/.exec(block)?.[1];
		const lines = block.split("\n").slice(title ? 1 : 0);
		if (title === "Around the league") {
			for (const line of lines) {
				const m = /^- \*\*(.+?):\*\* (.+)$/.exec(line);
				const game = m
					? games.find((g) => m[1]!.startsWith(headerOf(g)))
					: undefined;
				if (m && game) {
					for (const v of verifyRecap(`**${m[1]}**\n\n${m[2]}`, game)) {
						out.push(v);
					}
				}
			}
		} else if (title === "Streaks") {
			for (const line of lines) {
				const m = /^- \*\*(Winning|Losing):\*\* (.+)$/.exec(line);
				for (const part of m?.[2]!.split(", ") ?? []) {
					const pm = /^(.+) (\d+)$/.exec(part);
					const row = pm ? rows.get(pm[1]!) : undefined;
					if (
						pm &&
						(!row?.streak ||
							row.streak.count !== Number(pm[2]) ||
							row.streak.won !== (m![1] === "Winning"))
					) {
						add("streak", `said ${part}`, line);
					}
				}
			}
		} else if (title === "By the numbers") {
			const quarters = new Set<string>();
			const figures = new Set<string>();
			for (const g of games) {
				const [a, b] = g.teams;
				for (let q = 0; q < (a.ptsQtrs?.length ?? 0); q++) {
					quarters.add(`${a.ptsQtrs![q]}-${b.ptsQtrs?.[q]}`);
					quarters.add(`${b.ptsQtrs?.[q]}-${a.ptsQtrs![q]}`);
				}
				for (const r of g.runs?.runs ?? []) {
					quarters.add(`${r.pts}-${r.opp}`);
				}
				for (const t of g.teams) {
					figures.add(String(t.pts));
					for (const p of t.players) {
						figures.add(`${p.fg} of ${p.fga}`);
						figures.add(`${p.tp} of ${p.tpa}`);
						figures.add(`${p.ft} of ${p.fta}`);
						for (const n of [p.pts, p.reb, p.ast, p.blk, p.stl, p.tp]) {
							figures.add(String(n));
						}
					}
				}
				figures.add(String(g.flow?.leadChanges ?? -1));
			}
			for (const line of lines) {
				const fig = /^- \*\*(.+?):\*\*/.exec(line)?.[1];
				if (!fig) {
					continue;
				}
				const isPair = /^\d+-\d+$/.test(fig);
				if (
					isPair
						? !quarters.has(fig)
						: !figures.has(fig) && !/deficit/.test(line)
				) {
					add("figure", `no ${fig} in the night's games`, line);
				}
			}
		}
		// A record in parentheses after a club's name is its record.
		if (title === "The race" || title === "Worth watching") {
			for (const m of block.matchAll(
				/(?:the |\*\*|at )([A-Z][\w ]*?) \((\d+)-(\d+)\)/g,
			)) {
				const row = rows.get(m[1]!);
				if (row && (row.won !== Number(m[2]) || row.lost !== Number(m[3]))) {
					add(
						"record",
						`said ${m[1]} ${m[2]}-${m[3]}, real ${row.won}-${row.lost}`,
						block,
					);
				}
			}
		}
	}
	return out;
};
