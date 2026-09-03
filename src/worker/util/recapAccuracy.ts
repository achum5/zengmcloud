import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";

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

const splitSentences = (text: string): string[] =>
	text
		.replaceAll("**", "")
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
	/quarter|after one|at the break|halftime|half|first|second|third|fourth|\brun\b|stretch|outscor|closed|opened|took over|settled it|broke it open|spurt|glass|boards|rebound|free throw|from the line|threes|from deep|assists|pushed|improved|moved|fell to|dropped to|climbed|meeting|season series|this season|at home|on the road|away from home|own building|bench|reserves/i;

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

	for (const sentence of splitSentences(recap)) {
		// --- the final score -------------------------------------------------
		for (const m of sentence.matchAll(/\b(\d{2,3})-(\d{2,3})\b/g)) {
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
			if (!finalPts.has(a) || !finalPts.has(b) || a === b) {
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
				const after = sentence.slice(m.index + m[0].length);
				const fromMatch = /\b(?:for|from)\s+/.exec(after);
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
		for (const m of sentence.matchAll(
			/(first|second|third|fourth) quarter[^.]{0,40}?(\d+)-(\d+)/g,
		)) {
			const idx = { first: 0, second: 1, third: 2, fourth: 3 }[
				m[1] as "first" | "second" | "third" | "fourth"
			];
			const hq = home.ptsQtrs ?? [];
			const aq = away.ptsQtrs ?? [];
			if (idx < hq.length && idx < aq.length) {
				const said = new Set([Number(m[2]), Number(m[3])]);
				if (!said.has(hq[idx]!) || !said.has(aq[idx]!)) {
					add(
						"quarter score",
						`${m[1]} said ${m[2]}-${m[3]}, real ${hq[idx]}-${aq[idx]}`,
						sentence,
					);
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
	if (
		/never trailed|wire to wire|led from|in front from the opening tip|start to finish/i.test(
			text,
		) &&
		/comeback|erased|rallied|stormed back|came from \d|deficit|down \d+ (?:at|after)/i.test(
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
