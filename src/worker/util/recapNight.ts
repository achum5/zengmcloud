// THE NIGHT'S CLOSING SECTIONS.
//
// What a league's nightly roundup carries under its games, each only on a
// night that has one: By the numbers (the night's figures, a run, a quarter,
// a shooting line, each with what it was), Streaks (who is winning and who
// is losing, league-wide), The race (clinches, the top of each conference,
// the cut lines, a magic number late in the season, who crossed a line),
// Milestones and firsts (career marks, a first triple-double, a career
// high, a debut), Moves (a traded or signed man's first game with his new
// club, or against his old one) and Worth watching (the next day's best
// game, with why).
//
// Everything here is read off the night's games and the standings; nothing
// is said that the box scores and the schedule do not hold. recapDay.ts
// writes the night's stories and hands these what it has said, so no
// section says a thing a paragraph above it already told.

import type {
	RecapDayStandings,
	RecapGame,
	RecapNextSlate,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import {
	careerHighOf,
	draftTag,
	firstOf,
	isDebut,
	moveNote,
	raceNote,
} from "./recapPeople.ts";
import type { RunFact } from "./recapStory.ts";
import {
	cap,
	gbText,
	nick,
	numWord,
	ordinal,
	pick,
	poss,
	statPhrase,
	theNick,
} from "./recapText.ts";

type Rng = () => number;

// What a section needs of a game's story (recapDay's Story has it all).
export type NightStory = {
	game: RecapGame;
	W: RecapTeam;
	L: RecapTeam;
	shape: { margin: number; ot: number; regPeriods: number };
	closer?: RunFact;
	breaker?: RunFact;
	comeback: number;
};

const section = (title: string, lines: string[]): string =>
	[`### ${title}`, ...lines].join("\n");

const fmtNum = (n: number) => n.toLocaleString("en-US");

const STAT_WORD = {
	pts: "points",
	reb: "rebounds",
	ast: "assists",
	tp: "3-pointers",
} as const;

// A club by its city where the city is its own - "Denver's closing run at
// New York" - and by its nickname where two clubs share one.
const placesOf = (
	standings: RecapDayStandings | undefined,
	games: RecapGame[],
): ((t: RecapTeam) => string) => {
	const count = new Map<string, number>();
	const seen = new Set<number>();
	const add = (tid: number | undefined, region: string) => {
		if (tid !== undefined && seen.has(tid)) {
			return;
		}
		if (tid !== undefined) {
			seen.add(tid);
		}
		count.set(region, (count.get(region) ?? 0) + 1);
	};
	for (const c of standings?.confs ?? []) {
		for (const t of c.teams) {
			add(t.tid, t.region);
		}
	}
	for (const g of games) {
		for (const t of g.teams) {
			add(t.tid, t.region);
		}
	}
	return (t) =>
		standings && t.region && count.get(t.region) === 1 ? t.region : theNick(t);
};

// The periods a figure covers, for one club: a run across the third and
// fourth quarters is the fourth quarter's story too.
const periodsOf = (tid: number, from: number, to: number): string[] =>
	Array.from(
		{ length: Math.max(1, to - from + 1) },
		(_, i) => `${tid}:${from + i}`,
	);

const otherOf = (game: RecapGame, t: RecapTeam): RecapTeam =>
	game.teams[0] === t ? game.teams[1] : game.teams[0];

// ---------------------------------------------------------- BY THE NUMBERS

type Figure = {
	fig: string;
	text: string;
	weight: number;
	gid: number;
	// What kind of figure (a run, a quarter, a man's line), and the team and
	// period it belongs to: a quarter and the run inside it are one story.
	kind: string;
	spots?: string[];
};

// The night's figures: 3 to 5 of them, at most two from a game, none that a
// paragraph above already gave.
export const byTheNumbers = (
	stories: NightStory[],
	said: string,
	standings: RecapDayStandings | undefined,
): string | undefined => {
	const place = placesOf(
		standings,
		stories.map((s) => s.game),
	);
	const figs: Figure[] = [];
	for (const s of stories) {
		const g = s.game;
		if (g.allStar) {
			continue;
		}
		const vs = (t: RecapTeam): string => {
			const opp = place(otherOf(g, t));
			return g.neutral || g.teams[0] === t ? `against ${opp}` : `at ${opp}`;
		};
		const add = (f: Omit<Figure, "gid">, told: boolean) => {
			if (!told) {
				figs.push({ ...f, gid: g.gid });
			}
		};
		const W = s.W;
		// The run that ended it, and the one that broke it open.
		const runText = (r: RunFact, closing: boolean): string => {
			const when =
				closing || r.period !== r.endPeriod
					? ""
					: r.period > s.shape.regPeriods
						? "overtime "
						: s.shape.regPeriods === 4
							? `${ordinal(r.period)}-quarter `
							: "";
			return `${poss(place(W))} ${closing ? "closing " : when}run ${vs(W)}`;
		};
		const c = s.closer;
		if (c && c.mine && c.pts - c.opp >= 12 && (c.closed || c.ended)) {
			add(
				{
					fig: `${c.pts}-${c.opp}`,
					text: runText(c, true),
					weight: c.pts - c.opp + (s.shape.margin <= 10 ? 6 : 0),
					kind: "run",
					spots: periodsOf(W.tid, c.period, c.endPeriod),
				},
				said.includes(`${c.pts}-${c.opp}`),
			);
		}
		const b = s.breaker;
		if (b && b !== c && b.mine && b.pts - b.opp >= 15) {
			add(
				{
					fig: `${b.pts}-${b.opp}`,
					text: runText(b, false),
					weight: b.pts - b.opp,
					kind: "run",
					spots: periodsOf(W.tid, b.period, b.endPeriod),
				},
				said.includes(`${b.pts}-${b.opp}`),
			);
		}
		// A quarter one side owned.
		if (s.shape.regPeriods === 4) {
			for (const t of g.teams) {
				const opp = otherOf(g, t);
				for (let q = 0; q < 4; q++) {
					const a = t.ptsQtrs?.[q];
					const o = opp.ptsQtrs?.[q];
					if (a === undefined || o === undefined) {
						continue;
					}
					if (a - o >= 18 || (a >= 44 && a - o >= 10)) {
						add(
							{
								fig: `${a}-${o}`,
								text: `${poss(place(t))} ${ordinal(q + 1)} quarter ${vs(t)}`,
								weight: a - o + (a >= 44 ? 6 : 0),
								kind: "quarter",
								spots: periodsOf(t.tid, q + 1, q + 1),
							},
							said.includes(`${a}-${o}`),
						);
					}
				}
			}
		}
		// Lead changes, a deficit erased.
		const changes = g.flow?.leadChanges ?? 0;
		if (changes >= 16) {
			add(
				{
					fig: `${changes}`,
					text: `lead changes in ${poss(place(W))} win ${vs(W)}`,
					weight: changes - 4,
					kind: "changes",
				},
				new RegExp(String.raw`\b${changes} (?:times|lead changes)`).test(said),
			);
		}
		if (s.comeback >= 18) {
			add(
				{
					fig: `${s.comeback}`,
					text: `the deficit ${place(W)} overcame ${vs(W)}`,
					weight: s.comeback,
					kind: "comeback",
				},
				new RegExp(String.raw`\b${s.comeback}(?: down|-point| points)`).test(
					said,
				),
			);
		}
		if (
			W.seasonHighs?.leaguePts &&
			W.seasonHighs.priorGames >= 15 &&
			W.pts >= 130
		) {
			add(
				{
					fig: `${W.pts}`,
					text: `points for ${place(W)} ${vs(W)}, the most by any team this season`,
					weight: 20,
					kind: "team",
				},
				/most (?:by|any team)/.test(said),
			);
		}
		// A man's line.
		for (const t of g.teams) {
			for (const p of t.players) {
				const name = p.name;
				const nameTold = said.includes(name);
				const counted = (n: number, unit: string) =>
					nameTold && new RegExp(String.raw`\b${n} ${unit}`).test(said);
				if (p.fga >= 10 && p.fg / p.fga >= 0.85) {
					add(
						{
							fig: `${p.fg} of ${p.fga}`,
							text: `${poss(name)} shooting for ${place(t)} ${vs(t)}`,
							weight: p.fg * 1.5 - (p.fga - p.fg) * 3,
							kind: "shooting",
						},
						nameTold && said.includes(`${p.fg} of ${p.fga}`),
					);
				}
				if (p.tp >= 7 && p.tpa - p.tp <= 2) {
					add(
						{
							fig: `${p.tp} of ${p.tpa}`,
							text: `${poss(name)} 3-point shooting for ${place(t)} ${vs(t)}`,
							weight: p.tp * 2,
							kind: "shooting",
						},
						counted(p.tp, "(?:3-pointers|3s|threes)"),
					);
				} else if (p.tp >= 9) {
					add(
						{
							fig: `${p.tp}`,
							text: `3-pointers by ${name} for ${place(t)} ${vs(t)}`,
							weight: p.tp * 2,
							kind: "shooting",
						},
						counted(p.tp, "(?:3-pointers|3s|threes)"),
					);
				}
				if (p.fta >= 15 && p.ft === p.fta) {
					add(
						{
							fig: `${p.ft} of ${p.fta}`,
							text: `${poss(name)} free throws for ${place(t)} ${vs(t)}`,
							weight: p.fta,
							kind: "shooting",
						},
						nameTold && said.includes(`${p.ft} of ${p.fta}`),
					);
				}
				const big: [number, number, string, number][] = [
					[p.pts, 45, "points", 0.6],
					[p.reb, 20, "rebounds", 1],
					[p.ast, 16, "assists", 1],
					[p.blk, 8, "blocks", 2.5],
					[p.stl, 7, "steals", 2.5],
				];
				for (const [n, bar, unit, w] of big) {
					if (n >= bar) {
						add(
							{
								fig: `${n}`,
								text: `${unit} for ${name} ${vs(t)}`,
								weight: n * w,
								kind: "line",
							},
							counted(n, unit),
						);
					}
				}
			}
		}
	}
	figs.sort((a, b) => b.weight - a.weight);
	// At most two from a game and two of a kind - five runs and quarters is
	// one figure five times - and never a quarter beside the run inside it.
	const chosen: Figure[] = [];
	const count = (key: (f: Figure) => unknown, f: Figure) =>
		chosen.filter((x) => key(x) === key(f)).length;
	for (const f of figs) {
		if (
			count((x) => x.gid, f) >= 2 ||
			count((x) => x.kind, f) >= 2 ||
			chosen.some((x) => x.spots?.some((p) => f.spots?.includes(p))) ||
			chosen.some((x) => x.fig === f.fig)
		) {
			continue;
		}
		chosen.push(f);
		if (chosen.length === 5) {
			break;
		}
	}
	if (chosen.length < 3) {
		return undefined;
	}
	return section(
		"By the numbers",
		chosen.map((f) => `- **${f.fig}:** ${cap(f.text)}`),
	);
};

// ---------------------------------------------------------------- STREAKS

// Who is winning and who is losing, league-wide, three games or more: a
// night with a run of four or more somewhere.
export const streaksSection = (
	standings: RecapDayStandings | undefined,
): string | undefined => {
	if (!standings) {
		return undefined;
	}
	type Row = { name: string; count: number };
	const winning: Row[] = [];
	const losing: Row[] = [];
	for (const c of standings.confs) {
		for (const t of c.teams) {
			const s = t.streak;
			if (!s || s.count < 3) {
				continue;
			}
			(s.won ? winning : losing).push({ name: t.name, count: s.count });
		}
	}
	const order = (rows: Row[]) =>
		rows
			.sort((a, b) => b.count - a.count || a.name.localeCompare(b.name))
			.slice(0, 5);
	const top = Math.max(0, ...[...winning, ...losing].map((r) => r.count));
	if (top < 4) {
		return undefined;
	}
	const line = (label: string, rows: Row[]) =>
		rows.length > 0
			? [
					`- **${label}:** ${order(rows)
						.map((r) => `${r.name} ${r.count}`)
						.join(", ")}`,
				]
			: [];
	return section("Streaks", [
		...line("Winning", winning),
		...line("Losing", losing),
	]);
};

// ---------------------------------------------------------------- THE RACE

// Games enough for the top of a conference to mean anything, for a meeting
// of two of its best, and for the cut line.
const LEAD_GAMES = 10;
const RACE_GAMES = 15;
const CUT_GAMES = 30;

const confShort = (name: string): string => {
	const m = /^(east|west|north|south)(?:ern)? conference$/i.exec(name.trim());
	return m
		? `the ${m[1]![0]!.toUpperCase()}${m[1]!.slice(1).toLowerCase()}`
		: `the ${name}`;
};

// The standings after the night: what it settled (a clinch, an
// elimination), the top of each conference with a magic number late on,
// who crossed a line tonight, and how close the cut lines are.
export const raceSection = (
	games: RecapGame[],
	standings: RecapDayStandings | undefined,
	said: string,
	rng: Rng,
): string | undefined => {
	if (!standings || games.some((g) => g.playoffs)) {
		return undefined;
	}
	// Each line with its weight, 0 first: what the night settled, who
	// crossed a line, the top and the cut lines, then magic numbers.
	const lines: { text: string; pri: number }[] = [];
	const add = (pri: number, text: string) => {
		lines.push({ text, pri });
	};
	const named = new Set<number>();
	// What the night settled, for the clubs a paragraph above has not.
	for (const g of games) {
		for (const t of g.teams) {
			const won = g.winnerTid === t.tid;
			const note = raceNote(t, { S: cap(theNick(t)), one: false }, won);
			if (!note) {
				continue;
			}
			const told = new RegExp(
				String.raw`${nick(t)}[^.]*(?:clinch|eliminated|first team to)`,
			).test(said);
			if (!told) {
				add(0, note);
			}
			named.add(t.tid);
		}
	}
	const rules = standings.race;
	const numGames = standings.numGames;
	const played = new Set(games.flatMap((g) => g.teams.map((t) => t.tid)));
	const winners = new Set(games.map((g) => g.winnerTid));
	const rankBefore = new Map<number, number>();
	for (const g of games) {
		for (const t of g.teams) {
			if (t.standing?.rankBefore !== undefined) {
				rankBefore.set(t.tid, t.standing.rankBefore);
			}
		}
	}
	for (const conf of standings.confs) {
		const [first, second] = conf.teams;
		if (!first) {
			continue;
		}
		const where = confShort(conf.name);
		const gp = first.won + first.lost;
		const perfect = first.lost === 0 && first.won >= 5;
		// The top of a conference, ten games in (five without a loss), on a
		// night that changed it - a new leader, the leader beaten, a perfect
		// record at every fifth win - or when it is within a game, or late
		// enough for a magic number. Not every night: "the Nets lead the
		// East" six evenings running is the same line six times.
		const tid = first.tid;
		const before =
			first.rankBefore ?? (tid !== undefined ? rankBefore.get(tid) : undefined);
		const changed =
			tid === undefined ||
			(played.has(tid) &&
				((before ?? 1) > 1 ||
					!winners.has(tid) ||
					(perfect && first.won % 5 === 0)));
		const magic =
			second && numGames !== undefined
				? numGames + 1 - first.won - second.lost
				: undefined;
		// Only on a night that moved it: the leader won, or the runner-up
		// lost.
		const moved =
			(tid !== undefined && winners.has(tid)) ||
			(second?.tid !== undefined &&
				played.has(second.tid) &&
				!winners.has(second.tid));
		const magicNews =
			magic !== undefined &&
			magic >= 1 &&
			magic <= 10 &&
			moved &&
			!new RegExp(String.raw`${first.name}[^.]*clinch`).test(said);
		const tight =
			second !== undefined &&
			second.gb <= 1 &&
			[tid, second.tid].some((x) => x !== undefined && played.has(x));
		const told = new RegExp(
			String.raw`${first.name}[^.]*(?:first place|unbeaten|perfect)`,
		).test(said);
		if (
			(gp >= LEAD_GAMES || perfect) &&
			!told &&
			(changed || tight || magicNews)
		) {
			const who = `the ${first.name} (${first.won}-${first.lost})`;
			if (perfect) {
				add(
					2,
					`The ${first.name} are still perfect at ${first.won}-0 atop ${where}.`,
				);
			} else if (second && second.gb === 0) {
				add(
					2,
					`${cap(who)} and the ${second.name} (${second.won}-${second.lost}) are tied for first in ${where}.`,
				);
			} else if (second) {
				add(
					2,
					pick(
						rng,
						[
							`${cap(who)} lead ${where} by ${gbText(second.gb)} over the ${second.name}.`,
							`${cap(who)} are ${gbText(second.gb)} ahead of the ${second.name} in ${where}.`,
						],
						"night:race:lead",
					),
				);
			} else {
				add(2, `${cap(who)} lead ${where}.`);
			}
			// The magic number for first: wins by the leader or losses by the
			// runner-up, a tie going to the runner-up.
			if (magicNews) {
				add(
					3,
					pick(
						rng,
						[
							`${cap(poss(`the ${first.name}`))} magic number for first in ${where} is ${magic}.`,
							magic === 1
								? `One more ${first.name} win or ${second!.name} loss clinches first in ${where} for the ${first.name}.`
								: `Any combination of ${numWord(magic!)} ${first.name} wins or ${second!.name} losses clinches first in ${where} for the ${first.name}.`,
						],
						"night:race:magic",
					),
				);
			}
		}
		// Who crossed a line tonight, the playoff line or the play-in's.
		if (rules && rules.byConf && first.won + first.lost >= CUT_GAMES) {
			const playIn = rules.reach > rules.direct;
			const zone = (rank: number) =>
				rank <= rules.direct ? 0 : rank <= rules.reach ? 1 : 2;
			let crossed = false;
			for (const t of conf.teams) {
				const before = t.rankBefore;
				if (
					t.tid === undefined ||
					before === undefined ||
					!played.has(t.tid) ||
					named.has(t.tid) ||
					t.rank === 1 ||
					zone(t.rank) === zone(before) ||
					(!playIn && zone(t.rank) + zone(before) === 3)
				) {
					continue;
				}
				const from = `from ${ordinal(before)} to ${ordinal(t.rank)} in ${where}`;
				if (t.rank < before) {
					const into =
						zone(t.rank) === 1
							? "into a play-in spot"
							: playIn
								? `into the top ${numWord(rules.direct)}`
								: "into a playoff spot";
					add(1, `The ${t.name} moved ${from}, ${into}.`);
				} else {
					const out =
						zone(t.rank) === 1
							? "down into the play-in"
							: playIn
								? "out of the play-in spots"
								: "out of the playoff spots";
					add(1, `The ${t.name} slipped ${from}, ${out}.`);
				}
				named.add(t.tid);
				crossed = true;
			}
			// The cut lines, when they are close, on a night that touched
			// them: one of the two clubs played, or a club crossed a line.
			const cut = (spot: number, what: string) => {
				const inside = conf.teams.find((t) => t.rank === spot);
				const outside = conf.teams.find((t) => t.rank === spot + 1);
				if (!inside || !outside) {
					return;
				}
				const touched =
					crossed ||
					[inside.tid, outside.tid].some(
						(x) => x !== undefined && played.has(x),
					);
				if (!touched) {
					return;
				}
				const gap = Math.round((outside.gb - inside.gb) * 2) / 2;
				if (gap <= 0) {
					add(
						2,
						pick(
							rng,
							[
								`The ${inside.name} and the ${outside.name} are tied for ${what} in ${where}.`,
								`The ${inside.name} and the ${outside.name} are level for ${what} in ${where}.`,
							],
							"night:race:cutTied",
						),
					);
				} else if (gap <= 2) {
					add(
						2,
						pick(
							rng,
							[
								`The ${inside.name} hold ${what} in ${where} by ${gbText(gap)} over the ${outside.name}.`,
								`The ${inside.name} are ${gbText(gap)} ahead of the ${outside.name} for ${what} in ${where}.`,
								`The ${outside.name} are ${gbText(gap)} behind the ${inside.name} for ${what} in ${where}.`,
							],
							"night:race:cut",
						),
					);
				}
			};
			if (rules.reach > rules.direct) {
				cut(rules.reach, "the last play-in spot");
			} else {
				cut(rules.direct, "the last playoff spot");
			}
		}
	}
	if (lines.length === 0) {
		return undefined;
	}
	// Six at most, dropping the least of them, latest first.
	while (lines.length > 6) {
		const least = Math.max(...lines.map((l) => l.pri));
		lines.splice(
			lines.findLastIndex((l) => l.pri === least),
			1,
		);
	}
	return section(
		"The race",
		lines.map((l) => `- ${l.text}`),
	);
};

// ---------------------------------------------------- MILESTONES AND FIRSTS

type Mark = { text: string; weight: number };

const CAREER_TOLD = /career|debut|first game|for the season/;

// A round number a league-wide roundup carries: 5,000 career points but not
// 3,000, 2,000 points in a season but not 1,500. Every night somebody passes
// a thousand of something; the game's own recap has those.
const notableMark = (m: NonNullable<RecapPlayer["milestone"]>): boolean =>
	m.scope === "career"
		? m.stat === "pts"
			? m.mark % 5000 === 0 || m.mark >= 15000
			: m.stat === "tp"
				? m.mark % 1000 === 0 || m.mark >= 2000
				: m.mark % 2000 === 0 || m.mark >= 8000
		: m.stat === "pts"
			? m.mark >= 2000
			: m.stat === "tp"
				? m.mark >= 300
				: m.mark >= 1000;

// Career marks, firsts, career highs and debuts across the night, for the
// men a paragraph above has not given one.
export const milestonesSection = (
	games: RecapGame[],
	said: string,
): string | undefined => {
	const sentences = said.split(/(?<=[!.?])\s+/);
	const toldOf = (p: RecapPlayer) =>
		CAREER_TOLD.test(sentences.filter((s) => s.includes(p.name)).join(" "));
	const marks: Mark[] = [];
	for (const g of games) {
		if (g.allStar) {
			continue;
		}
		for (const t of g.teams) {
			for (const p of t.players) {
				if (toldOf(p)) {
					continue;
				}
				const who = said.includes(p.name)
					? p.name
					: `${poss(theNick(t))} ${p.name}`;
				const first = firstOf(p);
				const high = careerHighOf(p);
				const m = p.milestone;
				if (first === "triple-double") {
					marks.push({
						text: `${who} had ${statPhrase(p)} for his first career triple-double.`,
						weight: 50,
					});
				} else if (first === "40-point game" || first === "30-point game") {
					marks.push({
						text: `${who} scored ${p.pts}, the first ${first} of his career.`,
						weight: first === "40-point game" ? 45 : 30,
					});
				} else if (high?.key === "pts" && p.pts >= 20) {
					marks.push({
						text: `${who} scored a career-high ${p.pts} points.`,
						weight: 20 + p.pts / 2,
					});
				} else if (high && high.key !== "pts") {
					marks.push({
						text: `${who} had a career-high ${high.n} ${STAT_WORD[high.key]}.`,
						weight: 18,
					});
				} else if (
					isDebut(p) &&
					((p.draft?.round === 1 && p.draft.pick <= 10) || p.pts >= 12)
				) {
					const tag = draftTag(p);
					marks.push({
						text: `${who}${tag ? `, ${tag},` : ""} had ${statPhrase(p)} in his debut.`,
						weight:
							15 +
							(p.draft?.round === 1 ? 11 - Math.min(10, p.draft.pick) : 0) +
							p.pts / 4,
					});
				} else if (first === "double-double") {
					marks.push({
						text: `${who} had ${statPhrase(p)} for his first career double-double.`,
						weight: 8,
					});
				}
				if (m && notableMark(m)) {
					const word = STAT_WORD[m.stat];
					marks.push(
						m.scope === "career"
							? {
									text: `${who} went past ${fmtNum(m.mark)} career ${word}.`,
									weight: 25 + m.mark / 1000,
								}
							: {
									text: `${who} went past ${fmtNum(m.mark)} ${word} for the season.`,
									weight: 5 + m.mark / 1000,
								},
					);
				}
			}
		}
	}
	if (marks.length === 0) {
		return undefined;
	}
	marks.sort((a, b) => b.weight - a.weight);
	return section(
		"Milestones and firsts",
		marks.slice(0, 5).map((m) => `- ${cap(m.text)}`),
	);
};

// ---------------------------------------------------------------- MOVES

// A man's first game for the club that traded for or signed him, or his
// first against the club that traded him away.
export const movesSection = (
	games: RecapGame[],
	said: string,
	rng: Rng,
): string | undefined => {
	const sentences = said.split(/(?<=[!.?])\s+/);
	const out: { text: string; weight: number }[] = [];
	for (const g of games) {
		if (g.allStar) {
			continue;
		}
		for (const t of g.teams) {
			for (const p of t.players) {
				if (isDebut(p) || (!p.newTeam && p.formerTeam?.via !== "trade")) {
					continue;
				}
				if (
					/debut|first game|since they traded|since leaving/.test(
						sentences.filter((s) => s.includes(p.name)).join(" "),
					)
				) {
					continue;
				}
				const note = moveNote(p, t, rng, false);
				if (note) {
					out.push({
						text: note,
						weight:
							p.pts +
							(p.newTeam?.via === "trade" || p.formerTeam ? 10 : 0) +
							(p.newTeam?.midSeason ? 5 : 0),
					});
				}
			}
		}
	}
	if (out.length === 0) {
		return undefined;
	}
	out.sort((a, b) => b.weight - a.weight);
	return section(
		"Moves",
		out.slice(0, 4).map((x) => `- ${x.text}`),
	);
};

// ---------------------------------------------------------- WORTH WATCHING

type Row = RecapDayStandings["confs"][number]["teams"][number] & {
	conf: string;
	confRank: number;
};

// The next day's best game and why: first place on the line, two of a
// conference's best, a cut-line meeting, a streak - and failing all of
// those, the two best records.
export const worthWatching = (
	next: RecapNextSlate | undefined,
	standings: RecapDayStandings | undefined,
	rng: Rng,
): string | undefined => {
	if (!next || !standings || next.games.length === 0) {
		return undefined;
	}
	const rows = new Map<number, Row>();
	for (const c of standings.confs) {
		for (const t of c.teams) {
			if (t.tid !== undefined) {
				rows.set(t.tid, { ...t, conf: c.name, confRank: t.rank });
			}
		}
	}
	// How far each conference's leader is clear of second.
	const clear = new Map<string, number>();
	for (const c of standings.confs) {
		const second = c.teams.find((t) => t.rank === 2);
		if (second) {
			clear.set(c.name, Math.round(second.gb * 2) / 2);
		}
	}
	const rules = standings.race;
	const pct = (r: Row) => (r.won + r.lost > 0 ? r.won / (r.won + r.lost) : 0);
	const rec = (r: Row) => `${r.won}-${r.lost}`;
	type Option = { text: string; weight: number; head: string };
	const options: Option[] = [];
	for (const x of next.games) {
		const h = rows.get(x.homeTid);
		const a = rows.get(x.awayTid);
		if (!h || !a) {
			continue;
		}
		const gp = Math.min(h.won + h.lost, a.won + a.lost);
		if (gp < 5) {
			continue;
		}
		const head = `${a.name} (${rec(a)}) at ${h.name} (${rec(h)})`;
		const why: string[] = [];
		let weight = pct(h) + pct(a);
		const same = h.conf === a.conf;
		const where = confShort(h.conf);
		const [hi, lo] = h.rank <= a.rank ? [h, a] : [a, h];
		const gap = Math.abs(hi.gb - lo.gb);
		if (same && gp >= RACE_GAMES && hi.rank === 1 && lo.rank === 2) {
			weight += 1.5;
			why.push(
				gap === 0
					? `They are tied for first in ${where}.`
					: `First place in ${where} is on the line: the ${hi.name} lead by ${gbText(gap)}.`,
			);
		} else if (same && gp >= RACE_GAMES && lo.rank <= 3) {
			weight += 0.8;
			const place = ["first", "second", "third"];
			why.push(
				pick(
					rng,
					[
						`Two of the top three in ${where} meet.`,
						`The ${place[hi.rank - 1]}- and ${place[lo.rank - 1]}-place teams in ${where} meet.`,
					],
					"night:watch:top3",
				),
			);
		} else if (same && rules?.byConf && gp >= CUT_GAMES && gap <= 1.5) {
			const line = [rules.direct, rules.reach].find(
				(l) => hi.rank === l && lo.rank === l + 1,
			);
			if (line !== undefined) {
				weight += 0.7;
				const spot =
					line === rules.reach && rules.reach > rules.direct
						? "the last play-in spot"
						: rules.reach > rules.direct
							? "the last guaranteed playoff spot"
							: "the last playoff spot";
				why.push(
					gap === 0
						? `They are tied for ${spot} in ${where}.`
						: `The ${hi.name} hold ${spot} in ${where} by ${gbText(gap)} over the ${lo.name}.`,
				);
			}
		}
		// A conference's leader, from the other conference or below the top
		// three of its own.
		const leaders =
			why.length === 0 && gp >= LEAD_GAMES
				? [a, h].filter((t) => t.rank === 1)
				: [];
		// A run worth seeing: a winning streak, or a long skid.
		const streaky = [h, a]
			.filter((t) =>
				t.streak?.won ? t.streak.count >= 4 : (t.streak?.count ?? 0) >= 8,
			)
			.sort((p, q) => q.streak!.count - p.streak!.count)[0];
		const run = (t: Row) =>
			`${t.streak!.won ? "won" : "lost"} ${numWord(t.streak!.count)} straight`;
		if (leaders.length === 2) {
			weight += 1.2;
			why.push(
				`The ${confShort(a.conf).replace(/^the /, "")} and ${confShort(h.conf).replace(/^the /, "")} leaders meet.`,
			);
		} else if (leaders.length === 1) {
			const t = leaders[0]!;
			weight += 0.4;
			const by = clear.get(t.conf);
			const lead =
				by === undefined
					? `lead ${confShort(t.conf)}`
					: by === 0
						? `are tied for first in ${confShort(t.conf)}`
						: pick(
								rng,
								[
									`lead ${confShort(t.conf)} by ${gbText(by)}`,
									`are ${gbText(by)} clear in ${confShort(t.conf)}`,
									`sit ${gbText(by)} up in ${confShort(t.conf)}`,
								],
								"night:watch:leader",
							);
			why.push(
				streaky === t
					? `The ${t.name} ${lead} and have ${run(t)}.`
					: `The ${t.name} ${lead}.`,
			);
		}
		if (streaky && !(leaders.length === 1 && leaders[0] === streaky)) {
			weight += streaky.streak!.won ? 0.2 + streaky.streak!.count / 40 : 0.05;
			why.push(`The ${streaky.name} have ${run(streaky)}.`);
		}
		options.push({ head, text: why.join(" "), weight });
	}
	if (options.length === 0) {
		return undefined;
	}
	options.sort((p, q) => q.weight - p.weight);
	const best = options[0]!;
	return section("Worth watching", [
		best.text ? `- **${best.head}:** ${best.text}` : `- **${best.head}**`,
	]);
};
