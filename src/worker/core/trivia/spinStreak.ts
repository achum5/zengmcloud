import { PHASE } from "../../../common/constants.ts";
import { helpers as commonHelpers } from "../../../common/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import getPlayoffsByConf from "../season/getPlayoffsByConf.ts";
import { mergedSeasons } from "./criteria.ts";
import { getTriviaPool, type TriviaPlayer, type TriviaPool } from "./pool.ts";
import type { TeamSeason } from "../../../common/types.ts";

// Spin Streak: three reels land on a season, a player from that season, and a
// category ("Team", "Alma Mater", "Jersey Number"...). Pick the right one of
// three answers to extend the streak; one miss ends the run. The worker builds
// one question at a time so every answer comes straight from the league's own
// history, with wrong answers chosen to be plausible rather than random.

export type SpinDifficulty = "easy" | "normal" | "hard";

export type SpinTeam = {
	tid: number;
	abbrev: string;
	region: string;
	name: string;
	colors: [string, string, string];
	imgURL?: string;
};

export type SpinOption = {
	id: string;
	kind: "team" | "flag" | "number" | "text" | "college" | "player";
	label: string;
	sub?: string;
	team?: SpinTeam;
	country?: string;
};

export type SpinQuestion = {
	id: string;
	season: number;
	pid: number;
	firstName: string;
	lastName: string;
	category: string;
	categoryLabel: string;
	options: SpinOption[];
	answer: number;
	// What the reels flick past before they land.
	decoys: {
		seasons: number[];
		players: { firstName: string; lastName: string }[];
		categories: string[];
	};
};

type SeasonLine =
	ReturnType<typeof mergedSeasons> extends Map<number, infer V> ? V : never;

type Ctx = {
	p: TriviaPlayer;
	season: number;
	line: SeasonLine;
	// The stint with the most games that season - "his team" in a traded year.
	tid: number;
	jerseyNumber: string | undefined;
	pos: string;
	pool: TriviaPool;
	diff: SpinDifficulty;
	streak: number;
};

type Built = { options: SpinOption[]; answer: number } | undefined;

type CategoryDef = {
	key: string;
	label: string;
	weight: number;
	build: (ctx: Ctx) => Built | Promise<Built>;
};

// --- Small helpers -----------------------------------------------------------

const random = <T>(arr: readonly T[]): T =>
	arr[Math.floor(Math.random() * arr.length)]!;

const shuffle = <T>(arr: T[]): T[] => {
	for (let i = arr.length - 1; i > 0; i--) {
		const j = Math.floor(Math.random() * (i + 1));
		[arr[i], arr[j]] = [arr[j]!, arr[i]!];
	}
	return arr;
};

// Weighted pick from [item, weight] pairs.
const weighted = <T>(pairs: [T, number][]): T | undefined => {
	let total = 0;
	for (const [, w] of pairs) {
		total += Math.max(0, w);
	}
	if (total <= 0) {
		return pairs.length > 0 ? random(pairs)[0] : undefined;
	}
	let r = Math.random() * total;
	for (const [item, w] of pairs) {
		r -= Math.max(0, w);
		if (r <= 0) {
			return item;
		}
	}
	return pairs.at(-1)?.[0];
};

// Answer + two distractors, shuffled.
const finish = (correct: SpinOption, wrong: SpinOption[]): Built => {
	if (wrong.length < 2) {
		return undefined;
	}
	const options = shuffle([correct, ...wrong.slice(0, 2)]);
	return { options, answer: options.indexOf(correct) };
};

// How tight wrong answers get: forgiving on Easy, close calls on Hard, and a
// little tighter the longer the streak runs.
const tightness = (ctx: Ctx) => {
	const base = ctx.diff === "easy" ? 1 : ctx.diff === "normal" ? 0.65 : 0.4;
	return base * Math.max(0.6, 1 - ctx.streak * 0.02);
};

// Two wrong numbers around the right one. The answer lands low, middle or high
// at random, so "pick the middle one" is never a strategy.
const numericOptions = (
	ctx: Ctx,
	value: number,
	{
		gap,
		min = 0,
		max = Infinity,
		decimals = 0,
		fmt,
		kind = "number",
	}: {
		gap: number;
		min?: number;
		max?: number;
		decimals?: number;
		fmt?: (v: number) => string;
		kind?: SpinOption["kind"];
	},
): Built => {
	const step = 10 ** -decimals;
	const g2 = Math.max(step, gap * tightness(ctx));
	const round = (v: number) => Math.round(v / step) * step;
	const label = fmt ?? ((v: number) => v.toFixed(decimals));
	const patterns = shuffle([
		[-2, -1],
		[-1, 1],
		[1, 2],
	]);
	for (const pattern of patterns) {
		const vals = pattern.map((k) => {
			// Jitter so the spacing isn't a giveaway arithmetic sequence.
			const jitter = 0.75 + Math.random() * 0.5;
			let v = round(value + k * g2 * jitter);
			if (v === round(value)) {
				v = round(value + Math.sign(k) * step);
			}
			return v;
		});
		if (
			vals.every((v) => v >= min && v <= max) &&
			new Set([...vals.map(label), label(value)]).size === 3
		) {
			return finish(
				{ id: "a", kind, label: label(value) },
				vals.map((v, i) => ({ id: `w${i}`, kind, label: label(v) })),
			);
		}
	}
	return undefined;
};

const fmtHeight = (inches: number) =>
	`${Math.floor(inches / 12)}'${Math.round(inches % 12)}"`;

// Team-season rows by season, cached alongside the pool.
let tsCache: { key: string; bySeason: Map<number, TeamSeason[]> } | undefined;
const poolKey = () => `${g.get("lid")}-${g.get("season")}-${g.get("phase")}`;
const getTeamSeasons = async (season: number): Promise<TeamSeason[]> => {
	const key = poolKey();
	if (tsCache?.key !== key) {
		tsCache = { key, bySeason: new Map() };
	}
	let rows = tsCache.bySeason.get(season);
	if (!rows) {
		rows = await idb.getCopies.teamSeasons({ season }, "noCopyCache");
		tsCache.bySeason.set(season, rows);
	}
	return rows;
};

const toSpinTeam = (ts: TeamSeason): SpinTeam => ({
	tid: ts.tid,
	abbrev: ts.abbrev,
	region: ts.region,
	name: ts.name,
	colors: ts.colors,
	imgURL: ts.imgURLSmall ?? ts.imgURL,
});

const teamOption = (ts: TeamSeason, id: string): SpinOption => ({
	id,
	kind: "team",
	label: ts.abbrev,
	sub: `${ts.region} ${ts.name}`.trim(),
	team: toSpinTeam(ts),
});

// The right team plus two others from that season. On Hard the distractors come
// from the same conference when it can, so the logos are rivals, not randoms.
const teamOptions = async (
	ctx: Ctx,
	season: number,
	tid: number,
	exclude: Set<number>,
): Promise<Built> => {
	let rows = await getTeamSeasons(season);
	let correct = rows.find((ts) => ts.tid === tid);
	if (!correct) {
		// A draft before the league's first season: use the earliest season that
		// franchise has, and draw the others from that season too.
		const all = await idb.getCopies.teamSeasons({ tid }, "noCopyCache");
		const first = all.sort((a, b) => a.season - b.season)[0];
		if (!first) {
			return undefined;
		}
		rows = await getTeamSeasons(first.season);
		correct = rows.find((ts) => ts.tid === tid);
		if (!correct) {
			return undefined;
		}
	}
	let others = rows.filter((ts) => ts.tid !== tid && !exclude.has(ts.tid));
	if (ctx.diff !== "easy") {
		const sameConf = others.filter((ts) => ts.cid === correct.cid);
		if (sameConf.length >= 2 && (ctx.diff === "hard" || Math.random() < 0.5)) {
			others = sameConf;
		}
	}
	shuffle(others);
	return finish(
		teamOption(correct, "a"),
		others.slice(0, 2).map((ts, i) => teamOption(ts, `w${i}`)),
	);
};

// Distinct text answers drawn by frequency from a population of values.
const textOptions = (
	correctLabel: string,
	population: [string, number][],
	kind: SpinOption["kind"],
	extra?: Partial<SpinOption>,
	extraFor?: (label: string) => Partial<SpinOption>,
): Built => {
	const pool = population.filter(([v]) => v !== correctLabel);
	const picked: string[] = [];
	for (let i = 0; i < 20 && picked.length < 2 && pool.length > 0; i++) {
		const v = weighted(pool);
		if (v !== undefined && !picked.includes(v)) {
			picked.push(v);
		}
	}
	return finish(
		{
			id: "a",
			kind,
			label: correctLabel,
			...extra,
			...extraFor?.(correctLabel),
		},
		picked.map((label, i) => ({
			id: `w${i}`,
			kind,
			label,
			...extra,
			...extraFor?.(label),
		})),
	);
};

const counts = (values: string[]) => {
	const m = new Map<string, number>();
	for (const v of values) {
		m.set(v, (m.get(v) ?? 0) + 1);
	}
	return [...m];
};

const seasonComplete = (season: number) =>
	season < g.get("season") ||
	(season === g.get("season") && g.get("phase") > PHASE.PLAYOFFS);

const recordOf = (ts: TeamSeason) => {
	const ties = ts.tied ?? 0;
	const otl = ts.otl ?? 0;
	return `${ts.won}-${ts.lost}${otl > 0 ? `-${otl}` : ""}${ties > 0 ? `-${ties}` : ""}`;
};

const titleCase = (s: string) =>
	s.replace(/\b([a-z])/g, (m) => m.toUpperCase());

// Honors, best first. A player who won several that season is asked about the
// biggest one; the wrong answers are honors he did not win that year.
const HONORS: { label: string; match: (t: string) => boolean }[] = [
	{ label: "MVP", match: (t) => t === "Most Valuable Player" },
	{ label: "Finals MVP", match: (t) => t === "Finals MVP" },
	{
		label: "Defensive POY",
		match: (t) => t === "Defensive Player of the Year",
	},
	{ label: "Rookie of the Year", match: (t) => t === "Rookie of the Year" },
	{ label: "Sixth Man", match: (t) => t === "Sixth Man of the Year" },
	{ label: "Most Improved", match: (t) => t === "Most Improved Player" },
	{ label: "All-League", match: (t) => t.includes("All-League") },
	{ label: "All-Defensive", match: (t) => t.includes("All-Defensive") },
	{ label: "All-Rookie", match: (t) => t.includes("All-Rookie") },
	{ label: "All-Star", match: (t) => t === "All-Star" },
	{ label: "Dunk Contest", match: (t) => t === "Slam Dunk Contest Winner" },
	{
		label: "3-Point Contest",
		match: (t) => t === "Three-Point Contest Winner",
	},
];

const POSITIONS = ["PG", "SG", "G", "GF", "SF", "F", "PF", "FC", "C"];

const ordinalRound = (round: number) => `${commonHelpers.ordinal(round)} round`;

// --- Categories --------------------------------------------------------------

const CATEGORIES: CategoryDef[] = [
	// Identity
	{
		key: "draftTeam",
		label: "Draft Team",
		weight: 2,
		build: async (ctx) => {
			const { p } = ctx;
			if (p.draft.round < 1 || p.draftTid < 0) {
				return undefined;
			}
			return teamOptions(ctx, p.draft.year, p.draftTid, new Set());
		},
	},
	{
		key: "college",
		label: "Alma Mater",
		weight: 2,
		build: (ctx) => {
			const college = ctx.p.college.trim();
			if (college === "" || college === "None") {
				return undefined;
			}
			// Schools that produced players around the same time.
			const near = ctx.pool.players.filter(
				(o) =>
					Math.abs(o.draft.year - ctx.p.draft.year) <= 4 &&
					o.college.trim() !== "" &&
					o.college !== "None",
			);
			const pop = counts(near.map((o) => o.college.trim()));
			return textOptions(college, pop, "college");
		},
	},
	{
		key: "birthplace",
		label: "Birthplace",
		weight: 2,
		build: (ctx) => {
			const loc = ctx.p.bornLoc.trim();
			if (loc === "") {
				return undefined;
			}
			const country = commonHelpers.getCountry(loc);
			if (country === "USA") {
				// US-born: guess the state.
				const stateOf = (l: string) => l.split(", ").at(-1) ?? "";
				const state = stateOf(loc);
				if (state === "" || state === "USA") {
					return undefined;
				}
				const pop = counts(
					ctx.pool.players
						.filter((o) => commonHelpers.getCountry(o.bornLoc) === "USA")
						.map((o) => stateOf(o.bornLoc))
						.filter((s) => s !== "" && s !== "USA"),
				);
				return textOptions(state, pop, "flag", {
					country: "USA",
					sub: "USA",
				});
			}
			const pop = counts(
				ctx.pool.players
					.map((o) => commonHelpers.getCountry(o.bornLoc))
					.filter((c) => c !== "None" && c !== country),
			);
			return textOptions(country, pop, "flag", undefined, (label) => ({
				country: label,
			}));
		},
	},
	{
		key: "draftYear",
		label: "Draft Year",
		weight: 1,
		build: (ctx) => {
			if (ctx.p.draft.round < 1) {
				return undefined;
			}
			return numericOptions(ctx, ctx.p.draft.year, {
				gap: 3,
				max: ctx.p.firstSeason,
			});
		},
	},
	{
		key: "draftPick",
		label: "Draft Pick",
		weight: 1,
		build: (ctx) => {
			const { draft } = ctx.p;
			const numTeams = g.get("numActiveTeams");
			const numRounds = Math.max(2, g.get("numDraftRounds"));
			const label = (round: number, pick: number) =>
				round < 1 ? "Undrafted" : `#${pick}`;
			const sub = (round: number) => (round < 1 ? "" : ordinalRound(round));
			const opt = (id: string, round: number, pick: number): SpinOption => ({
				id,
				kind: "number",
				label: label(round, pick),
				sub: sub(round),
			});
			if (draft.year < ctx.pool.minSeason - 1) {
				return undefined;
			}
			const correct =
				draft.round < 1 ? opt("a", 0, 0) : opt("a", draft.round, draft.pick);
			const t = tightness(ctx);
			const seen = new Set([`${correct.label}|${correct.sub}`]);
			const wrong: SpinOption[] = [];
			for (let i = 0; i < 40 && wrong.length < 2; i++) {
				let round: number;
				let pick: number;
				if (draft.round < 1 || Math.random() < 0.25) {
					round = 1 + Math.floor(Math.random() * numRounds);
					pick = 1 + Math.floor(Math.random() * numTeams);
				} else {
					round = draft.round;
					const spread = Math.max(2, Math.round(numTeams * 0.4 * t));
					pick =
						draft.pick +
						(Math.random() < 0.5 ? -1 : 1) *
							(1 + Math.floor(Math.random() * spread));
					if (pick < 1 || pick > numTeams) {
						continue;
					}
				}
				if (draft.round >= 2 && Math.random() < 0.2) {
					round = 0;
				}
				const o = opt(`w${wrong.length}`, round, pick);
				const k = `${o.label}|${o.sub}`;
				if (!seen.has(k)) {
					seen.add(k);
					wrong.push(o);
				}
			}
			return finish(correct, wrong);
		},
	},
	{
		key: "height",
		label: "Height",
		weight: 1,
		build: (ctx) => {
			if (!(ctx.p.hgt > 0)) {
				return undefined;
			}
			return numericOptions(ctx, Math.round(ctx.p.hgt), {
				gap: 3,
				min: 60,
				fmt: fmtHeight,
			});
		},
	},

	// That season
	{
		key: "team",
		label: "Team",
		weight: 3,
		build: (ctx) => {
			// Every team he suited up for that year is off the table as a wrong
			// answer - a traded player's other team is not "wrong".
			const exclude = new Set(
				ctx.p.rows
					.filter((r) => r.season === ctx.season && r.gp > 0)
					.map((r) => r.tid),
			);
			return teamOptions(ctx, ctx.season, ctx.tid, exclude);
		},
	},
	{
		key: "jersey",
		label: "Jersey Number",
		weight: 2,
		build: (ctx) => {
			const num = ctx.jerseyNumber;
			if (num === undefined || num === "") {
				return undefined;
			}
			const thatSeason = new Set(
				ctx.p.rows
					.filter((r) => r.season === ctx.season)
					.map((r) => r.jerseyNumber ?? ""),
			);
			const otherYears = [
				...new Set(
					ctx.p.rows
						.map((r) => r.jerseyNumber)
						.filter((j): j is string => !!j && !thatSeason.has(j)),
				),
			];
			const candidates: string[] = [];
			if (ctx.diff !== "easy") {
				candidates.push(...shuffle(otherYears));
				const n = Number(num);
				if (Number.isInteger(n)) {
					for (const d of shuffle([1, -1, 10, -10, 2, -2])) {
						if (n + d >= 0 && n + d <= 99) {
							candidates.push(String(n + d));
						}
					}
					if (num.length === 2) {
						candidates.push(`${num[1]}${num[0]}`.replace(/^0(?=\d)/, ""));
					}
				}
			}
			const common = shuffle([
				"0",
				"1",
				"2",
				"3",
				"4",
				"5",
				"6",
				"7",
				"8",
				"9",
				"10",
				"11",
				"12",
				"13",
				"14",
				"15",
				"20",
				"21",
				"22",
				"23",
				"24",
				"25",
				"30",
				"31",
				"32",
				"33",
				"34",
				"35",
				"40",
				"41",
				"42",
				"44",
				"45",
				"50",
				"55",
			]);
			candidates.push(...common);
			const wrong: string[] = [];
			for (const c of candidates) {
				if (!thatSeason.has(c) && c !== num && !wrong.includes(c)) {
					wrong.push(c);
				}
				if (wrong.length === 2) {
					break;
				}
			}
			return finish(
				{ id: "a", kind: "number", label: num },
				wrong.map((label, i) => ({ id: `w${i}`, kind: "number", label })),
			);
		},
	},
	{
		key: "ppg",
		label: "Points Per Game",
		weight: 2,
		build: (ctx) =>
			ctx.line.gp >= 10
				? numericOptions(
						ctx,
						Math.round((10 * ctx.line.pts) / ctx.line.gp) / 10,
						{
							gap: Math.max(2, (ctx.line.pts / ctx.line.gp) * 0.3),
							decimals: 1,
						},
					)
				: undefined,
	},
	{
		key: "rpg",
		label: "Rebounds Per Game",
		weight: 1,
		build: (ctx) =>
			ctx.line.gp >= 10
				? numericOptions(
						ctx,
						Math.round((10 * ctx.line.trb) / ctx.line.gp) / 10,
						{
							gap: Math.max(1, (ctx.line.trb / ctx.line.gp) * 0.3),
							decimals: 1,
						},
					)
				: undefined,
	},
	{
		key: "apg",
		label: "Assists Per Game",
		weight: 1,
		build: (ctx) =>
			ctx.line.gp >= 10
				? numericOptions(
						ctx,
						Math.round((10 * ctx.line.ast) / ctx.line.gp) / 10,
						{
							gap: Math.max(0.8, (ctx.line.ast / ctx.line.gp) * 0.3),
							decimals: 1,
						},
					)
				: undefined,
	},
	{
		key: "age",
		label: "Age",
		weight: 1,
		build: (ctx) =>
			numericOptions(ctx, ctx.season - ctx.p.bornYear, { gap: 3, min: 17 }),
	},
	{
		key: "gp",
		label: "Games Played",
		weight: 1,
		build: (ctx) =>
			numericOptions(ctx, ctx.line.gp, {
				gap: 14,
				min: 1,
				max: g.get("numGames", ctx.season),
			}),
	},
	{
		key: "position",
		label: "Position",
		weight: 1,
		build: (ctx) => {
			const idx = POSITIONS.indexOf(ctx.pos);
			if (idx < 0) {
				return undefined;
			}
			// Hard: neighbours on the PG-to-C spectrum. Easy: the far end.
			const others = POSITIONS.filter((pos) => pos !== ctx.pos).sort(
				(a, b) =>
					Math.abs(POSITIONS.indexOf(a) - idx) -
					Math.abs(POSITIONS.indexOf(b) - idx),
			);
			const pickFrom =
				ctx.diff === "hard"
					? others.slice(0, 4)
					: ctx.diff === "easy"
						? others.slice(-4)
						: others;
			const wrong = shuffle([...pickFrom]).slice(0, 2);
			return finish(
				{ id: "a", kind: "text", label: ctx.pos },
				wrong.map((label, i) => ({ id: `w${i}`, kind: "text", label })),
			);
		},
	},
	{
		key: "honor",
		label: "Award",
		weight: 2,
		build: (ctx) => {
			const won = new Set(
				ctx.p.awards
					.filter((a) => a.season === ctx.season)
					.flatMap((a) =>
						HONORS.filter((h) => h.match(a.type)).map((h) => h.label),
					),
			);
			const best = HONORS.find((h) => won.has(h.label));
			if (!best) {
				return undefined;
			}
			const wrong = shuffle(
				HONORS.filter((h) => !won.has(h.label)).map((h) => h.label),
			).slice(0, 2);
			return finish(
				{ id: "a", kind: "text", label: best.label },
				wrong.map((label, i) => ({ id: `w${i}`, kind: "text", label })),
			);
		},
	},
	{
		key: "playoffs",
		label: "Team Finish",
		weight: 1,
		build: async (ctx) => {
			if (!seasonComplete(ctx.season)) {
				return undefined;
			}
			const numRounds = g.get("numGamesPlayoffSeries", ctx.season).length;
			if (numRounds < 1) {
				return undefined;
			}
			const ts = (await getTeamSeasons(ctx.season)).find(
				(row) => row.tid === ctx.tid,
			);
			if (!ts) {
				return undefined;
			}
			const byConf = await getPlayoffsByConf(ctx.season);
			const labelFor = (won: number) => {
				if (won < 0) {
					return "Missed Playoffs";
				}
				if (won >= numRounds) {
					return "Won Title";
				}
				return `Lost in ${titleCase(
					commonHelpers.playoffRoundName(won, numRounds, byConf),
				)}`;
			};
			const all: string[] = [];
			for (let won = -1; won <= numRounds; won++) {
				all.push(labelFor(won));
			}
			const answer = labelFor(ts.playoffRoundsWon);
			const wrong = shuffle(all.filter((l) => l !== answer)).slice(0, 2);
			return finish(
				{ id: "a", kind: "text", label: answer },
				wrong.map((label, i) => ({ id: `w${i}`, kind: "text", label })),
			);
		},
	},
	{
		key: "record",
		label: "Team Record",
		weight: 1,
		build: async (ctx) => {
			if (!seasonComplete(ctx.season)) {
				return undefined;
			}
			const rows = await getTeamSeasons(ctx.season);
			const ts = rows.find((row) => row.tid === ctx.tid);
			if (!ts || ts.won + ts.lost <= 0) {
				return undefined;
			}
			const answer = recordOf(ts);
			const minGap = Math.max(1, Math.round(10 * tightness(ctx)));
			const others = rows
				.filter(
					(row) =>
						row.tid !== ctx.tid &&
						recordOf(row) !== answer &&
						Math.abs(row.won - ts.won) >= minGap,
				)
				.sort((a, b) => Math.abs(a.won - ts.won) - Math.abs(b.won - ts.won));
			const near = others.slice(0, ctx.diff === "hard" ? 4 : 8);
			const wrong: string[] = [];
			for (const row of shuffle(near)) {
				const r = recordOf(row);
				if (!wrong.includes(r)) {
					wrong.push(r);
				}
			}
			return finish(
				{ id: "a", kind: "number", label: answer },
				wrong.map((label, i) => ({ id: `w${i}`, kind: "number", label })),
			);
		},
	},
	{
		key: "teammate",
		label: "Teammate",
		weight: 2,
		build: (ctx) => {
			const key = (season: number, tid: number) => `${season}-${tid}`;
			const mine = new Set(ctx.p.rows.map((r) => key(r.season, r.tid)));
			const teammates: TriviaPlayer[] = [];
			const elsewhere: TriviaPlayer[] = [];
			for (const o of ctx.pool.players) {
				if (o.pid === ctx.p.pid) {
					continue;
				}
				const rows = o.rows.filter((r) => r.season === ctx.season && r.gp > 0);
				if (rows.length === 0) {
					continue;
				}
				if (rows.some((r) => r.tid === ctx.tid)) {
					teammates.push(o);
				} else if (!o.rows.some((r) => mine.has(key(r.season, r.tid)))) {
					elsewhere.push(o);
				}
			}
			if (teammates.length === 0 || elsewhere.length < 2) {
				return undefined;
			}
			// A recognisable teammate; the wrong answers are about as famous, so the
			// right one doesn't stand out as the only name you know.
			teammates.sort((a, b) => b.popularity - a.popularity);
			const mate = random(
				teammates.slice(0, Math.max(1, Math.min(4, teammates.length))),
			);
			elsewhere.sort(
				(a, b) =>
					Math.abs(a.popularity - mate.popularity) -
					Math.abs(b.popularity - mate.popularity),
			);
			const wrong = shuffle(elsewhere.slice(0, 10)).slice(0, 2);
			const opt = (o: TriviaPlayer, id: string): SpinOption => ({
				id,
				kind: "player",
				label: o.lastName || o.name,
				sub: o.lastName ? o.firstName : undefined,
			});
			return finish(
				opt(mate, "a"),
				wrong.map((o, i) => opt(o, `w${i}`)),
			);
		},
	},

	// Career
	{
		key: "careerHigh",
		label: "Career-High Points",
		weight: 1,
		build: (ctx) =>
			ctx.p.gameHigh.pts > 0
				? numericOptions(ctx, ctx.p.gameHigh.pts, {
						gap: Math.max(4, ctx.p.gameHigh.pts * 0.2),
						min: 1,
					})
				: undefined,
	},
	{
		key: "careerPpg",
		label: "Career PPG",
		weight: 1,
		build: (ctx) =>
			ctx.p.tot.gp >= 50
				? numericOptions(
						ctx,
						Math.round((10 * ctx.p.tot.pts) / ctx.p.tot.gp) / 10,
						{
							gap: Math.max(1.5, (ctx.p.tot.pts / ctx.p.tot.gp) * 0.3),
							decimals: 1,
						},
					)
				: undefined,
	},
	{
		key: "numTeams",
		label: "Teams Played For",
		weight: 1,
		build: (ctx) =>
			numericOptions(ctx, ctx.p.teamsPlayed.length, { gap: 1.5, min: 1 }),
	},
	{
		key: "allStars",
		label: "All-Star Selections",
		weight: 1,
		build: (ctx) => {
			const n = ctx.p.awards.filter((a) => a.type === "All-Star").length;
			if (n === 0 && ctx.p.tot.seasons < 6) {
				return undefined;
			}
			return numericOptions(ctx, n, { gap: n >= 6 ? 3 : 1.5 });
		},
	},
	{
		key: "titles",
		label: "Championships",
		weight: 1,
		build: (ctx) => {
			const n = ctx.p.awards.filter(
				(a) => a.type === "Won Championship",
			).length;
			if (n === 0) {
				return undefined;
			}
			return numericOptions(ctx, n, { gap: 1.5 });
		},
	},
];

export const SPIN_CATEGORY_LABELS = CATEGORIES.map((c) => c.label);

// --- Player draw -------------------------------------------------------------

let sortedCache: { key: string; players: TriviaPlayer[] } | undefined;
const famousFirst = (pool: TriviaPool) => {
	const key = poolKey();
	if (sortedCache?.key !== key) {
		sortedCache = {
			key,
			players: pool.players
				.filter((p) => p.tot.gp >= 20)
				.sort((a, b) => b.popularity - a.popularity),
		};
	}
	return sortedCache.players;
};

// How deep into the league's fame list players are drawn from. The streak
// pushes deeper, so a long run starts reaching for role players.
const depth = (diff: SpinDifficulty, streak: number, n: number) => {
	const [share, grow, cap, min] =
		diff === "easy"
			? [0.05, 0.004, 0.12, 60]
			: diff === "normal"
				? [0.15, 0.01, 0.35, 150]
				: [0.4, 0.015, 0.8, 300];
	return Math.min(
		n,
		Math.max(min, Math.round(n * Math.min(cap, share + grow * streak))),
	);
};

export const generateSpinQuestion = async ({
	difficulty,
	streak,
	recentPids,
	lastCategory,
}: {
	difficulty: SpinDifficulty;
	streak: number;
	recentPids: number[];
	lastCategory?: string;
}): Promise<SpinQuestion | undefined> => {
	const pool = await getTriviaPool();
	const ranked = famousFirst(pool);
	if (ranked.length < 10) {
		return undefined;
	}
	const recent = new Set(recentPids);
	const top = ranked.slice(0, depth(difficulty, streak, ranked.length));

	for (let attempt = 0; attempt < 40; attempt++) {
		const p = random(top);
		if (recent.has(p.pid) && attempt < 30) {
			continue;
		}

		// A season he really played, weighted by games so a 3-game cameo year
		// rarely comes up.
		const lines = [...mergedSeasons(p)].filter(([, s]) => s.gp > 0);
		const picked = weighted(
			lines.map((l) => [l, l[1].gp] as [typeof l, number]),
		);
		if (!picked) {
			continue;
		}
		const [season, line] = picked;
		const stints = p.rows.filter((r) => r.season === season && r.gp > 0);
		const main = stints.sort((a, b) => b.gp - a.gp)[0];
		if (!main) {
			continue;
		}

		const ctx: Ctx = {
			p,
			season,
			line,
			tid: main.tid,
			jerseyNumber: main.jerseyNumber,
			pos: main.pos,
			pool,
			diff: difficulty,
			streak,
		};

		const cats = CATEGORIES.filter((c) => c.key !== lastCategory);
		const order: CategoryDef[] = [];
		const remaining = [...cats];
		while (remaining.length > 0) {
			const c = weighted(
				remaining.map((x) => [x, x.weight] as [CategoryDef, number]),
			)!;
			order.push(c);
			remaining.splice(remaining.indexOf(c), 1);
		}

		for (const cat of order.slice(0, 6)) {
			let built: Built;
			try {
				built = await cat.build(ctx);
			} catch (error) {
				console.error(`Spin Streak category ${cat.key} failed`, error);
				built = undefined;
			}
			if (!built) {
				continue;
			}

			const decoyPlayers = shuffle(
				top.slice(0, 200).filter((o) => o.pid !== p.pid),
			)
				.slice(0, 7)
				.map((o) => ({ firstName: o.firstName, lastName: o.lastName }));
			const decoySeasons: number[] = [];
			for (let i = 0; i < 7; i++) {
				decoySeasons.push(
					pool.minSeason +
						Math.floor(Math.random() * (pool.maxSeason - pool.minSeason + 1)),
				);
			}
			const decoyCats = shuffle(
				CATEGORIES.filter((c) => c.key !== cat.key).map((c) => c.label),
			).slice(0, 7);

			return {
				id: `${p.pid}-${season}-${cat.key}-${Math.random().toString(36).slice(2, 8)}`,
				season,
				pid: p.pid,
				firstName: p.firstName,
				lastName: p.lastName,
				category: cat.key,
				categoryLabel: cat.label,
				options: built.options,
				answer: built.answer,
				decoys: {
					seasons: decoySeasons,
					players: decoyPlayers,
					categories: decoyCats,
				},
			};
		}
		// Nothing askable about this player-season; try someone else.
	}

	return undefined;
};
