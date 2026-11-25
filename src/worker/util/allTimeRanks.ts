// A PLAYER'S ALL-TIME RANKS: his place on every all-time list there is for
// him (common/allTimeStats.ts) and the men around him on it, two ahead and
// one behind, the way a broadcast shows the list. For anyone who has played,
// retired too. Read from the same careers as the Milestone Watch.

import {
	ALL_TIME_PER_GAME,
	ALL_TIME_TOTALS,
	type AllTimeScope,
} from "../../common/allTimeStats.ts";
import g from "./g.ts";
import {
	type CareerRow,
	type Sums,
	getPlayerMilestoneExtras,
	loadRows,
} from "./milestoneWatch.ts";

export type LadderRow = {
	pid: number;
	name: string;
	tid: number;
	active: boolean;
	rank: number;
	value: number;
	from: number;
	to: number;
};

export type AllTimeEntry = {
	key: string;
	// Undefined when he isn't on the list: none of it, or (a rate) not enough
	// games, minutes or makes for it yet.
	value: number | undefined;
	rank: number | undefined;
	// How many players are on the list.
	of: number;
	// What it takes to be on a rate's list, in this league.
	needs?: { stat: string; value: number; have: number };
	ladder: LadderRow[];
};

const total = (t: Sums, key: string) => {
	if (key === "trb") {
		return t.trb + t.orb + t.drb;
	}
	if (key === "ws") {
		return t.ows + t.dws;
	}
	return t[key as keyof Sums] ?? 0;
};

const rate = (t: Sums, key: string) => {
	switch (key) {
		case "ppg":
			return t.pts / t.gp;
		case "rpg":
			return (t.trb + t.orb + t.drb) / t.gp;
		case "apg":
			return t.ast / t.gp;
		case "spg":
			return t.stl / t.gp;
		case "bpg":
			return t.blk / t.gp;
		case "mpg":
			return t.min / t.gp;
		case "fgp":
			return (100 * t.fg) / t.fga;
		case "tpp":
			return (100 * t.tp) / t.tpa;
		case "ftp":
			return (100 * t.ft) / t.fta;
		case "per":
			return t.perMin / t.min;
		case "ws48":
			return ((t.ows + t.dws) * 48) / t.min;
		case "bpm":
			return t.bpmMin / t.min;
	}
	return Number.NaN;
};

// A list's minimum in this league: fewer games, or shorter ones, ask for less.
const scaledMin = (stat: string, value: number) => {
	const games = g.get("numGames") / 82;
	const minutes = (g.get("numPeriods") * g.get("quarterLength")) / 48;
	return Math.round(value * games * (stat === "min" ? minutes : 1));
};

const ladderRow = (row: CareerRow, rank: number, value: number): LadderRow => ({
	pid: row.pid,
	name: row.name,
	tid: row.tid,
	active: row.active,
	rank,
	value,
	from: row.from,
	to: row.to,
});

// One list: everyone with a value on it, best first, and his spot.
const entry = (
	rows: CareerRow[],
	me: CareerRow,
	key: string,
	valueOf: (row: CareerRow) => number | undefined,
): Omit<AllTimeEntry, "needs"> => {
	const list: { row: CareerRow; value: number }[] = [];
	for (const row of rows) {
		const value = valueOf(row);
		if (value !== undefined && Number.isFinite(value)) {
			list.push({ row, value });
		}
	}
	list.sort((a, b) => b.value - a.value);

	// Tied, a man shares the place of the first on that value.
	const ranks: number[] = [];
	for (let i = 0; i < list.length; i++) {
		ranks.push(
			i > 0 && list[i]!.value === list[i - 1]!.value ? ranks[i - 1]! : i + 1,
		);
	}

	const i = list.findIndex((item) => item.row.pid === me.pid);
	if (i < 0) {
		return {
			key,
			value: undefined,
			rank: undefined,
			of: list.length,
			ladder: [],
		};
	}

	// Two ahead and one behind, or four from the top or bottom of the list.
	let start = Math.max(0, i - 2);
	const end = Math.min(list.length, start + 4);
	start = Math.max(0, end - 4);
	const ladder: LadderRow[] = [];
	for (let j = start; j < end; j++) {
		ladder.push(ladderRow(list[j]!.row, ranks[j]!, list[j]!.value));
	}

	return {
		key,
		value: list[i]!.value,
		rank: ranks[i],
		of: list.length,
		ladder,
	};
};

export const getPlayerAllTime = async (pid: number) => {
	const rows = await loadRows();
	const me = rows.find((row) => row.pid === pid);
	if (!me || (me.reg.gp === 0 && me.reg.min === 0 && me.reg.pts === 0)) {
		return undefined;
	}

	const positive = (sums: (row: CareerRow) => Sums, key: string) => {
		return (row: CareerRow) => {
			const value = total(sums(row), key);
			return value > 0 ? value : undefined;
		};
	};

	const totals = ALL_TIME_TOTALS.map((stat) =>
		entry(
			rows,
			me,
			stat.key,
			positive((row) => row.reg, stat.key),
		),
	);

	const perGame: AllTimeEntry[] = ALL_TIME_PER_GAME.map((stat) => {
		const needs = scaledMin(stat.min.stat, stat.min.value);
		const out = entry(rows, me, stat.key, (row) =>
			row.reg[stat.min.stat] >= needs ? rate(row.reg, stat.key) : undefined,
		);
		const have = me.reg[stat.min.stat];
		if (have >= needs) {
			return out;
		}
		const mine = rate(me.reg, stat.key);
		return {
			...out,
			value: Number.isFinite(mine) ? mine : undefined,
			needs: { stat: stat.min.stat, value: needs, have },
		};
	});

	const playoffs =
		me.po.gp > 0
			? ALL_TIME_TOTALS.map((stat) =>
					entry(
						rows,
						me,
						stat.key,
						positive((row) => row.po, stat.key),
					),
				)
			: [];

	const scopes: Record<AllTimeScope, AllTimeEntry[]> = {
		totals,
		perGame,
		playoffs,
	};

	return {
		active: me.active,
		scopes,
		...(await getPlayerMilestoneExtras(rows, me)),
	};
};
