// THE DBA REPLAY (docs/smart-cpu.md, yardstick C): every trade of the owner's
// league from `from` to `to`, put to a valuation at the moment it was made.
//
// - Human-CPU trades: would the CPU side have said yes? Vanilla did (the
//   league plays on it), so vanilla's yes rate is the check on how faithful
//   the rebuilt moments are. The ones the humans clearly won - their team
//   better and the CPU's worse, then and in the seasons after - a smart CPU
//   must turn down.
// - CPU-CPU trades: would each side have said yes? The howlers - a contender
//   (top-8 record) clearly worse for it - a smart CPU must not make.
//
// Team strength is the sim-fitted roster model (realisticFrontOffice/
// strength.ts rosterPoints, points a game): the roster just before and just
// after the trade; and at the start of each of the next three seasons, the
// roster it had against the same roster with the trade undone (what it got
// still there taken off, what it gave added back while his contract then
// would have run; a pick counts as the player drafted with it). Trades it
// made with those players later aren't followed.

import type { Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import {
	playerLine,
	rosterPoints,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { LeagueState, loadLeague } from "./state.ts";
import { PHASE, Timeline, type Moment, type ReplayTrade } from "./timeline.ts";
import type { Valuation } from "./valuations.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

export type ReplayOptions = {
	// The full export (events, every player's history).
	league: string;
	// What to load into the test league: the export or its slim copy (only
	// the teams, settings and current season are read from it).
	db?: string;
	from: number;
	to: number;
	valuations: Valuation[];
	// Points a game, for "clearly" better or worse.
	clear?: number;
	// Draft classes in the league at once (Timeline.prospectClasses).
	prospectClasses?: number;
	out?: string;
	log?: (line: string) => void;
};

type SideRow = {
	tid: number;
	abbrev: string;
	human: boolean;
	receives: string[];
	strategy?: string;
	record?: string;
	// Rank of its record that season (1 best), the season's standings.
	rank?: number;
	before: number;
	after: number;
	// Strength change: now, and at the start of each of the next three
	// seasons (null: not played yet).
	now: number;
	next: (number | null)[];
	dv: Record<string, number | null>;
	titles: number[];
};

export type ReplayRow = {
	eid: number;
	season: number;
	phase: number;
	games: number;
	kind: "HC" | "CC";
	inSeason: boolean;
	sides: [SideRow, SideRow];
	// For a human's trade: which side is the human's, which the CPU's.
	humanSide?: number;
	// Clearly won by the human (HC), a contender clearly worse (CC, its side).
	humansWon?: boolean;
	howlerSide?: number;
	// The change counted as "now" and "later" for each side (see judge).
	nowOf: [number, number];
	laterOf: [number | null, number | null];
	errors: string[];
	aiTrade?: unknown;
};

const lineOf = (p: Player) => {
	const r = p.ratings.at(-1)!;
	return { ...playerLine(r as any, r.pos), valueNoPot: p.valueNoPot };
};
const strengthOf = (players: Player[]) => rosterPoints(players.map(lineOf));

const fmt = (x: number | null | undefined, d = 1) =>
	x === null || x === undefined || Number.isNaN(x)
		? "  -  "
		: `${x >= 0 ? "+" : ""}${x.toFixed(d)}`;

const pct = (a: number, b: number) =>
	b > 0 ? `${a}/${b} (${((100 * a) / b).toFixed(0)}%)` : "0/0";

export const runDbaReplay = async (opts: ReplayOptions) => {
	const t0 = Date.now();
	const lines: string[] = [];
	const say = (s: string) => {
		lines.push(s);
		(opts.log ?? console.log)(s);
	};
	const clear = opts.clear ?? 0.5;
	const fs = await importNode("node:fs/promises");

	await loadLeague(opts.db ?? opts.league);
	const data = JSON.parse(await fs.readFile(opts.league, "utf8"));
	const tl = new Timeline(data);
	if (opts.prospectClasses !== undefined) {
		tl.prospectClasses = opts.prospectClasses;
	}
	say(
		`DBA replay ${opts.from}-${opts.to}: ${opts.valuations.map((v) => v.name).join(", ")}; humans ${tl.humans.map((t) => tl.abbrev(t)).join(", ")}; difficultyTrade ${g.get("difficultyTrade")}; ${tl.prospectClasses} draft classes at a time`,
	);
	say("\nThe rebuild, checked against the export:");
	for (const line of tl.check(opts.from)) {
		say(`  ${line}`);
	}

	const state = new LeagueState(tl, opts.from);
	await state.rebuildStrategies(Math.min(opts.to, tl.currentSeason - 1));
	const lastReview = state.strategies.get(tl.currentSeason - 1);
	if (lastReview) {
		const cpu = data.teams.filter((t: any) => !tl.humans.includes(t.tid));
		const same = cpu.filter((t: any) => lastReview.get(t.tid) === t.strategy);
		say(
			`  vanilla's strategies, rebuilt review by review from ${tl.startingSeason}, against the export's now: ${same.length}/${cpu.length} CPU teams the same`,
		);
	}
	say(`  (${((Date.now() - t0) / 1000).toFixed(0)} s)`);

	const trades = tl.trades.filter(
		(t) => t.season >= opts.from && t.season <= opts.to && t.kind !== "HH",
	);
	const rows: ReplayRow[] = [];
	// For the seasons after: what each side got and gave, and until when
	// what it gave would have stayed.
	const flows = new Map<
		number,
		{ got: Set<number>[]; gave: Set<number>[]; until: Map<number, number> }
	>();
	const pickPlayer = (dpid: number, tid: number) => {
		const info = tl.picks.get(dpid);
		if (info?.usedBy !== undefined) {
			const p = tl.players.get(info.usedBy);
			if (p?.draft?.tid === tid) {
				return p.pid as number;
			}
		}
		return undefined;
	};
	const label = (p: Player) => {
		const r = p.ratings.at(-1)!;
		const c = p.contracts.at(-1);
		const exp = c ? c.start + c.amounts.length - 1 : p.contractDemand?.exp;
		const amount = c
			? c.amounts[
					Math.max(0, Math.min(c.amounts.length - 1, g.get("season") - c.start))
				]!
			: (p.contractDemand?.amount ?? 0);
		return `${p.firstName} ${p.lastName} (${g.get("season") - p.born.year}y ${r.ovr}/${r.pot} $${(amount / 1000).toFixed(1)}M->${exp})`;
	};
	const pickLabel = (dpid: number) => {
		const info = tl.picks.get(dpid);
		if (!info) {
			return `pick ${dpid}`;
		}
		return `${info.season} ${info.round === 1 ? "1st" : "2nd"} (${tl.abbrev(info.originalTid)})`;
	};

	for (const [n, t] of trades.entries()) {
		const userTid = t.human ?? tl.humans[0]!;
		await state.apply(t.moment, userTid);
		const S = t.season;
		const standings = tl.standings(S);
		const sides = [] as unknown as [SideRow, SideRow];
		const errors: string[] = [];
		const until = new Map<number, number>();
		const rosters = await Promise.all(
			t.sides.map((s) => idb.cache.players.indexGetAll("playersByTid", s.tid)),
		);
		for (const [i, s] of t.sides.entries()) {
			const other = t.sides[1 - i]!;
			const roster = rosters[i]!;
			const incoming: Player[] = [];
			for (const pid of s.pids) {
				const p = await idb.cache.players.get(pid);
				if (p) {
					incoming.push(p);
					const c = p.contracts.at(-1);
					until.set(
						pid,
						c ? c.start + c.amounts.length - 1 : (p.contractDemand?.exp ?? S),
					);
				}
			}
			const before = strengthOf(roster);
			const after = strengthOf([
				...roster.filter((p) => !other.pids.includes(p.pid)),
				...incoming,
			]);
			const rec = tl.recordAt(s.tid, t.moment);
			const ts = tl.records.get(`${S}:${s.tid}`);
			sides.push({
				tid: s.tid,
				abbrev: tl.abbrev(s.tid),
				human: tl.humans.includes(s.tid),
				receives: [...incoming.map(label), ...s.dpids.map(pickLabel)],
				strategy: state.strategiesAt(t.moment)?.get(s.tid),
				record: rec
					? `${rec.won}-${rec.lost} (final ${ts.won}-${ts.lost})`
					: undefined,
				rank: standings.findIndex((r) => r.tid === s.tid) + 1,
				before,
				after,
				now: after - before,
				next: [null, null, null],
				dv: {},
				titles: [S, S + 1, S + 2, S + 3].filter(
					(y) => tl.champion(y) === s.tid,
				),
			});
		}
		// Drafted players' first contracts, for how long a pick's player
		// would have stayed.
		const got = t.sides.map((s) => {
			const set = new Set<number>(s.pids);
			for (const dpid of s.dpids) {
				const pid = pickPlayer(dpid, s.tid);
				if (pid !== undefined) {
					set.add(pid);
					const first = tl.players.get(pid)?.contracts?.[0];
					until.set(
						pid,
						first ? first.start + first.amounts.length - 1 : S + 3,
					);
				}
			}
			return set;
		});
		flows.set(t.eid, { got, gave: [got[1]!, got[0]!], until });

		for (const v of opts.valuations) {
			await state.restoreStrategies();
			try {
				await v.prepare?.(t.moment);
			} catch (error) {
				errors.push(`${v.name} prepare: ${(error as Error).message}`);
				for (const side of sides) {
					side.dv[v.name] = null;
				}
				continue;
			}
			for (const [i, s] of t.sides.entries()) {
				const other = t.sides[1 - i]!;
				try {
					sides[i]!.dv[v.name] = await v.evaluate({
						tid: s.tid,
						partnerTid: other.tid,
						pidsAdd: s.pids,
						pidsRemove: other.pids,
						dpidsAdd: s.dpids,
						dpidsRemove: other.dpids,
						vsHuman: tl.humans.includes(other.tid),
						initiator: t.kind === "CC" && i === 0,
					});
				} catch (error) {
					sides[i]!.dv[v.name] = null;
					errors.push(
						`${v.name} ${tl.abbrev(s.tid)}: ${(error as Error).message}`,
					);
				}
			}
		}
		const humanSide =
			t.kind === "HC"
				? tl.humans.includes(t.sides[0].tid)
					? 0
					: 1
				: undefined;
		rows.push({
			eid: t.eid,
			season: S,
			phase: t.phase,
			games: Math.round(t.moment.games),
			kind: t.kind as "HC" | "CC",
			inSeason: t.phase <= PHASE.AFTER_TRADE_DEADLINE,
			sides,
			humanSide,
			nowOf: [0, 0],
			laterOf: [null, null],
			errors,
			aiTrade: t.aiTrade,
		});
		if ((n + 1) % 25 === 0) {
			(opts.log ?? console.log)(
				`  ${n + 1}/${trades.length} trades (${((Date.now() - t0) / 1000).toFixed(0)} s)`,
			);
		}
	}

	// The seasons after: each team at the start of each season, with and
	// without the trade.
	const lastSeason = Math.min(opts.to + 3, tl.currentSeason);
	for (let Y = opts.from + 1; Y <= lastSeason; Y++) {
		const m: Moment | undefined = tl.seasonStart(Y);
		if (!m) {
			continue;
		}
		await state.apply(m, tl.humans[0]!);
		const byTid = Map.groupBy(
			await idb.cache.players.indexGetAll("playersByTid", [0, Infinity]),
			(p) => p.tid,
		);
		for (const row of rows) {
			const k = Y - row.season - 1;
			if (k < 0 || k > 2) {
				continue;
			}
			const flow = flows.get(row.eid)!;
			for (const [i, side] of row.sides.entries()) {
				const actual = byTid.get(side.tid) ?? [];
				const undone = actual.filter((p) => !flow.got[i]!.has(p.pid));
				for (const pid of flow.gave[i]!) {
					const p = await idb.cache.players.get(pid);
					if (
						p &&
						p.tid !== side.tid &&
						Y <= (flow.until.get(pid) ?? row.season)
					) {
						undone.push(p);
					}
				}
				side.next[k] = strengthOf(actual) - strengthOf(undone);
			}
		}
	}

	// Now and later, for each side: an in-season trade's now is the rest of
	// that season, its later the next two; an offseason trade's now is the
	// coming season, its later the two after.
	const mean = (xs: (number | null)[]) => {
		const ys = xs.filter((x): x is number => x !== null);
		return ys.length > 0 ? ys.reduce((a, b) => a + b, 0) / ys.length : null;
	};
	for (const row of rows) {
		for (const [i, side] of row.sides.entries()) {
			if (row.inSeason) {
				row.nowOf[i] = side.now;
				row.laterOf[i] = mean(side.next.slice(0, 2));
			} else {
				row.nowOf[i] = side.next[0] ?? Number.NaN;
				row.laterOf[i] = mean(side.next.slice(1, 3));
			}
		}
		if (row.kind === "HC") {
			const h = row.humanSide!;
			const c = 1 - h;
			const laterOk =
				row.laterOf[h] === null ||
				row.laterOf[c] === null ||
				(row.laterOf[h]! >= clear && row.laterOf[c]! <= -clear);
			row.humansWon =
				row.nowOf[h]! >= clear && row.nowOf[c]! <= -clear && laterOk;
		} else {
			// A contender: top 8 that season (in the season) or the next
			// (in the offseason).
			const contenderSeason = row.inSeason ? row.season : row.season + 1;
			const top8 = tl
				.standings(contenderSeason)
				.slice(0, 8)
				.map((r) => r.tid);
			for (const [i, side] of row.sides.entries()) {
				if (top8.includes(side.tid) && row.nowOf[i]! <= -clear) {
					if (
						row.howlerSide === undefined ||
						row.nowOf[i]! < row.nowOf[row.howlerSide]!
					) {
						row.howlerSide = i;
					}
				}
			}
		}
	}

	// ---- The report.
	const names = opts.valuations.map((v) => v.name);
	const yes = (x: number | null | undefined) =>
		x !== null && x !== undefined && x > 0;
	const hc = rows.filter((r) => r.kind === "HC");
	const cc = rows.filter((r) => r.kind === "CC");
	const won = hc.filter((r) => r.humansWon);
	const howlers = cc.filter((r) => r.howlerSide !== undefined);
	const cpuDv = (r: ReplayRow, v: string) => r.sides[1 - r.humanSide!]!.dv[v];
	say(
		`\n${hc.length} human-CPU trades, ${cc.length} CPU-CPU (${((Date.now() - t0) / 1000).toFixed(0)} s). "Clearly": ${clear} point a game or more.`,
	);
	say("\nHuman-CPU trades: the CPU side says yes");
	const groups: [string, ReplayRow[]][] = [
		["all", hc],
		["  in the season", hc.filter((r) => r.inSeason)],
		["  draft and offseason", hc.filter((r) => !r.inSeason)],
		...tl.humans.map(
			(h) =>
				[
					`  with ${tl.abbrev(h)}`,
					hc.filter((r) => r.sides[r.humanSide!]!.tid === h),
				] as [string, ReplayRow[]],
		),
		["humans clearly won", won],
	];
	for (const [name, list] of groups) {
		say(
			`  ${name.padEnd(22)} ${names.map((v) => `${v} ${pct(list.filter((r) => yes(cpuDv(r, v))).length, list.length)}`).join("   ")}`,
		);
	}
	for (const v of names) {
		const dvs = hc
			.map((r) => cpuDv(r, v))
			.filter((x): x is number => x !== null)
			.sort((a, b) => a - b);
		const errs = hc.filter((r) => cpuDv(r, v) === null).length;
		if (dvs.length > 0) {
			say(
				`  ${v}: CPU side's dv - median ${dvs[Math.floor(dvs.length / 2)]!.toFixed(2)}, 10th pct ${dvs[Math.floor(dvs.length / 10)]!.toFixed(2)}${errs > 0 ? `; ${errs} errors` : ""}`,
			);
		}
	}
	// Trades are struck at the margin (a human asks what would make it work;
	// vanilla adds until its side is just over 0), so a small error in the
	// rebuild flips a yes: how many are within a little of one.
	const near = (x: number | null | undefined, by: number) =>
		x !== null && x !== undefined && x > -by;
	const hasPick = (r: ReplayRow) =>
		r.sides.some((s) => s.receives.some((x) => /^\d{4} (1st|2nd)/.test(x)));
	for (const [name, list] of [
		["all", hc],
		["players only", hc.filter((r) => !hasPick(r))],
		["with picks", hc.filter(hasPick)],
	] as [string, ReplayRow[]][]) {
		say(
			`  ${name.padEnd(14)} CPU side > 0 / > -0.25 / > -1: ${names.map((v) => `${v} ${list.filter((r) => yes(cpuDv(r, v))).length}/${list.filter((r) => near(cpuDv(r, v), 0.25)).length}/${list.filter((r) => near(cpuDv(r, v), 1)).length} of ${list.length}`).join(", ")}`,
		);
	}
	say("\nCPU-CPU trades");
	say(
		`  the side that had to say yes (vanilla's teams[1]) > 0 / > -0.25 / > -1: ${names.map((v) => `${v} ${cc.filter((r) => yes(r.sides[1]!.dv[v])).length}/${cc.filter((r) => near(r.sides[1]!.dv[v], 0.25)).length}/${cc.filter((r) => near(r.sides[1]!.dv[v], 1)).length} of ${cc.length}`).join(", ")}`,
	);
	const ruleVanilla = (r: ReplayRow, v: string) =>
		yes(r.sides[1]!.dv[v]) && Math.abs(r.sides[0]!.dv[v] ?? Infinity) <= 15;
	const both = (r: ReplayRow, v: string) =>
		yes(r.sides[0]!.dv[v]) && yes(r.sides[1]!.dv[v]);
	for (const [name, list] of [
		["all", cc],
		["  stock (no engine note)", cc.filter((r) => r.aiTrade === undefined)],
		["howlers", howlers],
	] as [string, ReplayRow[]][]) {
		say(
			`  ${name.padEnd(24)} vanilla's own rule (partner > 0, starter within 15): ${names.map((v) => `${v} ${pct(list.filter((r) => ruleVanilla(r, v)).length, list.length)}`).join(", ")}`,
		);
		say(
			`  ${"".padEnd(24)} both sides > 0: ${names.map((v) => `${v} ${pct(list.filter((r) => both(r, v)).length, list.length)}`).join(", ")}`,
		);
	}
	say(
		`  howlers, the contender's own answer: ${names.map((v) => `${v} ${pct(howlers.filter((r) => yes(r.sides[r.howlerSide!]!.dv[v])).length, howlers.length)}`).join(", ")}`,
	);

	const describe = (r: ReplayRow, focus: number) => {
		const a = r.sides[focus]!;
		const b = r.sides[1 - focus]!;
		const phase = r.inSeason
			? `game ${r.games}`
			: [
					"preseason",
					"season",
					"deadline",
					"playoffs",
					"lottery",
					"draft",
					"after draft",
					"re-signing",
					"free agency",
				][r.phase];
		return [
			`${r.season} ${phase}: ${a.abbrev} ${a.record ?? ""} [${a.strategy ?? "?"}] gets ${a.receives.join(" + ") || "nothing"}`,
			`      ${b.abbrev} ${b.record ?? ""} [${b.strategy ?? "?"}] gets ${b.receives.join(" + ") || "nothing"}`,
			`      strength now ${a.abbrev} ${fmt(r.nowOf[focus])} ${b.abbrev} ${fmt(r.nowOf[1 - focus])}; later ${a.abbrev} ${fmt(r.laterOf[focus])} ${b.abbrev} ${fmt(r.laterOf[1 - focus])}; next 3 seasons ${a.abbrev} ${a.next.map((x) => fmt(x)).join(" ")} / ${b.abbrev} ${b.next.map((x) => fmt(x)).join(" ")}`,
			`      titles ${r.season}-${r.season + 3}: ${a.abbrev} ${a.titles.join(",") || "-"}, ${b.abbrev} ${b.titles.join(",") || "-"}; ${b.abbrev} says yes? ${names.map((v) => `${v} ${fmt(b.dv[v], 2)}`).join(", ")}; ${a.abbrev}: ${names.map((v) => `${v} ${fmt(a.dv[v], 2)}`).join(", ")}`,
		].join("\n");
	};
	const margin = (r: ReplayRow, i: number) =>
		r.nowOf[i]! -
		r.nowOf[1 - i]! +
		(r.laterOf[i] ?? 0) -
		(r.laterOf[1 - i] ?? 0);
	say(`\nHumans clearly won (${won.length}), most lopsided first:`);
	for (const r of [...won].sort(
		(a, b) => margin(b, b.humanSide!) - margin(a, a.humanSide!),
	)) {
		say(`  ${describe(r, r.humanSide!)}`);
	}
	say(`\nCPU howlers (${howlers.length}), the contender worst off first:`);
	for (const r of [...howlers].sort(
		(a, b) => a.nowOf[a.howlerSide!]! - b.nowOf[b.howlerSide!]!,
	)) {
		say(`  ${describe(r, 1 - r.howlerSide!)}`);
	}
	const errors = rows.filter((r) => r.errors.length > 0);
	if (errors.length > 0) {
		say(
			`\n${errors.length} trades with errors, e.g. ${errors
				.slice(0, 3)
				.map((r) => `${r.season} eid ${r.eid}: ${r.errors.join("; ")}`)
				.join(" | ")}`,
		);
	}
	say(`\nDone in ${((Date.now() - t0) / 1000).toFixed(0)} s.`);
	if (opts.out) {
		await fs.mkdir(opts.out, { recursive: true });
		await fs.writeFile(`${opts.out}/dba-replay.txt`, lines.join("\n"));
		await fs.writeFile(
			`${opts.out}/dba-replay.json`,
			JSON.stringify(rows, null, 1),
		);
	}
	return { rows, lines, trades: trades as ReplayTrade[] };
};
