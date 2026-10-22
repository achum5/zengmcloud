import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { PHASE, PLAYER } from "../../common/constants.ts";
import type { SeasonSnapshot } from "./runLeague.ts";

// Everything one arena league produced, reduced to numbers that can be pooled
// across leagues. Lists (rather than means) wherever a median or a share is
// wanted at the end, so pooling stays honest.
export type LeagueMetrics = {
	numSeasons: number;
	winsPerTeamSeason: number[];
	winSdPerSeason: number[];
	yoyPairs: [number, number][];
	teams60PerSeason: number[];
	under25PerSeason: number[];
	champions: number[];
	bestRecordWonTitle: boolean[];
	championWins: number[];
	middleFourthPlus: number;
	middleTeamSeasons: number;
	rebuildLengths: number[];
	rebuildsOpen: number;
	ageTop4: number[];
	ageBottom5: number[];
	continuity: number[];
	tradesPerSeason: number[];
	inSeasonTradesPerSeason: number[];
	offseasonTradesPerSeason: number[];
	deadlineWeekShare: number[];
	playersPerTrade: number[];
	tradesWithFirst: boolean[];
	firstsPerSeason: number[];
	starMovesPerSeason: number[];
	starMovesByTrade: number;
	starMovesByFreeAgency: number;
	tradeHindsightGaps: number[];
	taxpayersPerSeason: number[];
	capSpaceTeamsPerSummer: number[];
	deadMoneyShares: number[];
	top10FaStayed: number;
	top10FaLeft: number;
	top10FaUnsigned: number;
	// The same top 10, veterans only: not coming off a rookie deal.
	top10VetFas: number;
	top10VetFaStayed: number;
};

const OFFSEASON_PHASES = new Set<number>([
	PHASE.DRAFT_LOTTERY,
	PHASE.DRAFT,
	PHASE.AFTER_DRAFT,
	PHASE.RESIGN_PLAYERS,
	PHASE.FREE_AGENCY,
]);

// Trades in the preseason count as offseason ones too: no games have been
// played, and the NBA counts training camp trades that way. Stock and the
// default market make none there.
const OFFSEASON_TRADE_PHASES = new Set<number>([
	PHASE.PRESEASON,
	...OFFSEASON_PHASES,
]);

const sd = (xs: number[]) => {
	const mean = xs.reduce((a, x) => a + x, 0) / xs.length;
	return Math.sqrt(xs.reduce((a, x) => a + (x - mean) ** 2, 0) / xs.length);
};

const ws = (row: any) => (row.ows ?? 0) + (row.dws ?? 0);

export const computeMetrics = async (
	snapshots: SeasonSnapshot[],
): Promise<LeagueMetrics> => {
	await idb.cache.flush();
	const seasons = snapshots.map((s) => s.season);
	const seasonSet = new Set(seasons);
	const firstSeason = seasons[0]!;
	const lastSeason = seasons.at(-1)!;

	const teamSeasons = (await idb.league.getAll("teamSeasons")).filter((ts) =>
		seasonSet.has(ts.season),
	);
	const players = await idb.league.getAll("players");
	const playersByPid = new Map(players.map((p) => [p.pid, p]));
	const statsRows = players.flatMap((p) =>
		p.stats
			.filter((row) => !row.playoffs && row.season >= firstSeason - 1)
			.map((row) => ({ ...row, pid: p.pid })),
	);
	const events = await idb.league.getAll("events");
	const allStars = await idb.league.getAll("allStars");

	const numRounds = g.get("numGamesPlayoffSeries", "current").length;

	// Wins scaled to 82 games, by season then tid.
	const winsBy = new Map<number, Map<number, number>>();
	const playoffsBy = new Map<number, Map<number, number>>();
	for (const ts of teamSeasons) {
		const gp = ts.won + ts.lost + (ts.tied ?? 0) + (ts.otl ?? 0);
		if (gp === 0) {
			continue;
		}
		if (!winsBy.has(ts.season)) {
			winsBy.set(ts.season, new Map());
			playoffsBy.set(ts.season, new Map());
		}
		winsBy.get(ts.season)!.set(ts.tid, (82 * ts.won) / gp);
		playoffsBy.get(ts.season)!.set(ts.tid, ts.playoffRoundsWon);
	}

	const m: LeagueMetrics = {
		numSeasons: seasons.length,
		winsPerTeamSeason: [],
		winSdPerSeason: [],
		yoyPairs: [],
		teams60PerSeason: [],
		under25PerSeason: [],
		champions: [],
		bestRecordWonTitle: [],
		championWins: [],
		middleFourthPlus: 0,
		middleTeamSeasons: 0,
		rebuildLengths: [],
		rebuildsOpen: 0,
		ageTop4: [],
		ageBottom5: [],
		continuity: [],
		tradesPerSeason: [],
		inSeasonTradesPerSeason: [],
		offseasonTradesPerSeason: [],
		deadlineWeekShare: [],
		playersPerTrade: [],
		tradesWithFirst: [],
		firstsPerSeason: [],
		starMovesPerSeason: [],
		starMovesByTrade: 0,
		starMovesByFreeAgency: 0,
		tradeHindsightGaps: [],
		taxpayersPerSeason: [],
		capSpaceTeamsPerSummer: [],
		deadMoneyShares: [],
		top10FaStayed: 0,
		top10FaLeft: 0,
		top10FaUnsigned: 0,
		top10VetFas: 0,
		top10VetFaStayed: 0,
	};

	// WINNING
	for (const s of seasons) {
		const wins = winsBy.get(s);
		if (!wins) {
			continue;
		}
		const list = [...wins.values()];
		m.winsPerTeamSeason.push(...list);
		m.winSdPerSeason.push(sd(list));
		m.teams60PerSeason.push(list.filter((w) => w >= 60).length);
		m.under25PerSeason.push(list.filter((w) => w < 25).length);
		const prev = winsBy.get(s - 1);
		if (prev) {
			for (const [tid, w] of wins) {
				const wPrev = prev.get(tid);
				if (wPrev !== undefined) {
					m.yoyPairs.push([wPrev, w]);
				}
			}
		}
		const champ = [...playoffsBy.get(s)!].find(([, r]) => r === numRounds);
		if (champ) {
			m.champions.push(champ[0]);
			m.championWins.push(wins.get(champ[0])!);
			const best = Math.max(...list);
			m.bestRecordWonTitle.push(wins.get(champ[0])! === best);
		}
	}

	// DIRECTION
	const tids = [...new Set(teamSeasons.map((ts) => ts.tid))];
	for (const tid of tids) {
		let middleStreak = 0;
		let rebuildStart: number | undefined;
		for (const s of seasons) {
			const w = winsBy.get(s)?.get(tid);
			if (w === undefined) {
				continue;
			}
			if (w >= 35 && w <= 47) {
				middleStreak += 1;
				m.middleTeamSeasons += 1;
				if (middleStreak >= 4) {
					m.middleFourthPlus += 1;
				}
			} else {
				middleStreak = 0;
			}

			const madePlayoffs = (playoffsBy.get(s)?.get(tid) ?? -1) >= 0;
			if (rebuildStart !== undefined && madePlayoffs) {
				m.rebuildLengths.push(s - rebuildStart);
				rebuildStart = undefined;
			}
			if (rebuildStart === undefined && w < 82 * 0.4 && !madePlayoffs) {
				rebuildStart = s;
			}
		}
		if (rebuildStart !== undefined) {
			m.rebuildsOpen += 1;
		}
	}

	// Minutes, by season and tid, for age and continuity.
	const minutesBy = new Map<string, { pid: number; min: number }[]>();
	for (const row of statsRows as any[]) {
		const key = `${row.season}:${row.tid}`;
		const list = minutesBy.get(key) ?? [];
		list.push({ pid: row.pid, min: row.min ?? 0 });
		minutesBy.set(key, list);
	}
	for (const s of seasons) {
		const wins = winsBy.get(s);
		if (!wins) {
			continue;
		}
		const ranked = [...wins].sort((a, b) => b[1] - a[1]).map(([tid]) => tid);
		const ageOf = (tid: number) => {
			let num = 0;
			let den = 0;
			for (const { pid, min } of minutesBy.get(`${s}:${tid}`) ?? []) {
				const p = playersByPid.get(pid);
				if (p) {
					num += (s - p.born.year) * min;
					den += min;
				}
			}
			return den > 0 ? num / den : undefined;
		};
		for (const tid of ranked.slice(0, 4)) {
			const age = ageOf(tid);
			if (age !== undefined) {
				m.ageTop4.push(age);
			}
		}
		for (const tid of ranked.slice(-5)) {
			const age = ageOf(tid);
			if (age !== undefined) {
				m.ageBottom5.push(age);
			}
		}
		if (s > firstSeason) {
			for (const tid of ranked) {
				const prevPids = new Set(
					(minutesBy.get(`${s - 1}:${tid}`) ?? []).map((x) => x.pid),
				);
				let returning = 0;
				let total = 0;
				for (const { pid, min } of minutesBy.get(`${s}:${tid}`) ?? []) {
					total += min;
					if (prevPids.has(pid)) {
						returning += min;
					}
				}
				if (total > 0) {
					m.continuity.push(returning / total);
				}
			}
		}
	}

	// TRADES
	const trades = events.filter(
		(e): e is Extract<typeof e, { type: "trade" }> =>
			e.type === "trade" && seasonSet.has(e.season),
	);
	for (const snapshot of snapshots) {
		const s = snapshot.season;
		const inSeason = trades.filter(
			(e) => e.season === s && !OFFSEASON_TRADE_PHASES.has(e.phase as number),
		);
		const offseason = trades.filter(
			(e) => e.season === s && OFFSEASON_TRADE_PHASES.has(e.phase as number),
		);
		m.tradesPerSeason.push(inSeason.length + offseason.length);
		m.inSeasonTradesPerSeason.push(inSeason.length);
		m.offseasonTradesPerSeason.push(offseason.length);
		if (inSeason.length > 0) {
			m.deadlineWeekShare.push(
				(snapshot.tradesAtDeadline - snapshot.tradesBeforeDeadlineWeek) /
					inSeason.length,
			);
		}
		let firsts = 0;
		for (const e of [...inSeason, ...offseason]) {
			const assets = e.teams?.flatMap((t) => t.assets) ?? [];
			m.playersPerTrade.push(assets.filter((a) => "pid" in a).length);
			const numFirsts = assets.filter(
				(a) => "dpid" in a && a.round === 1,
			).length;
			firsts += numFirsts;
			m.tradesWithFirst.push(numFirsts > 0);
		}
		m.firstsPerSeason.push(firsts);
	}

	// Hindsight: what each side got out of the deal over the next three
	// seasons, in win shares - a player's own, from the day he arrived; a
	// pick's, from the player it became. Only deals whose window has closed.
	const draftedBy = new Map<string, number>();
	for (const p of players) {
		if (p.draft.year >= firstSeason && p.draft.round > 0) {
			draftedBy.set(
				`${p.draft.year}:${p.draft.round}:${p.draft.originalTid}`,
				p.pid,
			);
		}
	}
	const wsFor = (pid: number, tid: number, from: number, to: number) =>
		(statsRows as any[])
			.filter(
				(row) =>
					row.pid === pid &&
					row.season >= from &&
					row.season <= to &&
					(row.season > from || row.tid === tid),
			)
			.reduce((sum, row) => sum + Math.max(0, ws(row)), 0);
	for (const e of trades) {
		if (e.season + 2 > lastSeason || !e.teams) {
			continue;
		}
		const sides = e.teams.map((side, k) => {
			let total = 0;
			for (const a of side.assets) {
				if ("pid" in a) {
					total += wsFor(a.pid, e.tids[k]!, e.season, e.season + 2);
				} else if (typeof a.season === "number") {
					if (a.season + 2 > lastSeason) {
						return undefined;
					}
					const pid = draftedBy.get(`${a.season}:${a.round}:${a.originalTid}`);
					if (pid !== undefined) {
						total += wsFor(pid, -999, a.season + 1, a.season + 3);
					}
				}
			}
			return total;
		});
		if (sides[0] !== undefined && sides[1] !== undefined) {
			const sum = sides[0] + sides[1];
			if (sum > 1) {
				m.tradeHindsightGaps.push(Math.abs(sides[0] - sides[1]) / sum);
			}
		}
	}

	// Stars: anyone named an All-Star in the previous three seasons, moving to a
	// new team during the league year.
	const allStarPids = new Map<number, Set<number>>();
	for (const as of allStars) {
		const pids = new Set<number>();
		for (const p of [...as.teams[0], ...as.teams[1]]) {
			pids.add(p.pid);
		}
		allStarPids.set(as.season, pids);
	}
	for (const s of seasons.slice(1)) {
		const stars = new Set<number>();
		for (const y of [s - 3, s - 2, s - 1]) {
			for (const pid of allStarPids.get(y) ?? []) {
				stars.add(pid);
			}
		}
		let moves = 0;
		for (const pid of stars) {
			const p = playersByPid.get(pid);
			for (const t of p?.transactions ?? []) {
				const inYear =
					(t.season === s - 1 && OFFSEASON_PHASES.has(t.phase)) ||
					(t.season === s && !OFFSEASON_PHASES.has(t.phase));
				if (!inYear) {
					continue;
				}
				if (t.type === "trade") {
					moves += 1;
					m.starMovesByTrade += 1;
				} else if (t.type === "freeAgent") {
					moves += 1;
					m.starMovesByFreeAgency += 1;
				}
			}
		}
		m.starMovesPerSeason.push(moves);
	}

	// CAP & ROSTER
	for (const snapshot of snapshots) {
		if (snapshot.payrolls.length > 0) {
			m.taxpayersPerSeason.push(
				snapshot.payrolls.filter((p) => p > snapshot.luxuryPayroll).length,
			);
			for (const [tid, payroll] of snapshot.payrolls.entries()) {
				if (payroll > 0) {
					m.deadMoneyShares.push(snapshot.deadMoney[tid]! / payroll);
				}
			}
		}
		if (snapshot.capSpace.length > 0) {
			m.capSpaceTeamsPerSummer.push(
				snapshot.capSpace.filter((share) => share >= 0.15).length,
			);
		}
		const top10 = [...snapshot.expiring]
			.filter((x) => x.tidAfter !== undefined && x.tidAfter !== PLAYER.RETIRED)
			.sort((a, b) => b.value - a.value)
			.slice(0, 10);
		for (const x of top10) {
			if (x.tidAfter === x.tid) {
				m.top10FaStayed += 1;
			} else if (x.tidAfter === PLAYER.FREE_AGENT) {
				m.top10FaUnsigned += 1;
			} else {
				m.top10FaLeft += 1;
			}
			if (!x.rookie) {
				m.top10VetFas += 1;
				if (x.tidAfter === x.tid) {
					m.top10VetFaStayed += 1;
				}
			}
		}
	}

	return m;
};
