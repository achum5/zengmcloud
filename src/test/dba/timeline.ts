// THE DBA REPLAY, part 1: the owner's league as it stood at any moment, read
// off its export (docs/smart-cpu.md, yardstick C; "The DBA replay").
//
// Pure: no game code, only the export's JSON. Everything is keyed by event id
// (eid), the one clock the export keeps: a moment is "just before event eid",
// with every event before it applied.
//
// What each piece comes from, and how close it gets:
//
// - Who is on which team: every draft, signing, trade, release and retirement
//   is an event with an eid, so rosters are exact to the event - except that
//   contracts running out make no event: a player whose contract ended last
//   season (or ends this one, once free agency opens) is a free agent unless
//   a later contract or event says otherwise.
// - Ratings: one row a season. In-season progression changes that row in
//   place, so the row is where he ended the season (or is now, for the
//   current one), not where he stood on the day. The biggest blind spot.
// - Contracts: p.contracts, each placed in time by its event (signings),
//   the draft (rookie deals), or the offseason before it starts.
// - Picks: who owns each, from its original team through every trade.
// - The day in the season: from how many injury events came before (they
//   come at an even rate, game by game); a team's record on that day is its
//   final record pro-rated, since game results aren't kept.
// - Injuries: injured/healed events; games left estimated the same way.
// - Box score (vanilla's value blends in PER): the season's rows, minutes
//   scaled to the day, PER as at the end of the season.

export const FREE_AGENT = -1;
export const UNDRAFTED = -2;
export const RETIRED = -3;
// Not in the league at that moment (not yet a prospect, or gone).
export const ABSENT = -99;

export const PHASE = {
	PRESEASON: 0,
	REGULAR_SEASON: 1,
	AFTER_TRADE_DEADLINE: 2,
	PLAYOFFS: 3,
	DRAFT_LOTTERY: 4,
	DRAFT: 5,
	AFTER_DRAFT: 6,
	RESIGN_PLAYERS: 7,
	FREE_AGENCY: 8,
} as const;

export type Moment = {
	// Just before this event: every event with a smaller eid has happened.
	eid: number;
	season: number;
	phase: number;
	// Regular-season games played by then, per team (0-numGames).
	games: number;
};

export type TradeSide = {
	tid: number;
	// What this team RECEIVES (the export's teams[i].assets).
	pids: number[];
	dpids: number[];
};

export type ReplayTrade = {
	eid: number;
	season: number;
	phase: number;
	// The export's order: for a human's trade the human first; for a CPU
	// trade the team that started it (vanilla's betweenAiTeams teams[0]).
	sides: [TradeSide, TradeSide];
	kind: "HC" | "CC" | "HH";
	human?: number;
	cpu?: number;
	moment: Moment;
	// Games played by then, from the traded players' minutes with the team
	// that gave them up (a check on the injury clock), when one was there
	// all season.
	gamesFromMinutes?: number;
	names: Record<number, string>;
	// The fork's CPU trade engines of 2026-07 to 10 noted why they traded;
	// stock trades have none.
	aiTrade?: unknown;
};

export type PickInfo = {
	dpid: number;
	season: number;
	round: number;
	originalTid: number;
	// Pick in the round, once known (after the lottery).
	pick: number;
	// The draft event that used it, and whom.
	usedEid?: number;
	usedBy?: number;
	owners: { eid: number; tid: number }[];
};

type SeasonMarks = {
	season: number;
	firstEid: number;
	lastEid: number;
	// First regular-season game (the first injury), end of the regular
	// season (the playoffs open), the lottery, the draft, free agency.
	firstGameEid: number;
	regularEndEid: number;
	lotteryEid: number;
	draftStartEid: number;
	draftEndEid: number;
	faStartEid: number;
	// Regular-season injury events in order, and how many come a game.
	injuryEids: number[];
	injuriesPerGame: number;
	numGames: number;
};

const bisectLeft = (xs: number[], x: number) => {
	let lo = 0;
	let hi = xs.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (xs[mid]! < x) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	return lo;
};

const contractExp = (c: { start: number; amounts: number[] }) =>
	c.start + c.amounts.length - 1;

const capFromText = (text: string) => {
	const m = /to \$([\d.]+)M/.exec(text);
	return m ? Math.round(Number(m[1]) * 1000) : undefined;
};

export class Timeline {
	data: any;
	players = new Map<number, any>();
	humans: number[];
	numGames: number;
	startingSeason: number;
	currentSeason: number;
	seasons = new Map<number, SeasonMarks>();
	// Each player's moves: to a team, to free agency, into retirement.
	moves = new Map<number, { eid: number; tid: number }[]>();
	initialTid = new Map<number, number>();
	draftEid = new Map<number, number>();
	// When each of a player's contracts was signed (aligned with p.contracts).
	contractTimes = new Map<number, number[]>();
	injuries = new Map<number, { eid: number; out: number; healed: number }[]>();
	picks = new Map<number, PickInfo>();
	trades: ReplayTrade[] = [];
	caps: { eid: number; cap: number }[] = [];
	// Draft classes in the league at once: this season's and the next ones,
	// this many in all (stock generates three; the export now holds eight,
	// and seven matches vanilla's answers best). Which classes existed back
	// then isn't kept, and prospects' ratings only as they were in their
	// draft year.
	prospectClasses = 7;
	// Final (or current) record by season and team.
	records = new Map<string, any>();

	constructor(data: any) {
		this.data = data;
		const ga = data.gameAttributes;
		const unwrap = (x: any) =>
			Array.isArray(x) && x[0] && typeof x[0] === "object" && "start" in x[0]
				? x.at(-1).value
				: x;
		this.humans = unwrap(ga.userTids);
		this.numGames = unwrap(ga.numGames);
		this.startingSeason = data.startingSeason ?? unwrap(ga.startingSeason);
		this.currentSeason = unwrap(ga.season);
		for (const p of data.players) {
			this.players.set(p.pid, p);
		}
		for (const t of data.teams) {
			for (const ts of t.seasons) {
				this.records.set(`${ts.season}:${t.tid}`, ts);
			}
		}
		const events = [...data.events].sort((a: any, b: any) => a.eid - b.eid);
		this.buildSeasons(events);
		this.buildMoves(events);
		this.buildContracts(events);
		this.buildInjuries(events);
		this.buildPicks(events);
		this.buildTrades(events);
	}

	private buildSeasons(events: any[]) {
		const bySeason = Map.groupBy(events, (e) => e.season as number);
		for (const [season, list] of bySeason) {
			const of = (type: string) =>
				list.filter((e) => e.type === type).map((e) => e.eid as number);
			// The playoffs open with "made the playoffs" (the first awards,
			// the All-Stars, come at 70% of the regular season).
			const madePlayoffs = of("madePlayoffs");
			const lottery = of("draftLottery");
			const drafts = of("draft");
			const reSigned = of("reSigned");
			const injured = of("injured");
			const lastEid = list.at(-1)!.eid;
			const regularEndEid =
				madePlayoffs.length > 0 ? madePlayoffs[0]! : Infinity;
			const draftEndEid = drafts.length > 0 ? drafts.at(-1)! : Infinity;
			const injuryEids = injured.filter((eid) => eid < regularEndEid);
			let numGames = this.numGames;
			if (regularEndEid === Infinity) {
				// The season being played: the games so far.
				let gp = 0;
				let n = 0;
				for (const t of this.data.teams) {
					const ts = this.records.get(`${season}:${t.tid}`);
					if (ts) {
						gp += ts.won + ts.lost + (ts.tied ?? 0) + (ts.otl ?? 0);
						n += 1;
					}
				}
				numGames = n > 0 ? gp / n : 0;
			}
			// CPU re-signings come as free agency opens.
			const faStartEid =
				reSigned.length > 0
					? Math.max(
							...reSigned.filter((eid) => eid > draftEndEid),
							draftEndEid,
						)
					: draftEndEid;
			this.seasons.set(season, {
				season,
				firstEid: list[0]!.eid,
				lastEid,
				firstGameEid: injuryEids[0] ?? list[0]!.eid,
				regularEndEid,
				lotteryEid: lottery.length > 0 ? lottery[0]! : draftEndEid,
				draftStartEid: drafts.length > 0 ? drafts[0]! : Infinity,
				draftEndEid,
				faStartEid,
				injuryEids,
				injuriesPerGame: numGames > 0 ? injuryEids.length / numGames : 1,
				numGames,
			});
		}
		for (const e of events) {
			if (e.type === "gameAttribute" && /Salary cap/.test(e.text ?? "")) {
				const cap = capFromText(e.text);
				if (cap !== undefined) {
					if (this.caps.length === 0) {
						const m = /from \$([\d.]+)M/.exec(e.text);
						if (m) {
							this.caps.push({
								eid: -Infinity,
								cap: Math.round(Number(m[1]) * 1000),
							});
						}
					}
					this.caps.push({ eid: e.eid, cap });
				}
			}
		}
	}

	private addMove(pid: number, eid: number, tid: number) {
		const list = this.moves.get(pid) ?? [];
		list.push({ eid, tid });
		this.moves.set(pid, list);
	}

	private buildMoves(events: any[]) {
		for (const p of this.data.players) {
			const first = p.transactions?.[0];
			if (
				first &&
				first.type === "draft" &&
				first.eid === undefined &&
				first.season <= this.startingSeason &&
				first.phase < 0
			) {
				// In the league from the start (a real player's team at creation).
				this.initialTid.set(p.pid, first.tid);
			}
		}
		for (const e of events) {
			switch (e.type) {
				case "draft":
				case "freeAgent":
					if (e.pids?.length === 1 && e.tids?.length >= 1) {
						this.addMove(e.pids[0], e.eid, e.tids[0]);
						if (e.type === "draft") {
							this.draftEid.set(e.pids[0], e.eid);
						}
					}
					break;
				case "release":
					for (const pid of e.pids ?? []) {
						this.addMove(pid, e.eid, FREE_AGENT);
					}
					break;
				case "retired":
					for (const pid of e.pids ?? []) {
						this.addMove(pid, e.eid, RETIRED);
					}
					break;
				case "trade":
					for (const [i, side] of e.teams.entries()) {
						for (const a of side.assets) {
							if (a.pid !== undefined) {
								this.addMove(a.pid, e.eid, e.tids[i]);
							}
						}
					}
					break;
			}
		}
	}

	private buildContracts(events: any[]) {
		// Signings by player, to place contracts without an eid.
		const signings = new Map<
			number,
			{ eid: number; exp: number; season: number }[]
		>();
		for (const e of events) {
			if ((e.type === "reSigned" || e.type === "freeAgent") && e.contract) {
				for (const pid of e.pids ?? []) {
					const list = signings.get(pid) ?? [];
					list.push({ eid: e.eid, exp: e.contract.exp, season: e.season });
					signings.set(pid, list);
				}
			}
		}
		for (const p of this.data.players) {
			const times: number[] = [];
			for (const c of p.contracts ?? []) {
				let time: number;
				if (c.eid !== undefined) {
					time = c.eid;
				} else if (c.type === "rookie" && this.draftEid.has(p.pid)) {
					time = this.draftEid.get(p.pid)!;
				} else {
					const match = (signings.get(p.pid) ?? []).find(
						(s) => s.exp === contractExp(c) && s.season <= c.start,
					);
					if (match) {
						time = match.eid;
					} else if (c.start <= this.startingSeason) {
						time = -Infinity;
					} else {
						// Signed in the offseason before it starts.
						const prev = this.seasons.get(c.start - 1);
						time = prev ? prev.faStartEid : -Infinity;
					}
				}
				times.push(time);
			}
			this.contractTimes.set(p.pid, times);
		}
	}

	private buildInjuries(events: any[]) {
		const open = new Map<
			number,
			{ eid: number; out: number; healed: number }
		>();
		for (const e of events) {
			if (e.type === "injured" && e.pids?.length === 1) {
				const m = /out for (\d+) game/.exec(e.text ?? "");
				const row = { eid: e.eid, out: m ? Number(m[1]) : 1, healed: Infinity };
				const list = this.injuries.get(e.pids[0]) ?? [];
				list.push(row);
				this.injuries.set(e.pids[0], list);
				open.set(e.pids[0], row);
			} else if (e.type === "healed" && e.pids?.length === 1) {
				const row = open.get(e.pids[0]);
				if (row) {
					row.healed = e.eid;
					open.delete(e.pids[0]);
				}
			}
		}
	}

	private pickInfo(
		dpid: number,
		season: number,
		round: number,
		originalTid: number,
	) {
		let info = this.picks.get(dpid);
		if (!info) {
			info = { dpid, season, round, originalTid, pick: 0, owners: [] };
			this.picks.set(dpid, info);
		}
		return info;
	}

	private buildPicks(events: any[]) {
		for (const dp of this.data.draftPicks) {
			if (typeof dp.season === "number") {
				const info = this.pickInfo(
					dp.dpid,
					dp.season,
					dp.round,
					dp.originalTid,
				);
				info.pick = dp.pick;
			}
		}
		for (const p of this.data.players) {
			const d = p.draft;
			if (d && d.dpid !== undefined && d.round > 0) {
				const info = this.pickInfo(d.dpid, d.year, d.round, d.originalTid);
				info.pick = d.pick;
				info.usedBy = p.pid;
				info.usedEid = this.draftEid.get(p.pid);
			}
		}
		for (const e of events) {
			if (e.type !== "trade") {
				continue;
			}
			for (const [i, side] of e.teams.entries()) {
				for (const a of side.assets) {
					if (a.dpid !== undefined) {
						const info = this.pickInfo(
							a.dpid,
							a.season,
							a.round,
							a.originalTid,
						);
						info.owners.push({ eid: e.eid, tid: e.tids[i] });
					}
				}
			}
		}
	}

	private buildTrades(events: any[]) {
		for (const e of events) {
			if (e.type !== "trade" || e.tids.length !== 2) {
				continue;
			}
			const humans = e.tids.filter((tid: number) => this.humans.includes(tid));
			const kind =
				humans.length === 0 ? "CC" : humans.length === 1 ? "HC" : "HH";
			const names: Record<number, string> = {};
			const sides = e.teams.map((side: any, i: number) => {
				const pids: number[] = [];
				const dpids: number[] = [];
				for (const a of side.assets) {
					if (a.pid !== undefined) {
						pids.push(a.pid);
						names[a.pid] = a.name;
					} else if (a.dpid !== undefined) {
						dpids.push(a.dpid);
					}
				}
				return { tid: e.tids[i], pids, dpids };
			}) as [TradeSide, TradeSide];
			const moment = this.momentAt(e.eid, e.season, e.phase);
			// The traded players' minutes with the team that gave them up, for
			// one who was there from the season's start.
			let gamesFromMinutes: number | undefined;
			if (e.phase === PHASE.REGULAR_SEASON) {
				for (const [i, side] of e.teams.entries()) {
					const from = e.tids[1 - i];
					for (const a of side.assets) {
						if (a.pid === undefined) {
							continue;
						}
						const p = this.players.get(a.pid);
						// statsIndex is his new team's row; the one before, his old
						// team's.
						const row = p?.stats?.[a.statsIndex - 1];
						const firstRow = p?.stats?.find(
							(s: any) => s.season === e.season && !s.playoffs,
						);
						if (
							row &&
							row === firstRow &&
							row.tid === from &&
							row.minAvailable !== undefined
						) {
							const g = row.minAvailable / 48.4;
							gamesFromMinutes = Math.max(gamesFromMinutes ?? 0, g);
						}
					}
				}
			}
			this.trades.push({
				eid: e.eid,
				season: e.season,
				phase: e.phase,
				sides,
				kind,
				human: kind === "HC" ? humans[0] : undefined,
				cpu:
					kind === "HC"
						? e.tids.find((tid: number) => !this.humans.includes(tid))
						: undefined,
				moment,
				gamesFromMinutes,
				names,
				aiTrade: e.aiTrade,
			});
		}
	}

	// Regular-season games played by this event, per team.
	gamesAt(season: number, eid: number, phase: number) {
		const marks = this.seasons.get(season);
		if (!marks || phase <= PHASE.PRESEASON) {
			return 0;
		}
		if (phase >= PHASE.PLAYOFFS || eid >= marks.regularEndEid) {
			return marks.numGames;
		}
		const n = bisectLeft(marks.injuryEids, eid);
		return Math.min(marks.numGames, n / marks.injuriesPerGame);
	}

	momentAt(eid: number, season: number, phase: number): Moment {
		return { eid, season, phase, games: this.gamesAt(season, eid, phase) };
	}

	// The opening day of a season's regular season.
	seasonStart(season: number): Moment | undefined {
		const marks = this.seasons.get(season);
		if (!marks) {
			return undefined;
		}
		return {
			eid: marks.firstGameEid,
			season,
			phase: PHASE.REGULAR_SEASON,
			games: 0,
		};
	}

	// The trade deadline (60% of the season): just after the last game before it.
	deadline(season: number, share = 0.6): Moment | undefined {
		const marks = this.seasons.get(season);
		if (!marks || marks.regularEndEid === Infinity) {
			return undefined;
		}
		const i = Math.floor(share * this.numGames * marks.injuriesPerGame);
		const eid =
			marks.injuryEids[Math.min(i, marks.injuryEids.length - 1)] ??
			marks.firstGameEid;
		return {
			eid,
			season,
			phase: PHASE.REGULAR_SEASON,
			games: share * this.numGames,
		};
	}

	// When the CPU reviews its strategy (vanilla's team.updateStrategies, in
	// newPhaseBeforeDraft): after the playoffs and retirements, before the
	// lottery.
	beforeDraft(season: number): Moment | undefined {
		const marks = this.seasons.get(season);
		if (!marks || marks.regularEndEid === Infinity) {
			return undefined;
		}
		return {
			eid: marks.lotteryEid,
			season,
			phase: PHASE.DRAFT_LOTTERY,
			games: marks.numGames,
		};
	}

	salaryCapAt(eid: number) {
		let cap = this.caps[0]?.cap ?? 90000;
		for (const c of this.caps) {
			if (c.eid < eid) {
				cap = c.cap;
			}
		}
		return cap;
	}

	// The contract he's under at that moment, and its index in p.contracts.
	contractAt(pid: number, m: Moment) {
		const p = this.players.get(pid);
		const times = this.contractTimes.get(pid) ?? [];
		// The last signed, not the last in the list: the list isn't always in
		// order (a re-signing ended early can come after the deal that
		// followed it).
		let index = -1;
		for (const [i, time] of times.entries()) {
			if (time < m.eid && (index < 0 || time >= times[index]!)) {
				index = i;
			}
		}
		return index >= 0 ? { index, contract: p.contracts[index] } : undefined;
	}

	// Where the player was at that moment: a team, free agency, a prospect,
	// retired, or not in the league.
	tidAt(pid: number, m: Moment): number {
		const p = this.players.get(pid);
		if (!p) {
			return ABSENT;
		}
		let tid: number | undefined = this.initialTid.get(pid);
		for (const move of this.moves.get(pid) ?? []) {
			if (move.eid < m.eid) {
				tid = move.tid;
			} else {
				break;
			}
		}
		if (tid === RETIRED) {
			return RETIRED;
		}
		const inLeague = p.ratings.some((r: any) => r.season === m.season);
		if (tid === undefined) {
			// Never drafted or signed yet: a prospect for a coming draft (see
			// prospectClasses), else a free agent if he was in the league
			// this season, else not there.
			const year = p.draft?.year;
			if (
				typeof year === "number" &&
				(year > m.season ||
					(year === m.season &&
						m.eid <= (this.seasons.get(year)?.draftEndEid ?? Infinity)))
			) {
				const after =
					m.eid > (this.seasons.get(m.season)?.draftEndEid ?? Infinity) ? 1 : 0;
				return year < m.season + after + this.prospectClasses
					? UNDRAFTED
					: ABSENT;
			}
			return inLeague ? FREE_AGENT : ABSENT;
		}
		if (!inLeague) {
			// Out of the league this season (retired without an event, or gone
			// abroad and back).
			return p.ratings.some((r: any) => r.season > m.season)
				? FREE_AGENT
				: ABSENT;
		}
		if (tid >= 0) {
			// A contract that ran out with no new one: a free agent.
			const c = this.contractAt(pid, m);
			if (c) {
				const exp = contractExp(c.contract);
				if (
					exp < m.season ||
					(exp === m.season &&
						m.phase >= PHASE.FREE_AGENCY &&
						m.eid > (this.seasons.get(m.season)?.faStartEid ?? Infinity))
				) {
					return FREE_AGENT;
				}
			}
		}
		return tid;
	}

	// Who owned the pick then, if it still existed.
	pickOwnerAt(dpid: number, m: Moment): number | undefined {
		const info = this.picks.get(dpid);
		if (!info) {
			return undefined;
		}
		if (info.usedEid !== undefined && info.usedEid < m.eid) {
			return undefined;
		}
		if (info.season < m.season) {
			return undefined;
		}
		// Picks run seven drafts out (numSeasonsFutureDraftPicks); the new
		// season's come once this season's draft is done.
		const draftDone =
			m.eid > (this.seasons.get(m.season)?.draftEndEid ?? Infinity);
		if (info.season > m.season + (draftDone ? 7 : 6)) {
			return undefined;
		}
		if (info.season === m.season && draftDone) {
			return undefined;
		}
		let tid = info.originalTid;
		for (const o of info.owners) {
			if (o.eid < m.eid) {
				tid = o.tid;
			}
		}
		return tid;
	}

	// The pick in the round, once the lottery has set it.
	pickNumberAt(dpid: number, m: Moment) {
		const info = this.picks.get(dpid)!;
		if (
			info.season === m.season &&
			m.phase >= PHASE.DRAFT_LOTTERY &&
			m.eid > (this.seasons.get(m.season)?.lotteryEid ?? Infinity)
		) {
			return info.pick;
		}
		return 0;
	}

	// Injury on that day, with the games he still had to miss.
	injuryAt(pid: number, m: Moment) {
		for (const row of this.injuries.get(pid) ?? []) {
			if (row.eid < m.eid && row.healed > m.eid) {
				const marks = this.seasons.get(m.season);
				let elapsed = 0;
				if (marks) {
					const a = bisectLeft(marks.injuryEids, row.eid);
					const b = bisectLeft(marks.injuryEids, m.eid);
					elapsed = Math.max(0, b - a) / marks.injuriesPerGame;
				}
				const gamesRemaining = Math.max(1, Math.round(row.out - elapsed));
				return { type: "Injured", gamesRemaining };
			}
		}
		return { type: "Healthy", gamesRemaining: 0 };
	}

	// A team's record that day: the season's final record, pro-rated.
	recordAt(tid: number, m: Moment) {
		const ts = this.records.get(`${m.season}:${tid}`);
		if (!ts) {
			return undefined;
		}
		const gp = ts.won + ts.lost + (ts.tied ?? 0) + (ts.otl ?? 0);
		const share = gp > 0 ? Math.min(1, m.games / gp) : 0;
		const won = Math.round(ts.won * share);
		const games = Math.round(Math.min(gp, m.games));
		return { ts, won, lost: Math.max(0, games - won) };
	}

	// Final standings of a season, best first.
	standings(season: number) {
		const rows: { tid: number; winp: number; won: number; lost: number }[] = [];
		for (const t of this.data.teams) {
			const ts = this.records.get(`${season}:${t.tid}`);
			if (ts && !t.disabled) {
				const gp = ts.won + ts.lost;
				rows.push({
					tid: t.tid,
					winp: gp > 0 ? ts.won / gp : 0,
					won: ts.won,
					lost: ts.lost,
				});
			}
		}
		return rows.sort((a, b) => b.winp - a.winp);
	}

	champion(season: number) {
		for (const t of this.data.teams) {
			const ts = this.records.get(`${season}:${t.tid}`);
			if (ts && ts.playoffRoundsWon === 4) {
				return t.tid as number;
			}
		}
		return undefined;
	}

	abbrev(tid: number) {
		return (
			this.data.teams.find((t: any) => t.tid === tid)?.abbrev ?? String(tid)
		);
	}

	name(pid: number) {
		const p = this.players.get(pid);
		return p ? `${p.firstName} ${p.lastName}` : String(pid);
	}

	// Checks of the rebuild against what the export itself says.
	check(from: number) {
		const out: string[] = [];
		// Every traded player was on the team giving him up, every pick with
		// the team giving it.
		let assets = 0;
		let misplaced = 0;
		const examples: string[] = [];
		for (const t of this.trades) {
			if (t.season < from) {
				continue;
			}
			for (const [i, side] of t.sides.entries()) {
				const giver = t.sides[1 - i]!.tid;
				for (const pid of side.pids) {
					assets += 1;
					const tid = this.tidAt(pid, t.moment);
					if (tid !== giver) {
						misplaced += 1;
						if (examples.length < 8) {
							examples.push(
								`${t.season} eid ${t.eid}: ${this.name(pid)} at ${tid} not ${this.abbrev(giver)}`,
							);
						}
					}
				}
				for (const dpid of side.dpids) {
					assets += 1;
					const tid = this.pickOwnerAt(dpid, t.moment);
					if (tid !== giver) {
						misplaced += 1;
						if (examples.length < 8) {
							examples.push(
								`${t.season} eid ${t.eid}: pick ${dpid} with ${tid} not ${this.abbrev(giver)}`,
							);
						}
					}
				}
			}
		}
		out.push(
			`traded assets where the rebuild had them, ${from} on: ${assets - misplaced}/${assets}${examples.length > 0 ? ` (misses: ${examples.join("; ")})` : ""}`,
		);
		// Now: every player's team in the rebuild against the export's.
		const now: Moment = {
			eid: Infinity,
			season: this.currentSeason,
			phase: this.data.gameAttributes.phase,
			games: this.gamesAt(
				this.currentSeason,
				Infinity,
				this.data.gameAttributes.phase,
			),
		};
		let n = 0;
		let wrong = 0;
		const wrongEx: string[] = [];
		for (const p of this.data.players) {
			if (p.tid < FREE_AGENT) {
				continue;
			}
			n += 1;
			const tid = this.tidAt(p.pid, now);
			if (tid !== p.tid) {
				wrong += 1;
				if (wrongEx.length < 6) {
					wrongEx.push(`${this.name(p.pid)} ${tid} not ${p.tid}`);
				}
			}
		}
		out.push(
			`players on a team or in free agency now, rebuilt to the right place: ${n - wrong}/${n}${wrongEx.length > 0 ? ` (${wrongEx.join("; ")})` : ""}`,
		);
		// Each season's pay: the team that paid him for a season (contracts'
		// payments) had him on opening day or at the deadline.
		let paid = 0;
		let found = 0;
		for (let season = from; season < this.currentSeason; season++) {
			const start = this.seasonStart(season);
			const deadline = this.deadline(season);
			if (!start || !deadline) {
				continue;
			}
			for (const p of this.data.players) {
				for (const c of p.contracts ?? []) {
					for (const pay of c.payments ?? []) {
						if (pay.season === season && pay.tid >= 0 && pay.amount > 0) {
							paid += 1;
							const a = this.tidAt(p.pid, start);
							const b = this.tidAt(p.pid, deadline);
							if (a === pay.tid || b === pay.tid) {
								found += 1;
							}
						}
					}
				}
			}
		}
		out.push(
			`season pay (contract payments) by a team the rebuild had him on, opening day or deadline, ${from}-${this.currentSeason - 1}: ${found}/${paid}`,
		);
		// The injury clock against the traded players' minutes.
		const diffs: number[] = [];
		for (const t of this.trades) {
			if (
				t.season >= from &&
				t.gamesFromMinutes !== undefined &&
				t.season < this.currentSeason
			) {
				diffs.push(t.moment.games - t.gamesFromMinutes);
			}
		}
		if (diffs.length > 0) {
			const mean = diffs.reduce((a, b) => a + b, 0) / diffs.length;
			const mad = diffs.reduce((a, b) => a + Math.abs(b), 0) / diffs.length;
			out.push(
				`day of the season: injury clock minus traded players' minutes, ${diffs.length} in-season trades: mean ${mean.toFixed(1)} games, mean absolute ${mad.toFixed(1)}`,
			);
		}
		// Picks: the rebuilt owner when each was used, against the drafting team.
		let used = 0;
		let right = 0;
		for (const info of this.picks.values()) {
			if (
				info.usedEid !== undefined &&
				info.usedBy !== undefined &&
				info.season >= from
			) {
				used += 1;
				const owner = this.pickOwnerAt(info.dpid, {
					eid: info.usedEid,
					season: info.season,
					phase: PHASE.DRAFT,
					games: this.numGames,
				});
				if (owner === this.players.get(info.usedBy)?.draft?.tid) {
					right += 1;
				}
			}
		}
		out.push(
			`picks used ${from} on, rebuilt owner = drafting team: ${right}/${used}`,
		);
		return out;
	}
}
