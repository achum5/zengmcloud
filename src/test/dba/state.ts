// THE DBA REPLAY, part 2: the test league set to a moment of the owner's
// league (timeline.ts), so the game's own code - vanilla's trade values, the
// realistic mode's, any valuation to come - can be asked about it.
//
// The league is loaded once (the export, or its slim copy: only its teams,
// settings and current season matter) and then, moment by moment, its cache
// is rewritten: every player in the league then, with the ratings, box score,
// contract, injury and team he had; every pick with its owner then; each
// team's record that day and vanilla's strategy for it; the season, phase
// and salary cap. Nothing is written to the database.

import { PLAYER } from "../../common/constants.ts";
import type { Player, Team } from "../../common/types.ts";
import { league, player, team } from "../../worker/core/index.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers, local } from "../../worker/util/index.ts";
import { getDefaultSettings } from "../../worker/views/newLeague.ts";
import {
	ABSENT,
	FREE_AGENT,
	PHASE,
	RETIRED,
	UNDRAFTED,
	type Moment,
	type Timeline,
} from "./timeline.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const unwrap = (x: unknown) =>
	Array.isArray(x) &&
	x.length > 0 &&
	typeof x[0] === "object" &&
	x[0] &&
	"start" in x[0]
		? (x as { value: unknown }[]).at(-1)!.value
		: x;

// The export into the test league (fake-indexeddb), as
// src/test/owner/ownerLeague.test.ts loads it.
export const loadLeague = async (file: string) => {
	const { makeSeededRandom } = await import("../../common/random.ts");
	Math.random = makeSeededRandom(1);
	const fs = await importNode("node:fs/promises");
	const data = JSON.parse(await fs.readFile(file, "utf8"));
	const ga = data.gameAttributes;
	const settings: Record<string, unknown> =
		helpers.deepCopy(getDefaultSettings());
	for (const key of Object.keys(settings)) {
		if (Object.hasOwn(ga, key)) {
			settings[key] = unwrap(ga[key]);
		}
	}
	(idb as any).league = undefined;
	await league.createStream(data, {
		confs: unwrap(ga.confs),
		divs: unwrap(ga.divs),
		fromFile: {
			gameAttributes: ga,
			hasRookieContracts: true,
			maxGid: undefined,
			startingSeason: data.startingSeason ?? unwrap(ga.startingSeason),
			teams: data.teams,
			version: data.version,
		},
		getLeagueOptions: undefined,
		keptKeys: new Set([...Object.keys(data), "startingSeason", "version"]),
		lid: 79,
		name: "DBA replay",
		setLeagueCreationStatus: () => {},
		settings,
		shuffleRosters: false,
		startingSeasonFromInput: undefined,
		teamsFromInput: data.teams,
		tid: 0,
	} as any);
	await league.loadGameAttributes();
	// The replay rebuilds each moment in the cache alone (team seasons under
	// made-up row ids among them); saving that to the database clashes with
	// the export's own rows. Nothing here reads the database back.
	idb.cache.stopAutoFlush();
};

const contractExp = (c: { start: number; amounts: number[] }) =>
	c.start + c.amounts.length - 1;

export class LeagueState {
	tl: Timeline;
	// Every player the replay may need (in the league at some point from
	// `from` - 1 on).
	pids: number[];
	// Vanilla's strategy for each team, by when it was set: the review after
	// season S's playoffs (key S), read from S's lottery on.
	strategies = new Map<number, Map<number, Team["strategy"]>>();
	private base: Record<string, number> = {};
	private lid = 1000;
	moment: Moment | undefined;

	constructor(tl: Timeline, from: number) {
		this.tl = tl;
		this.pids = [];
		for (const p of tl.players.values()) {
			if (
				p.ratings.some((r: any) => r.season >= from - 1) ||
				(typeof p.draft?.year === "number" && p.draft.year >= from - 1)
			) {
				this.pids.push(p.pid);
			}
		}
		for (const key of [
			"salaryCap",
			"minContract",
			"maxContract",
			"luxuryPayroll",
			"minPayroll",
		]) {
			this.base[key] = g.get(key as any) as number;
		}
	}

	// The box score he had by that day: earlier seasons whole; this season's
	// rows for the teams he'd played for so far, the current stint's minutes
	// cut to the games played by then (PER and other rates stay the season's).
	private statsAt(raw: any, m: Moment) {
		const S = m.season;
		const rows: any[] = raw.stats ?? [];
		if (m.phase >= PHASE.PLAYOFFS) {
			return rows.filter((s) => s.season <= S);
		}
		const before = rows.filter((s) => s.season < S);
		if (m.phase <= PHASE.PRESEASON || m.games <= 0) {
			return before;
		}
		const marks = this.tl.seasons.get(S);
		const stints: { tid: number; from: number; to: number }[] = [];
		let tid = this.tl.tidAt(raw.pid, {
			...m,
			eid: marks?.firstGameEid ?? m.eid,
			games: 0,
		});
		let from = 0;
		for (const move of this.tl.moves.get(raw.pid) ?? []) {
			if (move.eid < (marks?.firstGameEid ?? 0)) {
				continue;
			}
			if (move.eid >= m.eid) {
				break;
			}
			const at = this.tl.gamesAt(S, move.eid, PHASE.REGULAR_SEASON);
			stints.push({ tid, from, to: at });
			tid = move.tid;
			from = at;
		}
		stints.push({ tid, from, to: m.games });
		const out = [...before];
		const seasonRows = rows.filter((s) => s.season === S && !s.playoffs);
		let k = 0;
		for (const stint of stints) {
			if (stint.tid < 0) {
				continue;
			}
			const i = seasonRows.findIndex((s, j) => j >= k && s.tid === stint.tid);
			if (i < 0) {
				continue;
			}
			k = i + 1;
			const row = seasonRows[i]!;
			// How much of the row's games he'd played by then.
			const nextMove = (this.tl.moves.get(raw.pid) ?? []).find(
				(mv) => mv.eid >= m.eid && mv.tid !== stint.tid,
			);
			const end =
				stint.to < m.games
					? stint.to
					: nextMove && nextMove.eid < (marks?.regularEndEid ?? Infinity)
						? this.tl.gamesAt(S, nextMove.eid, PHASE.REGULAR_SEASON)
						: (marks?.numGames ?? this.tl.numGames);
			const share =
				end > stint.from
					? Math.max(
							0,
							Math.min(1, (stint.to - stint.from) / (end - stint.from)),
						)
					: 1;
			out.push(
				share < 1
					? {
							...row,
							min: (row.min ?? 0) * share,
							gp: Math.round((row.gp ?? 0) * share),
							gs: Math.round((row.gs ?? 0) * share),
						}
					: row,
			);
		}
		return out;
	}

	// The player as he was at that moment, where `tid` puts him.
	private playerAt(raw: any, m: Moment, tid: number): Player {
		const S = m.season;
		let ratings = raw.ratings.filter((r: any) => r.season <= S);
		if (tid === UNDRAFTED || ratings.length === 0) {
			// A prospect: his rating the year of his draft (the export keeps
			// no earlier one).
			ratings = [raw.ratings[0]];
		}
		const times = this.tl.contractTimes.get(raw.pid) ?? [];
		const signed = (raw.contracts ?? [])
			.map((c: any, i: number) => ({ c, time: times[i]! }))
			.filter((x: any) => x.time < m.eid)
			.sort((a: any, b: any) => a.time - b.time)
			.map((x: any) => x.c);
		const contracts = signed.map((c: any) => ({ ...c }));
		let contractDemand: any;
		const minContract = g.get("minContract");
		if (tid >= 0) {
			const last = contracts.at(-1);
			if (last) {
				// Still under it then: a release came later, if at all.
				delete last.endedEarly;
			}
			if (!last || contractExp(last) < S) {
				// Drafted and not signed yet (a second-rounder): what he signs.
				const next = (raw.contracts ?? []).find(
					(_: any, i: number) => times[i]! >= m.eid,
				);
				contractDemand = next
					? { amount: next.amounts[0], exp: contractExp(next) }
					: { amount: minContract, exp: S + 2 };
			}
		} else {
			contractDemand = { amount: minContract, exp: S + 1 };
		}
		let draft = raw.draft;
		if (
			tid === UNDRAFTED ||
			(tid < 0 &&
				this.tl.draftEid.get(raw.pid) === undefined &&
				draft?.year >= S)
		) {
			draft = {
				...raw.draft,
				round: 0,
				pick: 0,
				tid: PLAYER.UNDRAFTED,
				originalTid: PLAYER.UNDRAFTED,
				dpid: undefined,
			};
		}
		return {
			...raw,
			tid,
			ratings,
			stats: this.statsAt(raw, m),
			contracts,
			contractDemand,
			draft,
			injury:
				tid >= FREE_AGENT
					? this.tl.injuryAt(raw.pid, m)
					: { type: "Healthy", gamesRemaining: 0 },
			injuries: (raw.injuries ?? []).filter((x: any) => x.season < S),
			awards: (raw.awards ?? []).filter((x: any) => x.season < S),
			transactions: [],
			gamesUntilTradable: 0,
			numDaysFreeAgent: 0,
			ptModifier: 1,
			statsTids: [
				...new Set(
					(raw.stats ?? [])
						.filter((s: any) => s.season <= S)
						.map((s: any) => s.tid),
				),
			],
			retiredYear: Infinity,
		};
	}

	// The league set to that moment. `userTid`: the human on this device -
	// vanilla treats trades with it specially.
	async apply(m: Moment, userTid: number) {
		const tl = this.tl;
		const S = m.season;
		this.moment = m;
		g.setWithoutSavingToDB("season", S);
		g.setWithoutSavingToDB("phase", m.phase as any);
		g.setWithoutSavingToDB("userTid", userTid);
		// Each moment its own league id: the realistic mode's caches are keyed
		// by it, and must not carry over from another moment.
		this.lid += 1;
		g.setWithoutSavingToDB("lid", this.lid);
		const cap = tl.salaryCapAt(m.eid);
		const scale = cap / this.base.salaryCap!;
		g.setWithoutSavingToDB("salaryCap", cap);
		for (const key of [
			"minContract",
			"maxContract",
			"luxuryPayroll",
			"minPayroll",
		] as const) {
			g.setWithoutSavingToDB(
				key,
				Math.round((this.base[key]! * scale) / 10) * 10,
			);
		}

		// Players.
		const want = new Map<number, Player>();
		for (const pid of this.pids) {
			const tid = tl.tidAt(pid, m);
			if (tid === ABSENT || tid === RETIRED) {
				continue;
			}
			want.set(pid, this.playerAt(tl.players.get(pid), m, tid));
		}
		for (const p of await idb.cache.players.getAll()) {
			if (!want.has(p.pid)) {
				await idb.cache.players.delete(p.pid);
			}
		}
		await idb.cache.players.putAll(want.values());
		local.playerOvrMeanStdStale = true;
		await player.updateOvrMeanStd();
		for (const p of want.values()) {
			await player.updateValues(p);
		}

		// Picks.
		const picks = new Map<number, any>();
		for (const info of tl.picks.values()) {
			const owner = tl.pickOwnerAt(info.dpid, m);
			if (owner !== undefined) {
				picks.set(info.dpid, {
					dpid: info.dpid,
					tid: owner,
					originalTid: info.originalTid,
					round: info.round,
					season: info.season,
					pick: tl.pickNumberAt(info.dpid, m),
				});
			}
		}
		for (const dp of await idb.cache.draftPicks.getAll()) {
			if (!picks.has(dp.dpid)) {
				await idb.cache.draftPicks.delete(dp.dpid);
			}
		}
		await idb.cache.draftPicks.putAll(picks.values());

		// Records that day, this season; last season's whole.
		for (const t of tl.data.teams) {
			const rec = tl.recordAt(t.tid, m);
			if (rec) {
				await this.putTeamSeason({
					...helpers.deepCopy(rec.ts),
					won: rec.won,
					lost: rec.lost,
					tied: 0,
					otl: 0,
					playoffRoundsWon:
						m.phase >= PHASE.PLAYOFFS ? rec.ts.playoffRoundsWon : -1,
				});
			}
			const prev = tl.records.get(`${S - 1}:${t.tid}`);
			if (prev) {
				await this.putTeamSeason(helpers.deepCopy(prev));
			}
		}

		// Vanilla's strategies: the last review before this moment.
		await this.restoreStrategies();
		g.setWithoutSavingToDB(
			"numActiveTeams",
			(await idb.cache.teams.getAll()).filter((t) => !t.disabled).length,
		);
	}

	// A team's season row, in place of the one the cache has for that season
	// and team, if any (its row id kept: ids in the export and the cache
	// needn't agree).
	private async putTeamSeason(row: any) {
		const existing = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[row.season, row.tid],
		);
		row.rid = existing ? existing.rid : 1_000_000 + row.season * 100 + row.tid;
		await idb.cache.teamSeasons.put(row);
	}

	// Vanilla's strategies for this moment, after anything (the realistic
	// mode's lanes) changed them.
	async restoreStrategies() {
		const strategies = this.moment ? this.strategiesAt(this.moment) : undefined;
		for (const t of await idb.cache.teams.getAll()) {
			const s = strategies?.get(t.tid);
			if (s && t.strategy !== s) {
				t.strategy = s;
				await idb.cache.teams.put(t);
			}
		}
	}

	strategiesAt(m: Moment) {
		const lottery = this.tl.seasons.get(m.season)?.lotteryEid ?? Infinity;
		const key = m.eid > lottery ? m.season : m.season - 1;
		return this.strategies.get(key);
	}

	// Vanilla's strategies season by season, by its own review
	// (team.updateStrategies, after each season's playoffs), from the first
	// season on: it only changes a team's strategy on a clear score either
	// way, so where each stands depends on where it started. A team starts
	// contending if it won half its games the first season.
	async rebuildStrategies(to: number) {
		const tl = this.tl;
		const first = tl.startingSeason;
		let current = new Map<number, Team["strategy"]>();
		for (const t of tl.data.teams) {
			const ts = tl.records.get(`${first}:${t.tid}`);
			current.set(t.tid, ts && ts.won >= ts.lost ? "contending" : "rebuilding");
		}
		this.strategies.set(first - 1, current);
		const savedPids = this.pids;
		// Every player there was, for the early seasons.
		this.pids = [...tl.players.keys()];
		for (let season = first; season <= to; season++) {
			const m = tl.beforeDraft(season);
			if (!m) {
				break;
			}
			this.strategies.set(season - 1, current);
			await this.apply({ ...m, eid: m.eid - 0.5 }, tl.humans[0]!);
			for (const t of await idb.cache.teams.getAll()) {
				const s = current.get(t.tid);
				if (s) {
					t.strategy = s;
					await idb.cache.teams.put(t);
				}
			}
			await team.updateStrategies();
			current = new Map(
				(await idb.cache.teams.getAll()).map((t) => [t.tid, t.strategy]),
			);
			this.strategies.set(season, current);
		}
		this.pids = savedPids;
	}
}
