// Multiplayer free-agency board: each user team ranks the free agents it wants
// (blind to the other teams), and when everyone readies up for the next FA day
// the boards resolve BEFORE the day sims, waiver-style:
//
//   - Round by round, each team's claim is its highest-ranked player who is
//     still a free agent, willing to sign with them, and affordable.
//   - A player claimed by one team signs unopposed. A player claimed by
//     several is decided by a single 1-100 roll, with each team's band sized
//     by mood: weight = 1.5^mood, so each +1 of mood multiplies your odds by
//     1.5. Equal moods = even odds.
//   - A team that lands a player is done for the day (one board signing per
//     team per day); losers roll again next round with their next claim.
//
// Everything about the resolution - every board, every band, every roll - is
// recorded to the faDayResults store (synced like any other data) so the whole
// room can see exactly what happened and why. Contracts use the engine's own
// mood-adjusted asking price, and signings go through player.sign inside a
// capture window, so they publish like any other transaction.

import { PHASE, PLAYER } from "../../../common/constants.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { player, team } from "../index.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { syncDebugLog } from "./debugLog.ts";
import type { FaBoardEntry, SyncTransport } from "./types.ts";
import type {
	FaDayResultItem,
	FaDayResults,
	FaRollTeam,
} from "../../../common/types.ts";

// Each +1 of mood multiplies a team's odds by this. Chosen deliberately gentle
// so upsets stay common (+1 mood -> 60/40, +3 vs 0 -> 77/23).
const MOOD_ODDS_BASE = 1.5;

let currentTransport: SyncTransport | undefined;
let unsubscribe: (() => void) | undefined;
let latestBoards: Record<string, FaBoardEntry | null> | undefined;

export const setupFaBoard = (transport: SyncTransport) => {
	teardownFaBoard();
	currentTransport = transport;
	unsubscribe = transport.subscribeFaBoard?.((boards) => {
		latestBoards = boards;
	});
};

export const teardownFaBoard = () => {
	unsubscribe?.();
	unsubscribe = undefined;
	currentTransport = undefined;
	latestBoards = undefined;
};

// Publish this device's team board (empty clears it). Boards are per TEAM: the
// newest entry from any of a team's devices wins.
export const setFaBoard = async (pids: number[]) => {
	const engine = getSyncEngine();
	if (!engine || !currentTransport?.publishFaBoard) {
		throw new Error("Connect to a shared league first.");
	}
	const clean = [...new Set(pids)].filter((pid) => typeof pid === "number");
	await currentTransport.publishFaBoard(
		clean.length > 0
			? {
					season: g.get("season"),
					tid: g.get("userTid"),
					pids: clean,
					at: Date.now(),
					name: engine.localName,
				}
			: null,
	);
};

// The newest board entry per team, for the current season, user teams only.
const boardsPerTeam = (): Map<number, number[]> => {
	const season = g.get("season");
	const userTids = g.get("userTids");
	const newest = new Map<number, FaBoardEntry>();
	for (const entry of Object.values(latestBoards ?? {})) {
		if (
			!entry ||
			entry.season !== season ||
			typeof entry.tid !== "number" ||
			!userTids.includes(entry.tid) ||
			!Array.isArray(entry.pids)
		) {
			continue;
		}
		const prev = newest.get(entry.tid);
		if (!prev || (entry.at ?? 0) > (prev.at ?? 0)) {
			newest.set(entry.tid, entry);
		}
	}
	const out = new Map<number, number[]>();
	for (const [tid, entry] of newest) {
		if (entry.pids.length > 0) {
			out.set(tid, entry.pids);
		}
	}
	return out;
};

// This device's own team's current board (for the UI).
export const getMyFaBoard = (): number[] => {
	if (!currentTransport) {
		return [];
	}
	return boardsPerTeam().get(g.get("userTid")) ?? [];
};

export const faBoardActive = (): boolean =>
	currentTransport !== undefined && g.get("phase") === PHASE.FREE_AGENCY;

// Sum the mood components the same way moodInfo does - this is the number the
// UI shows as the player's mood toward a team.
const moodSum = (components: Record<string, any>): number => {
	let sum = 0;
	for (const [key, value] of Object.entries(components)) {
		if (key === "custom") {
			for (const row of value ?? []) {
				sum += row.amount;
			}
		} else if (typeof value === "number") {
			sum += value;
		}
	}
	return sum;
};

// Split 1-100 into contiguous bands proportional to the weights. Every team
// gets at least a 1-wide band; the last band always ends at 100.
export const oddsBands = (
	weights: number[],
): { pct: number; lo: number; hi: number }[] => {
	const total = weights.reduce((s, w) => s + w, 0);
	const out: { pct: number; lo: number; hi: number }[] = [];
	let prevHi = 0;
	let cum = 0;
	for (const [i, w] of weights.entries()) {
		cum += w;
		let hi = i === weights.length - 1 ? 100 : Math.round((cum / total) * 100);
		hi = Math.min(100 - (weights.length - 1 - i), Math.max(hi, prevHi + 1));
		out.push({
			pct: Math.round((w / total) * 1000) / 10,
			lo: prevHi + 1,
			hi,
		});
		prevHi = hi;
	}
	return out;
};

type Candidate = {
	pid: number;
	name: string;
	mood: number;
	amount: number;
	exp: number;
};

// Resolve every team's board for the FA day that's about to sim. Runs on the
// device that won the day-advance claim, right before freeAgentsPlay - so
// exactly once per day, published with the day's changes.
export const resolveFaBoards = async (): Promise<void> => {
	if (g.get("phase") !== PHASE.FREE_AGENCY) {
		return;
	}
	const boards = boardsPerTeam();
	if (boards.size === 0) {
		return;
	}

	const season = g.get("season");
	const daysLeft = g.get("daysLeft");
	const teamInfo = g.get("teamInfoCache");
	const abbrevFor = (tid: number) => teamInfo[tid]?.abbrev ?? `#${tid}`;

	const items: FaDayResultItem[] = [];
	const revealedBoards: FaDayResults["boards"] = [];

	changeTracker.beginSim();
	try {
		const salaryCapType = g.get("salaryCapType");

		// Per team: the ordered list of candidates it can actually sign today.
		// Refusals and cap problems are recorded once, up front (payroll can't
		// change mid-resolution since each team signs at most one player).
		const effective = new Map<number, Candidate[]>();
		for (const [tid, pids] of boards) {
			const rows: Candidate[] = [];
			const boardNames: { pid: number; name: string }[] = [];
			const payroll = await team.getPayroll(tid);
			for (const pid of pids) {
				const p = await idb.cache.players.get(pid);
				if (!p) {
					continue;
				}
				const name = `${p.firstName} ${p.lastName}`;
				boardNames.push({ pid, name });
				if (p.tid !== PLAYER.FREE_AGENT) {
					continue;
				}
				const mood = await player.moodInfo(p, tid);
				if (!mood.willing) {
					items.push({
						type: "refused",
						pid,
						name,
						tid,
						abbrev: abbrevFor(tid),
					});
					continue;
				}
				const amount = mood.contractAmount;
				if (
					salaryCapType !== "none" &&
					payroll + amount - 1 > g.get("salaryCap") &&
					amount - 1 > g.get("minContract")
				) {
					items.push({
						type: "ineligible",
						pid,
						name,
						tid,
						abbrev: abbrevFor(tid),
					});
					continue;
				}
				rows.push({
					pid,
					name,
					mood: moodSum(mood.components),
					amount,
					exp: p.contract.exp,
				});
			}
			effective.set(tid, rows);
			revealedBoards.push({ tid, abbrev: abbrevFor(tid), pids: boardNames });
		}

		const taken = new Set<number>();
		const done = new Set<number>();
		let round = 0;
		while (round < 100) {
			round += 1;

			// Current claims: each still-active team's best available candidate.
			const claims = new Map<number, (Candidate & { tid: number })[]>();
			for (const [tid, rows] of effective) {
				if (done.has(tid)) {
					continue;
				}
				const row = rows.find((r) => !taken.has(r.pid));
				if (!row) {
					done.add(tid);
					continue;
				}
				const list = claims.get(row.pid) ?? [];
				list.push({ ...row, tid });
				claims.set(row.pid, list);
			}
			if (claims.size === 0) {
				break;
			}

			for (const [pid, contenders] of claims) {
				taken.add(pid);
				let winner: Candidate & { tid: number };

				if (contenders.length === 1) {
					winner = contenders[0]!;
					items.push({
						type: "unopposed",
						pid,
						name: winner.name,
						round,
						tid: winner.tid,
						abbrev: abbrevFor(winner.tid),
						amount: winner.amount,
						exp: winner.exp,
					});
				} else {
					// Sort by tid for a stable display order; the roll doesn't care.
					contenders.sort((a, b) => a.tid - b.tid);
					const bands = oddsBands(
						contenders.map((c) => MOOD_ODDS_BASE ** c.mood),
					);
					const roll = 1 + Math.floor(Math.random() * 100);
					const winnerIndex = bands.findIndex(
						(b) => roll >= b.lo && roll <= b.hi,
					);
					winner = contenders[winnerIndex === -1 ? 0 : winnerIndex]!;
					const rollTeams: FaRollTeam[] = contenders.map((c, i) => ({
						tid: c.tid,
						abbrev: abbrevFor(c.tid),
						mood: Math.round(c.mood * 10) / 10,
						oddsPct: bands[i]!.pct,
						lo: bands[i]!.lo,
						hi: bands[i]!.hi,
					}));
					items.push({
						type: "contest",
						pid,
						name: winner.name,
						round,
						teams: rollTeams,
						roll,
						winnerTid: winner.tid,
						amount: winner.amount,
						exp: winner.exp,
					});
				}

				const p = await idb.cache.players.get(pid);
				if (p && p.tid === PLAYER.FREE_AGENT) {
					await player.sign(
						p,
						winner.tid,
						{ amount: winner.amount, exp: winner.exp },
						PHASE.FREE_AGENCY,
					);
					await idb.cache.players.put(p);
					await team.rosterAutoSort(winner.tid);
				}
				done.add(winner.tid);
			}
		}

		const results: FaDayResults = {
			key: `${season}-${daysLeft}`,
			season,
			daysLeft,
			items,
			boards: revealedBoards,
			at: Date.now(),
		};
		await idb.cache.faDayResults.put(results);
	} finally {
		changeTracker.endSim();
	}

	syncDebugLog("faBoard:resolved", {
		season,
		daysLeft,
		items: items.length,
	});

	// Push the roll outcomes to the teams involved - fire-and-forget on the
	// notifications channel, fully decoupled from sync.
	const engine = getSyncEngine();
	if (engine) {
		for (const item of items) {
			if (item.type !== "contest") {
				continue;
			}
			const winner = item.teams.find((t) => t.tid === item.winnerTid);
			for (const t of item.teams) {
				const mine = t.tid === item.winnerTid;
				try {
					await engine.publishNotification({
						title: mine ? `You won ${item.name}` : `You lost ${item.name}`,
						body: mine
							? `Rolled ${item.roll}, your range was ${t.lo}–${t.hi} (${t.oddsPct}%).`
							: `${winner?.abbrev ?? "Another team"} won: rolled ${item.roll}, your range was ${t.lo}–${t.hi} (${t.oddsPct}%).`,
						targetTids: [t.tid],
						path: "free_agents",
					});
				} catch {
					// A missed push is harmless; the results panel has everything.
				}
			}
		}
	}
};
