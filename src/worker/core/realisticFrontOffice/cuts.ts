import type { Player } from "../../../common/types.ts";
import { g, helpers } from "../../util/index.ts";
import { player } from "../index.ts";
import { currentOutlook, loadTeam } from "./market.ts";
import {
	franchiseValue,
	teamStrengths,
	type LeagueOutlook,
} from "./outlook.ts";

// CUTS. A CPU team over the roster limit releases whoever it can best do
// without: the player whose release leaves its franchise worth the most - his
// salary still owed unless he was just drafted, his upside gone with him, and
// the floor's positions as they are left. Stock cuts the lowest value, salary
// and fit aside.

// What the team is worth without him.
const worthWithout = (
	outlook: LeagueOutlook,
	tid: number,
	players: readonly Player[],
	picks: Parameters<typeof teamStrengths>[3],
	p: Player,
	free: boolean,
) => {
	const view = free
		? outlook
		: {
				...outlook,
				deadMoney: new Map(outlook.deadMoney).set(tid, [
					...(outlook.deadMoney.get(tid) ?? []),
					{ amount: p.contract.amount, exp: p.contract.exp },
				]),
			};
	return franchiseValue(
		view,
		teamStrengths(
			view,
			tid,
			players.filter((x) => x !== p),
			picks,
		),
	);
};

const CUT_CANDIDATES = 8;

// Releases this many of the team's players, and returns their pids.
export const frontOfficeCuts = async (
	players: readonly Player[],
	numToDrop: number,
) => {
	const tid = players[0]?.tid;
	if (tid === undefined || numToDrop <= 0) {
		return [];
	}
	const outlook = await currentOutlook();
	const phase = g.get("phase");
	const season = g.get("season");
	const released: number[] = [];
	for (let k = 0; k < numToDrop; k++) {
		const t = await loadTeam(outlook, tid);
		let best: { p: Player; value: number; free: boolean } | undefined;
		// Only the least valuable few are worth trying: a good player's salary
		// is owed either way. Ties go to the lower pid, so every device in a
		// shared league cuts the same player.
		const tried = [...t.players]
			.sort((a, b) => a.value - b.value || a.pid - b.pid)
			.slice(0, CUT_CANDIDATES)
			.sort((a, b) => a.pid - b.pid);
		for (const p of tried) {
			const free = helpers.justDrafted(p, phase, season);
			const value = worthWithout(outlook, tid, t.players, t.picks, p, free);
			if (!best || value > best.value) {
				best = { p, value, free };
			}
		}
		if (!best) {
			break;
		}
		await player.release(best.p, best.free);
		released.push(best.p.pid);
	}
	return released;
};

// The free agents a team short of players chooses among, best first by value.
const FILL_CANDIDATES = 15;

// A CPU team under the roster minimum signs, one at a time, the minimum-salary
// free agent worth the most to it - a guard when it is short of guards - from
// `candidates`, best first by value; undefined if there are none.
export const frontOfficeFill = async (
	tid: number,
	candidates: readonly Player[],
) => {
	if (candidates.length === 0) {
		return undefined;
	}
	const outlook = await currentOutlook();
	const t = await loadTeam(outlook, tid);
	let best: { p: Player; value: number } | undefined;
	// The first few by value are enough to choose from.
	for (const p of candidates.slice(0, FILL_CANDIDATES)) {
		const signed = { ...p, tid } as Player;
		const value = franchiseValue(
			outlook,
			teamStrengths(outlook, tid, [...t.players, signed], t.picks),
		);
		if (!best || value > best.value) {
			best = { p, value };
		}
	}
	return best?.p;
};
