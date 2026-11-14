import type { Player } from "../../../common/types.ts";
import { g, helpers } from "../../util/index.ts";
import { player } from "../index.ts";
import { currentOutlook, loadTeam } from "./market.ts";
import {
	franchiseValue,
	teamStrengths,
	type LeagueOutlook,
} from "./outlook.ts";
import { contractOf } from "./contractOf.ts";
import { smartCpuOn } from "./isOn.ts";
import {
	prepareSignings,
	smartSigningPrice,
	viewFor,
} from "../smartCpu/signing.ts";
import { timed } from "../smartCpu/clock.ts";

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
					contractOf(p),
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
		tid,
	);
};

const CUT_CANDIDATES = 8;

// Whom the team can best do without of these players, and what it is worth
// without him.
const bestCut = (
	outlook: LeagueOutlook,
	tid: number,
	players: readonly Player[],
	picks: Parameters<typeof teamStrengths>[3],
) => {
	const phase = g.get("phase");
	const season = g.get("season");
	let best: { p: Player; value: number; free: boolean } | undefined;
	// Only the least valuable few are worth trying: a good player's salary
	// is owed either way. Ties go to the lower pid, so every device in a
	// shared league cuts the same player.
	const tried = [...players]
		.sort((a, b) => a.value - b.value || a.pid - b.pid)
		.slice(0, CUT_CANDIDATES)
		.sort((a, b) => a.pid - b.pid);
	for (const p of tried) {
		const free = helpers.justDrafted(
			{ draft: p.draft, contract: contractOf(p) },
			phase,
			season,
		);
		const value = worthWithout(outlook, tid, players, picks, p, free);
		if (!best || value > best.value) {
			best = { p, value, free };
		}
	}
	return best;
};

// With the smart CPU: whom the team can best do without, by the GM's view
// (the same choice, its let-go decisions reused).
const smartCut = (outlook: LeagueOutlook, tid: number) =>
	timed("cuts", () => smartCutNow(outlook, tid));

const smartCutNow = async (outlook: LeagueOutlook, tid: number) => {
	const view = viewFor(outlook);
	const h = await view.holdingsOf(tid);
	let best: { p: Player; value: number } | undefined;
	for (const p of [...h.players]
		.sort((a, b) => a.value - b.value || a.pid - b.pid)
		.slice(0, CUT_CANDIDATES)
		.sort((a, b) => a.pid - b.pid)) {
		const value = view.value(
			tid,
			{ players: h.players.filter((x) => x !== p), picks: h.picks },
			view.owedIfReleased(p),
		);
		if (!best || value > best.value) {
			best = { p, value };
		}
	}
	return best && { p: best.p, free: view.owedIfReleased(best.p).length === 0 };
};

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
	const released: number[] = [];
	if (smartCpuOn()) {
		for (let k = 0; k < numToDrop; k++) {
			const best = await smartCut(outlook, tid);
			if (!best) {
				break;
			}
			await player.release(best.p, best.free);
			released.push(best.p.pid);
		}
		return released;
	}
	for (let k = 0; k < numToDrop; k++) {
		const t = await loadTeam(outlook, tid);
		const best = bestCut(outlook, tid, t.players, t.picks);
		if (!best) {
			break;
		}
		await player.release(best.p, best.free);
		released.push(best.p.pid);
	}
	return released;
};

// Whether a CPU team at the roster limit is better off signing him, on these
// terms, and releasing whoever it can then best do without - the cut it
// will make (frontOfficeCuts). The signing was priced cutting the least
// valuable player (afterCuts), and the whole team looked at again may cut
// another: without this, a team signed a player every day only to release
// him.
export const worthSigningAtLimit = async (tid: number, signed: Player) => {
	const outlook = await currentOutlook();
	if (smartCpuOn()) {
		// Better off on the roster it will have, and he isn't the one cut.
		const view = viewFor(outlook);
		const h = await view.holdingsOf(tid);
		const withHim = { players: [...h.players, signed], picks: h.picks };
		const [kept] = view.afterCuts(tid, withHim);
		return (
			kept.players.includes(signed) &&
			view.value(tid, withHim) > view.value(tid, h)
		);
	}
	const t = await loadTeam(outlook, tid);
	const best = bestCut(outlook, tid, [...t.players, signed], t.picks);
	return best !== undefined && best.p !== signed && best.value > t.value;
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
	if (smartCpuOn()) {
		// The one worth most to it on what he asks, by the GM's worth.
		const tried = candidates.slice(0, FILL_CANDIDATES);
		await prepareSignings(outlook, tried);
		const h = viewFor(outlook).heldNow(tid);
		let best: { p: Player; gain: number } | undefined;
		for (const p of tried) {
			const { gain } = smartSigningPrice(
				outlook,
				{ tid, players: h.players, picks: h.picks },
				p,
				contractOf(p),
			);
			if (!best || gain > best.gain) {
				best = { p, gain };
			}
		}
		return best?.p;
	}
	const t = await loadTeam(outlook, tid);
	let best: { p: Player; value: number } | undefined;
	// The first few by value are enough to choose from.
	for (const p of candidates.slice(0, FILL_CANDIDATES)) {
		const signed = { ...p, tid } as Player;
		const value = franchiseValue(
			outlook,
			teamStrengths(outlook, tid, [...t.players, signed], t.picks),
			tid,
		);
		if (!best || value > best.value) {
			best = { p, value };
		}
	}
	return best?.p;
};
