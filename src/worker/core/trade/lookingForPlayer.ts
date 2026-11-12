import {
	getContractExp,
	getCurrentContract,
	getNearestContractAmount,
} from "../../../common/contracts.ts";
import type { Player } from "../../../common/types.ts";
import { g } from "../../util/index.ts";

// WHAT THE USER IS AFTER IN A PLAYER, on the trading block: a PER of at
// least so much, under contract through such a season, paid at most so much
// (thousands, as contracts are). Any of them, or none.
export type PlayerWanted = {
	per?: number;
	through?: number;
	salaryMost?: number;
};

// Whether he asked for a player of some sort - the first thing a team puts
// up is one, then (a salary cap alone asks for nobody: only that nobody
// over it comes back).
export const wantsPlayer = (want: PlayerWanted | undefined): boolean =>
	want !== undefined && (want.per !== undefined || want.through !== undefined);

// His PER: this season's, or - not on the floor yet this season - the last
// he had. Undefined if he has none (or the game has no PER).
export const perOf = (p: Pick<Player, "stats">): number | undefined => {
	for (let i = p.stats.length - 1; i >= 0; i--) {
		const s = p.stats[i] as { playoffs?: boolean; gp?: number; per?: number };
		if (!s.playoffs && (s.gp ?? 0) > 0 && typeof s.per === "number") {
			return s.per;
		}
	}
	return undefined;
};

// Paid more than he would take on (what he is paid now).
export const paidTooMuch = (
	p: Player,
	want: PlayerWanted | undefined,
): boolean => {
	if (want?.salaryMost === undefined) {
		return false;
	}
	const contract = getCurrentContract(p);
	return (
		contract !== undefined &&
		getNearestContractAmount(contract, g.get("season")) > want.salaryMost
	);
};

// A player he is after: all he asked for, at once.
export const isWanted = (
	p: Player,
	want: PlayerWanted | undefined,
): boolean => {
	if (paidTooMuch(p, want)) {
		return false;
	}
	if (!want) {
		return true;
	}
	if (want.per !== undefined && !((perOf(p) ?? -Infinity) >= want.per)) {
		return false;
	}
	if (want.through !== undefined) {
		const contract = getCurrentContract(p);
		if (!contract || getContractExp(contract) < want.through) {
			return false;
		}
	}
	return true;
};
