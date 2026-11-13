import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { currentOutlook } from "../realisticFrontOffice/market.ts";
import { ensureRetention } from "../realisticFrontOffice/outlook.ts";
import { viewFor } from "./signing.ts";
import type { Assets } from "./worth.ts";

// A HUMAN'S OFFER, BY THE GM'S WORTH (docs/smart-cpu.md, layer 2). A CPU
// team takes it if it gains by layer 1's worth and no CPU team keen on what
// it gives would really give more (the market rule, owner 2026-11-13). No
// trade difficulty: whether the GM needs one is for the bench to say.

export type SmartHumanVerdict = {
	gain: number;
	worth: number;
	// The deal is good, but a CPU team would really give more.
	betterElsewhere: boolean;
};

export const smartJudgeOffer = async (
	cpuTid: number,
	humanTid: number,
	// What the CPU team gives and gets.
	out: Assets,
	inn: Assets,
): Promise<SmartHumanVerdict> => {
	const outlook = await currentOutlook();
	const view = viewFor(outlook);
	await view.prime();
	const players: Player[] = [];
	for (const pid of [...out.pids, ...inn.pids]) {
		const p = await idb.cache.players.get(pid);
		if (p) {
			players.push(p);
		}
	}
	for (const tid of [cpuTid, humanTid, ...(await view.keenest())]) {
		await ensureRetention(outlook, players, tid);
	}
	const v = await view.verdict(cpuTid, humanTid, out, inn);
	return {
		gain: v.gain,
		worth: v.worth,
		betterElsewhere: v.worth > 0 && !(v.gain > 0),
	};
};

// What a CPU team says when it turns an offer down, by how far it is from a
// deal (titles of its worth).
export const smartRejection = (v: SmartHumanVerdict) => {
	if (v.betterElsewhere) {
		return 'Trade rejected! "I can get more than that elsewhere."';
	}
	const message =
		v.worth > -0.005
			? "Close, but not quite good enough."
			: v.worth > -0.03
				? "That's not a good deal for me."
				: "What, are you crazy?!";
	return `Trade rejected! "${message}"`;
};
