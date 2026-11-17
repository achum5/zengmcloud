import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { currentOutlook } from "../realisticFrontOffice/market.ts";
import {
	curveAt,
	curvesFor,
	ensureRetention,
	type LeagueOutlook,
	PATIENCE,
	teamStrengths,
} from "../realisticFrontOffice/outlook.ts";
import { timed } from "./clock.ts";
import { viewFor } from "./signing.ts";
import type { Assets, Holdings } from "./worth.ts";

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

export const smartJudgeOffer = (
	cpuTid: number,
	humanTid: number,
	out: Assets,
	inn: Assets,
) => timed("human offers", () => judgeNow(cpuTid, humanTid, out, inn));

const judgeNow = async (
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

// THE SMART CPU ON VANILLA'S RULES (realisticFrontOffice/isOn.ts,
// smartVanillaOn). A human's offer vanilla takes at the league's trade
// difficulty, a CPU team still turns down if it goes against the team's
// direction - a contender left weaker this season and next, a rebuilding team
// left weaker after that - or, both teams contenders, if it helps the human's
// team more than the CPU team in title chances this season and next: no
// poaching a contender's star to double one's own chances.

// Title chances this season and next at which a team is a contender, and at
// or under which it is rebuilding.
export const CONTENDER = 0.03;
export const REBUILDING = 0.005;
// What a deal may cost a team in its direction before it says no (titles):
// a share of what it has, and at least the model's noise - near the top a
// bench swap moves title chances by a point.
export const DIRECTION_SLACK = 0.005;
export const DIRECTION_SHARE = 0.02;
// A rival contender's gain it won't give: more than this (titles, this season
// and next) and more than this many times its own gain.
export const RIVAL_FLOOR = 0.01;
export const RIVAL_RATIO = 1.5;

export type SmartGuard = (
	| { ok: true }
	| { ok: false; reason: "now" | "later" | "rival"; message: string }
) & {
	// Title chances this season and next, before and after, for explaining.
	cpu?: [number, number];
	human?: [number, number];
};

// A team's title chances each season on these holdings, cut to the limit.
const titleOdds = (
	view: ReturnType<typeof viewFor>,
	outlook: LeagueOutlook,
	tid: number,
	h: Holdings,
) => {
	const [cut] = view.afterCuts(tid, h);
	const { title } = curvesFor(outlook, tid);
	return teamStrengths(outlook, tid, cut.players, cut.picks).map((s, y) =>
		curveAt(title[y]!, s),
	);
};

// This season and next.
const near = (odds: readonly number[]) => (odds[0] ?? 0) + (odds[1] ?? 0);

// The seasons after, each worth less the further off.
const later = (odds: readonly number[]) => {
	let sum = 0;
	for (let y = 2; y < odds.length; y++) {
		sum += PATIENCE ** y * odds[y]!;
	}
	return sum;
};

export const smartGuardOffer = (
	cpuTid: number,
	humanTid: number,
	// What the CPU team gives and gets.
	out: Assets,
	inn: Assets,
) => timed("human offers", () => guardNow(cpuTid, humanTid, out, inn));

const guardNow = async (
	cpuTid: number,
	humanTid: number,
	out: Assets,
	inn: Assets,
) => {
	const outlook = await currentOutlook();
	const view = viewFor(outlook);
	await view.prime();
	return guardWith(view, outlook, cpuTid, humanTid, out, inn);
};

// The same on a view and outlook of the caller's (the DBA replay's).
export const guardWith = async (
	view: ReturnType<typeof viewFor>,
	outlook: LeagueOutlook,
	cpuTid: number,
	humanTid: number,
	out: Assets,
	inn: Assets,
): Promise<SmartGuard> => {
	const o = await view.assetsOf(out);
	const i = await view.assetsOf(inn);
	for (const tid of [cpuTid, humanTid]) {
		await ensureRetention(outlook, [...o.players, ...i.players], tid);
	}
	const cpuHeld = await view.holdingsOf(cpuTid);
	const humanHeld = await view.holdingsOf(humanTid);
	const cpuBefore = titleOdds(view, outlook, cpuTid, cpuHeld);
	const cpuAfter = titleOdds(
		view,
		outlook,
		cpuTid,
		view.swappedFrom(cpuHeld, o, i),
	);
	const cpuNear = near(cpuBefore);
	const cpu: [number, number] = [cpuNear, near(cpuAfter)];
	if (
		cpuNear >= CONTENDER &&
		near(cpuAfter) <
			cpuNear - Math.max(DIRECTION_SLACK, DIRECTION_SHARE * cpuNear)
	) {
		return {
			ok: false,
			reason: "now",
			message: 'Trade rejected! "We\'re trying to win now."',
			cpu,
		};
	}
	if (
		cpuNear <= REBUILDING &&
		later(cpuAfter) < later(cpuBefore) - DIRECTION_SLACK
	) {
		return {
			ok: false,
			reason: "later",
			message: 'Trade rejected! "We\'re building for later."',
			cpu,
		};
	}
	if (cpuNear >= CONTENDER) {
		const humanBefore = titleOdds(view, outlook, humanTid, humanHeld);
		if (near(humanBefore) >= CONTENDER) {
			const humanAfter = titleOdds(
				view,
				outlook,
				humanTid,
				view.swappedFrom(humanHeld, i, o),
			);
			const humanGain = near(humanAfter) - near(humanBefore);
			const cpuGain = near(cpuAfter) - cpuNear;
			if (
				humanGain > RIVAL_FLOOR &&
				humanGain > RIVAL_RATIO * Math.max(0, cpuGain)
			) {
				return {
					ok: false,
					reason: "rival",
					message: 'Trade rejected! "I\'m not helping a rival contender."',
					cpu,
					human: [near(humanBefore), near(humanAfter)],
				};
			}
		}
	}
	return { ok: true, cpu };
};
