import { PHASE, PLAYER } from "../../../common/constants.ts";
import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { getContractDemand } from "../contract/index.ts";
import { notePoolLeft } from "../realisticFrontOffice/freeAgency.ts";
import { smartVanillaOn } from "../realisticFrontOffice/isOn.ts";
import { currentOutlook } from "../realisticFrontOffice/market.ts";
import { ensureRetention } from "../realisticFrontOffice/outlook.ts";
import { timed } from "./clock.ts";
import { clearRoomNow, clearRoomSettings, roomToSign } from "./clearRoom.ts";
import {
	prepareSignings,
	signedOn,
	smartSigningPrice,
	viewFor,
} from "./signing.ts";

// THE SMART CPU ON VANILLA'S RULES (realisticFrontOffice/isOn.ts,
// smartVanillaOn): free agency and re-signing run as in vanilla - a player
// asks what vanilla asks, re-signs only if willing, and a CPU team signs
// within vanilla's cap rules - but whom a CPU team signs, and whether it
// re-signs a player, is the smart CPU's call: what he does for the team's
// title chances on those terms, his salary counted (smartSigningPrice).

// WHAT ROOM STILL BUYS. A team's unspent room is priced by the free agents
// it is expected to land with it (payroll.ts, signingsWith), and in free
// agency by how much of the opening pool is still out there - measured each
// day, here as in the realistic mode's free agency. Unmeasured, the room was
// priced all summer as if the whole market were still there: every real free
// agent had to beat a signing that no longer existed, and teams with room
// signed nobody until the season began (League 5, docs/smart-cpu.md).
export const noteVanillaPool = async () => {
	if (!smartVanillaOn() || g.get("phase") !== PHASE.FREE_AGENCY) {
		return;
	}
	await notePoolLeft(
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT),
	);
};

// Free agents a CPU team weighs on a day it signs, the best by value first:
// the rest never come close.
export const VANILLA_FA_CANDIDATES = 10;

// Whether vanilla lets a CPU team sign him today (freeAgents/getBest.ts):
// under the cap, or for the minimum with places to spare, and within a
// secondary hard cap.
const vanillaMaySign = (
	p: Player,
	rosterSize: number,
	payroll: number,
	hardCap: number,
) => {
	const { amount } = getContractDemand(p);
	const minContract = g.get("minContract");
	const maxRosterSize = g.get("maxRosterSize");
	const underCap =
		g.get("salaryCapType") === "none" || amount + payroll <= g.get("salaryCap");
	const hardCapOk =
		amount + payroll <= hardCap ||
		(amount <= minContract && rosterSize < maxRosterSize);
	const normal = underCap && amount > minContract;
	const atMinimum = amount <= minContract && rosterSize < maxRosterSize - 2;
	return (normal || atMinimum) && hardCapOk;
};

// The free agent a CPU team signs today, if any: of those vanilla lets it
// sign, the one it gains most from on what he asks - by more than its read
// of him could be off.
//
// THE MARGIN OF ERROR, as in its trades (owner, 2026-11-19: a deal is made
// only if each side gains by more than its own read of it could be off;
// SmartView.marginNow) and the realistic mode's free agency. Without it a
// team with no title shot for the length of a contract signed anyone its
// read put a hair above nothing - him and the room he took both came to next
// to nothing in titles, and picking the best of ten free agents a day all
// summer favours the ones it overrates: in League 15 Milwaukee (16-66) gave
// Josh Selby (23, 49) $19.7M a season for three (owner, 2026-11-23: "the cpu
// gm might as well keep his payroll open in case anything else opens up").
// Measured in docs/smart-cpu.md. Re-signing (below) has no margin: he is no
// pick of many, and the other side of it is losing him for nothing.
// Settable for a run each way (test/smart/loadLeague.ts, VANILLA_SET).
export const vanillaSigningSettings = { margin: true };

export const smartVanillaSigning = (
	tid: number,
	available: readonly Player[],
	payroll: number,
	hardCap: number,
) =>
	timed("vanilla signing", async () => {
		const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		const tried: Player[] = [];
		for (const p of available) {
			if (tried.length >= VANILLA_FA_CANDIDATES) {
				break;
			}
			if (vanillaMaySign(p, roster.length, payroll, hardCap)) {
				tried.push(p);
			}
		}
		// In free agency, a free agent it can't pay may be worth making room
		// for (clearRoom.ts): only one who would be among its three best.
		const ovrOf = (x: Player) => x.ratings.at(-1)!.ovr;
		const third = roster.map(ovrOf).sort((a, b) => b - a)[2] ?? -Infinity;
		const targets =
			g.get("phase") === PHASE.FREE_AGENCY
				? available
						.filter(
							(p) =>
								!tried.includes(p) &&
								getContractDemand(p).amount > g.get("minContract") &&
								ovrOf(p) >= third,
						)
						.slice(0, clearRoomSettings.targets)
				: [];
		if (tried.length === 0 && targets.length === 0) {
			return undefined;
		}
		const outlook = await currentOutlook();
		await prepareSignings(outlook, [...tried, ...targets]);
		await ensureRetention(outlook, tried, tid);
		const view = viewFor(outlook);
		const h = await view.holdingsOf(tid);
		const none = { players: [], picks: [] };
		let best: { p: Player; net: number } | undefined;
		for (const p of tried) {
			const c = getContractDemand(p);
			const contract = { amount: c.amount, exp: c.exp };
			const { net } = smartSigningPrice(
				outlook,
				{ tid, players: h.players, picks: h.picks },
				p,
				contract,
			);
			// Ties to the better player, then the lower pid, so every device in a
			// shared league signs the same one.
			if (
				net > 0 &&
				(!vanillaSigningSettings.margin ||
					net >
						view.marginNow(tid, none, {
							players: [signedOn(p, tid, contract)],
							picks: [],
						})) &&
				(!best ||
					net > best.net ||
					(net === best.net &&
						(p.value > best.p.value ||
							(p.value === best.p.value && p.pid < best.p.pid))))
			) {
				best = { p, net };
			}
		}
		if (targets.length > 0) {
			const plan = await roomToSign(view, tid, targets, best?.net ?? 0);
			if (plan && (await clearRoomNow(view, tid, plan))) {
				return plan.p;
			}
		}
		return best?.p;
	});

// Whether a CPU team re-signs him on what he asks: if it is better off with
// him on those terms than without him.
export const smartVanillaReSign = (
	p: Player,
	contract: { amount: number; exp: number },
) =>
	timed("vanilla re-signing", async () => {
		const outlook = await currentOutlook();
		await prepareSignings(outlook, [p]);
		await ensureRetention(outlook, [p], p.tid);
		const h = await viewFor(outlook).holdingsOf(p.tid);
		return (
			smartSigningPrice(
				outlook,
				{ tid: p.tid, players: h.players, picks: h.picks },
				p,
				contract,
			).net > 0
		);
	});
