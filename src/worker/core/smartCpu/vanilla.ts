import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { getContractDemand } from "../contract/index.ts";
import { currentOutlook } from "../realisticFrontOffice/market.ts";
import { ensureRetention } from "../realisticFrontOffice/outlook.ts";
import { timed } from "./clock.ts";
import { prepareSignings, smartSigningPrice, viewFor } from "./signing.ts";

// THE SMART CPU ON VANILLA'S RULES (realisticFrontOffice/isOn.ts,
// smartVanillaOn): free agency and re-signing run as in vanilla - a player
// asks what vanilla asks, re-signs only if willing, and a CPU team signs
// within vanilla's cap rules - but whom a CPU team signs, and whether it
// re-signs a player, is the smart CPU's call: what he does for the team's
// title chances on those terms, his salary counted (smartSigningPrice).

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
// sign, the one it gains most from on what he asks.
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
		if (tried.length === 0) {
			return undefined;
		}
		const outlook = await currentOutlook();
		await prepareSignings(outlook, tried);
		await ensureRetention(outlook, tried, tid);
		const h = await viewFor(outlook).holdingsOf(tid);
		let best: { p: Player; net: number } | undefined;
		for (const p of tried) {
			const c = getContractDemand(p);
			const { net } = smartSigningPrice(
				outlook,
				{ tid, players: h.players, picks: h.picks },
				p,
				{ amount: c.amount, exp: c.exp },
			);
			// Ties to the better player, then the lower pid, so every device in a
			// shared league signs the same one.
			if (
				net > 0 &&
				(!best ||
					net > best.net ||
					(net === best.net &&
						(p.value > best.p.value ||
							(p.value === best.p.value && p.pid < best.p.pid))))
			) {
				best = { p, net };
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
