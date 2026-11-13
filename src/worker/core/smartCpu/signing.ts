import type { FlatContractInfo, Player } from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import {
	ensureRetention,
	type LeagueOutlook,
} from "../realisticFrontOffice/outlook.ts";
import { SmartView, type Holdings } from "./worth.ts";

// SIGNINGS BY THE GM'S WORTH (docs/smart-cpu.md, layer 2). With the smart
// CPU on, the offer market (realisticFrontOffice/freeAgency.ts) prices every
// signing here - free agents in every stage and in season, re-signing,
// matching an offer sheet - and everything it builds on that price (boards,
// asks, odds) stands as it is.

// The teams humans run: none in spectator mode.
const humanTids = () =>
	g.get("spectator") ? [] : (g.get("userTids") as number[]);

// The GM's view of the league for an outlook, kept as long as the outlook.
const views = new WeakMap<LeagueOutlook, SmartView>();
export const viewFor = (outlook: LeagueOutlook) => {
	let view = views.get(outlook);
	if (!view) {
		view = new SmartView(outlook, humanTids());
		views.set(outlook, view);
	}
	return view;
};

// Before a batch of signings is priced: every team read as it stands, and
// these players' chances of staying with the teams that set prices.
export const prepareSignings = async (
	outlook: LeagueOutlook,
	players: readonly Player[],
) => {
	const view = viewFor(outlook);
	await view.prime();
	for (const tid of await view.keenest()) {
		await ensureRetention(outlook, players, tid);
	}
};

// He as signed by a team on these terms.
export const signedOn = (
	p: Player,
	tid: number,
	contract: FlatContractInfo,
): Player => ({
	...p,
	tid,
	contractDemand: {
		...contractOf(p),
		amount: contract.amount,
		exp: contract.exp,
	},
});

// What signing him on these terms is worth to a team that holds `t` (him
// not among them, or replaced): what he does for its goal, his salary
// counted as the room and money it takes, on the roster it will have once
// cut to the limit. In titles, for priceSigning's callers: the GM's worth
// carries no margin of error.
//
// No market-price floor, as a trade's assets have (layer 1): a contract a
// team creates resells only for players of about the same salary, so its
// room is spent either way, and a price set by contenders, whose money
// past the cap costs them next to nothing, made any salary look worth
// paying (re-signing a 35-year-old was "worth" billions a year).
export const smartSigningPrice = (
	outlook: LeagueOutlook,
	t: Pick<Holdings, "players" | "picks"> & { tid: number },
	p: Player,
	contract: FlatContractInfo,
) => {
	const view = viewFor(outlook);
	const signed = signedOn(p, t.tid, contract);
	const h = {
		players: t.players.filter((x) => x.pid !== p.pid),
		picks: t.picks,
	};
	const gain =
		view.value(t.tid, { players: [...h.players, signed], picks: h.picks }) -
		view.value(t.tid, h);
	return { gain, margin: 0, net: gain };
};
