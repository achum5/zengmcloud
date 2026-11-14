import type { DraftPick, Player, TradeTeams } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import isUntradable from "../trade/isUntradable.ts";
import summary from "../trade/summary.ts";
import { currentOutlook } from "../realisticFrontOffice/market.ts";
import { ensureRetention } from "../realisticFrontOffice/outlook.ts";
import { viewFor } from "./signing.ts";
import type { Holdings, SmartView } from "./worth.ts";

// THE TRADE SCREENS ON THE GM (docs/smart-cpu.md, layer 4): the trading
// block, what would make this work and trade proposals, with the smart CPU
// on, from the GM's own offers - as it offers another CPU team - every step
// by its full worth. The game's own search (trade/makeItWork.ts) asks a
// valuation of hundreds of packages, made for vanilla's values that add up:
// on the GM's worth a trading block took two minutes, and a quicker read of
// a package from its pieces' prices offered deals the full worth then
// refused, or asked seven picks for one player.

const teamsOf = (
	user: number,
	userGives: Holdings,
	cpu: number,
	cpuGives: Holdings,
): TradeTeams => [
	{
		tid: user,
		pids: userGives.players.map((p) => p.pid),
		dpids: userGives.picks.map((dp) => dp.dpid),
		pidsExcluded: [],
		dpidsExcluded: [],
	},
	{
		tid: cpu,
		pids: cpuGives.players.map((p) => p.pid),
		dpids: cpuGives.picks.map((dp) => dp.dpid),
		pidsExcluded: [],
		dpidsExcluded: [],
	},
];

const ready = async () => {
	const view = viewFor(await currentOutlook());
	await view.prime();
	return view;
};

const cpuTeams = async (view: SmartView, but: number) =>
	(await idb.cache.teams.getAll())
		.filter((t) => !t.disabled && t.tid !== but && !view.humans.has(t.tid))
		.map((t) => t.tid);

// The trading block: each CPU team's real offer for what the user puts up.
export const smartTradingBlock = async (
	user: number,
	pids: number[],
	dpids: number[],
) => {
	const view = await ready();
	const out = await view.assetsOf({ pids, dpids });
	const offers: TradeTeams[] = [];
	for (const tid of await cpuTeams(view, user)) {
		const made = await view.offerFrom(tid, user, out);
		if (made) {
			offers.push(teamsOf(user, out, tid, made.offer));
		}
	}
	return offers;
};

// Trade proposals: real offers from CPU teams, each for what the user is
// asked about (one of its players or picks, the better players more often,
// as the game's own proposals choose), at most `max` of them.
export const smartProposals = async (
	user: number,
	asks: readonly { tid: number; pids: number[]; dpids: number[] }[],
	max: number,
	keep: (teams: TradeTeams) => boolean,
) => {
	const view = await ready();
	const offers: TradeTeams[] = [];
	for (const ask of asks) {
		if (view.humans.has(ask.tid) || ask.tid === user) {
			continue;
		}
		const out = await view.assetsOf(ask);
		const made = await view.offerFrom(ask.tid, user, out);
		if (!made) {
			continue;
		}
		const teams = teamsOf(user, out, ask.tid, made.offer);
		if (keep(teams)) {
			offers.push(teams);
			if (offers.length >= max) {
				break;
			}
		}
	}
	return offers;
};

// Whether a CPU team's offer still stands: still legal, and still good for
// it by its full worth.
export const smartOfferStands = async (teams: TradeTeams) => {
	if ((await summary(teams)).warning !== null) {
		return false;
	}
	const view = await ready();
	const userGives = await view.assetsOf(teams[0]);
	const cpuGives = await view.assetsOf(teams[1]);
	return (
		(await view.dealWorth(teams[1].tid, teams[0].tid, cpuGives, userGives)) > 0
	);
};

// What would make this work: what the CPU team would take from the user for
// what is on the table. Its most wanted of the user's players and picks are
// added until it says yes by its full worth, then those the user would miss
// most taken back while it still does. Undefined if nothing would.
export const smartMakeItWork = async (teams: TradeTeams) => {
	const view = await ready();
	const user = teams[0].tid;
	const cpu = teams[1].tid;
	const cpuGives = await view.assetsOf({
		pids: teams[1].pids,
		dpids: teams[1].dpids,
	});
	let userGives = await view.assetsOf({
		pids: teams[0].pids,
		dpids: teams[0].dpids,
	});
	const worth = async (x: Holdings) => view.dealWorth(cpu, user, cpuGives, x);
	const legal = async (x: Holdings) =>
		(await summary(teamsOf(user, x, cpu, cpuGives))).warning === null;
	if ((await worth(userGives)) > 0 && (await legal(userGives))) {
		return teams;
	}
	// What the user could add: its players and nearest picks not on the
	// table, the CPU team's favourite first.
	const held = await view.holdingsOf(user);
	const on = new Set([...userGives.players, ...userGives.picks]);
	const pieces: (Player | DraftPick)[] = [
		...held.players.filter((p) => !isUntradable(p).untradable),
		...[...held.picks]
			.filter((dp) => typeof dp.season === "number")
			.sort(
				(a, b) =>
					(a.season as number) - (b.season as number) ||
					a.round - b.round ||
					a.dpid - b.dpid,
			)
			.slice(0, 4),
	].filter((x) => !on.has(x));
	await ensureRetention(
		view.outlook,
		pieces.filter((x): x is Player => "pid" in x),
		cpu,
	);
	const one = (x: Player | DraftPick): Holdings =>
		"pid" in x ? { players: [x], picks: [] } : { players: [], picks: [x] };
	const plus = (h: Holdings, x: Player | DraftPick): Holdings =>
		"pid" in x
			? { players: [...h.players, x], picks: h.picks }
			: { players: h.players, picks: [...h.picks, x] };
	const less = (h: Holdings, x: Player | DraftPick): Holdings => ({
		players: h.players.filter((p) => p !== x),
		picks: h.picks.filter((dp) => dp !== x),
	});
	const cpuWants = new Map<Player | DraftPick, number>();
	for (const x of pieces) {
		cpuWants.set(x, view.useOfNow(cpu, one(x)));
	}
	pieces.sort((a, b) => cpuWants.get(b)! - cpuWants.get(a)!);
	const added: (Player | DraftPick)[] = [];
	let yes = false;
	for (const x of pieces) {
		if (added.length >= 5) {
			break;
		}
		const next = plus(userGives, x);
		if (!(await legal(next)) && added.length >= 4) {
			continue;
		}
		userGives = next;
		added.push(x);
		if ((await worth(userGives)) > 0 && (await legal(userGives))) {
			yes = true;
			break;
		}
	}
	if (!yes) {
		return undefined;
	}
	// Back what the user would miss most, while the CPU team still says yes.
	const userMisses = new Map<Player | DraftPick, number>();
	for (const x of added) {
		userMisses.set(x, view.useOfNow(user, one(x)));
	}
	for (const x of [...added].sort(
		(a, b) => userMisses.get(b)! - userMisses.get(a)!,
	)) {
		const without = less(userGives, x);
		if ((await worth(without)) > 0 && (await legal(without))) {
			userGives = without;
		}
	}
	return teamsOf(user, userGives, cpu, cpuGives);
};
