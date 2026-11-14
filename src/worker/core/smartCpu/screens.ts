import type { DraftPick, Player, TradeTeams } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import isUntradable from "../trade/isUntradable.ts";
import summary from "../trade/summary.ts";
import { currentOutlook } from "../realisticFrontOffice/market.ts";
import { ensureRetention } from "../realisticFrontOffice/outlook.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import { salaryRulesAllow } from "./tradeRules.ts";
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

// For probes: each step of what would make this work.
export const smartScreensDebug: { log?: (line: string) => void } = {};

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
// added until it says yes by its full worth; where the trade rules on salary
// need one side to send out more, that side adds the players it would miss
// least (as NBA teams fill a trade); then what the user would miss most is
// taken back while the CPU team still says yes. Undefined if nothing would
// - a deal it would take that no filler makes legal comes back as it is,
// for the screen to say so.
export const smartMakeItWork = async (teams: TradeTeams) => {
	const view = await ready();
	const user = teams[0].tid;
	const cpu = teams[1].tid;
	let cpuGives = await view.assetsOf({
		pids: teams[1].pids,
		dpids: teams[1].dpids,
	});
	let userGives = await view.assetsOf({
		pids: teams[0].pids,
		dpids: teams[0].dpids,
	});
	const result = (u: Holdings, c: Holdings): TradeTeams => {
		const [t0, t1] = teamsOf(user, u, cpu, c);
		return [
			{
				...t0,
				pidsExcluded: teams[0].pidsExcluded,
				dpidsExcluded: teams[0].dpidsExcluded,
			},
			{
				...t1,
				pidsExcluded: teams[1].pidsExcluded,
				dpidsExcluded: teams[1].dpidsExcluded,
			},
		];
	};
	const yes = async (u: Holdings, c: Holdings) =>
		(await view.dealWorth(cpu, user, c, u)) > 0;
	const legal = async (u: Holdings, c: Holdings) =>
		(await summary(result(u, c))).warning === null;
	if ((await yes(userGives, cpuGives)) && (await legal(userGives, cpuGives))) {
		return teams;
	}
	const userPayroll = await view.tradePayroll(user);
	const cpuPayroll = await view.tradePayroll(cpu);
	const salary = (h: Holdings) =>
		h.players.reduce((sum, p) => sum + contractOf(p).amount, 0);
	// The side the rules on salary want to send out more.
	const cpuShort = (u: Holdings, c: Holdings) =>
		!salaryRulesAllow(cpu, cpuPayroll, salary(u), salary(c));
	const userShort = (u: Holdings, c: Holdings) =>
		!salaryRulesAllow(user, userPayroll, salary(c), salary(u));
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
	const on = (h: Holdings, x: Player | DraftPick) =>
		h.players.includes(x as Player) || h.picks.includes(x as DraftPick);
	const userExcluded = new Set([
		...teams[0].pidsExcluded,
		...teams[0].dpidsExcluded.map((dpid) => -1 - dpid),
	]);
	const cpuExcluded = new Set(teams[1].pidsExcluded);
	const tradable = (p: Player) => !isUntradable(p).untradable;
	// What the user could add: its players and nearest picks, the CPU team's
	// favourite first.
	const userHeld = await view.holdingsOf(user);
	const pieces: (Player | DraftPick)[] = [
		...userHeld.players.filter((p) => tradable(p) && !userExcluded.has(p.pid)),
		...[...userHeld.picks]
			.filter(
				(dp) =>
					typeof dp.season === "number" && !userExcluded.has(-1 - dp.dpid),
			)
			.sort(
				(a, b) =>
					(a.season as number) - (b.season as number) ||
					a.round - b.round ||
					a.dpid - b.dpid,
			)
			.slice(0, 4),
	];
	await ensureRetention(
		view.outlook,
		pieces.filter((x): x is Player => "pid" in x),
		cpu,
	);
	const cpuWants = new Map<Player | DraftPick, number>();
	const userMisses = new Map<Player | DraftPick, number>();
	for (const x of pieces) {
		cpuWants.set(x, view.useOfNow(cpu, one(x)));
		userMisses.set(x, view.useOfNow(user, one(x)));
	}
	const wanted = [...pieces].sort(
		(a, b) => cpuWants.get(b)! - cpuWants.get(a)!,
	);
	const userFillers = pieces
		.filter((x): x is Player => "pid" in x)
		.sort((a, b) => userMisses.get(a)! - userMisses.get(b)!);
	// What the CPU team could add to fill: the players it would miss least.
	const cpuHeld = await view.holdingsOf(cpu);
	const cpuUses = view.usesOf(cpu, cpuHeld);
	const cpuFillers = cpuHeld.players
		.filter((p) => tradable(p) && !cpuExcluded.has(p.pid))
		.sort((a, b) => cpuUses.get(a.pid)! - cpuUses.get(b.pid)! || a.pid - b.pid);
	// A filler for a side short of salary to send out: the one it would miss
	// least that settles the rules alone, or else its biggest salary.
	const fillerFor = (
		fillers: readonly Player[],
		has: Holdings,
		settles: (p: Player) => boolean,
	) => {
		const free = fillers.filter((p) => !on(has, p));
		return (
			free.find(settles) ??
			free.reduce<Player | undefined>(
				(best, p) =>
					!best || contractOf(p).amount > contractOf(best).amount ? p : best,
				undefined,
			)
		);
	};
	const added: (Player | DraftPick)[] = [];
	const cpuAdded: Player[] = [];
	let wantedAdded = 0;
	let cpuFilled = 0;
	let userFilled = 0;
	let done = false;
	for (let step = 0; step < 16; step++) {
		smartScreensDebug.log?.(
			`user gives ${userGives.players.map((p) => p.pid)} +${userGives.picks.length} picks, cpu gives ${cpuGives.players.map((p) => p.pid)}: worth ${(await view.dealWorth(cpu, user, cpuGives, userGives)).toFixed(4)}, ${(await summary(result(userGives, cpuGives))).warning}`,
		);
		// First what it takes for the CPU team to say yes, then the rules.
		if (!(await yes(userGives, cpuGives))) {
			const free = wanted.filter((y) => !on(userGives, y));
			if (free.length === 0 || wantedAdded >= 5) {
				break;
			}
			// One enough alone, the one the user would miss least - or else
			// the CPU team's favourite.
			let x: Player | DraftPick | undefined;
			for (const y of [...free].sort(
				(a, b) => userMisses.get(a)! - userMisses.get(b)!,
			)) {
				if (await yes(plus(userGives, y), cpuGives)) {
					x = y;
					break;
				}
			}
			x ??= free[0]!;
			userGives = plus(userGives, x);
			added.push(x);
			wantedAdded += 1;
			continue;
		}
		if (cpuShort(userGives, cpuGives)) {
			const p = fillerFor(
				cpuFillers,
				cpuGives,
				(q) => !cpuShort(userGives, plus(cpuGives, q)),
			);
			if (!p || cpuFilled >= 3) {
				break;
			}
			cpuGives = plus(cpuGives, p);
			cpuAdded.push(p);
			cpuFilled += 1;
			continue;
		}
		if (userShort(userGives, cpuGives)) {
			const p = fillerFor(
				userFillers,
				userGives,
				(q) => !userShort(plus(userGives, q), cpuGives),
			);
			if (!p || userFilled >= 3) {
				break;
			}
			userGives = plus(userGives, p);
			added.push(p);
			userFilled += 1;
			continue;
		}
		done = true;
		break;
	}
	if (!done) {
		return undefined;
	}
	// Back what the user would miss most, while the CPU team still says yes
	// and the rules still allow it.
	for (const x of [...added].sort(
		(a, b) => userMisses.get(b)! - userMisses.get(a)!,
	)) {
		const without = less(userGives, x);
		if (
			!cpuShort(without, cpuGives) &&
			!userShort(without, cpuGives) &&
			(await yes(without, cpuGives))
		) {
			userGives = without;
		}
	}
	// And any filler of its own it no longer needs to send.
	for (const p of [...cpuAdded].reverse()) {
		const without = less(cpuGives, p);
		if (
			!cpuShort(userGives, without) &&
			!userShort(userGives, without) &&
			(await yes(userGives, without))
		) {
			cpuGives = without;
		}
	}
	return result(userGives, cpuGives);
};
