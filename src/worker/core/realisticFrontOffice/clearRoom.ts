import type { Player, Team } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { contractOf } from "./contractOf.ts";
import {
	count,
	loadTeam,
	makeDeal,
	marginOf,
	MIN_GAIN,
	priceFor,
	releaseAlternative,
	salariesWork,
	settingFor,
	singlePrice,
	type Asset,
	type Deal,
	type TeamState,
} from "./market.ts";
import {
	franchiseValue,
	teamStrengths,
	type LeagueOutlook,
	type LetGoDecisions,
} from "./outlook.ts";
import { checkFor, mostFor } from "./stages.ts";

// CLEARING ROOM FOR A FREE AGENT (owner's call, 2026-11-11). A CPU team that
// would gain more from a free agent than the cap rules let it pay him can
// trade salary away first: one to three of its players - and a pick or two,
// if that is what it takes - to a team with the room to take them. It offers
// him what it would with that room, if the trade and the signing together do
// more for it than its other offers, short and long term alike. The trade is
// made only if he takes the offer, just before he signs; if the other team
// would no longer take the players by then, the offer falls through.
export const clearRoomSettings = {
	on: true,
	// Free agents a team looks at clearing room for each stage: the ones it
	// would gain most from at their asks, among those it may not pay enough.
	targets: 2,
	// Players it would send, at most.
	players: 3,
	// Teams it calls to take them, and picks it may add.
	partners: 4,
	sweeteners: 2,
};

// How far clearing room got, for the arena and audits: teams with free
// agents worth it they may not pay enough, trades found, offers made with
// one, offers taken, trades made, and trades that fell through. `log`, when
// set, gets a line for each step.
export const roomClock = {
	wanted: 0,
	planned: 0,
	offered: 0,
	taken: 0,
	made: 0,
	failed: 0,
	log: undefined as string[] | undefined,
};

export type RoomPlan = {
	tid: number;
	partner: number;
	// What it sends: players, and picks if it took them.
	give: Asset[];
	// Salary off its books.
	freed: number;
	// The team after the trade, before the signing.
	state: TeamState;
	// What the trade alone does for it, past what it must beat besides
	// keeping what it has (releaseAlternative), and the margin of error.
	value: number;
	margin: number;
	// The other team's gain and margin.
	partnerGain: number;
	partnerMargin: number;
	// The free agent it is for.
	pid: number;
};

const salaryOf = (assets: readonly Asset[]) =>
	assets.reduce(
		(a, x) => a + (x.kind === "player" ? contractOf(x.p).amount : 0),
		0,
	);

// A team after sending these away.
export const stateAfter = (
	outlook: LeagueOutlook,
	t: TeamState,
	give: readonly Asset[],
): TeamState => {
	const outPids = new Set(
		give.flatMap((x) => (x.kind === "player" ? [x.p.pid] : [])),
	);
	const outDpids = new Set(
		give.flatMap((x) => (x.kind === "pick" ? [x.dp.dpid] : [])),
	);
	const players = t.players.filter((p) => !outPids.has(p.pid));
	const picks = t.picks.filter((dp) => !outDpids.has(dp.dpid));
	const letGo: LetGoDecisions = { decisions: new Map(), record: true };
	const path = teamStrengths(
		outlook,
		t.tid,
		players,
		picks,
		undefined,
		undefined,
		letGo,
	);
	letGo.record = false;
	return {
		...t,
		players,
		picks,
		letGo,
		tradablePlayers: t.tradablePlayers.filter((p) => !outPids.has(p.pid)),
		payroll: t.payroll - salaryOf(give),
		path,
		value: franchiseValue(outlook, path, t.tid),
		rosterKey: players
			.map((p) => `${p.pid}.${p.injury.gamesRemaining}`)
			.sort()
			.join(),
	};
};

// The least salary a team must shed to be allowed to pay him `amount`, if it
// can at all.
export const sheddingFor = (
	team: Pick<Team, "tid" | "midLevelSeason">,
	t: TeamState,
	p: Player,
	amount: number,
) => {
	const allows = (shed: number) =>
		(mostFor({
			t: team,
			p,
			payroll: t.payroll - shed,
			rosterSize: t.players.length,
		}) ?? -Infinity) >= amount;
	if (allows(0)) {
		return 0;
	}
	if (!allows(t.payroll)) {
		return undefined;
	}
	let lo = 0;
	let hi = t.payroll;
	for (let i = 0; i < 16; i++) {
		const mid = (lo + hi) / 2;
		if (allows(mid)) {
			hi = mid;
		} else {
			lo = mid;
		}
	}
	return hi;
};

// What a pick costs the team sending it for each bit it is worth to the
// team getting it, about (measured: 1.15 for a coming first, 1.5 for a far
// one) - for choosing the players to send, before any pick is priced.
const PICK_RATE = 1.3;
// Players looked at for a trade, by salary.
const PLAYER_OPTIONS = 12;

// Players it might send to shed `need`: the sets of up to `players` that
// free enough for the least - what losing them costs it, and what the other
// team would want with them, about (screened against `x`).
const playerSets = (
	outlook: LeagueOutlook,
	t: TeamState,
	need: number,
	keep: number,
	x: TeamState,
) => {
	const minContract = g.get("minContract");
	const options = t.tradablePlayers
		.filter((p) => p.pid !== keep && contractOf(p).amount > minContract)
		.sort((a, b) => contractOf(b).amount - contractOf(a).amount)
		.slice(0, PLAYER_OPTIONS)
		.map((p) => {
			const asset: Asset = { kind: "player", p };
			return {
				asset,
				salary: contractOf(p).amount,
				// What losing him costs it, his salary's room counted.
				cost: -singlePrice(outlook, t, "-", asset),
				// What he is worth to the other team past its margin of error.
				worth:
					singlePrice(outlook, x, "+", asset) -
					marginOf(outlook, x, [], [asset]),
			};
		});
	type Option = (typeof options)[number];
	const scored: { set: Option[]; score: number }[] = [];
	const consider = (set: Option[]) => {
		if (set.reduce((a, o) => a + o.salary, 0) < need) {
			return;
		}
		const cost = set.reduce((a, o) => a + o.cost, 0);
		const short = Math.max(0, -set.reduce((a, o) => a + o.worth, 0));
		scored.push({ set, score: cost + PICK_RATE * short });
	};
	const n = options.length;
	for (let i = 0; i < n; i++) {
		consider([options[i]!]);
		if (clearRoomSettings.players < 2) {
			continue;
		}
		for (let j = i + 1; j < n; j++) {
			consider([options[i]!, options[j]!]);
			if (clearRoomSettings.players < 3) {
				continue;
			}
			for (let k = j + 1; k < n; k++) {
				consider([options[i]!, options[j]!, options[k]!]);
			}
		}
	}
	return scored
		.sort((a, b) => a.score - b.score)
		.slice(0, 2)
		.map(({ set }) => set.map((o) => o.asset));
};

// A team that would take these players into its room, and the picks it
// takes to do it: none if it would take them as they are, else the pick or
// two that cover what it is short for the least to the sender.
const takerFor = (
	outlook: LeagueOutlook,
	t: TeamState,
	players: readonly Asset[],
	partners: readonly TeamState[],
) => {
	const picks = t.picks
		.filter((dp) => typeof dp.season === "number")
		.map((dp) => {
			const asset: Asset = { kind: "pick", dp };
			return { asset, cost: -singlePrice(outlook, t, "-", asset) };
		});
	const accepts = (x: TeamState, give: Asset[]) => {
		if (!salariesWork(t, give, [])) {
			return undefined;
		}
		const gain =
			priceFor(outlook, x, [], give, t) -
			settingFor(x.tid, "tradeFatigue") * x.trades;
		const margin = marginOf(outlook, x, [], give);
		return { gain, margin, ok: gain - margin >= MIN_GAIN };
	};
	let best:
		| {
				partner: TeamState;
				give: Asset[];
				gain: number;
				margin: number;
				cost: number;
		  }
		| undefined;
	for (const x of partners
		.filter((x) => x.tid !== t.tid && salariesWork(x, [], players))
		.sort((a, b) => a.payroll - b.payroll)
		.slice(0, clearRoomSettings.partners)) {
		const alone = accepts(x, [...players]);
		if (!alone) {
			continue;
		}
		if (alone.ok) {
			return {
				partner: x,
				give: [...players],
				gain: alone.gain,
				margin: alone.margin,
			};
		}
		if (clearRoomSettings.sweeteners <= 0) {
			continue;
		}
		// What it is short, and the picks that cover it for the least.
		const short = alone.margin + MIN_GAIN - alone.gain;
		const worth = (a: Asset) => singlePrice(outlook, x, "+", a);
		const combos: { assets: Asset[]; cost: number; worth: number }[] = [];
		for (let i = 0; i < picks.length; i++) {
			const a = picks[i]!;
			combos.push({ assets: [a.asset], cost: a.cost, worth: worth(a.asset) });
			if (clearRoomSettings.sweeteners < 2) {
				continue;
			}
			for (let j = i + 1; j < picks.length; j++) {
				const b = picks[j]!;
				combos.push({
					assets: [a.asset, b.asset],
					cost: a.cost + b.cost,
					worth: worth(a.asset) + worth(b.asset),
				});
			}
		}
		const tries = combos
			.filter((c) => c.worth >= short && (!best || c.cost < best.cost))
			.sort((a, b) => a.cost - b.cost)
			.slice(0, 3);
		for (const c of tries) {
			const give = [...players, ...c.assets];
			const priced = accepts(x, give);
			if (priced?.ok) {
				best = {
					partner: x,
					give,
					gain: priced.gain,
					margin: priced.margin,
					cost: c.cost,
				};
				break;
			}
		}
	}
	return best;
};

// The ways a team could shed enough salary to pay him `amount`: each a
// trade another team would make, and what it does for this one.
export const roomPlans = (
	outlook: LeagueOutlook,
	team: Pick<Team, "tid" | "midLevelSeason">,
	t: TeamState,
	p: Player,
	amount: number,
	partners: readonly TeamState[],
): RoomPlan[] => {
	const need = sheddingFor(team, t, p, amount);
	if (need === undefined || need <= 0) {
		return [];
	}
	const able = partners.filter(
		(x) => x.tid !== t.tid && x.payroll < g.get("salaryCap"),
	);
	if (able.length === 0) {
		return [];
	}
	// Screened against the team with the most room.
	const screen = able.reduce((a, b) => (b.payroll < a.payroll ? b : a));
	const plans: RoomPlan[] = [];
	for (const players of playerSets(outlook, t, need, p.pid, screen)) {
		const taker = takerFor(outlook, t, players, able);
		if (!taker) {
			continue;
		}
		const floor = releaseAlternative(outlook, t, taker.give, []);
		if (floor === undefined || !Number.isFinite(floor)) {
			continue;
		}
		const value =
			priceFor(outlook, t, taker.give, [], taker.partner) -
			floor -
			settingFor(t.tid, "tradeFatigue") * t.trades;
		plans.push({
			tid: t.tid,
			partner: taker.partner.tid,
			give: taker.give,
			freed: salaryOf(taker.give),
			state: stateAfter(outlook, t, taker.give),
			value,
			margin: marginOf(outlook, t, taker.give, []),
			partnerGain: taker.gain,
			partnerMargin: taker.margin,
			pid: p.pid,
		});
	}
	return plans;
};

// As he takes the offer: the trade, if both teams would still make it and
// it still clears the room. True once made.
export const carryOut = async (
	outlook: LeagueOutlook,
	plan: RoomPlan,
	p: Player,
	amount: number,
	name: string,
): Promise<boolean> => {
	const t = await loadTeam(outlook, plan.tid);
	const x = await loadTeam(outlook, plan.partner);
	const pids = new Set(t.players.map((q) => q.pid));
	const dpids = new Set(t.picks.map((dp) => dp.dpid));
	if (
		!plan.give.every((a) =>
			a.kind === "player" ? pids.has(a.p.pid) : dpids.has(a.dp.dpid),
		)
	) {
		return false;
	}
	// The players as they are now.
	const give: Asset[] = plan.give.map((a) =>
		a.kind === "player"
			? { kind: "player", p: t.players.find((q) => q.pid === a.p.pid)! }
			: a,
	);
	if (!salariesWork(x, [], give) || !salariesWork(t, give, [])) {
		return false;
	}
	const gain =
		priceFor(outlook, x, [], give, t) -
		settingFor(x.tid, "tradeFatigue") * x.trades;
	const margin = marginOf(outlook, x, [], give);
	if (gain - margin < MIN_GAIN) {
		return false;
	}
	const own = await idb.cache.teams.get(t.tid);
	if (
		!own ||
		!checkFor({
			t: own,
			p,
			payroll: t.payroll - salaryOf(give),
			amount,
			rosterSize: t.players.length - count(give),
		}).ok
	) {
		return false;
	}
	const deal: Deal = {
		tids: [t.tid, x.tid],
		give: [
			{
				pids: give.flatMap((a) => (a.kind === "player" ? [a.p.pid] : [])),
				dpids: give.flatMap((a) => (a.kind === "pick" ? [a.dp.dpid] : [])),
			},
			{ pids: [], dpids: [] },
		],
		gains: [plan.value, gain],
		margins: [plan.margin, margin],
		assets: [give, []],
	};
	return makeDeal(outlook, deal, t.tid, [t, x], undefined, [
		`Clears cap room to sign ${name}.`,
		undefined,
	]);
};
