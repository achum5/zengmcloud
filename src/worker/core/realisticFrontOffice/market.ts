import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import type {
	DraftPick,
	FrontOfficeBreakdown,
	Player,
	TradeTeams,
} from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import getDaysLeftSchedule from "../season/getDaysLeftSchedule.ts";
import getPayroll from "../team/getPayroll.ts";
import isUntradable from "../trade/isUntradable.ts";
import processTrade from "../trade/processTrade.ts";
import summary from "../trade/summary.ts";
import { explainDeal, type ExplainSide } from "./explain.ts";
import {
	draftPlaceMisread,
	placeBySeason,
	sideMargin,
	type MovedPlayer,
} from "./margin.ts";
import { notifyProposal, refreshProposals } from "./proposals.ts";
import { valueOverReplacement } from "./strength.ts";
import {
	buildOutlook,
	ensureRetention,
	franchiseValue,
	OUTLOOK_YEARS,
	pickLineAt,
	placesAfter,
	retentionOf,
	seasonLine,
	seasonValue,
	teamStrengths,
	type LetGoDecisions,
	type LeagueOutlook,
} from "./outlook.ts";
import { waitingValues } from "./timing.ts";
import type { TradedAway } from "./tradePenalty.ts";
import {
	closeWindows,
	holdDeal,
	isNotable,
	isReserved,
	loadReserved,
} from "./windows.ts";

// CPU TEAMS TRADING WITH EACH OTHER. Every day a few teams work the phones:
// each prices what every other team has, and what it has itself, by what it
// does to its own franchise value (outlook.ts), and looks for the deal both
// sides gain most from. A deal only happens if both teams come out ahead by
// their own reckoning - which, with every front office seeing the same truth,
// means their situations differ: a contender buying now from a team building
// for later, a team with two of something trading one for what it lacks.
//
// Players and picks, up to two assets a side, or nothing at all on one side -
// a player into another team's cap room. Salaries must match the way the
// trade screen requires. In the offseason, when rosters turn over, every team
// also gets to shop in a few windows (tradeWindow).

// Teams looking for a deal each day, and how many other teams each one calls.
const SHOPPERS_PER_DAY = 2;
// Teams a day that look again at what they would offer the human teams.
const PROPOSERS_PER_DAY = 1;
const PARTNERS_PER_SHOPPER = 6;

// The smallest gain, in titles, that makes a trade worth making for either
// side. Stops teams churning their rosters over rounding.
export const MIN_GAIN = 0.002;

// Deals priced exactly per pair of teams, after a quick screen of many.
const CANDIDATES_PER_PAIR = 10;

// What a team puts on the table: its best players, and its picks in the next
// few drafts. The end of the bench and picks a decade off move nothing.
const PLAYERS_ON_TABLE = 10;
const PICK_DRAFTS_ON_TABLE = 3;

export type TeamState = {
	tid: number;
	players: Player[];
	picks: DraftPick[];
	tradablePlayers: Player[];
	payroll: number;
	// Players traded away this season, last season and the one before - the
	// trade penalty's count.
	tradedAway: TradedAway;
	// Strength each season of the outlook, as the roster stands.
	path: number[];
	// Whom it lets go after their rookie deals, as the roster stands.
	letGo?: LetGoDecisions;
	value: number;
	// Its players and their injuries, for remembering what its assets are
	// worth while the roster stands.
	rosterKey: string;
};

export type Deal = {
	// What each side gives.
	give: [
		{ pids: number[]; dpids: number[] },
		{ pids: number[]; dpids: number[] },
	];
	tids: [number, number];
	gains: [number, number];
	// Each side's margin of error on its gain (margin.ts).
	margins: [number, number];
	assets: [Asset[], Asset[]];
};

// The smaller of the two sides' gains once each clears its margin of error.
const netOf = (deal: Pick<Deal, "gains" | "margins">) =>
	Math.min(deal.gains[0] - deal.margins[0], deal.gains[1] - deal.margins[1]);

// For the arena, to measure what a behaviour is worth by turning it off.
export const marketSettings = {
	valueWaiting: true,
	// Every team shops once the draft order is set, after the draft, and once
	// teams have re-signed their own players.
	offseasonWindows: true,
	// Teams shopping each day of free agency.
	freeAgencyShoppers: 4,
	// One side may give nothing.
	oneSided: true,
	// Picks for picks, no player moving.
	pickSwaps: true,
	// Every tradable player on the table, not only each team's best.
	wholeRoster: true,
	// Picks moved count towards the margin of error, as the players they are
	// expected to become.
	pickMargins: true,
	// On this many of the last trading days before the deadline, every team
	// shops, as in the offseason windows: the last chance to make a deal that
	// helps this season, and when a deal put off for the deadline gets decided.
	// 0 is off.
	deadlineWindow: 7,
	// Every team also shops once ratings have progressed in the preseason,
	// before any games.
	preseasonWindow: true,
	// A deal's teams' picks land where the deal leaves their rosters.
	placesAfter: true,
	// Packages of three assets, not only one or two.
	triples: true,
	// Which deals two CPU teams agree are held a trading day for other teams'
	// bids (windows.ts), over the league's setting.
	windows: undefined as "notable" | "all" | "off" | undefined,
};

// For the arena's version-against-version leagues: settings for some teams'
// own decisions, over marketSettings.
export const teamMarketSettings = new Map<
	number,
	Partial<typeof marketSettings>
>();
export const settingFor = <K extends keyof typeof marketSettings>(
	tid: number,
	key: K,
): (typeof marketSettings)[K] =>
	(teamMarketSettings.get(tid)?.[key] ??
		marketSettings[key]) as (typeof marketSettings)[K];

// Where the time goes, for the arena's speed report.
export const marketClock = {
	days: 0,
	ms: 0,
	outlooks: 0,
	outlookMs: 0,
	trades: 0,
	// Deals both teams wanted, put off because one would rather decide at the
	// deadline.
	deferred: 0,
	// Deals held a day for bids (windows.ts): how many, how many were made as
	// agreed, and the sides that took another team's offer instead.
	held: 0,
	heldKept: 0,
	heldTaken: 0,
};

// Called with every deal just before it is made, with both rosters as they
// stand - the arena's trade checker grades the market against the sim this way.
export const tradeWatchers: ((deal: {
	tids: [number, number];
	give: Deal["give"];
	gains: [number, number];
	rosters: [Player[], Player[]];
	picks: [DraftPick[], DraftPick[]];
	outlook: LeagueOutlook;
	breakdown: FrontOfficeBreakdown;
	margins: [number, number];
	// Before the deadline, what waiting was worth to each side (waitingOf).
	waiting?: ReturnType<typeof waitingOf>;
}) => void)[] = [];

// Called with every outlook as it is built - for the arena, to measure its
// projections.
export const outlookWatchers: ((outlook: LeagueOutlook) => void)[] = [];

// The outlook is rebuilt about once a week of games, keyed on league state
// alone, so any device in charge of simming builds the same one.
let cache: { key: string; outlook: LeagueOutlook } | undefined;

export const gamesPlayed = async () => {
	let total = 0;
	for (const ts of await idb.cache.teamSeasons.getAll()) {
		if (ts.season === g.get("season")) {
			total += ts.won + ts.lost + (ts.tied ?? 0) + (ts.otl ?? 0);
		}
	}
	return total;
};

const getOutlook = async (games: number) => {
	const week = Math.floor(games / Math.max(1, g.get("numActiveTeams") * 3));
	// The league too: a worker can open another league at the same point.
	const key = `${g.get("lid")}:${g.get("season")}:${g.get("phase")}:${week}`;
	if (cache?.key !== key) {
		const started = performance.now();
		cache = { key, outlook: await buildOutlook() };
		marketClock.outlooks += 1;
		marketClock.outlookMs += performance.now() - started;
		for (const watch of outlookWatchers) {
			watch(cache.outlook);
		}
	}
	return cache.outlook;
};

export const aiTids = async () => {
	const teams = await idb.cache.teams.getAll();
	return teams
		.filter(
			(t) =>
				!t.disabled &&
				((local.autoPlayUntil || g.get("spectator")) &&
				!g.get("challengeNoTrades")
					? true
					: !g.get("userTids").includes(t.tid)),
		)
		.map((t) => t.tid);
};

const valueOf = (
	outlook: LeagueOutlook,
	tid: number,
	players: readonly Player[],
	picks: readonly DraftPick[],
	trading?: { tradedAway: TradedAway; extra: number },
	known?: LetGoDecisions,
) =>
	franchiseValue(
		outlook,
		teamStrengths(outlook, tid, players, picks, trading, undefined, known),
	);

const tradedAwayOf = async (tid: number): Promise<TradedAway> => {
	const season = g.get("season");
	const count = async (s: number) =>
		(await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [s, tid]))
			?.numPlayersTradedAway ?? 0;
	return [
		await count(season),
		await count(season - 1),
		await count(season - 2),
	];
};

export const loadTeam = async (
	outlook: LeagueOutlook,
	tid: number,
): Promise<TeamState> => {
	const players = await idb.cache.players.indexGetAll("playersByTid", tid);
	// Anyone who joined since the outlook was built.
	await ensureRetention(outlook, players, tid);
	const picks = (
		await idb.cache.draftPicks.indexGetAll("draftPicksByTid", tid)
	).filter((dp) => typeof dp.season === "number");
	const letGo: LetGoDecisions = { decisions: new Map(), record: true };
	const path = teamStrengths(
		outlook,
		tid,
		players,
		picks,
		undefined,
		undefined,
		letGo,
	);
	letGo.record = false;
	return {
		tid,
		players,
		picks,
		letGo,
		tradablePlayers: players.filter((p) => !isUntradable(p).untradable),
		payroll: await getPayroll(tid),
		tradedAway: await tradedAwayOf(tid),
		path,
		value: franchiseValue(outlook, path),
		rosterKey: players
			.map((p) => `${p.pid}.${p.injury.gamesRemaining}`)
			.sort()
			.join(),
	};
};

export type Asset =
	| { kind: "player"; p: Player }
	| { kind: "pick"; dp: DraftPick };

const salaryOf = (assets: readonly Asset[]) =>
	assets.reduce(
		(a, x) => a + (x.kind === "player" ? x.p.contract.amount : 0),
		0,
	);

// The trade screen's salary rule: a team over the cap after the trade must
// take back no more than softCapTradeSalaryMatch% of what it sends.
export const salariesWork = (
	t: TeamState,
	outgoing: readonly Asset[],
	incoming: readonly Asset[],
) => {
	const out = salaryOf(outgoing);
	const inc = salaryOf(incoming);
	const after = t.payroll + inc - out;
	if (g.get("salaryCapType") === "none" || after <= g.get("salaryCap")) {
		return true;
	}
	if (g.get("salaryCapType") === "hard") {
		return inc <= out;
	}
	const ratio =
		out > 0 ? Math.floor((100 * inc) / out) : inc > 0 ? Infinity : 100;
	return ratio <= g.get("softCapTradeSalaryMatch");
};

const applyTrade = (
	t: TeamState,
	outgoing: readonly Asset[],
	incoming: readonly Asset[],
) => {
	const outPids = new Set(
		outgoing.flatMap((x) => (x.kind === "player" ? [x.p.pid] : [])),
	);
	const outDpids = new Set(
		outgoing.flatMap((x) => (x.kind === "pick" ? [x.dp.dpid] : [])),
	);
	return {
		players: [
			...t.players.filter((p) => !outPids.has(p.pid)),
			...incoming.flatMap((x) => (x.kind === "player" ? [x.p] : [])),
		],
		picks: [
			...t.picks.filter((dp) => !outDpids.has(dp.dpid)),
			...incoming.flatMap((x) => (x.kind === "pick" ? [x.dp] : [])),
		],
	};
};

// Two-asset packages are built from a team's most valuable assets only, and
// three-asset ones from fewer still.
const PAIR_ASSETS = 10;
const TRIPLE_ASSETS = 6;

// Every single asset, every pair of the ones that matter most to their team
// and every three of the very top - and nothing at all, for a one-sided deal.
export const packages = (
	assets: readonly Asset[],
	worth: (x: Asset) => number,
) => {
	const out: Asset[][] = assets.map((x) => [x]);
	if (marketSettings.oneSided) {
		out.push([]);
	}
	const top = [...assets]
		.sort((x, y) => worth(y) - worth(x))
		.slice(0, PAIR_ASSETS);
	for (let i = 0; i < top.length; i++) {
		for (let j = i + 1; j < top.length; j++) {
			out.push([top[i]!, top[j]!]);
		}
	}
	if (marketSettings.triples) {
		const few = top.slice(0, TRIPLE_ASSETS);
		for (let i = 0; i < few.length; i++) {
			for (let j = i + 1; j < few.length; j++) {
				for (let k = j + 1; k < few.length; k++) {
					out.push([few[i]!, few[j]!, few[k]!]);
				}
			}
		}
	}
	return out;
};

export const assetKey = (x: Asset) =>
	x.kind === "player" ? `p${x.p.pid}` : `d${x.dp.dpid}`;

// What a deal does to a team's franchise value. Priced in full - with the
// other team, `partner` - both teams' picks land where the deal leaves their
// rosters; the quick screens of one asset at a time leave them be.
// The last deal's places, for pricing it from the other side next: they are
// the same both ways.
let lastPlaces:
	| {
			outlook: LeagueOutlook;
			teams: readonly [TeamState, TeamState];
			rosterKeys: readonly [string, string];
			outgoing: readonly Asset[];
			incoming: readonly Asset[];
			view: LeagueOutlook;
	  }
	| undefined;

export const priceFor = (
	outlook: LeagueOutlook,
	t: TeamState,
	outgoing: readonly Asset[],
	incoming: readonly Asset[],
	partner?: TeamState,
) => {
	const after = applyTrade(t, outgoing, incoming);
	let view = outlook;
	if (
		settingFor(t.tid, "placesAfter") &&
		partner &&
		count(outgoing) + count(incoming) > 0
	) {
		const last = lastPlaces;
		if (
			last &&
			last.outlook === outlook &&
			last.teams[0] === partner &&
			last.teams[1] === t &&
			last.rosterKeys[0] === partner.rosterKey &&
			last.rosterKeys[1] === t.rosterKey &&
			last.outgoing === incoming &&
			last.incoming === outgoing
		) {
			view = last.view;
		} else {
			view = placesAfter(outlook, [
				{ tid: t.tid, before: t.players, after: after.players },
				{
					tid: partner.tid,
					before: partner.players,
					after: applyTrade(partner, incoming, outgoing).players,
				},
			]);
		}
		lastPlaces = {
			outlook,
			teams: [t, partner],
			rosterKeys: [t.rosterKey, partner.rosterKey],
			outgoing,
			incoming,
			view,
		};
	}
	return (
		valueOf(
			view,
			t.tid,
			after.players,
			after.picks,
			{
				tradedAway: t.tradedAway,
				extra: count(outgoing),
			},
			t.letGo,
		) - t.value
	);
};

// The best deal between two teams, if there is one both would make. `single`
// holds what one asset is worth to one team, gained or lost, for the day.
// What a team puts on the table: its tradable players, best first, and its
// picks in the next few drafts - none held in a deal waiting for bids.
export const tableAssets = (t: TeamState): Asset[] =>
	[
		...[...t.tradablePlayers]
			.sort((x, y) => y.valueNoPot - x.valueNoPot)
			.slice(0, marketSettings.wholeRoster ? undefined : PLAYERS_ON_TABLE)
			.map((p) => ({ kind: "player" as const, p })),
		...t.picks
			.filter(
				(dp) =>
					typeof dp.season === "number" &&
					dp.season <= g.get("season") + PICK_DRAFTS_ON_TABLE,
			)
			.map((dp) => ({ kind: "pick" as const, dp })),
	].filter((x) => !isReserved(x));

export const bestDealWith = (
	outlook: LeagueOutlook,
	a: TeamState,
	b: TeamState,
	single: Map<string, number>,
): Deal | undefined => {
	const assetsA = tableAssets(a);
	const assetsB = tableAssets(b);

	// One asset at a time, each way: what losing it costs its team and what
	// gaining it is worth to the other. The screen adds these up; the deals
	// that pass it are then priced whole.
	const price = (
		t: TeamState,
		outgoing: readonly Asset[],
		incoming: readonly Asset[],
	) => priceFor(outlook, t, outgoing, incoming);
	// Remembered while both the team's roster and the asset stand as they are.
	const key = (t: TeamState, sign: "-" | "+", x: Asset) =>
		`${t.tid}:${t.rosterKey}${sign}${assetKey(x)}${x.kind === "player" ? `.${x.p.injury.gamesRemaining}` : ""}`;
	const remember = (
		t: TeamState,
		sign: "-" | "+",
		x: Asset,
		compute: () => number,
	) => {
		const k = key(t, sign, x);
		if (!single.has(k)) {
			single.set(k, compute());
		}
	};
	for (const x of assetsA) {
		remember(a, "-", x, () => price(a, [x], []));
		remember(b, "+", x, () => price(b, [], [x]));
	}
	for (const y of assetsB) {
		remember(b, "-", y, () => price(b, [y], []));
		remember(a, "+", y, () => price(a, [], [y]));
	}
	const screen = (t: TeamState, outgoing: Asset[], incoming: Asset[]) =>
		outgoing.reduce((s, x) => s + single.get(key(t, "-", x))!, 0) +
		incoming.reduce((s, x) => s + single.get(key(t, "+", x))!, 0);

	// Each package's screen once: what it costs the team giving it and what it
	// is worth to the team getting it.
	const summarize = (giver: TeamState, getter: TeamState, pkgs: Asset[][]) =>
		pkgs.map((assets) => ({
			assets,
			cost: screen(giver, assets, []),
			worth: screen(getter, [], assets),
			hasPlayer: assets.some((x) => x.kind === "player"),
		}));
	const packagesA = summarize(
		a,
		b,
		packages(assetsA, (x) => -single.get(key(a, "-", x))!),
	);
	const packagesB = summarize(
		b,
		a,
		packages(assetsB, (x) => -single.get(key(b, "-", x))!),
	);
	const candidates: { giveA: Asset[]; giveB: Asset[]; screen: number }[] = [];
	for (const pa of packagesA) {
		for (const pb of packagesB) {
			if (pa.assets.length === 0 && pb.assets.length === 0) {
				continue;
			}
			if (!marketSettings.pickSwaps && !pa.hasPlayer && !pb.hasPlayer) {
				continue;
			}
			const worst = Math.min(pa.cost + pb.worth, pb.cost + pa.worth);
			if (worst > 0) {
				candidates.push({ giveA: pa.assets, giveB: pb.assets, screen: worst });
			}
		}
	}
	candidates.sort((x, y) => y.screen - x.screen);

	// Rosters only have to fit once games are played; till then a team can
	// carry extra players and cut the worst before the season.
	const maxRosterSize =
		g.get("phase") === PHASE.REGULAR_SEASON ? g.get("maxRosterSize") : Infinity;
	let best: Deal | undefined;
	let checked = 0;
	for (const { giveA, giveB } of candidates) {
		if (checked >= CANDIDATES_PER_PAIR) {
			break;
		}
		const sizeA = a.players.length + count(giveB) - count(giveA);
		const sizeB = b.players.length + count(giveA) - count(giveB);
		if (
			sizeA > maxRosterSize ||
			sizeB > maxRosterSize ||
			!salariesWork(a, giveA, giveB) ||
			!salariesWork(b, giveB, giveA)
		) {
			continue;
		}
		checked += 1;
		// A deal one side would not make needs no price from the other.
		const gainA = priceFor(outlook, a, giveA, giveB, b);
		if (gainA < MIN_GAIN) {
			continue;
		}
		const gains: [number, number] = [
			gainA,
			priceFor(outlook, b, giveB, giveA, a),
		];
		if (gains[1] < MIN_GAIN) {
			continue;
		}
		const margins: [number, number] = [
			marginOf(outlook, a, giveA, giveB),
			marginOf(outlook, b, giveB, giveA),
		];
		const candidate = { gains, margins };
		if (
			gains[0] - margins[0] >= MIN_GAIN &&
			gains[1] - margins[1] >= MIN_GAIN &&
			(!best || netOf(candidate) > netOf(best))
		) {
			best = {
				tids: [a.tid, b.tid],
				give: [split(giveA), split(giveB)],
				gains,
				margins,
				assets: [giveA, giveB],
			};
		}
	}
	return best;
};

// The margin of error on a team's gain from a deal: every player it gives or
// gets, at his place on the roster he is priced for, and every pick as the
// player it is expected to become.
export const marginOf = (
	outlook: LeagueOutlook,
	t: TeamState,
	outgoing: readonly Asset[],
	incoming: readonly Asset[],
) => {
	const after = applyTrade(t, outgoing, incoming).players;
	const moved: MovedPlayer[] = [];
	// A pick: what it is worth if its team is misread a little either way.
	const addPick = (dp: DraftPick, roster: readonly Player[]) => {
		const spread = draftPlaceMisread(outlook, dp);
		const worth = (at: ReturnType<typeof pickLineAt>) =>
			at ? at.line.availability * valueOverReplacement(at.line) : 0;
		const place: number[] = [];
		const availability: number[] = [];
		const error: number[] = [];
		for (let y = 0; y <= OUTLOOK_YEARS; y++) {
			const at = pickLineAt(outlook, dp, y, 0, t.tid);
			const early = pickLineAt(outlook, dp, y, -spread, t.tid);
			const late = pickLineAt(outlook, dp, y, spread, t.tid);
			const line = (at ?? early ?? late)?.line;
			place.push(
				line ? roster.filter((p) => p.valueNoPot > line.valueNoPot).length : 0,
			);
			availability.push(line ? 1 : 0);
			error.push(Math.abs(worth(early) - worth(late)) / 2);
		}
		moved.push({ place, availability, error });
	};
	const add = (p: Player, roster: readonly Player[]) => {
		const lines = Array.from({ length: OUTLOOK_YEARS + 1 }, (_, y) =>
			seasonLine(
				outlook.lines,
				p,
				outlook.season,
				outlook.offset,
				y,
				outlook.remaining.get(t.tid) ?? 0,
				retentionOf(outlook, p.pid, t.tid),
			),
		);
		const ovr = lines.map((line) => line.valueNoPot);
		moved.push({
			place: placeBySeason(
				roster.filter((x) => x !== p).map((x) => x.valueNoPot),
				ovr,
			),
			availability: lines.map((line) => line.availability),
			ovr,
		});
	};
	for (const x of incoming) {
		if (x.kind === "player") {
			add(x.p, after);
		} else if (marketSettings.pickMargins) {
			addPick(x.dp, after);
		}
	}
	for (const x of outgoing) {
		if (x.kind === "player") {
			add(x.p, t.players);
		} else if (marketSettings.pickMargins) {
			addPick(x.dp, t.players);
		}
	}
	return sideMargin(outlook, t.path, moved);
};

// A deal found earlier, as it stands today - for the arena, to see how long
// deals stay on offer. Undefined if a side no longer has what it was to give,
// or the deal no longer fits the rules.
export const priceDeal = (
	outlook: LeagueOutlook,
	a: TeamState,
	b: TeamState,
	give: Deal["give"],
) => {
	const assetsOf = (t: TeamState, side: Deal["give"][number]) => {
		const assets: Asset[] = [];
		for (const pid of side.pids) {
			const p = t.tradablePlayers.find((x) => x.pid === pid);
			if (!p) {
				return;
			}
			assets.push({ kind: "player", p });
		}
		for (const dpid of side.dpids) {
			const dp = t.picks.find((x) => x.dpid === dpid);
			if (!dp) {
				return;
			}
			assets.push({ kind: "pick", dp });
		}
		return assets;
	};
	const giveA = assetsOf(a, give[0]);
	const giveB = assetsOf(b, give[1]);
	const maxRosterSize =
		g.get("phase") === PHASE.REGULAR_SEASON ? g.get("maxRosterSize") : Infinity;
	if (
		!giveA ||
		!giveB ||
		a.players.length + count(giveB) - count(giveA) > maxRosterSize ||
		b.players.length + count(giveA) - count(giveB) > maxRosterSize ||
		!salariesWork(a, giveA, giveB) ||
		!salariesWork(b, giveB, giveA)
	) {
		return;
	}
	return {
		tids: [a.tid, b.tid] as [number, number],
		assets: [giveA, giveB] as [Asset[], Asset[]],
		gains: [
			priceFor(outlook, a, giveA, giveB, b),
			priceFor(outlook, b, giveB, giveA, a),
		] as [number, number],
		margins: [
			marginOf(outlook, a, giveA, giveB),
			marginOf(outlook, b, giveB, giveA),
		] as [number, number],
	};
};

export const count = (assets: readonly Asset[]) =>
	assets.filter((x) => x.kind === "player").length;

const split = (assets: readonly Asset[]) => ({
	pids: assets.flatMap((x) => (x.kind === "player" ? [x.p.pid] : [])),
	dpids: assets.flatMap((x) => (x.kind === "pick" ? [x.dp.dpid] : [])),
});

// The best deal this team can find with any of these teams, made or not.
export const findBestDeal = async (
	outlook: LeagueOutlook,
	a: TeamState,
	partners: readonly number[],
	single: Map<string, number>,
) => {
	let best: Deal | undefined;
	for (const otherTid of partners) {
		const b = await loadTeam(outlook, otherTid);
		// How each side's players would feel about re-signing with the other.
		await ensureRetention(outlook, b.tradablePlayers, a.tid);
		await ensureRetention(outlook, a.tradablePlayers, b.tid);
		const deal = bestDealWith(outlook, a, b, single);
		if (deal && (!best || netOf(deal) > netOf(best))) {
			best = deal;
		}
	}
	return best;
};

// The outlook the market is working from today.
export const currentOutlook = async () => getOutlook(await gamesPlayed());

// Before the deadline: what the deal is worth to each team made today, and
// decided on at the deadline (timing.ts). Either team that would rather wait
// puts it off.
export const waitingOf = (
	outlook: LeagueOutlook,
	deal: Pick<Deal, "tids" | "assets">,
	sides: readonly [TeamState, TeamState],
) => {
	const view = outlook.deadline!;
	// The two teams' picks land where the deal leaves them, as in priceFor.
	const traded = ([0, 1] as const).map((k) =>
		applyTrade(sides[k], deal.assets[k], deal.assets[k === 0 ? 1 : 0]),
	);
	const placed = marketSettings.placesAfter
		? placesAfter(
				outlook,
				([0, 1] as const).map((k) => ({
					tid: sides[k].tid,
					before: sides[k].players,
					after: traded[k]!.players,
				})),
			)
		: outlook;
	return ([0, 1] as const).map((k) => {
		const t = sides[k];
		const pathBefore = teamStrengths(outlook, t.tid, t.players, t.picks);
		const pathAfter = teamStrengths(
			settingFor(t.tid, "placesAfter") ? placed : outlook,
			t.tid,
			traded[k]!.players,
			traded[k]!.picks,
			{
				tradedAway: t.tradedAway,
				extra: count(deal.assets[k]),
			},
			undefined,
			t.letGo,
		);
		const later =
			franchiseValue(outlook, pathAfter) -
			seasonValue(outlook, 0, pathAfter[0]!) -
			(franchiseValue(outlook, pathBefore) -
				seasonValue(outlook, 0, pathBefore[0]!));
		const before = pathBefore[0]!;
		const after = pathAfter[0]!;
		return {
			before,
			after,
			later,
			...waitingValues({ view, before, after, later }),
		};
	});
};

// Called with every deal put off because a side would rather wait - for the
// arena, to see what is being left for the deadline.
export const deferWatchers: ((deal: {
	tids: [number, number];
	gains: [number, number];
	margins: [number, number];
	waiting: ReturnType<typeof waitingOf>;
}) => void)[] = [];

const timed = async (work: () => Promise<void>) => {
	const started = performance.now();
	try {
		await work();
	} finally {
		marketClock.days += 1;
		marketClock.ms += performance.now() - started;
	}
};

// A day of the season, or of free agency.
export const tradeDay = () =>
	timed(async () => {
		const phase = g.get("phase");
		if (phase === PHASE.REGULAR_SEASON) {
			await closeWindows();
			await shop(
				(await inDeadlineWindow()) ? Infinity : SHOPPERS_PER_DAY,
				await anotherTradingDay(),
			);
		} else if (phase === PHASE.FREE_AGENCY) {
			await closeWindows();
			await shop(marketSettings.freeAgencyShoppers, await anotherTradingDay());
		}
	});

// Whether there is a trading day after this one, before the deadline or the
// end of the regular season or of free agency - for a deal to wait for bids.
const anotherTradingDay = async () => {
	if (g.get("phase") === PHASE.FREE_AGENCY) {
		return g.get("daysLeft") > 1;
	}
	try {
		if ((await getDaysLeftSchedule("tradeDeadline")) <= 1) {
			return false;
		}
	} catch {
		// No deadline left this season.
	}
	return (await getDaysLeftSchedule()) > 1;
};

// The last days before the trade deadline (marketSettings.deadlineWindow).
const inDeadlineWindow = async () => {
	if (marketSettings.deadlineWindow <= 0) {
		return false;
	}
	try {
		return (
			(await getDaysLeftSchedule("tradeDeadline")) <
			marketSettings.deadlineWindow
		);
	} catch {
		// No deadline left this season.
		return false;
	}
};

// The offseason's trading windows, as each of these phases opens: every team
// gets to shop.
const WINDOW_PHASES: readonly number[] = [
	PHASE.DRAFT,
	PHASE.AFTER_DRAFT,
	PHASE.RESIGN_PLAYERS,
];

export const tradeWindow = async () => {
	if (
		(marketSettings.offseasonWindows &&
			WINDOW_PHASES.includes(g.get("phase"))) ||
		(marketSettings.preseasonWindow && g.get("phase") === PHASE.PRESEASON)
	) {
		await timed(async () => {
			await closeWindows();
			await shop(Infinity);
		});
	}
};

// The trade screen's own check of a deal: no warning.
export const legalDeal = async (deal: Pick<Deal, "tids" | "give">) =>
	!(
		await summary([
			{
				tid: deal.tids[0],
				pids: deal.give[0].pids,
				dpids: deal.give[0].dpids,
				pidsExcluded: [],
				dpidsExcluded: [],
			},
			{
				tid: deal.tids[1],
				pids: deal.give[1].pids,
				dpids: deal.give[1].dpids,
				pidsExcluded: [],
				dpidsExcluded: [],
			},
		])
	).warning;

// Makes a deal two CPU teams agreed, with its record for the trade history;
// false if the trade screen would not allow it.
export const makeDeal = async (
	outlook: LeagueOutlook,
	best: Deal,
	initiatorTid: number,
	sides: readonly [TeamState, TeamState],
	waiting?: ReturnType<typeof waitingOf>,
) => {
	const teams: TradeTeams = [
		{
			tid: best.tids[0],
			pids: best.give[0].pids,
			dpids: best.give[0].dpids,
			pidsExcluded: [],
			dpidsExcluded: [],
		},
		{
			tid: best.tids[1],
			pids: best.give[1].pids,
			dpids: best.give[1].dpids,
			pidsExcluded: [],
			dpidsExcluded: [],
		},
	];
	const tradeSummary = await summary(teams);
	if (tradeSummary.warning) {
		return false;
	}
	// Stamped on the trade, for the trade history: where each team stood
	// and what the deal was worth to it.
	const standing = (t: number) => {
		const o = outlook.teams.get(t);
		return o
			? `${o.strength[0]! >= 0 ? "+" : ""}${o.strength[0]!.toFixed(1)} now, ${(100 * o.title[0]!).toFixed(1)}% title`
			: "";
	};
	const assetsOf = (k: 0 | 1) => ({
		players: best.assets[k].flatMap((x) => (x.kind === "player" ? [x.p] : [])),
		picks: best.assets[k].flatMap((x) => (x.kind === "pick" ? [x.dp] : [])),
	});
	const sideFor = (k: 0 | 1): ExplainSide => ({
		tid: best.tids[k],
		players: sides[k].players,
		picks: sides[k].picks,
		tradedAway: sides[k].tradedAway,
		gain: best.gains[k],
		margin: best.margins[k],
		gives: assetsOf(k),
		gets: assetsOf(k === 0 ? 1 : 0),
	});
	const breakdown = explainDeal(outlook, [sideFor(0), sideFor(1)]);
	for (const watch of tradeWatchers) {
		watch({
			tids: best.tids,
			give: best.give,
			gains: best.gains,
			rosters: [sides[0].players, sides[1].players],
			picks: [sides[0].picks, sides[1].picks],
			outlook,
			breakdown,
			margins: best.margins,
			waiting,
		});
	}
	await processTrade(teams, undefined, {
		initiatorTid,
		tiers: [standing(best.tids[0]), standing(best.tids[1])],
		dv: Math.round(Math.min(...best.gains) * 10_000) / 10_000,
		motivation: `both gain: ${best.gains.map((x) => `+${(100 * x).toFixed(2)}`).join(" / ")} title-equivalents x100`,
		frontOffice: breakdown,
	});
	marketClock.trades += 1;
	return true;
};

// What one asset at a time is worth to each team, for the screens: kept for
// the outlook's life, under each team's roster as it was (TeamState.rosterKey).
const screens = new WeakMap<LeagueOutlook, Map<string, number>>();
const screensOf = (outlook: LeagueOutlook) => {
	let map = screens.get(outlook);
	if (!map) {
		map = new Map();
		screens.set(outlook, map);
	}
	return map;
};

// `holds`: notable deals wait a day for bids (windows.ts).
const shop = async (numShoppers: number, holds = false) => {
	const phase = g.get("phase");
	const tids = await aiTids();
	if (tids.length < 2) {
		return;
	}
	const games = await gamesPlayed();
	const outlook = await getOutlook(games);
	await loadReserved();
	const random = makeSeededRandom(
		g.get("season") * 100_003 + phase * 1009 + games + g.get("daysLeft") * 7,
	);

	const shoppers: number[] = [];
	const pool = [...tids];
	while (shoppers.length < numShoppers && pool.length > 0) {
		shoppers.push(pool.splice(Math.floor(random() * pool.length), 1)[0]!);
	}

	const single = screensOf(outlook);
	for (const tid of shoppers) {
		const a = await loadTeam(outlook, tid);
		const others = tids.filter((x) => x !== tid);
		const partners: number[] = [];
		while (partners.length < PARTNERS_PER_SHOPPER && others.length > 0) {
			partners.push(others.splice(Math.floor(random() * others.length), 1)[0]!);
		}
		const best = await findBestDeal(outlook, a, partners, single);
		if (!best) {
			continue;
		}
		const sides = [
			await loadTeam(outlook, best.tids[0]),
			await loadTeam(outlook, best.tids[1]),
		] as const;
		const waiting = outlook.deadline
			? waitingOf(outlook, best, sides)
			: undefined;
		if (
			marketSettings.valueWaiting &&
			waiting?.some((side) => side.wait > side.now)
		) {
			marketClock.deferred += 1;
			for (const watch of deferWatchers) {
				watch({
					tids: best.tids,
					gains: best.gains,
					margins: best.margins,
					waiting,
				});
			}
			continue;
		}
		if (holds && isNotable(best, sides, tid)) {
			// Held until the next trading day, for other teams to bid on
			// either side's assets (windows.ts).
			if (await legalDeal(best)) {
				await holdDeal(tid, best);
			}
			continue;
		}
		await makeDeal(outlook, best, tid, sides, waiting);
	}

	// Then what the day's first shopper would offer the human teams: one team
	// a day, each about every month - looking at a deal with a human takes
	// about a quarter of a second.
	const humanTids = g.get("userTids").filter((tid) => !tids.includes(tid));
	if (humanTids.length > 0) {
		for (const tid of shoppers.slice(0, PROPOSERS_PER_DAY)) {
			const fresh = await refreshProposals(outlook, tid, humanTids);
			for (const humanTid of fresh) {
				await notifyProposal(tid, humanTid);
			}
		}
	}
};
