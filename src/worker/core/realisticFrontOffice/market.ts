import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import type { DraftPick, Player, TradeTeams } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import getPayroll from "../team/getPayroll.ts";
import isUntradable from "../trade/isUntradable.ts";
import processTrade from "../trade/processTrade.ts";
import summary from "../trade/summary.ts";
import { sideMargin, type MovedPlayer } from "./margin.ts";
import {
	buildOutlook,
	ensureRetention,
	franchiseValue,
	OUTLOOK_YEARS,
	retentionOf,
	seasonLine,
	seasonValue,
	teamStrengths,
	type LeagueOutlook,
} from "./outlook.ts";
import { prefersWaiting } from "./timing.ts";
import type { TradedAway } from "./tradePenalty.ts";

// CPU TEAMS TRADING WITH EACH OTHER. Every day a few teams work the phones:
// each prices what every other team has, and what it has itself, by what it
// does to its own franchise value (outlook.ts), and looks for the deal both
// sides gain most from. A deal only happens if both teams come out ahead by
// their own reckoning - which, with every front office seeing the same truth,
// means their situations differ: a contender buying now from a team building
// for later, a team with two of something trading one for what it lacks.
//
// First version: players and picks, one to two assets a side; salaries must
// match the way the trade screen requires. Cap room, the trade penalty and
// re-signing intentions do not come into it yet.

// Teams looking for a deal each day, and how many other teams each one calls.
const SHOPPERS_PER_DAY = 2;
const PARTNERS_PER_SHOPPER = 6;

// The smallest gain, in titles, that makes a trade worth making for either
// side. Stops teams churning their rosters over rounding.
const MIN_GAIN = 0.002;

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
	value: number;
};

type Deal = {
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
};

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
}) => void)[] = [];

// The outlook is rebuilt about once a week of games, keyed on league state
// alone, so any device in charge of simming builds the same one.
let cache: { key: string; outlook: LeagueOutlook } | undefined;

const gamesPlayed = async () => {
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
	const key = `${g.get("season")}:${g.get("phase")}:${week}`;
	if (cache?.key !== key) {
		const started = performance.now();
		cache = { key, outlook: await buildOutlook() };
		marketClock.outlooks += 1;
		marketClock.outlookMs += performance.now() - started;
	}
	return cache.outlook;
};

const aiTids = async () => {
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
) =>
	franchiseValue(outlook, teamStrengths(outlook, tid, players, picks, trading));

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
	const path = teamStrengths(outlook, tid, players, picks);
	return {
		tid,
		players,
		picks,
		tradablePlayers: players.filter((p) => !isUntradable(p).untradable),
		payroll: await getPayroll(tid),
		tradedAway: await tradedAwayOf(tid),
		path,
		value: franchiseValue(outlook, path),
	};
};

type Asset = { kind: "player"; p: Player } | { kind: "pick"; dp: DraftPick };

const salaryOf = (assets: readonly Asset[]) =>
	assets.reduce(
		(a, x) => a + (x.kind === "player" ? x.p.contract.amount : 0),
		0,
	);

// The trade screen's salary rule: a team over the cap after the trade must
// take back no more than softCapTradeSalaryMatch% of what it sends.
const salariesWork = (
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

// Two-asset packages are built from a team's most valuable assets only.
const PAIR_ASSETS = 10;

// Every single asset, and every pair of the ones that matter most to their
// team.
const packages = (assets: readonly Asset[], worth: (x: Asset) => number) => {
	const out: Asset[][] = assets.map((x) => [x]);
	const top = [...assets]
		.sort((x, y) => worth(y) - worth(x))
		.slice(0, PAIR_ASSETS);
	for (let i = 0; i < top.length; i++) {
		for (let j = i + 1; j < top.length; j++) {
			out.push([top[i]!, top[j]!]);
		}
	}
	return out;
};

const assetKey = (x: Asset) =>
	x.kind === "player" ? `p${x.p.pid}` : `d${x.dp.dpid}`;

// The best deal between two teams, if there is one both would make. `single`
// holds what one asset is worth to one team, gained or lost, for the day.
export const bestDealWith = (
	outlook: LeagueOutlook,
	a: TeamState,
	b: TeamState,
	single: Map<string, number>,
): Deal | undefined => {
	const assetsOf = (t: TeamState): Asset[] => [
		...[...t.tradablePlayers]
			.sort((x, y) => y.valueNoPot - x.valueNoPot)
			.slice(0, PLAYERS_ON_TABLE)
			.map((p) => ({ kind: "player" as const, p })),
		...t.picks
			.filter(
				(dp) =>
					typeof dp.season === "number" &&
					dp.season <= g.get("season") + PICK_DRAFTS_ON_TABLE,
			)
			.map((dp) => ({ kind: "pick" as const, dp })),
	];
	const assetsA = assetsOf(a);
	const assetsB = assetsOf(b);

	// One asset at a time, each way: what losing it costs its team and what
	// gaining it is worth to the other. The screen adds these up; the deals
	// that pass it are then priced whole.
	const price = (
		t: TeamState,
		outgoing: readonly Asset[],
		incoming: readonly Asset[],
	) => {
		const after = applyTrade(t, outgoing, incoming);
		return (
			valueOf(outlook, t.tid, after.players, after.picks, {
				tradedAway: t.tradedAway,
				extra: count(outgoing),
			}) - t.value
		);
	};
	const remember = (key: string, compute: () => number) => {
		if (!single.has(key)) {
			single.set(key, compute());
		}
	};
	for (const x of assetsA) {
		remember(`${a.tid}-${assetKey(x)}`, () => price(a, [x], []));
		remember(`${b.tid}+${assetKey(x)}`, () => price(b, [], [x]));
	}
	for (const y of assetsB) {
		remember(`${b.tid}-${assetKey(y)}`, () => price(b, [y], []));
		remember(`${a.tid}+${assetKey(y)}`, () => price(a, [], [y]));
	}
	const screen = (t: TeamState, outgoing: Asset[], incoming: Asset[]) =>
		outgoing.reduce((s, x) => s + single.get(`${t.tid}-${assetKey(x)}`)!, 0) +
		incoming.reduce((s, x) => s + single.get(`${t.tid}+${assetKey(x)}`)!, 0);

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
		packages(assetsA, (x) => -single.get(`${a.tid}-${assetKey(x)}`)!),
	);
	const packagesB = summarize(
		b,
		a,
		packages(assetsB, (x) => -single.get(`${b.tid}-${assetKey(x)}`)!),
	);
	const candidates: { giveA: Asset[]; giveB: Asset[]; screen: number }[] = [];
	for (const pa of packagesA) {
		for (const pb of packagesB) {
			// At least one player moves: a straight swap of picks is not a
			// basketball trade.
			if (!pa.hasPlayer && !pb.hasPlayer) {
				continue;
			}
			const worst = Math.min(pa.cost + pb.worth, pb.cost + pa.worth);
			if (worst > 0) {
				candidates.push({ giveA: pa.assets, giveB: pb.assets, screen: worst });
			}
		}
	}
	candidates.sort((x, y) => y.screen - x.screen);

	const maxRosterSize = g.get("maxRosterSize");
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
		const gains: [number, number] = [
			price(a, giveA, giveB),
			price(b, giveB, giveA),
		];
		if (gains[0] < MIN_GAIN || gains[1] < MIN_GAIN) {
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
// gets, at his place on the roster he is priced for.
const marginOf = (
	outlook: LeagueOutlook,
	t: TeamState,
	outgoing: readonly Asset[],
	incoming: readonly Asset[],
) => {
	const after = applyTrade(t, outgoing, incoming).players;
	const moved: MovedPlayer[] = [];
	const add = (p: Player, roster: readonly Player[]) => {
		const place = [...roster]
			.sort((x, y) => y.valueNoPot - x.valueNoPot)
			.indexOf(p);
		moved.push({
			place,
			availability: Array.from(
				{ length: OUTLOOK_YEARS + 1 },
				(_, y) =>
					seasonLine(
						outlook.lines,
						p,
						outlook.season,
						outlook.offset,
						y,
						outlook.remaining.get(t.tid) ?? 0,
						retentionOf(outlook, p.pid, t.tid),
					).availability,
			),
		});
	};
	for (const x of incoming) {
		if (x.kind === "player") {
			add(x.p, after);
		}
	}
	for (const x of outgoing) {
		if (x.kind === "player") {
			add(x.p, t.players);
		}
	}
	return sideMargin(outlook, t.path, moved);
};

const count = (assets: readonly Asset[]) =>
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

// Before the deadline: whether either team would rather decide on this deal
// then than make it today (timing.ts).
const eitherWouldWait = async (outlook: LeagueOutlook, deal: Deal) => {
	const view = outlook.deadline!;
	const sides = [
		await loadTeam(outlook, deal.tids[0]),
		await loadTeam(outlook, deal.tids[1]),
	];
	for (const k of [0, 1] as const) {
		const t = sides[k]!;
		const j = k === 0 ? 1 : 0;
		const after = applyTrade(t, deal.assets[k], deal.assets[j]);
		const pathBefore = teamStrengths(outlook, t.tid, t.players, t.picks);
		const pathAfter = teamStrengths(
			outlook,
			t.tid,
			after.players,
			after.picks,
			{
				tradedAway: t.tradedAway,
				extra: count(deal.assets[k]),
			},
		);
		const later =
			franchiseValue(outlook, pathAfter) -
			seasonValue(outlook, 0, pathAfter[0]!) -
			(franchiseValue(outlook, pathBefore) -
				seasonValue(outlook, 0, pathBefore[0]!));
		if (
			prefersWaiting({
				view,
				before: pathBefore[0]!,
				after: pathAfter[0]!,
				later,
			})
		) {
			return true;
		}
	}
	return false;
};

export const tradeDay = async () => {
	const started = performance.now();
	try {
		await tradeDayInner();
	} finally {
		marketClock.days += 1;
		marketClock.ms += performance.now() - started;
	}
};

const tradeDayInner = async () => {
	const phase = g.get("phase");
	if (
		phase !== PHASE.REGULAR_SEASON &&
		phase !== PHASE.FREE_AGENCY &&
		phase !== PHASE.RESIGN_PLAYERS &&
		phase !== PHASE.AFTER_DRAFT &&
		phase !== PHASE.PRESEASON
	) {
		return;
	}
	const tids = await aiTids();
	if (tids.length < 2) {
		return;
	}
	const games = await gamesPlayed();
	const outlook = await getOutlook(games);
	const random = makeSeededRandom(
		g.get("season") * 100_003 + phase * 1009 + games + g.get("daysLeft") * 7,
	);

	const shoppers: number[] = [];
	const pool = [...tids];
	while (shoppers.length < SHOPPERS_PER_DAY && pool.length > 0) {
		shoppers.push(pool.splice(Math.floor(random() * pool.length), 1)[0]!);
	}

	const single = new Map<string, number>();
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
		if (
			marketSettings.valueWaiting &&
			outlook.deadline &&
			(await eitherWouldWait(outlook, best))
		) {
			marketClock.deferred += 1;
			continue;
		}
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
			continue;
		}
		// Stamped on the trade, for the trade history: where each team stood
		// and what the deal was worth to it.
		const standing = (t: number) => {
			const o = outlook.teams.get(t);
			return o
				? `${o.strength[0]! >= 0 ? "+" : ""}${o.strength[0]!.toFixed(1)} now, ${(100 * o.title[0]!).toFixed(1)}% title`
				: "";
		};
		for (const watch of tradeWatchers) {
			watch({
				tids: best.tids,
				give: best.give,
				gains: best.gains,
				rosters: [
					await idb.cache.players.indexGetAll("playersByTid", best.tids[0]),
					await idb.cache.players.indexGetAll("playersByTid", best.tids[1]),
				],
				picks: [
					await idb.cache.draftPicks.indexGetAll(
						"draftPicksByTid",
						best.tids[0],
					),
					await idb.cache.draftPicks.indexGetAll(
						"draftPicksByTid",
						best.tids[1],
					),
				],
				outlook,
			});
		}
		await processTrade(teams, undefined, {
			initiatorTid: tid,
			tiers: [standing(best.tids[0]), standing(best.tids[1])],
			dv: Math.round(Math.min(...best.gains) * 10_000) / 10_000,
			motivation: `both gain: ${best.gains.map((x) => `+${(100 * x).toFixed(2)}`).join(" / ")} title-equivalents x100`,
		});
		marketClock.trades += 1;
		// What these two teams' assets are worth has changed.
		for (const key of single.keys()) {
			if (
				best.tids.some(
					(t) => key.startsWith(`${t}-`) || key.startsWith(`${t}+`),
				)
			) {
				single.delete(key);
			}
		}
	}
};
