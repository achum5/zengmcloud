import { PHASE, PLAYER } from "../../../common/constants.ts";
import type { MoodComponents, Player } from "../../../common/types.ts";
import { last } from "../../../common/utils.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import moodComponents from "../player/moodComponents.ts";
import moodInfo from "../player/moodInfo.ts";
import { realisticFreeAgencyOn } from "./isOn.ts";
import { SLOT_MINUTES } from "./strength.ts";

// HOW A PLAYER CHOOSES BETWEEN OFFERS (owner's call: an offer market, every
// team on the same terms). Any team offers whatever money and years it likes;
// the player weighs each offer - the money and years against what he is
// asking, and the team: winning, the role he would have, his mood traits -
// and chooses among them with odds, or waits for more. An offer at his asking
// price from a team he feels nothing about is worth exactly as much to him as
// waiting.

export const offerSettings = {
	// How much chance there is in his choice. An offer worth this much more to
	// him (in the log of money) is e times as likely to be chosen.
	temperature: 0.1,
	// Each point of mood toward a team is worth this much money to him, in the
	// log of money: 0.04 is about 4%. Stock asks up to 5% more a point from
	// teams a player dislikes, and the multiplayer board's rolls give 1.5 times
	// the odds a point - about what this and the temperature come to.
	moodWeight: 0.04,
	// What waiting for more is worth to him, against an offer at his asking
	// price from a team he is neutral on: in free agency, and once games are
	// played, when there is less to wait for.
	waitFreeAgency: 0,
	waitSeason: -0.1,
	// What testing free agency is worth to a player whose own team offers to
	// re-sign him, before the teams he likes elsewhere, against an offer at his
	// asking price from a team he is neutral on. Tuned in the arena so veterans
	// re-sign about as often as the NBA's unrestricted free agents (~32%).
	marketTesting: 0,
};

export type Offer = {
	tid: number;
	pid: number;
	amount: number;
	exp: number;
};

// The length he wants, in seasons, from the contract he is asking for.
export const wantedYears = (p: Pick<Player, "contract">) => {
	const first =
		g.get("season") + (g.get("phase") <= PHASE.AFTER_TRADE_DEADLINE ? 0 : 1);
	return Math.max(1, p.contract.exp - first + 1);
};

export const offerYears = (exp: number) => {
	const first =
		g.get("season") + (g.get("phase") <= PHASE.AFTER_TRADE_DEADLINE ? 0 : 1);
	return exp - first + 1;
};

// His asking price for a contract of this many years: his ask for the length
// he wants, and more for each year shorter or longer, as contract negotiation
// prices it (contractNegotiation/generateContractOptions).
export const askFor = (
	p: Pick<Player, "contract" | "ratings">,
	years: number,
) => {
	const ovr = last(p.ratings).ovr;
	const growthFactor = 0.15 + (ovr % 10) * 0.01 - 0.05;
	const off = Math.abs(years - wantedYears(p));
	return (
		Math.max(g.get("minContract"), p.contract.amount) * (1 + off * growthFactor)
	);
};

// What an offer is worth to him: the money against his ask for that length,
// and his feeling for the team (`appeal`, in points of mood).
export const offerUtility = ({
	amount,
	ask,
	appeal,
}: {
	amount: number;
	ask: number;
	appeal: number;
}) => Math.log(amount / ask) + offerSettings.moodWeight * appeal;

export const waitUtility = () =>
	g.get("phase") === PHASE.FREE_AGENCY ||
	g.get("phase") === PHASE.RESIGN_PLAYERS
		? offerSettings.waitFreeAgency
		: offerSettings.waitSeason;

// The chance of each choice, given what each is worth to him: the offers in
// order, then waiting (if `wait` is given). They add up to 1.
export const choiceOdds = (utilities: readonly number[], wait?: number) => {
	const all = wait === undefined ? [...utilities] : [...utilities, wait];
	const top = Math.max(...all);
	const weights = all.map((u) =>
		Math.exp((u - top) / offerSettings.temperature),
	);
	const total = weights.reduce((a, w) => a + w, 0);
	return weights.map((w) => w / total);
};

// The minutes he would get with a team, against the minutes a player as good
// as him gets on an average team, in points of mood: the role he would have.
// `others` are the players already there.
export const rolePoints = (
	p: Pick<Player, "valueNoPot" | "moodTraits">,
	others: readonly Pick<Player, "valueNoPot">[],
	leagueRank: number,
) => {
	const place = others.filter((x) => x.valueNoPot > p.valueNoPot).length;
	const there = SLOT_MINUTES[place] ?? 0;
	const usual =
		SLOT_MINUTES[Math.min(SLOT_MINUTES.length - 1, Math.floor(leagueRank))] ??
		0;
	// A tenth of the game is a point, as stock's playing time mood counts it.
	let points = helpers.bound((there - usual) * 10, -2, 2);
	if (g.get("playerMoodTraits")) {
		if (p.moodTraits.includes("F")) {
			points *= 2.5;
		}
		if (p.moodTraits.includes("W")) {
			points *= 0.5;
		}
	}
	return points;
};

const sumMood = (components: MoodComponents) => {
	let sum = 0;
	for (const key of helpers.keys(components)) {
		if (key === "custom") {
			for (const row of components.custom!) {
				sum += row.amount;
			}
		} else {
			sum += components[key];
		}
	}
	return sum;
};

// His feeling for a team, in points of mood: the game's own mood toward it
// (winning, hype, market, facilities, loyalty, his traits, ...) and the role
// he would have there. With his own team the game's mood already counts the
// minutes he got.
export const appealOf = async (
	p: Player,
	tid: number,
	roster: readonly Pick<Player, "valueNoPot">[],
	leagueRank: number,
) => {
	const mood = sumMood(await moodComponents(p, tid));
	if (p.tid === tid || (p.tid === PLAYER.FREE_AGENT && hadLastSeason(p, tid))) {
		return mood;
	}
	return mood + rolePoints(p, roster, leagueRank);
};

const hadLastSeason = (p: Pick<Player, "stats">, tid: number) =>
	p.stats.at(-1)?.tid === tid && p.stats.at(-1)?.season === g.get("season");

// What every player feels about each team before his own traits, place and
// history - its market, facilities, winning, hype and trades - and where each
// team's players stand, for weighing the market. Kept until the outlook is
// next rebuilt (clearMarketView).
type MarketView = {
	key: string;
	moods: Map<number, MoodComponents>;
	rosters: Map<number, number[]>;
	values: number[];
	// Each team's cap room next season, for what it has committed so far.
	room: Map<number, number>;
};
let marketView: MarketView | undefined;

export const clearMarketView = () => {
	marketView = undefined;
};

const NEUTRAL = {
	pid: -1,
	tid: PLAYER.FREE_AGENT,
	stats: [],
	relatives: [],
	moodTraits: [],
	contract: { amount: 0, exp: 0 },
} as unknown as Player;

const getMarketView = async () => {
	const key = `${g.get("lid")}:${g.get("season")}:${g.get("phase")}`;
	if (marketView?.key !== key) {
		const moods = new Map<number, MoodComponents>();
		const rosters = new Map<number, number[]>();
		for (const t of await idb.cache.teams.getAll()) {
			if (!t.disabled) {
				moods.set(t.tid, await moodComponents(NEUTRAL, t.tid));
				rosters.set(t.tid, []);
			}
		}
		const values: number[] = [];
		const committed = new Map<number, number>();
		for (const p of await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		])) {
			rosters.get(p.tid)?.push(p.valueNoPot);
			values.push(p.valueNoPot);
			if (p.contract.exp > g.get("season")) {
				committed.set(p.tid, (committed.get(p.tid) ?? 0) + p.contract.amount);
			}
		}
		values.sort((a, b) => b - a);
		const room = new Map<number, number>();
		for (const tid of moods.keys()) {
			room.set(tid, g.get("salaryCap") - (committed.get(tid) ?? 0));
		}
		marketView = { key, moods, rosters, values, room };
	}
	return marketView;
};

// A team's mood components as one player feels them, by his traits - as
// player/moodComponents modulates them.
const withTraits = (components: MoodComponents, traits: readonly string[]) => {
	const c = { ...components };
	if (g.get("playerMoodTraits")) {
		if (traits.includes("F")) {
			c.marketSize *= 2.5;
			c.hype *= 2.5;
			c.playingTime *= 2.5;
		}
		if (traits.includes("L")) {
			c.marketSize *= 0.5;
			c.loyalty *= 2.5;
			c.trades *= 2.5;
		}
		if (traits.includes("$")) {
			c.facilities *= 1.5;
			c.marketSize *= 0.5;
			c.teamPerformance *= 0.5;
		}
		if (traits.includes("W")) {
			c.marketSize *= 0.5;
			c.playingTime *= 0.5;
			c.teamPerformance *= 2.5;
		}
	}
	return c;
};

// Where a player of his value usually plays: his place in an average team's
// rotation.
const usualPlace = (values: readonly number[], value: number) => {
	let lo = 0;
	let hi = values.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (values[mid]! > value) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	return lo / Math.max(1, g.get("numActiveTeams"));
};

// What he expects testing the market to bring him, in the units of offers:
// the few teams he would like most that could pay his asking price (their cap
// room next season, or the minimum), taken together as a choice among them
// is (the log-sum of their weights) - about as many as bid for a free agent.
// A role player many teams could pay expects more from the market than a
// star few could.
const MARKET_TEAMS = 3;

export const marketUtility = async (p: Player, ownTid: number) => {
	const view = await getMarketView();
	const rank = usualPlace(view.values, p.valueNoPot);
	const minContract = g.get("minContract");
	const ask = Math.max(minContract, p.contract.amount);
	const { temperature, moodWeight } = offerSettings;
	const weights: number[] = [];
	for (const [tid, components] of view.moods) {
		if (tid === ownTid) {
			continue;
		}
		const others = (view.rosters.get(tid) ?? []).map((valueNoPot) => ({
			valueNoPot,
		}));
		const appeal =
			sumMood(withTraits(components, p.moodTraits)) +
			rolePoints(p, others, rank);
		const afford = Math.max(minContract, view.room.get(tid) ?? 0);
		weights.push(
			Math.exp(
				(moodWeight * appeal + Math.log(Math.min(1, afford / ask))) /
					temperature,
			),
		);
	}
	weights.sort((a, b) => b - a);
	const weight = weights.slice(0, MARKET_TEAMS).reduce((a, w) => a + w, 0);
	return (
		offerSettings.marketTesting +
		temperature * Math.log(Math.max(weight, Number.MIN_VALUE))
	);
};

// His chance of re-signing with a team that offers his asking price for the
// length he wants, against testing the market - by stock's rules, his chance
// of being willing to.
export const stayChance = async (p: Player, tid: number) => {
	const own = p.tid === tid ? p : { ...p, tid };
	if (!realisticFreeAgencyOn()) {
		return (await moodInfo(own, tid)).probWilling;
	}
	const appeal = sumMood(await moodComponents(own, tid));
	const [stay] = choiceOdds(
		[offerSettings.moodWeight * appeal],
		await marketUtility(p, tid),
	);
	return stay!;
};
