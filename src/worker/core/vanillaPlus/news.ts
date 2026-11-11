import { PHASE } from "../../../common/constants.ts";
import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { vanillaPlusOn } from "../realisticFrontOffice/isOn.ts";
import {
	laneWatchers,
	marketWatchers,
	type LaneChange,
	type MarketEvent,
} from "./events.ts";

// THE NEWS (docs/vanilla-plus.md): stories about what front offices are
// really doing - a team changing direction, a contender hunting for help, a
// seller hearing offers, a trade and why each side made it. Only from what
// the market really did or considered; no valuations, no ratings; never a
// human team's plans. Saved as "frontOffice" events: the News Feed and the
// ticker show them.
export const newsSettings = {
	// Stories a day besides trades.
	perDay: 2,
	// A story about a player this good or better (stock's value).
	minValue: 58,
	// The same story about the same team or player again only after this many
	// market days.
	quietDays: 20,
};

type Item =
	| { type: "lane"; change: LaneChange }
	| { type: "market"; event: MarketEvent };

const queue: Item[] = [];
laneWatchers.push((change) => {
	if (vanillaPlusOn()) {
		queue.push({ type: "lane", change });
	}
});
marketWatchers.push((event) => {
	if (vanillaPlusOn()) {
		queue.push({ type: "market", event });
	}
});

// When a story was last told, by its subject.
const told = new Map<string, number>();
let day = 0;

const pick = <T>(xs: readonly T[]) =>
	xs[Math.floor(Math.random() * xs.length)]!;

const teamInfo = (tid: number) => g.get("teamInfoCache")[tid];

const teamLink = (tid: number, short = false) => {
	const t = teamInfo(tid);
	if (!t) {
		return "???";
	}
	return `<a href="${helpers.leagueUrl(["roster", `${t.abbrev}_${tid}`, g.get("season")])}">${short ? t.region : `${t.region} ${t.name}`}</a>`;
};

const playerLink = (p: Player) =>
	`<a href="${helpers.leagueUrl(["player", p.pid])}">${p.firstName} ${p.lastName}</a>`;

const age = (p: Player) => g.get("season") - p.born.year;

// Where a player stands in the league, in words.
const standing = (p: Player, rank: Map<number, number>) => {
	const r = rank.get(p.pid) ?? Infinity;
	return r <= 10
		? "one of the league's best players"
		: r <= 30
			? "an All-Star-caliber player"
			: r <= 90
				? "a starter"
				: "a rotation player";
};

const needOf = (p: Player) => {
	const pos = p.ratings.at(-1)!.pos;
	return pos.includes("C")
		? "a center"
		: pos === "PF" || pos === "F"
			? "size up front"
			: pos === "SF" || pos === "GF"
				? "a wing"
				: "backcourt help";
};

// The same need as a position, for "a hole at ...".
const holeOf = (p: Player) => {
	const pos = p.ratings.at(-1)!.pos;
	return pos.includes("C")
		? "center"
		: pos === "PF" || pos === "F"
			? "power forward"
			: pos === "SF" || pos === "GF"
				? "the wing"
				: "guard";
};

const recordOf = async (tid: number) => {
	const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
		g.get("season"),
		tid,
	]);
	return ts && ts.won + ts.lost > 0 ? `${ts.won}-${ts.lost}` : undefined;
};

const humans = () =>
	g.get("spectator") ? new Set<number>() : new Set(g.get("userTids"));

const fresh = (key: string) => {
	const last = told.get(key);
	return last === undefined || day - last >= newsSettings.quietDays;
};

type Story = { text: string; tids: number[]; pids: number[]; score: number };

// What a trade's other side got, in words.
const gotPhrase = async (teams: [number[], number[]], dpidsGot: number[]) => {
	const parts: string[] = [];
	const firsts = [];
	const seconds = [];
	for (const dpid of dpidsGot) {
		const dp = await idb.cache.draftPicks.get(dpid);
		if (dp?.round === 1) {
			firsts.push(dp);
		} else if (dp) {
			seconds.push(dp);
		}
	}
	const n = (k: number, word: string) =>
		`${["", "a", "two", "three", "four", "five"][k] ?? k} ${word}${k > 1 ? "s" : ""}`;
	const young: Player[] = [];
	for (const pid of teams[0]) {
		const p = await idb.cache.players.get(pid);
		if (p && age(p) <= 24) {
			young.push(p);
		}
	}
	if (firsts.length > 0) {
		parts.push(n(firsts.length, "first-round pick"));
	}
	if (young.length > 0) {
		parts.push(
			young.length === 1
				? `${age(young[0]!)}-year-old ${playerLink(young[0]!)}`
				: n(young.length, "young player"),
		);
	}
	if (seconds.length > 0 && parts.length === 0) {
		parts.push(n(seconds.length, "second-round pick"));
	}
	return parts.length > 0 ? parts.join(" and ") : "salary relief";
};

const tradeStory = async (
	event: Extract<MarketEvent, { kind: "trade" }>,
	rank: Map<number, number>,
): Promise<Story | undefined> => {
	const p = await idb.cache.players.get(event.pid);
	if (!p || p.value < newsSettings.minValue) {
		return;
	}
	const [a, b] = event.teams;
	// The buyer gets him; the seller gave him.
	const sellerSide = a.pids.includes(p.pid) ? a : b;
	const buyerSide = sellerSide === a ? b : a;
	const seller = sellerSide.tid;
	const buyer = buyerSide.tid;
	const got = await gotPhrase([buyerSide.pids, []], buyerSide.dpids);
	const record = await recordOf(buyer);
	const why: string[] = [];
	if (event.fit >= 1.1) {
		why.push(
			pick([
				`${teamLink(buyer, true)} needed ${needOf(p)}, and ${p.lastName} fills it.`,
				`The ${teamInfo(buyer)?.name} had a hole at ${holeOf(p)}; now they don't.`,
			]),
		);
	} else {
		// In the offseason the record is last season's, and the help is for the
		// next one.
		const inSeason = g.get("phase") === PHASE.REGULAR_SEASON;
		why.push(
			inSeason
				? `${teamLink(buyer, true)}${record ? `, ${record},` : ""} get ${standing(p, rank)} for the stretch ahead.`
				: `${teamLink(buyer, true)}${record ? `, coming off a ${record} season,` : ""} get ${standing(p, rank)} for next season.`,
		);
	}
	if (event.role === "seller" && event.offers >= 2) {
		why.push(
			pick([
				`${teamLink(seller, true)} heard ${event.offers} offers and took the best of them, according to a source familiar with the talks.`,
				`At least ${event.offers} teams made offers; ${teamLink(seller, true)} took the best, per league sources.`,
			]),
		);
	} else {
		why.push(`${teamLink(seller, true)} get ${got} in return.`);
	}
	const headline = pick([
		`${teamLink(buyer)} land ${playerLink(p)} from ${teamLink(seller, true)}.`,
		`${playerLink(p)} is headed to ${teamLink(buyer, true)}.`,
		`${teamLink(seller, true)} deal ${playerLink(p)} to the ${teamInfo(buyer)?.name}.`,
	]);
	return {
		text: `${headline} ${why.join(" ")}`,
		tids: [buyer, seller],
		pids: [p.pid],
		score: p.value >= 62 ? 20 : 12,
	};
};

const laneStory = async (change: LaneChange, players: Player[]) => {
	const t = teamInfo(change.tid);
	if (!t || !change.from || !fresh(`lane:${change.tid}`)) {
		return;
	}
	const record = await recordOf(change.tid);
	const roster = players
		.filter((p) => p.tid === change.tid)
		.sort((x, y) => y.value - x.value);
	const vet = roster.find((p) => age(p) >= 27);
	const team = teamLink(change.tid);
	const short = teamLink(change.tid, true);
	let text: string | undefined;
	if (change.to === "rebuild") {
		text = pick([
			`The ${t.name} are open for business. ${short}${record ? `, ${record},` : ""} will listen on anyone over 27${vet ? `, ${playerLink(vet)} included` : ""}, league sources say.`,
			`${team} are starting over, sources say: veterans for picks and young players${vet ? `, with ${playerLink(vet)} the biggest name available` : ""}.`,
		]);
	} else if (change.to === "allIn") {
		text = pick([
			`${team} are going for it. Sources describe the front office as aggressive buyers, ready to move picks and prospects for the right player.`,
			`${short}${record ? ` (${record})` : ""} have told teams they're buyers, per league sources, and they're willing to part with future picks.`,
		]);
	} else if (change.to === "retool" && change.from !== "rebuild") {
		text = pick([
			`${team} are retooling, not tearing down: expect older pieces${vet ? ` like ${playerLink(vet)}` : ""} to be available for younger starters, a source said.`,
			`Change is coming in ${short}. The ${t.name} are looking to get younger without bottoming out, according to people around the team.`,
		]);
	} else if (change.from === "rebuild" && change.to === "playoff") {
		text = `The rebuild is over in ${short}. ${team} are expected to add, not subtract, sources say.`;
	}
	if (!text) {
		return;
	}
	told.set(`lane:${change.tid}`, day);
	return {
		text,
		tids: [change.tid],
		pids: vet ? [vet.pid] : [],
		score: change.to === "rebuild" || change.to === "allIn" ? 12 : 10,
	};
};

const huntStory = async (
	event: Extract<MarketEvent, { kind: "hunt" }>,
	lanes: Map<number, string>,
) => {
	if (lanes.get(event.buyer) !== "allIn" || !fresh(`hunt:${event.buyer}`)) {
		return;
	}
	const targets: Player[] = [];
	for (const pid of event.pids) {
		const p = await idb.cache.players.get(pid);
		if (p && p.value >= newsSettings.minValue) {
			targets.push(p);
		}
	}
	if (targets.length === 0) {
		return;
	}
	const record = await recordOf(event.buyer);
	const names =
		targets.length === 1
			? playerLink(targets[0]!)
			: `${playerLink(targets[0]!)} and ${playerLink(targets[1]!)}`;
	told.set(`hunt:${event.buyer}`, day);
	return {
		text: pick([
			`Contending ${teamLink(event.buyer)}${record ? ` (${record})` : ""} have been canvassing the league for ${needOf(targets[0]!)}. ${names} ${targets.length === 1 ? "is" : "are"} among the names they've asked about, per sources.`,
			`${teamLink(event.buyer, true)} are looking for ${needOf(targets[0]!)}, and they've checked in on ${names}, league sources say.`,
		]),
		tids: [event.buyer, ...targets.map((p) => p.tid)],
		pids: targets.map((p) => p.pid),
		score: targets[0]!.value >= 65 ? 15 : 10,
	};
};

const shoppedStory = async (
	event: Extract<MarketEvent, { kind: "shopped" }>,
) => {
	if (event.offers < 2 || !fresh(`shopped:${event.pid}`)) {
		return;
	}
	const p = await idb.cache.players.get(event.pid);
	if (!p || p.value < newsSettings.minValue || p.tid !== event.seller) {
		return;
	}
	told.set(`shopped:${event.pid}`, day);
	return {
		text: pick([
			`At least ${event.offers} teams have called ${teamLink(event.seller)} about ${playerLink(p)}, according to league sources. No deal yet.`,
			`The market for ${playerLink(p)} is heating up: ${event.offers} teams have made offers, sources say, and ${teamLink(event.seller, true)} are taking their time.`,
		]),
		tids: [event.seller],
		pids: [p.pid],
		score: p.value >= 65 ? 15 : 10,
	};
};

// Write the day's stories.
export const flushNews = async () => {
	day += 1;
	if (queue.length === 0) {
		return;
	}
	const items = queue.splice(0, queue.length);
	const hidden = humans();
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	const rank = new Map(
		[...players]
			.sort((a, b) => b.value - a.value)
			.map((p, i) => [p.pid, i + 1]),
	);
	const lanes = new Map(
		(await idb.cache.teams.getAll()).map((t) => [t.tid, t.lane ?? ""]),
	);
	const trades: Story[] = [];
	const others: Story[] = [];
	for (const item of items) {
		let story: Story | undefined;
		if (item.type === "lane") {
			if (!hidden.has(item.change.tid)) {
				story = await laneStory(item.change, players);
			}
		} else if (item.event.kind === "trade") {
			story = await tradeStory(item.event, rank);
			if (story) {
				trades.push(story);
				continue;
			}
		} else if (item.event.kind === "hunt") {
			if (!hidden.has(item.event.buyer)) {
				story = await huntStory(item.event, lanes);
			}
		} else if (!hidden.has(item.event.seller)) {
			story = await shoppedStory(item.event);
		}
		if (story) {
			others.push(story);
		}
	}
	others.sort((a, b) => b.score - a.score);
	for (const story of [...trades, ...others.slice(0, newsSettings.perDay)]) {
		await logEvent({
			type: "frontOffice",
			text: story.text,
			tids: story.tids,
			pids: story.pids,
			score: story.score,
			showNotification: false,
		});
	}
};
