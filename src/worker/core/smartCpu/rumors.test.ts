import { assert, beforeEach, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { PHASE } from "../../../common/constants.ts";
import type { DraftPick, Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { player } from "../index.ts";
import type { MarketTrace } from "./market.ts";
import { beginTalks, endTalks, noteTalk, tellRumors } from "./rumors.ts";

// THE RUMOR MILL: a moment's talks told as stories - the furthest each got,
// the best players and closest talks first, one story a team, and the same
// player again only once his talks go further.

const teams = [
	{ tid: 0, region: "Los Angeles", name: "Clippers", abbrev: "LAC" },
	{ tid: 1, region: "Portland", name: "Trail Blazers", abbrev: "POR" },
	{ tid: 2, region: "Utah", name: "Jazz", abbrev: "UTA" },
	{ tid: 3, region: "Miami", name: "Heat", abbrev: "MIA" },
];

let players: Player[];

beforeEach(async () => {
	resetG();
	players = [];
	for (let i = 0; i < 8; i++) {
		const p = player.generate(i % 4, 26, 2016, true, DEFAULT_LEVEL) as Player;
		p.pid = i;
		p.firstName = "Player";
		p.lastName = String(i);
		p.value = 70 - i;
		players.push(p);
	}
	await resetCache({ players, events: [] });
	g.setWithoutSavingToDB("teamInfoCache", teams as any);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
	g.setWithoutSavingToDB("smartMarketMoment", "2016:1:week:2016-01-03");
	g.setWithoutSavingToDB("nbaCalendar", false);
});

const pick = (round: number) => ({ dpid: round, round }) as DraftPick;

const moment = async (traces: MarketTrace[]) => {
	beginTalks();
	try {
		for (const t of traces) {
			noteTalk(t);
		}
		await tellRumors([]);
	} finally {
		endTalks();
	}
	return (await idb.cache.events.getAll()).filter((e) => e.type === "rumor");
};

const strip = (html: string | undefined) =>
	(html ?? "").replace(/<[^>]+>/g, "");

test("a talk is told by the furthest it got, with the offer in words", async () => {
	const p = players[0]!;
	const offer = {
		players: [players[5]!, players[1]!],
		picks: [pick(1), pick(1), pick(2)],
	};
	const rumors = await moment([
		{ stage: "shopped", seller: 0, p, buyers: [1, 2] },
		{
			stage: "seller says no",
			seller: 0,
			buyer: 1,
			p,
			sellerGain: -0.004,
			offer,
		},
	]);
	assert.strictEqual(rumors.length, 1);
	const r = rumors[0]!;
	assert.deepEqual(r.tids, [1, 0]);
	assert.deepEqual(r.pids, [0]);
	assert.strictEqual(r.rumor?.tier, 2);
	const text = strip(r.text);
	// The better player first, then picks by round.
	assert.include(
		text,
		"Player 1, Player 5, two first-round picks and a second-round pick",
	);
	assert.notInclude(text, "???");
});

test("one story a team a moment, the closest talks first, and one team after a player is no story", async () => {
	g.setWithoutSavingToDB("smartMarketMoment", "2016:1:deadline:2016-02-01");
	const rumors = await moment([
		// The Clippers' best player, two teams after him (Portland, Utah).
		{ stage: "shopped", seller: 0, p: players[0]!, buyers: [1, 2] },
		// Utah's, close to a deal with Miami: the closest talk, told first.
		{
			stage: "within margin",
			seller: 2,
			buyer: 3,
			p: players[2]!,
			sellerGain: 0.004,
		},
		// Portland's, one team after him: no story.
		{ stage: "shopped", seller: 1, p: players[1]!, buyers: [3] },
		// The Clippers' fifth man, Portland's offer turned down.
		{
			stage: "seller says no",
			seller: 0,
			buyer: 1,
			p: players[4]!,
			sellerGain: -0.05,
			offer: { players: [], picks: [pick(1)] },
		},
	]);
	// The Clippers' best player's story would name Utah again.
	assert.deepEqual(
		rumors.map((r) => r.pids?.[0]),
		[2, 4],
	);
	// An offer far short of what his team wants is left out.
	assert.notInclude(strip(rumors[1]!.text), "first-round pick");
	// "the Trail Blazers' offer", never "Blazers's".
	assert.notInclude(rumors.map((r) => strip(r.text)).join(" "), "s's");
});

test("the same player again only once his talks go further", async () => {
	const p = players[0]!;
	const interest: MarketTrace = {
		stage: "shopped",
		seller: 0,
		p,
		buyers: [1, 2],
	};
	assert.strictEqual((await moment([interest])).length, 1);
	// The same interest a moment later: quiet.
	assert.strictEqual((await moment([interest])).length, 1);
	// An offer turned down: further, so told.
	const rumors = await moment([
		interest,
		{
			stage: "seller says no",
			seller: 0,
			buyer: 1,
			p,
			sellerGain: -0.002,
			offer: { players: [], picks: [pick(1)] },
		},
	]);
	assert.strictEqual(rumors.length, 2);
	assert.strictEqual(rumors[1]!.rumor?.tier, 2);
});

test("no talks are kept between moments", async () => {
	noteTalk({ stage: "shopped", seller: 0, p: players[0]!, buyers: [1, 2] });
	assert.strictEqual((await moment([])).length, 0);
});

test("a player his team must move is told as open for offers, with who called", async () => {
	const rumors = await moment([
		{
			stage: "shopped",
			seller: 0,
			p: players[4]!,
			buyers: [1, 2],
			mustMove: true,
		},
	]);
	assert.strictEqual(rumors.length, 1);
	assert.deepEqual(rumors[0]!.tids, [0, 1, 2]);
	const text = strip(rumors[0]!.text);
	assert.include(text, "Trail Blazers");
	assert.include(text, "Jazz");
});

test("a team after many players is a story of its own, then quiet a while", async () => {
	const busy = [0, 1, 2, 4, 5, 6].map((i): MarketTrace => ({
		stage: "shopped",
		seller: players[i]!.tid,
		p: players[i]!,
		buyers: [3],
	}));
	const rumors = await moment(busy);
	assert.strictEqual(rumors.length, 1);
	const r = rumors[0]!;
	assert.deepEqual(r.tids, [3]);
	assert.deepEqual(r.pids, [0, 1]);
	assert.strictEqual(r.rumor?.team, 3);
	assert.include(strip(r.text), "six players");
	assert.strictEqual((await moment(busy)).length, 1);
});

test("an offer of many players is told as a package built around its best two", async () => {
	const p = players[0]!;
	const rumors = await moment([
		{
			stage: "seller says no",
			seller: 0,
			buyer: 1,
			p,
			sellerGain: -0.003,
			offer: {
				players: [players[7]!, players[5]!, players[1]!, players[6]!],
				picks: [pick(1)],
			},
		},
	]);
	assert.strictEqual(rumors.length, 1);
	assert.include(
		strip(rumors[0]!.text),
		"a package built around Player 1 and Player 5, with a first-round pick",
	);
});
