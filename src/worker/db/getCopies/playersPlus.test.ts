import { assert, beforeAll, describe, test } from "vitest";
import { PLAYER } from "../../../common/constants.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { player } from "../../core/index.ts";
import { idb } from "../index.ts";
import { g, helpers } from "../../util/index.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import type { Player } from "../../../common/types.ts";

let p: Player;
beforeAll(async () => {
	resetG();
	g.setWithoutSavingToDB("season", 2011);
	p = {
		pid: 0,
		...player.generate(PLAYER.UNDRAFTED, 19, 2011, false, DEFAULT_LEVEL),
	};
	p.tid = 4;
	g.setWithoutSavingToDB("season", 2012);
	await resetCache({
		players: [p],
	});
	p.contract.exp = g.get("season") + 1;
	player.addStatsRow(p, g.get("season"), false);
	player.addStatsRow(p, g.get("season"), true);
	player.addStatsRow(p, g.get("season"), false);
	const stats = p.stats;
	stats[0].gp = 5;
	stats[0].fg = 20;
	stats[1].gp = 3;
	stats[1].fg = 30;
	stats[2].season = 2013;
	stats[2].tid = 0;
	stats[2].gp = 8;
	stats[2].fg = 56;
	await player.develop(p, 0);

	player.addRatingsRow(p);
	await player.develop(p, 0);

	player.addRatingsRow(p);
	assert(p.ratings[2]);
	p.ratings[2].season = 2013;
	await player.develop(p, 0);

	player.addRatingsRow(p);
	assert(p.ratings[3]);
	p.ratings[3].season = 2014;
	await player.develop(p, 0);
});

test("return requested info if tid/season match", async () => {
	const pf = await idb.getCopy.playersPlus(p, {
		attrs: ["tid", "awards"],
		ratings: ["season", "ovr"],
		stats: ["season", "tid", "fg", "fgp", "per"],
		tid: 4,
		season: 2012,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.tid, 4);
	assert.strictEqual(pf.awards.length, 0);
	assert.strictEqual(pf.ratings.season, 2012);
	assert.strictEqual(typeof pf.ratings.ovr, "number");
	assert.strictEqual(Object.keys(pf.ratings).length, 2);
	assert.strictEqual(pf.stats.season, 2012);
	assert.strictEqual(pf.stats.tid, 4);
	assert.strictEqual(typeof pf.stats.fg, "number");
	assert.strictEqual(typeof pf.stats.fgp, "number");
	assert.strictEqual(typeof pf.stats.per, "number");
	assert.strictEqual(Object.keys(pf.stats).length, 6);
	assert(!Object.hasOwn(pf, "careerStats"));
	assert(!Object.hasOwn(pf, "careerStatsPlayoffs"));
});

test("return requested info if tid/season match for an array of player objects", async () => {
	const pf = await idb.getCopies.playersPlus([p, p], {
		attrs: ["tid", "awards"],
		ratings: ["season", "ovr"],
		stats: ["season", "tid", "fg", "fgp", "per"],
		tid: 4,
		season: 2012,
	});

	for (const i of [0, 1] as const) {
		assert.strictEqual(pf[i].tid, 4);
		assert.strictEqual(pf[i].awards.length, 0);
		assert.strictEqual(pf[i].ratings.season, 2012);
		assert.strictEqual(typeof pf[i].ratings.ovr, "number");
		assert.strictEqual(Object.keys(pf[i].ratings).length, 2);
		assert.strictEqual(pf[i].stats.season, 2012);
		assert.strictEqual(pf[i].stats.tid, 4);
		assert.strictEqual(typeof pf[i].stats.fg, "number");
		assert.strictEqual(typeof pf[i].stats.fgp, "number");
		assert.strictEqual(typeof pf[i].stats.per, "number");
		assert.strictEqual(Object.keys(pf[i].stats).length, 6);
		assert(!Object.hasOwn(pf[i], "careerStats"));
		assert(!Object.hasOwn(pf[i], "careerStatsPlayoffs"));
	}
});

test("return requested info if tid/season match, even when no attrs requested", async () => {
	const pf = await idb.getCopy.playersPlus(p, {
		ratings: ["season", "ovr"],
		stats: ["season", "tid", "fg", "fgp", "per"],
		tid: 4,
		season: 2012,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.ratings.season, 2012);
	assert.strictEqual(typeof pf.ratings.ovr, "number");
	assert.strictEqual(Object.keys(pf.ratings).length, 2);
	assert.strictEqual(pf.stats.season, 2012);
	assert.strictEqual(pf.stats.tid, 4);
	assert.strictEqual(typeof pf.stats.fg, "number");
	assert.strictEqual(typeof pf.stats.fgp, "number");
	assert.strictEqual(typeof pf.stats.per, "number");
	assert.strictEqual(Object.keys(pf.stats).length, 6);
	assert(!Object.hasOwn(pf, "careerStats"));
	assert(!Object.hasOwn(pf, "careerStatsPlayoffs"));
});

test("return requested info if tid/season match, even when no ratings requested", async () => {
	const pf = await idb.getCopy.playersPlus(p, {
		attrs: ["tid", "awards"],
		stats: ["season", "tid", "fg", "fgp", "per"],
		tid: 4,
		season: 2012,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.tid, 4);
	assert.strictEqual(pf.awards.length, 0);
	assert(!Object.hasOwn(pf, "ratings"));
	assert.strictEqual(pf.stats.season, 2012);
	assert.strictEqual(pf.stats.tid, 4);
	assert.strictEqual(typeof pf.stats.fg, "number");
	assert.strictEqual(typeof pf.stats.fgp, "number");
	assert.strictEqual(typeof pf.stats.per, "number");
	assert.strictEqual(Object.keys(pf.stats).length, 6);
	assert(!Object.hasOwn(pf, "careerStats"));
	assert(!Object.hasOwn(pf, "careerStatsPlayoffs"));
});

test("return requested info if tid/season match, even when no stats requested", async () => {
	const pf = await idb.getCopy.playersPlus(p, {
		attrs: ["tid", "awards"],
		ratings: ["season", "ovr"],
		tid: 4,
		season: 2012,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.tid, 4);
	assert.strictEqual(pf.awards.length, 0);
	assert.strictEqual(pf.ratings.season, 2012);
	assert.strictEqual(typeof pf.ratings.ovr, "number");
	assert.strictEqual(Object.keys(pf.ratings).length, 2);
	assert(!Object.hasOwn(pf, "stats"));
	assert(!Object.hasOwn(pf, "careerStats"));
	assert(!Object.hasOwn(pf, "careerStatsPlayoffs"));
});

test("return undefined if tid does not match any on record", async () => {
	const pf = await idb.getCopy.playersPlus(p, {
		attrs: ["tid", "awards"],
		ratings: ["season", "ovr"],
		stats: ["season", "tid", "fg", "fgp", "per"],
		tid: 5,
		season: 2012,
	});
	assert.strictEqual(pf, undefined);
});

test("return undefined if season does not match any on record", async () => {
	const pf = await idb.getCopy.playersPlus(p, {
		attrs: ["tid", "awards"],
		ratings: ["season", "ovr"],
		stats: ["season", "abbrev", "fg", "fgp", "per"],
		tid: 4,
		season: 2014,
	});
	assert.strictEqual(pf, undefined);
});
test('return season totals is options.statType is "totals", and per-game averages otherwise', async () => {
	let pf = await idb.getCopy.playersPlus(p, {
		stats: ["gp", "fg"],
		tid: 4,
		season: 2012,
		statType: "totals",
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.stats.gp, 5);
	assert.strictEqual(pf.stats.fg, 20);
	pf = await idb.getCopy.playersPlus(p, {
		stats: ["gp", "fg"],
		tid: 4,
		season: 2012,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.stats.gp, 5);
	assert.strictEqual(pf.stats.fg, 4);
});

test("return playoff stats if options.playoffs is true", async () => {
	const pf = await idb.getCopy.playersPlus(p, {
		stats: ["gp", "fg"],
		tid: 4,
		season: 2012,
		playoffs: true,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.stats[0].playoffs, false);
	assert.strictEqual(pf.stats[0].gp, 5);
	assert.strictEqual(pf.stats[0].fg, 4);
	assert.strictEqual(pf.stats[1].playoffs, true);
	assert.strictEqual(pf.stats[1].gp, 3);
	assert.strictEqual(pf.stats[1].fg, 10);
});

test("not return undefined with options.showNoStats even if tid does not match any on record", async () => {
	const pf = await idb.getCopy.playersPlus(p, {
		stats: ["gp", "fg"],
		tid: 5,
		season: 2012,
		showNoStats: true,
	});
	assert.strictEqual(typeof pf, "object");
});

test("not return undefined with options.showNoStats if season does not match any on record", async () => {
	const pf = await idb.getCopy.playersPlus(p, {
		stats: ["gp", "fg"],
		tid: 4,
		season: 2015,
		showNoStats: true,
	});
	assert.strictEqual(typeof pf, "object");
});

test("not return undefined with options.showRookies if the player was drafted this season", async () => {
	g.setWithoutSavingToDB("season", 2011);
	let pf = await idb.getCopy.playersPlus(p, {
		stats: ["gp", "fg"],
		tid: 5,
		season: 2011,
		showRookies: true,
	});
	assert.strictEqual(typeof pf, "object");
	g.setWithoutSavingToDB("season", 2012);
	pf = await idb.getCopy.playersPlus(p, {
		stats: ["gp", "fg"],
		tid: 5,
		season: 2011,
		showRookies: true,
	});
	assert.strictEqual(pf, undefined);
});

test("fuzz ratings if options.fuzz is true", async () => {
	let pf = await idb.getCopy.playersPlus(p, {
		ratings: ["ovr"],
		tid: 4,
		season: 2012,
		fuzz: false,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert(p.ratings[1]);

	assert.strictEqual(pf.ratings.ovr, p.ratings[1].ovr);
	pf = await idb.getCopy.playersPlus(p, {
		ratings: ["ovr"],
		tid: 4,
		season: 2012,
		fuzz: true,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	// This will break if ovr + fuzz is over 100 (should check bounds), but that never happens in practice
	assert.strictEqual(
		pf.ratings.ovr,
		Math.round(p.ratings[1].ovr + p.ratings[1].fuzz),
	);
});

test("return stats from previous season if options.oldStats is true and current season has no stats record", async () => {
	g.setWithoutSavingToDB("season", 2013);
	let pf = await idb.getCopy.playersPlus(p, {
		stats: ["gp", "fg"],
		tid: 0,
		season: 2013,
		oldStats: true,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.stats.gp, 8);
	assert.strictEqual(pf.stats.fg, 7);
	pf = await idb.getCopy.playersPlus(p, {
		stats: ["gp", "fg"],
		tid: 0,
		season: 2014,
		oldStats: false,
	});
	assert.strictEqual(pf, undefined);
	g.setWithoutSavingToDB("season", 2014);
	pf = await idb.getCopy.playersPlus(p, {
		stats: ["gp", "fg"],
		tid: 0,
		season: 2014,
		oldStats: true,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.stats.gp, 8);
	assert.strictEqual(pf.stats.fg, 7);
	g.setWithoutSavingToDB("season", 2012);
});

test("adjust cashOwed by options.numGamesRemaining", async () => {
	g.setWithoutSavingToDB("season", 2012);
	let pf = await idb.getCopy.playersPlus(p, {
		attrs: ["cashOwed"],
		tid: 4,
		season: 2012,
		numGamesRemaining: 82,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.cashOwed, (p.contract.amount * 2) / 1000);
	pf = await idb.getCopy.playersPlus(p, {
		attrs: ["cashOwed"],
		tid: 4,
		season: 2012,
		numGamesRemaining: 41,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.cashOwed, (p.contract.amount * 1.5) / 1000);
	pf = await idb.getCopy.playersPlus(p, {
		attrs: ["cashOwed"],
		tid: 4,
		season: 2012,
		numGamesRemaining: 0,
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.cashOwed, p.contract.amount / 1000);
});

test("return stats and ratings from all seasons and teams if no season or team is specified", async () => {
	const pf = await idb.getCopy.playersPlus(p, {
		attrs: ["tid", "awards"],
		ratings: ["season", "ovr"],
		stats: ["season", "tid", "fg"],
		statType: "totals",
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.tid, 4);
	assert.strictEqual(pf.awards.length, 0);
	assert.strictEqual(pf.ratings[0].season, 2011);
	assert.strictEqual(typeof pf.ratings[0].ovr, "number");
	assert.strictEqual(pf.ratings[1].season, 2012);
	assert.strictEqual(typeof pf.ratings[1].ovr, "number");
	assert.strictEqual(pf.ratings[2].season, 2013);
	assert.strictEqual(typeof pf.ratings[2].ovr, "number");
	assert.strictEqual(pf.stats[0].season, 2012);
	assert.strictEqual(pf.stats[0].tid, 4);
	assert.strictEqual(pf.stats[0].fg, 20);
	assert.strictEqual(pf.stats[1].season, 2013);
	assert.strictEqual(pf.stats[1].tid, 0);
	assert.strictEqual(pf.stats[1].fg, 56);
	assert.strictEqual(pf.careerStats.fg, 76);
	assert(!Object.hasOwn(pf, "careerStatsPlayoffs"));
});

test("return stats and ratings from all seasons with a specific team if no season is specified but a team is", async () => {
	const pf = await idb.getCopy.playersPlus(p, {
		attrs: ["tid", "awards"],
		ratings: ["season", "ovr"],
		stats: ["season", "tid", "fg"],
		tid: 4,
		statType: "totals",
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.tid, 4);
	assert.strictEqual(pf.awards.length, 0);
	assert.strictEqual(pf.ratings[0].season, 2012);
	assert.strictEqual(typeof pf.ratings[0].ovr, "number");
	assert.strictEqual(pf.ratings.length, 1);
	assert.strictEqual(pf.stats[0].season, 2012);
	assert.strictEqual(pf.stats[0].tid, 4);
	assert.strictEqual(pf.stats[0].fg, 20);
	assert.strictEqual(pf.stats.length, 1);
	assert.strictEqual(pf.careerStats.fg, 20);
	assert(!Object.hasOwn(pf, "careerStatsPlayoffs"));
});

test("mergeStats combines stats from multiple teams in the same season", async () => {
	const p2 = helpers.deepCopy(p);
	p2.stats[1].playoffs = false;
	p2.stats[1].tid = 20;

	const pf = await idb.getCopy.playersPlus(p2, {
		attrs: ["tid"],
		stats: ["season", "fg", "tid"],
		season: 2012,
		mergeStats: "totOnly",
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.stats.tid, 20);
	assert.strictEqual(pf.stats.fg, (30 + 20) / 8);
});

test("mergeStats combines stats from multiple teams in the same season, for multiple seasons", async () => {
	const p2 = helpers.deepCopy(p);
	p2.stats[1].playoffs = false;
	p2.stats[1].tid = 20;

	const pf = await idb.getCopy.playersPlus(p2, {
		attrs: ["tid"],
		stats: ["season", "fg", "tid"],
		mergeStats: "totOnly",
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.stats.length, 2);
	assert.strictEqual(pf.stats[0].tid, 20);
	assert.strictEqual(pf.stats[0].fg, (30 + 20) / 8);
	assert.strictEqual(pf.stats[1].fg, 56 / 8);
});

test("mergeStats totAndTeams results ", async () => {
	const p2 = helpers.deepCopy(p);
	p2.stats[1].playoffs = false;
	p2.stats[1].tid = 20;

	const pf = await idb.getCopy.playersPlus(p2, {
		attrs: ["tid"],
		stats: ["season", "fg", "tid"],
		ratings: ["season", "tid"],
		mergeStats: "totAndTeams",
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	assert.strictEqual(pf.stats.length, 4);

	assert.strictEqual(pf.stats[0].tid, 4);
	assert.strictEqual(pf.stats[1].tid, 20);
	assert.strictEqual(pf.stats[2].tid, PLAYER.TOT);
	assert.strictEqual(pf.stats[3].tid, 0);

	assert.strictEqual(pf.stats[0].season, 2012);
	assert.strictEqual(pf.stats[1].season, 2012);
	assert.strictEqual(pf.stats[2].season, 2012);
	assert.strictEqual(pf.stats[3].season, 2013);

	assert.strictEqual(pf.stats[0].fg, 20 / 5);
	assert.strictEqual(pf.stats[1].fg, 30 / 3);
	assert.strictEqual(pf.stats[2].fg, 50 / 8);
	assert.strictEqual(pf.stats[3].fg, 56 / 8);

	assert.strictEqual(pf.stats[0].hasTot, true);
	assert.strictEqual(pf.stats[1].hasTot, true);
	assert.strictEqual(pf.stats[2].hasTot, undefined);
	assert.strictEqual(pf.stats[3].hasTot, undefined);

	assert.deepStrictEqual(pf.ratings, [
		{
			season: 2011,
			tid: undefined,
		},
		{
			season: 2012,
			tid: 20,
		},
		{
			season: 2013,
			tid: 0,
		},
		{
			season: 2014,
			tid: undefined,
		},
	]);
});

test("mergeStats totOnly when first row has >0 GP and second has 0 GP", async () => {
	const p2 = helpers.deepCopy(p);
	p2.stats[1].playoffs = false;
	p2.stats[1].tid = 20;
	p2.stats[1].gp = 0;
	p2.stats[1].fg = 0;

	const pf = await idb.getCopy.playersPlus(p2, {
		stats: ["gp", "tid"],
		season: p2.stats[1].season,
		mergeStats: "totOnly",
	});

	if (!pf) {
		throw new Error("Missing player");
	}

	// There was a bug where this returned 0, even though it should be 5 GP from the first season
	assert.strictEqual(pf.stats.gp, 5);
	assert.strictEqual(pf.stats.tid, 4);
});

test("careerStats works when player has no stats rows", async () => {
	const p = {
		pid: 0,
		...player.generate(PLAYER.UNDRAFTED, 19, 2011, false, DEFAULT_LEVEL),
	};
	const pf = await idb.getCopy.playersPlus(p, {
		stats: ["gp", "playoffs", "bpm"],
	});

	// Why is playoffs undefined? Ultimately comes from `row.playoffs = ps.playoffs;` - we don't know what to set the default value (true/false/"combined") if it does not exist. Might be better to just not have playoffs in career stats since it is implied from the property name (like careerStatsPlayoffs)
	assert.deepStrictEqual(pf, {
		stats: [],
		careerStats: { gp: 0, playoffs: undefined, bpm: 0 },
	});
});

test("hideRatingsOnesDigit floors ratings to the tens digit (display only)", async () => {
	const opts = {
		ratings: ["season", "ovr", "pot", "stre"],
		season: 2012,
	};

	g.setWithoutSavingToDB("hideRatingsOnesDigit", false);
	const full = await idb.getCopy.playersPlus(p, opts);

	g.setWithoutSavingToDB("hideRatingsOnesDigit", true);
	const coarse = await idb.getCopy.playersPlus(p, opts);
	g.setWithoutSavingToDB("hideRatingsOnesDigit", false);

	if (!full || !coarse) {
		throw new Error("Missing player");
	}

	// ovr/pot/attributes are floored to the tens digit; season is untouched.
	assert.strictEqual(coarse.ratings.ovr, Math.floor(full.ratings.ovr / 10));
	assert.strictEqual(coarse.ratings.pot, Math.floor(full.ratings.pot / 10));
	assert.strictEqual(coarse.ratings.stre, Math.floor(full.ratings.stre / 10));
	assert.strictEqual(coarse.ratings.season, 2012);
});

// The rounding is for screens. Team overalls, league-wide ranks and any other
// arithmetic have to see the real ratings - a team ovr built from 0-10 inputs is
// meaningless, and ranking on them puts a third of the league in a tie.
test("coarsenRatings: false opts out of the display rounding", async () => {
	const opts = {
		ratings: ["season", "ovr", "pot", "stre"],
		season: 2012,
	};

	g.setWithoutSavingToDB("hideRatingsOnesDigit", false);
	const full = await idb.getCopy.playersPlus(p, opts);

	g.setWithoutSavingToDB("hideRatingsOnesDigit", true);
	const optedOut = await idb.getCopy.playersPlus(p, {
		...opts,
		coarsenRatings: false,
	});
	g.setWithoutSavingToDB("hideRatingsOnesDigit", false);

	if (!full || !optedOut) {
		throw new Error("Missing player");
	}

	assert.deepStrictEqual(optedOut.ratings, full.ratings);
});

// Scouting a draft class is the one place the tens digit really matters, so
// there's an option to let prospects keep their exact ratings. It ends the
// moment they're drafted - the exemption is keyed on the player's CURRENT tid,
// not on whether he was a prospect at the time.
describe("hideRatingsOnesDigitExceptProspects", () => {
	const opts = {
		ratings: ["season", "ovr", "pot", "stre"],
		season: 2012,
	};

	const readBack = async (tid: number, exceptProspects: boolean) => {
		const original = p.tid;
		p.tid = tid;
		g.setWithoutSavingToDB("hideRatingsOnesDigit", true);
		g.setWithoutSavingToDB(
			"hideRatingsOnesDigitExceptProspects",
			exceptProspects,
		);
		const out = await idb.getCopy.playersPlus(p, opts);
		g.setWithoutSavingToDB("hideRatingsOnesDigit", false);
		g.setWithoutSavingToDB("hideRatingsOnesDigitExceptProspects", false);
		p.tid = original;
		if (!out) {
			throw new Error("Missing player");
		}
		return out;
	};

	const trueRatings = async () => {
		g.setWithoutSavingToDB("hideRatingsOnesDigit", false);
		const out = await idb.getCopy.playersPlus(p, opts);
		if (!out) {
			throw new Error("Missing player");
		}
		return out.ratings;
	};

	test("an undrafted prospect keeps his exact ratings", async () => {
		const full = await trueRatings();
		const prospect = await readBack(PLAYER.UNDRAFTED, true);
		assert.deepStrictEqual(prospect.ratings, full);
	});

	test("the same player on a team does not", async () => {
		const full = await trueRatings();
		const drafted = await readBack(4, true);
		assert.strictEqual(drafted.ratings.ovr, Math.floor(full.ovr / 10));
	});

	test("a free agent is not a prospect", async () => {
		const full = await trueRatings();
		const freeAgent = await readBack(PLAYER.FREE_AGENT, true);
		assert.strictEqual(freeAgent.ratings.ovr, Math.floor(full.ovr / 10));
	});

	test("a retired player is not a prospect", async () => {
		const full = await trueRatings();
		const retired = await readBack(PLAYER.RETIRED, true);
		assert.strictEqual(retired.ratings.ovr, Math.floor(full.ovr / 10));
	});

	// With the option off, coarse ratings mean coarse ratings for everyone.
	test("the option off leaves prospects coarse", async () => {
		const full = await trueRatings();
		const prospect = await readBack(PLAYER.UNDRAFTED, false);
		assert.strictEqual(prospect.ratings.ovr, Math.floor(full.ovr / 10));
	});
});
