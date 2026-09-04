import { assert, describe, test } from "vitest";
import { capsForSeasons, type SeasonCaps } from "./capsForSeasons.ts";
import { PHASE } from "../../common/constants.ts";

// The real-players data, verbatim: every cap change lands at the draft lottery
// (phase 4), which is the END of its season, so the event dated 2014 is what
// the 2015 season gets played under.
const REAL_EVENTS = [
	{
		season: 2013,
		phase: PHASE.DRAFT_LOTTERY,
		info: { luxuryPayroll: 71750, salaryCap: 58700, minPayroll: 39150 },
	},
	{ season: 2014, phase: PHASE.PRESEASON, info: { pace: 93.9 } },
	{
		season: 2014,
		phase: PHASE.DRAFT_LOTTERY,
		info: { luxuryPayroll: 76850, salaryCap: 63050, minPayroll: 42050 },
	},
	{
		season: 2015,
		phase: PHASE.DRAFT_LOTTERY,
		info: { luxuryPayroll: 84750, salaryCap: 70000, minPayroll: 46650 },
	},
	{
		season: 2016,
		phase: PHASE.DRAFT_LOTTERY,
		info: { luxuryPayroll: 113300, salaryCap: 94150, minPayroll: 62750 },
	},
	{
		season: 2017,
		phase: PHASE.DRAFT_LOTTERY,
		info: { luxuryPayroll: 119250, salaryCap: 99100, minPayroll: 66050 },
	},
	{
		season: 2018,
		phase: PHASE.DRAFT_LOTTERY,
		info: { luxuryPayroll: 123750, salaryCap: 101850, minPayroll: 67900 },
	},
];

// A league in 2014 has already taken the 2013 change and nothing since.
const CURRENT_2014: SeasonCaps = {
	salaryCap: 58700,
	luxuryPayroll: 71750,
	minPayroll: 39150,
	hardCapAmount: 0,
	hardCapTids: [],
	hardCapUseLuxuryTax: true,
};

const seasons = [2014, 2015, 2016, 2017, 2018];

describe("capsForSeasons", () => {
	// The reported bug: the hard cap tracked the luxury tax line, the league was
	// playing 2014 under 71.75, and the finances page showed 76.85 for 2014
	// through 2018 - the one pending change, applied to every column, because
	// the cached events stop at the current season.
	test("every season gets the cap it will actually be played under", () => {
		const caps = capsForSeasons({
			seasons,
			current: CURRENT_2014,
			events: REAL_EVENTS,
			season: 2014,
			phase: PHASE.REGULAR_SEASON,
		});

		assert.deepStrictEqual(
			caps.map((row) => row.luxuryPayroll),
			[71750, 76850, 84750, 113300, 119250],
		);
		assert.deepStrictEqual(
			caps.map((row) => row.salaryCap),
			[58700, 63050, 70000, 94150, 99100],
		);
	});

	// The season being played is governed by what is in force today; the change
	// dated to it does not land until its draft lottery, after the games.
	test("the current season keeps today's cap", () => {
		for (const phase of [
			PHASE.PRESEASON,
			PHASE.REGULAR_SEASON,
			PHASE.AFTER_TRADE_DEADLINE,
			PHASE.PLAYOFFS,
		]) {
			const caps = capsForSeasons({
				seasons: [2014],
				current: CURRENT_2014,
				events: REAL_EVENTS,
				season: 2014,
				phase,
			});
			assert.strictEqual(caps[0]!.luxuryPayroll, 71750, `phase ${phase}`);
		}
	});

	// Past the draft lottery the 2014 change has fired, so it is in `current`
	// and the projection starts from there.
	test("the offseason projects from the change that just landed", () => {
		const caps = capsForSeasons({
			seasons: [2015, 2016, 2017],
			current: { ...CURRENT_2014, luxuryPayroll: 76850, salaryCap: 63050 },
			events: REAL_EVENTS,
			season: 2014,
			phase: PHASE.FREE_AGENCY,
		});
		assert.deepStrictEqual(
			caps.map((row) => row.luxuryPayroll),
			[76850, 84750, 113300],
		);
	});

	// A league created in 2014 can still have 2013's event sitting in the
	// database - nothing ever loaded it, so nothing ever deleted it. Applying it
	// would walk the cap BACKWARDS.
	test("an event that already fired is ignored", () => {
		const caps = capsForSeasons({
			seasons: [2014, 2015, 2016],
			current: { ...CURRENT_2014, luxuryPayroll: 76850 },
			events: [
				{
					season: 2010,
					phase: PHASE.DRAFT_LOTTERY,
					info: { luxuryPayroll: 65000, salaryCap: 53000 },
				},
				...REAL_EVENTS,
			],
			season: 2014,
			phase: PHASE.DRAFT_LOTTERY,
		});
		// 2015 is played under the change that just landed, and 2016 under the
		// one dated 2015 - neither of them under 2010's.
		assert.deepStrictEqual(
			caps.map((row) => row.luxuryPayroll),
			[76850, 76850, 84750],
		);
	});

	test("no events leaves every season on today's cap", () => {
		const caps = capsForSeasons({
			seasons,
			current: CURRENT_2014,
			events: [],
			season: 2014,
			phase: PHASE.REGULAR_SEASON,
		});
		assert.deepStrictEqual(
			caps.map((row) => row.luxuryPayroll),
			[71750, 71750, 71750, 71750, 71750],
		);
	});

	// Events out of order, and one that carries only a hard cap.
	test("unsorted events, and the hard cap settings, come through", () => {
		const caps = capsForSeasons({
			seasons: [2015, 2016],
			current: CURRENT_2014,
			events: [
				{
					season: 2015,
					phase: PHASE.DRAFT_LOTTERY,
					info: { hardCapAmount: 90000, hardCapTids: [3] },
				},
				{
					season: 2014,
					phase: PHASE.DRAFT_LOTTERY,
					info: { hardCapAmount: 80000, hardCapUseLuxuryTax: false },
				},
			],
			season: 2014,
			phase: PHASE.REGULAR_SEASON,
		});
		assert.deepStrictEqual(caps[0]!.hardCapAmount, 80000);
		assert.strictEqual(caps[0]!.hardCapUseLuxuryTax, false);
		assert.deepStrictEqual(caps[0]!.hardCapTids, []);
		assert.deepStrictEqual(caps[1]!.hardCapAmount, 90000);
		assert.deepStrictEqual(caps[1]!.hardCapTids, [3]);
	});
});
