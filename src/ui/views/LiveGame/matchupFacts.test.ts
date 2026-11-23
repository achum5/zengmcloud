import { assert, test } from "vitest";
import type { GamecastInfo } from "../../../worker/views/liveGameInfo.ts";
import { matchupFacts, seasonSeriesText } from "./matchupFacts.ts";

const NOP = { tid: 1, abbrev: "NOP" };
const CLE = { tid: 2, abbrev: "CLE" };

// NOP at home in the first and third meetings.
const info = (
	now: number,
	extra: Partial<GamecastInfo> = {},
): GamecastInfo => ({
	series: [
		{ gid: 10, tids: [1, 2], pts: [101, 95] },
		{ gid: 11, tids: [2, 1], pts: [99, 104] },
		{ gid: 12, tids: [1, 2], pts: [90, 97] },
		{ gid: 13, tids: [2, 1], pts: [110, 100] },
	].map((s, i) =>
		i === now
			? { gid: s.gid, tids: s.tids, now: true }
			: i > now
				? { gid: s.gid, tids: s.tids }
				: s,
	) as GamecastInfo["series"],
	standings: [],
	recent: [],
	...extra,
});

test("the season series going into the game, away first", () => {
	assert.strictEqual(
		seasonSeriesText(info(0), [CLE, NOP]),
		"1st of 4 meetings",
	);
	assert.strictEqual(
		seasonSeriesText(info(1), [NOP, CLE]),
		"NOP leads season series 1-0",
	);
	assert.strictEqual(
		seasonSeriesText(info(2), [CLE, NOP]),
		"NOP leads season series 2-0",
	);
	// (Game 3 went to CLE.)
	assert.strictEqual(
		seasonSeriesText(info(3), [NOP, CLE]),
		"NOP leads season series 2-1",
	);
	// The only meeting says nothing.
	assert.isUndefined(
		seasonSeriesText(
			{
				series: [{ gid: 1, tids: [1, 2], now: true }],
				standings: [],
				recent: [],
			},
			[CLE, NOP],
		),
	);
});

test("a tied season series", () => {
	const tied: GamecastInfo = {
		series: [
			{ gid: 1, tids: [1, 2], pts: [100, 90] },
			{ gid: 2, tids: [2, 1], pts: [100, 90] },
			{ gid: 3, tids: [1, 2], now: true },
		],
		standings: [],
		recent: [],
	};
	assert.strictEqual(
		seasonSeriesText(tied, [CLE, NOP]),
		"Season series tied 1-1",
	);
});

test("the facts: the spread, the total, the series", () => {
	assert.deepEqual(
		matchupFacts({
			gid: 12,
			teams: [NOP, CLE],
			spread: -3.5,
			gamecast: info(2, { total: 228.5 }),
		}),
		["CLE -3.5", "O/U 228.5", "NOP leads season series 2-0"],
	);
	// A pick'em, no total (an old game), the first meeting.
	assert.deepEqual(
		matchupFacts({ gid: 10, teams: [NOP, CLE], spread: 0, gamecast: info(0) }),
		["PK", "1st of 4 meetings"],
	);
	// Nothing known.
	assert.deepEqual(matchupFacts({ gid: 10, teams: [NOP, CLE] }), []);
});

test("a play-in game is just the Play-In: the seeds are by the teams", () => {
	assert.deepEqual(
		matchupFacts({
			gid: 50,
			playoffs: true,
			teams: [NOP, CLE],
			spread: 2.5,
			gamecast: {
				series: [{ gid: 50, tids: [1, 2], now: true }],
				standings: [],
				recent: [],
				playIn: { seeds: { 1: 7, 2: 8 }, stage: 0 },
				total: 210.5,
			},
		}),
		["NOP -2.5", "O/U 210.5", "Play-In"],
	);
});

test("a playoff game: its round, which game, the series going into it", () => {
	assert.deepEqual(
		matchupFacts({
			gid: 12,
			playoffs: true,
			teams: [NOP, CLE],
			gamecast: info(2, {
				bracket: [
					{
						name: "1st round",
						toWin: 4,
						series: [{ teams: [undefined, undefined], now: true }],
					},
				],
			}),
		}),
		["1st round · Game 3 · NOP leads 2-0"],
	);
});
