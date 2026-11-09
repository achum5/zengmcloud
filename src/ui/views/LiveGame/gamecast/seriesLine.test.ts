import { assert, test } from "vitest";
import type { GamecastInfo } from "../../../../worker/views/liveGameInfo.ts";
import { recordText, seriesLine } from "./seriesLine.ts";

const BOS = { tid: 1, abbrev: "BOS" };
const NYK = { tid: 2, abbrev: "NYK" };

// Box score order: home first. BOS at home in games 1, 2 and 4.
const info = (now: number): GamecastInfo => ({
	series: [
		{ gid: 10, tids: [1, 2], pts: [101, 95] },
		{ gid: 11, tids: [1, 2], pts: [99, 104] },
		{ gid: 12, tids: [2, 1], pts: [90, 97] },
		{ gid: 13, tids: [1, 2], pts: [110, 100] },
	].map((s, i) =>
		i === now
			? { ...s, now: true }
			: i > now
				? { gid: s.gid, tids: s.tids }
				: s,
	) as GamecastInfo["series"],
	bracket: [
		{
			name: "1st round",
			toWin: 4,
			series: [{ teams: [undefined, undefined], now: true }],
		},
	],
	standings: [],
	recent: [],
});

test("a playoff game's line: its round, which game, the series going into it", () => {
	assert.strictEqual(
		seriesLine(info(0), true, [NYK, BOS]),
		"1st round · Game 1",
	);
	assert.strictEqual(
		seriesLine(info(2), true, [BOS, NYK]),
		"1st round · Game 3 · Tied 1-1",
	);
	assert.strictEqual(
		seriesLine(info(3), true, [NYK, BOS]),
		"1st round · Game 4 · BOS leads 2-1",
	);
});

test("a finished game's own score is left out - the replay gives nothing away", () => {
	const done = info(3);
	done.series[3] = { ...done.series[3]!, pts: [110, 100] };
	assert.strictEqual(
		seriesLine(done, true, [NYK, BOS]),
		"1st round · Game 4 · BOS leads 2-1",
	);
});

test("no line outside the playoffs", () => {
	assert.isUndefined(seriesLine(info(2), false, [BOS, NYK]));
	assert.isUndefined(seriesLine(undefined, true, [BOS, NYK]));
});

test("a series won reads as won", () => {
	assert.strictEqual(recordText([4, 2], [BOS, NYK], 4), "BOS wins 4-2");
	assert.strictEqual(recordText([2, 3], [BOS, NYK], 4), "NYK leads 3-2");
});
