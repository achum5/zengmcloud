import { assert, test } from "vitest";
import {
	analyzeRuns,
	findDroughts,
	findRuns,
	type LogScore,
} from "./recapRuns.ts";

// Four 12-minute quarters.
const ENDS = [720, 1440, 2160, 2880];

// A log from a list of [seconds, side, points, pid?, basket?].
const logOf = (
	rows: [number, 0 | 1, number, number?, boolean?][],
): LogScore[] =>
	rows.map(([t, side, pts, pid, fg]) => ({
		t,
		side,
		pts,
		...(pid !== undefined ? { pid } : {}),
		...(fg !== undefined ? { fg } : {}),
	}));

test("a run is counted both ways, with the man who scored most of it", () => {
	// Home scores 2, then away goes 2,3,2,1(home),3,2,2 = away 14, home 1.
	const log = logOf([
		[100, 0, 2, 1, true],
		[130, 1, 2, 7, true],
		[160, 1, 3, 7, true],
		[190, 1, 2, 8, true],
		[200, 0, 1, 1, false],
		[230, 1, 3, 7, true],
		[260, 1, 2, 9, true],
		[290, 1, 2, 7, true],
		[400, 0, 2, 2, true],
	]);
	const runs = findRuns(log, ENDS);
	assert.strictEqual(runs.length, 1);
	const r = runs[0]!;
	assert.strictEqual(r.side, 1);
	assert.strictEqual(r.pts, 14);
	assert.strictEqual(r.opp, 1);
	assert.deepEqual(r.from.score, [2, 0]);
	assert.deepEqual(r.to.score, [3, 14]);
	assert.strictEqual(r.from.period, 1);
	assert.strictEqual(r.from.clock, 720 - 130);
	assert.deepEqual(r.top, { pid: 7, pts: 10 });
	assert.notOk(r.closed);
});

test("an unanswered run is preferred over the slightly bigger one around it", () => {
	// Away: 2, then home answers 2, then away 15 straight, then home 0.
	const log = logOf([
		[10, 1, 2],
		[40, 0, 2],
		[70, 1, 3],
		[100, 1, 3],
		[130, 1, 3],
		[160, 1, 3],
		[190, 1, 3],
	]);
	const runs = findRuns(log, ENDS);
	assert.strictEqual(runs.length, 1);
	assert.strictEqual(runs[0]!.pts, 15);
	assert.strictEqual(runs[0]!.opp, 0);
	assert.ok(runs[0]!.closed, "nothing was scored after it");
});

test("a closing run knows it ended the game and what came after", () => {
	const log = logOf([
		[2500, 0, 2],
		[2520, 1, 2],
		[2600, 0, 3],
		[2650, 0, 3],
		[2700, 0, 2],
		[2750, 0, 2],
		[2800, 0, 3],
	]);
	const runs = findRuns(log, ENDS);
	const last = runs.at(-1)!;
	assert.strictEqual(last.pts, 13);
	assert.strictEqual(last.opp, 0);
	assert.ok(last.closed);
	assert.deepEqual(last.after, [0, 0]);
	assert.strictEqual(last.to.period, 4);
});

test("points spread over more than eight minutes are not a run", () => {
	const rows: [number, 0 | 1, number][] = [];
	for (let i = 0; i < 6; i++) {
		rows.push([i * 200, 0, 2]);
	}
	assert.strictEqual(findRuns(logOf(rows), ENDS).length, 0);
});

test("droughts: without a point, and without a basket while free throws fell", () => {
	const rows: [number, 0 | 1, number, number?, boolean?][] = [
		[100, 0, 2, 1, true],
		[200, 1, 2, 5, true],
		// Home: only free throws from 100 to 800.
		[400, 0, 2, 1, false],
		[800, 0, 2, 2, true],
	];
	// Then a basket every three minutes to the end, and away scoring late.
	for (let t = 980; t <= 2780; t += 180) {
		rows.push([t, 0, 2, 3, true]);
	}
	rows.push([2850, 1, 3, 5, true]);
	const d = findDroughts(logOf(rows), ENDS, true);
	const homeFg = d.find((x) => x.side === 0 && x.kind === "fg")!;
	assert.strictEqual(homeFg.secs, 700);
	assert.strictEqual(homeFg.endedBy, 2);
	assert.strictEqual(homeFg.from.clock, 620);
	const homePts = d.find((x) => x.side === 0 && x.kind === "pts")!;
	assert.strictEqual(homePts.secs, 400);
	// Away: 2 at 200, then nothing until 2850 - and that is the longest.
	const awayPts = d.find((x) => x.side === 1 && x.kind === "pts")!;
	assert.strictEqual(awayPts.secs, 2650);
	assert.notOk(awayPts.toEnd);
	assert.strictEqual(awayPts.to.period, 4);
});

test("no log, no analysis", () => {
	assert.strictEqual(analyzeRuns([], ENDS), undefined);
	assert.strictEqual(analyzeRuns(logOf([[10, 0, 2]]), []), undefined);
});
