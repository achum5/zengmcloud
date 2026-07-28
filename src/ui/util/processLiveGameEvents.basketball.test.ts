import { assert, describe, test } from "vitest";
import processLiveGameEvents from "./processLiveGameEvents.basketball.tsx";
import { filterPlayerHighlights } from "./filterPlayerHighlights.ts";

const PLAYER_STATS = [
	"min",
	"fg",
	"fga",
	"tp",
	"tpa",
	"ft",
	"fta",
	"orb",
	"drb",
	"ast",
	"tov",
	"stl",
	"blk",
	"ba",
	"pf",
	"pts",
	"pm",
] as const;

const makePlayer = (pid: number) => {
	const p: any = {
		pid,
		name: `Player ${pid}`,
		injury: { type: "Healthy", gamesRemaining: 0 },
		inGame: false,
	};
	for (const stat of PLAYER_STATS) {
		p[stat] = 0;
	}
	return p;
};

// Ten players per team, pids 0-9 (display team 0, raw t=1) and 100-109 (display
// team 1, raw t=0). processLiveGameEvents swaps the raw team index so the home
// team sits at the bottom of the box score, the same as boxScoreToLiveSim does.
const makeTeam = (base: number) => {
	const t: any = {
		tid: base,
		abbrev: `T${base}`,
		ptsQtrs: [],
		players: Array.from({ length: 10 }, (_, i) => makePlayer(base + i)),
	};
	for (const stat of PLAYER_STATS) {
		t[stat] = 0;
	}
	return t;
};

const makeBoxScore = () => ({
	gid: 42,
	numPeriods: 4,
	quarter: "",
	time: "12:00",
	teams: [makeTeam(0), makeTeam(100)] as any[],
});

// A short stretch of a game for display team 0 (raw t = 1): the starters play a
// possession, the bench comes in and plays two, and one of them scores.
const sample = () => {
	const events: any[] = [
		{ type: "init", boxScore: {} },
		{ type: "period", period: 1, clock: 720 },
	];
	for (let pid = 0; pid < 5; pid++) {
		events.push(
			{ type: "stat", t: 1, pid, s: "gs", amt: 1 },
			{ type: "stat", t: 0, pid: 100 + pid, s: "gs", amt: 1 },
		);
	}
	for (let pid = 0; pid < 5; pid++) {
		events.push({ type: "stat", t: 1, pid, s: "min", amt: 1 });
	}
	events.push({
		type: "sub",
		t: 1,
		pids: [5, 6, 7, 8, 9],
		pidsOff: [0, 1, 2, 3, 4],
		clock: 660,
	});
	for (let pid = 5; pid < 10; pid++) {
		events.push({ type: "stat", t: 1, pid, s: "min", amt: 2 });
	}
	events.push(
		{ type: "fgaAtRim", t: 1, pid: 5, clock: 640 },
		{ type: "fgAtRim", t: 1, pid: 5, pidDefense: 100, clock: 639 },
		{ type: "stat", t: 1, pid: 5, s: "fg", amt: 1 },
		{ type: "stat", t: 1, pid: 5, s: "fga", amt: 1 },
		{ type: "stat", t: 1, pid: 5, s: "pts", amt: 2 },
		{ type: "gameOver", clock: 0 },
	);
	return events;
};

const playAll = (boxScore: any, events: any[]) => {
	const remaining = events.slice();
	let quarters: any = [];
	let overtimes = 0;
	const texts: any[] = [];
	while (remaining.length > 0) {
		const output = processLiveGameEvents({
			events: remaining as any,
			boxScore,
			overtimes,
			quarters,
		});
		overtimes = output.overtimes;
		quarters = output.quarters;
		if (output.text !== undefined) {
			texts.push(output.event?.type);
		}
	}
	return texts;
};

describe("silent substitutions", () => {
	// The bug: a highlight reel dropped every substitution, so `inGame` never
	// moved off the opening lineup. The live box score only redraws a player's row
	// while he's on the court, which left every bench player's line reading zero
	// for the whole reel even though the team totals underneath were right.
	test("a highlight reel still tracks who is on the floor", () => {
		const boxScore = makeBoxScore();
		const filtered = filterPlayerHighlights(sample(), 5);
		playAll(boxScore, filtered);

		const players = boxScore.teams[0].players;
		for (let pid = 0; pid < 5; pid++) {
			assert.strictEqual(players[pid].inGame, false, `starter ${pid} is off`);
		}
		for (let pid = 5; pid < 10; pid++) {
			assert.strictEqual(players[pid].inGame, true, `sub ${pid} is on`);
		}
	});

	test("the bench's stats accumulate, and add up to the team total", () => {
		const boxScore = makeBoxScore();
		const filtered = filterPlayerHighlights(sample(), 5);
		playAll(boxScore, filtered);

		const players = boxScore.teams[0].players;
		for (let pid = 5; pid < 10; pid++) {
			assert.strictEqual(players[pid].min, 2, `sub ${pid} played`);
		}
		assert.strictEqual(players[5].pts, 2);

		const totalMin = players.reduce((sum: number, p: any) => sum + p.min, 0);
		assert.strictEqual(totalMin, boxScore.teams[0].min);
		const totalPts = players.reduce((sum: number, p: any) => sum + p.pts, 0);
		assert.strictEqual(totalPts, boxScore.teams[0].pts);
	});

	// Plus/minus is credited to whoever is on the court, so freezing the lineup
	// handed the whole game's scoring to the starters.
	test("plus/minus follows the lineup, not the starters", () => {
		const boxScore = makeBoxScore();
		const filtered = filterPlayerHighlights(sample(), 5);
		playAll(boxScore, filtered);

		const players = boxScore.teams[0].players;
		assert.strictEqual(players[0].pm, 0, "a starter on the bench gets nothing");
		assert.strictEqual(
			players[5].pm,
			2,
			"the five on the floor get the bucket",
		);
		assert.strictEqual(boxScore.teams[1].players[0].pm, -2);
	});

	// It's a highlight reel: it stops on the highlight and its build-up, and
	// nothing else.
	test("a substitution never stops the reel", () => {
		const boxScore = makeBoxScore();
		const filtered = filterPlayerHighlights(sample(), 5);
		const stopped = playAll(boxScore, filtered);
		assert.ok(!stopped.includes("sub"));
		assert.deepStrictEqual(stopped, [
			"period",
			"fgaAtRim",
			"fgAtRim",
			"gameOver",
		]);
	});

	// An ordinary live sim is unchanged - there, a substitution IS a play-by-play
	// line.
	test("an unfiltered substitution is still announced", () => {
		const boxScore = makeBoxScore();
		const stopped = playAll(boxScore, sample());
		assert.ok(stopped.includes("sub"));
	});
});
