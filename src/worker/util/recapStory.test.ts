import { assert, describe, test } from "vitest";
import type { GameFlow } from "../../common/gameFlow.ts";
import { getAutoRecap } from "./getAutoRecap.ts";
import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "./getDayGamesForRecap.ts";
import { verifyRecap } from "./recapAccuracy.ts";

// THE STORY OF A GAME (recapStory.ts): the things a recap written as a story
// does that one written as a list of sentences did not.

const player = (
	name: string,
	pid: number,
	p: Partial<RecapPlayer> = {},
): RecapPlayer => ({
	name,
	pid,
	min: 30,
	pts: 10,
	reb: 4,
	ast: 2,
	stl: 0,
	blk: 0,
	tov: 1,
	fg: 4,
	fga: 9,
	tp: 0,
	tpa: 2,
	ft: 2,
	fta: 2,
	pf: 2,
	...p,
});

// A side whose points add up to its quarters.
const side = (
	tid: number,
	name: string,
	ptsQtrs: number[],
	top: Partial<RecapPlayer> & { name: string; pid: number },
): RecapTeam => {
	const pts = ptsQtrs.reduce((a, b) => a + b, 0);
	const star = player(top.name, top.pid, top);
	const rest = pts - star.pts;
	const shares = [0.26, 0.22, 0.2, 0.17, 0.15];
	const others = shares.map((s, i) =>
		player(`${name} Man${i + 2}`, tid * 100 + i + 2, {
			pts: Math.round(rest * s),
		}),
	);
	others[0]!.pts += rest - others.reduce((a, p) => a + p.pts, 0);
	return {
		tid,
		region: "",
		name,
		abbrev: name.slice(0, 3).toUpperCase(),
		pts,
		ptsQtrs,
		players: [star, ...others],
	};
};

const game = (
	teams: [RecapTeam, RecapTeam],
	flow: GameFlow | undefined,
	gid = 1,
): RecapGame => ({
	gid,
	day: 1,
	overtimes: 0,
	winnerTid: teams[0].pts > teams[1].pts ? teams[0].tid : teams[1].tid,
	playoffs: false,
	clutchPlays: [],
	teams,
	flow,
});

const bodyOf = (recap: string) => recap.split("\n\n").slice(1);
const sentencesOf = (text: string) => text.split(/(?<=[!.?])\s+(?=[A-Z])/);

describe("a recap is a story", () => {
	// Down nine after three, won the fourth 34-18: the last quarter is the
	// game, and the man who led it is in the sentence that tells it.
	const comeback = (gid: number) => {
		const hawks = side(1, "Hawks", [22, 24, 20, 34], {
			name: "Ace Hawk",
			pid: 11,
			pts: 27,
			fg: 10,
			fga: 19,
		});
		const bulls = side(2, "Bulls", [28, 26, 21, 18], {
			name: "Bo Bull",
			pid: 21,
			pts: 24,
			fg: 9,
			fga: 20,
		});
		return game(
			[hawks, bulls],
			{
				leadChanges: 3,
				ties: 2,
				maxLead: [8, 12],
				maxLeadAt: [4, 3],
				lastLead: { side: 0, pid: 11, period: 4, clock: 380, pts: [86, 85] },
				// Home pid, home pts, away pid, away pts, for each period.
				periodTop: [11, 6, 21, 9, 11, 5, 21, 7, 102, 6, 21, 6, 11, 14, 202, 5],
			},
			gid,
		);
	};

	test("the lede is one sentence with the result in it", () => {
		for (let gid = 1; gid <= 20; gid++) {
			const g = comeback(gid);
			const recap = getAutoRecap(g);
			const lede = sentencesOf(bodyOf(recap)[0]!)[0]!;
			assert.match(lede, /Hawks/, recap);
			assert.match(lede, /Bulls/, recap);
			// The score is in the headline or the lede, and nowhere twice.
			assert.isAtMost(recap.split("100-93").length - 1, 1, recap);
			assert.isAtLeast(recap.split("100-93").length - 1, 1, recap);
			assert.deepEqual(verifyRecap(recap, g), [], recap);
		}
	});

	test("the quarter that won it names the man whose quarter it was", () => {
		let named = 0;
		for (let gid = 1; gid <= 20; gid++) {
			const g = comeback(gid);
			const recap = getAutoRecap(g);
			// The fourth quarter's score, however it is said.
			assert.match(recap, /34-18/, `no fourth quarter in:\n${recap}`);
			if (/scoring 14 of his 27/.test(recap)) {
				named += 1;
			}
			assert.deepEqual(verifyRecap(recap, g), [], recap);
		}
		assert.isAbove(named, 0);
	});

	test("the checker holds a quarter's scorer to the log", () => {
		const g = comeback(1);
		const kinds = (text: string) =>
			verifyRecap(`**x**\n\n${text}`, g).map((v) => v.kind);
		assert.deepEqual(
			kinds(
				"The Hawks won the final quarter 34-18, with Ace Hawk scoring 14 of his 27 points.",
			),
			[],
		);
		// He led no quarter with 16.
		assert.include(
			kinds(
				"The Hawks won the final quarter 34-18, with Ace Hawk scoring 16 of his 27 points.",
			),
			"quarter scorer",
		);
		// And 27 is his night, not 30.
		assert.include(
			kinds(
				"The Hawks won the final quarter 34-18, with Ace Hawk scoring 14 of his 30 points.",
			),
			"quarter scorer",
		);
	});

	test("the checker holds a run's scorer to the log", () => {
		const hawks = side(1, "Hawks", [30, 28, 26, 24], {
			name: "Ace Hawk",
			pid: 11,
			pts: 27,
		});
		const bulls = side(2, "Bulls", [20, 22, 24, 22], {
			name: "Bo Bull",
			pid: 21,
			pts: 24,
		});
		const g = game([hawks, bulls], {
			leadChanges: 0,
			ties: 0,
			maxLead: [24, 0],
			maxLeadAt: [4, 1],
			run: {
				side: 0,
				pts: 15,
				period: 3,
				clock: 500,
				endPeriod: 3,
				topPid: 11,
				topPts: 8,
			},
		});
		const kinds = (text: string) =>
			verifyRecap(`**x**\n\n${text}`, g).map((v) => v.kind);
		assert.deepEqual(
			kinds(
				"The Hawks scored 15 straight points in the third quarter, eight of them from Ace Hawk, to put it away.",
			),
			[],
		);
		assert.include(
			kinds(
				"The Hawks scored 15 straight points in the third quarter, nine of them from Ace Hawk, to put it away.",
			),
			"run scorer",
		);
	});

	// "The Suns led 52-46 at halftime. The lead had grown to 7 going into the
	// fourth. A chipped in 23 points for the Suns, and B contributed 19." -
	// a page of that is what this engine used to write. A story has few
	// sentences that are only one bare fact.
	test("few sentences are a bare fact standing alone", () => {
		let short = 0;
		let total = 0;
		for (let gid = 1; gid <= 30; gid++) {
			const recap = getAutoRecap(comeback(gid));
			for (const para of bodyOf(recap)) {
				for (const s of sentencesOf(para)) {
					total += 1;
					if (s.split(" ").length <= 6) {
						short += 1;
					}
				}
			}
		}
		assert.isBelow(short / total, 0.12, `${short} of ${total}`);
	});

	// Quarters that do not add up to the final are no quarters: nothing is
	// told from them.
	test("inconsistent quarter scores tell no story", () => {
		const hawks = side(1, "Hawks", [22, 24, 20, 34], {
			name: "Ace Hawk",
			pid: 11,
			pts: 27,
		});
		const bulls = side(2, "Bulls", [28, 26, 21, 18], {
			name: "Bo Bull",
			pid: 21,
			pts: 24,
		});
		hawks.ptsQtrs = [20, 20, 20, 20];
		const recap = getAutoRecap(game([hawks, bulls], undefined));
		assert.notMatch(recap, /halftime|quarter/, recap);
		assert.notMatch(recap, /undefined|NaN/, recap);
	});
});
