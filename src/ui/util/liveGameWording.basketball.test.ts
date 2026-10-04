import { afterEach, assert, describe, test } from "vitest";
import processLiveGameEvents from "./processLiveGameEvents.basketball.tsx";
import { finishOf, type Finish } from "./liveGameWording.basketball.ts";
import { local } from "./local.ts";

const makeBoxScore = (gid: number) => {
	const team = (base: number) => ({
		ptsQtrs: [0],
		players: Array.from({ length: 5 }, (_, i) => ({
			pid: base + i,
			name: `Player ${base + i}`,
			pts: 0,
			blk: 0,
			pf: 0,
		})),
	});
	return {
		gid,
		numPeriods: 4,
		quarter: "1st quarter",
		time: "12:00",
		teams: [team(0), team(100)],
	};
};

// What the words say, for each finish.
const SAYS: Record<Finish, RegExp> = {
	dunk: /throws it down|slams it home|blows the dunk|blocked the dunk/,
	layup:
		/layup is good|lays it in|missed the layup|blows the layup|blocked the layup/,
	tip: /tips it in/,
	rollOut: /rolls out/,
	rimOut: /rims out/,
	brick: /bricks it/,
	swish: /Swish/,
	rattle: /rattles around/,
	airball: /airball/,
	plain: /It's good|No good/,
};

const TYPES = [
	"fgTipIn",
	"fgTipInAndOne",
	"fgPutBack",
	"fgPutBackAndOne",
	"fgAtRim",
	"fgAtRimAndOne",
	"blkAtRim",
	"blkTipIn",
	"blkPutBack",
	"missTipIn",
	"missAtRim",
	"missPutBack",
	"missLowPost",
	"missMidRange",
	"missTp",
	"shootoutShot",
];

const sampleEvent = (type: string, n: number) => {
	const blocked = type.startsWith("blk");
	return {
		type,
		t: blocked ? 1 : 0,
		pid: blocked ? 100 + (n % 5) : n % 5,
		pidDefense: 100,
		clock: 720 - n * 0.7,
		period: 1 + (n % 4),
		made: n % 2 === 0,
	};
};

const textOf = (gid: number, event: any): string => {
	const output = processLiveGameEvents({
		events: [event],
		boxScore: makeBoxScore(gid),
		overtimes: 0,
		quarters: [],
	});
	assert.strictEqual(typeof output.text, "string");
	return output.text as string;
};

afterEach(() => {
	local.getState().actions.update({ gender: "male" });
});

describe("live game wording", () => {
	for (const gender of ["male", "female"] as const) {
		test(`the finish matches the words (${gender})`, () => {
			local.getState().actions.update({ gender });
			for (const type of TYPES) {
				for (let n = 0; n < 150; n++) {
					const event = sampleEvent(type, n);
					const text = textOf(7, event);
					const finish = finishOf(event, 7, gender);
					assert.isDefined(finish, type);
					assert.match(text, SAYS[finish!], `${type} #${n}: ${finish}`);
				}
			}
		});
	}

	test("the same play always reads the same way", () => {
		const event = sampleEvent("fgAtRim", 3);
		const first = textOf(11, event);
		for (let i = 0; i < 5; i++) {
			assert.strictEqual(textOf(11, { ...event }), first);
		}
	});

	test("the odds are the same as before", () => {
		const share = (
			type: string,
			gender: "female" | "male",
			finish: Finish,
		): number => {
			let hits = 0;
			const N = 4000;
			for (let n = 0; n < N; n++) {
				if (finishOf(sampleEvent(type, n), n % 97, gender) === finish) {
					hits += 1;
				}
			}
			return hits / N;
		};
		// Two of five at-rim makes are slams for men, almost none for women.
		assert.closeTo(share("fgAtRim", "male", "layup"), 2 / 5, 0.04);
		assert.isAbove(share("fgAtRim", "female", "layup"), 0.97);
		assert.closeTo(share("fgPutBack", "male", "dunk"), 1 / 2, 0.04);
		assert.strictEqual(share("fgPutBack", "female", "dunk"), 0);
		assert.strictEqual(share("blkAtRim", "female", "dunk"), 0);
		assert.closeTo(share("missMidRange", "male", "plain"), 4 / 6, 0.04);
		assert.closeTo(share("missAtRim", "male", "rollOut"), 1 / 5, 0.04);
	});
});
