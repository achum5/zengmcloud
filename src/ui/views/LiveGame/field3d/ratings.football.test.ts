import { assert, beforeAll, describe, test } from "vitest";
import { compileField, type FieldTimeline } from "./director.ts";
import type { RosterMan } from "./personnel.ts";
import {
	elusiveness,
	flairOf,
	speedsOf,
	type FieldRatings,
} from "./ratings.ts";
import { dist } from "./geometry.ts";
import { simGames, type SimGame } from "./testGames.ts";

let games: SimGame[] = [];

beforeAll(async () => {
	games = await simGames([18, 19]);
}, 120_000);

// Everybody in the game rated so.
const staged = (
	{ events, roster }: SimGame,
	ratings?: (p: RosterMan) => FieldRatings,
): FieldTimeline =>
	compileField({
		events,
		players: ratings
			? roster.map((p) => ({ ...p, ratings: ratings(p) }))
			: roster,
		gid: 1,
	});

const MISSES = ["hurdle", "spin", "juke"] as const;
const missesIn = (tl: FieldTimeline) => {
	const n = { hurdle: 0, spin: 0, juke: 0 };
	for (const play of tl.plays) {
		for (const part of play.parts) {
			for (const l of part.legs) {
				if ((MISSES as readonly string[]).includes(l.move)) {
					n[l.move as (typeof MISSES)[number]] += 1;
				}
			}
		}
	}
	return n;
};

describe("3D field ratings", () => {
	test("faster than the others at his position, quicker flat out - within reason", () => {
		const speeds = speedsOf([
			{ pid: 1, pos: "RB", ratings: { spd: 95 } },
			{ pid: 2, pos: "RB", ratings: { spd: 60 } },
			{ pid: 3, pos: "RB", ratings: { spd: 65 } },
			{ pid: 4, pos: "K", ratings: { spd: 40 } },
			{ pid: 5, pos: "QB" },
		]);
		assert.isAbove(speeds.get(1)!, 1.05);
		assert.isAtMost(speeds.get(1)!, 1.07);
		assert.isBelow(speeds.get(2)!, 1);
		// (Nobody else at his position, or nothing to go by: as he was.)
		assert.isUndefined(speeds.get(4));
		assert.isUndefined(speeds.get(5));
		assert.strictEqual(elusiveness({ elu: 90 }), 1);
		assert.strictEqual(elusiveness({ elu: 5 }), 0);
		assert.isUndefined(elusiveness({}));
	});

	test("the stars stand out against the others at their position, and nobody else", () => {
		const flair = flairOf([
			{ pid: 1, pos: "QB", ratings: { thp: 95 } },
			{ pid: 2, pos: "QB", ratings: { thp: 55 } },
			{ pid: 3, pos: "WR", ratings: { hnd: 70, stre: 30 } },
			{ pid: 4, pos: "WR", ratings: { hnd: 72, stre: 30 } },
			{ pid: 5, pos: "K", ratings: { thp: 90 } },
		]);
		assert.isAbove(flair.get(1)!.arm!, 0.4);
		assert.isBelow(flair.get(2)!.arm!, -0.4);
		// (Next to nothing between them: next to no difference.)
		assert.isBelow(Math.abs(flair.get(3)!.hands!), 0.2);
		assert.strictEqual(flair.get(3)!.power, 0);
		assert.isUndefined(flair.get(5));
	});

	test("a big arm throws it harder, a weak one softer", () => {
		const game = games[0]!;
		const qbs = game.roster.filter((p) => p.pos === "QB");
		const strong = new Set(qbs.filter((p) => p.team === 0).map((p) => p.pid));
		const plain = staged(game);
		const rated = staged(game, (p) =>
			p.pos === "QB" ? { thp: strong.has(p.pid) ? 95 : 45 } : {},
		);
		// Feet a second through the air, each quarterback's passes.
		const pace = (tl: FieldTimeline, side: 0 | 1) => {
			let ft = 0;
			let ms = 0;
			for (const play of tl.plays) {
				if (play.kind !== "pass" || play.offense !== side) {
					continue;
				}
				for (const b of play.ball) {
					if (b.kind === "fly" && b.spiral) {
						ft += dist(b.from, b.to);
						ms += b.t1 - b.t0;
					}
				}
			}
			return ft / Math.max(1, ms);
		};
		assert.isAbove(pace(rated, 0), pace(plain, 0) * 1.01);
		assert.isBelow(pace(rated, 1), pace(plain, 1) * 0.99);
	}, 80_000);

	test("a game with ratings is staged line for line as one without", () => {
		for (const game of games) {
			const plain = staged(game);
			const rated = staged(game, (p) => ({
				spd: 40 + ((p.pid * 37) % 55),
				elu: (p.pid * 53) % 90,
			}));
			assert.strictEqual(rated.beats.length, plain.beats.length);
			assert.deepEqual(
				rated.beats.map((b) => b.i),
				plain.beats.map((b) => b.i),
			);
			for (let k = 1; k < rated.beats.length; k++) {
				assert.isAtLeast(
					rated.beats[k]!.actionStart,
					rated.beats[k - 1]!.actionStart,
				);
			}
		}
	}, 80_000);

	test("the slipperier the man with the ball, the more men he makes miss - and the more he goes over or round them", () => {
		let slick = { hurdle: 0, spin: 0, juke: 0 };
		let stiff = { hurdle: 0, spin: 0, juke: 0 };
		for (const game of games) {
			const a = missesIn(staged(game, () => ({ elu: 90 })));
			const b = missesIn(staged(game, () => ({ elu: 0 })));
			slick = {
				hurdle: slick.hurdle + a.hurdle,
				spin: slick.spin + a.spin,
				juke: slick.juke + a.juke,
			};
			stiff = {
				hurdle: stiff.hurdle + b.hurdle,
				spin: stiff.spin + b.spin,
				juke: stiff.juke + b.juke,
			};
		}
		// (A receiver beating the man right on him as he catches it does
		// whoever he is: the slipperier only make more of the rest.)
		const all = (n: typeof slick) => n.hurdle + n.spin + n.juke;
		assert.isAbove(all(slick), all(stiff), JSON.stringify({ slick, stiff }));
		assert.isAbove(
			slick.hurdle + slick.spin,
			stiff.hurdle + stiff.spin,
			JSON.stringify({ slick, stiff }),
		);
	}, 80_000);
});
