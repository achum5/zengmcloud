import { assert, beforeAll, describe, test } from "vitest";
import { compileField, type FieldTimeline } from "./director.ts";
import type { RosterMan } from "./personnel.ts";
import {
	elusiveness,
	flairOf,
	speedsOf,
	type FieldRatings,
} from "./ratings.ts";
import { dist, type Pt } from "./geometry.ts";
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

// Where a man is at t in a play.
const posAt = (
	part: FieldTimeline["plays"][number]["parts"][number],
	t: number,
): Pt => {
	const l = part.legs.findLast((x) => x.t0 <= t) ?? part.legs[0]!;
	const u =
		l.t1 > l.t0 ? Math.min(1, Math.max(0, (t - l.t0) / (l.t1 - l.t0))) : 1;
	return {
		x: l.from.x + (l.to.x - l.from.x) * u,
		y: l.from.y + (l.to.y - l.from.y) * u,
	};
};

// On the passes `side` throws: how far its linemen have given ground two
// seconds in, and how far the other side's corners are from the nearest
// man going out, a second and a half in (feet, on average).
const PROTECT_MS = 2500;
const COVER_MS = 1500;
const passPlays = (tl: FieldTimeline, roster: RosterMan[], side: 0 | 1) => {
	const pos = new Map(roster.map((p) => [p.pid, p.pos]));
	let given = 0;
	let linemen = 0;
	let worked = 0;
	let rushers = 0;
	let apart = 0;
	let corners = 0;
	for (const play of tl.plays) {
		if (play.kind !== "pass" || play.offense !== side) {
			continue;
		}
		const out = play.parts.filter(
			(x) =>
				x.team === side && ["WR", "TE", "RB"].includes(pos.get(x.pid) ?? ""),
		);
		for (const part of play.parts) {
			const at = pos.get(part.pid);
			if (part.team === side && at === "OL") {
				given += dist(
					posAt(part, play.snap),
					posAt(part, play.snap + PROTECT_MS),
				);
				linemen += 1;
			} else if (part.team !== side && at === "DL") {
				// (Across the field, over and back, working the man on him.)
				let lo = Infinity;
				let hi = -Infinity;
				for (let t = play.snap + 700; t <= play.snap + PROTECT_MS; t += 50) {
					const y = posAt(part, t).y;
					lo = Math.min(lo, y);
					hi = Math.max(hi, y);
				}
				worked += hi - lo;
				rushers += 1;
			} else if (part.team !== side && at === "CB" && out.length > 0) {
				const t = play.snap + COVER_MS;
				const me = posAt(part, t);
				apart += Math.min(...out.map((x) => dist(me, posAt(x, t))));
				corners += 1;
			}
		}
	}
	return {
		given: given / linemen,
		worked: worked / rushers,
		apart: apart / corners,
	};
};

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
		const line = flairOf([
			{ pid: 1, pos: "DL", ratings: { prs: 90 } },
			{ pid: 2, pos: "DL", ratings: { prs: 50 } },
			{ pid: 3, pos: "CB", ratings: { pcv: 40 } },
			{ pid: 4, pos: "CB", ratings: { pcv: 80 } },
		]);
		assert.isAbove(line.get(1)!.rush!, 0.3);
		assert.isBelow(line.get(3)!.cover!, -0.3);
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

	test("a pass rusher who has the better of the man on him works him harder, and backs him up further - and a cover man who stands out sits tighter", () => {
		const game = games[0]!;
		const plain = staged(game);
		// Side 0's rushers and corners all stars, side 1's not.
		const rated = staged(game, (p) =>
			p.pos === "DL"
				? { prs: p.team === 0 ? 95 : 45 }
				: p.pos === "CB"
					? { pcv: p.team === 0 ? 95 : 45 }
					: p.pos === "OL"
						? { pbk: 60 }
						: {},
		);
		// (Side 1 throwing into side 0's: the rush on, the corners tight.)
		const a = passPlays(plain, game.roster, 1);
		const b = passPlays(rated, game.roster, 1);
		assert.isAbove(b.worked, a.worked * 1.05, JSON.stringify({ a, b }));
		assert.isAbove(b.given, a.given, JSON.stringify({ a, b }));
		assert.isBelow(b.apart, a.apart * 0.97, JSON.stringify({ a, b }));
		// And the other way round.
		const c = passPlays(plain, game.roster, 0);
		const d = passPlays(rated, game.roster, 0);
		assert.isBelow(d.worked, c.worked * 0.97, JSON.stringify({ c, d }));
		assert.isBelow(d.given, c.given, JSON.stringify({ c, d }));
		assert.isAbove(d.apart, c.apart * 1.01, JSON.stringify({ c, d }));
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
