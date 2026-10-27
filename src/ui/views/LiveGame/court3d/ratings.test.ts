import { assert, describe, test } from "vitest";
import { compileCourt } from "./director.ts";
import type { CourtRatings } from "./ratings.ts";
import { fakeGame, gidOf } from "./testGame.ts";

// Players play like their ratings: the same game, everybody rated the same
// way, and what they do at the rim shows it.
const play = (seed: string, ratings: CourtRatings) => {
	const { events, players } = fakeGame(seed, 150);
	return compileCourt({
		events,
		players: players.map((p) => ({ ...p, ratings })),
		gid: gidOf(seed),
	});
};
const count = (seeds: string[], ratings: CourtRatings) => {
	const n: Record<string, number> = {};
	for (const seed of seeds) {
		for (const tr of play(seed, ratings).tracks.values()) {
			for (const a of tr.acts) {
				n[a.anim] = (n[a.anim] ?? 0) + 1;
			}
		}
	}
	return n;
};
const all = (v: number): CourtRatings => ({
	hgt: 50,
	stre: v,
	spd: v,
	jmp: v,
	ins: v,
	dnk: v,
	drb: v,
	tp: v,
	fg: v,
	diq: v,
	oiq: v,
	pss: v,
	reb: v,
});

describe("3D: players play like their ratings", () => {
	test("a high flier puts on a show at the rim; a plodder just puts it in", () => {
		const seeds = ["r1", "r2", "r3"];
		const hi = count(seeds, all(85));
		const lo = count(seeds, all(30));
		const show = (n: Record<string, number>) => {
			const dunks =
				(n.dunk ?? 0) + (n.dunk1 ?? 0) + (n.tomahawk ?? 0) + (n.windmill ?? 0);
			return { dunks, share: ((n.tomahawk ?? 0) + (n.windmill ?? 0)) / dunks };
		};
		const h = show(hi);
		const l = show(lo);
		assert.isAbove(h.dunks, 10);
		assert.isAbove(l.dunks, 10);
		assert.isAbove(h.share, l.share + 0.2, JSON.stringify({ h, l }));
		assert.strictEqual(lo.windmill ?? 0, 0);
	});

	test("a crafty finisher rolls it in; a strong one goes up strong", () => {
		const seeds = ["r1", "r2", "r3"];
		const crafty = count(seeds, { ...all(50), ins: 85, drb: 85, stre: 30 });
		const strong = count(seeds, { ...all(50), ins: 30, drb: 30, stre: 85 });
		const roll = (n: Record<string, number>) =>
			(n.fingerRoll ?? 0) /
			Math.max(1, (n.fingerRoll ?? 0) + (n.powerLayup ?? 0));
		assert.isAbove(roll(crafty), roll(strong) + 0.3);
	});
});
