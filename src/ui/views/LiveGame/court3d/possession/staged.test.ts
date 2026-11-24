import { assert, describe, test, vi } from "vitest";
import { compileCourt } from "../director.ts";
import { fakeGame, gidOf } from "../testGame.ts";
import * as loop from "./loop.ts";

// What the ten were given and what came of it, each time the sim steps them.
const seen: {
	x: Parameters<typeof loop.defendTogether>[0];
	out: ReturnType<typeof loop.defendTogether>;
}[] = [];
vi.mock("./loop.ts", async (actual) => {
	const real = await actual<typeof loop>();
	return {
		...real,
		defendTogether: (x: Parameters<typeof loop.defendTogether>[0]) => {
			const out = real.defendTogether(x);
			seen.push({ x, out });
			return out;
		},
	};
});

// A game staged by the new possession sim (see README.md).
const staged = (seed: string, possessions: number) => {
	const { events, players } = fakeGame(seed, possessions);
	return compileCourt({
		events,
		players,
		gid: gidOf(seed),
		step: true,
		possession: true,
	});
};

describe("3D possession sim: a game staged by it", () => {
	test("the same game stages the same way every time (every device agrees)", () => {
		const a = staged("same", 40);
		const b = staged("same", 40);
		assert.strictEqual(JSON.stringify(a.beats), JSON.stringify(b.beats));
		assert.strictEqual(JSON.stringify(a.ball), JSON.stringify(b.ball));
		assert.strictEqual(
			JSON.stringify([...a.tracks.values()]),
			JSON.stringify([...b.tracks.values()]),
		);
	}, 600_000);

	test("both ends decide for themselves: the defense guards its men, the four off the ball work their spots, most of the ball in play", () => {
		seen.length = 0;
		staged("both", 30);
		let def = 0;
		let defOwn = 0;
		let off = 0;
		let offOwn = 0;
		for (const { x, out } of seen) {
			for (const tr of x.men) {
				const anim = out.anims.get(tr.pid);
				for (const s of out.samples.get(tr.pid) ?? []) {
					if (!x.live.hit(s.t, s.t + 1) || x.skipAt(s.t)) {
						continue;
					}
					const mine = anim?.(s.t) !== undefined;
					if (x.offenseAt(s.t) !== tr.team) {
						def += 1;
						defOwn += mine ? 1 : 0;
					} else if (x.holderAt(s.t) !== tr.pid) {
						off += 1;
						offOwn += mine ? 1 : 0;
					}
				}
			}
		}
		assert.isAbove(def, 0);
		assert.isAbove(defOwn / def, 0.5, `defense ${defOwn / def}`);
		assert.isAbove(offOwn / off, 0.3, `offense ${offOwn / off}`);
	}, 600_000);
});
