import { assert, beforeAll, test } from "vitest";
import { crowdAt } from "./crowd.ts";
import { compileField, type FieldTimeline } from "./director.ts";
import { simGames } from "./testGames.ts";

let tls: FieldTimeline[] = [];

beforeAll(async () => {
	const games = await simGames([31, 32, 33]);
	tls = games.map(({ events, roster }, k) =>
		compileField({ events, players: roster, gid: 31 + k }),
	);
}, 180_000);

// The stands fill and empty: full through the first half, half of them
// empty as the third quarter starts and filled again a few minutes in.
test("the crowd comes back from halftime", () => {
	for (const [k, tl] of tls.entries()) {
		const third = tl.plays.filter((p) => p.quarter === 3);
		assert.isAbove(third.length, 5, `${k}`);
		for (const p of tl.plays.filter((p) => p.quarter <= 2)) {
			assert.strictEqual(crowdAt(tl, p.snap, 0).full, 1, `${k} ${p.i}`);
		}
		assert.isAtMost(crowdAt(tl, third[0]!.snap, 0).full, 0.6, `${k}`);
		assert.isAbove(crowdAt(tl, third.at(-1)!.snap, 0).full, 0.95, `${k}`);
		// Filling, never emptying again, through the quarter.
		let last = 0;
		for (const p of third) {
			const full = crowdAt(tl, p.snap, 0).full;
			assert.isAtLeast(full, last, `${k} ${p.i}`);
			last = full;
		}
	}
});

// A close game late in the fourth (or in overtime) has the stadium on its
// feet, and the benches up; the first three quarters never do.
test("a tight finish has the crowd on its feet", () => {
	let tense = 0;
	for (const tl of tls) {
		for (const p of tl.plays) {
			if ((p.tense ?? 0) > 0) {
				assert.isAtLeast(p.quarter, 4, `${p.i}`);
			}
			if (p.tense === 1 && !p.postgame) {
				tense += 1;
				assert.isAtLeast(crowdAt(tl, p.snap, 0).up, 0.9, `${p.i}`);
			}
		}
	}
	assert.isAbove(tense, 0);
});

// At the final whistle a home win brings the stadium up, arms going, and a
// road win sits it down - whatever the finish had it doing.
test("the stadium at the final whistle", () => {
	const seen = new Set<number>();
	for (const [k, tl] of tls.entries()) {
		const post = tl.plays.at(-1)!.postgame!;
		assert.isDefined(post.winner, `${k}`);
		seen.add(post.winner!);
		const at = (ms: number) => crowdAt(tl, post.from + ms, 0);
		if (post.winner === 1) {
			assert.isAbove(at(1000).up, 0.95, `${k}`);
			assert.isAbove(at(10_000).up, 0.5, `${k}`);
		} else {
			assert.isAtMost(at(1000).up, 0.4, `${k}`);
			assert.strictEqual(at(6000).up, 0, `${k}`);
		}
	}
	assert.strictEqual(seen.size, 2);
});

// Third down, the visitors with the ball: the home crowd up and loud; the
// home team's own third downs, it leaves be.
test("the home crowd up for the visitors' third downs", () => {
	let road = 0;
	let home = 0;
	for (const tl of tls) {
		for (const p of tl.plays) {
			if (
				!p.situation.startsWith("3rd") ||
				p.kind === "kneel" ||
				p.kind === "punt" ||
				p.kind === "fieldGoal"
			) {
				continue;
			}
			const c = crowdAt(tl, p.snap, 0);
			if (p.offense === 0) {
				road += 1;
				assert.isAtLeast(c.up, 0.7, `${p.i}`);
			} else if (c.up === 0) {
				home += 1;
			}
		}
	}
	assert.isAtLeast(road, 10);
	assert.isAtLeast(home, 10);
});
