import { assert, test } from "vitest";
import { normalCdf } from "./seasonSim.ts";
import { rankedLanding } from "./outlook.ts";

// THE FINISH, RANKED: a team's place in a draft is how many teams finish
// worse, each drawn around where it is expected to finish.

const n = 30;
const others = (field: readonly number[], i: number) =>
	field.filter((_, j) => j !== i);

test("each place is some team's, once", () => {
	// A league spread from worst to best, three seasons out.
	const field = Array.from({ length: n }, (_, i) => 6 + (i * 17) / (n - 1));
	const byTeam = field.map((c, i) => rankedLanding(n, c, 7, others(field, i)));
	for (const w of byTeam) {
		assert.closeTo(
			w.reduce((a, x) => a + x, 0),
			1,
			1e-9,
		);
	}
	for (let k = 0; k < n; k++) {
		const taken = byTeam.reduce((a, w) => a + w[k]!, 0);
		assert.closeTo(taken, 1, 0.01, `place ${k}`);
	}
});

test("a team like every other is as likely to finish anywhere", () => {
	const field = Array.from({ length: n }, () => 14.5);
	const w = rankedLanding(n, 14.5, 7, others(field, 0));
	for (const x of w) {
		assert.closeTo(x, 1 / n, 0.002);
	}
});

test("a good team can fall apart: more at the bottom than its own bell curve", () => {
	// Expected 25th of 30 two drafts out, the rest of the league spread out.
	const field = Array.from({ length: n }, (_, i) => 4 + (i * 21) / (n - 1));
	const i = n - 2;
	const ranked = rankedLanding(n, field[i]!, 6.5, others(field, i));
	const bottomFive = ranked.slice(0, 5).reduce((a, x) => a + x, 0);
	const bell = normalCdf((4.5 - field[i]!) / 6.5);
	assert.isAbove(bottomFive, bell);
	// And it still most likely finishes near the top.
	const topTen = ranked.slice(n - 10).reduce((a, x) => a + x, 0);
	assert.isAbove(topTen, 0.5);
});

test("a team expected worse finishes worse", () => {
	const field = Array.from({ length: n }, (_, i) => i);
	const mean = (w: number[]) => w.reduce((a, x, k) => a + x * k, 0);
	const worse = rankedLanding(n, 5, 5, others(field, 5));
	const better = rankedLanding(n, 20, 5, others(field, 20));
	assert.isBelow(mean(worse), mean(better));
});
