import { assert, test } from "vitest";
import { seatsAt } from "./evaluate.ts";
import { crowdAt } from "./scene.ts";
import { compile } from "./testGame.ts";

// The building empties and fills: the seats full through the first half,
// half of them empty as the third quarter starts and filled again a few
// minutes in.

test("the crowd comes back from halftime", () => {
	for (const seed of ["seats-a", "seats-b"]) {
		const { tl } = compile(seed, 80);
		const periods = tl.beats.filter((b) => b.type === "period");
		assert.isAtLeast(periods.length, 2, seed);
		const third = periods[1]!;
		const fourth = periods[2];
		for (let t = 0; t < periods[0]!.preStart; t += 5000) {
			assert.strictEqual(seatsAt(tl, t), 1, `${seed} ${t}`);
		}
		assert.isAtMost(seatsAt(tl, third.preStart + 1), 0.6, seed);
		const end = fourth?.preStart ?? tl.end;
		assert.isAbove(seatsAt(tl, end - 1), 0.95, seed);
		// Filling, never emptying again, through the quarter.
		let last = 0;
		for (let t = third.preStart + 1; t < end; t += 2000) {
			const s = seatsAt(tl, t);
			assert.isAtLeast(s, last, `${seed} ${t}`);
			last = s;
		}
	}
}, 60_000);

// At the final horn a home win brings the building up, arms going, and a
// road win sits it down - whatever the tension of the finish had it doing.
test("the building at the final horn", () => {
	const seen = new Set<number>();
	for (const seed of ["seats-a", "seats-b", "a", "b", "c", "d"]) {
		const { tl } = compile(seed, 80);
		const over = tl.beats.find((b) => b.type === "gameOver")!;
		const cheer = tl.fx.find(
			(f) => f.kind === "cheer" && f.t >= over.actionStart - 1,
		)!;
		seen.add(cheer.team!);
		const at = (ms: number) => crowdAt(tl, over.actionStart + ms, 0);
		if (cheer.team === 1) {
			assert.isAbove(at(1000).up, 0.95, seed);
			assert.isAbove(at(8000).up, 0.5, seed);
		} else {
			assert.isAtMost(at(1000).up, 0.4, seed);
			assert.strictEqual(at(4000).up, 0, seed);
		}
	}
	// Both: a home win and a road win.
	assert.strictEqual(seen.size, 2);
}, 60_000);

// A visitor at the line: the home fans behind the basket he shoots at are
// up and waving at him from his walk there till the ball comes down - and
// sat again after it; a home shooter they leave be.
test("the fans behind the basket wave at a visitor at the line", () => {
	let road = 0;
	let home = 0;
	for (const seed of ["a", "b"]) {
		const { tl } = compile(seed, 80);
		for (const b of tl.beats) {
			if (b.type !== "ft" && b.type !== "missFt") {
				continue;
			}
			const shot = tl.fx
				.filter(
					(f) =>
						(f.kind === "swish" || f.kind === "clank") &&
						f.t >= b.preStart &&
						f.t <= b.end,
				)
				.at(-1)!;
			const at = `${seed} @${Math.round(b.actionStart)}`;
			const set = crowdAt(tl, (b.preStart + shot.t) / 2 + 400, 0).ends;
			const after = crowdAt(tl, shot.t + 1000, 0).ends;
			assert.isUndefined(after, at);
			if (shot.rim === 0) {
				road += 1;
				assert.strictEqual(set?.[0]?.up, 1, at);
				assert.isUndefined(set?.[1], at);
				// Their arms going.
				const arms = new Set(
					[0, 90, 180].map(
						(now) => crowdAt(tl, shot.t - 300, now).ends?.[0]?.wave,
					),
				);
				assert.strictEqual(arms.size, 2, at);
			} else {
				home += 1;
				assert.isUndefined(set, at);
			}
		}
	}
	assert.isAtLeast(road, 4);
	assert.isAtLeast(home, 4);
}, 60_000);
