import { assert, beforeAll, test } from "vitest";
import { aimFor, onCrowd } from "./aim.ts";
import { boardAt, crowdAt } from "./crowd.ts";
import { compileField, type FieldTimeline } from "./director.ts";
import { SIDE_ROOM } from "./geometry.ts";
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

// Now and then over a timeout the cheerleaders are not on, the picture on
// the crowd from down low - up and waving at it - till the next play.
test("the picture on the crowd over a timeout", () => {
	let shots = 0;
	for (const tl of tls) {
		for (const p of tl.plays) {
			const shot = p.timeout?.crowd;
			if (!shot) {
				continue;
			}
			shots += 1;
			assert.isUndefined(p.timeout!.cheer, `${p.i}`);
			assert.include(tl.cuts, shot.cut, `${p.i}`);
			assert.isAbove(shot.cut, p.timeout!.cut, `${p.i}`);
			const t = (shot.cut + p.t1) / 2;
			const m = { t, play: p, players: [] };
			assert.isTrue(onCrowd(m), `${p.i}`);
			assert.isFalse(onCrowd({ ...m, t: shot.cut - 1 }), `${p.i}`);
			assert.isBelow(aimFor(m).y, -SIDE_ROOM, `${p.i}`);
			assert.strictEqual(crowdAt(tl, t, 0).up, 1, `${p.i}`);
			const arms = new Set(
				[0, 130, 260, 390].map((now) => crowdAt(tl, t, now).wave),
			);
			assert.strictEqual(arms.size, 2, `${p.i}`);
		}
	}
	assert.isAtLeast(shots, 2);
});

// Between quarters - not at the half, nor the end - the whole stadium,
// panning along it, as the teams change ends; and from it to the next play.
test("the look round the stadium between quarters", () => {
	for (const [k, tl] of tls.entries()) {
		const looks = tl.plays.filter((p) => p.between !== undefined);
		assert.deepEqual(
			looks.map((p) => p.quarter),
			[1, 3],
			`${k}`,
		);
		for (const p of looks) {
			const next = tl.plays[tl.plays.indexOf(p) + 1]!;
			assert.strictEqual(next.quarter, p.quarter + 1, `${p.i}`);
			assert.include(tl.cuts, p.between!, `${p.i}`);
			assert.isAbove(p.between!, p.whistle, `${p.i}`);
			const at = (t: number) => aimFor({ t, play: p, players: [] });
			const a = at(p.between! + 1);
			const b = at(p.t1 - 1);
			assert.isAbove(a.width, 300, `${p.i}`);
			assert.isAbove(b.x, a.x + 40, `${p.i}`);
		}
	}
});

// The boards: DEFENSE, blinking, while the visitors have the ball from
// scrimmage; a shout for the home side's big play; the team's name once the
// game is over.
test("what the boards show", () => {
	let defense = 0;
	let shouts = 0;
	for (const tl of tls) {
		for (const p of tl.plays) {
			if (p.offense === 0 && (p.kind === "pass" || p.kind === "run")) {
				defense += 1;
				const blink = new Set(
					[0, 420, 840].map((ms) => boardAt(tl, p.snap - 1000 + ms)),
				);
				assert.sameMembers([...blink], ["defense", "defense2"], `${p.i}`);
			}
			const c = p.cheer;
			if (c?.what && !p.postgame) {
				const at = boardAt(tl, c.t + 500);
				if (c.side === 1) {
					shouts += 1;
					assert.strictEqual(at, c.what, `${p.i}`);
				} else {
					assert.notStrictEqual(at, c.what, `${p.i}`);
				}
			}
		}
		const post = tl.plays.at(-1)!.postgame!;
		assert.strictEqual(boardAt(tl, post.from + 4000), "name");
	}
	assert.isAbove(defense, 50);
	assert.isAbove(shouts, 3);
});
