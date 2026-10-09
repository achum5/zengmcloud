import { assert, test } from "vitest";
import { evalPlayer } from "./evaluate.ts";
import { crewAt, crewFor } from "./crew.ts";
import { STRIP_MS } from "./director.ts";
import { inWarmup } from "./scene.ts";
import { compile, gidOf } from "./testGame.ts";

// A sub goes to the scorer's table before he comes on - out of his chair
// while play goes on, down on a knee in front of the table, his warm-up top
// pulled off - and comes on from there. Off the floor a man wears his
// warm-up top until he has played; after that, his uniform.

const SEEDS = ["check-a", "check-b", "check-c"];

test("subs check in at the table and come on from there", () => {
	let subs = 0;
	let checked = 0;
	for (const seed of SEEDS) {
		const { events, tl } = compile(seed, 150);
		for (const e of events) {
			if (e.type === "sub" && !e.silent) {
				subs += e.pids?.length ?? 0;
			}
		}
		for (const ci of tl.checkIns ?? []) {
			checked += 1;
			assert.isBelow(ci.t0, ci.kneel);
			assert.isBelow(ci.kneel, ci.strip);
			assert.isAtMost(ci.strip + STRIP_MS, ci.t1);
			// Down there a few seconds at least.
			assert.isAtLeast(ci.strip - ci.kneel, 600);
			const spot = ci.path.at(-1)!;
			// In front of the table, off the floor.
			assert.isBelow(spot.y, -3);
			assert.isAbove(spot.y, -5);
			// Not on the floor while he waits.
			for (let t = ci.t0; t < ci.t1 - 1; t += 250) {
				assert.isFalse(
					evalPlayer(tl, ci.pid, t).shown,
					`${seed} ${ci.pid} at ${t}`,
				);
			}
			// And on from the table.
			const on = evalPlayer(tl, ci.pid, ci.t1 + 1);
			assert.isTrue(on.shown);
			assert.isBelow(Math.hypot(on.x - spot.x, on.y - spot.y), 1.5);
		}
	}
	assert.isAbove(subs, 30);
	assert.isAtLeast(checked / subs, 0.75);
}, 60_000);

test("the warm-up top comes off once a man has played, or at the table", () => {
	const { tl, players } = compile("check-a", 150);
	let fresh = 0;
	for (const p of players) {
		const tr = tl.tracks.get(p.pid)!;
		const firstOn = tr.shown.find(([, on]) => on)?.[0];
		if (firstOn === undefined) {
			assert.isTrue(inWarmup(tl, p.pid, tl.end));
			continue;
		}
		assert.isFalse(inWarmup(tl, p.pid, firstOn + 1));
		assert.isFalse(inWarmup(tl, p.pid, tl.end));
		if (firstOn > 1000) {
			fresh += 1;
			assert.isTrue(inWarmup(tl, p.pid, firstOn - 30_000));
			const ci = tl.checkIns?.find(
				(c) => c.t1 <= firstOn + 200 && c.pid === p.pid,
			);
			if (ci) {
				assert.isTrue(inWarmup(tl, p.pid, ci.strip));
				assert.isFalse(inWarmup(tl, p.pid, ci.strip + STRIP_MS));
			}
		}
	}
	assert.isAbove(fresh, 0);
}, 60_000);

test("the scorer's table is staffed, sitting behind it", () => {
	const { tl } = compile("check-a", 40);
	const crew = crewFor(gidOf("check-a"), undefined, undefined);
	const table = crew.filter((m) => m.role === "table");
	assert.isAtLeast(table.length, 3);
	for (const t of [5000, 60_000, tl.end - 1000]) {
		const states = crewAt(tl, t, crew).states.filter((s) =>
			table.some((m) => m.pid === s.pid),
		);
		assert.strictEqual(states.length, table.length);
		for (const s of states) {
			assert.strictEqual(s.anim, "sit");
			// Behind the table (its back edge is 7.2 feet off the floor).
			assert.isBelow(s.y, -7.2);
			// Facing the floor.
			assert.isAbove(Math.sin(s.yaw), 0.6);
		}
	}
}, 60_000);
