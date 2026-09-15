import { assert, describe, test } from "vitest";
import { bodyRenderOrder } from "./bodyOrder.ts";

// The bug this pins: React re-inserts the keyed children whose ORDER changed,
// and a re-inserted element loses the CSS transform transition it was about to
// start - so that body teleports to its new spot while the rest glide. At the
// opening tip the jump scene and the possession after it list the same ten men
// in completely different orders, which snapped six or seven of the ten on the
// one beat where everybody crosses the floor.
describe("bodyRenderOrder", () => {
	const jump = [
		// The two jumpers first, then the ring of eight in roster order - exactly
		// the shape the tip scene hands over.
		{ pid: 91, role: "main" },
		{ pid: 14, role: "defender" },
		{ pid: 60, role: "onCourt" },
		{ pid: 7, role: "onCourt" },
	];
	// The same four, listed the way a half-court set lists them (by position).
	const set = [
		{ pid: 7, role: "onCourt" },
		{ pid: 91, role: "onCourt" },
		{ pid: 60, role: "onCourt" },
		{ pid: 14, role: "onCourt" },
	];

	test("two scenes with the same players come out in the same order", () => {
		assert.deepStrictEqual(
			bodyRenderOrder(jump).map((a) => a.pid),
			bodyRenderOrder(set).map((a) => a.pid),
		);
	});

	test("the order does not depend on the scene at all", () => {
		const shuffled = [jump[2]!, jump[0]!, jump[3]!, jump[1]!];
		assert.deepStrictEqual(
			bodyRenderOrder(shuffled).map((a) => a.pid),
			[7, 14, 60, 91],
		);
	});

	test("everyone is still there, exactly once", () => {
		const out = bodyRenderOrder(jump);
		assert.strictEqual(out.length, jump.length);
		for (const actor of jump) {
			assert.strictEqual(out.filter((a) => a === actor).length, 1);
		}
	});

	// The scene's own order means something elsewhere (the play text hugs the
	// actors, the first occurrence of a pid wins), so this must not sort in place.
	test("the scene's own list is left alone", () => {
		const before = jump.map((a) => a.pid);
		bodyRenderOrder(jump);
		assert.deepStrictEqual(
			jump.map((a) => a.pid),
			before,
		);
	});

	// A man dropping out of a scene (or joining one) must not reshuffle anybody
	// else, or he takes the rest of the floor's glide down with him.
	test("adding or dropping a body leaves the others in the same order", () => {
		const without = bodyRenderOrder(set.filter((a) => a.pid !== 60));
		const with60 = bodyRenderOrder(set).filter((a) => a.pid !== 60);
		assert.deepStrictEqual(
			without.map((a) => a.pid),
			with60.map((a) => a.pid),
		);
	});
});
