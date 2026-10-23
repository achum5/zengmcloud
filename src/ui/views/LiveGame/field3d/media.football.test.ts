import { assert, test } from "vitest";
import type { Play } from "./director.ts";
import type { FieldMoment } from "./evaluate.ts";
import { FIELD_L, FIELD_W, LIMIT_LINE, MID_Y, TEAM_AREA } from "./geometry.ts";
import { photographersAt, photographersFor } from "./media.ts";

test("the photographers off the field, outside the team areas, clear of the posts and of each other", () => {
	for (let gid = 0; gid < 30; gid++) {
		const men = photographersFor(gid);
		assert.isAtLeast(men.length, 20);
		assert.deepEqual(photographersFor(gid), men);
		for (const [k, s] of men.entries()) {
			const { x, y } = s.at;
			const behind = x < 0 || x > FIELD_L;
			const beside = y < -LIMIT_LINE || y > FIELD_W + LIMIT_LINE;
			assert.isTrue(behind || beside, `${s.id} at ${x}, ${y}`);
			if (beside) {
				assert.isTrue(x < TEAM_AREA[0] || x > TEAM_AREA[1], `${s.id} at ${x}`);
			} else {
				assert.isAtLeast(Math.abs(y - MID_Y), 8, `${s.id} at ${y}`);
			}
			for (const o of men.slice(k + 1)) {
				assert.isAtLeast(Math.hypot(o.at.x - x, o.at.y - y), 4, `${s.id}`);
			}
		}
	}
});

test("the cameras up while the ball is live near enough, and down between plays", () => {
	const men = photographersFor(3);
	const play = { snap: 10_000, whistle: 15_000 } as Play;
	const at = (t: number, x: number): FieldMoment => ({
		t,
		play,
		players: [],
		focus: { x, y: MID_Y, z: 0 },
	});
	const up = (m: FieldMoment) =>
		photographersAt(men, m).filter((s) => s.arms?.move === "shoot").length;
	assert.strictEqual(up(at(5000, 20)), 0);
	assert.strictEqual(up(at(30_000, 20)), 0);
	// (Down at the goal line, the ones in that end of the stadium.)
	const live = photographersAt(men, at(12_000, 20));
	for (const s of live) {
		const far = Math.hypot(s.x - 20, s.y - MID_Y);
		if (s.arms) {
			assert.isBelow(far, 240);
		}
		if (far < 100) {
			assert.isDefined(s.arms, `${s.id}`);
		}
	}
	assert.isAbove(up(at(12_000, 20)), 8);
});
