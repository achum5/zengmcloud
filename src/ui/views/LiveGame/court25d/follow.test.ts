import { assert, test } from "vitest";
import { advanceLead, FOLLOW_BEHIND, followRate } from "./follow.ts";

// A follower's picture, driven by late, uneven reports of where the other
// court is: it should keep moving, never stop at each report.

test("the other court's estimate never runs past the play it hasn't reached", () => {
	assert.strictEqual(advanceLead(1000, 1500, 1000, 1), 1500);
	assert.strictEqual(advanceLead(1000, 5000, 1000, 1), 2000);
});

test("steady at the gap, easing either side of it", () => {
	assert.strictEqual(followRate(FOLLOW_BEHIND), 1);
	assert.isAbove(followRate(FOLLOW_BEHIND + 1000), 1);
	assert.isBelow(followRate(FOLLOW_BEHIND - 500), 1);
	assert.isAbove(followRate(0), 0);
	assert.isAtMost(followRate(1e9), 2.5);
});

test("reports arriving late and unevenly don't stop the picture", () => {
	// The other court plays at 1x from t=0, through lines every 2500ms of
	// timeline; each report reaches this device 150-650ms late.
	const lines = Array.from({ length: 40 }, (_, i) => (i + 1) * 2500);
	const delays = lines.map((_, i) => 150 + ((i * 7919) % 500));
	let t = 0;
	let lead = 0;
	let reported = 0;
	let stopped = 0;
	const dt = 16;
	for (let now = 0; now < 90_000; now += dt) {
		// Report k (the other court reached line k) arrives late.
		while (
			reported < lines.length &&
			lines[reported]! + delays[reported]! <= now
		) {
			lead = Math.max(lead, lines[reported]!);
			reported += 1;
		}
		const target = lines[reported] ?? lines.at(-1)!;
		lead = advanceLead(Math.max(lead, t), target, dt, 1);
		const before = t;
		t = Math.min(target, t + dt * followRate(lead - t));
		if (now > 5000 && t === before) {
			stopped += 1;
		}
	}
	assert.strictEqual(stopped, 0);
});
