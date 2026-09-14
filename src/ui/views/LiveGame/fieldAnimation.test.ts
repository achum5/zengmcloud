import { assert, describe, test } from "vitest";
import {
	ballAngle,
	ballHeight,
	ballLift,
	fieldGlideSeconds,
	impactReaction,
	nextTumble,
	jobProgress,
	pathProgress,
	pointAlongPath,
	speedFor,
} from "./fieldAnimation.ts";

describe("ballHeight", () => {
	test("a ball is on the ground when it leaves and when it arrives", () => {
		assert.strictEqual(ballHeight("pass", 20, 0), 0);
		assert.strictEqual(ballHeight("pass", 20, 1), 0);
		assert.ok(ballHeight("pass", 20, 0.5) > 0);
	});

	test("a punt hangs far higher than a throw that travels the same distance", () => {
		const dist = 40;
		assert.ok(ballHeight("punt", dist, 0.5) > 2 * ballHeight("pass", dist, 0.5));
	});

	test("a carried ball never leaves the ground - that is what carrying is", () => {
		for (const p of [0.1, 0.3, 0.5, 0.9]) {
			assert.strictEqual(ballHeight("carry", 30, p), 0);
		}
	});

	test("a longer throw is a higher throw, up to a ceiling", () => {
		assert.ok(ballHeight("pass", 40, 0.5) > ballHeight("pass", 8, 0.5));
		// The ceiling holds, so a 70-yard heave doesn't fill the screen.
		assert.ok(ballHeight("pass", 70, 0.5) <= 7);
	});
});

describe("ballLift", () => {
	test("height reads as a bigger ball and a smaller, fainter shadow", () => {
		const low = ballLift(1);
		const high = ballLift(12);
		assert.ok(high.scale > low.scale);
		assert.ok(high.shadowScale < low.shadowScale);
		assert.ok(high.shadowOpacity < low.shadowOpacity);
	});

	test("the shadow never disappears entirely, so the ball keeps a position", () => {
		const absurd = ballLift(500);
		assert.ok(absurd.shadowScale > 0);
		assert.ok(absurd.shadowOpacity > 0);
	});
});

describe("ballAngle", () => {
	test("a spiral points where it is going; a kick tumbles independently", () => {
		assert.strictEqual(ballAngle("pass", 33, 200), 33);
		assert.strictEqual(ballAngle("snap", 33, 200), 33);
		assert.strictEqual(ballAngle("carry", 33, 200), 33);
		assert.strictEqual(ballAngle("punt", 33, 200), 200);
		assert.strictEqual(ballAngle("kick", 33, 200), 200);
		assert.strictEqual(ballAngle("loose", 33, 200), 200);
	});
});

describe("nextTumble", () => {
	test("rotation accumulates with distance travelled", () => {
		const a = nextTumble(0, { x: 0, y: 0 }, { x: 1, y: 0 }, "punt");
		const b = nextTumble(0, { x: 0, y: 0 }, { x: 2, y: 0 }, "punt");
		assert.ok(b > a);
	});

	test("a loose ball tumbles faster than a kicked one - that is the panic", () => {
		const kicked = nextTumble(0, { x: 0, y: 0 }, { x: 3, y: 0 }, "kick");
		const loose = nextTumble(0, { x: 0, y: 0 }, { x: 3, y: 0 }, "loose");
		assert.ok(loose > kicked);
	});
});

describe("impactReaction", () => {
	test("a score always flares OUTWARD and a tackle never does", () => {
		for (let p = 0.02; p < 1; p += 0.02) {
			const score = impactReaction("score", p);
			const tackle = impactReaction("tackle", p);
			assert.ok(score.scale > 1, `score at ${p} did not flare`);
			assert.ok(tackle.scale <= 1.6, `tackle at ${p} flared like a score`);
		}
	});

	test("a score is always the brighter of the two, so they can never be confused", () => {
		for (let p = 0.02; p < 0.9; p += 0.02) {
			assert.ok(
				impactReaction("score", p).opacity >
					impactReaction("tackle", p).opacity,
				`tackle was as bright as a score at ${p}`,
			);
		}
	});

	test("both are over when the beat is over", () => {
		for (const kind of ["score", "tackle"] as const) {
			assert.strictEqual(impactReaction(kind, 1).opacity, 0);
			assert.strictEqual(impactReaction(kind, 1.4).opacity, 0);
		}
	});
});

describe("fieldGlideSeconds", () => {
	test("a longer run takes longer, but never longer than the play is shown", () => {
		const sceneMs = 700;
		const short = fieldGlideSeconds(3, sceneMs);
		const long = fieldGlideSeconds(80, sceneMs);
		assert.ok(long > short);
		assert.ok(long <= (sceneMs / 1000) * 0.84 + 1e-9);
	});

	test("even at top speed a body still moves rather than teleporting", () => {
		assert.ok(fieldGlideSeconds(50, 120) > 0);
	});
});

describe("pointAlongPath", () => {
	const path = [
		{ x: 0, y: 0 },
		{ x: 10, y: 0 },
		{ x: 10, y: 10 },
	];

	test("the ends are the ends", () => {
		assert.deepStrictEqual(pointAlongPath(path, 0), path[0]);
		assert.deepStrictEqual(pointAlongPath(path, -1), path[0]);
		assert.deepStrictEqual(pointAlongPath(path, 1), path[2]);
		assert.deepStrictEqual(pointAlongPath(path, 2), path[2]);
	});

	// A route is run at a steady speed, so halfway through the PLAY is halfway
	// along the route's LENGTH - not halfway through its waypoints. Get that
	// wrong and a receiver crawls through a long stem and snaps through his
	// break, which is the opposite of what a route looks like.
	test("progress is along the length, not the waypoints", () => {
		const half = pointAlongPath(path, 0.5);
		assert.ok(Math.abs(half.x - 10) < 1e-9);
		assert.ok(Math.abs(half.y - 0) < 1e-9);
		const quarter = pointAlongPath(path, 0.25);
		assert.ok(Math.abs(quarter.x - 5) < 1e-9);
	});

	test("a path that goes nowhere is not a division by zero", () => {
		const still = [
			{ x: 4, y: 4 },
			{ x: 4, y: 4 },
		];
		assert.deepStrictEqual(pointAlongPath(still, 0.5), still[0]);
		assert.deepStrictEqual(pointAlongPath([], 0.5), { x: 0, y: 0 });
	});
});

describe("pathProgress", () => {
	test("a man with no delay is moving from the snap", () => {
		assert.strictEqual(pathProgress(0, undefined), 0);
		assert.strictEqual(pathProgress(0.5, 0), 0.5);
		assert.strictEqual(pathProgress(1, 0), 1);
	});

	test("a delayed man stands still and then has the rest of the play", () => {
		assert.strictEqual(pathProgress(0.2, 0.3), 0);
		assert.strictEqual(pathProgress(0.3, 0.3), 0);
		// Half of what is left of the play after his delay.
		assert.ok(Math.abs(pathProgress(0.65, 0.3) - 0.5) < 1e-9);
		assert.strictEqual(pathProgress(1, 0.3), 1);
	});

	test("however late he is, he still finishes", () => {
		assert.strictEqual(pathProgress(1, 5), 1);
	});
});

describe("speed", () => {
	test("a receiver is faster than a lineman and nobody is slower than average", () => {
		assert.ok(speedFor("WR") > speedFor("LB"));
		assert.ok(speedFor("LB") > speedFor("OL"));
		assert.ok(speedFor("CB") > speedFor("DL"));
		for (const pos of ["WR", "CB", "RB", "S", "LB", "TE", "QB", "OL", "DL", "K"]) {
			assert.ok(speedFor(pos) >= 1, `${pos} was slower than walking pace`);
		}
		// An unknown position still runs: a roster with an odd label is not a
		// reason to leave somebody standing on the ball.
		assert.ok(speedFor(undefined) >= 1);
		assert.ok(speedFor("WEIRD") >= 1);
	});

	// The point of speed is separation: given the same job and the same moment
	// in the play, a fast man is further along it.
	test("given the same job, the fast man is further along it", () => {
		const at = (pos: string) => jobProgress(0.5, 0, pos);
		assert.ok(at("WR") > at("TE"));
		assert.ok(at("TE") > at("OL"));
	});

	// And nobody is ever left short of where the play says he finished, which
	// would be a worse lie than everybody running the same speed.
	test("everybody still finishes", () => {
		for (const pos of ["WR", "OL", "K", undefined]) {
			assert.strictEqual(jobProgress(1, 0, pos), 1);
			assert.strictEqual(jobProgress(1, 0.4, pos), 1);
		}
	});

	test("a delayed man still has not moved during his delay", () => {
		assert.strictEqual(jobProgress(0.2, 0.35, "WR"), 0);
	});
});
