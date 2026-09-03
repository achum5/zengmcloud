import { assert, beforeEach, describe, test } from "vitest";
import { clearCourtRng, seedCourtRng } from "./courtRng.ts";
import {
	APRON,
	benchHuddle,
	COURT_H,
	COURT_W,
	HEAVE_MAX_SECONDS,
	MOTION_HANDLER_SLOT,
	MOTION_OFFENSE_SPOTS,
	motionHandlerSpot,
	OFFENSE_SPOTS,
	possessionBeats,
	rimXFor,
	setupBallPath,
	setupBeatMs,
	synthHeaveSpot,
	synthOutOfBoundsPath,
	TRANSITION_OFFENSE_SPOTS,
} from "./courtSpots.ts";

// Depth from the rim the team is attacking, which is what "how far out was
// that shot from" means regardless of which end of the floor it happened at.
const depthOf = (t: 0 | 1, spot: { x: number; y: number }) =>
	t === 0 ? spot.x : COURT_W - spot.x;

const sample = (n: number, f: (i: number) => number): number[] => {
	const out: number[] = [];
	for (let i = 0; i < n; i++) {
		seedCourtRng(`sample|${i}`);
		out.push(f(i));
	}
	return out;
};

const mean = (xs: number[]) => xs.reduce((a, b) => a + b, 0) / xs.length;

beforeEach(() => {
	clearCourtRng();
});

describe("synthHeaveSpot", () => {
	// THE COMPLAINT: "wayyyy too many half court heaves at end of quarters. They
	// go in at the same rate as normal 3s." The court drew EVERY three launched
	// inside 1.5s from beyond half court - including ordinary end-of-quarter
	// looks, which go in at an ordinary three's rate. Two things fix that: the
	// caller now only asks for a heave when the SIM flagged the shot desperate,
	// and how far out the launch comes from now scales with the clock.
	test("less time on the clock means a deeper launch", () => {
		for (const t of [0, 1] as const) {
			const late = mean(sample(40, () => depthOf(t, synthHeaveSpot(t, 0.1))));
			const mid = mean(sample(40, () => depthOf(t, synthHeaveSpot(t, 0.8))));
			const early = mean(sample(40, () => depthOf(t, synthHeaveSpot(t, 1.5))));
			assert.ok(late > mid, `${late} > ${mid}`);
			assert.ok(mid > early, `${mid} > ${early}`);
		}
	});

	test("a tenth of a second is a genuine half-court heave", () => {
		// Half court is 47ft from the baseline. With no time to do anything but
		// turn and throw it, that's where it comes from.
		for (const t of [0, 1] as const) {
			const depths = sample(40, () => depthOf(t, synthHeaveSpot(t, 0.1)));
			assert.ok(
				Math.min(...depths) > 44,
				`nothing closer than 44ft, got ${Math.min(...depths)}`,
			);
		}
	});

	test("the top of the window is a deep three, not a heave", () => {
		// This is the case that was being drawn from half court and going in. With
		// a second and a half a player can get to the logo and shoot it - deep,
		// but a shot, and it has to LOOK like a shot when it drops.
		for (const t of [0, 1] as const) {
			const depths = sample(40, () =>
				depthOf(t, synthHeaveSpot(t, HEAVE_MAX_SECONDS)),
			);
			assert.ok(
				Math.max(...depths) < 40,
				`nothing past 40ft, got ${Math.max(...depths)}`,
			);
			// Still clearly behind the arc (23.75ft), so it never reads as a normal
			// catch-and-shoot three either.
			assert.ok(
				Math.min(...depths) > 25,
				`nothing inside 25ft, got ${Math.min(...depths)}`,
			);
		}
	});

	test("a shot from the deepest launch stays on the floor", () => {
		// The spot carries a face and a name tag, so it must not end up off the
		// side of the graphic.
		for (const t of [0, 1] as const) {
			for (const secs of [0, 0.4, 1, HEAVE_MAX_SECONDS]) {
				const ys = sample(30, () => synthHeaveSpot(t, secs).y);
				assert.ok(Math.min(...ys) > 2, `y=${Math.min(...ys)} at ${secs}s`);
				assert.ok(
					Math.max(...ys) < COURT_H - 2,
					`y=${Math.max(...ys)} at ${secs}s`,
				);
			}
		}
	});
});

describe("synthOutOfBoundsPath", () => {
	// Out of bounds used to have no court animation at all: the text changed and
	// the floor sat on whatever play came before it. The travel IS the play here
	// - the ball has to be seen crossing a line.
	test("the ball ends up past a sideline, and the near one", () => {
		for (const t of [0, 1] as const) {
			for (let i = 0; i < 40; i++) {
				seedCourtRng(`oob|${t}|${i}`);
				const { from, to } = synthOutOfBoundsPath(t);
				assert.ok(
					from.y > 0 && from.y < COURT_H,
					`starts on the floor, got y=${from.y}`,
				);
				const outTop = to.y < 0;
				const outBottom = to.y > COURT_H;
				assert.ok(outTop || outBottom, `ends out of bounds, got y=${to.y}`);
				assert.strictEqual(
					outTop,
					from.y < COURT_H / 2,
					"takes the shorter way out",
				);
			}
		}
	});

	test("it dies inside the frame, not off the edge of it", () => {
		for (const t of [0, 1] as const) {
			for (let i = 0; i < 40; i++) {
				seedCourtRng(`oob-frame|${t}|${i}`);
				const { to } = synthOutOfBoundsPath(t);
				assert.ok(to.y >= -APRON, `y=${to.y} above the frame`);
				assert.ok(to.y <= COURT_H + APRON, `y=${to.y} below the frame`);
				assert.ok(to.x >= 0 && to.x <= COURT_W, `x=${to.x} off the ends`);
			}
		}
	});

	test("the ball goes out at the end the offense is attacking", () => {
		// The formation behind the play is anchored on this, so a ball dying in the
		// wrong backcourt would leave ten players at the other end of the floor.
		for (let i = 0; i < 40; i++) {
			seedCourtRng(`oob-end|${i}`);
			assert.ok(synthOutOfBoundsPath(0).from.x < COURT_W / 2);
			seedCourtRng(`oob-end|${i}`);
			assert.ok(synthOutOfBoundsPath(1).from.x > COURT_W / 2);
		}
	});
});

describe("benchHuddle", () => {
	test("each team huddles on its own side of the scorer's table", () => {
		const away = benchHuddle(0, 5);
		const home = benchHuddle(1, 5);
		assert.ok(Math.max(...away.map((s) => s.x)) < COURT_W / 2);
		assert.ok(Math.min(...home.map((s) => s.x)) > COURT_W / 2);
	});

	test("nobody stands off the court", () => {
		for (const t of [0, 1] as const) {
			for (const spot of benchHuddle(t, 5)) {
				assert.ok(spot.x > 4 && spot.x < COURT_W - 4, `x=${spot.x}`);
				assert.ok(spot.y > 4 && spot.y < COURT_H - 4, `y=${spot.y}`);
			}
		}
	});

	test("a short-handed lineup still gets a huddle", () => {
		// inGame can be under five (an injury mid-play), and the caller indexes
		// straight into this array.
		for (const n of [0, 1, 2, 5]) {
			assert.strictEqual(benchHuddle(0, n).length, n);
		}
	});
});

// THE POSSESSION THE SIM ACTUALLY PLAYED.
//
// The court used to render every possession as one identical beat, whether the
// engine spent two seconds on it or twenty-two. Measured over ten engine-simmed
// games the clock a possession burns runs 2s to 24s (mean 12, middle half 9-15),
// so that one beat was flattening a four-fold spread. These pin the two rules
// that undo it: how many beats a possession is worth, and the budget they share
// so the extra movement never costs the viewer time.
describe("possessionBeats", () => {
	test("a putback gets no beat at all", () => {
		// It follows its own miss by a fraction of a second - nobody brought the
		// ball anywhere, and a trip up the floor here would be a lie.
		for (const gap of [0, 0.4, 1, 1.9]) {
			assert.strictEqual(possessionBeats(gap, false), 0, `gap=${gap}`);
			assert.strictEqual(possessionBeats(gap, true), 0, `gap=${gap}`);
		}
	});

	test("more clock burned means more basketball shown", () => {
		// Monotonic, which is the whole point: a grind must never read as quicker
		// than a quick hitter.
		let prev = 0;
		for (let gap = 2; gap <= 26; gap += 0.5) {
			const beats = possessionBeats(gap, false);
			assert.ok(beats >= prev, `gap=${gap} went backwards`);
			prev = beats;
		}
		assert.strictEqual(possessionBeats(5, false), 1);
		assert.strictEqual(possessionBeats(11, false), 2);
		assert.strictEqual(possessionBeats(20, false), 3);
	});

	test("a break is one push however long the clock says", () => {
		for (const gap of [3, 9, 17, 24]) {
			assert.strictEqual(possessionBeats(gap, true), 1, `gap=${gap}`);
		}
	});

	test("an unknown gap still develops the possession once", () => {
		// A period boundary makes the clock jump back up, so the caller hands over
		// undefined rather than a negative. That must not silently mean "no beat" -
		// the shot would teleport, which is the bug this replaced.
		assert.strictEqual(possessionBeats(undefined, false), 1);
		assert.strictEqual(possessionBeats(Number.NaN, false), 1);
	});

	test("never more phases than the offense has sets to move through", () => {
		for (let gap = 2; gap <= 30; gap += 0.25) {
			assert.ok(
				possessionBeats(gap, false) <= MOTION_OFFENSE_SPOTS.length,
				`gap=${gap}`,
			);
		}
	});
});

describe("setupBeatMs", () => {
	test("a possession's beats share about one beat of time", () => {
		// The realism is paid for in ball movement, NOT in the viewer's time: three
		// beats of a grind must not take three times as long as one quick hitter.
		const sceneMs = 1100;
		const total = (gap: number) => {
			const beats = possessionBeats(gap, false);
			return setupBeatMs(sceneMs, gap, beats) * beats;
		};
		for (const gap of [5, 11, 20]) {
			assert.ok(total(gap) <= sceneMs * 1.65, `gap=${gap} -> ${total(gap)}`);
		}
		// And a grind still feels longer than a quick hitter, or the clock would
		// be telling the viewer nothing.
		assert.ok(total(20) > total(5));
	});

	test("a beat is never too short for the glide it triggers", () => {
		// Bodies glide to their new spots for the length of the beat; a beat under
		// a couple of hundred ms leaves them still moving when the next one fires.
		for (const sceneMs of [200, 400, 1100, 4000]) {
			for (const gap of [2, 8, 15, 24, undefined]) {
				const beats = Math.max(1, possessionBeats(gap, false));
				assert.ok(
					setupBeatMs(sceneMs, gap, beats) >= 240,
					`sceneMs=${sceneMs} gap=${gap}`,
				);
			}
		}
	});
});

// WHERE THE FIVE MOVE WHILE THE BALL GOES AROUND.
describe("motion phases", () => {
	test("every phase fields the same five men", () => {
		for (const phase of MOTION_OFFENSE_SPOTS) {
			assert.strictEqual(phase.length, OFFENSE_SPOTS.length);
		}
	});

	test("nobody moves anywhere he could not stand", () => {
		// Depth is measured from the rim being attacked; past half court the
		// player would be in his own backcourt with the ball at the other end.
		for (const phase of [...MOTION_OFFENSE_SPOTS, TRANSITION_OFFENSE_SPOTS]) {
			for (const spot of phase) {
				assert.ok(spot.depth >= 4 && spot.depth <= 44, `depth=${spot.depth}`);
				assert.ok(
					spot.across >= 4 && spot.across <= 46,
					`across=${spot.across}`,
				);
			}
		}
	});

	test("a player keeps his position through the motion", () => {
		// Slot order is guard-wing-big, and the motion must not scramble it: the
		// point never ends up posting up under the rim, and the center never ends
		// up parked in a corner. (The big IS allowed further out than the guard -
		// that is a ball screen, which is exactly what phase 2 draws.)
		for (const [i, phase] of MOTION_OFFENSE_SPOTS.entries()) {
			assert.ok(phase[0]!.depth >= 12, `phase ${i}: the point is at the rim`);
			assert.ok(
				phase[4]!.across >= 12 && phase[4]!.across <= 38,
				`phase ${i}: the big is in the corner`,
			);
		}
	});

	test("the ball actually crosses the floor as it is reversed", () => {
		// A reversal that rattles between two neighbouring spots reads as a fumble,
		// not as ball movement.
		for (const t of [0, 1] as const) {
			for (let m = 1; m < MOTION_HANDLER_SLOT.length; m++) {
				const from = motionHandlerSpot(t, m - 1);
				const to = motionHandlerSpot(t, m);
				assert.ok(
					Math.abs(to.y - from.y) > 6,
					`t=${t} phase ${m}: swung only ${Math.abs(to.y - from.y).toFixed(1)}ft across`,
				);
			}
		}
	});

	test("the handler is always in the offense's own half", () => {
		for (const t of [0, 1] as const) {
			for (let m = 0; m < MOTION_HANDLER_SLOT.length + 2; m++) {
				const spot = motionHandlerSpot(t, m);
				if (t === 0) {
					assert.ok(spot.x < COURT_W / 2, `t=0 m=${m} x=${spot.x}`);
				} else {
					assert.ok(spot.x > COURT_W / 2, `t=1 m=${m} x=${spot.x}`);
				}
			}
		}
	});

	test("a phase past the last one holds the last set", () => {
		// The caller counts beats, not phases; an off-by-one must not throw.
		for (const t of [0, 1] as const) {
			assert.deepStrictEqual(
				motionHandlerSpot(t, 99),
				motionHandlerSpot(t, MOTION_OFFENSE_SPOTS.length - 1),
			);
			assert.deepStrictEqual(motionHandlerSpot(t, -3), motionHandlerSpot(t, 0));
		}
	});
});

describe("setupBallPath", () => {
	test("the ball is brought up from the end the possession was won at", () => {
		for (const t of [0, 1] as const) {
			const { ballFrom, ballTo } = setupBallPath(t, false);
			assert.strictEqual(ballFrom.x, rimXFor(t === 0 ? 1 : 0));
			// And it ends up in the offense's own half.
			assert.ok(t === 0 ? ballTo.x < COURT_W / 2 : ballTo.x > COURT_W / 2);
		}
	});

	test("an offensive rebound does not send the ball back down the floor", () => {
		// The possession never changed ends. Bringing it up from the other rim
		// would draw a full-court trip that never happened.
		for (const t of [0, 1] as const) {
			const { ballFrom } = setupBallPath(t, false, true);
			assert.ok(
				t === 0 ? ballFrom.x < COURT_W / 2 : ballFrom.x > COURT_W / 2,
				`t=${t} came from x=${ballFrom.x}`,
			);
		}
	});
});
