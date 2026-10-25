import { assert, describe, test } from "vitest";
import { bodyPoint, evalPlayer, fastAt, poseOf, withBody } from "./evaluate.ts";
import { type AnimName, bodyOf, legPhases, skeleton } from "./poses.ts";
import type { CourtTimeline } from "./director.ts";
import { compile } from "./testGame.ts";

// How quick he goes at t (feet a second), over the frame before.
const speedOf = (tl: CourtTimeline, pid: number, t: number) => {
	const a = evalPlayer(tl, pid, t - 50);
	const b = evalPlayer(tl, pid, t);
	return Math.hypot(b.x - a.x, b.y - a.y) / 0.05;
};

describe("footwork", () => {
	// Shuffling - straight across, or on a slant, partly ahead or back as
	// well - the foot he is not stepping with stays where it is on the floor:
	// his feet step out the way he goes, not out to his side and then slid
	// back under him.
	test("a shuffling man's planted foot stays put, on a slant too", () => {
		const body = bodyOf();
		const STEP = 32;
		let travel = 0;
		let slide = 0;
		const { tl } = compile("a", 120);
		for (const tr of tl.tracks.values()) {
			let prev:
				| { x: number; y: number; feet: { x: number; y: number }[] }
				| undefined;
			for (let t = 200_000; t < 500_000; t += STEP) {
				const st = evalPlayer(tl, tr.pid, t);
				if (
					!st.shown ||
					st.anim !== "shuffle" ||
					(st.from !== undefined && st.from.w > 0.05)
				) {
					prev = undefined;
					continue;
				}
				const me = withBody(st, body);
				const sk = skeleton(body, poseOf(me));
				const feet = [bodyPoint(me, sk.legR.end), bodyPoint(me, sk.legL.end)];
				if (prev) {
					const d = Math.hypot(st.x - prev.x, st.y - prev.y);
					// (At a shuffle's pace: just setting off or pulling up, his
					// feet come under him as he settles.)
					if ((d * 1000) / STEP >= 6) {
						travel += d;
						slide += Math.min(
							...feet.map((f, i) =>
								Math.hypot(f.x - prev!.feet[i]!.x, f.y - prev!.feet[i]!.y),
							),
						);
					}
				}
				prev = { x: st.x, y: st.y, feet };
			}
		}
		assert.isAbove(travel, 100);
		assert.isBelow(slide / travel, 0.42);
	}, 120_000);

	// Drifting along the arc with his eyes on the ball, a shooter side-steps
	// - a foot out the way he goes and the other closed up after it - not a
	// run with his legs turned only so far under him and his feet sliding
	// the rest of the way.
	// Setting off from where he stood, both his feet were swept round under
	// him over the floor - from his stance, out wide, to where his strides
	// have them - rather than stepping off, one and then the other.
	// (And the same out of strides all but stopped into another way of
	// going: a jog pulled up and then a backpedal.)
	test("setting off from where he stands, his feet step off - not both slid over the floor", () => {
		const body = bodyOf();
		const STEP = 16;
		const FORWARD = new Set<AnimName>(["jog", "run", "sprint", "walk"]);
		const SIDEWAYS = new Set<AnimName>(["back", "shuffle", "sideStep"]);
		const STANDS = new Set<AnimName>([
			"ready",
			"stance",
			...FORWARD,
			...SIDEWAYS,
		]);
		const GOES = new Set<AnimName>([...FORWARD, ...SIDEWAYS]);
		// (A change of way of going counted only from all but a standstill.)
		const offFrom = (from: AnimName, to: AnimName, v: number) =>
			from === "ready" ||
			from === "stance" ||
			(v < 2 &&
				((FORWARD.has(from) && SIDEWAYS.has(to)) ||
					(SIDEWAYS.has(from) && FORWARD.has(to))));
		let starts = 0;
		let slid = 0;
		const { tl } = compile("a", 120);
		for (const tr of tl.tracks.values()) {
			let prev: { x: number; y: number; z: number }[] | undefined;
			for (let t = 200_000; t < 500_000; t += STEP) {
				const st = evalPlayer(tl, tr.pid, t);
				const f = st.from;
				if (
					!st.shown ||
					fastAt(tl, t) > 1 ||
					f === undefined ||
					f.w <= 0.02 ||
					!STANDS.has(f.anim) ||
					!GOES.has(st.anim) ||
					(prev === undefined &&
						!offFrom(f.anim, st.anim, speedOf(tl, tr.pid, t)))
				) {
					prev = undefined;
					continue;
				}
				const me = withBody(st, body);
				const sk = skeleton(body, poseOf(me));
				const feet = [sk.legR, sk.legL].map((l) => bodyPoint(me, l.end));
				if (prev) {
					const was = prev;
					slid += Math.min(
						...feet.map((q, i) =>
							q.z < body.ankleH + 0.05 && was[i]!.z < body.ankleH + 0.05
								? Math.hypot(q.x - was[i]!.x, q.y - was[i]!.y)
								: 0,
						),
					);
				} else {
					starts += 1;
				}
				prev = feet;
			}
		}
		assert.isAbove(starts, 50);
		assert.isBelow(slid / starts, 0.04, `${slid} ft over ${starts} starts`);
	}, 120_000);

	test("a shooter drifting along the arc side-steps, a foot planted", () => {
		const { travel, slide } = plantedSlide("sideStep");
		assert.isAbove(travel, 20);
		assert.isBelow(slide, 0.35);
	}, 120_000);

	// Closing out, his short, choppy steps each land and stay put - not his
	// feet chopping up and down in place as he slides along over them.
	test("a man closing out chops his feet, each step planted", () => {
		const { travel, slide } = plantedSlide("closeout");
		assert.isAbove(travel, 10);
		assert.isBelow(slide, 0.5);
	}, 120_000);

	// Getting going from a standstill, or pulling up to one, his strides keep
	// a beat - short, quick steps - not one long stride gone through in slow
	// motion while he glides; and a foot down still stays where it is.
	test("setting off and pulling up, short quick steps - no slow-motion stride", () => {
		const { slow, slowMo, travel, slide } = goingSlow();
		assert.isAbove(slow, 2000);
		assert.isBelow(slowMo / slow, SLOW_MO_MOST, `${slowMo} of ${slow}`);
		assert.isAbove(travel, 2000);
		assert.isBelow(slide / travel, SLIDE_MOST);
	}, 240_000);

	// And each foot's stride goes over to the next over its swing - not
	// snapped longer as the other foot comes down, or shorter as he pulls
	// up late in a step, or started over as one run turns into another.
	// (63 such frames in game a before; 3 now, where one run hands on to
	// the next after a moment stood between them.)
	test("a foot's stride grows or shrinks over its swing, not in a frame", () => {
		const { short, jumps } = strideJumps();
		assert.isAbove(short, 2000);
		assert.isAtMost(jumps, 5);
	}, 240_000);

	// Backpedalling, then turned and away: his legs come round under him
	// over a couple of steps - not a foot swung from behind him to out in
	// front of him in a frame or two as his hips go round.
	test("turned out of a backpedal, his feet come round, not flung", () => {
		const { turns, flung, worst } = turnAndRun();
		assert.isAbove(turns, 100);
		assert.isBelow(flung / turns, 0.2);
		assert.isBelow(worst, 1.2);
	}, 240_000);

	// Cutting, going round a corner, a backpedal getting going: a foot on
	// the floor points where it came down while he turns over it - not spun
	// round under him with the rest of him - and goes round to his new way
	// while it is up. (Before: a degree a frame a foot down, and 99 feet
	// spun more than 30 degrees in their time down.)
	// Getting going he leans into it, pulling up he sits back against it - a
	// moment behind how hard he speeds up or slows down, not flicked from the
	// one to the other each time one short run of a man staying with his
	// gives way to the next. (Before: his head thrown more than a foot in a
	// frame 957 times in the first fifteen minutes.)
	test("his body leans into getting going and pulling up, never flicked", () => {
		const { frames, leaning, bracing, flicked } = headFlicks();
		assert.isAbove(leaning, frames * 0.05);
		assert.isAbove(bracing, frames * 0.025);
		assert.isAtMost(flicked, 10);
	}, 240_000);

	test("a foot down on his strides stays pointed where it came down", () => {
		const { down, spun, far } = plantedSpin();
		assert.isAbove(down, 10_000);
		assert.isBelow(spun / down, 0.35);
		assert.isBelow(far, 60);
	}, 240_000);

	// Sliding with a quick man, a defender's push steps get longer - his feet
	// further apart each push - not quicker and quicker until his legs
	// patter. (Before: nearly six pushes a second at ten feet a second.)
	test("sliding fast, his push steps lengthen, not patter", () => {
		const { time, pushes, gap } = fastSlides();
		assert.isAbove(time, 3);
		assert.isBelow(pushes / time, 4.4);
		assert.isAbove(gap, 3.6);
	}, 240_000);

	// Sliding with his man and turning to keep square to him, a defender's
	// planted foot stays pointed where it came down - the push foot through
	// the push, the lead foot through the close - not swivelled round with
	// his hips. (Before: planted feet turning 1.4 degrees a frame, as much as
	// his body; 20 a game turned more than 30 degrees while down.)
	test("sliding, the foot he is down on stays pointed where it came down", () => {
		const { down, spun, far } = slideSpin();
		assert.isAbove(down, 2000);
		assert.isBelow(spun / down, 0.3);
		assert.isAtMost(far, 6);
	}, 240_000);
});

const RUNNING = new Set<AnimName>(["run", "jog", "sprint", "dribble"]);
// (Before short steps: 0.71 of them, and 0.065 ft a foot.)
const SLOW_MO_MOST = 0.15;
const SLIDE_MOST = 0.06;

// Running (any pace) over the first fifteen minutes of a game, frame by
// frame: the frames he goes 1 to 6 ft a second, and of those the ones his
// strides go round slower than 0.6 a second; and how far he went with a
// foot in its time down, and how far that foot slid along the floor (the
// least of the two, both down; not easing in or out of something else).
const goingSlow = () => {
	const body = bodyOf();
	const STEP = 32;
	const tl = (game ??= compile("a", 140).tl);
	let slow = 0;
	let slowMo = 0;
	let travel = 0;
	let slide = 0;
	for (const tr of tl.tracks.values()) {
		let prev:
			| {
					x: number;
					y: number;
					phase: number;
					anim: AnimName;
					feet: { x: number; y: number }[];
					down: boolean[];
			  }
			| undefined;
		for (let t = 0; t < Math.min(tl.end, 900_000); t += STEP) {
			const st = evalPlayer(tl, tr.pid, t);
			if (!st.shown || !RUNNING.has(st.anim)) {
				prev = undefined;
				continue;
			}
			const me = withBody(st, body);
			const sk = skeleton(body, poseOf(me));
			const feet = [bodyPoint(me, sk.legR.end), bodyPoint(me, sk.legL.end)];
			const legs = legPhases(st.anim, st.phase)!;
			// (In its time down, not just landing or leaving the floor.)
			const down = [legs.r, legs.l].map(
				(phi) => phi > 0.02 && phi < legs.duty - 0.02,
			);
			if (prev && prev.anim === st.anim) {
				const d = Math.hypot(st.x - prev.x, st.y - prev.y);
				const v = (d * 1000) / STEP;
				if (v >= 1 && v < 6) {
					slow += 1;
					if (((st.phase - prev.phase) * 1000) / STEP < 0.6) {
						slowMo += 1;
					}
				}
				const both = [0, 1].filter(
					(i) =>
						down[i] && prev!.down[i] && Math.abs(st.phase - prev!.phase) < 0.3,
				);
				if (
					both.length > 0 &&
					d > 0.02 &&
					!(st.from !== undefined && st.from.w > 0.05)
				) {
					travel += d;
					slide += Math.min(
						...both.map((i) =>
							Math.hypot(
								feet[i]!.x - prev!.feet[i]!.x,
								feet[i]!.y - prev!.feet[i]!.y,
							),
						),
					);
				}
			}
			prev = { x: st.x, y: st.y, phase: st.phase, anim: st.anim, feet, down };
		}
	}
	return { slow, slowMo, travel, slide };
};

// Over the first fifteen minutes of a game, frame by frame on the same
// steps (not easing in or out of something else): the frames a leg's
// stride is short, and the frames one changed by more than a third of a
// full one. (Before: 52.)
const strideJumps = () => {
	const STEP = 32;
	const tl = (game ??= compile("a", 140).tl);
	let short = 0;
	let jumps = 0;
	for (const tr of tl.tracks.values()) {
		let prev: ReturnType<typeof evalPlayer> | undefined;
		for (let t = 0; t < Math.min(tl.end, 900_000); t += STEP) {
			// (Not where the picture runs through it fast.)
			if (fastAt(tl, t) > 1) {
				prev = undefined;
				continue;
			}
			const st = evalPlayer(tl, tr.pid, t);
			const was = prev;
			prev = st;
			if (
				!was ||
				!st.shown ||
				!was.shown ||
				was.anim !== st.anim ||
				(st.from !== undefined && st.from.w > 0.05)
			) {
				continue;
			}
			const a = was.stepLen ?? [1, 1];
			const b = st.stepLen ?? [1, 1];
			short += st.stepLen ? 1 : 0;
			if (Math.abs(a[0] - b[0]) > 0.33 || Math.abs(a[1] - b[1]) > 0.33) {
				jumps += 1;
			}
		}
	}
	return { short, jumps };
};

// Every turn out of a backpedal into a run in the first fifteen minutes of
// a game, frame by frame through it: how many there were, the frames a foot
// went more than 0.75 ft round him (beyond what his hips did), and the most.
const FORWARD = new Set<AnimName>([
	"run",
	"sprint",
	"jog",
	"walk",
	"dribble",
	"dribbleWalk",
	"carry",
]);
const turnAndRun = () => {
	const body = bodyOf();
	const tl = (game ??= compile("a", 140).tl);
	let turns = 0;
	let flung = 0;
	let worst = 0;
	for (const tr of tl.tracks.values()) {
		let back = -Infinity;
		let done = -Infinity;
		for (let t = 0; t < Math.min(tl.end, 900_000); t += 40) {
			const st = evalPlayer(tl, tr.pid, t);
			if (!st.shown) {
				continue;
			}
			if (st.anim === "back") {
				back = t;
				continue;
			}
			if (!FORWARD.has(st.anim) || t - back > 80 || t < done) {
				continue;
			}
			turns += 1;
			done = back + 500;
			let prev:
				| { x: number; y: number; feet: { x: number; y: number; z: number }[] }
				| undefined;
			for (let u = back; u < done; u += 16) {
				const s = evalPlayer(tl, tr.pid, u);
				const me = withBody(s, body);
				const sk = skeleton(body, poseOf(me));
				const hips = bodyPoint(me, sk.pelvis);
				const feet = [sk.legR.end, sk.legL.end].map((j) => {
					const f = bodyPoint(me, j);
					return { x: f.x - hips.x, y: f.y - hips.y, z: f.z };
				});
				if (prev && Math.hypot(s.x - prev.x, s.y - prev.y) < 1) {
					for (const [i, f] of feet.entries()) {
						const was = prev.feet[i]!;
						const d = Math.hypot(f.x - was.x, f.y - was.y, f.z - was.z);
						worst = Math.max(worst, d);
						if (d > 0.75) {
							flung += 1;
						}
					}
				}
				prev = { x: s.x, y: s.y, feet };
			}
		}
	}
	return { turns, flung, worst };
};

// Moving like this over the first fifteen minutes of a game: how far he
// went, and how far the foot that moved least slid along the floor for
// every foot of it (frame to frame; not easing in or out of it).
const plantedSlide = (anim: AnimName) => {
	const body = bodyOf();
	const STEP = 32;
	let travel = 0;
	let slide = 0;
	const tl = (game ??= compile("a", 140).tl);
	for (const tr of tl.tracks.values()) {
		let prev:
			| { x: number; y: number; feet: { x: number; y: number }[] }
			| undefined;
		for (let t = 0; t < Math.min(tl.end, 900_000); t += STEP) {
			const st = evalPlayer(tl, tr.pid, t);
			if (
				!st.shown ||
				st.anim !== anim ||
				(st.from !== undefined && st.from.w > 0.05)
			) {
				prev = undefined;
				continue;
			}
			const me = withBody(st, body);
			const sk = skeleton(body, poseOf(me));
			const feet = [bodyPoint(me, sk.legR.end), bodyPoint(me, sk.legL.end)];
			if (prev) {
				const d = Math.hypot(st.x - prev.x, st.y - prev.y);
				if (d > 0.02) {
					travel += d;
					slide += Math.min(
						...feet.map((f, i) =>
							Math.hypot(f.x - prev!.feet[i]!.x, f.y - prev!.feet[i]!.y),
						),
					);
				}
			}
			prev = { x: st.x, y: st.y, feet };
		}
	}
	return { travel, slide: slide / travel };
};
// On his strides over the first fifteen minutes of a game (not easing in or
// out of something else), frame by frame: the frames a foot is down, how
// far round its feet turned on the floor between them (degrees), and how
// many feet turned more than 30 degrees in all in their time down.
const STRIDES = new Set<AnimName>([...RUNNING, "walk", "back", "slide"]);
const plantedSpin = () => {
	const body = bodyOf();
	const STEP = 32;
	const tl = (game ??= compile("a", 140).tl);
	let down = 0;
	let spun = 0;
	let far = 0;
	for (const tr of tl.tracks.values()) {
		let prev: { yaws: number[]; down: boolean[]; phase: number } | undefined;
		const turned = [0, 0];
		for (let t = 0; t < Math.min(tl.end, 900_000); t += STEP) {
			const st = evalPlayer(tl, tr.pid, t);
			if (
				fastAt(tl, t) > 1 ||
				!st.shown ||
				!STRIDES.has(st.anim) ||
				(st.from !== undefined && st.from.w > 0.05)
			) {
				prev = undefined;
				turned.fill(0);
				continue;
			}
			const me = withBody(st, body);
			const sk = skeleton(body, poseOf(me));
			const yaws = [sk.legR, sk.legL].map((l) => {
				const heel = bodyPoint(me, l.end);
				const toe = bodyPoint(me, l.tip!);
				return Math.atan2(toe.y - heel.y, toe.x - heel.x);
			});
			const legs = legPhases(st.anim, st.phase)!;
			// (In its time down, not just landing or leaving the floor.)
			const isDown = [legs.r, legs.l].map((phi) => {
				const c = legs.back ? (((legs.duty - phi) % 1) + 1) % 1 : phi;
				return c > 0.02 && c < legs.duty - 0.02;
			});
			for (const i of [0, 1]) {
				if (
					prev &&
					isDown[i] &&
					prev.down[i] &&
					Math.abs(st.phase - prev.phase) < 0.3
				) {
					let d = yaws[i]! - prev.yaws[i]!;
					d -= Math.round(d / (Math.PI * 2)) * Math.PI * 2;
					const deg = (Math.abs(d) * 180) / Math.PI;
					down += 1;
					spun += deg;
					turned[i]! += deg;
				} else {
					if (turned[i]! > 30) {
						far += 1;
					}
					turned[i] = 0;
				}
			}
			prev = { yaws, down: isDown, phase: st.phase };
		}
	}
	return { down, spun, far };
};
// Frame by frame over the first fifteen minutes of a game (not where the
// picture runs through it fast): the frames he is driving into it or bracing
// against it to speak of, and the frames his head goes more than 40 feet a
// second.
const headFlicks = () => {
	const body = bodyOf();
	const STEP = 16;
	const tl = (game ??= compile("a", 140).tl);
	let frames = 0;
	let leaning = 0;
	let bracing = 0;
	let flicked = 0;
	for (const tr of tl.tracks.values()) {
		let prev: { x: number; y: number; z: number } | undefined;
		for (let t = 0; t < Math.min(tl.end, 900_000); t += STEP) {
			const st = evalPlayer(tl, tr.pid, t);
			if (fastAt(tl, t) > 1 || !st.shown) {
				prev = undefined;
				continue;
			}
			const me = withBody(st, body);
			const head = bodyPoint(me, skeleton(body, poseOf(me)).head);
			if (prev) {
				frames += 1;
				const v =
					(Math.hypot(head.x - prev.x, head.y - prev.y, head.z - prev.z) *
						1000) /
					STEP;
				if (v > 40) {
					flicked += 1;
				}
				if ((st.drive ?? 0) > 0.4) {
					leaning += 1;
				}
				if ((st.drive ?? 0) < -0.4) {
					bracing += 1;
				}
			}
			prev = head;
		}
	}
	return { frames, leaning, bracing, flicked };
};
let game: ReturnType<typeof compile>["tl"] | undefined;

// Shuffling straight across at 8 ft a second or more, unblended, over the
// first fifteen minutes: how long, how many pushes he took meanwhile, and
// the furthest apart his feet got.
const fastSlides = () => {
	const body = bodyOf();
	const STEP = 16;
	const tl = (game ??= compile("a", 140).tl);
	let time = 0;
	let pushes = 0;
	let gap = 0;
	for (const tr of tl.tracks.values()) {
		let prev: { x: number; y: number; phase: number } | undefined;
		for (let t = 0; t < Math.min(tl.end, 900_000); t += STEP) {
			const st = evalPlayer(tl, tr.pid, t);
			if (fastAt(tl, t) > 1 || !st.shown || st.anim !== "shuffle" || st.from) {
				prev = undefined;
				continue;
			}
			const way = st.way ?? [0, 0];
			const m = Math.hypot(way[0], way[1]);
			if (
				prev &&
				m > 1e-3 &&
				Math.abs(way[1]) / m > 0.6 &&
				(Math.hypot(st.x - prev.x, st.y - prev.y) * 1000) / STEP >= 8
			) {
				time += STEP / 1000;
				pushes += st.phase - prev.phase;
				const me = withBody(st, body);
				const sk = skeleton(body, poseOf(me));
				const [r, l] = [bodyPoint(me, sk.legR.end), bodyPoint(me, sk.legL.end)];
				gap = Math.max(gap, Math.hypot(r.x - l.x, r.y - l.y));
			}
			prev = { x: st.x, y: st.y, phase: st.phase };
		}
	}
	return { time, pushes, gap };
};

// Each foot down on a slide over the first fifteen minutes, frame by frame
// (not just landing or coming up): how many frames, how far it turned on
// the floor in all (degrees), and how many times one turned more than 30
// degrees in its time down. (Down: the foot he pushes off through the push,
// the one he leads with through the close - his left going to his left.)
const slideSpin = () => {
	const body = bodyOf();
	const STEP = 32;
	const tl = (game ??= compile("a", 140).tl);
	let down = 0;
	let spun = 0;
	let far = 0;
	for (const tr of tl.tracks.values()) {
		let prev: { yaws: number[]; down: boolean[]; phase: number } | undefined;
		const turned = [0, 0];
		for (let t = 0; t < Math.min(tl.end, 900_000); t += STEP) {
			const st = evalPlayer(tl, tr.pid, t);
			if (
				fastAt(tl, t) > 1 ||
				!st.shown ||
				st.anim !== "shuffle" ||
				!st.way ||
				(st.from !== undefined && st.from.w > 0.05)
			) {
				prev = undefined;
				turned.fill(0);
				continue;
			}
			const me = withBody(st, body);
			const sk = skeleton(body, poseOf(me));
			const yaws = [sk.legR, sk.legL].map((l) => {
				const heel = bodyPoint(me, l.end);
				const toe = bodyPoint(me, l.tip!);
				return Math.atan2(toe.y - heel.y, toe.x - heel.x);
			});
			const f = st.phase - Math.floor(st.phase);
			const left = st.way[1] > 0;
			const on = f < 0.5 === left ? 0 : 1;
			const inside = Math.abs(f - 0.5) > 0.04 && f > 0.04 && f < 0.96;
			const isDown = [0, 1].map((i) => inside && i === on);
			for (const i of [0, 1]) {
				if (
					prev &&
					isDown[i] &&
					prev.down[i] &&
					Math.abs(st.phase - prev.phase) < 0.3
				) {
					let d = yaws[i]! - prev.yaws[i]!;
					d -= Math.round(d / (Math.PI * 2)) * Math.PI * 2;
					const deg = (Math.abs(d) * 180) / Math.PI;
					down += 1;
					spun += deg;
					turned[i]! += deg;
				} else {
					if (turned[i]! > 30) {
						far += 1;
					}
					turned[i] = 0;
				}
			}
			prev = { yaws, down: isDown, phase: st.phase };
		}
	}
	return { down, spun, far };
};
