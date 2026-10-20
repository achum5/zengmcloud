import { assert, test } from "vitest";
import {
	addReport,
	advanceLead,
	agrees,
	dueChecks,
	FOLLOW_BEHIND,
	type FollowReport,
	followRate,
	followStep,
	makeFollow,
	noteShown,
	SYNC_BEHIND,
} from "./follow.ts";

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

// Playing along: the other device's court plays one show ms a ms (except
// paused); it reports every second, each report reaching this device
// 80-700ms later (never out of order), stamped by a clock 40s off this one.
const playAlong = ({
	seconds,
	start = 0,
	pause,
	reloadAt,
	frameMs = 1000 / 60,
}: {
	seconds: number;
	start?: number;
	pause?: { at: number; for: number };
	reloadAt?: number;
	frameMs?: number;
}) => {
	const f = makeFollow();
	const skew = 40_000;
	const inbox: { due: number; r: FollowReport }[] = [];
	const frames: {
		now: number;
		ms: number;
		dt: number;
		w: number;
		other: number;
	}[] = [];
	let other = start;
	let paused = false;
	let sent = -Infinity;
	let n = 0;
	let w = 0;
	let snaps = 0;
	let reloaded = false;
	for (let now = 0; now < seconds * 1000; now += frameMs) {
		const p = !!pause && now >= pause.at && now < pause.at + pause.for;
		if (!p) {
			other += frameMs;
		}
		if (reloadAt !== undefined && !reloaded && now >= reloadAt) {
			reloaded = true;
			other = 0;
		}
		if (p !== paused || now - sent >= 1000) {
			paused = p;
			sent = now;
			n += 1;
			const due = Math.max(
				inbox.at(-1)?.due ?? 0,
				now + 80 + ((n * 7919) % 620),
			);
			inbox.push({
				due,
				r: {
					t: other * 2,
					w: other,
					rp: -1,
					cursor: 0,
					paused: p,
					at: now + skew,
					recv: 0,
				},
			});
		}
		while (inbox.length > 0 && inbox[0]!.due <= now) {
			const { r } = inbox.shift()!;
			addReport(f, { ...r, recv: now });
		}
		const step = followStep(f, w, now, frameMs);
		if (step.snap) {
			snaps += 1;
			w = step.snap.w;
		} else {
			w += step.ms;
		}
		frames.push({ now, ms: step.ms, dt: frameMs, w, other });
	}
	return { frames, snaps };
};

test("playing along: never stops, never ahead, about a second behind", () => {
	const { frames, snaps } = playAlong({ seconds: 120 });
	assert.strictEqual(snaps, 0);
	const later = frames.filter((x) => x.now > 3000);
	assert.isTrue(later.every((x) => x.ms > 0));
	assert.isTrue(frames.every((x) => x.w <= x.other));
	const gaps = later.map((x) => x.other - x.w);
	assert.isAtMost(Math.max(...gaps), SYNC_BEHIND + 900);
	assert.isAtLeast(Math.min(...gaps), SYNC_BEHIND - 200);
	// Almost all of it at the other court's own speed.
	const even = later.filter((x) => Math.abs(x.ms / x.dt - 1) < 0.02).length;
	assert.isAbove(even / later.length, 0.9);
	assert.isTrue(later.every((x) => x.ms >= 0.8 * x.dt - 1e-9));
});

test("playing along on a slow device: the same", () => {
	const { frames } = playAlong({ seconds: 60, frameMs: 50 });
	const later = frames.filter((x) => x.now > 3000);
	assert.isTrue(later.every((x) => x.ms > 0 && x.w <= x.other));
});

test("the other device pauses: on to the picture it stopped on, held there", () => {
	const { frames } = playAlong({
		seconds: 40,
		pause: { at: 20_000, for: 5000 },
	});
	assert.isTrue(frames.every((x) => x.w <= x.other));
	const stopped = frames.find(
		(x) => x.now > 20_000 && x.now < 25_000 && x.ms === 0,
	)!;
	assert.isDefined(stopped);
	// The very picture the other court stopped on.
	assert.approximately(stopped.w, stopped.other, 1);
	// Going again soon after it does, and on without stopping from there.
	const going = frames.find((x) => x.now > 25_000 && x.ms > 0)!;
	assert.isBelow(going.now, 25_000 + 2500);
	assert.isTrue(frames.filter((x) => x.now > going.now).every((x) => x.ms > 0));
});

test("joining late: straight to where the other court was", () => {
	const { frames, snaps } = playAlong({ seconds: 10, start: 600_000 });
	assert.strictEqual(snaps, 1);
	const last = frames.at(-1)!;
	assert.isAtMost(last.other - last.w, SYNC_BEHIND + 900);
});

test("the other court starting over: cut back to it", () => {
	const { frames, snaps } = playAlong({ seconds: 40, reloadAt: 30_000 });
	assert.strictEqual(snaps, 1);
	const last = frames.at(-1)!;
	assert.isTrue(last.w <= last.other && last.other - last.w < 3000);
});

test("checked against what this court showed: agreeing, through a cut, and not", () => {
	const f = makeFollow();
	const per = () => 2;
	const report = (w: number, t: number): FollowReport => ({
		t,
		w,
		rp: -1,
		cursor: 0,
		paused: false,
		at: 0,
		recv: 0,
	});
	// This court played on at 2 game ms a show ms, then cut ahead at 1000.
	for (let w = 0; w <= 1000; w += 16) {
		noteShown(f, { w, t: 2 * w, rp: -1 });
	}
	noteShown(f, { w: 1000, t: 2000, rp: -1 });
	noteShown(f, { w: 1000, t: 90_000, rp: -1 });
	for (let w = 1016; w <= 2000; w += 16) {
		noteShown(f, { w, t: 90_000 + 2 * (w - 1000), rp: -1 });
	}
	assert.isTrue(agrees(f, report(500, 1000), per));
	// Either side of the cut.
	assert.isTrue(agrees(f, report(1000, 2000), per));
	assert.isTrue(agrees(f, report(1000, 90_000), per));
	assert.isTrue(agrees(f, report(1500, 91_000), per));
	// The other court went on further (a skip there).
	assert.isFalse(agrees(f, report(1500, 150_000), per));
	assert.isFalse(agrees(f, report(500, 5000), per));
	// In a replay there, not here.
	assert.isFalse(agrees(f, { ...report(500, 1000), rp: 900 }, per));

	// Out of a replay between two moments here: the one after says.
	const g = makeFollow();
	noteShown(g, { w: 0, t: 5000, rp: 4000 });
	noteShown(g, { w: 16, t: 5000, rp: 4006 });
	noteShown(g, { w: 32, t: 5032, rp: -1 });
	assert.isTrue(agrees(g, report(20, 5008), per));
});

test("a report is checked once this court is past it, or at it when paused", () => {
	const f = makeFollow();
	const r = (w: number, paused: boolean, at: number): FollowReport => ({
		t: 0,
		w,
		rp: -1,
		cursor: 0,
		paused,
		at,
		recv: at,
	});
	addReport(f, r(1000, false, 1));
	addReport(f, r(2000, true, 2));
	assert.strictEqual(dueChecks(f, 1100).length, 0);
	assert.strictEqual(dueChecks(f, 1400).length, 1);
	assert.strictEqual(dueChecks(f, 1999.9999).length, 1);
});
