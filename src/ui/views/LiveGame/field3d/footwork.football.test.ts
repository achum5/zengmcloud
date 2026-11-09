import { assert, beforeAll, describe, test } from "vitest";
import { bodyPoint } from "../court3d/evaluate.ts";
import { legPhases, skeleton } from "../court3d/poses.ts";
import { drawnBody, footballBody } from "./body.ts";
import { compileField, type FieldTimeline } from "./director.ts";
import { momentAt } from "./evaluate.ts";
import { gaitOf } from "./poses.ts";
import { poseOf } from "./sprite.ts";
import { simGames } from "./testGames.ts";

// FOOTWORK, held to what the court's is (see court3d/footwork.test.ts): a
// man on his strides has a foot down on the turf, and it stays put while the
// rest of him goes on over it - not slid along under him. Over a stretch of
// a game, every man on the field.
const STEP = 32;
type Feet = { x: number; y: number }[];
const wrap = (a: number) => a - Math.round(a / (2 * Math.PI)) * 2 * Math.PI;
const measure = (tl: FieldTimeline, pids: Set<number>) => {
	const body = drawnBody(footballBody());
	const prev = new Map<
		number,
		{
			t: number;
			x: number;
			y: number;
			yaw: number;
			feet: Feet;
			heads: number[];
			down: boolean[];
		}
	>();
	// Running: how far each foot down (the middle of its time down) goes,
	// against how far he does.
	let went = 0;
	let slid = 0;
	// Getting going and pulling up, his steps short: how far the steadier of
	// his feet goes, against how far he does.
	let shortWent = 0;
	let shortSlid = 0;
	// Turning as he runs: how far round each foot down goes, against how far
	// he does.
	let turned = 0;
	let spun = 0;
	for (let t = 60_000; t < 240_000; t += STEP) {
		for (const st of momentAt(tl, t).players) {
			if (!pids.has(st.id)) {
				continue;
			}
			const gait = st.from ? undefined : gaitOf(st.move);
			const legs = gait ? legPhases(gait, st.phase) : undefined;
			if (!gait || !legs) {
				prev.delete(st.id);
				continue;
			}
			const sk = skeleton(body, poseOf(st));
			const feet = [bodyPoint(st, sk.legR.end), bodyPoint(st, sk.legL.end)];
			// (Which way each points: heel to toe.)
			const heads = [sk.legR, sk.legL].map((l, i) => {
				const toe = bodyPoint(st, l.tip ?? l.end);
				return Math.atan2(toe.y - feet[i]!.y, toe.x - feet[i]!.x);
			});
			const down = [legs.r, legs.l].map(
				(f) => f > legs.duty * 0.1 && f < legs.duty * 0.9,
			);
			const p = prev.get(st.id);
			prev.set(st.id, { t, x: st.x, y: st.y, yaw: st.yaw, feet, heads, down });
			if (!p || p.t !== t - STEP) {
				continue;
			}
			const d = Math.hypot(st.x - p.x, st.y - p.y);
			// (On his strides - not put somewhere new between two plays.)
			if ((d * 1000) / STEP >= 40) {
				continue;
			}
			const moved = feet.map((f, i) =>
				Math.hypot(f.x - p.feet[i]!.x, f.y - p.feet[i]!.y),
			);
			if (gait === "run" || gait === "jog" || gait === "sprint") {
				const turn = Math.abs(wrap(st.yaw - p.yaw));
				for (const i of [0, 1]) {
					if (down[i] && p.down[i]) {
						went += d;
						slid += moved[i]!;
						if (turn > 0.01) {
							turned += turn;
							spun += Math.abs(wrap(heads[i]! - p.heads[i]!));
						}
					}
				}
			}
			if (
				st.len !== undefined &&
				Math.min(...st.len) < 0.9 &&
				(d * 1000) / STEP >= 2
			) {
				shortWent += d;
				shortSlid += Math.min(...moved);
			}
		}
	}
	return { went, slid, shortWent, shortSlid, turned, spun };
};

describe("footwork", () => {
	let got: ReturnType<typeof measure>;
	beforeAll(async () => {
		const [game] = await simGames([7]);
		const tl = compileField({
			events: game!.events,
			players: game!.roster,
			gid: 1,
		});
		got = measure(tl, new Set(game!.roster.map((r) => r.pid)));
	}, 240_000);

	// (Before football's strides were the court's: 0.14.)
	test("running, a foot down stays put", () => {
		assert.isAbove(got.went, 3000);
		assert.isBelow(got.slid / got.went, 0.05);
	});

	// (Before: all the way round with him, 1.)
	test("cutting, a foot down stays pointed where it came down - mostly", () => {
		assert.isAbove(got.turned, 50);
		assert.isBelow(got.spun / got.turned, 0.45);
	});

	test("getting going and pulling up, short steps - his feet not skated", () => {
		assert.isAbove(got.shortWent, 1000);
		assert.isBelow(got.shortSlid / got.shortWent, 0.22);
	});
});
