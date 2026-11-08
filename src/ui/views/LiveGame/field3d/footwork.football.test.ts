import { assert, beforeAll, describe, test } from "vitest";
import { bodyPoint } from "../court3d/evaluate.ts";
import { skeleton } from "../court3d/poses.ts";
import { drawnBody, footballBody } from "./body.ts";
import { compileField, type FieldTimeline } from "./director.ts";
import { momentAt } from "./evaluate.ts";
import { moveKind } from "./poses.ts";
import { poseOf } from "./sprite.ts";
import { simGames } from "./testGames.ts";

// FOOTWORK, the way the court's is held to it (see court3d/footwork.test.ts):
// a man on his strides has a foot down on the turf, and the foot he is down
// on stays put while the rest of him goes on over it - not slid along under
// him. How far the steadier of his two feet goes, frame by frame, against
// how far he does, over a stretch of a game.
const STEP = 20;
const measure = (tl: FieldTimeline, pids: Set<number>) => {
	const body = drawnBody(footballBody());
	const prev = new Map<
		number,
		{ t: number; x: number; y: number; feet: { x: number; y: number }[] }
	>();
	let travel = 0;
	let slide = 0;
	// (Getting going and pulling up: his steps short.)
	let shortTravel = 0;
	let shortSlide = 0;
	for (let t = 60_000; t < 240_000; t += STEP) {
		for (const st of momentAt(tl, t).players) {
			if (!pids.has(st.id)) {
				continue;
			}
			const sk = skeleton(body, poseOf(st));
			const feet = [bodyPoint(st, sk.legR.end), bodyPoint(st, sk.legL.end)];
			const p = prev.get(st.id);
			prev.set(st.id, { t, x: st.x, y: st.y, feet });
			if (
				!p ||
				p.t !== t - STEP ||
				moveKind(st.move) !== "cycle" ||
				st.from !== undefined
			) {
				continue;
			}
			const d = Math.hypot(st.x - p.x, st.y - p.y);
			const v = (d * 1000) / STEP;
			// (On his strides - not put somewhere new between two plays.)
			if (v < 2 || v >= 40) {
				continue;
			}
			const s = Math.min(
				...feet.map((f, i) =>
					Math.hypot(f.x - p.feet[i]!.x, f.y - p.feet[i]!.y),
				),
			);
			travel += d;
			slide += s;
			if (st.len !== undefined && st.len < 0.9) {
				shortTravel += d;
				shortSlide += s;
			}
		}
	}
	return { travel, slide, shortTravel, shortSlide };
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

	// (Where it stands: 0.33 - the court's men, measured so, 0.11.)
	test("on his strides, the foot he is down on stays put - mostly", () => {
		assert.isAbove(got.travel, 5000);
		assert.isBelow(got.slide / got.travel, 0.36);
	});

	// (Where it stands: 0.49.)
	test("getting going and pulling up, short steps - his feet not skated", () => {
		assert.isAbove(got.shortTravel, 500);
		assert.isBelow(got.shortSlide / got.shortTravel, 0.52);
	});
});
