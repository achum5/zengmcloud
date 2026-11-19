import { assert, test } from "vitest";
import { makeCamera, MAIN_RIG } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import { headOf, kitsFor, type Look } from "./figure.ts";
import { ANIMS, bodyOf, type AnimName } from "./poses.ts";
import { build, NECK } from "./sculpt.ts";

// HIS HEAD ON HIS NECK, whatever he does: the top of his neck in his head
// where it is drawn (see figure.ts) - tipped to one side on a crossover,
// turned with his shoulders on a spin or a lean jumper, not left in the
// middle of him with his head off to one side of it - and his neck no
// longer bent right over than stood up: in at the back of his head, not on
// round under his chin.
test("his neck comes up into his head, no longer than it is, in every move", () => {
	const cam = makeCamera({ x: 47, width: 30, y: 24 }, 640, 360, MAIN_RIG);
	const body = bodyOf();
	const look: Look = {
		kit: kitsFor(undefined, undefined)[0]!,
		skin: "#8d5524",
		hair: "#1f1612",
		jerseyNumber: "",
		name: "",
		lastName: "",
		wordmark: "",
	};
	const neckOf = (anim: AnimName, phase: number) => {
		const st = {
			pid: 1,
			team: 0,
			shown: true,
			x: 47,
			y: 25,
			z: 0,
			yaw: 0.7,
			anim,
			phase,
			moving: false,
			holding: false,
		} as PlayerState;
		const b = build(cam, st, body, look, 0.02, 0, 0, true);
		return { st, neck: b.masses.find((m) => m.kind === NECK) };
	};
	const stood = neckOf("ready", 0).neck!;
	const stoodLong = Math.hypot(
		stood.B.f - stood.A.f,
		stood.B.s - stood.A.s,
		stood.B.u - stood.A.u,
	);
	let worst = 0;
	let where = "";
	let longest = 0;
	let longWhere = "";
	for (const anim of Object.keys(ANIMS) as AnimName[]) {
		for (const phase of [0, 0.25, 0.45, 0.6, 0.8, 1]) {
			const { st, neck } = neckOf(anim, phase);
			if (!neck) {
				continue;
			}
			const head = headOf(st, body);
			const off =
				Math.hypot(neck.B.f - head.f, neck.B.s - head.s, neck.B.u - head.u) /
				body.headR;
			if (off > worst) {
				worst = off;
				where = `${anim} at ${phase}`;
			}
			const long =
				Math.hypot(
					neck.B.f - neck.A.f,
					neck.B.s - neck.A.s,
					neck.B.u - neck.A.u,
				) / stoodLong;
			if (long > longest) {
				longest = long;
				longWhere = `${anim} at ${phase}`;
			}
		}
	}
	// (Its top inside his head, just short of the middle of it - never out
	// to one side of it, or down under it.)
	assert.isBelow(worst, 0.65, where);
	assert.isBelow(longest, 1.15, longWhere);
});
