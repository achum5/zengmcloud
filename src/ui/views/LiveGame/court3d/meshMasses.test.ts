// A TOOL, not a test: the court's Classic player as the sculpt lays him
// (sculpt.ts's masses), stood with his arms out from his sides - written out
// for tools/court3d-body/classic.py to build in Blender. Skipped unless
// asked for (see tools/court3d-body/build.sh): COURT3D_BODY_OUT=masses.json.
import { test } from "vitest";
import { makeCamera, MAIN_RIG } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import { kitsFor, type Look } from "./figure.ts";
import { ANIMS, bodyOf, poseAt } from "./poses.ts";
import { build } from "./sculpt.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);
const OUT = (
	globalThis as { process?: { env: Record<string, string | undefined> } }
).process?.env.COURT3D_BODY_OUT;
test.skipIf(!OUT)("the Classic player's masses, for Blender", async () => {
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
	// Stood up straight, legs a little apart, arms out from his sides.
	const pose = {
		...poseAt("ready", 0),
		hipN: 0,
		kneeN: 0,
		hipF: 0,
		kneeF: 0,
		shN: 0,
		shF: 0,
		elN: 8,
		elF: 8,
		abN: 45,
		abF: 45,
		lean: 0,
		// (His legs apart: his shorts two legs, not one.)
		wide: 0.6,
		wrN: 0,
		wrF: 0,
		twist: 0,
		tilt: 0,
		plant: 0,
		toe: 0,
		toeN: 0,
		toeF: 0,
		flare: 0,
		tuck: 0,
	};
	(ANIMS as Record<string, unknown>).blenderRest = {
		kind: "loop",
		n: 1,
		fps: 1,
		pose: () => pose,
	};
	const st = {
		pid: 1,
		team: 0,
		shown: true,
		x: 47,
		y: 25,
		z: 0,
		yaw: 0,
		anim: "blenderRest",
		phase: 0,
		moving: false,
		holding: false,
	} as unknown as PlayerState;
	const b = build(cam, st, body, look, 0.02, 0, 0, true);
	const masses = b.masses.map((m) => ({
		A: [m.A.f, m.A.s, m.A.u],
		B: [m.B.f, m.B.s, m.B.u],
		r: Array.from(m.rad, (x) => x / m.k),
		kind: m.kind,
		group: m.group,
		side: m.side,
		flat: !!m.flat,
	}));
	const fs = await importNode("node:fs");
	fs.writeFileSync(OUT!, JSON.stringify({ body, masses, sk: b.sk, pose }));
	(ANIMS as Record<string, unknown>).blenderRest = undefined;
});
