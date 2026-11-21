import { beforeAll, describe, expect, test } from "vitest";
import {
	drawMesh,
	loadMesh,
	MESH_KINDS,
	meshReady,
	poseBones,
	skinned,
} from "./meshBody.ts";
import { bodyOf, MESH_BUILD, poseAt, skeleton, type V3 } from "./poses.ts";

// A MAN DRAWN AS ONE (see meshBody.ts): his mesh on his skeleton, whatever
// he does - all of him where a man is, and drawn whole: no gaps through
// him where his faces meet.

const POSES = [
	["easy", 0],
	["run", 0.25],
	["shoot", 0.35],
	["stance", 0],
	["rebound", 0.5],
	["layup", 0.5],
] as const;

describe("mesh bodies", () => {
	beforeAll(async () => {
		await loadMesh();
	});

	test("loaded on asking", () => {
		expect(meshReady()).toBe(true);
	});

	test("every point of him finite, and him his height standing", () => {
		const body = bodyOf(78, 210, MESH_BUILD);
		const typical = bodyOf(undefined, undefined, MESH_BUILD);
		for (const [anim, phase] of POSES) {
			const sk = skeleton(body, poseAt(anim, phase));
			const out = new Float32Array(20000 * 3);
			skinned(poseBones(sk, body, typical), body.H, out);
			let top = -Infinity;
			let low = Infinity;
			for (let i = 0; i < out.length / 3; i++) {
				const u = out[i * 3 + 2]!;
				if (u === 0 && out[i * 3] === 0) {
					continue;
				}
				expect(Number.isFinite(u)).toBe(true);
				top = Math.max(top, u);
				low = Math.min(low, u);
			}
			if (anim === "easy") {
				// (Up to the top of his head - or, with no head of its own (the
				// court's drawn on him), to the top of his neck under it.)
				const tall = MESH_KINDS.includes("head")
					? body.H
					: sk.head.u - body.headR * 0.6;
				expect(top - low).toBeGreaterThan(tall * 0.9);
				expect(top - low).toBeLessThan(tall * 1.08);
			}
		}
	});

	test("drawn whole: no gaps through him", () => {
		const body = bodyOf(78, 210, MESH_BUILD);
		const typical = bodyOf(undefined, undefined, MESH_BUILD);
		const W = 240;
		const Hh = 520;
		const k = Hh / (body.H * 1.2);
		for (const [anim, phase] of POSES) {
			const sk = skeleton(body, poseAt(anim, phase));
			const d = new Uint8ClampedArray(W * Hh * 4);
			const z = new Float32Array(W * Hh).fill(Infinity);
			const ink = { r: 0, g: 0, b: 0, cloth: false, tone: 0 };
			drawMesh(
				poseBones(sk, body, typical),
				body.H,
				{
					see: (v: V3) => ({
						x: W / 2 - v.s * k,
						y: Hh - 20 - v.u * k,
						depth: -v.f,
					}),
					rt: { f: 0, s: -1, u: 0 },
					dn: { f: 0, s: 0, u: -1 },
					toward: { f: 1, s: 0, u: 0 },
					w: W,
					h: Hh,
					paint: () => {
						ink.r = 200;
						ink.g = 200;
						ink.b = 200;
						return 0;
					},
					hidden: MESH_KINDS.map(() => false),
					cuts: MESH_KINDS.map(() => false),
					keep: () => true,
					lift: MESH_KINDS.map(() => 0),
					faced: MESH_KINDS.map(() => false),
					faceAt: new Float32Array(4),
					ink,
				},
				d,
				z,
			);
			// (A pixel left out between two of him either side of it.)
			let gaps = 0;
			const solid = (x: number, y: number) => d[(y * W + x) * 4 + 3]! >= 200;
			for (let y = 1; y < Hh - 1; y++) {
				for (let x = 1; x < W - 1; x++) {
					if (
						d[(y * W + x) * 4 + 3]! < 140 &&
						solid(x - 1, y) &&
						solid(x + 1, y) &&
						solid(x, y - 1) &&
						solid(x, y + 1)
					) {
						gaps++;
					}
				}
			}
			expect(gaps, anim).toBe(0);
		}
	});
});
