// A LOOK AT A CAREER, not an assertion about one. Aging is art as much as
// model, and the only way to judge it is to put the same head at 19 and at 38
// side by side and look. Writes a page of progression strips - one row per
// player, one face per age - for a browser to screenshot.
//
// Skipped unless AGING_OUT is set, like the recap corpus, so CI pays nothing.
//
//   SPORT=basketball AGING_OUT=/tmp/aging.html \
//     npx vitest run --project basketball src/worker/util/agingPreview.test.ts

import { faceToSvgString, generate } from "facesjs";
import { describe, test } from "vitest";
import { ageFace, applyRealisticFace } from "./realisticFaces.ts";

const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const OUT = nodeEnv.AGING_OUT;

const seeded = (seed: number) => {
	let a = seed >>> 0;
	return () => {
		a += 0x6d2b79f5;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

const RACES = ["black", "white", "brown", "asian"] as const;

// facesjs's generate() reads Math.random directly, so a preview is only
// reproducible - and only comparable across a change - if it is lent a seeded
// one for the length of the call.
const withSeededRandom = <T>(seed: number, fn: () => T): T => {
	const real = Math.random;
	const rand = seeded(seed * 104_729 + 7);
	Math.random = rand;
	try {
		return fn();
	} finally {
		Math.random = real;
	}
};

describe.skipIf(!OUT)("aging preview", () => {
	test("writes progression strips", async () => {
		const { writeFileSync } = await import(("node" + ":fs") as any);

		const FROM = 19;
		const TO = 38;
		const SHOW = Number(nodeEnv.AGING_EVERY ?? 3);
		const ROWS = Number(nodeEnv.AGING_ROWS ?? 10);
		const SEED0 = Number(nodeEnv.AGING_SEED ?? 0);

		const ages: number[] = [];
		for (let age = FROM; age <= TO; age += SHOW) {
			ages.push(age);
		}
		if (ages.at(-1) !== TO) {
			ages.push(TO);
		}

		let html =
			`<!doctype html><meta charset="utf-8">` +
			`<style>body{background:#1c2024;margin:0;padding:18px;` +
			`font:12px system-ui;color:#c9d1d9}` +
			`table{border-collapse:collapse}` +
			`td,th{padding:2px 3px;text-align:center}` +
			`th{font-weight:600;color:#8b949e;font-size:11px}` +
			`.f{width:96px;height:144px}` +
			`.lbl{text-align:right;padding-right:10px;color:#8b949e;white-space:nowrap}` +
			`</style><table>`;
		html += `<tr><th></th>${ages.map((a) => `<th>${a}</th>`).join("")}</tr>`;

		for (let i = 0; i < ROWS; i++) {
			const pid = SEED0 + i;
			const rand = seeded(pid * 7919 + 13);
			// Keyed to the pid, not the row, so a strip shows the same man
			// whatever offset the page starts at.
			const race = RACES[pid % RACES.length]!;
			const face: any = withSeededRandom(pid, () =>
				generate({ jersey: { id: "jersey" } }, { gender: "male", race }),
			);
			applyRealisticFace(face, { age: FROM, race, pid, rand });

			const cells: string[] = [];
			const want = new Set(ages);
			if (want.has(FROM)) {
				cells.push(snap(face));
			}
			for (let age = FROM + 1; age <= TO; age++) {
				ageFace(face, age, pid, rand);
				if (want.has(age)) {
					cells.push(snap(face));
				}
			}
			html += `<tr><td class="lbl">#${pid} ${race}</td>${cells
				.map((c) => `<td><div class="f">${c}</div></td>`)
				.join("")}</tr>`;
		}

		html += "</table>";
		writeFileSync(OUT!, html);
	});
});

const snap = (face: any): string =>
	faceToSvgString(
		structuredClone(face) as any,
		{
			accessories: { id: "none" },
			glasses: { id: "none" },
		} as any,
	);
