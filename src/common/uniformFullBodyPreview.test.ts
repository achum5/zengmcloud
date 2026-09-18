// A LOOK AT THE PLAYER, not an assertion about him. The full body is art, and
// the only way to judge art is to render it and look, so this writes a page of
// real composites - the faces.js face over the body SVG, at several skin
// tones, heights and uniforms - and leaves it somewhere a browser can open it.
//
// Skipped entirely unless BODY_OUT is set, exactly like the recap corpus, so it
// costs CI nothing.
//
//   SPORT=basketball BODY_OUT=/tmp/body.html \
//     npx vitest run --project basketball src/common/uniformFullBodyPreview.test.ts

import { faceToSvgString, generate } from "facesjs";
import { describe, test } from "vitest";
import { presetToSpec } from "./uniform.ts";
import { buildFullBodySvg, FULL_BODY_VIEWBOX } from "./uniformFullBody.ts";

// Same two dodges the recap corpus uses: this file is typechecked without
// node types, so process.env is reached through globalThis and node:fs is
// imported under a name the resolver cannot see.
const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const OUT = nodeEnv.BODY_OUT;

describe.skipIf(!OUT)("full body preview", () => {
	test("writes a page of players", async () => {
		const { writeFileSync } = await import(("node" + ":fs") as any);

		const cases: {
			label: string;
			race: "white" | "black" | "brown" | "asian";
			hgt: number;
			colors: [string, string, string];
			jersey: string;
			number: string;
		}[] = [
			{
				label: "Celtics, 6'8\"",
				race: "black",
				hgt: 80,
				colors: ["#008348", "#ffffff", "#000000"],
				jersey: "jersey3",
				number: "10",
			},
			{
				label: "Lakers, 6'2\"",
				race: "white",
				hgt: 74,
				colors: ["#552583", "#fdb927", "#000000"],
				jersey: "jersey",
				number: "7",
			},
			{
				label: "Heat, 7'1\"",
				race: "brown",
				hgt: 85,
				colors: ["#98002e", "#f9a01b", "#000000"],
				jersey: "jersey2",
				number: "33",
			},
			{
				label: "Knicks, 5'11\"",
				race: "asian",
				hgt: 71,
				colors: ["#006bb6", "#f58426", "#ffffff"],
				jersey: "jersey4",
				number: "4",
			},
			{
				label: "Jazz, 6'5\"",
				race: "black",
				hgt: 77,
				colors: ["#002b5c", "#f9a01b", "#00471b"],
				jersey: "jersey5",
				number: "21",
			},
		];

		let html =
			`<!doctype html><meta charset="utf-8">` +
			`<style>body{background:#22262a;margin:0;padding:24px;` +
			`font:13px system-ui;color:#ccc;display:flex;gap:8px;flex-wrap:wrap}` +
			`figure{margin:0;width:260px;text-align:center}` +
			`.wrap{position:relative;aspect-ratio:400/1300}` +
			`.wrap>*{position:absolute;top:0;left:0;width:100%}` +
			// The face box must be EXACTLY 600 of the 1300 units tall. Stretching
			// it taller leaves the faces.js jersey's own bottom edge (at y=610,
			// outside its viewBox) unclipped, which paints a black bar across
			// the chest that the real component never shows.
			`.body{height:100%}` +
			`.face{height:${(600 / 1300) * 100}%}` +
			`.face>svg{width:100%;height:100%}</style>`;

		for (const [i, c] of cases.entries()) {
			const face = generate(
				{ race: c.race },
				{ gender: "male", race: c.race },
			) as any;
			// Deterministic-ish: force a mid skin tone per race is not needed, the
			// generator already picks one. Keep the body size as generated.
			const spec = presetToSpec(c.jersey, c.colors)!;
			const body = buildFullBodySvg({
				spec,
				teamColors: c.colors,
				skinColor: face.body.color,
				bodySize: face.body.size,
				hgt: c.hgt,
				jerseyNumber: c.number,
				idBase: `p${i}`,
			});
			// teamColors matters: without it the face wears a jersey in the
			// generator's random colours while the body wears the team's, and
			// every seam looks broken for a reason that is not the seam's.
			const faceSvg = faceToSvgString(face, {
				teamColors: c.colors,
				jersey: { id: c.jersey },
			} as any);
			html +=
				`<figure><div class="wrap">` +
				`<svg class="body" viewBox="${FULL_BODY_VIEWBOX}">${body}</svg>` +
				`<div class="face">${faceSvg}</div>` +
				`</div><figcaption>${c.label}</figcaption></figure>`;
		}

		writeFileSync(OUT!, html);

		console.log(`wrote ${OUT}`);
	});
});
