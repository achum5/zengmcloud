import { assert, describe, test } from "vitest";
import { buildFullBodySvg, contrastColor } from "./uniformFullBody.ts";
import type { UniformSpec } from "./uniform.ts";

const COLORS: [string, string, string] = ["#007a33", "#ffffff", "#ba9653"];

const build = (
	spec: UniformSpec = {},
	extra: Partial<Parameters<typeof buildFullBodySvg>[0]> = {},
) =>
	buildFullBodySvg({
		spec,
		teamColors: COLORS,
		skinColor: "#ad7460",
		jerseyNumber: "0",
		idBase: "t",
		...extra,
	});

describe("buildFullBodySvg", () => {
	test("wordmark and number are escaped and colors baked in", () => {
		const svg = build({
			base: "#ffffff",
			wordmark: { text: 'BOS<T>&"ON', color: "#007a33" },
			number: { color: "#007a33" },
		});
		assert.include(svg, "BOS&lt;T&gt;&amp;&quot;ON");
		assert.notInclude(svg, "<T>");
		assert.include(svg, 'fill="#ffffff"');
	});

	test("shorts follow the jersey unless told otherwise", () => {
		assert.include(build({ base: "#123456" }), 'fill="#123456" stroke');
		const svg = build({
			base: "#123456",
			shorts: { base: "#654321", belt: "#ffffff" },
		});
		assert.include(svg, 'fill="#654321"');
		assert.include(svg, 'fill="#ffffff" stroke');
	});

	test("an unsanitized spec cannot inject markup", () => {
		const svg = build({
			base: '"><script>x</script>',
			wordmark: { text: "OK", color: "bad" },
		} as UniformSpec);
		assert.notInclude(svg, "script");
	});

	test("height stretches the legs, body size scales the width", () => {
		assert.include(build({}, { hgt: 85 }), "scale(1 1.084");
		assert.notInclude(build({}, { hgt: 78 }), "scale(1 1.084");
		assert.include(build({}, { bodySize: 1.05 }), "scale(1.05 1)");
	});

	test("pinstripes mirror about center on jersey and shorts", () => {
		const svg = build({ pinstripes: { color: "#ba9653", gap: 20 } });
		const matches = [...svg.matchAll(/M(\d+) (588|886)v/g)];
		assert.ok(matches.length > 8);
		for (const [, x, y] of matches) {
			assert.include(svg, `M${400 - Number(x)} ${y}v`);
		}
	});
});

describe("contrastColor", () => {
	test("light jerseys get dark text, dark jerseys light", () => {
		assert.equal(contrastColor("#ffffff"), "#000000");
		assert.equal(contrastColor("#007a33"), "#ffffff");
		assert.equal(contrastColor("#fff"), "#000000");
	});
});

// THE SEAM. The body only looks like one drawing if it starts exactly where
// faces.js stops, so the two numbers that pin it are worth holding onto.
describe("the join with the faces.js face", () => {
	const seam = () => build();

	test("the body starts exactly where the face ends", () => {
		const svg = seam();
		// faces.js's body path starts at (10,600) - the widest point of its
		// shoulder - so the arm's outer edge has to start there too.
		assert.include(svg, "M10 600");
		// Every basketball jersey faces.js draws runs its side from (80,610)
		// up to (90,520), so by the time the canvas cuts it at y=600 the edge
		// has already reached 82.6. Anchoring the torso at 80 left a ledge
		// down both sides of the chest.
		assert.include(svg, "M82.6 600");
		assert.include(svg, "317.4 600");
	});

	test("nothing that meets the seam outlines its own top edge", () => {
		// faces.js fills an OPEN path: only the drawn segments are stroked, so
		// there is no line along the bottom where its canvas ends. A closed
		// stroked shape here put a black line across the top of the arm and the
		// chest that the face could not quite cover - a notch poking past the
		// shoulder, and a bar over the jersey. So anything reaching y=600 is
		// filled closed but outlined with an open path.
		const outlines = [
			...seam().matchAll(
				/<path fill="none" stroke="#000" stroke-width="6"[^>]*d="([^"]+)"/g,
			),
		].map((m) => m[1]!);
		const atSeam = outlines.filter((d) => d.includes(" 600"));
		assert.ok(atSeam.length >= 2, "expected the arm and torso outlines");
		for (const d of atSeam) {
			assert.notMatch(d, /Z/, `outline closes over the seam: ${d}`);
		}
	});

	test("both sides come from one shape, so they cannot drift apart", () => {
		// The limbs are drawn once and mirrored about x=200.
		assert.include(seam(), 'transform="translate(400,0) scale(-1,1)"');
	});

	test("the armhole binding carries on down the side seam", () => {
		// faces.js finishes the armhole with a stack of bands that arrives at
		// the seam 16 units wide; picking it up with a plain 6-wide outline
		// left a step at both armpits, so the whole stack continues.
		const svg = build({
			arm: [
				{ color: "#000000", width: 16 },
				{ color: "#007a33", width: 12 },
				{ color: "#ba9653", width: 6 },
			],
		});
		for (const width of [16, 12, 6]) {
			assert.include(svg, `stroke-width="${width}" d="M82.6 600`);
		}
	});
});
