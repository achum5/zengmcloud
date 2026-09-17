import { assert, describe, test } from "vitest";
import {
	buildJerseySvg,
	cleanUniformSpec,
	isUniformJersey,
	parseUniform,
	presetToSpec,
	serializeUniform,
	UNIFORM_JERSEY_PREFIX,
	type UniformSpec,
} from "./uniform.ts";

const COLORS: [string, string, string] = ["#007a33", "#ffffff", "#ba9653"];

describe("parse and serialize", () => {
	test("round-trips a full spec", () => {
		const spec: UniformSpec = {
			base: "#ffffff",
			collar: [
				{ color: "#000000", width: 16 },
				{ color: "#007a33", width: 9 },
			],
			arm: [{ color: "#007a33", width: 7 }],
			yoke: "#007a33",
			band: "#ba9653",
			pinstripes: { color: "#ba9653", gap: 12, width: 2 },
			image: { url: "https://example.com/a.png", scale: 1.5, dx: 10 },
			shorts: { base: "#007a33", belt: "#ffffff", side: "#000000" },
			wordmark: { text: "BOSTON", color: "#ffffff" },
			number: { color: "#ffffff", outline: "#000000" },
		};
		assert.deepEqual(parseUniform(serializeUniform(spec)), spec);
	});

	test("preset ids and garbage are not uniforms", () => {
		assert.equal(parseUniform("jersey3"), undefined);
		assert.equal(parseUniform(undefined), undefined);
		assert.equal(parseUniform(`${UNIFORM_JERSEY_PREFIX}{not json`), undefined);
		assert.isFalse(isUniformJersey("jersey3"));
		assert.isTrue(isUniformJersey(`${UNIFORM_JERSEY_PREFIX}{}`));
	});

	test("a bad field is dropped, not fatal", () => {
		const cleaned = cleanUniformSpec({
			base: 'url(javascript:alert(1)) "><script>',
			collar: [
				{ color: "#fff", width: 9 },
				{ color: "red", width: 9 },
				{ color: "#fff", width: "wide" },
			],
			yoke: "#007a33",
			pinstripes: { color: "#123456", gap: 10000 },
			image: { url: "javascript:alert(1)" },
		});
		assert.deepEqual(cleaned, {
			collar: [{ color: "#fff", width: 9 }],
			yoke: "#007a33",
			pinstripes: { color: "#123456", gap: 60 },
		});
	});

	test("wordmark text is trimmed and capped", () => {
		const cleaned = cleanUniformSpec({
			wordmark: { text: "  A VERY LONG WORDMARK INDEED  " },
		});
		assert.equal(cleaned.wordmark!.text!.length, 16);
	});
});

describe("buildJerseySvg", () => {
	test("bakes the colors in and stays symmetric about x=200", () => {
		const svg = buildJerseySvg(
			{
				base: "#ffffff",
				collar: [
					{ color: "#000000", width: 16 },
					{ color: "#007a33", width: 9 },
				],
				arm: [{ color: "#007a33", width: 7 }],
				yoke: "#123456",
				pinstripes: { color: "#ba9653", gap: 20, width: 2 },
			},
			COLORS,
			"t1",
		);
		assert.include(svg, 'fill="#ffffff"');
		assert.include(svg, 'stroke="#007a33" stroke-width="9"');
		assert.include(svg, 'id="t1c"');
		// Every pinstripe right of center has its mirror on the left.
		const xs = [...svg.matchAll(/M(\d+) 505v110/g)].map((m) => Number(m[1]));
		for (const x of xs) {
			assert.include(xs, 400 - x, `pinstripe at ${x} has no mirror`);
		}
	});

	test("no placeholders survive - the string is final", () => {
		const svg = buildJerseySvg(presetToSpec("jersey2", COLORS), COLORS, "x");
		assert.notMatch(svg, /\$\[/);
	});

	test("base falls back to the first team color", () => {
		const svg = buildJerseySvg({}, COLORS, "x");
		assert.include(svg, 'fill="#007a33"');
	});

	test("the image URL is escaped and drawn as a pattern fill", () => {
		const svg = buildJerseySvg(
			{
				image: {
					url: 'https://example.com/a.png?x="1"&y=<2>',
					scale: 2,
					dx: 20,
				},
			},
			COLORS,
			"z",
		);
		assert.include(
			svg,
			"https://example.com/a.png?x=&quot;1&quot;&amp;y=&lt;2&gt;",
		);
		assert.include(svg, 'fill="url(#zp)"');
		// The image lives inside the pattern, never loose in the group - loose
		// extents would pull the body-size scaling off center.
		const image = svg.indexOf("<image");
		assert.ok(
			image > svg.indexOf("<pattern") && image < svg.indexOf("</pattern>"),
		);
	});

	test("an unsanitized spec is cleaned before drawing", () => {
		const svg = buildJerseySvg(
			{ base: '"><script>alert(1)</script>' } as UniformSpec,
			COLORS,
			"x",
		);
		assert.notInclude(svg, "script");
	});
});

describe("presetToSpec", () => {
	test("every basketball preset converts, and plain is empty", () => {
		assert.deepEqual(presetToSpec("jersey", COLORS), {});
		for (const id of ["jersey2", "jersey3", "jersey4", "jersey5"]) {
			const spec = presetToSpec(id, COLORS);
			assert.ok(
				(spec.collar?.length ?? 0) > 0,
				`${id} should carry collar trim`,
			);
			// And the conversion itself must be storable.
			assert.deepEqual(parseUniform(serializeUniform(spec)), spec);
		}
		assert.equal(presetToSpec("jersey3", COLORS).band, "#ffffff");
		assert.ok(presetToSpec("jersey5", COLORS).pinstripes);
	});
});
