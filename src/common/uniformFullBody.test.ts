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
		const matches = [...svg.matchAll(/M(\d+) (588|870)v/g)];
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
