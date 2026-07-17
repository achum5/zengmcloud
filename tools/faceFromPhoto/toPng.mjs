// Dev-only helper: rasterize one or more SVG files (or a slot's whole option
// row) into a single PNG so a human — or a vision model in the refine loop —
// can eyeball them. Uses the pre-installed Chromium via Playwright.
//
//   node tools/faceFromPhoto/toPng.mjs face out/mike-miller.svg
//   node tools/faceFromPhoto/toPng.mjs slot head        # contact strip of a slot

import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { chromium } from "playwright";
import { faceToSvgString, generate, svgsIndex } from "facesjs";

const here = dirname(fileURLToPath(import.meta.url));
const outDir = join(here, "out");
const [mode, arg] = process.argv.slice(2);

let cells = [];
if (mode === "slot") {
	const base = generate(
		{
			facialHair: { id: "none" },
			glasses: { id: "none" },
			accessories: { id: "none" },
			hair: { id: "short", color: "#4a3626" },
			body: { color: "#e7b489" },
			fatness: 0.3,
			eye: { angle: 0 },
			eyebrow: { angle: 0 },
		},
		{ gender: "male", race: "white" },
	);
	cells = svgsIndex[arg]
		.filter((id) => !id.startsWith("female"))
		.map((id) => ({ label: id, svg: faceToSvgString(base, { [arg]: { id } }) }));
} else {
	cells = process.argv
		.slice(3)
		.map((p) => ({ label: p.split("/").pop(), svg: readFileSync(p, "utf8") }));
}

const html = `<!doctype html><meta charset=utf8><style>
 body{margin:0;background:#fff;display:flex;flex-wrap:wrap;gap:6px;padding:8px;width:${mode === "slot" ? "1100px" : "auto"}}
 figure{margin:0;text-align:center;font:12px sans-serif}
 svg{width:120px;height:180px} figcaption{color:#333}
</style>${cells
	.map((c) => `<figure>${c.svg}<figcaption>${c.label}</figcaption></figure>`)
	.join("")}`;

const file = join(outDir, `_view-${mode}-${(arg ?? "").replace(/[^\w]/g, "")}.png`);
const browser = await chromium.launch();
const page = await browser.newPage();
await page.setContent(html);
await page.locator("body").screenshot({ path: file });
await browser.close();
console.log(file);
