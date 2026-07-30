// Renders a { srID: FaceConfig } dataset into one labeled review sheet, so a
// batch of just-built faces can be eyeballed against the source photos.
//
//   node tools/faceFromPhoto/renderFaces.mjs <faces.json> <out.png> [names.json]
//
// names.json (optional) maps srID -> display label; otherwise the srID shows.

import { readFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";
import { faceToSvgString } from "facesjs";
import { chromium } from "playwright";

const [facesPath, outPath, namesPath] = process.argv.slice(2);
if (!facesPath || !outPath) {
	console.error(
		"usage: node renderFaces.mjs <faces.json> <out.png> [names.json]",
	);
	process.exit(1);
}
const faces = JSON.parse(readFileSync(resolve(facesPath), "utf8"));
const names =
	namesPath && existsSync(resolve(namesPath))
		? JSON.parse(readFileSync(resolve(namesPath), "utf8"))
		: {};

const cells = Object.entries(faces)
	.map(
		([id, f]) =>
			`<figure>${faceToSvgString(f)}<figcaption>${names[id] ?? id}</figcaption></figure>`,
	)
	.join("");
const html = `<!doctype html><meta charset=utf8><style>
 body{margin:0;background:#fff;display:flex;flex-wrap:wrap;gap:10px;padding:12px;font:14px sans-serif;width:840px}
 figure{margin:0;text-align:center} svg{width:150px;height:225px}
</style>${cells}`;

const b = await chromium.launch();
const p = await b.newPage();
await p.setContent(html);
await p.locator("body").screenshot({ path: resolve(outPath) });
await b.close();
console.log("wrote", outPath);
