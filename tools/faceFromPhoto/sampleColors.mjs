// Reads skin and hair color straight off the PIXELS of each player photo.
//
//   node tools/faceFromPhoto/sampleColors.mjs <photo-dir> [colors-out.json]
//   -> { "<srID>": { skin: "#6f4a2c", hair: "#141414", raceGuess, bald? } }
//
// Why this exists: the vision step is asked for a lot of things, and the one
// thing it is genuinely bad at is naming a hex. Ask a chat for a skin tone and
// it returns something plausible and wrong - and skin tone is the single
// biggest contributor to whether a 40px avatar reads as the right person. The
// pixels are right there and cost nothing, so sample them and let the vision
// step spend its attention on the things only it can do (face shape, hair
// style, facial hair).
//
// Feed the result to specsToFaces.mjs, which overlays these over whatever the
// chat guessed.
//
// Uses Playwright's chromium (already a dependency of the catalog renderer) to
// decode the images, so there's no native image library to install.

import { readdirSync, readFileSync, writeFileSync, existsSync } from "node:fs";
import { resolve, join, extname, basename } from "node:path";
import { chromium } from "playwright";
import { generate } from "facesjs";

const [dirArg, outArg] = process.argv.slice(2);
if (!dirArg) {
	console.error(
		"usage: node tools/faceFromPhoto/sampleColors.mjs <photo-dir> [colors-out.json]",
	);
	process.exit(1);
}
const dir = resolve(dirArg);
if (!existsSync(dir)) {
	console.error(`no such directory: ${dir}`);
	process.exit(1);
}
const outPath = resolve(outArg ?? join(dir, "..", "colors.json"));

const IMAGE_EXT = new Set([".jpg", ".jpeg", ".png", ".webp"]);
const files = readdirSync(dir)
	.filter((f) => IMAGE_EXT.has(extname(f).toLowerCase()))
	.sort();

if (files.length === 0) {
	console.error(`no images in ${dir}`);
	process.exit(1);
}

// Sampling happens in the page, where there's a real image decoder and a
// canvas. Everything below runs on the pixels of ONE photo.
const SAMPLE_IN_PAGE = async (src) => {
	const img = new Image();
	img.src = src;
	await img.decode();

	const W = 200;
	const H = Math.max(1, Math.round((img.naturalHeight / img.naturalWidth) * W));
	const canvas = document.createElement("canvas");
	canvas.width = W;
	canvas.height = H;
	const ctx = canvas.getContext("2d", { willReadFrequently: true });
	ctx.drawImage(img, 0, 0, W, H);
	const { data } = ctx.getImageData(0, 0, W, H);

	const at = (x, y) => {
		const i = (y * W + x) * 4;
		return [data[i], data[i + 1], data[i + 2], data[i + 3]];
	};

	// Many of these photos have had their background removed (see debgLeague.py),
	// so the subject is whatever is opaque. Find its bounding box and work in
	// those coordinates - otherwise a portrait with a lot of empty margin gets
	// sampled in the wrong place entirely.
	let x0 = W;
	let y0 = H;
	let x1 = -1;
	let y1 = -1;
	let opaque = 0;
	for (let y = 0; y < H; y++) {
		for (let x = 0; x < W; x++) {
			if (at(x, y)[3] >= 200) {
				opaque += 1;
				if (x < x0) {
					x0 = x;
				}
				if (x > x1) {
					x1 = x;
				}
				if (y < y0) {
					y0 = y;
				}
				if (y > y1) {
					y1 = y;
				}
			}
		}
	}
	// A photo with no alpha channel is fully opaque; use the whole frame.
	if (x1 < 0 || opaque > W * H * 0.98) {
		x0 = 0;
		y0 = 0;
		x1 = W - 1;
		y1 = H - 1;
	}
	const bw = x1 - x0 + 1;
	const bh = y1 - y0 + 1;

	const sqDist = (a, b) =>
		(a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2 + (a[2] - b[2]) ** 2;

	// On a photo with no alpha the background is still there, and above a bald
	// head the "crown" is backdrop, not scalp - which sampled a studio blue and
	// called it hair. Read the four corners; if they agree with each other it's a
	// plain backdrop and anything matching it can be thrown away. If they
	// disagree the crop is tight or busy, so trust nothing and skip the test.
	const patch = (px0, px1, py0, py1) => {
		const px = [];
		for (let y = py0; y < py1; y++) {
			for (let x = px0; x < px1; x++) {
				if (x < 0 || y < 0 || x >= W || y >= H) {
					continue;
				}
				const p = at(x, y);
				if (p[3] >= 200) {
					px.push([p[0], p[1], p[2]]);
				}
			}
		}
		if (px.length === 0) {
			return undefined;
		}
		return [0, 1, 2].map((c) => px.reduce((s, q) => s + q[c], 0) / px.length);
	};

	// Sampled BESIDE THE TEMPLES, not at the corners. The bottom corners of a
	// headshot are shoulders and the top corners can be a tall afro, but the
	// strips either side of the head at eye level are backdrop on essentially
	// every portrait, and are never hair. Both sides have to agree, so a busy or
	// tightly-cropped photo just opts out.
	const eyeY0 = Math.round(H * 0.33);
	const eyeY1 = Math.round(H * 0.45);
	const left = patch(0, 5, eyeY0, eyeY1);
	const right = patch(W - 5, W, eyeY0, eyeY1);
	let background;
	if (left && right && sqDist(left, right) < 1200) {
		background = [0, 1, 2].map((c) => (left[c] + right[c]) / 2);
	}
	const isBackground = (p) =>
		background !== undefined && sqDist(p, background) < 900;

	// Collect the subject's pixels inside a fractional box of its bounding area.
	// Returns how much of the box survived, so the caller can tell "mostly hair"
	// from "mostly sky".
	const region = (fx0, fx1, fy0, fy1) => {
		const px = [];
		let total = 0;
		for (
			let y = Math.round(y0 + bh * fy0);
			y < Math.round(y0 + bh * fy1);
			y++
		) {
			for (
				let x = Math.round(x0 + bw * fx0);
				x < Math.round(x0 + bw * fx1);
				x++
			) {
				if (x < 0 || y < 0 || x >= W || y >= H) {
					continue;
				}
				total += 1;
				const p = at(x, y);
				const rgb = [p[0], p[1], p[2]];
				if (p[3] >= 200 && !isBackground(rgb)) {
					px.push(rgb);
				}
			}
		}
		return { px, coverage: total > 0 ? px.length / total : 0 };
	};

	// The MEDOID - the observed pixel closest to the per-channel median. A plain
	// per-channel median can invent a color that appears nowhere in the photo
	// (and on a face with a strong shadow it lands between skin and shadow);
	// the medoid is always a color the camera actually saw.
	const medoid = (px) => {
		if (px.length === 0) {
			return undefined;
		}
		const med = [0, 1, 2].map((c) => {
			const s = px.map((p) => p[c]).sort((a, b) => a - b);
			return s[Math.floor(s.length / 2)];
		});
		let best;
		let bestD = Infinity;
		for (const p of px) {
			const d =
				(p[0] - med[0]) ** 2 + (p[1] - med[1]) ** 2 + (p[2] - med[2]) ** 2;
			if (d < bestD) {
				bestD = d;
				best = p;
			}
		}
		return best;
	};

	const lum = (p) => 0.2126 * p[0] + 0.7152 * p[1] + 0.0722 * p[2];

	// Skin: the cheeks and jaw. Deliberately below the eyes and inside the face,
	// so hair, eyebrows and the neckline stay out of it. Trim the darkest and
	// brightest fifth first - that's shadow under the jaw and specular highlight
	// on the forehead, neither of which is the person's colour.
	let skinPx = region(0.3, 0.7, 0.48, 0.74).px;
	if (skinPx.length > 40) {
		skinPx.sort((a, b) => lum(a) - lum(b));
		const cut = Math.floor(skinPx.length * 0.2);
		skinPx = skinPx.slice(cut, skinPx.length - cut);
	}
	const skin = medoid(skinPx);

	// Hair: the crown. Whatever is left there after the backdrop is removed - if
	// most of the box WAS backdrop there is no hair above this head, and if what
	// remains matches his skin it's a shaved scalp. Both mean bald.
	const crown = region(0.32, 0.68, 0.02, 0.18);
	const hair = crown.coverage >= 0.35 ? medoid(crown.px) : undefined;

	return { skin, hair, hairCoverage: crown.coverage };
};

const hex = (p) =>
	`#${p
		.map((v) =>
			Math.max(0, Math.min(255, Math.round(v)))
				.toString(16)
				.padStart(2, "0"),
		)
		.join("")}`;

const dist = (a, b) =>
	(a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2 + (a[2] - b[2]) ** 2;

const parseHex = (h) => {
	const s = h.replace("#", "");
	return [
		Number.parseInt(s.slice(0, 2), 16),
		Number.parseInt(s.slice(2, 4), 16),
		Number.parseInt(s.slice(4, 6), 16),
	];
};

// faces.js keeps its skin palettes internal (not exported, and the package's
// export map blocks reaching into build/), so learn them from the library
// itself: generate a pile of faces per race and collect the skin colors it
// actually produces. Self-calibrating, so a facesjs update can't leave a
// hardcoded copy behind.
const RACES = ["white", "black", "brown", "asian"];
const skinPalette = new Map(
	RACES.map((race) => {
		const seen = new Set();
		for (let i = 0; i < 60; i++) {
			seen.add(generate(undefined, { race, gender: "male" }).body.color);
		}
		return [race, [...seen].map(parseHex)];
	}),
);

// Which palette the sampled tone is nearest. This only chooses DEFAULTS for the
// slots nobody predicts - the exact sampled hex is what actually gets rendered.
const nearestRace = (skin) => {
	let best = "white";
	let bestD = Infinity;
	for (const [race, swatches] of skinPalette) {
		for (const swatch of swatches) {
			const d = dist(skin, swatch);
			if (d < bestD) {
				bestD = d;
				best = race;
			}
		}
	}
	return best;
};

// PW_CHROME is an escape hatch for environments where only the full chromium
// build is present (Playwright otherwise wants its headless shell).
const browser = await chromium.launch(
	process.env.PW_CHROME ? { executablePath: process.env.PW_CHROME } : {},
);
const page = await browser.newPage();

const out = {};
const skipped = [];

for (const file of files) {
	const srID = basename(file, extname(file));
	let result;
	try {
		// Passed as a data URL rather than file://. The blank page the sampler
		// runs in has no origin, so a file:// image is refused as cross-origin
		// and the canvas would be tainted even if it loaded.
		const ext = extname(file).toLowerCase();
		const mime =
			ext === ".png"
				? "image/png"
				: ext === ".webp"
					? "image/webp"
					: "image/jpeg";
		const dataUrl = `data:${mime};base64,${readFileSync(join(dir, file)).toString("base64")}`;
		result = await page.evaluate(SAMPLE_IN_PAGE, dataUrl);
	} catch (error) {
		skipped.push(`${srID} (unreadable: ${error.message.split("\n")[0]})`);
		continue;
	}

	if (!result.skin) {
		skipped.push(`${srID} (no opaque pixels where a face should be)`);
		continue;
	}

	const entry = { skin: hex(result.skin), raceGuess: nearestRace(result.skin) };

	// Believe the hair sample only if it's meaningfully darker or otherwise
	// different from the skin. On a bald or shaved head the crown IS skin, and
	// writing that back as a hair colour paints a skin-coloured wig on him.
	if (result.hair && dist(result.hair, result.skin) > 900) {
		entry.hair = hex(result.hair);
	} else {
		entry.bald = true;
	}

	out[srID] = entry;
}

await browser.close();

writeFileSync(outPath, `${JSON.stringify(out, null, 2)}\n`);

console.log(`sampled ${Object.keys(out).length} of ${files.length} photos`);
console.log(`-> ${outPath}`);
if (skipped.length > 0) {
	console.log(`\nskipped ${skipped.length}:`);
	for (const s of skipped.slice(0, 20)) {
		console.log(`  ${s}`);
	}
	if (skipped.length > 20) {
		console.log(`  ...and ${skipped.length - 20} more`);
	}
}
const baldCount = Object.values(out).filter((e) => e.bald).length;
if (baldCount > 0) {
	console.log(
		`\n${baldCount} read as bald/shaved (crown matched skin) - the vision step should confirm.`,
	);
}
