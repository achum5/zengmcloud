// Generates the "prompt kit" for converting player photos into faces.js specs
// using a Claude chat (your subscription) — no API key, no per-call billing.
//
//   node tools/faceFromPhoto/buildPromptKit.mjs
//   -> prompt-kit/INSTRUCTIONS.md    paste this into a Claude chat
//   -> prompt-kit/catalog-<slot>.png attach these so it picks by appearance
//
// Workflow:
//   1. downloadImages.mjs  -> a folder of <srID>.jpg photos
//   2. New Claude chat: paste INSTRUCTIONS.md, attach the catalog-*.png sheets,
//      then attach a batch of player photos. It replies with { srID: spec }.
//   3. Save its JSON, run specsToFaces.mjs -> applyFaces.mjs -> re-import.

import { writeFileSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { chromium } from "playwright";
import { faceToSvgString, generate, svgsIndex } from "facesjs";

const here = dirname(fileURLToPath(import.meta.url));
const kitDir = join(here, "prompt-kit");
mkdirSync(kitDir, { recursive: true });

// The slots that carry identity, and which the catalog sheets illustrate.
const SHEET_SLOTS = [
	"head",
	"hair",
	"facialHair",
	"glasses",
	"accessories",
	"nose",
	"eye",
	"eyebrow",
	"mouth",
];

const idList = (slot) => svgsIndex[slot].join(", ");

const INSTRUCTIONS = `# Convert player headshots to faces.js specs

You turn real basketball player headshots into **faces.js** cartoon-avatar
specs. For every player photo attached to this chat, output a compact JSON
"spec" that, rendered, looks as much like that player as possible. The goal is
recognizable likeness — skin tone, hair, facial hair, glasses/headband, face
shape — not a perfect portrait.

Each photo is named \`<srID>.<ext>\` (e.g. \`piercpa01.jpg\`). Use that srID as
the key. **Output ONE JSON object** mapping each srID to its spec, wrapped in a
single \`\`\`json code fence, and nothing else.

## Output format (exactly this shape)

\`\`\`json
{
  "piercpa01": {
    "gender": "male",
    "race": "black",
    "colors": { "skin": "#6f4a2c", "hair": "#141414", "shave": "rgba(0,0,0,0.18)" },
    "shape": { "fatness": 0.3, "nose": 1.0, "ear": 0.9 },
    "slots": {
      "head": "head7", "hair": "short", "facialHair": "goatee1",
      "glasses": "none", "accessories": "headband",
      "eye": "eye8", "eyebrow": "eyebrow3", "nose": "nose5", "mouth": "mouth"
    }
  }
}
\`\`\`

## Rules (accuracy first)

- **Sample skin and hair color exactly** as hex from the photo — these matter
  most for likeness. \`colors.skin\` is the face color; \`colors.hair\` the hair.
- \`race\` (white | black | brown | asian) seeds coherent defaults for anything
  you leave unset — pick by appearance.
- **Only use ids from the lists below** (or the attached catalog sheets). Any id
  not on the list is discarded and replaced by a default, so don't guess names.
- **Only add what's actually in the photo.** Bald → \`hair: "bald"\` or
  \`"short-bald"\`. Clean-shaven → \`facialHair: "none"\`. Add
  \`accessories: "headband"\` / \`glasses: "..."\` ONLY if worn in the shot.
- \`colors.shave\` is a faint beard-shadow: \`"rgba(0,0,0,0)"\` clean-shaven, up to
  about \`"rgba(0,0,0,0.25)"\` for heavy stubble.
- Numbers: \`fatness\` 0–1 (build/face width), \`nose\` 0.5–1.25, \`ear\` 0.5–1.5,
  \`eye.angle\`/\`eyebrow.angle\` -15..20 (optional, as \`{ "id": "...", "angle": n }\`).
- Anything you omit stays a coherent default — a short, confident spec beats a
  full one with wrong guesses.

## Valid ids per slot

- **head** (face shape): ${idList("head")}
- **hair**: ${idList("hair")}
- **facialHair**: ${idList("facialHair")}
- **glasses**: ${idList("glasses")}
- **accessories** (headband, etc.): ${idList("accessories")}
- **nose**: ${idList("nose")}
- **eye**: ${idList("eye")}
- **eyebrow**: ${idList("eyebrow")}
- **mouth**: ${idList("mouth")}
- **ear**: ${idList("ear")}

Ids beginning \`female…\` are female-featured variants — use them for female
players. The attached \`catalog-*.png\` sheets show every option labeled with its
id; match by appearance.
`;

writeFileSync(join(kitDir, "INSTRUCTIONS.md"), INSTRUCTIONS);

// Render one labeled contact sheet per identity slot, so the chat can pick by
// appearance instead of by id name alone.
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

const browser = await chromium.launch();
const page = await browser.newPage();
for (const slot of SHEET_SLOTS) {
	const cells = svgsIndex[slot]
		.filter((id) => !id.startsWith("female"))
		.map(
			(id) =>
				`<figure>${faceToSvgString(base, { [slot]: { id } })}<figcaption>${id}</figcaption></figure>`,
		)
		.join("");
	const html = `<!doctype html><meta charset=utf8><style>
	 body{margin:0;background:#fff;display:flex;flex-wrap:wrap;gap:6px;padding:10px;width:1100px;font:12px sans-serif}
	 figure{margin:0;text-align:center} svg{width:110px;height:165px}
	 figcaption{color:#222}
	 h1{width:100%;margin:0 0 6px;font-size:16px}
	</style><h1>faces.js — ${slot} options</h1>${cells}`;
	await page.setContent(html);
	await page.locator("body").screenshot({ path: join(kitDir, `catalog-${slot}.png`) });
}
await browser.close();

console.log(`Wrote prompt-kit/INSTRUCTIONS.md and ${SHEET_SLOTS.length} catalog sheets.`);
