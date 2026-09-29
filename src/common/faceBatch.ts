import { svgsIndex, type FaceConfig } from "facesjs";
import { FACE_FROM_PHOTO_PROMPT } from "./faceFromPhotoPrompt.ts";
import { parseFaceJson } from "./repairFaceJson.ts";

// BATCH CONVERSION: many photos, one reply, one paste.
//
// The single-photo prompt is the whole method; a batch only changes what goes
// in (several labelled photos) and what comes out (one object keyed by player
// id). So the batch prompt is the single prompt with a header that says how the
// photos are labelled and how to answer, and the same answer format repeated at
// the very end, where a model reading 8000 words of instructions weighs it most.

export type BatchEntry = {
	// 1-based position in the batch, printed on the photo's label.
	n: number;
	pid: number;
	name: string;
};

// "#3 · id 1234 · First Last" - printed under each photo, and the same line in
// the roster, so the model can tie a photo to an id without trusting the order
// attachments arrive in.
export const batchLabel = (entry: BatchEntry) =>
	`#${entry.n} · id ${entry.pid} · ${entry.name}`;

const replyFormat = (entries: BatchEntry[]) => {
	const first = entries[0]?.pid ?? 1234;
	const second = entries[1]?.pid ?? 5678;
	return `Reply with ONE fenced \`json\` code block holding ONE object, and
nothing before it. Its keys are the player ids from the roster, as strings, in
roster order; each value is that player's complete face object with every key
from the output shape, written on ONE line. Every player on the roster appears
exactly once:

\`\`\`json
{
"${first}": {"fatness": 0.3, "teamColors": ["#89bfd3", "#7a1319", "#07364f"], "hairBg": {"id": "none"}, "...": "every key, on this one line"},
"${second}": {"fatness": 0.5, "teamColors": ["#89bfd3", "#7a1319", "#07364f"], "hairBg": {"id": "none"}, "...": "every key, on this one line"}
}
\`\`\`

(The \`"..."\` entries only mark where the rest of the keys go — write the
real keys, never a \`"..."\` key.)

After the block you may add a short \`Notes:\` list, one line per player that
needs one, each starting with his roster line's \`#n\` and name. Skip players
you are sure about.`;
};

export const buildBatchPrompt = (
	entries: BatchEntry[],
	source: "sheet" | "files",
	// Columns in the contact sheet. With it, each roster line also gives its
	// photo's place in the grid - the fallback when a screenshot cuts the
	// labels off, which on a hand-taken screenshot of the sheet is common.
	sheetCols?: number,
) => {
	const place = (i: number) =>
		source === "sheet" && sheetCols
			? ` (row ${Math.floor(i / sheetCols) + 1}, column ${(i % sheetCols) + 1})`
			: "";
	const roster = entries
		.map((entry, i) => `${batchLabel(entry)}${place(i)}`)
		.join("\n");
	const photos =
		source === "sheet"
			? `The attached image is a CONTACT SHEET: ${entries.length} headshots in a grid, each with a white label strip directly under it.`
			: `There are ${entries.length} attached photos, each with a white label strip along its bottom edge.`;

	return `# Batch mode: ${entries.length} players

This message converts several DIFFERENT players at once. ${photos} The label
reads \`#n · id … · Name\` and matches one line of this roster:

${roster}

How to work through a batch:

- Match every answer to its player by the LABEL, never by the order the
  images arrived in.${
		source === "sheet" && sheetCols
			? `
- A label cut off or unreadable (a screenshot can crop the bottom row): use
  the grid place the roster gives for that line instead, counting rows from
  the top and columns from the left. Never leave a visible photo out, and
  never give it a placeholder face because its label is missing.`
			: ""
	}
- The label strip is not part of the photo. Ignore it when you judge colors.
- Do each player as a separate, complete job: run the whole method below on
  him, from studying the face to the final check, before you move on. Photos
  in one batch are not related, so nothing carries over from the last one.
  The typical batch failure is a row of faces that share the same eyes,
  nose, head and mouth because the first answer set a pattern; decide every
  slot from THIS photo.
- The name is only there to keep the answers matched. Read the face from the
  photo; if you happen to know the player, the photo still wins wherever the
  two disagree, because it is the look he has in this game.
- A photo that is missing, blank or unreadable still gets an entry: your best
  neutral face, flagged in the notes.

## Batch reply format (this replaces the single-photo output instructions below)

${replyFormat(entries)}

The method for each face follows. Wherever it talks about "the photo" or "the
reply", read it as one player's photo and that player's entry in the batch
object.

---

${FACE_FROM_PHOTO_PROMPT}

---

## Batch reply format, again

${replyFormat(entries)}

The roster, once more:

${roster}
`;
};

// Every key facesjs reads. A model that drops one (it happens, mostly with
// hairBg and the line slots) would otherwise leave a slot that throws while
// drawing, so a missing slot is filled with its plain "none" option - the same
// default the prompt tells the model to use for anything unreadable.
const REQUIRED_SLOTS = [
	"accessories",
	"body",
	"ear",
	"eye",
	"eyeLine",
	"eyebrow",
	"facialHair",
	"glasses",
	"hair",
	"hairBg",
	"head",
	"jersey",
	"miscLine",
	"mouth",
	"nose",
	"smileLine",
] as const;

const SLOT_DEFAULTS: Record<(typeof REQUIRED_SLOTS)[number], object> = {
	accessories: { id: "none" },
	body: { id: "body", color: "#bb876f", size: 1 },
	ear: { id: "ear2", size: 1 },
	eye: { id: "eye13", angle: 0 },
	eyeLine: { id: "none" },
	eyebrow: { id: "eyebrow15", angle: 0 },
	facialHair: { id: "none" },
	glasses: { id: "none" },
	hair: { id: "short", color: "#272421", flip: false },
	hairBg: { id: "none" },
	head: { id: "head5", shave: "rgba(0,0,0,0)" },
	jersey: { id: "jersey" },
	miscLine: { id: "none" },
	mouth: { id: "straight", flip: false },
	nose: { id: "nose7", flip: false, size: 1 },
	smileLine: { id: "none", size: 1 },
};

const isRecord = (value: unknown): value is Record<string, any> =>
	typeof value === "object" && value !== null && !Array.isArray(value);

// A pasted object is a face if it has the slots that only a face has. Used to
// tell a face from the batch object that holds faces.
const looksLikeFace = (value: unknown): value is Record<string, any> =>
	isRecord(value) && isRecord(value.head) && isRecord(value.hair);

export type CheckedFace = {
	face: FaceConfig;
	// Things the reviewer should look at: slots that were missing and got a
	// default, ids faces.js doesn't have (they draw as a blank slot).
	warnings: string[];
};

export const checkFace = (raw: Record<string, any>): CheckedFace => {
	const warnings: string[] = [];
	const face: Record<string, any> = { ...raw };
	delete face["..."];

	if (typeof face.fatness !== "number") {
		face.fatness = 0.4;
		warnings.push("fatness missing");
	}
	if (!Array.isArray(face.teamColors)) {
		face.teamColors = ["#89bfd3", "#7a1319", "#07364f"];
	}

	for (const slot of REQUIRED_SLOTS) {
		if (!isRecord(face[slot])) {
			face[slot] = { ...SLOT_DEFAULTS[slot] };
			if (slot !== "jersey") {
				warnings.push(`${slot} missing`);
			}
			continue;
		}
		const ids = (svgsIndex as Record<string, readonly string[]>)[slot];
		const id = face[slot].id;
		if (ids && typeof id === "string" && !ids.includes(id)) {
			warnings.push(`${slot}: no such id "${id}"`);
		}
	}

	return { face: face as FaceConfig, warnings };
};

// Where each top-level `"key": {` starts in the text, with the index of the
// brace. Only keys at depth 1 of an object count, found by walking the text
// with a string-aware brace counter, so a face's own inner `"head": {` is never
// mistaken for a player.
const balancedObjectEnd = (text: string, start: number) => {
	let depth = 0;
	let inString = false;
	let escaped = false;
	for (let i = start; i < text.length; i++) {
		const ch = text[i]!;
		if (inString) {
			if (escaped) {
				escaped = false;
			} else if (ch === "\\") {
				escaped = true;
			} else if (ch === '"') {
				inString = false;
			}
			continue;
		}
		if (ch === '"') {
			inString = true;
		} else if (ch === "{") {
			depth += 1;
		} else if (ch === "}") {
			depth -= 1;
			if (depth === 0) {
				return i;
			}
		}
	}
	return -1;
};

// The key in front of an object: `"1234"`, `1234`, `"id 1234"`, `"#3 · id 1234
// · Name"` - models decorate keys, and the id is the only part that matters.
const KEYED_OBJECT = /"?([^\n"{}]*?\b(\d+)[^\n":{}]*?)"?\s*:\s*{/g;

const pidFromKey = (key: string, validPids: Set<number> | undefined) => {
	const idMatch = /\bid\s*[#:]?\s*(\d+)/i.exec(key);
	if (idMatch) {
		return Number(idMatch[1]);
	}
	const numbers = [...key.matchAll(/\d+/g)].map((m) => Number(m[0]));
	if (validPids) {
		const hit = numbers.find((n) => validPids.has(n));
		if (hit !== undefined) {
			return hit;
		}
	}
	return numbers.length === 1 ? numbers[0] : undefined;
};

export type ParsedBatch = {
	faces: Map<number, CheckedFace>;
	// Ids in the reply that aren't in the batch - a typo'd id, or a reply to a
	// different batch. Never applied.
	unknown: number[];
};

// Pull every player's face out of a pasted batch reply. Tolerant by design:
// one object or several code blocks, keys decorated or bare, a face on one line
// or pretty-printed, curly quotes, a reply cut off mid-object (everything
// before the cut is kept). A face that can't be read is simply absent, and the
// page shows that player as still waiting.
export const parseFaceBatch = (
	text: string,
	validPids?: Iterable<number>,
): ParsedBatch => {
	const valid = validPids ? new Set(validPids) : undefined;
	const clean = text.replace(/[‘’“”]/g, '"');
	const faces = new Map<number, CheckedFace>();
	const unknown: number[] = [];

	for (const match of clean.matchAll(KEYED_OBJECT)) {
		const braceAt = match.index + match[0].length - 1;
		const pid = pidFromKey(match[1]!, valid);
		if (pid === undefined) {
			continue;
		}
		const end = balancedObjectEnd(clean, braceAt);
		if (end < 0) {
			continue;
		}
		const parsed = parseFaceJson(clean.slice(braceAt, end + 1));
		if (!looksLikeFace(parsed)) {
			continue;
		}
		if (valid && !valid.has(pid)) {
			if (!unknown.includes(pid)) {
				unknown.push(pid);
			}
			continue;
		}
		faces.set(pid, checkFace(parsed));
	}

	return { faces, unknown };
};
