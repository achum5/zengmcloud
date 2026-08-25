import { assert, describe, test } from "vitest";
import { svgsIndex } from "facesjs";
import { FACE_FROM_PHOTO_PROMPT } from "./faceFromPhotoPrompt.ts";

// THE PROMPT HANDS A MODEL A MENU, and a menu that has drifted from the kitchen
// is worse than no menu: an id faces.js does not have renders as a BLANK SLOT,
// so a face comes back missing its nose and nothing anywhere says why.
//
// It had drifted already. `blush` has been a miscLine option the whole time and
// was never listed, so no photo could ever produce it and nobody could tell
// from the prompt that it existed. That is the cheap failure; the expensive one
// is the other direction, where a facesjs upgrade retires an id the prompt goes
// on recommending.
//
// So the lists are checked against facesjs itself rather than maintained by
// hand. Everything in the prompt must exist, and everything that exists must be
// in the prompt - see the exemptions below for the two cases where it must not.
const section = FACE_FROM_PHOTO_PROMPT.split(
	"## Allowed `id` values",
)[1]!.split("## What the shapes")[0]!;

// "- **slot** (an aside): a, b, c" possibly wrapped over several lines.
const listed = (slot: string): string[] => {
	const re = new RegExp(
		String.raw`- \*\*${slot}\*\*[^:]*:([\s\S]*?)(?=\n- \*\*|\n\n|$)`,
	);
	const m = re.exec(section);
	assert.isNotNull(m, `${slot} is not listed in the prompt at all`);
	return m![1]!
		.replaceAll("\n", " ")
		.split(",")
		.map((s) => s.trim())
		.filter(Boolean);
};

// Every slot the prompt is responsible for. `body` and `jersey` are in the
// prompt but ZenGM overwrites both, so they are checked like the rest rather
// than trusted.
const SLOTS = [
	"head",
	"hair",
	"hairBg",
	"facialHair",
	"eye",
	"eyebrow",
	"nose",
	"mouth",
	"ear",
	"body",
	"jersey",
	"eyeLine",
	"smileLine",
	"miscLine",
	"glasses",
	"accessories",
] as const;

describe("the prompt's id lists match the faces.js build we ship", () => {
	for (const slot of SLOTS) {
		test(slot, () => {
			// female* ids are deliberately left out of the lists - the prompt names
			// them in prose instead, with the rule about when they apply - so they
			// are not part of this comparison.
			const real: string[] = [...svgsIndex[slot]];
			const usable = real.filter((id) => !id.startsWith("female"));
			const inPrompt = listed(slot);

			const missing = usable.filter((id) => !inPrompt.includes(id));
			assert.deepEqual(
				missing,
				[],
				`faces.js has ${slot} ids the prompt never offers`,
			);

			const unreal = inPrompt.filter((id) => !usable.includes(id));
			assert.deepEqual(
				unreal,
				[],
				`the prompt offers ${slot} ids faces.js does not have - these render as a blank slot`,
			);
		});
	}
});

// The shape guide is the half of the prompt that cannot be checked against
// anything: it says what the drawings LOOK like, which only a person looking at
// a render can know (tools/faceFromPhoto/renderSheets.mjs). What can be checked
// is that it still talks about every group it claims to cover, so a rewrite
// cannot quietly drop one.
describe("the shape guide covers every slot that has one", () => {
	const guide = FACE_FROM_PHOTO_PROMPT.split("## What the shapes")[1]!;
	for (const slot of [
		"head",
		"eye",
		"eyebrow",
		"nose",
		"mouth",
		"hair",
		"facialHair",
		"eyeLine",
		"smileLine",
		"miscLine",
		"glasses",
		"accessories",
	]) {
		test(slot, () => {
			assert.include(guide, `**${slot}**`);
		});
	}
});
