import type { FaceConfig } from "facesjs";
import type { Cap } from "./cap.ts";
import { loadHead, type HeadSprite } from "./faces.ts";

// HIS FACE WITH THE CAP ON, for the court to stamp on his shoulders (see
// faces.ts): drawn by faces.js in the cap's colors, and the team's logo
// (`crest`, if it has been read) laid on the front of the crown.
//
// faces.js draws in a 400 x 600 box, the crown's front about here in it.
const FRONT = { x: 200, y: 146, w: 86, h: 62 };
export const cappedHead = async (
	face: FaceConfig | undefined,
	cap: Cap,
	crest: HTMLCanvasElement | undefined,
): Promise<{ sprite?: HeadSprite; skin?: string }> => {
	if (!face) {
		return {};
	}
	const capped = structuredClone(face);
	capped.accessories = { id: "hat3" };
	// (faces.js puts its second color on the crown, its first on the panels.)
	const head = await loadHead(capped, [cap.panel, cap.crown, cap.trim], true);
	const sprite = head.sprite;
	if (sprite && crest) {
		const g = sprite.img.getContext("2d");
		if (g) {
			// The sprite is the faces.js box cropped and scaled: from where the
			// middle of his face is in each.
			const k = sprite.h / 400;
			const fit = Math.min(FRONT.w / crest.width, FRONT.h / crest.height) * k;
			const w = crest.width * fit;
			const h = crest.height * fit;
			g.drawImage(
				crest,
				sprite.cx + (FRONT.x - 200) * k - w / 2,
				sprite.cy + (FRONT.y - 300) * k - h / 2,
				w,
				h,
			);
		}
	}
	return head;
};
