import { fieldLineBoxes } from "./LiveGame/field3d/fieldLines.ts";
import {
	BORDER,
	FIELD_L,
	FIELD_W,
	GOAL_L,
	GOAL_R,
} from "./LiveGame/field3d/geometry.ts";

// THE TEMPLATE A WHOLE-FIELD PICTURE IS PAINTED ON (see StadiumLook's
// fieldURL): the field seen from above, as the 3D game lays it down - end
// line to end line, sideline to sideline, and the six-foot white border round
// it - at 8 px a foot, its lines where the game's are.

const FT = 8;

export const FIELD_TEMPLATE_W = (FIELD_L + 2 * BORDER) * FT;
export const FIELD_TEMPLATE_H = (FIELD_W + 2 * BORDER) * FT;

export const fieldTemplate = (): HTMLCanvasElement => {
	const cv = document.createElement("canvas");
	cv.width = FIELD_TEMPLATE_W;
	cv.height = FIELD_TEMPLATE_H;
	const g = cv.getContext("2d")!;
	// Field feet to pixels.
	g.translate(BORDER * FT, BORDER * FT);
	g.scale(FT, FT);
	g.fillStyle = "#3f7d34";
	g.fillRect(0, 0, FIELD_L, FIELD_W);
	g.fillStyle = "#2f5f27";
	g.fillRect(0, 0, GOAL_L, FIELD_W);
	g.fillRect(GOAL_R, 0, FIELD_L - GOAL_R, FIELD_W);
	g.fillStyle = "#ffffff";
	for (const b of fieldLineBoxes()) {
		g.fillRect(b.x0, b.y0, b.x1 - b.x0, b.y1 - b.y0);
	}
	return cv;
};
