// Where each faces.js feature sits inside the 400x600 face, as an SVG viewBox
// [x, y, width, height]. Cropping to the feature is what makes a wall of
// thumbnails readable - at full-face size a nose is a dozen pixels and every
// option looks identical.
//
// Measured off rendered faces, not guessed from the artwork's coordinates. If
// facesjs ever redraws something, re-derive with tools/faceFromPhoto/renderSheets.mjs.
export const FACE_CROPS: Record<string, [number, number, number, number]> = {
	head: [0, 40, 400, 420],
	hair: [0, 20, 400, 400],
	hairBg: [0, 20, 400, 400],
	ear: [0, 60, 400, 380],
	eye: [95, 240, 210, 95],
	eyebrow: [95, 212, 210, 85],
	nose: [145, 292, 110, 102],
	mouth: [140, 350, 120, 95],
	facialHair: [70, 280, 260, 180],
	eyeLine: [95, 240, 210, 95],
	smileLine: [110, 290, 180, 140],
	miscLine: [60, 80, 280, 340],
	glasses: [80, 210, 240, 150],
	accessories: [0, 0, 400, 300],
	body: [0, 330, 400, 270],
	jersey: [0, 330, 400, 270],
};

// The whole face, for a slot with no entry above.
export const FULL_FACE: [number, number, number, number] = [0, 0, 400, 600];
