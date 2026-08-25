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
	// Was [140, 350, 120, 95], which cut the bottom off every open mouth -
	// `smile`, `smile3`, `angry` and `mouth` all ran past the foot of the window,
	// so the widest grin in the set and a neutral bar looked equally like a
	// stripe. Re-derived by rendering the whole slot; this clears the lowest of
	// them with room to spare.
	mouth: [120, 335, 160, 140],
	// Sits low on purpose. The obvious guess - centre it on the mouth - starts
	// the window mid-eye and stops at the chin, so a full beard is cut off at
	// exactly the part that distinguishes it from a goatee, and half the
	// thumbnail is spent on eyes that never change. This runs from the top of
	// the sideburns down past the jaw, and out wide enough for mutton chops.
	facialHair: [60, 320, 280, 200],
	// Wider and taller than the eye window it used to share: eyeLine's marks are
	// crow's feet, under-eye lines and cheek lines, and half of them fell
	// outside a window sized for the eye itself.
	eyeLine: [60, 180, 280, 220],
	// The folds sit LOWER than the old [110, 290, 180, 140] window reached -
	// all four options were clipped to a few pixels at the bottom edge, which
	// is also why nobody noticed that line4 curves the opposite way to the rest.
	smileLine: [100, 300, 200, 200],
	// miscLine covers the forehead AND the chin, and the old 340-tall window
	// stopped at the mouth: chin1 and chin2 rendered as blank thumbnails
	// indistinguishable from `none`.
	miscLine: [60, 90, 280, 430],
	glasses: [80, 210, 240, 150],
	// Deep enough for eye-black, which sits under the eyes and was entirely
	// outside a window that stopped at the brow line.
	accessories: [0, 0, 400, 380],
	body: [0, 330, 400, 270],
	jersey: [0, 330, 400, 270],
};

// The whole face, for a slot with no entry above.
export const FULL_FACE: [number, number, number, number] = [0, 0, 400, 600];
