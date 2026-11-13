// A TEAM'S CAP ON HIS HEAD - a draft pick's (see draftWalk.ts): the crown in
// the team's main color, the panels at its sides in its second, the button
// on top and the piping along the peak in its third.
//
// Facing the camera it is his faces.js face's own cap (the game's own art:
// faces.js draws his hair under it), with the team's logo on the front of
// it (see capHead.ts); side on and from behind it is drawn here, over his
// head as figure.ts has drawn it, to match.

export type Cap = {
	crown: string;
	panel: string;
	trim: string;
};

export const capOf = (colors: [string, string, string] | undefined): Cap => ({
	crown: colors?.[0] ?? "#1d3461",
	panel: colors?.[1] ?? "#f2f2ee",
	trim: colors?.[2] ?? "#f2c14e",
});

type P2 = { x: number; y: number };
const INK = "#17120f";

// Darker (f < 0) or lighter (f > 0).
const tone = (c: string, f: number): string => {
	const m = /^#?([\da-f]{6})$/i.exec(c.trim());
	if (!m) {
		return c;
	}
	const n = Number.parseInt(m[1]!, 16);
	const mix = (v: number) =>
		Math.round(f < 0 ? v * (1 + f) : v + (255 - v) * f)
			.toString(16)
			.padStart(2, "0");
	return `#${mix((n >> 16) & 255)}${mix((n >> 8) & 255)}${mix(n & 255)}`;
};

const through = (pts: P2[]): Path2D => {
	const p = new Path2D();
	const n = pts.length;
	const mid = (i: number): P2 => ({
		x: (pts[i % n]!.x + pts[(i + 1) % n]!.x) / 2,
		y: (pts[i % n]!.y + pts[(i + 1) % n]!.y) / 2,
	});
	const m0 = mid(0);
	p.moveTo(m0.x, m0.y);
	for (let i = 1; i <= n; i++) {
		const c = pts[i % n]!;
		const m = mid(i);
		p.quadraticCurveTo(c.x, c.y, m.x, m.y);
	}
	p.closePath();
	return p;
};

// SIDE ON: the crown over his skull from his brow round to the back of his
// head, low over his ear; the peak out in front of his brow, a little down.
// `c` is the middle of his face and `r` its radius, as profileHead has
// them; `turn` the way his nose points.
export const capSide = (
	ctx: CanvasRenderingContext2D,
	c: P2,
	r: number,
	turn: 1 | -1,
	cap: Cap,
	ink: number,
) => {
	const P = (u: number, v: number): P2 => ({
		x: c.x + turn * u * r,
		y: c.y + v * r,
	});
	const crown = through([
		P(0.94, -0.6),
		P(0.9, -0.98),
		P(0.5, -1.3),
		P(-0.14, -1.36),
		P(-0.78, -1.14),
		P(-1.12, -0.62),
		P(-1.08, -0.2),
		P(-0.6, -0.32),
		P(0.0, -0.48),
		P(0.5, -0.56),
	]);
	const peak = through([
		P(0.86, -0.66),
		P(1.3, -0.62),
		P(1.72, -0.46),
		P(1.74, -0.36),
		P(1.3, -0.44),
		P(0.88, -0.5),
	]);
	if (ink > 0) {
		ctx.strokeStyle = INK;
		ctx.lineJoin = "round";
		ctx.lineWidth = ink * 2;
		ctx.stroke(crown);
		ctx.stroke(peak);
	}
	ctx.fillStyle = tone(cap.crown, -0.22);
	ctx.fill(peak);
	ctx.fillStyle = cap.crown;
	ctx.fill(crown);
	ctx.save();
	ctx.clip(crown);
	// The panel at its side, back from the seam over his ear.
	ctx.fillStyle = cap.panel;
	ctx.fill(
		through([
			P(-0.2, -1.5),
			P(-0.34, -0.9),
			P(-0.3, -0.3),
			P(-1.3, -0.1),
			P(-1.3, -1.5),
		]),
	);
	// Round, not flat: lit from above and in front, in shade low at the back.
	const lit = ctx.createLinearGradient(
		P(0.6, -1.3).x,
		P(0.6, -1.3).y,
		P(-0.9, -0.2).x,
		P(-0.9, -0.2).y,
	);
	lit.addColorStop(0, "rgba(255, 255, 255, 0.16)");
	lit.addColorStop(0.5, "rgba(255, 255, 255, 0)");
	lit.addColorStop(1, "rgba(0, 0, 0, 0.26)");
	ctx.fillStyle = lit;
	ctx.fillRect(c.x - r * 2, c.y - r * 2, r * 4, r * 4);
	ctx.restore();
	// The piping where the peak meets the crown, and the button on top.
	ctx.strokeStyle = cap.trim;
	ctx.lineWidth = Math.max(1, r * 0.07);
	ctx.lineCap = "round";
	ctx.beginPath();
	const a = P(0.9, -0.6);
	const b = P(1.3, -0.56);
	const d = P(1.7, -0.42);
	ctx.moveTo(a.x, a.y);
	ctx.quadraticCurveTo(b.x, b.y, d.x, d.y);
	ctx.stroke();
	ctx.lineCap = "butt";
	const top = P(-0.14, -1.38);
	ctx.fillStyle = cap.trim;
	ctx.beginPath();
	ctx.ellipse(top.x, top.y, r * 0.13, r * 0.07, 0, 0, Math.PI * 2);
	ctx.fill();
};

// FROM BEHIND: the crown over the back of his skull down to the band, the
// gap above its strap, the seams up to the button. `x` is the middle of his
// head across and `y` the middle of his face down the screen, as the back
// of his head is drawn; `side` (0 to 1) how side on he is, and `turn` which
// way his nose points - the gap is round toward the back of his head.
export const capBack = (
	ctx: CanvasRenderingContext2D,
	x: number,
	y: number,
	r: number,
	cap: Cap,
	ink: number,
	side = 0,
	turn = 1,
) => {
	const band = y - r * 0.22;
	const top = y - r * 1.36;
	const crown = new Path2D();
	crown.moveTo(x - r * 1.06, band);
	crown.bezierCurveTo(x - r * 1.14, y - r * 1.0, x - r * 0.62, top, x, top);
	crown.bezierCurveTo(
		x + r * 0.62,
		top,
		x + r * 1.14,
		y - r * 1.0,
		x + r * 1.06,
		band,
	);
	crown.quadraticCurveTo(x, band + r * 0.16, x - r * 1.06, band);
	crown.closePath();
	if (ink > 0) {
		ctx.strokeStyle = INK;
		ctx.lineJoin = "round";
		ctx.lineWidth = ink * 2;
		ctx.stroke(crown);
	}
	ctx.fillStyle = cap.panel;
	ctx.fill(crown);
	ctx.save();
	ctx.clip(crown);
	// (The front of it, in the crown's color, shows past the panels the more
	// side on he is.)
	if (side > 0.05) {
		ctx.fillStyle = cap.crown;
		const w = r * 2.4 * Math.min(1, side);
		ctx.fillRect(turn > 0 ? x + r * 1.2 - w : x - r * 1.2, top - r, w, r * 3);
	}
	// The gap over the strap, his hair - or his head - showing through it,
	// round toward the back of his head.
	const gx = x - turn * side * r * 0.8;
	ctx.fillStyle = "rgba(0, 0, 0, 0.55)";
	ctx.beginPath();
	ctx.ellipse(gx, band + r * 0.06, r * 0.34, r * 0.3, 0, Math.PI, Math.PI * 2);
	ctx.fill();
	ctx.strokeStyle = tone(cap.panel, -0.3);
	ctx.lineWidth = Math.max(1, r * 0.035);
	ctx.beginPath();
	for (const k of [-0.5, 0, 0.5]) {
		ctx.moveTo(x, top);
		ctx.quadraticCurveTo(
			gx + k * r * 1.5,
			y - r * 0.9,
			gx + k * r * 1.9,
			band + r * 0.1,
		);
	}
	ctx.stroke();
	const lit = ctx.createRadialGradient(
		x - r * 0.25,
		y - r * 1.1,
		r * 0.08,
		x,
		y - r * 0.7,
		r * 1.3,
	);
	lit.addColorStop(0, "rgba(255, 255, 255, 0.18)");
	lit.addColorStop(0.5, "rgba(255, 255, 255, 0)");
	lit.addColorStop(1, "rgba(0, 0, 0, 0.3)");
	ctx.fillStyle = lit;
	ctx.fillRect(x - r * 2, y - r * 2.4, r * 4, r * 4);
	ctx.restore();
	ctx.fillStyle = cap.trim;
	ctx.beginPath();
	ctx.ellipse(x, top + r * 0.02, r * 0.13, r * 0.07, 0, 0, Math.PI * 2);
	ctx.fill();
};
