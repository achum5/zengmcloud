import { project, type Camera } from "../court3d/camera.ts";
import { FIGURE_SCALE } from "./body.ts";
import type { Pt3 } from "./geometry.ts";
import { BALL_BROWN, BALL_LEN, BALL_R } from "./sculpt.ts";
import type { Cast } from "./sprite.ts";

// THE BALL ON ITS OWN: spotted on the turf, its points up and down the field,
// or stood up on end - on its tee for a kickoff, under the holder's finger
// for a field goal. (In a man's hands, it is drawn with him - see sculpt.ts.)

const TEE_ORANGE = "#f26b1d";
const TEE_H = 0.22;

// Its shadows laid across the turf by the same lights (or sun) as the men's
// (see Cast): its own shape in black, sheared down from where it is drawn
// as theirs are - fading as it goes up, gone by CAST_FADE feet.
const CAST_FADE = 14;
const castBall = (
	ctx: CanvasRenderingContext2D,
	casts: Cast[] | undefined,
	z: number,
	floor: number,
	path: () => void,
) => {
	const fade = 1 - z / FIGURE_SCALE / CAST_FADE;
	if (!casts || fade <= 0) {
		return;
	}
	const m = ctx.getTransform();
	const alpha0 = ctx.globalAlpha;
	ctx.fillStyle = "#000";
	for (const { lean, run, alpha } of casts) {
		ctx.globalAlpha = alpha0 * alpha * fade;
		ctx.transform(1, 0, -lean, -run, lean * floor, floor * (1 + run));
		ctx.beginPath();
		path();
		ctx.fill();
		ctx.setTransform(m);
	}
	ctx.globalAlpha = alpha0;
};

// How high the middle of it is drawn, on the turf (true feet - drawn up
// by FIGURE_SCALE): for a ball handed down onto it to end up just there.
export const restingZ = (stood: "tee" | "hold" | undefined): number =>
	(stood ? BALL_LEN / 2 + (stood === "tee" ? TEE_H : 0) : BALL_R * 0.8) /
	FIGURE_SCALE;

// The tee, the ball kicked off it: on its side on the turf (or in the air,
// knocked off its spot), a squat orange cup.
export const drawTee = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: Pt3,
) => {
	const base = project(cam, at);
	const ground = project(cam, { ...at, z: 0 });
	const k = base.k;
	if (base.x < -20 || base.x > cam.viewW + 20) {
		return;
	}
	ctx.save();
	ctx.fillStyle = "rgba(0, 0, 0, 0.25)";
	ctx.beginPath();
	ctx.ellipse(
		ground.x,
		ground.y,
		Math.max(1, 0.3 * k),
		Math.max(0.5, 0.1 * k),
		0,
		0,
		Math.PI * 2,
	);
	ctx.fill();
	ctx.fillStyle = TEE_ORANGE;
	ctx.beginPath();
	ctx.ellipse(
		base.x,
		base.y - Math.max(0.5, 0.1 * k),
		Math.max(1, 0.3 * k),
		Math.max(0.7, 0.14 * k),
		0,
		0,
		Math.PI * 2,
	);
	ctx.fill();
	ctx.restore();
};

// Spotted, put down where it was: its points up and down the field.
const ALONG: Pt3 = { x: 1, y: 0, z: 0 };

export const drawLooseBall = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: Pt3,
	stood: "tee" | "hold" | undefined,
	casts?: Cast[],
	// On its side, the way it lies (spotted - see ALONG - if not given).
	lies?: Pt3,
) => {
	const base = project(cam, { ...at, z: 0 });
	const k = base.k;
	if (
		base.x < -20 ||
		base.x > cam.viewW + 20 ||
		base.y < -20 ||
		base.y > cam.viewH + 20
	) {
		return;
	}
	// On end (on its tee, a little up off the turf); on its side, its points
	// the way it lies as the camera has them - end on to it from behind.
	const tee = stood === "tee";
	const lift = tee ? TEE_H * k : 0;
	let rx = Math.max(1, BALL_R * k);
	let ry = Math.max(0.8, (BALL_LEN / 2) * k);
	let turn = 0;
	let cx = base.x;
	let cy = base.y - lift - ry;
	// (Its shadow on the turf, the two ways across it - see below.)
	let along = { x: rx * 1.05, y: 0 };
	let aside = { x: 0, y: Math.max(0.6, ry * 0.4) };
	if (!stood) {
		const w = lies ?? ALONG;
		const flat = Math.hypot(w.x, w.y) || 1;
		const wx = w.x / flat;
		const wy = w.y / flat;
		const half = BALL_LEN / 2;
		// (Its middle as high off the turf as it lies - see restingZ.)
		const up = BALL_R * 0.8;
		const mid = project(cam, { x: at.x, y: at.y, z: up });
		const tip = project(cam, {
			x: at.x + wx * half,
			y: at.y + wy * half,
			z: up,
		});
		const dx = tip.x - mid.x;
		const dy = tip.y - mid.y;
		turn = Math.atan2(dy, dx);
		// (Seen across its middle, as wide as it is; from over it, a
		// little less.)
		ry = Math.max(0.8, BALL_R * (0.8 + 0.2 * Math.abs(Math.sin(turn))) * k);
		rx = Math.max(1, ry, Math.hypot(dx, dy));
		cx = mid.x;
		cy = mid.y;
		const end = project(cam, {
			x: at.x + wx * half,
			y: at.y + wy * half,
			z: 0,
		});
		const side = project(cam, {
			x: at.x - wy * BALL_R,
			y: at.y + wx * BALL_R,
			z: 0,
		});
		along = { x: (end.x - base.x) * 1.05, y: (end.y - base.y) * 1.05 };
		aside = { x: (side.x - base.x) * 0.8, y: (side.y - base.y) * 0.8 };
		const thin = Math.hypot(aside.x, aside.y);
		if (thin < 0.6) {
			aside =
				thin > 0
					? { x: (aside.x / thin) * 0.6, y: (aside.y / thin) * 0.6 }
					: { x: 0, y: 0.6 };
		}
	}
	const high = Math.hypot(rx * Math.sin(turn), ry * Math.cos(turn));
	const wide = Math.hypot(rx * Math.cos(turn), ry * Math.sin(turn));
	ctx.save();
	castBall(ctx, casts, at.z, base.y, () => {
		ctx.ellipse(cx, cy, rx, ry, turn, 0, Math.PI * 2);
	});
	if (tee) {
		ctx.fillStyle = TEE_ORANGE;
		ctx.beginPath();
		ctx.ellipse(
			cx,
			base.y,
			Math.max(1, 0.36 * k),
			Math.max(0.6, 0.12 * k),
			0,
			0,
			Math.PI * 2,
		);
		ctx.fill();
		ctx.fillRect(
			cx - Math.max(0.5, 0.2 * k),
			base.y - lift,
			Math.max(1, 0.4 * k),
			lift,
		);
	} else {
		// Its shadow, laid flat on the turf under it the way it lies.
		ctx.fillStyle = "rgba(0, 0, 0, 0.28)";
		ctx.beginPath();
		const m = ctx.getTransform();
		ctx.transform(along.x, along.y, aside.x, aside.y, base.x, base.y);
		ctx.arc(0, 0, 1, 0, Math.PI * 2);
		ctx.setTransform(m);
		ctx.fill();
	}
	ctx.fillStyle = BALL_BROWN;
	ctx.strokeStyle = "rgba(20, 8, 2, 0.85)";
	ctx.lineWidth = Math.max(0.6, k * 0.06);
	ctx.beginPath();
	ctx.ellipse(cx, cy, rx, ry, turn, 0, Math.PI * 2);
	ctx.fill();
	ctx.stroke();
	// The light on it, and its laces.
	ctx.fillStyle = "rgba(255, 220, 180, 0.28)";
	ctx.beginPath();
	ctx.ellipse(
		cx - wide * 0.2,
		cy - high * 0.35,
		rx * 0.5,
		ry * 0.32,
		turn,
		0,
		Math.PI * 2,
	);
	ctx.fill();
	ctx.strokeStyle = "#f4efe6";
	ctx.lineWidth = Math.max(0.5, k * 0.05);
	ctx.beginPath();
	if (stood) {
		ctx.moveTo(cx, cy - ry * 0.45);
		ctx.lineTo(cx, cy + ry * 0.35);
	} else {
		// (Along it, on top.)
		const ux = Math.cos(turn) * rx * 0.35;
		const uy = Math.sin(turn) * rx * 0.35;
		ctx.moveTo(cx - ux, cy - ry * 0.3 - uy);
		ctx.lineTo(cx + ux, cy - ry * 0.3 + uy);
	}
	ctx.stroke();
	ctx.restore();
};

// The ball in the air: its shadow on the turf under it, and the ball itself
// end-on or side-on to the camera as it points - a long ellipse along the
// way its points go on the screen.
export const drawFlyingBall = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: Pt3,
	axis: Pt3,
	casts?: Cast[],
) => {
	const c = project(cam, at);
	if (c.x < -20 || c.x > cam.viewW + 20 || c.y < -40 || c.y > cam.viewH + 20) {
		return;
	}
	const k = c.k;
	const len = Math.hypot(axis.x, axis.y, axis.z) || 1;
	const half = BALL_LEN / 2;
	const tip = project(cam, {
		x: at.x + (axis.x / len) * half,
		y: at.y + (axis.y / len) * half,
		z: at.z + (axis.z / len) * half,
	});
	const dx = tip.x - c.x;
	const dy = tip.y - c.y;
	const rx = Math.max(Math.max(1, BALL_R * k), Math.hypot(dx, dy));
	const ry = Math.max(0.8, BALL_R * k);
	const ground = project(cam, { x: at.x, y: at.y, z: 0 });
	ctx.save();
	ctx.fillStyle = `rgba(0, 0, 0, ${Math.max(0.08, 0.28 - at.z * 0.006)})`;
	ctx.beginPath();
	ctx.ellipse(
		ground.x,
		ground.y,
		Math.max(1, half * ground.k),
		Math.max(0.6, BALL_R * 0.4 * ground.k),
		0,
		0,
		Math.PI * 2,
	);
	ctx.fill();
	const turn = Math.atan2(dy, dx);
	castBall(ctx, casts, at.z, ground.y, () => {
		ctx.ellipse(c.x, c.y, rx, ry, turn, 0, Math.PI * 2);
	});
	ctx.translate(c.x, c.y);
	ctx.rotate(turn);
	ctx.fillStyle = BALL_BROWN;
	ctx.strokeStyle = "rgba(20, 8, 2, 0.85)";
	ctx.lineWidth = Math.max(0.6, k * 0.06);
	ctx.beginPath();
	ctx.ellipse(0, 0, rx, ry, 0, 0, Math.PI * 2);
	ctx.fill();
	ctx.stroke();
	ctx.fillStyle = "rgba(255, 220, 180, 0.28)";
	ctx.beginPath();
	ctx.ellipse(-rx * 0.1, -ry * 0.35, rx * 0.5, ry * 0.32, 0, 0, Math.PI * 2);
	ctx.fill();
	ctx.restore();
};
