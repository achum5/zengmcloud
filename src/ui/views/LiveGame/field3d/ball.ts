import { project, type Camera } from "../court3d/camera.ts";
import type { Pt3 } from "./geometry.ts";
import { BALL_BROWN, BALL_LEN, BALL_R } from "./sculpt.ts";

// THE BALL ON ITS OWN: spotted on the turf, its points up and down the field,
// or stood up on end - on its tee for a kickoff, under the holder's finger
// for a field goal. (In a man's hands, it is drawn with him - see sculpt.ts.)

const TEE_ORANGE = "#f26b1d";
const TEE_H = 0.22;

export const drawLooseBall = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: Pt3,
	stood: "tee" | "hold" | undefined,
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
	// On end (on its tee, a little up off the turf); spotted, on its side.
	const tee = stood === "tee";
	const rx = Math.max(1, (stood ? BALL_R : BALL_LEN / 2) * k);
	const ry = Math.max(0.8, (stood ? BALL_LEN / 2 : BALL_R * 0.8) * k);
	const lift = tee ? TEE_H * k : 0;
	const cx = base.x;
	const cy = base.y - lift - ry;
	ctx.save();
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
		// Its shadow.
		ctx.fillStyle = "rgba(0, 0, 0, 0.28)";
		ctx.beginPath();
		ctx.ellipse(
			cx,
			base.y,
			rx * 1.05,
			Math.max(0.6, ry * 0.4),
			0,
			0,
			Math.PI * 2,
		);
		ctx.fill();
	}
	ctx.fillStyle = BALL_BROWN;
	ctx.strokeStyle = "rgba(20, 8, 2, 0.85)";
	ctx.lineWidth = Math.max(0.6, k * 0.06);
	ctx.beginPath();
	ctx.ellipse(cx, cy, rx, ry, 0, 0, Math.PI * 2);
	ctx.fill();
	ctx.stroke();
	// The light on it, and its laces.
	ctx.fillStyle = "rgba(255, 220, 180, 0.28)";
	ctx.beginPath();
	ctx.ellipse(
		cx - rx * 0.2,
		cy - ry * 0.35,
		rx * 0.5,
		ry * 0.32,
		0,
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
		ctx.moveTo(cx - rx * 0.35, cy - ry * 0.3);
		ctx.lineTo(cx + rx * 0.35, cy - ry * 0.3);
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
	ctx.translate(c.x, c.y);
	ctx.rotate(Math.atan2(dy, dx));
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
