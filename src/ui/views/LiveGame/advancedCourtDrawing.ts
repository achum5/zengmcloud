import type { AdvancedBall, AdvancedFrame } from "./advancedCourtMotion.ts";

const ELEVATION = 0.55;
const line = (
	ctx: CanvasRenderingContext2D,
	points: number[],
	color: string,
	width: number,
) => {
	ctx.beginPath();
	ctx.moveTo(points[0]!, points[1]!);
	for (let i = 2; i < points.length; i += 2) {
		ctx.lineTo(points[i]!, points[i + 1]!);
	}
	ctx.strokeStyle = color;
	ctx.lineWidth = width;
	ctx.lineCap = "round";
	ctx.lineJoin = "round";
	ctx.stroke();
};
const ellipse = (
	ctx: CanvasRenderingContext2D,
	x: number,
	y: number,
	rx: number,
	ry: number,
	color: string,
) => {
	ctx.beginPath();
	ctx.ellipse(x, y, rx, ry, 0, 0, Math.PI * 2);
	ctx.fillStyle = color;
	ctx.fill();
};
export const drawAdvancedCourt = (
	ctx: CanvasRenderingContext2D,
	frame: AdvancedFrame,
	width: number,
	height: number,
	dpr: number,
	trail: AdvancedBall[] = [],
) => {
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.clearRect(0, 0, width * dpr, height * dpr);
	const scale = width / 104;
	ctx.setTransform(
		scale * dpr,
		0,
		0,
		scale * dpr,
		5 * scale * dpr,
		2.5 * scale * dpr,
	);
	// Raised rims share the shot's projected height. The floor's basket marks
	// remain beneath them, like the ground shadow of the hoop.
	for (const rimX of [5.25, 88.75]) {
		const dir = rimX < 47 ? -1 : 1;
		const y = 25 - 6 * ELEVATION;
		line(
			ctx,
			[rimX + dir * 3, 25, rimX + dir * 3, y, rimX + dir * 0.9, y],
			"#89919a",
			0.3,
		);
		line(
			ctx,
			[rimX + dir * 0.95, y - 2.2, rimX + dir * 0.95, y + 2.2],
			"rgba(226,237,246,0.85)",
			0.42,
		);
		for (const offset of [-0.55, 0, 0.55]) {
			line(
				ctx,
				[rimX + offset, y, rimX + offset * 0.5, y + 1.2],
				"rgba(255,255,255,0.7)",
				0.07,
			);
		}
		ctx.beginPath();
		ctx.ellipse(rimX, y, 0.85, 0.65, 0, 0, Math.PI * 2);
		ctx.strokeStyle = "#e87930";
		ctx.lineWidth = 0.17;
		ctx.stroke();
	}
	// Keep the same faces, full names, and numbered chips as Basic. This layer
	// adds only the ball and possession cues above those shared markers.
	const owner = frame.players.find((p) => p.pid === frame.ball.owner);
	if (owner && frame.ball.visible) {
		const radius =
			owner.role === "onCourt"
				? Math.max(9 / scale, 1.5)
				: Math.max(18 / scale, 3.3);
		ctx.beginPath();
		ctx.arc(owner.x, owner.y, radius, 0, Math.PI * 2);
		ctx.strokeStyle = "rgba(20,25,32,0.8)";
		ctx.lineWidth = 0.65;
		ctx.stroke();
		ctx.strokeStyle = "#ffd45c";
		ctx.lineWidth = 0.3;
		ctx.stroke();
	}
	const { ball, impact } = frame;
	if (impact && impact.progress < 1) {
		const opacity = (1 - impact.progress) * 0.8;
		ctx.beginPath();
		ctx.ellipse(
			impact.x,
			impact.y,
			1 + impact.progress * 1.8,
			0.7 + impact.progress,
			0,
			0,
			Math.PI * 2,
		);
		ctx.strokeStyle = impact.made
			? `rgba(255,231,169,${opacity})`
			: `rgba(255,255,255,${opacity * 0.6})`;
		ctx.lineWidth = 0.22;
		ctx.stroke();
	}
	if (ball.visible && ball.owner === undefined) {
		const points = [...trail, ball].filter(
			(point) => point.visible && point.owner === undefined,
		);
		for (let i = 1; i < points.length; i++) {
			const from = points[i - 1]!;
			const to = points[i]!;
			line(
				ctx,
				[from.x, from.y - from.z * ELEVATION, to.x, to.y - to.z * ELEVATION],
				`rgba(255,210,100,${0.15 + (i / points.length) * 0.65})`,
				Math.max(0.22, 1.5 / scale),
			);
		}
	}
	if (ball.visible) {
		ellipse(
			ctx,
			ball.x + 0.2,
			ball.y + 0.2,
			0.55 + ball.z * 0.025,
			0.24,
			"rgba(20,25,32,0.28)",
		);
		ctx.save();
		ctx.translate(ball.x, ball.y - ball.z * ELEVATION);
		ctx.rotate((ball.x + ball.y + ball.z) * 1.4);
		ellipse(ctx, 0, 0, 0.55, 0.55, "#342116");
		ellipse(ctx, 0, 0, 0.47, 0.47, "#f29439");
		line(ctx, [-0.47, 0, 0.47, 0], "#824019", 0.07);
		line(ctx, [0, -0.47, 0, 0.47], "#824019", 0.07);
		ctx.beginPath();
		ctx.ellipse(0, 0, 0.21, 0.47, 0, 0, Math.PI * 2);
		ctx.strokeStyle = "#824019";
		ctx.lineWidth = 0.06;
		ctx.stroke();
		ellipse(ctx, -0.16, -0.17, 0.13, 0.11, "rgba(255,255,255,0.45)");
		ctx.restore();
	}
};
