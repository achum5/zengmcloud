import type { AdvancedFrame, AdvancedPlayer } from "./advancedCourtMotion.ts";
import { playerHand } from "./advancedCourtMotion.ts";
import type { CourtTeam } from "./LiveCourt.tsx";
import type { PlayerFace } from "../../util/playerFaces.ts";

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
const contrast = (color: string) => {
	const hex = color.replace("#", "");
	if (!/^[\da-f]{6}$/i.test(hex)) {
		return "#fff";
	}
	const value = Number.parseInt(hex, 16);
	return (value >> 16) * 0.299 +
		((value >> 8) & 255) * 0.587 +
		(value & 255) * 0.114 >
		155
		? "#14202e"
		: "#fff";
};

const drawPlayer = (
	ctx: CanvasRenderingContext2D,
	p: AdvancedPlayer,
	face: PlayerFace | undefined,
	team: CourtTeam | undefined,
	owner: boolean,
) => {
	const primary = team?.colors?.[0] ?? (p.t === 0 ? "#d64d38" : "#2675c9");
	// A light away uniform keeps two teams legible even if their primary colors match.
	const jersey = p.t === 0 ? "#f5f3ed" : primary;
	const trim = p.t === 0 ? primary : (team?.colors?.[1] ?? "#f5f3ed");
	const skin = face?.face?.body?.color ?? "#b9815a";
	const scale = Math.max(0.86, Math.min(1.13, (face?.hgt ?? 78) / 78));
	const girth = Math.max(
		0.92,
		Math.min(1.12, Math.sqrt((face?.weight ?? 215) / 215)),
	);
	const direction = Math.cos(p.angle) >= 0 ? 1 : -1;

	ctx.save();
	ctx.translate(p.x, p.y);
	ellipse(ctx, 0.15, 0.16, 1.13 * scale, 0.42 * scale, "rgba(15,22,31,0.26)");
	if (owner || p.role === "main") {
		ctx.beginPath();
		ctx.ellipse(0, 0.05, 1.55, 0.63, 0, 0, Math.PI * 2);
		ctx.strokeStyle = owner ? "#ffcc62" : "rgba(255,255,255,0.85)";
		ctx.lineWidth = 0.16;
		ctx.stroke();
	}
	ctx.scale(p.displayScale, p.displayScale);
	ctx.translate(0, -p.jump * ELEVATION);
	// Arms keep world-sized endpoints to meet the ball exactly. Body dimensions
	// vary by build, while its root and hand remain the animation anchors.
	const hand = playerHand(p);
	const hx = (hand.x - p.x) / p.displayScale;
	const hy =
		(hand.y - p.y) / p.displayScale -
		(hand.z / p.displayScale - p.jump) * ELEVATION;
	const bob = Math.abs(p.stride) * 0.1;
	const hipY = -1.05 * scale - bob;
	const shoulderY = -2.6 * scale - bob;
	const arm = (side: number) => {
		const shoulderX = side * 0.61 * girth;
		let endX = side * (0.85 + Math.abs(p.stride) * 0.32);
		let endY = -1.45 * scale + p.stride * side * 0.45;
		if (p.shooting) {
			endX = hx + side * 0.12;
			endY = hy;
		} else if (owner && side === direction) {
			endX = hx;
			endY = hy;
		} else if (p.defending) {
			endX = side * 1.5;
			endY = -2.3 * scale + p.stride * 0.15;
		}
		line(
			ctx,
			[
				shoulderX,
				shoulderY,
				(shoulderX + endX) / 2 + side * 0.18,
				(shoulderY + endY) / 2 + 0.3,
				endX,
				endY,
			],
			"rgba(22,23,30,0.65)",
			0.42,
		);
		line(
			ctx,
			[
				shoulderX,
				shoulderY,
				(shoulderX + endX) / 2 + side * 0.18,
				(shoulderY + endY) / 2 + 0.3,
				endX,
				endY,
			],
			skin,
			0.29,
		);
	};
	arm(-direction);
	for (const side of [-1, 1]) {
		const footX = side * 0.4 + p.stride * side * 0.36;
		const footY = Math.max(-0.5, p.stride * side * 0.3);
		line(
			ctx,
			[side * 0.37 * girth, hipY, footX - direction * 0.14, -0.5, footX, footY],
			skin,
			0.38,
		);
		line(
			ctx,
			[footX - direction * 0.12, footY, footX + direction * 0.32, footY],
			"#f9fafc",
			0.3,
		);
		line(
			ctx,
			[
				footX - direction * 0.1,
				footY + 0.12,
				footX + direction * 0.32,
				footY + 0.12,
			],
			"#263344",
			0.1,
		);
	}
	// Shorts, jersey side panels, neckline, and real uniform number.
	ctx.fillStyle = jersey;
	ctx.strokeStyle = "rgba(19,28,42,0.7)";
	ctx.lineWidth = 0.1;
	ctx.beginPath();
	ctx.moveTo(-0.62 * girth, hipY - 0.48);
	ctx.lineTo(0.62 * girth, hipY - 0.48);
	ctx.lineTo(0.69 * girth, hipY + 0.43);
	ctx.lineTo(0.07, hipY + 0.43);
	ctx.lineTo(0, hipY + 0.08);
	ctx.lineTo(-0.07, hipY + 0.43);
	ctx.lineTo(-0.69 * girth, hipY + 0.43);
	ctx.closePath();
	ctx.fill();
	ctx.stroke();
	ctx.beginPath();
	ctx.moveTo(-0.61 * girth, shoulderY);
	ctx.lineTo(0.61 * girth, shoulderY);
	ctx.lineTo(0.51 * girth, hipY - 0.1);
	ctx.lineTo(-0.51 * girth, hipY - 0.1);
	ctx.closePath();
	ctx.fill();
	ctx.stroke();
	line(
		ctx,
		[-0.51 * girth, shoulderY + 0.12, -0.43 * girth, hipY - 0.14],
		trim,
		0.15,
	);
	line(
		ctx,
		[0.51 * girth, shoulderY + 0.12, 0.43 * girth, hipY - 0.14],
		trim,
		0.15,
	);
	line(
		ctx,
		[-0.23, shoulderY + 0.04, 0, shoulderY + 0.2, 0.23, shoulderY + 0.04],
		trim,
		0.14,
	);
	ctx.font = "bold 0.74px system-ui, sans-serif";
	ctx.textAlign = "center";
	ctx.textBaseline = "middle";
	ctx.fillStyle = contrast(jersey);
	ctx.fillText(face?.jerseyNumber ?? "", 0, (shoulderY + hipY) / 2, 0.9);
	arm(direction);
	ellipse(ctx, 0, shoulderY - 0.27, 0.19, 0.31, skin);
	ellipse(ctx, direction * 0.04, shoulderY - 0.62, 0.43, 0.49, "#27313c");
	ellipse(ctx, direction * 0.08, shoulderY - 0.59, 0.38, 0.44, skin);
	ellipse(ctx, -direction * 0.04, shoulderY - 0.82, 0.34, 0.19, "#30251f");
	ellipse(ctx, direction * 0.37, shoulderY - 0.56, 0.12, 0.12, skin);
	ctx.restore();
};

export const drawAdvancedCourt = (
	ctx: CanvasRenderingContext2D,
	frame: AdvancedFrame,
	teams: [CourtTeam | undefined, CourtTeam | undefined],
	appearances: Map<number, PlayerFace>,
	width: number,
	height: number,
	dpr: number,
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
	const sorted = frame.players
		.slice()
		.sort((a, b) => a.y - b.y || a.pid - b.pid);
	for (const p of sorted) {
		drawPlayer(
			ctx,
			p,
			appearances.get(p.pid),
			teams[p.t],
			frame.ball.owner === p.pid,
		);
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
	// Only label the involved players; never cover the floor with ten nameplates.
	const labeled: { x: number; y: number; w: number }[] = [];
	for (const p of sorted.filter((a) => a.role !== "onCourt")) {
		const fontSize = Math.max(1, Math.min(2.5, 10 / scale));
		ctx.font = `600 ${fontSize}px system-ui, sans-serif`;
		const name = p.name.split(" ").slice(1).join(" ") || p.name;
		const label = `${p.role === "in" ? "↑ " : p.role === "out" ? "↓ " : ""}${name}`;
		const w = Math.min(24, ctx.measureText(label).width + 1.2);
		const x = Math.max(w / 2 - 4, Math.min(98 - w / 2, p.x));
		let y = p.y + 1.2;
		for (const other of labeled) {
			if (
				Math.abs(x - other.x) < (w + other.w) / 2 &&
				Math.abs(y - other.y) < fontSize + 0.5
			) {
				y = other.y + fontSize + 0.7;
			}
		}
		y = Math.min(51, y);
		labeled.push({ x, y, w });
		ctx.fillStyle = "rgba(15,25,39,0.88)";
		ctx.beginPath();
		ctx.roundRect(x - w / 2, y - fontSize * 0.6, w, fontSize + 0.5, 0.25);
		ctx.fill();
		ctx.fillStyle = "#fff";
		ctx.textAlign = "center";
		ctx.textBaseline = "middle";
		ctx.fillText(label, x, y + 0.1, w - 0.7);
	}
};
