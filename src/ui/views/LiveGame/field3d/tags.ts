import { project, type Camera } from "../court3d/camera.ts";
import { drawPixelText, pixelTextWidth } from "../court3d/pixelFont.ts";
import { FIGURE_SCALE } from "./body.ts";
import type { Play } from "./director.ts";
import type { Role } from "./formations.ts";
import type { FieldLook } from "./kit.ts";
import type { FieldPlayerState } from "./sprite.ts";

// WHO'S WHO: as on a broadcast, whoever has the ball tagged under him - his
// number in his team's colors, his name on a dark plate.

// With the ball only to snap it or to set it down for the kick.
const SNAPPED_TO = new Set<Role>(["C", "LS", "H"]);
// Faded in and out over (ms).
const IN_MS = 150;
const OUT_MS = 300;
// Over his head, as drawn (feet).
const TAG_OVER = 8.2 * FIGURE_SCALE;

// Where a box over the picture is (fractions of it across, and its top
// down): the score bug, the line over it.
export type TagCover = { x0: number; x1: number; y0: number };

const clamp01 = (u: number) => Math.min(1, Math.max(0, u));

// How much of each man's tag shows at t: the man with the ball, or it just
// away from him.
const shownAt = (play: Play, t: number): Map<number, number> => {
	const shown = new Map<number, number>();
	let i = -1;
	for (let j = 0; j < play.ball.length && play.ball[j]!.t0 <= t; j++) {
		i = j;
	}
	const seg = play.ball[i];
	const prev = play.ball[i - 1];
	// (Only the men in the play - not an official it is tossed to - and not
	// one with it only to snap it.)
	const named = (pid: number) => {
		const role = play.parts.find((p) => p.pid === pid)?.role;
		return role !== undefined && !SNAPPED_TO.has(role);
	};
	if (seg?.kind === "held" && named(seg.pid)) {
		if (t >= play.snap || prev?.kind === "held") {
			shown.set(seg.pid, clamp01((t - seg.t0) / IN_MS));
		}
	} else if (seg && prev?.kind === "held" && named(prev.pid)) {
		shown.set(prev.pid, clamp01(1 - (t - seg.t0) / OUT_MS));
	}
	for (const [pid, a] of shown) {
		if (a <= 0) {
			shown.delete(pid);
		}
	}
	return shown;
};

type Placed = { x: number; y: number; w: number; h: number };

const overlaps = (a: Placed, b: Placed) =>
	a.x < b.x + b.w && a.x + a.w > b.x && a.y < b.y + b.h && a.y + a.h > b.y;

export const drawTags = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	play: Play | undefined,
	t: number,
	players: FieldPlayerState[],
	lookFor: (pid: number) => FieldLook | undefined,
	scale: number,
	clear: TagCover[] = [],
) => {
	if (!play) {
		return;
	}
	const shown = shownAt(play, t);
	if (shown.size === 0) {
		return;
	}
	const pad = 3 * scale;
	const h = 13 * scale;
	const tags = [];
	for (const st of players) {
		const a = shown.get(st.id);
		const look = lookFor(st.id);
		if (a === undefined || !look) {
			continue;
		}
		const w = pixelTextWidth(look.lastName, scale);
		if (w === 0) {
			continue;
		}
		const num = look.jerseyNumber;
		const nw = num ? pixelTextWidth(num, scale) + 6 * scale : 0;
		const feet = project(cam, { x: st.x, y: st.y, z: 0 });
		// (Out of the picture: none.)
		if (
			feet.x < 0 ||
			feet.x > cam.viewW ||
			feet.y < 0 ||
			feet.y > cam.viewH + 4 * scale
		) {
			continue;
		}
		tags.push({ st, a, look, w, num, nw, total: nw + w + 2 * pad, feet });
	}
	// The man with the ball first, the rest top to bottom; each kept off
	// those down before it.
	const holder = players.find((p) => p.ball)?.id;
	tags.sort((a, b) =>
		a.st.id === holder ? -1 : b.st.id === holder ? 1 : a.feet.y - b.feet.y,
	);
	const placed: Placed[] = [];
	const alpha = ctx.globalAlpha;
	for (const { st, a, look, w, num, nw, total, feet } of tags) {
		const x = Math.round(
			Math.max(scale, Math.min(cam.viewW - total - scale, feet.x - total / 2)),
		);
		let y = Math.round(
			Math.min(
				feet.y + Math.max(4 * scale, 0.5 * feet.k),
				cam.viewH - h - scale,
			),
		);
		// (Behind the score bug or the line over it, over his head instead.)
		const behind = clear.some(
			(b) =>
				x < b.x1 * cam.viewW &&
				x + total > b.x0 * cam.viewW &&
				y + h > b.y0 * cam.viewH,
		);
		if (behind) {
			y = Math.max(
				scale,
				Math.round(
					project(cam, { x: st.x, y: st.y, z: TAG_OVER }).y - h - 2 * scale,
				),
			);
		}
		const box = { x, y, w: total, h };
		// (Moved off one in the way, not further: too far from him, it is no
		// longer his.)
		const hit = placed.find((p) => overlaps(p, box));
		if (hit) {
			box.y = behind ? hit.y - h - scale : hit.y + hit.h + scale;
		}
		if (placed.some((p) => overlaps(p, box))) {
			continue;
		}
		placed.push(box);
		ctx.globalAlpha = alpha * a;
		ctx.fillStyle = "rgba(10, 10, 14, 0.82)";
		// The notch up at him, if there is room below him for it.
		if (box.y - 3 * scale >= feet.y) {
			ctx.beginPath();
			ctx.moveTo(Math.round(feet.x) - 3 * scale, box.y);
			ctx.lineTo(Math.round(feet.x), box.y - 3 * scale);
			ctx.lineTo(Math.round(feet.x) + 3 * scale, box.y);
			ctx.closePath();
			ctx.fill();
		}
		ctx.fillRect(x + nw, box.y, w + 2 * pad, h);
		if (num) {
			ctx.fillStyle = look.kit.jersey;
			ctx.fillRect(x, box.y, nw, h);
			drawPixelText(
				ctx,
				num,
				x + 3 * scale,
				box.y + 3 * scale,
				look.kit.number,
				scale,
			);
		}
		ctx.fillStyle = look.kit.helmet;
		ctx.fillRect(x, box.y + h - scale, total, scale);
		drawPixelText(
			ctx,
			look.lastName,
			x + nw + pad,
			box.y + 3 * scale,
			"#ffffff",
			scale,
		);
	}
	ctx.globalAlpha = alpha;
};
