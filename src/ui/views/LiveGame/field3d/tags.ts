import { project, type Camera } from "../court3d/camera.ts";
import { drawPixelText, pixelTextWidth } from "../court3d/pixelFont.ts";
import { FIGURE_SCALE } from "./body.ts";
import type { Play } from "./director.ts";
import type { Role } from "./formations.ts";
import type { FieldLook } from "./kit.ts";
import type { FieldPlayerState } from "./sprite.ts";

// WHO'S WHO: a broadcast's tags under the men - his number in his team's
// colors, his name on a dark plate. Before the snap on the quarterback, the
// backs and the receivers (for a kick, the kicker and the men back to field
// it); from the snap on whoever has the ball.

const NAMED = new Set<Role>([
	"QB",
	"RB",
	"FB",
	"TE",
	"TE2",
	"X",
	"Z",
	"SL",
	"SL2",
	"K",
	"P",
	"KR",
	"PR",
]);
// With the ball only to snap it or to set it down for the kick.
const SNAPPED_TO = new Set<Role>(["C", "LS", "H"]);
// Faded in and out over (ms) - the men before the snap from as they come
// up to the line, apart.
const IN_MS = 400;
const UP_MS = 1500;
const OUT_MS = 300;
// Over his head, as drawn (feet).
const TAG_OVER = 8.2 * FIGURE_SCALE;

// Where a box over the picture is (fractions of it across, and its top
// down): the score bug, the line over it.
export type TagCover = { x0: number; x1: number; y0: number };

const clamp01 = (u: number) => Math.min(1, Math.max(0, u));

// How much of each man's tag shows at t.
const shownAt = (
	play: Play,
	t: number,
	lineUp: boolean,
): Map<number, number> => {
	const shown = new Map<number, number>();
	const show = (pid: number, a: number) => {
		if (a > 0) {
			shown.set(pid, Math.max(shown.get(pid) ?? 0, a));
		}
	};
	if (lineUp && t < play.snap + OUT_MS) {
		const a = Math.min(
			clamp01((t - Math.max(play.t0, play.set - UP_MS)) / IN_MS),
			clamp01((play.snap + OUT_MS - t) / OUT_MS),
		);
		for (const part of play.parts) {
			if (NAMED.has(part.role)) {
				show(part.pid, a);
			}
		}
	}
	// The ball's last stop by t: a man with it, or it just away from one.
	let i = -1;
	for (let j = 0; j < play.ball.length && play.ball[j]!.t0 <= t; j++) {
		i = j;
	}
	const seg = play.ball[i];
	const roleOf = (pid: number) => play.parts.find((p) => p.pid === pid)?.role;
	if (seg?.kind === "held" && !SNAPPED_TO.has(roleOf(seg.pid) ?? "C")) {
		const prev = play.ball[i - 1];
		// (Caught by the man tagged while it was in the air: still his.)
		const caught =
			prev?.kind === "fly" && targetOf(play, prev.t1, prev.to) === seg.pid;
		show(
			seg.pid,
			caught
				? 1
				: prev?.kind === "held" || t >= play.snap
					? clamp01((t - seg.t0) / (prev?.kind === "fly" ? IN_MS : 120))
					: 0,
		);
	} else {
		const prev = play.ball[i - 1];
		if (
			seg &&
			prev?.kind === "held" &&
			!SNAPPED_TO.has(roleOf(prev.pid) ?? "C")
		) {
			show(prev.pid, clamp01(1 - (t - seg.t0) / OUT_MS));
		}
		// Thrown, punted or kicked off: the man it is going to as well.
		// (From where it last stopped: a punter drops it, then kicks it.)
		let k = i - 1;
		while (play.ball[k]?.kind === "fly") {
			k--;
		}
		const from = play.ball[k];
		if (
			seg?.kind === "fly" &&
			seg.t0 < play.whistle &&
			(from?.kind === "held"
				? NAMED.has(roleOf(from.pid) ?? "C")
				: from?.kind === "spot" && from.stood === "tee")
		) {
			const to = targetOf(play, seg.t1, seg.to);
			if (to !== undefined) {
				show(to, clamp01((t - seg.t0) / IN_MS));
			}
		}
	}
	// (Only the men in the play: not an official it is tossed to.)
	const inPlay = new Set(play.parts.map((p) => p.pid));
	for (const pid of shown.keys()) {
		if (!inPlay.has(pid)) {
			shown.delete(pid);
		}
	}
	return shown;
};

// Where a man is at t, near enough.
const atTime = (part: Play["parts"][number], t: number) => {
	const leg = part.legs.find((l) => l.t0 <= t && l.t1 >= t) ?? part.legs.at(-1);
	if (!leg) {
		return undefined;
	}
	const u = leg.t1 > leg.t0 ? clamp01((t - leg.t0) / (leg.t1 - leg.t0)) : 1;
	return {
		x: leg.from.x + (leg.to.x - leg.from.x) * u,
		y: leg.from.y + (leg.to.y - leg.from.y) * u,
	};
};
// Who is under the ball where it comes down: a receiver, a returner.
const TARGET_FT = 12;
const targets = new WeakMap<Play, Map<number, number | undefined>>();
const targetOf = (play: Play, t1: number, to: { x: number; y: number }) => {
	let known = targets.get(play);
	if (!known) {
		known = new Map();
		targets.set(play, known);
	}
	if (!known.has(t1)) {
		let best: number | undefined;
		let bestD = TARGET_FT;
		for (const part of play.parts) {
			if (!NAMED.has(part.role)) {
				continue;
			}
			const at = atTime(part, t1);
			const d = at ? Math.hypot(at.x - to.x, at.y - to.y) : Infinity;
			if (d < bestD) {
				best = part.pid;
				bestD = d;
			}
		}
		known.set(t1, best);
	}
	return known.get(t1);
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
	// The men before the snap too: not in a replay.
	lineUp = true,
) => {
	if (!play) {
		return;
	}
	const shown = shownAt(play, t, lineUp);
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
