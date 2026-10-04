import { makeCourtRng } from "../courtRng.ts";
import type { RetroTimeline } from "./director.ts";
import {
	evalBall,
	evalPlayer,
	offenseAt,
	recentFx,
	type BallState,
} from "./evaluate.ts";
import {
	BACKBOARD_INSET,
	camLimits,
	COURT_H,
	COURT_W,
	DEPTH,
	FAR_Y,
	K,
	persp,
	project,
	RIM_R,
	RIM_Z,
	rimX,
	VIEW_H,
	type Side,
} from "./geometry.ts";
import type { Body } from "./poses.ts";
import {
	drawPixelText,
	pixelTextWidth,
	type Look,
	type SpriteCache,
} from "./sprites.ts";

// DRAWING THE ARENA.
//
// Everything is drawn into a small buffer (VIEW_H pixels tall) and scaled up
// with no smoothing, so the court, the crowd and the players share one chunky
// pixel grid. The floor is painted once, top-down, and laid into perspective
// one screen row at a time - a classic trick that costs a couple of hundred
// tiny image copies a frame.

const rgb = (h: string): [number, number, number] => {
	const m = /^#?([\da-f]{6})$/i.exec(h.trim());
	const n = m ? Number.parseInt(m[1]!, 16) : 0x888888;
	return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
};
const css = (c: [number, number, number], a = 1) =>
	`rgba(${c[0]},${c[1]},${c[2]},${a})`;
const shade = (c: [number, number, number], f: number) =>
	c.map((v) => Math.max(0, Math.min(255, Math.round(v * f)))) as [
		number,
		number,
		number,
	];
const luminance = (h: string) => {
	const [r, g, b] = rgb(h);
	return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
};

// The floor texture: x from TX0 to TX1 feet, y from TY0 to TY1, R px a foot.
const R = 6;
const TX0 = -12;
const TX1 = 106;
const TY0 = -6;
const TY1 = 57;

const wordFor = (s: string | undefined) =>
	(s ?? "")
		.toUpperCase()
		.replaceAll(/[^\d '.A-Z-]/g, "")
		.trim();

export const buildFloor = (home: {
	colors?: [string, string, string];
	name?: string;
}): HTMLCanvasElement => {
	const floor = document.createElement("canvas");
	floor.width = (TX1 - TX0) * R;
	floor.height = (TY1 - TY0) * R;
	const g = floor.getContext("2d")!;
	const X = (x: number) => (x - TX0) * R;
	const Y = (y: number) => (y - TY0) * R;
	const rng = makeCourtRng("retro-floor");
	const colors = home.colors ?? ["#8c1d40", "#f2c14e", "#ffffff"];
	const paint =
		colors.find((c) => luminance(c) > 0.12 && luminance(c) < 0.75) ?? colors[0];
	const apron = css(shade(rgb(paint), 0.55));

	g.fillStyle = apron;
	g.fillRect(0, 0, floor.width, floor.height);
	g.fillStyle = css(shade(rgb(paint), 0.4));
	g.fillRect(0, 0, floor.width, Y(-3.2));
	const wood = rgb("#c98f55");
	for (let y = 0, i = 0; y < COURT_H; y += 1.25, i++) {
		g.fillStyle = css(shade(wood, i % 2 ? 1.04 : 0.97));
		g.fillRect(X(0), Y(y), COURT_W * R, 1.25 * R + 1);
		g.fillStyle = css(shade(wood, 0.84));
		for (let x = rng() * 8; x < COURT_W; x += 6 + rng() * 9) {
			g.fillRect(X(x), Y(y), 1, 1.25 * R);
		}
	}
	g.fillStyle = paint;
	g.fillRect(X(0), Y(17), 19 * R, 16 * R);
	g.fillRect(X(COURT_W - 19), Y(17), 19 * R, 16 * R);
	g.beginPath();
	g.ellipse(X(COURT_W / 2), Y(25), 6 * R, 6 * R, 0, 0, Math.PI * 2);
	g.fill();

	// The home name across center court, in pixels that survive the squash
	// (twice as tall as wide here, square once the floor is laid down).
	const word = wordFor(home.name).slice(0, 12);
	if (word) {
		const sx = word.length > 7 ? 1.5 : 2;
		const sy = sx * 2.3;
		drawPixelText(
			g,
			word,
			Math.round(X(COURT_W / 2) - pixelTextWidth(word, sx) / 2),
			Math.round(Y(25) - (5 * sy) / 2),
			"#f6efe0",
			sx,
			sy,
		);
	}

	// Lines, stroked with a pen twice as tall as it is wide.
	g.save();
	g.scale(1, 2);
	g.strokeStyle = "#f6efe0";
	g.lineWidth = 1.7;
	const Ys = (y: number) => Y(y) / 2;
	g.beginPath();
	g.rect(X(0), Ys(0), COURT_W * R, Ys(COURT_H) - Ys(0));
	g.moveTo(X(COURT_W / 2), Ys(0));
	g.lineTo(X(COURT_W / 2), Ys(COURT_H));
	const arc = (cx: number, cy: number, r: number, a0: number, a1: number) => {
		const n = 48;
		for (let i = 0; i <= n; i++) {
			const a = a0 + ((a1 - a0) * i) / n;
			const xx = X(cx + r * Math.cos(a));
			const yy = Ys(cy + r * Math.sin(a));
			if (i === 0) {
				g.moveTo(xx, yy);
			} else {
				g.lineTo(xx, yy);
			}
		}
	};
	arc(COURT_W / 2, 25, 6, 0, Math.PI * 2);
	for (const [bx, dir] of [
		[0, 1],
		[COURT_W, -1],
	] as const) {
		g.rect(Math.min(X(bx), X(bx + dir * 19)), Ys(17), 19 * R, Ys(33) - Ys(17));
		arc(bx + dir * 19, 25, 6, 0, Math.PI * 2);
		const rx = bx + dir * 5.25;
		const phi = Math.asin(22 / 23.75);
		g.moveTo(X(bx), Ys(3));
		g.lineTo(X(bx + dir * 14.2), Ys(3));
		g.moveTo(X(bx), Ys(47));
		g.lineTo(X(bx + dir * 14.2), Ys(47));
		if (dir === 1) {
			arc(rx, 25, 23.75, -phi, phi);
			arc(rx, 25, 4, -Math.PI / 2, Math.PI / 2);
		} else {
			arc(rx, 25, 23.75, Math.PI - phi, Math.PI + phi);
			arc(rx, 25, 4, Math.PI / 2, (3 * Math.PI) / 2);
		}
	}
	g.stroke();
	g.restore();
	return floor;
};

export type Fan = {
	row: number;
	wx: number;
	wy: number;
	shirt: string;
	skin: string;
	ph: number;
};

// Rows of pixel fans behind the far sideline, mostly in the home colors. Each
// row sits deeper, so it pans a little slower - parallax for free.
export const buildCrowd = (homeColors: string[] | undefined): Fan[] => {
	const rng = makeCourtRng("retro-crowd");
	const shirts = [
		...(homeColors ?? []),
		...(homeColors ?? []),
		"#f3efe6",
		"#4b6584",
		"#3d3a4b",
		"#c4572d",
		"#e0b54a",
	];
	const skins = [
		"#f0cdb4",
		"#d9a77f",
		"#9a6447",
		"#6b4430",
		"#4a2e22",
		"#c68e6a",
	];
	const fans: Fan[] = [];
	for (let row = 0; row < 7; row++) {
		for (let x = -30 + rng() * 2; x < 124; x += 1.9 + rng() * 0.6) {
			if (rng() < 0.08) {
				continue;
			}
			fans.push({
				row,
				wx: x,
				wy: -9 - row * 2.4,
				shirt: shirts[Math.floor(rng() * shirts.length)]!,
				skin: skins[Math.floor(rng() * skins.length)]!,
				ph: rng() * 6.28,
			});
		}
	}
	return fans;
};

export const buildBallSprite = (): HTMLCanvasElement => {
	const c = document.createElement("canvas");
	c.width = 7;
	c.height = 7;
	const g = c.getContext("2d")!;
	g.fillStyle = "#16121e";
	g.fillRect(1, 0, 5, 7);
	g.fillRect(0, 1, 7, 5);
	g.fillStyle = "#e8742a";
	g.fillRect(1, 1, 5, 5);
	g.fillStyle = "#f6a35e";
	g.fillRect(2, 1, 2, 1);
	g.fillRect(1, 2, 1, 1);
	g.fillStyle = "#8a3a12";
	g.fillRect(3, 1, 1, 5);
	g.fillRect(1, 3, 5, 1);
	return c;
};

export type ArenaTeam = {
	abbrev?: string;
	name?: string;
	colors?: [string, string, string];
};

export type Arena = {
	floor: HTMLCanvasElement;
	crowd: Fan[];
	ball: HTMLCanvasElement;
	boards: [string, string, string][];
	tagColors: [string, string];
	paint: string;
};

export const buildArena = (away: ArenaTeam, home: ArenaTeam): Arena => {
	const hc = home.colors ?? ["#8c1d40", "#f2c14e", "#ffffff"];
	const ac = away.colors ?? ["#1d3461", "#f28c28", "#ffffff"];
	const paint =
		hc.find((c) => luminance(c) > 0.12 && luminance(c) < 0.75) ?? hc[0];
	const readable = (bg: string) =>
		luminance(bg) > 0.55 ? "#16121e" : "#f6efe0";
	const homeWord = wordFor(home.name).slice(0, 10) || wordFor(home.abbrev);
	const awayWord = wordFor(away.abbrev);
	return {
		floor: buildFloor(home),
		crowd: buildCrowd(hc),
		ball: buildBallSprite(),
		boards: [
			[homeWord, paint, readable(paint)],
			["BASKETBALL", "#16121e", "#ffb547"],
			[wordFor(home.abbrev), "#f3efe6", paint],
			[awayWord || "VISITORS", ac[0], readable(ac[0])],
		],
		tagColors: [ac[0], paint],
		paint,
	};
};

export type FrameInput = {
	ctx: CanvasRenderingContext2D;
	viewW: number;
	camX: number;
	t: number;
	tl: RetroTimeline;
	arena: Arena;
	sprites: SpriteCache;
	lookFor: (pid: number) => Look;
	bodyFor: (pid: number) => Body;
	tagFor: (pid: number) => string;
};

// Where the camera wants to be: on the ball, leaning toward the rim the
// offense attacks.
export const cameraTarget = (
	tl: RetroTimeline,
	t: number,
	ball: BallState,
	viewW: number,
): number => {
	const off = offenseAt(tl, t);
	const lean = ball.holder === undefined ? 0 : off === 1 ? 5 : -5;
	// Pulled a little toward where the players are, so a ball brought up the
	// floor alone does not leave the other nine out of the picture.
	let sum = 0;
	let n = 0;
	for (const pid of tl.tracks.keys()) {
		const st = evalPlayer(tl, pid, t);
		if (st.shown) {
			sum += st.x;
			n += 1;
		}
	}
	const center = n > 0 ? sum / n : ball.x;
	const [lo, hi] = camLimits(viewW);
	return Math.min(hi, Math.max(lo, 0.7 * (ball.x + lean) + 0.3 * center));
};

const drawCrowd = (f: FrameInput) => {
	const { ctx, viewW, camX, t, tl, arena } = f;
	const g = ctx.createLinearGradient(0, 0, 0, 60);
	g.addColorStop(0, "#0c0a10");
	g.addColorStop(1, "#211b29");
	ctx.fillStyle = g;
	ctx.fillRect(0, 0, viewW, 62);
	const cheer = recentFx(tl, t, ["cheer"], 1800);
	const dt = cheer ? (t - cheer.t) / 1000 : 9;
	const amp =
		cheer && dt < 1.8 ? (cheer.team === 1 ? 2 : 1) * (1 - dt / 1.8) : 0;
	for (const fan of arena.crowd) {
		const p = project(viewW, camX, fan.wx, fan.wy, 0);
		if (p.x < -4 || p.x > viewW + 4) {
			continue;
		}
		const hop = amp ? Math.round(Math.abs(Math.sin(dt * 9 + fan.ph)) * amp) : 0;
		const x = Math.round(p.x);
		const y = 44 - fan.row * 6 - hop;
		ctx.fillStyle = css(shade(rgb(fan.shirt), 0.9 - fan.row * 0.04));
		ctx.fillRect(x - 1, y, 3, 3);
		ctx.fillStyle = css(shade(rgb(fan.skin), 0.8));
		ctx.fillRect(x, y - 2, 2, 2);
	}
};

const drawFloor = (f: FrameInput) => {
	const { ctx, viewW, camX, arena } = f;
	const floor = arena.floor;
	const top = Math.max(0, Math.ceil(FAR_Y + TY0 * K * DEPTH));
	const bot = Math.min(VIEW_H, Math.floor(FAR_Y + TY1 * K * DEPTH));
	for (let r = top; r < bot; r++) {
		const y = (r + 0.5 - FAR_Y) / (K * DEPTH);
		const ty = Math.floor((y - TY0) * R);
		if (ty < 0 || ty >= floor.height) {
			continue;
		}
		const s = persp(y);
		ctx.drawImage(
			floor,
			0,
			ty,
			floor.width,
			1,
			viewW / 2 + (TX0 - camX) * K * s,
			r,
			floor.width * ((K * s) / R),
			1,
		);
	}
	ctx.fillStyle = "#07060a";
	ctx.fillRect(0, bot, viewW, VIEW_H - bot);
};

const drawBoards = (f: FrameInput) => {
	const { ctx, viewW, camX, arena } = f;
	const y = -5.2;
	for (let x = -30, i = 0; x < 124; x += 12, i++) {
		const a = project(viewW, camX, x, y, 2.4);
		const b = project(viewW, camX, x + 12, y, 0);
		if (b.x < 0 || a.x > viewW) {
			continue;
		}
		const [word, bg, fg] = arena.boards[i % arena.boards.length]!;
		ctx.fillStyle = bg;
		ctx.fillRect(
			Math.round(a.x),
			Math.round(a.y),
			Math.ceil(b.x - a.x),
			Math.round(b.y - a.y),
		);
		ctx.fillStyle = "rgba(0,0,0,0.35)";
		ctx.fillRect(Math.round(a.x), Math.round(b.y) - 1, Math.ceil(b.x - a.x), 1);
		if (word) {
			const w = Math.min(word.length, Math.floor((b.x - a.x - 4) / 4));
			const shown = word.slice(0, w);
			drawPixelText(
				ctx,
				shown,
				Math.round((a.x + b.x) / 2 - pixelTextWidth(shown) / 2),
				Math.round((a.y + b.y) / 2 - 2),
				fg,
			);
		}
	}
};

const line3 = (
	f: FrameInput,
	a: { x: number; y: number; z: number },
	b: { x: number; y: number; z: number },
	color: string,
	w = 1,
) => {
	const p = project(f.viewW, f.camX, a.x, a.y, a.z);
	const q = project(f.viewW, f.camX, b.x, b.y, b.z);
	f.ctx.strokeStyle = color;
	f.ctx.lineWidth = w;
	f.ctx.beginPath();
	f.ctx.moveTo(p.x, p.y);
	f.ctx.lineTo(q.x, q.y);
	f.ctx.stroke();
};

const hoop = (f: FrameInput, side: Side) => {
	const { ctx, viewW, camX, t, tl, arena } = f;
	const dir = side === 0 ? 1 : -1;
	const base = side === 0 ? -5.5 : COURT_W + 5.5;
	const bb = side === 0 ? BACKBOARD_INSET : COURT_W - BACKBOARD_INSET;
	const rx = rimX(side);
	const clank = recentFx(tl, t, ["clank", "dunk"], 900, side);
	const swish = recentFx(tl, t, ["swish"], 900, side);
	const dtC = clank ? (t - clank.t) / 1000 : 9;
	const dtS = swish ? (t - swish.t) / 1000 : 9;
	const shake =
		clank && dtC < 0.9
			? Math.sin(dtC * 42) *
				Math.exp(-dtC * 6) *
				(clank.kind === "dunk" ? 0.35 : 0.18)
			: 0;
	const rz = RIM_Z + shake;
	const P = (x: number, y: number, z: number) => project(viewW, camX, x, y, z);

	const back = () => {
		const b0 = P(base - 1.5, 23, 0);
		const b1 = P(base + 1.5, 27, 0);
		const b2 = P(base + 1.5, 23, 3.4);
		const x0 = Math.round(Math.min(b0.x, b1.x));
		const w = Math.round(Math.abs(b1.x - b0.x));
		ctx.fillStyle = "#2b2532";
		ctx.fillRect(x0, Math.round(b2.y), w, Math.round(b1.y - b2.y));
		ctx.fillStyle = arena.paint;
		ctx.fillRect(x0, Math.round(b2.y), w, 3);
		line3(
			f,
			{ x: base, y: 25, z: 3.4 },
			{ x: base, y: 25, z: 11.6 },
			"#3a3442",
			3,
		);
		line3(
			f,
			{ x: base, y: 25, z: 11.2 },
			{ x: bb - dir * 0.3, y: 25, z: 11.2 },
			"#3a3442",
			2,
		);
		const c = [P(bb, 22, 9.5), P(bb, 28, 9.5), P(bb, 28, 13), P(bb, 22, 13)];
		ctx.fillStyle = "rgba(205,228,240,0.38)";
		ctx.beginPath();
		c.forEach((p, i) => (i ? ctx.lineTo(p.x, p.y) : ctx.moveTo(p.x, p.y)));
		ctx.closePath();
		ctx.fill();
		ctx.strokeStyle = "#f4f1ea";
		ctx.lineWidth = 1;
		ctx.stroke();
		const sq = [P(bb, 24, 10), P(bb, 26, 10), P(bb, 26, 11.5), P(bb, 24, 11.5)];
		ctx.strokeStyle = "#e8742a";
		ctx.beginPath();
		sq.forEach((p, i) => (i ? ctx.lineTo(p.x, p.y) : ctx.moveTo(p.x, p.y)));
		ctx.closePath();
		ctx.stroke();
		line3(
			f,
			{ x: bb, y: 25, z: rz },
			{ x: rx - dir * RIM_R, y: 25, z: rz },
			"#e8742a",
			1,
		);
		const rc = P(rx, 25, rz);
		ctx.strokeStyle = "#c4501a";
		ctx.lineWidth = 1;
		ctx.beginPath();
		ctx.ellipse(
			rc.x,
			rc.y,
			RIM_R * K * rc.s,
			RIM_R * K * DEPTH,
			0,
			Math.PI,
			Math.PI * 2,
		);
		ctx.stroke();
	};

	const front = () => {
		const rc = P(rx, 25, rz);
		const kick = dtS < 0.6 ? Math.sin((dtS / 0.6) * Math.PI) : 0;
		const sway = dtS < 0.9 ? Math.sin(dtS * 30) * Math.exp(-dtS * 5) * 0.18 : 0;
		const bottom = rz - 1.55 - kick * 0.45;
		const net = "rgba(246,243,236,0.85)";
		for (let i = 0; i <= 6; i++) {
			const a = Math.PI + (i / 6) * Math.PI;
			line3(
				f,
				{ x: rx + Math.cos(a) * RIM_R, y: 25 - Math.sin(a) * RIM_R, z: rz },
				{
					x: rx + Math.cos(a) * (0.42 - kick * 0.12) + sway,
					y: 25 - Math.sin(a) * 0.42,
					z: bottom,
				},
				net,
			);
		}
		ctx.strokeStyle = net;
		const bc = P(rx + sway, 25, bottom);
		ctx.beginPath();
		ctx.ellipse(
			bc.x,
			bc.y,
			(0.42 - kick * 0.12) * K * bc.s,
			0.42 * K * DEPTH,
			0,
			0,
			Math.PI,
		);
		ctx.stroke();
		const mc = P(rx + sway * 0.5, 25, (rz + bottom) / 2);
		ctx.beginPath();
		ctx.ellipse(mc.x, mc.y, 0.6 * K * mc.s, 0.6 * K * DEPTH, 0, 0, Math.PI);
		ctx.stroke();
		ctx.strokeStyle = "#ff7a2e";
		ctx.lineWidth = 1.2;
		ctx.beginPath();
		ctx.ellipse(rc.x, rc.y, RIM_R * K * rc.s, RIM_R * K * DEPTH, 0, 0, Math.PI);
		ctx.stroke();
	};
	return { back, front };
};

const shadow = (f: FrameInput, x: number, y: number, z: number, w: number) => {
	const p = project(f.viewW, f.camX, x, y, 0);
	const k = 1 / (1 + z * 0.12);
	f.ctx.fillStyle = `rgba(20,10,10,${0.34 * k})`;
	f.ctx.beginPath();
	f.ctx.ellipse(
		Math.round(p.x),
		Math.round(p.y),
		Math.max(2, w * k),
		Math.max(1, w * 0.36 * k),
		0,
		0,
		Math.PI * 2,
	);
	f.ctx.fill();
};

const SPARKS = (() => {
	const rng = makeCourtRng("retro-sparks");
	return Array.from({ length: 14 }, () => ({
		a: rng() * Math.PI * 2,
		v: 0.5 + rng(),
	}));
})();

export const drawFrame = (f: FrameInput) => {
	const { ctx, viewW, camX, t, tl, arena } = f;
	ctx.imageSmoothingEnabled = false;
	ctx.fillStyle = "#07060a";
	ctx.fillRect(0, 0, viewW, VIEW_H);
	drawCrowd(f);
	drawFloor(f);
	drawBoards(f);

	const ball = evalBall(tl, t, f.bodyFor);
	const states = [];
	for (const pid of tl.tracks.keys()) {
		const st = evalPlayer(tl, pid, t);
		if (st.shown) {
			states.push(st);
		}
	}
	for (const st of states) {
		shadow(f, st.x, st.y, st.z, f.bodyFor(st.pid).torsoW * 0.75);
	}
	shadow(f, ball.x, ball.y, ball.z, 2.2);

	const items: { d: number; draw: () => void }[] = [];
	for (const side of [0, 1] as const) {
		const h = hoop(f, side);
		items.push({ d: 24.25, draw: h.back }, { d: 25.75, draw: h.front });
	}
	for (const st of states) {
		items.push({
			d: st.y,
			draw: () => {
				const spr = f.sprites.get(
					st.pid,
					f.lookFor(st.pid),
					f.bodyFor(st.pid),
					st.anim,
					st.frame,
				);
				const p = project(viewW, camX, st.x, st.y, st.z);
				ctx.save();
				ctx.translate(Math.round(p.x), Math.round(p.y));
				if (st.face < 0) {
					ctx.scale(-1, 1);
				}
				ctx.drawImage(spr.canvas, -spr.ax, -spr.ay);
				ctx.restore();
			},
		});
	}
	const holder =
		ball.holder !== undefined
			? states.find((s) => s.pid === ball.holder)
			: undefined;
	items.push({
		d: holder ? holder.y + 0.01 : ball.y,
		draw: () => {
			const p = project(viewW, camX, ball.x, ball.y, ball.z);
			ctx.drawImage(arena.ball, Math.round(p.x) - 3, Math.round(p.y) - 3);
		},
	});
	items.sort((a, b) => a.d - b.d);
	for (const it of items) {
		it.draw();
	}

	// A block or a make throws a few sparks.
	const fx = recentFx(tl, t, ["block", "swish"], 380);
	if (fx) {
		const u = (t - fx.t) / 380;
		const at =
			fx.kind === "block"
				? project(viewW, camX, ball.x, ball.y, ball.z)
				: project(viewW, camX, rimX(fx.rim ?? 1), 25, RIM_Z - 0.8);
		ctx.fillStyle = `rgba(255,236,190,${1 - u})`;
		for (const s of SPARKS) {
			ctx.fillRect(
				Math.round(at.x + Math.cos(s.a) * s.v * u * 16),
				Math.round(at.y + Math.sin(s.a) * s.v * u * 10),
				1,
				1,
			);
		}
	}

	// The man with the ball wears his name.
	if (holder) {
		const spr = f.sprites.get(
			holder.pid,
			f.lookFor(holder.pid),
			f.bodyFor(holder.pid),
			holder.anim,
			holder.frame,
		);
		const p = project(viewW, camX, holder.x, holder.y, holder.z);
		const word = f.tagFor(holder.pid);
		if (word) {
			const w = pixelTextWidth(word) + 4;
			const x = Math.round(p.x - w / 2);
			const y = Math.max(1, Math.round(p.y - spr.top - 11));
			ctx.fillStyle = arena.tagColors[holder.team];
			ctx.fillRect(x, y, w, 9);
			ctx.fillStyle = "rgba(0,0,0,0.5)";
			ctx.fillRect(x, y + 9, w, 1);
			drawPixelText(
				ctx,
				word,
				x + 2,
				y + 2,
				luminance(arena.tagColors[holder.team]) > 0.6 ? "#16121e" : "#ffffff",
			);
		}
	}
	return ball;
};
