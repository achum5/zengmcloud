import {
	ballAtRim,
	benchPlane,
	drawBall,
	drawHoop,
	drawShadow,
	FLOOR,
	LED_WALL,
	RAFTERS,
	RIBBON,
	STANDS,
	standsPoint,
	TABLE_FRONT,
	TABLE_TOP,
	type BoardScreen,
	type HoopFx,
} from "./arena.ts";
import {
	depthOf,
	MAIN_RIG,
	project,
	type Camera,
	type CourtFit,
	type Rig,
	type Shot,
} from "./camera.ts";
import { drawCourtLines } from "./courtLines.ts";
import type { CourtTimeline } from "./director.ts";
import {
	arenaShotAt,
	evalBall,
	evalPlayer,
	offenseAt,
	recentFx,
	tensionAt,
	withBody,
	type BallState,
	type PlayerState,
} from "./evaluate.ts";
import type { Look } from "./figure.ts";
import { COURT_W, RIM_Z, seatSpot, type Pt3, type Side } from "./geometry.ts";
import { drawPixelText, pixelTextWidth } from "./pixelFont.ts";
import { drawTexturedPlane, type TexturedPlane } from "./planes.ts";
import type { AnimName, Body } from "./poses.ts";
import { drawSprite, type Scratch, type SpriteCache } from "./sprite.ts";

// ONE FRAME, AS PIXEL ART: the building and the floor laid out in
// perspective, the lines, the shadows, then the players, the two baskets and
// the ball from the back of the picture to the front - all drawn small, on a
// frame a few hundred pixels tall, for the page to blow up without smoothing.

// Everybody on the floor and the ball at time t.
export type Moment = {
	t: number;
	players: PlayerState[];
	ball: BallState;
};

export const momentAt = (
	tl: CourtTimeline,
	t: number,
	roster: { pid: number }[],
	bodyFor: (pid: number) => Body,
): Moment => {
	const players: PlayerState[] = [];
	for (const p of roster) {
		const st = evalPlayer(tl, p.pid, t);
		if (st.shown) {
			players.push(withBody(st, bodyFor(p.pid)));
		}
	}
	return { t, players, ball: evalBall(tl, t, bodyFor) };
};

// The building's pictures, painted once a game.
export type ArenaPaint = {
	// The home floor, from the 2D court's drawing (see courtTexture) - until it
	// is ready, plain wood.
	court?: HTMLCanvasElement;
	stands: HTMLCanvasElement;
	standsUp: HTMLCanvasElement;
	standsWave: HTMLCanvasElement;
	// Every screen the LED boards can show (see boardAt).
	boards: Record<"wall" | "ribbon", Record<BoardScreen, HTMLCanvasElement>>;
	rafters: HTMLCanvasElement;
	tableTop: HTMLCanvasElement;
	tableFront: HTMLCanvasElement;
	bench: [HTMLCanvasElement, HTMLCanvasElement];
};

// The court picture's plane: the 2D court's own frame, in feet.
const COURT_PICTURE: TexturedPlane = {
	origin: { x: -5, y: -2.5, z: 0 },
	alongX: { x: 1, y: 0, z: 0 },
	alongY: { x: 0, y: 1, z: 0 },
	w: COURT_W + 10,
	h: 55,
};

export type Frame = {
	// The frame being drawn: small, a pixel-art pixel to a canvas pixel.
	ctx: CanvasRenderingContext2D;
	scratch: Scratch;
	sprites: SpriteCache;
	cam: Camera;
	moment: Moment;
	tl: CourtTimeline;
	roster: { pid: number; team: Side }[];
	bodyFor: (pid: number) => Body;
	lookFor: (pid: number) => Look;
	padColor: string;
	// The home floor's line paint.
	lineColor: string;
	// The warm-up tops the bench wears, by team.
	warmups: [string, string];
	shotClock: string;
	arena: ArenaPaint;
	// The crowd on its feet (0..1), and which way its arms are this instant.
	crowd: { up: number; wave: boolean };
	// How many picture pixels to a pixel of the lettering drawn into it, so a
	// name reads at the same size on every screen.
	textScale: number;
	// The officials, the coaches and the photographers (see crew.ts), and
	// any of their cameras going off.
	crew?: { st: PlayerState; body: Body; look: Look }[];
	flashes?: Pt3[];
};

const fxLevel = (
	tl: CourtTimeline,
	t: number,
	kind: "swish" | "clank" | "dunk",
	ms: number,
	rim: Side,
): number => {
	const f = recentFx(tl, t, [kind], ms, rim);
	return f ? Math.max(0, 1 - (t - f.t) / ms) : 0;
};

const warmups = new WeakMap<Look, Look>();
const warmupLook = (look: Look, top: string): Look => {
	let out = warmups.get(look);
	if (!out || out.kit.jersey !== top) {
		out = {
			...look,
			kit: { ...look.kit, jersey: top, trim: top },
			jerseyNumber: "",
			lastName: "",
			wordmark: "",
		};
		warmups.set(look, out);
	}
	return out;
};

// A cheap hash to a number in [0, 1), so the same flashes go off every time.
const unitHash = (a: number, b: number): number => {
	let h =
		Math.imul((a | 0) ^ 0x9e3779b9, 0x85ebca6b) ^ Math.imul(b + 1, 0xc2b2ae35);
	h ^= h >>> 15;
	h = Math.imul(h, 0x2c1b3c6d);
	h ^= h >>> 12;
	return (h >>> 0) / 2 ** 32;
};

// Cameras going off in the stands after a big dunk.
const drawFlashes = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	tl: CourtTimeline,
	t: number,
) => {
	const fx = recentFx(tl, t, ["dunk"], 1700);
	if (!fx || !fx.big) {
		return;
	}
	const since = t - fx.t;
	for (let i = 0; i < 36; i++) {
		const fire = 60 + unitHash(fx.t, i * 3) * 1500;
		const age = since - fire;
		if (age < 0 || age > 110) {
			continue;
		}
		const p = project(
			cam,
			standsPoint(
				-30 + unitHash(fx.t, i * 3 + 1) * 154,
				1 + unitHash(fx.t, i * 3 + 2) * 26,
			),
		);
		if (
			p.x < -20 ||
			p.x > cam.viewW + 20 ||
			p.y < -20 ||
			p.y > cam.viewH + 20
		) {
			continue;
		}
		const a = 1 - age / 110;
		const r = Math.max(4, p.k * 1.4);
		const g = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, r);
		g.addColorStop(0, `rgba(255,255,255,${a})`);
		g.addColorStop(0.25, `rgba(235,240,255,${0.6 * a})`);
		g.addColorStop(1, "rgba(220,230,255,0)");
		ctx.fillStyle = g;
		ctx.beginPath();
		ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
		ctx.fill();
	}
};

// How much of the crowd is on its feet (0..1): the home crowd erupts at a
// big play of its team's, the road fans less so.
export const crowdUp = (tl: CourtTimeline, t: number): number => {
	const fx = recentFx(tl, t, ["roar"], 2700);
	if (!fx) {
		return 0;
	}
	const e = t - fx.t;
	const level =
		e < 260 ? e / 260 : e < 1800 ? 1 : Math.max(0, 1 - (e - 1800) / 900);
	return level * (fx.team === 1 ? 1 : 0.4);
};

// The crowd at t: on its feet for a big play, arms going - and in a tight
// finish on its feet the whole way, arms going up now and then (`now` is
// the wall clock, for the arms).
export const crowdAt = (
	tl: CourtTimeline,
	t: number,
	now: number,
): { up: number; wave: boolean } => {
	const roar = crowdUp(tl, t);
	const tense = tensionAt(tl, t);
	return {
		up: Math.max(roar, tense >= 1 ? 0.9 : tense * 0.7),
		wave:
			roar > 0.3
				? Math.sin(now / 130) > 0
				: tense >= 1 && Math.sin(now / 520) > 0.55,
	};
};

// The players who are not in the game, sitting in order on their bench -
// up on their feet in a tight finish.
const STANDING: AnimName[] = ["ready", "crossed", "ready", "crouch"];
const benchStates = (f: Frame, onFloor: Set<number>): PlayerState[] => {
	const out: PlayerState[] = [];
	const seat: [number, number] = [0, 0];
	// A big play brings the bench to its feet for a moment.
	const roar = recentFx(f.tl, f.moment.t, ["roar"], 1800);
	const tense = tensionAt(f.tl, f.moment.t) >= 1;
	for (const p of f.roster) {
		const i = seat[p.team]++;
		if (onFloor.has(p.pid)) {
			continue;
		}
		const at = seatSpot(p.team, i);
		const up = roar?.team === p.team;
		out.push({
			pid: p.pid,
			team: p.team,
			shown: true,
			x: at.x,
			y: at.y,
			z: 0,
			yaw: Math.PI / 2,
			anim: up
				? "cheer"
				: tense
					? STANDING[Math.abs(p.pid) % STANDING.length]!
					: "sit",
			phase: (f.moment.t / 1000) * 1.5 + ((p.pid * 0.37) % 1),
			moving: false,
		});
	}
	return out;
};

// A plain quad on the floor, in one color.
const floorQuad = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	x0: number,
	y0: number,
	x1: number,
	y1: number,
	color: string,
) => {
	ctx.fillStyle = color;
	ctx.beginPath();
	for (const [x, y] of [
		[x0, y0],
		[x1, y0],
		[x1, y1],
		[x0, y1],
	] as const) {
		const p = project(cam, { x, y, z: 0 });
		ctx.lineTo(p.x, p.y);
	}
	ctx.closePath();
	ctx.fill();
};

export const drawFrame = (f: Frame) => {
	const { ctx, cam, tl, arena } = f;
	const { t, players, ball } = f.moment;
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.fillStyle = "#07060a";
	ctx.fillRect(0, 0, ctx.canvas.width, ctx.canvas.height);

	const onFloor = new Set(players.map((p) => p.pid));
	const bench = benchStates(f, onFloor);

	// The building: the stands (on their feet after a big play), the LED
	// ribbon, the floor round the court, the court, the table, the benches.
	drawTexturedPlane(ctx, cam, STANDS, arena.stands, 24, 6);
	if (f.crowd.up > 0.01) {
		drawTexturedPlane(
			ctx,
			cam,
			STANDS,
			f.crowd.wave ? arena.standsWave : arena.standsUp,
			24,
			6,
			f.crowd.up,
		);
	}
	// The ribbon round the upper deck and the banners over it, the boards
	// along the front of the stands - showing what the moment calls for.
	const screen = boardAt(tl, t);
	drawTexturedPlane(ctx, cam, RIBBON, arena.boards.ribbon[screen], 24, 1);
	drawTexturedPlane(ctx, cam, RAFTERS, arena.rafters, 24, 2);
	drawTexturedPlane(ctx, cam, LED_WALL, arena.boards.wall[screen], 24, 1);
	floorQuad(
		ctx,
		cam,
		FLOOR.origin.x,
		FLOOR.origin.y,
		FLOOR.origin.x + FLOOR.w * FLOOR.alongX.x,
		FLOOR.origin.y + FLOOR.h * FLOOR.alongY.y,
		"#241e18",
	);
	if (arena.court) {
		drawTexturedPlane(ctx, cam, COURT_PICTURE, arena.court, 26, 12);
	} else {
		floorQuad(ctx, cam, 0, 0, COURT_W, 50, "#d8a865");
	}
	drawTexturedPlane(ctx, cam, TABLE_TOP, arena.tableTop, 4, 1);
	drawTexturedPlane(ctx, cam, TABLE_FRONT, arena.tableFront, 4, 1);
	drawTexturedPlane(ctx, cam, benchPlane(0), arena.bench[0], 6, 1);
	drawTexturedPlane(ctx, cam, benchPlane(1), arena.bench[1], 6, 1);
	drawFlashes(ctx, cam, tl, t);
	drawCourtLines(ctx, cam, f.lineColor);

	const crew = f.crew ?? [];
	// Shadows: soft pools under the feet, shrinking as they leave the floor.
	for (const st of [...players, ...bench, ...crew.map((c) => c.st)]) {
		const lift = Math.min(1, st.z / 4);
		drawShadow(ctx, cam, st.x, st.y, 1.25 * (1 - lift * 0.35), 1 - lift * 0.6);
	}
	drawShadow(
		ctx,
		cam,
		ball.x,
		ball.y,
		0.45,
		Math.max(0, 1 - ball.z / 14) * 0.9,
	);

	type Item = { depth: number; draw: () => void };
	const items: Item[] = [];
	for (const st of players) {
		items.push({
			depth: depthOf(cam, { x: st.x, y: st.y, z: 3 }),
			draw: () => {
				drawSprite(
					ctx,
					f.scratch,
					cam,
					st,
					f.bodyFor(st.pid),
					f.lookFor(st.pid),
					1,
					f.sprites,
				);
			},
		});
	}
	for (const st of bench) {
		items.push({
			depth: depthOf(cam, { x: st.x, y: st.y, z: 3 }),
			draw: () => {
				drawSprite(
					ctx,
					f.scratch,
					cam,
					st,
					f.bodyFor(st.pid),
					warmupLook(f.lookFor(st.pid), f.warmups[st.team]),
					1,
					f.sprites,
				);
			},
		});
	}
	for (const c of crew) {
		items.push({
			depth: depthOf(cam, { x: c.st.x, y: c.st.y, z: 3 }),
			draw: () => {
				drawSprite(ctx, f.scratch, cam, c.st, c.body, c.look, 1, f.sprites);
			},
		});
	}
	const spin = ball.roll ?? 0;
	// In somebody's hands it is drawn with him (see drawSprite).
	const inHands =
		ball.holder !== undefined &&
		players.some((p) => p.pid === ball.holder && p.holding);
	const rimSide = inHands ? undefined : ballAtRim(ball);
	for (const side of [0, 1] as const) {
		const rx = side === 0 ? 5.25 : COURT_W - 5.25;
		const fx: HoopFx = {
			swish: fxLevel(tl, t, "swish", 520, side),
			clank: fxLevel(tl, t, "clank", 420, side),
			dunk: fxLevel(tl, t, "dunk", 1100, side),
			big: recentFx(tl, t, ["dunk"], 1100, side)?.big,
			t,
		};
		items.push({
			depth: depthOf(cam, { x: rx, y: 25, z: RIM_Z }),
			draw: () => {
				drawHoop(
					ctx,
					cam,
					side,
					f.padColor,
					f.shotClock,
					fx,
					rimSide === side
						? () => {
								drawBall(ctx, cam, ball, spin);
							}
						: undefined,
				);
			},
		});
	}
	if (rimSide === undefined && !inHands) {
		items.push({
			depth: depthOf(cam, ball) - 0.3,
			draw: () => {
				drawBall(ctx, cam, ball, spin);
			},
		});
	}
	items.sort((a, b) => b.depth - a.depth);
	for (const it of items) {
		it.draw();
	}
	// A photographer's flash.
	for (const at of f.flashes ?? []) {
		const p = project(cam, at);
		const r = Math.max(3, p.k * 1.1);
		const g = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, r);
		g.addColorStop(0, "rgba(255,255,255,1)");
		g.addColorStop(0.3, "rgba(240,244,255,0.7)");
		g.addColorStop(1, "rgba(220,230,255,0)");
		ctx.fillStyle = g;
		ctx.beginPath();
		ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
		ctx.fill();
	}

	// Whoever has the ball, named under his feet.
	const holder =
		ball.holder === undefined
			? undefined
			: players.find((p) => p.pid === ball.holder);
	if (holder) {
		drawNameTag(ctx, cam, holder, f.lookFor(holder.pid).name, f.textScale);
	}
};

const drawNameTag = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	st: PlayerState,
	name: string,
	scale: number,
) => {
	const w = pixelTextWidth(name, scale);
	if (w === 0) {
		return;
	}
	const feet = project(cam, { x: st.x, y: st.y, z: 0 });
	const x = Math.round(feet.x - w / 2);
	const y = Math.round(feet.y + Math.max(3 * scale, 0.55 * feet.k));
	ctx.fillStyle = "rgba(8, 8, 12, 0.72)";
	ctx.fillRect(x - 2 * scale, y - 2 * scale, w + 4 * scale, 11 * scale);
	drawPixelText(ctx, name, x, y, "#ffffff", scale);
};

// WHERE THE CAMERA LOOKS: mostly at the ball, pulled toward the middle of
// the ten players, and wide enough to keep them - tight on a half-court set,
// wide when the whole floor is running.
const SHOOTING = new Set([
	"shoot",
	"fade",
	"hook",
	"layup",
	"dunk",
	"dunk1",
	"tomahawk",
]);

// Where the main camera looks - never tighter than `fit` lets it, so the
// whole floor stays in the picture, sideline to sideline.
export const aimFor = (
	m: Moment,
	narrow: boolean,
	tl: CourtTimeline,
	fit?: CourtFit,
): Shot => {
	const framed = (x: number, w: number): Shot => {
		const width = Math.max(w, fit?.min ?? 0);
		return {
			x: Math.min(COURT_W + 12 - width / 2, Math.max(width / 2 - 12, x)),
			width,
			y: fit ? fit.y(width) : 24,
		};
	};
	const { ball } = m;
	// Free throws: on the shooter, the line and the rim.
	const beat = beatAt(tl, m.t);
	if (beat && (beat.type === "ft" || beat.type === "missFt")) {
		const left = ball.x < COURT_W / 2;
		return framed(left ? 12.5 : COURT_W - 12.5, narrow ? 26 : 32);
	}
	let minX = ball.x;
	let maxX = ball.x;
	let sum = 0;
	let n = 0;
	for (const st of m.players) {
		if (st.y < -1.5) {
			continue;
		}
		minX = Math.min(minX, st.x);
		maxX = Math.max(maxX, st.x);
		sum += st.x;
		n += 1;
	}
	const mid = n > 0 ? sum / n : ball.x;
	const minW = narrow ? 34 : 44;
	const maxW = narrow ? 54 : 68;
	let width = Math.min(maxW, Math.max(minW, maxX - minX + 18));
	let x = ball.x * 0.62 + mid * 0.38;
	// A shot going up: push in on the shooter.
	const shooter = m.players.find((p) => SHOOTING.has(p.anim));
	if (shooter) {
		width *= 0.86;
		x = x * 0.6 + shooter.x * 0.4;
	}
	width = Math.max(width, fit?.min ?? 0);
	const room = width / 2 - 7;
	x = Math.min(ball.x + room, Math.max(ball.x - room, x));
	return framed(x, width);
};

// The play-by-play line being played out at t.
const beatAt = (tl: CourtTimeline, t: number) => {
	let lo = 0;
	let hi = tl.beats.length - 1;
	let ans = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (tl.beats[mid]!.preStart <= t) {
			ans = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	return ans >= 0 ? tl.beats[ans] : undefined;
};

// The slow-motion replay of a dunk: close on him and the rim, from
// courtside, looking up at the iron.
export const replayAim = (m: Moment, narrow: boolean): Shot => {
	const rimX = m.ball.x < COURT_W / 2 ? 5.25 : COURT_W - 5.25;
	return {
		x: m.ball.x * 0.55 + rimX * 0.45,
		width: narrow ? 22 : 26,
		y: 25,
		z: 6.5,
	};
};

// WHAT THE BOARDS SHOW: a shout for the home side's big play, DEFENSE
// (blinking) while the road team has the ball in play, otherwise the team
// and the chants in turn.
const AMBIENT: BoardScreen[] = ["name", "letsGo", "name", "noise"];
export const boardAt = (tl: CourtTimeline, t: number): BoardScreen => {
	const roar = recentFx(tl, t, ["roar"], 3200);
	if (roar?.team === 1 && roar.what) {
		return roar.what;
	}
	if (recentFx(tl, t, ["block"], 2400)?.team === 1) {
		return "block";
	}
	if (!arenaShotAt(tl, t) && offenseAt(tl, t) === 0) {
		return Math.floor(t / 420) % 2 ? "defense2" : "defense";
	}
	// Crunch time with the ball: the building as loud as it gets.
	if (!arenaShotAt(tl, t) && tensionAt(tl, t) >= 0.5) {
		return Math.floor(t / 2600) % 2 ? "letsGo" : "noise";
	}
	return AMBIENT[Math.floor(t / 8000) % AMBIENT.length]!;
};

// THE LOOK ROUND THE BUILDING over a break (see ArenaShot): the whole arena
// from up high, panning slowly; or low in the seats, looking up at the
// crowd and the banners.
const ARENA_RIG: Rig = { back: 60, high: 22, slide: 0.5, upright: false };
export const arenaAim = (
	tl: CourtTimeline,
	t: number,
	narrow: boolean,
): { shot: Shot; rig: Rig } | undefined => {
	const s = arenaShotAt(tl, t);
	if (!s) {
		return undefined;
	}
	const u = Math.min(1, Math.max(0, (t - s.t0) / Math.max(1, s.t1 - s.t0)));
	if (s.kind === "wide") {
		return {
			shot: {
				x: 40 + 14 * u,
				width: narrow ? 132 : 158,
				y: narrow ? -38 : -27,
			},
			rig: MAIN_RIG,
		};
	}
	return {
		shot: { x: 26 + 42 * u, width: narrow ? 70 : 90, y: -14, z: 16 },
		rig: ARENA_RIG,
	};
};

// Whether a point is on screen, for skipping work.
export const onScreen = (cam: Camera, x: number, y: number, z: number) => {
	const p = project(cam, { x, y, z });
	return p.x > -80 && p.x < cam.viewW + 80 && p.y > -80 && p.y < cam.viewH + 80;
};
