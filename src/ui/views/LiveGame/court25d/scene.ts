import {
	ballAtRim,
	BENCH_X,
	BENCH_Y,
	drawBall,
	drawHoop,
	drawShadow,
	type HoopFx,
} from "./arena.ts";
import { depthOf, project, type Camera, type Shot } from "./camera.ts";
import type { CourtTimeline } from "./director.ts";
import {
	evalBall,
	evalPlayer,
	recentFx,
	type BallState,
	type PlayerState,
} from "./evaluate.ts";
import { drawFigure, type Look } from "./figure.ts";
import { COURT_W, RIM_Z, type Side } from "./geometry.ts";
import type { Body } from "./poses.ts";

// ONE FRAME: everybody's reflection in the hardwood, their shadows, then the
// players, the two baskets and the ball from the back of the picture to the
// front.

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
			players.push(st);
		}
	}
	return { t, players, ball: evalBall(tl, t, bodyFor) };
};

export type Frame = {
	ctx: CanvasRenderingContext2D;
	// Scratch canvas the reflections are gathered on, so they fade as one.
	glossCtx: CanvasRenderingContext2D;
	cam: Camera;
	moment: Moment;
	tl: CourtTimeline;
	roster: { pid: number; team: Side }[];
	bodyFor: (pid: number) => Body;
	lookFor: (pid: number) => Look;
	padColor: string;
	// The warm-up tops the bench wears, by team.
	warmups: [string, string];
	shotClock: string;
	// Pixel ratio of the canvases.
	dpr: number;
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
		};
		warmups.set(look, out);
	}
	return out;
};

// The players who are not in the game, sitting in order on their bench.
const benchStates = (f: Frame, onFloor: Set<number>): PlayerState[] => {
	const out: PlayerState[] = [];
	const seat: [number, number] = [0, 0];
	for (const p of f.roster) {
		if (onFloor.has(p.pid) || seat[p.team] >= 10) {
			continue;
		}
		const i = seat[p.team]++;
		out.push({
			pid: p.pid,
			team: p.team,
			shown: true,
			x: BENCH_X[p.team] + 1.38 + i * 2.05,
			y: BENCH_Y + 0.9,
			z: 0,
			yaw: Math.PI / 2,
			anim: "sit",
			phase: (p.pid * 0.37) % 1,
			moving: false,
		});
	}
	return out;
};

export const drawFrame = (f: Frame) => {
	const { ctx, glossCtx, cam, tl } = f;
	const { t, players, ball } = f.moment;
	const W = ctx.canvas.width;
	const H = ctx.canvas.height;
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.clearRect(0, 0, W, H);
	ctx.setTransform(f.dpr, 0, 0, f.dpr, 0, 0);

	const onFloor = new Set(players.map((p) => p.pid));
	const bench = benchStates(f, onFloor);

	// Reflections in the polished floor.
	const g = glossCtx;
	if (g.canvas.width !== W || g.canvas.height !== H) {
		g.canvas.width = W;
		g.canvas.height = H;
	}
	g.setTransform(1, 0, 0, 1, 0, 0);
	g.clearRect(0, 0, W, H);
	g.setTransform(f.dpr, 0, 0, f.dpr, 0, 0);
	for (const st of players) {
		if (st.x > -3 && st.x < COURT_W + 3 && st.y > -2 && st.y < 52) {
			drawFigure(
				g,
				cam,
				st,
				f.bodyFor(st.pid),
				f.lookFor(st.pid),
				"reflection",
			);
		}
	}
	ctx.save();
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.globalAlpha = 0.17;
	ctx.drawImage(g.canvas, 0, 0);
	ctx.restore();

	// Shadows: soft pools under the feet, shrinking as they leave the floor.
	for (const st of [...players, ...bench]) {
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
				drawFigure(ctx, cam, st, f.bodyFor(st.pid), f.lookFor(st.pid));
			},
		});
	}
	for (const st of bench) {
		items.push({
			depth: depthOf(cam, { x: st.x, y: st.y, z: 3 }),
			draw: () => {
				drawFigure(
					ctx,
					cam,
					st,
					f.bodyFor(st.pid),
					warmupLook(f.lookFor(st.pid), f.warmups[st.team]),
				);
			},
		});
	}
	const spin = t * 0.011;
	const rimSide = ballAtRim(ball);
	for (const side of [0, 1] as const) {
		const rx = side === 0 ? 5.25 : COURT_W - 5.25;
		const fx: HoopFx = {
			swish: fxLevel(tl, t, "swish", 520, side),
			clank: fxLevel(tl, t, "clank", 420, side),
			dunk: fxLevel(tl, t, "dunk", 650, side),
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
	if (rimSide === undefined) {
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
};

// WHERE THE CAMERA LOOKS: mostly at the ball, pulled toward the middle of
// the ten players, and wide enough to keep them - tight on a half-court set,
// wide when the whole floor is running.
export const aimFor = (m: Moment, narrow: boolean): Shot => {
	const { ball } = m;
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
	const minW = narrow ? 42 : 52;
	const maxW = narrow ? 70 : 86;
	const width = Math.min(maxW, Math.max(minW, maxX - minX + 18));
	let x = ball.x * 0.62 + mid * 0.38;
	const room = width / 2 - 7;
	x = Math.min(ball.x + room, Math.max(ball.x - room, x));
	x = Math.min(COURT_W + 12 - width / 2, Math.max(width / 2 - 12, x));
	return { x, width, y: 23 };
};

// Whether a point is on screen, for skipping work.
export const onScreen = (cam: Camera, x: number, y: number, z: number) => {
	const p = project(cam, { x, y, z });
	return p.x > -80 && p.x < cam.viewW + 80 && p.y > -80 && p.y < cam.viewH + 80;
};
