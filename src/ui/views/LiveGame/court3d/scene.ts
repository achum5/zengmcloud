import {
	ballAtRim,
	benchPlane,
	drawBall,
	drawHoop,
	drawJumbo,
	drawShadow,
	FLOOR,
	LED_WALL,
	RAFTERS,
	RIBBON,
	STANDS,
	standsPoint,
	END_STANDS,
	FLOOR_EDGE,
	END_WALL,
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
import { callAt, rowSpot } from "./intro.ts";
import { drawLights } from "./lights.ts";
import { atTable, TABLE_SEAT_Y } from "./crew.ts";
import { drawFolk, type Folk } from "./courtside.ts";
import { STRIP_MS, type CheckIn, type CourtTimeline } from "./director.ts";
import {
	arenaShotAt,
	evalBall,
	evalPlayer,
	offenseAt,
	recentFx,
	seatsAt,
	tensionAt,
	withBody,
	type BallState,
	type PlayerState,
} from "./evaluate.ts";
import { lightness, type Look } from "./figure.ts";
import { COURT_W, RIM_Z, seatSpot, type Pt3, type Side } from "./geometry.ts";
import { drawPixelText, pixelTextWidth } from "./pixelFont.ts";
import { drawTexturedPlane, type TexturedPlane } from "./planes.ts";
import { ANIMS, type AnimName, type Body } from "./poses.ts";
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
	// The stands half empty (see seatsAt), side and ends.
	standsSparse?: HTMLCanvasElement;
	endStandsSparse?: HTMLCanvasElement;
	// Behind each basket (the same pictures at both ends).
	endStands?: [HTMLCanvasElement, HTMLCanvasElement, HTMLCanvasElement];
	// Every screen the LED boards can show (see boardAt).
	boards: Record<"wall" | "ribbon", Record<BoardScreen, HTMLCanvasElement>> &
		Partial<
			Record<"end" | "table" | "jumbo", Record<BoardScreen, HTMLCanvasElement>>
		>;
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
	// The floor round the court, past the lines (the court's apron color).
	apron?: string;
	// The warm-up tops the bench wears, by team.
	warmups: [string, string];
	shotClock: string;
	arena: ArenaPaint;
	// The crowd on its feet (0..1), and which way its arms are this instant.
	crowd: { up: number; wave: boolean };
	// The wall clock (ms), for what idles on its own time - the bench - rather
	// than the game's, which races through a fast-forward and jumps at a cut.
	now?: number;
	// How many picture pixels to a pixel of the lettering drawn into it, so a
	// name reads at the same size on every screen.
	textScale: number;
	// The officials, the coaches and the photographers (see crew.ts), and
	// any of their cameras going off.
	crew?: { st: PlayerState; body: Body; look: Look }[];
	// The people in the courtside seats (see courtside.ts).
	courtside?: Folk[];
	flashes?: Pt3[];
	// Each side's colors [road, home] (main, trim), for the lights swept over
	// the floor at the starting lineups.
	teamColors?: [string, string][];
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

// The bench's warm-up top: a long-sleeved shirt in the warm-up color, the
// team's name across the front in whichever of its colors stands out on it -
// no number, no name on the back, and not the uniform's own picture.
const warmups = new WeakMap<Look, Look>();
const warmupLook = (look: Look, top: string): Look => {
	let out = warmups.get(look);
	if (!out || out.kit.jersey !== top) {
		const k = look.kit;
		const logo = [k.chest ?? k.number, k.jersey, k.trim, "#f4f4f4"].sort(
			(a, b) =>
				Math.abs(lightness(b) - lightness(top)) -
				Math.abs(lightness(a) - lightness(top)),
		)[0]!;
		const shirt: Look = {
			...look,
			kit: { ...k, jersey: top, trim: top, chest: logo },
			outfit: { sleeves: "long", plain: true },
			jerseyNumber: "",
			lastName: "",
		};
		delete shirt.kitArt;
		out = shirt;
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
	const roar = Math.max(
		crowdUp(tl, t),
		// On their feet for each of the home team's starters called out.
		callAt(tl.intro, t)?.team === 1 ? 1 : 0,
	);
	const tense = tensionAt(tl, t);
	return {
		up: Math.max(roar, tense >= 1 ? 0.9 : tense * 0.7),
		wave:
			roar > 0.3
				? Math.sin(now / 130) > 0
				: tense >= 1 && Math.sin(now / 520) > 0.55,
	};
};

// Where a bench player is in his idle loop: the loop at its own pace on the
// wall clock, each man a little out of step with the next. It used to run
// every pose at 1.5 cycles a second of GAME time, whatever pace the pose was
// written for - so the whole bench rocked in its seats all night, and
// twitched at eight times that through every fast-forward.
export const benchPhase = (anim: AnimName, ms: number, pid: number): number => {
	const a = ANIMS[anim];
	const cyclesPerSecond = a.kind === "loop" ? a.fps / a.n : 0.25;
	return (ms / 1000) * cyclesPerSecond + ((Math.abs(pid) * 0.37) % 1);
};

// When each man first came on the floor (Infinity: not yet tonight).
const firstOn = new WeakMap<CourtTimeline, Map<number, number>>();
const firstOnOf = (tl: CourtTimeline, pid: number): number => {
	let m = firstOn.get(tl);
	if (!m) {
		m = new Map();
		for (const [id, tr] of tl.tracks) {
			m.set(id, tr.shown.find(([, on]) => on)?.[0] ?? Infinity);
		}
		firstOn.set(tl, m);
	}
	return m.get(pid) ?? Infinity;
};

// The check-in a man is on at t, if any.
const checkInAt = (
	tl: CourtTimeline,
	pid: number,
	t: number,
): CheckIn | undefined =>
	tl.checkIns?.find((c) => c.pid === pid && c.t0 <= t && t < c.t1);

// Off the floor, a man is in his warm-up top until he has been on it - or
// has pulled it off at the scorer's table on his way on.
export const inWarmup = (
	tl: CourtTimeline,
	pid: number,
	t: number,
): boolean => {
	if (t >= firstOnOf(tl, pid)) {
		return false;
	}
	const ci = checkInAt(tl, pid, t);
	return !ci || t < ci.strip + STRIP_MS * 0.55;
};

// Where a man checking in is at t: walking from his chair to the table,
// down on a knee there, then up and pulling his top off.
const checkInState = (
	ci: CheckIn,
	t: number,
	// Whether he has a top to pull off: not if he has played already.
	warmup: boolean,
): PlayerState => {
	const base = { pid: ci.pid, team: ci.team, shown: true, z: 0 };
	if (t < ci.kneel) {
		const total = ci.path.reduce(
			(sum, p, n) =>
				n === 0
					? 0
					: sum + Math.hypot(p.x - ci.path[n - 1]!.x, p.y - ci.path[n - 1]!.y),
			0,
		);
		let along =
			(total * Math.max(0, t - ci.t0)) / Math.max(1, ci.kneel - ci.t0);
		const walked = along;
		for (let n = 1; n < ci.path.length; n++) {
			const a = ci.path[n - 1]!;
			const b = ci.path[n]!;
			const len = Math.hypot(b.x - a.x, b.y - a.y);
			if (along <= len || n === ci.path.length - 1) {
				const u = len > 0 ? Math.min(1, along / len) : 1;
				return {
					...base,
					x: a.x + (b.x - a.x) * u,
					y: a.y + (b.y - a.y) * u,
					yaw: Math.atan2(b.y - a.y, b.x - a.x),
					anim: "walk",
					phase: walked / 4.8,
					moving: true,
				};
			}
			along -= len;
		}
	}
	const at = ci.path.at(-1)!;
	const still = { ...base, x: at.x, y: at.y, yaw: Math.PI / 2, moving: false };
	if (t < ci.strip) {
		return {
			...still,
			anim: "kneel",
			phase: (t / 1000) * 0.15 + ci.pid * 0.37,
		};
	}
	return {
		...still,
		anim: warmup && t < ci.strip + STRIP_MS ? "strip" : "ready",
		phase: Math.min(1, (t - ci.strip) / STRIP_MS),
	};
};

// The players who are not in the game, sitting in order on their bench -
// up on their feet in a tight finish - or on their way to the table to
// check in.
const STANDING: AnimName[] = ["ready", "crossed", "ready", "crouch"];
// A series won: the bench out onto the floor, round the men who won it in
// their own half - running there, then all over each other.
const STORM_SPEED = 17;
const RUN = ANIMS.run;
const stormState = (
	pid: number,
	team: Side,
	i: number,
	seat: { x: number; y: number },
	from: number,
	t: number,
	now: number,
): PlayerState | undefined => {
	const start = from + 400 + (i % 8) * 140;
	if (t < start) {
		return undefined;
	}
	const c = { x: COURT_W / 2 + (team === 0 ? -8 : 8), y: 25 };
	const a = (i / 9) * Math.PI * 2 + team;
	const r = 3.4 + (i % 3) * 0.9;
	const to = { x: c.x + Math.cos(a) * r, y: c.y + Math.sin(a) * r };
	const d = Math.hypot(to.x - seat.x, to.y - seat.y);
	const ran = ((t - start) / 1000) * STORM_SPEED;
	const base = { pid, team, shown: true, z: 0 };
	if (ran < d) {
		const u = ran / d;
		return {
			...base,
			x: seat.x + (to.x - seat.x) * u,
			y: seat.y + (to.y - seat.y) * u,
			yaw: Math.atan2(to.y - seat.y, to.x - seat.x),
			anim: "run",
			phase: ran / (RUN.kind === "cycle" ? RUN.stride : 8.6),
			moving: true,
		} as PlayerState;
	}
	return {
		...base,
		x: to.x,
		y: to.y,
		yaw: Math.atan2(c.y - to.y, c.x - to.x),
		anim: "celebrate",
		phase: benchPhase("celebrate", now, pid),
		moving: false,
	} as PlayerState;
};
const benchStates = (
	f: Frame,
	onFloor: Set<number>,
): { st: PlayerState; warm: boolean }[] => {
	const out: { st: PlayerState; warm: boolean }[] = [];
	const t = f.moment.t;
	const seat: [number, number] = [0, 0];
	// A big play brings the bench to its feet for a moment - and so does
	// each of its starters called out.
	const roar = recentFx(f.tl, t, ["roar"], 1800);
	const calling = callAt(f.tl.intro, t)?.team;
	const tense = tensionAt(f.tl, t) >= 1;
	const clinch = recentFx(f.tl, t, ["clinch"], 10 * 60_000);
	for (const p of f.roster) {
		const i = seat[p.team]++;
		if (onFloor.has(p.pid)) {
			continue;
		}
		const warm = inWarmup(f.tl, p.pid, t);
		const ci = checkInAt(f.tl, p.pid, t);
		if (ci) {
			out.push({
				st: checkInState(ci, t, firstOnOf(f.tl, p.pid) > ci.strip),
				warm,
			});
			continue;
		}
		const at = seatSpot(p.team, i);
		const storm =
			clinch?.team === p.team
				? stormState(p.pid, p.team, i, at, clinch.t, t, f.now ?? t)
				: undefined;
		if (storm) {
			out.push({ st: storm, warm });
			continue;
		}
		const up = roar?.team === p.team || calling === p.team;
		const anim: AnimName = up
			? "cheer"
			: tense
				? STANDING[Math.abs(p.pid) % STANDING.length]!
				: "sit";
		out.push({
			st: {
				pid: p.pid,
				team: p.team,
				shown: true,
				x: at.x,
				y: at.y,
				z: 0,
				yaw: Math.PI / 2,
				anim,
				phase: benchPhase(anim, f.now ?? t, p.pid),
				moving: false,
			},
			warm,
		});
	}
	return out;
};

// The warm-up tops dropped at the table by the men who checked in, lying
// there a while.
const drawDroppedTops = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	tl: CourtTimeline,
	t: number,
	colors: [string, string],
) => {
	for (const ci of tl.checkIns ?? []) {
		const down = ci.strip + STRIP_MS * 0.85;
		if (t < down || t > ci.t1 + 25_000 || firstOnOf(tl, ci.pid) < ci.strip) {
			continue;
		}
		const at = ci.path.at(-1)!;
		const side = ci.team === 0 ? -1 : 1;
		const cx = at.x + side * 1.1;
		const cy = at.y - 0.5;
		const r = 0.75;
		ctx.fillStyle = colors[ci.team];
		ctx.beginPath();
		for (let n = 0; n < 7; n++) {
			const a = (n / 7) * Math.PI * 2 + ci.pid;
			const k = r * (0.65 + 0.35 * Math.abs(Math.sin(ci.pid * 3.1 + n * 1.7)));
			const p = project(cam, {
				x: cx + Math.cos(a) * k * 1.3,
				y: cy + Math.sin(a) * k,
				z: 0.05,
			});
			ctx.lineTo(p.x, p.y);
		}
		ctx.closePath();
		ctx.fill();
		ctx.fillStyle = "rgba(0,0,0,0.25)";
		const a = project(cam, { x: cx - 0.5, y: cy, z: 0.06 });
		const b = project(cam, { x: cx + 0.5, y: cy + 0.15, z: 0.06 });
		ctx.fillRect(
			Math.min(a.x, b.x),
			a.y,
			Math.max(1, Math.abs(b.x - a.x)),
			Math.max(1, b.k * 0.1),
		);
	}
};

// A monitor on the table in front of each seat behind it, its back to the
// floor and the glow of its screen on the edges.
const drawMonitors = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	crew: { st: PlayerState }[],
) => {
	for (const { st } of crew) {
		if (!atTable(st.pid)) {
			continue;
		}
		const y = TABLE_SEAT_Y + 1.7;
		const corners = (x0: number, x1: number, z0: number, z1: number) =>
			[
				[x0, z0],
				[x1, z0],
				[x1, z1],
				[x0, z1],
			].map(([x, z]) => project(cam, { x: x!, y, z: z! }));
		const quad = (pts: { x: number; y: number }[], color: string) => {
			ctx.fillStyle = color;
			ctx.beginPath();
			for (const p of pts) {
				ctx.lineTo(p.x, p.y);
			}
			ctx.closePath();
			ctx.fill();
		};
		quad(corners(st.x - 0.95, st.x + 0.95, 3.25, 4.45), "#9fc3ff");
		quad(corners(st.x - 0.88, st.x + 0.88, 3.3, 4.38), "#101216");
		quad(corners(st.x - 0.12, st.x + 0.12, 2.7, 3.3), "#202228");
	}
};

// CONFETTI down from the rafters over the floor, the home team champions -
// fluttering as it falls, and lying where it lands. Each piece is its own
// from the moment it was let go, so the same instant always looks the same.
const CONFETTI = 900;
const CONFETTI_FROM = 44;
const drawConfetti = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	tl: CourtTimeline,
	t: number,
	colors: string[],
	// Those still in the air, or those on the floor.
	falling: boolean,
) => {
	const fx = recentFx(tl, t, ["confetti"], 30 * 60_000);
	if (!fx) {
		return;
	}
	for (let k = 0; k < CONFETTI; k++) {
		const dt = (t - fx.t - unitHash(k, 1) * 12_000) / 1000;
		if (dt < 0) {
			continue;
		}
		const v = 3.2 + unitHash(k, 4) * 3.2;
		const down = CONFETTI_FROM / v;
		const air = dt < down;
		if (air !== falling) {
			continue;
		}
		const u = Math.min(dt, down);
		const x =
			-8 + unitHash(k, 2) * (COURT_W + 16) + Math.sin(u * 2.3 + k) * 1.4;
		const y = -6 + unitHash(k, 3) * 62 + Math.cos(u * 1.7 + k) * 0.9;
		const z = Math.max(0, CONFETTI_FROM - v * u);
		const p = project(cam, { x, y, z });
		if (p.x < -4 || p.y < -4 || p.x > cam.viewW + 4 || p.y > cam.viewH + 4) {
			continue;
		}
		const size = Math.max(1.2, p.k * 0.5);
		// Turning over as it falls: edge on, then flat.
		const w = air ? size * Math.max(0.2, Math.abs(Math.sin(dt * 7 + k))) : size;
		ctx.fillStyle = colors[k % colors.length]!;
		ctx.fillRect(p.x - w / 2, p.y - size / 2, w, air ? size : size * 0.6);
	}
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
	// Fewer in their seats: the half-empty stands, the full ones over them
	// as faint as the seats are empty.
	const full = seatsAt(tl, t);
	const thin = full < 0.999 && arena.standsSparse !== undefined;
	const fill = thin ? Math.max(0, (full - 0.55) / 0.45) : 1;
	if (thin) {
		drawTexturedPlane(ctx, cam, STANDS, arena.standsSparse!, 24, 6);
	}
	if (fill > 0.01) {
		drawTexturedPlane(ctx, cam, STANDS, arena.stands, 24, 6, fill);
	}
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
	// Round the ends: the stands, and the boards along their front.
	for (const side of [0, 1] as const) {
		if (arena.endStands) {
			if (thin && arena.endStandsSparse) {
				drawTexturedPlane(
					ctx,
					cam,
					END_STANDS[side],
					arena.endStandsSparse,
					24,
					6,
				);
			}
			if (fill > 0.01) {
				drawTexturedPlane(
					ctx,
					cam,
					END_STANDS[side],
					arena.endStands[0],
					24,
					6,
					fill,
				);
			}
			if (f.crowd.up > 0.01) {
				drawTexturedPlane(
					ctx,
					cam,
					END_STANDS[side],
					arena.endStands[f.crowd.wave ? 2 : 1],
					24,
					6,
					f.crowd.up,
				);
			}
		}
		const end = arena.boards.end?.[screen];
		if (end) {
			drawTexturedPlane(ctx, cam, END_WALL[side], end, 24, 1);
		}
	}
	// The apron: the court's own color carried out past the lines all the
	// way back to the stands - under the seats behind the baskets, the
	// benches and the table along the far side.
	floorQuad(
		ctx,
		cam,
		FLOOR_EDGE.x0,
		FLOOR_EDGE.y0,
		FLOOR_EDGE.x1,
		FLOOR_EDGE.y1,
		f.apron ?? f.padColor,
	);
	if (arena.court) {
		drawTexturedPlane(ctx, cam, COURT_PICTURE, arena.court, 26, 12);
	} else {
		floorQuad(ctx, cam, 0, 0, COURT_W, 50, "#d8a865");
	}
	// The scorer's table, the people behind it hidden from the waist down,
	// their monitors on it.
	const crewAll = f.crew ?? [];
	const folk = f.courtside ?? [];
	// On their feet for a big play by their team, and some of them through
	// a tight finish.
	const roarNow = recentFx(tl, t, ["roar"], 2200);
	const tenseNow = tensionAt(tl, t) >= 1;
	const upOf = (p: Folk): number =>
		(roarNow?.team === p.team && unitHash(p.x * 7, p.y * 13) < 0.8) ||
		(tenseNow && unitHash(p.y * 5, p.x * 3) < 0.35)
			? 1
			: 0;
	// The courtside row along the far side, and the people at the table,
	// behind it and the benches - so drawn before them.
	for (const p of folk) {
		if (p.facing === "north") {
			drawFolk(ctx, cam, p, upOf(p), f.padColor);
		}
	}
	for (const c of crewAll) {
		if (atTable(c.st.pid)) {
			drawSprite(ctx, f.scratch, cam, c.st, c.body, c.look, 1, f.sprites);
		}
	}
	drawTexturedPlane(ctx, cam, TABLE_TOP, arena.tableTop, 4, 1);
	drawMonitors(ctx, cam, crewAll);
	drawTexturedPlane(
		ctx,
		cam,
		TABLE_FRONT,
		arena.boards.table?.[screen] ?? arena.tableFront,
		4,
		1,
	);
	drawTexturedPlane(ctx, cam, benchPlane(0), arena.bench[0], 6, 1);
	drawTexturedPlane(ctx, cam, benchPlane(1), arena.bench[1], 6, 1);
	drawFlashes(ctx, cam, tl, t);
	drawCourtLines(ctx, cam, f.lineColor);
	drawDroppedTops(ctx, cam, tl, t, f.warmups);
	const confetti = [
		...(f.teamColors?.[1] ?? DEFAULT_TEAM_COLORS[1]!),
		"#ffffff",
		"#f5cf4a",
	];
	drawConfetti(ctx, cam, tl, t, confetti, false);

	const crew = crewAll.filter((c) => !atTable(c.st.pid));
	// Shadows: soft pools under the feet, shrinking as they leave the floor.
	for (const st of [
		...players,
		...bench.map((b) => b.st),
		...crew.map((c) => c.st),
	]) {
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
	for (const { st, warm } of bench) {
		const look = f.lookFor(st.pid);
		items.push({
			depth: depthOf(cam, { x: st.x, y: st.y, z: 3 }),
			draw: () => {
				drawSprite(
					ctx,
					f.scratch,
					cam,
					st,
					f.bodyFor(st.pid),
					warm ? warmupLook(look, f.warmups[st.team]) : look,
					1,
					f.sprites,
				);
			},
		});
	}
	// The courtside seats behind the baselines, among the photographers
	// and in front of or behind the stanchion.
	for (const p of folk) {
		if (p.facing !== "north") {
			items.push({
				depth: depthOf(cam, { x: p.x, y: p.y, z: 3 }),
				draw: () => {
					drawFolk(ctx, cam, p, upOf(p), f.padColor);
				},
			});
		}
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
	drawConfetti(ctx, cam, tl, t, confetti, true);
	// The lights down for the starting lineups.
	drawLights(
		ctx,
		cam,
		tl.intro,
		t,
		players,
		f.teamColors ?? DEFAULT_TEAM_COLORS,
	);
	// The board over center court, when the picture takes it in.
	const jumbo = arena.boards.jumbo?.[screen];
	if (jumbo) {
		drawJumbo(ctx, cam, jumbo, (plane, img) => {
			drawTexturedPlane(ctx, cam, plane, img, 24, 1);
		});
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
		drawNameTag(ctx, cam, holder, f.lookFor(holder.pid), f.textScale);
	}
};

const drawNameTag = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	st: PlayerState,
	look: Look,
	scale: number,
) => {
	const w = pixelTextWidth(look.name, scale);
	if (w === 0) {
		return;
	}
	// A broadcast's tag: his number in his team's colors, his name on a
	// dark plate with the team's trim along its foot, and a notch up at him.
	const num = look.jerseyNumber;
	const nw = num ? pixelTextWidth(num, scale) + 6 * scale : 0;
	const pad = 3 * scale;
	const h = 13 * scale;
	const total = nw + w + 2 * pad;
	const feet = project(cam, { x: st.x, y: st.y, z: 0 });
	const x = Math.round(feet.x - total / 2);
	const y = Math.round(feet.y + Math.max(5 * scale, 0.6 * feet.k));
	// The notch.
	ctx.fillStyle = "rgba(10, 10, 14, 0.82)";
	ctx.beginPath();
	ctx.moveTo(Math.round(feet.x) - 3 * scale, y);
	ctx.lineTo(Math.round(feet.x), y - 3 * scale);
	ctx.lineTo(Math.round(feet.x) + 3 * scale, y);
	ctx.closePath();
	ctx.fill();
	ctx.fillRect(x + nw, y, w + 2 * pad, h);
	if (num) {
		ctx.fillStyle = look.kit.jersey;
		ctx.fillRect(x, y, nw, h);
		drawPixelText(
			ctx,
			num,
			x + 3 * scale,
			y + 3 * scale,
			look.kit.number,
			scale,
		);
	}
	ctx.fillStyle = look.kit.trim;
	ctx.fillRect(x, y + h - scale, total, scale);
	drawPixelText(ctx, look.name, x + nw + pad, y + 3 * scale, "#ffffff", scale);
};

// WHERE THE CAMERA LOOKS: mostly at the ball, pulled toward the middle of
// the ten players, and wide enough to keep them - tight on a half-court set,
// wide when the whole floor is running.
const SHOOTING = new Set([
	"shoot",
	"setShot",
	"fade",
	"hook",
	"layup",
	"fingerRoll",
	"powerLayup",
	"scoop",
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
		return framed(left ? 12.5 : COURT_W - 12.5, narrow ? 30 : 36);
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
	// Wide enough that the players read small on the floor, the way a
	// cartoon game shows them.
	const minW = narrow ? 56 : 72;
	const maxW = narrow ? 68 : 88;
	let width = Math.min(maxW, Math.max(minW, maxX - minX + 18));
	let x = ball.x * 0.62 + mid * 0.38;
	// A shot going up: push in on the shooter.
	const shooter = m.players.find((p) => SHOOTING.has(p.anim));
	if (shooter) {
		width *= 0.92;
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

// THE STARTING LINEUPS (see intro.ts): in closer than the game's own
// picture, on each man as he is called - his bench and his team's line both
// in it - panning from one to the next. After the last man called, the
// game's own picture.
const DEFAULT_TEAM_COLORS: [string, string][] = [
	["#ffffff", "#ffffff"],
	["#ffffff", "#ffffff"],
];
export const introAim = (
	tl: CourtTimeline,
	m: Moment,
	narrow: boolean,
): { shot: Shot; rig: Rig } | undefined => {
	const intro = tl.intro;
	const first = intro?.calls[0];
	const last = intro?.calls.at(-1);
	if (!intro || !first || !last || m.t < intro.t0 || m.t >= last.t1) {
		return undefined;
	}
	const c = callAt(intro, m.t) ?? first;
	const st = m.players.find((p) => p.pid === c.pid);
	return {
		shot: {
			x: st ? st.x : rowSpot(c.team, 0).x,
			width: narrow ? 34 : 42,
			y: 4,
		},
		rig: MAIN_RIG,
	};
};

// Whether a point is on screen, for skipping work.
export const onScreen = (cam: Camera, x: number, y: number, z: number) => {
	const p = project(cam, { x, y, z });
	return p.x > -80 && p.x < cam.viewW + 80 && p.y > -80 && p.y < cam.viewH + 80;
};
