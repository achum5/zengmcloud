import {
	ballAtRim,
	benchPlane,
	drawBall,
	drawGlints,
	drawHoop,
	drawJumbo,
	drawShadow,
	FLOOR,
	jumboScored,
	LED_WALL,
	RAFTERS,
	RIBBON,
	STANDS,
	standsPoint,
	END_STANDS,
	FLOOR_EDGE,
	END_WALL,
	drawCourtside,
	TABLE_FRONT,
	TABLE_TOP,
	type BoardScreen,
	type Courtside,
	type HoopFx,
} from "./arena.ts";
import {
	BASELINE_RIG,
	depthOf,
	MAIN_RIG,
	makeCamera,
	project,
	REPLAY_RIG,
	type Camera,
	type CourtFit,
	type Rig,
	type Shot,
} from "./camera.ts";
import { drawCourtLines } from "./courtLines.ts";
import { callAt, rowSpot } from "./intro.ts";
import { drawLights } from "./lights.ts";
import { atTable, TABLE_SEAT_Y } from "./crew.ts";
import {
	STRIP_MS,
	type Beat,
	type CheckIn,
	type CourtTimeline,
	type Huddle,
} from "./director.ts";
import {
	arenaShotAt,
	benchLook,
	ballFaceAt,
	evalBall,
	evalPlayer,
	shownAt,
	offenseAt,
	recentFx,
	seatsAt,
	tensionAt,
	withBody,
	type BallState,
	type Blend,
	type PlayerState,
} from "./evaluate.ts";
import { lightness, type Look } from "./figure.ts";
import {
	benchX,
	COURT_W,
	HUDDLE_Y,
	huddleCoach,
	huddleRing,
	RIM_Z,
	seatSpot,
	type Chair,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import { drawPixelText, pixelTextWidth } from "./pixelFont.ts";
import { isBreak, isFinal } from "./scoreBug.ts";
import type { Replay } from "./playback.ts";
import {
	drawTexturedLayers,
	drawTexturedPlane,
	type TexturedPlane,
} from "./planes.ts";
import {
	ANIMS,
	bodyOf,
	bounceAt,
	isGait,
	type AnimName,
	type Body,
} from "./poses.ts";
import {
	drawSprite,
	prepareSprites,
	startFrame,
	type Scratch,
	type SpriteCache,
} from "./sprite.ts";

// The floor is polished: everybody on it shows in it, upside down under
// his feet, this faintly.
const SHINE = 0.24;
const BALL_SHINE_Z = 8;
// And the lit boards standing on it, this strongly where they meet it -
// fading out up the reflection, as much of them as BOARD_SHINE_SHOWN.
const BOARD_SHINE = 0.45;
const BOARD_SHINE_SHOWN = 0.6;
// A board upright on the floor, as it shows in it: upside down under it.
export const inFloor = (p: TexturedPlane): TexturedPlane => ({
	...p,
	origin: { ...p.origin, z: -p.origin.z },
	alongX: { ...p.alongX, z: -p.alongX.z },
	alongY: { ...p.alongY, z: -p.alongY.z },
});
// A board along the court as it shows in the floor: upside down under it,
// plain at its foot, fading away up it - and only as far along as there is
// floor in front of it (the wall along the far side runs on behind the
// stands round the ends, and its shine showed over them). Made once a
// picture.
const shines = new WeakMap<
	HTMLCanvasElement,
	{ plane: TexturedPlane; img: HTMLCanvasElement }
>();
export const shineOf = (
	board: TexturedPlane,
	img: HTMLCanvasElement,
): { plane: TexturedPlane; img: HTMLCanvasElement } => {
	let out = shines.get(img);
	if (!out) {
		const k = img.width / board.w;
		const per = board.alongX.x;
		const at = (x: number) =>
			Math.max(0, Math.min(img.width, ((x - board.origin.x) / per) * k));
		const i0 = per > 0 ? Math.floor(at(FLOOR_EDGE.x0)) : 0;
		const i1 = per > 0 ? Math.ceil(at(FLOOR_EDGE.x1)) : img.width;
		const w = Math.max(1, i1 - i0);
		const cv = document.createElement("canvas");
		cv.width = w;
		cv.height = img.height;
		const g = cv.getContext("2d")!;
		g.drawImage(img, i0, 0, w, img.height, 0, 0, w, img.height);
		g.globalCompositeOperation = "destination-in";
		const fade = g.createLinearGradient(0, 0, 0, img.height);
		fade.addColorStop(0, "rgba(0,0,0,0)");
		fade.addColorStop(1 - BOARD_SHINE_SHOWN, "rgba(0,0,0,0)");
		fade.addColorStop(1 - BOARD_SHINE_SHOWN * 0.45, "rgba(0,0,0,0.3)");
		fade.addColorStop(1, "rgba(0,0,0,1)");
		g.fillStyle = fade;
		g.fillRect(0, 0, w, img.height);
		const u0 = i0 / k;
		const shown = inFloor(board);
		out = {
			plane: {
				...shown,
				origin: {
					x: shown.origin.x + shown.alongX.x * u0,
					y: shown.origin.y + shown.alongX.y * u0,
					z: shown.origin.z + shown.alongX.z * u0,
				},
				w: w / k,
			},
			img: cv,
		};
		shines.set(img, out);
	}
	return out;
};

// ONE FRAME, AS PIXEL ART: the building and the floor laid out in
// perspective, the lines, the shadows, then the players, the two baskets and
// the ball from the back of the picture to the front - all drawn small, on a
// frame a few hundred pixels tall, for the page to blow up without smoothing.

// Everybody on the floor and the ball at time t.
export type Moment = {
	t: number;
	players: PlayerState[];
	ball: BallState;
	// More balls than the one (the warm-ups - see pregame).
	balls?: BallState[];
	// Everyone on the floor in his warm-up top (the warm-ups).
	warm?: boolean;
};

export const momentAt = (
	tl: CourtTimeline,
	t: number,
	roster: { pid: number }[],
	bodyFor: (pid: number) => Body,
	// How fast it is being shown, and the picture's own clock (ms) - see
	// fastLegs.
	look?: { rate: number; wall: number },
): Moment => {
	const players: PlayerState[] = [];
	for (const p of roster) {
		// (The men on the bench are drawn there from elsewhere - see
		// benchStates.)
		if (!shownAt(tl, p.pid, t)) {
			continue;
		}
		const st = evalPlayer(tl, p.pid, t);
		if (st.shown) {
			players.push(
				withBody(
					look && look.rate > FF_FROM ? fastLegs(st, look) : st,
					bodyFor(p.pid),
				),
			);
		}
	}
	const ball = { ...evalBall(tl, t, bodyFor), face: ballFaceAt(tl, t) };
	// (Whoever has his hands near it, not holding it, has them on it.)
	for (const p of players) {
		if (p.pid === ball.holder) {
			p.ballFace = ball.face - p.yaw;
		}
		const dz = ball.z - p.z;
		if (
			p.pid !== ball.holder &&
			dz > BALL_BY_LOW &&
			Math.hypot(ball.x - p.x, ball.y - p.y) < BALL_BY_REACH
		) {
			p.ballBy = {
				x: ball.x - p.x,
				y: ball.y - p.y,
				z: dz,
				...(ball.holder !== undefined ? { held: true } : {}),
			};
		}
	}
	return { t, players, ball };
};
// How near him the ball is (feet, across the floor), and how far up off his
// feet at the least, for his hands to meet it.
const BALL_BY_REACH = 3.2;
const BALL_BY_LOW = 1.2;

// RUN THROUGH QUICK. Many times real speed, a frame of the picture is a good
// part of a stride on from the last - each one a different moment of it, his
// legs flickering from one to another at random. Past a few times real
// speed, a stride's legs go round on the picture's own clock instead - a
// quick, even stride as he goes - eased over from his own between FF_FROM
// and FF_ALL times real speed.
const FF_FROM = 3;
const FF_ALL = 6;
// One stride on the picture's clock (ms).
const FF_STRIDE_MS = 320;
const BALL_CYCLES = new Set<AnimName>([
	"dribble",
	"dribbleWalk",
	"carry",
	"post",
]);
const fastLegs = (
	st: PlayerState,
	look: { rate: number; wall: number },
): PlayerState => {
	if (ANIMS[st.anim].kind !== "cycle") {
		return st;
	}
	const u = Math.min(1, (look.rate - FF_FROM) / (FF_ALL - FF_FROM));
	const w = u * u * (3 - 2 * u);
	// (Each man on his own step.)
	const phase = look.wall / FF_STRIDE_MS + ((st.pid * 0.618034) % 1);
	// (His arms go with his legs once they are mostly on the picture's clock
	// - unless they are busy with the ball.)
	const arms = w >= 0.5 && !BALL_CYCLES.has(st.anim);
	// (His feet down on the game's clock are not the ones down on this one.)
	const lock: PlayerState["lock"] = st.lock && [
		st.lock[0] * (1 - w),
		st.lock[1] * (1 - w),
	];
	return {
		...st,
		...(arms ? { phase } : {}),
		stride: { anim: st.anim, phase, w },
		...(lock ? { lock } : {}),
		// (And the bob of it with them - not him jumping up and down at
		// random.)
		...(isGait(st.anim)
			? { z: st.z + (bounceAt(st.anim, phase) - st.z) * w }
			: {}),
	};
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
	// The seats on the floor behind each basket and in the far corners (see
	// paintCourtside).
	courtside?: Courtside;
	// The lit panel on the faces of the baskets' bases.
	stanchion?: HTMLCanvasElement;
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
	// The home floor's line paint ("" for none: its picture has its own).
	lineColor: string;
	// The floor round the court, past the lines (the court's apron color).
	apron?: string;
	// The warm-up tops the bench wears, by team.
	warmups: [string, string];
	shotClock: string;
	// The score [road, home], for the board over center court - as the
	// score bug has it.
	score?: [number, number];
	arena: ArenaPaint;
	// The crowd on its feet (0..1), and which way its arms are this instant -
	// and, if either end behind a basket is doing something of its own (see
	// crowdAt), that end's.
	crowd: Crowd;
	// The wall clock (ms), for what idles on its own time - the bench - rather
	// than the game's, which races through a fast-forward and jumps at a cut.
	now?: number;
	// How many picture pixels to a pixel of the lettering drawn into it, so a
	// name reads at the same size on every screen.
	textScale: number;
	// What is laid over the picture - the score bug, the line over it - each
	// a box (shares of the picture: across from x0 to x1, down from y0): the
	// tag under the man with the ball goes over his head instead of behind
	// one.
	tagClear?: { x0: number; x1: number; y0: number }[];
	// Off the play: a look round the building, in on a huddle, the starting
	// lineups - or a replay of it, shown clean - nobody's name tag over it.
	away?: boolean;
	// The officials, the coaches and the photographers (see crew.ts), and
	// any of their cameras going off.
	crew?: { st: PlayerState; body: Body; look: Look }[];
	flashes?: Pt3[];
	// Each side's colors [road, home] (main, trim), for the lights swept over
	// the floor at the starting lineups.
	teamColors?: [string, string][];
	// The picture all black (dipped between clips, held black till the men
	// in it can be drawn): none of it seen - only the new pictures of the
	// men in it made, with all the frame's time for them.
	black?: boolean;
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
// team's logo on the front - or its name, in whichever of its colors stands
// out on it - and his name and number on the back in that color; not the
// uniform's own picture.
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
	return crowdLevel(t - fx.t) * (fx.team === 1 ? 1 : 0.4);
};
// Up, a moment on its feet, and down: how much of it, `e` ms after.
const crowdLevel = (e: number): number =>
	e < 260 ? e / 260 : e < 1800 ? 1 : Math.max(0, 1 - (e - 1800) / 900);

// The final horn: who won, and how long ago (ms) - if it has gone.
const finalHorn = (
	tl: CourtTimeline,
	t: number,
): { since: number; winner: Side } | undefined => {
	const beat = beatAt(tl, t);
	if (beat?.type !== "gameOver" || t < beat.actionStart) {
		return undefined;
	}
	const cheer = recentFx(tl, t, ["cheer"], t - beat.actionStart + 1);
	return cheer?.team === undefined
		? undefined
		: { since: t - beat.actionStart, winner: cheer.team };
};
// At the horn, a home win brings the whole building up - arms going, then
// on its feet applauding as the teams shake hands; a road win sits it down
// (but for the road fans' few seconds).
const HORN_UP_MS = 260;
const HORN_WAVE_MS = 5000;
const HORN_SETTLE_MS = 4000;
const HORN_STAY = 0.55;

// THE FANS BEHIND THE BASKET: the visitors at the line, the home fans in
// the stands behind the basket he shoots at are up, arms going, to put him
// off - from his walk to the line until the ball comes down, then sat again.
// (A home shooter they leave be.)
const FT_WAVE_FROM = 400;
const FT_WAVE_UP_MS = 300;
const FT_WAVE_DOWN_MS = 600;
const FT_WAVE_EVERY = 90;
type Stand = { up: number; wave: boolean };
export type Crowd = Stand & { ends?: [Stand | undefined, Stand | undefined] };
const endsAt = (tl: CourtTimeline, t: number, now: number): Crowd["ends"] => {
	const beat = beatAt(tl, t);
	if (beat?.type !== "ft" && beat?.type !== "missFt") {
		return undefined;
	}
	// Its shot: the ball going in, or off the iron, at its line.
	const shot = recentFx(
		tl,
		beat.end,
		["swish", "clank"],
		beat.end - beat.preStart,
	);
	if (shot?.rim !== 0) {
		return undefined;
	}
	const from = beat.preStart + FT_WAVE_FROM;
	const up = Math.max(
		0,
		Math.min(1, (t - from) / FT_WAVE_UP_MS, 1 - (t - shot.t) / FT_WAVE_DOWN_MS),
	);
	if (up <= 0) {
		return undefined;
	}
	const end = { up, wave: Math.floor(now / FT_WAVE_EVERY) % 2 === 0 };
	return [end, undefined];
};

// The crowd at t: on its feet for a big play, arms going - and in a tight
// finish on its feet the whole way, arms going up now and then (`now` is
// the wall clock, for the arms).
export const crowdAt = (tl: CourtTimeline, t: number, now: number): Crowd => {
	const building = buildingAt(tl, t, now);
	const ends = endsAt(tl, t, now);
	return ends ? { ...building, ends } : building;
};
const buildingAt = (tl: CourtTimeline, t: number, now: number): Stand => {
	const horn = finalHorn(tl, t);
	if (horn) {
		const e = horn.since;
		if (horn.winner === 0) {
			// (The road fans, a few seconds, the way they take a big play.)
			return { up: crowdLevel(e) * 0.4, wave: false };
		}
		const level =
			e < HORN_UP_MS
				? e / HORN_UP_MS
				: e < HORN_WAVE_MS
					? 1
					: Math.max(
							HORN_STAY,
							1 - ((1 - HORN_STAY) * (e - HORN_WAVE_MS)) / HORN_SETTLE_MS,
						);
		return {
			up: level,
			wave:
				e < HORN_WAVE_MS ? Math.sin(now / 130) > 0 : Math.sin(now / 520) > 0.55,
		};
	}
	const roar = Math.max(
		crowdUp(tl, t),
		// On their feet for each of the home team's starters called out.
		callAt(tl.intro, t)?.team === 1 ? 1 : 0,
		// Up and going for the camera on them, over a break (see arenaAim).
		crowdShotAt(tl, t),
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
// The look up at the crowd from low in the seats over a break: the people
// it is on are up and waving at it, the way a building's crowd goes when
// it sees itself on the board - not sat still, waiting on the game.
const CROWD_SHOT_UP_MS = 350;
const crowdShotAt = (tl: CourtTimeline, t: number): number => {
	const s = arenaShotAt(tl, t);
	return s?.kind === "crowd"
		? Math.min(1, Math.max(0, (t - s.t0) / CROWD_SHOT_UP_MS))
		: 0;
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
// A break: the bench up and over round the five sat in the huddle, stood
// there with them facing the coach (see huddleRing) - then back to their
// seats as the break ends, sat down again. Up off the bench and back down
// into it over a moment either end (BENCH_EASE); walked there at a good
// clip (RING_PACE), each a beat after the man beside him (RING_NEXT).
const RING_PACE = 6;
const RING_NEXT = 110;
const WALK = ANIMS.walk;
// (Who goes where, the same all through it: the men on the bench but for
// any about to check in, from their seats to the table - the nearest the
// middle as many places as there are of them, each to the one on his side.)
const RING_KEEP = 12_000;
const ringOf = (f: Frame, h: Huddle): Map<number, Pt> => {
	const sat = new Set(h.seats.map((x) => x.pid));
	const men = f.roster.filter(
		(p) =>
			p.team === h.team &&
			!sat.has(p.pid) &&
			!f.tl.checkIns?.some(
				(c) => c.pid === p.pid && c.t1 > h.t0 - 2000 && c.t0 < h.t1 + RING_KEEP,
			),
	);
	const spots = huddleRing(h.team)
		.slice(0, men.length)
		.sort((a, b) => a.x - b.x);
	return new Map(men.map((p, k) => [p.pid, spots[k]!]));
};
const ringState = (
	f: Frame,
	pid: number,
	team: Side,
	seat: Pt,
	t: number,
): PlayerState | undefined => {
	const tl = f.tl;
	const now = f.now ?? t;
	const h = (tl.huddles ?? []).find(
		(x) => x.team === team && t >= x.called && t < x.t1 + RING_KEEP,
	);
	const spot = h && ringOf(f, h).get(pid);
	if (!h || !spot) {
		return undefined;
	}
	const k = Math.abs(spot.x - benchX(team));
	const d = Math.hypot(spot.x - seat.x, spot.y - seat.y);
	const walk = (d / RING_PACE) * 1000;
	const up = h.called + 500 + k * RING_NEXT * 0.5;
	const out = up + BENCH_EASE;
	const there = out + walk;
	const back = h.t1 + k * RING_NEXT * 0.3;
	const sat = back + walk;
	if (t < up || t >= sat + BENCH_EASE) {
		return undefined;
	}
	const base = { pid, team, shown: true, z: 0 };
	const coach = huddleCoach(team);
	const stand = STANDING[Math.abs(pid) % STANDING.length]!;
	const stood = (w: number, from: AnimName, at: Pt, yaw: number) =>
		({
			...base,
			x: at.x,
			y: at.y,
			yaw,
			anim: stand,
			phase: benchPhase(stand, now, pid),
			moving: false,
			...(w > 0 ? { from: { anim: from, phase: 0, w } } : {}),
		}) as PlayerState;
	const walking = (A: Pt, B: Pt, a: number) => {
		const u = Math.min(1, Math.max(0, (t - a) / walk));
		return {
			...base,
			x: A.x + (B.x - A.x) * u,
			y: A.y + (B.y - A.y) * u,
			yaw: Math.atan2(B.y - A.y, B.x - A.x),
			anim: "walk",
			phase: (u * d) / (WALK.kind === "cycle" ? WALK.stride : 4.6),
			moving: true,
		} as PlayerState;
	};
	const toCoach = Math.atan2(coach.y - spot.y, coach.x - spot.x);
	if (t < out) {
		return stood(1 - (t - up) / BENCH_EASE, "sit", seat, Math.PI / 2);
	}
	if (t < there) {
		return walking(seat, spot, out);
	}
	if (t < back) {
		return stood(1 - Math.min(1, (t - there) / 300), "walk", spot, toCoach);
	}
	if (t < sat) {
		return walking(spot, seat, back);
	}
	return {
		...stood(0, "sit", seat, Math.PI / 2),
		anim: "sit",
		phase: benchPhase("sit", now, pid),
		from: { anim: stand, phase: 0, w: 1 - (t - sat) / BENCH_EASE },
	} as PlayerState;
};
// How a bench is at t: up cheering, on its feet, or sat.
export type BenchMood = "up" | "stood" | "sat";
const benchMood = (tl: CourtTimeline, t: number, team: Side): BenchMood => {
	// A big play brings the bench to its feet for a moment - and so does
	// each of its starters called out.
	const roar = recentFx(tl, t, ["roar"], 1800);
	// At the final horn, the winners' bench up and going, then on its feet;
	// the losers' sat where they are.
	const horn = finalHorn(tl, t);
	const won = horn?.winner === team;
	return roar?.team === team ||
		callAt(tl.intro, t)?.team === team ||
		(won && horn.since < HORN_WAVE_MS)
		? "up"
		: won || (!horn && tensionAt(tl, t) >= 1)
			? "stood"
			: "sat";
};
// Up off the bench or sitting back down, a man gets there over a moment
// (BENCH_EASE), not in a frame: as it was at t, and if it changed lately,
// how it was before and how much of that still shows - itself, if it had
// just changed again then (the horn on a big play's heels), eased the same.
const BENCH_EASE = 450;
export type BenchEase = { mood: BenchMood; w: number; from?: BenchEase };
export const benchMoodAt = (
	tl: CourtTimeline,
	t: number,
	team: Side,
	depth = 2,
): { now: BenchMood; from?: BenchEase } => {
	const now = benchMood(tl, t, team);
	if (depth === 0 || benchMood(tl, t - BENCH_EASE, team) === now) {
		return { now };
	}
	// (When it changed: halved down to a few ms.)
	let lo = t - BENCH_EASE;
	let hi = t;
	for (let k = 0; k < 7; k++) {
		const mid = (lo + hi) / 2;
		if (benchMood(tl, mid, team) === now) {
			hi = mid;
		} else {
			lo = mid;
		}
	}
	const w = 1 - (t - hi) / BENCH_EASE;
	const before = benchMoodAt(tl, lo, team, depth - 1);
	return {
		now,
		from: {
			mood: before.now,
			w: w * w * (3 - 2 * w),
			...(before.from ? { from: before.from } : {}),
		},
	};
};
// How much of something a bench shows, by mood, eased the same way.
export const benchShare = (
	m: { now: BenchMood; from?: BenchEase },
	of: (mood: BenchMood) => number,
): number =>
	m.from
		? of(m.now) * (1 - m.from.w) +
			benchShare({ now: m.from.mood, from: m.from.from }, of) * m.from.w
		: of(m.now);
const benchStates = (
	f: Frame,
	onFloor: Set<number>,
): { st: PlayerState; warm: boolean }[] => {
	const out: { st: PlayerState; warm: boolean }[] = [];
	const t = f.moment.t;
	const seat: [number, number] = [0, 0];
	const moods = ([0, 1] as const).map((team) => benchMoodAt(f.tl, t, team));
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
		const ring = ringState(f, p.pid, p.team, at, t);
		if (ring) {
			out.push({ st: ring, warm });
			continue;
		}
		const mood = moods[p.team]!;
		const animOf = (m: BenchMood): AnimName =>
			m === "up"
				? "cheer"
				: m === "stood"
					? STANDING[Math.abs(p.pid) % STANDING.length]!
					: "sit";
		const anim = animOf(mood.now);
		const easeOf = (e: BenchEase): Blend => {
			const a = animOf(e.mood);
			return {
				anim: a,
				phase: benchPhase(a, f.now ?? t, p.pid),
				w: e.w,
				...(e.from ? { from: easeOf(e.from) } : {}),
			};
		};
		// (Following the play, sat or stood watching it - not up cheering.)
		const watching = benchShare(mood, (m) => (m === "up" ? 0 : 1));
		const look =
			watching > 0 ? benchLook(f.tl, t, at, Math.PI / 2) * watching : 0;
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
				...(look !== 0 ? { look } : {}),
				...(mood.from ? { from: easeOf(mood.from) } : {}),
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

// THE HUDDLE'S CHAIRS (see toHuddles): padded, black, the team's color
// across the back - each with its seat at the height of the hips of the man
// who sits in it (under his thighs), put out at the start of the break and
// cleared away at the end of it. The seat and its legs, and the back - each
// drawn in its own place among everybody else, so the back of it is behind
// him sat in it from the front, and over him from behind.
const CHAIR_PAD = "#1c1d22";
const CHAIR_EDGE = "#121316";
const CHAIR_LEG = "#3a3c42";
const CHAIR_FADE = 300;
const chairsAt = (tl: CourtTimeline, t: number) =>
	(tl.huddles ?? []).filter((h) => t >= h.t0 && t < h.t1);
const seatHeight = (b: Body) => b.shin + b.ankleH - b.thighR;
const drawChair = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	c: Chair,
	h: number,
	trim: string,
	part: "seat" | "back",
) => {
	const ux = Math.cos(c.yaw);
	const uy = Math.sin(c.yaw);
	const at = (f: number, s: number, z: number) =>
		project(cam, {
			x: c.x + ux * f - uy * s,
			y: c.y + uy * f + ux * s,
			z,
		});
	const quad = (pts: { x: number; y: number }[], color: string) => {
		ctx.fillStyle = color;
		ctx.beginPath();
		for (const p of pts) {
			ctx.lineTo(p.x, p.y);
		}
		ctx.closePath();
		ctx.fill();
	};
	const line = (
		a: { x: number; y: number; k: number },
		b: { x: number; y: number },
		w: number,
	) => {
		ctx.strokeStyle = CHAIR_LEG;
		ctx.lineWidth = Math.max(1, a.k * w);
		ctx.beginPath();
		ctx.moveTo(a.x, a.y);
		ctx.lineTo(b.x, b.y);
		ctx.stroke();
	};
	const F = 0.62;
	const S = 0.68;
	if (part === "seat") {
		for (const [f, s] of [
			[0.5, 0.56],
			[0.5, -0.56],
			[-0.5, 0.56],
			[-0.5, -0.56],
		] as const) {
			line(at(f, s, 0), at(f, s, h - 0.16), 0.07);
		}
		// The front of the pad, and the top of it.
		quad(
			[at(F, -S, h - 0.18), at(F, S, h - 0.18), at(F, S, h), at(F, -S, h)],
			CHAIR_EDGE,
		);
		quad([at(-F, -S, h), at(F, -S, h), at(F, S, h), at(-F, S, h)], CHAIR_PAD);
		return;
	}
	// Its uprights, and the padded back between them, the color across it.
	const B = -F - 0.04;
	const lean = 0.14;
	for (const s of [S - 0.08, -S + 0.08]) {
		line(at(B, s, h - 0.1), at(B - lean, s, h + 1.38), 0.06);
	}
	const back = (z0: number, z1: number, color: string) =>
		quad(
			[
				at(B - (lean * (z0 - h)) / 1.4, -S, z0),
				at(B - (lean * (z0 - h)) / 1.4, S, z0),
				at(B - (lean * (z1 - h)) / 1.4, S, z1),
				at(B - (lean * (z1 - h)) / 1.4, -S, z1),
			],
			color,
		);
	back(h + 0.5, h + 1.36, CHAIR_PAD);
	back(h + 0.82, h + 1.02, trim);
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
	startFrame(f.sprites);
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.fillStyle = "#07060a";
	ctx.fillRect(0, 0, ctx.canvas.width, ctx.canvas.height);

	const onFloor = new Set(players.map((p) => p.pid));
	const bench = benchStates(f, onFloor);
	// Everybody drawn as a sprite, in what he wears - his new pictures made
	// before anything is drawn, nearest first (see prepareSprites).
	const onCourt = players.map((st) => {
		const look = f.lookFor(st.pid);
		return {
			st,
			body: f.bodyFor(st.pid),
			look: f.moment.warm ? warmupLook(look, f.warmups[st.team]) : look,
		};
	});
	const benched = bench.map(({ st, warm }) => {
		const look = f.lookFor(st.pid);
		return {
			st,
			body: f.bodyFor(st.pid),
			look: warm ? warmupLook(look, f.warmups[st.team]) : look,
		};
	});
	f.sprites.black = f.black;
	prepareSprites(
		f.scratch,
		cam,
		[...onCourt, ...benched, ...(f.crew ?? [])],
		1,
		f.sprites,
	);
	if (f.black) {
		return;
	}

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
			// (This end's own, if it is up to more than the building is.)
			const own = f.crowd.ends?.[side];
			const c = own && own.up > f.crowd.up ? own : f.crowd;
			// (All at once: see drawTexturedLayers.)
			drawTexturedLayers(
				ctx,
				cam,
				END_STANDS[side],
				[
					...(thin && arena.endStandsSparse
						? [{ img: arena.endStandsSparse, alpha: 1 }]
						: []),
					{ img: arena.endStands[0], alpha: fill > 0.01 ? fill : 0 },
					{
						img: arena.endStands[c.wave ? 2 : 1],
						alpha: c.up > 0.01 ? c.up : 0,
					},
				],
				24,
				6,
			);
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
	// The lit boards along the far side - the front of the scorer's table,
	// the wall behind the benches - shine back off the floor in front of
	// them (under the table, the benches and everybody stood there): their
	// light added to the floor's, so where a board is dark the floor is just
	// the floor.
	ctx.globalCompositeOperation = "lighter";
	const table = shineOf(
		TABLE_FRONT,
		arena.boards.table?.[screen] ?? arena.tableFront,
	);
	drawTexturedPlane(ctx, cam, table.plane, table.img, 4, 1, BOARD_SHINE);
	const wall = shineOf(LED_WALL, arena.boards.wall[screen]);
	drawTexturedPlane(ctx, cam, wall.plane, wall.img, 24, 1, BOARD_SHINE);
	ctx.globalCompositeOperation = "source-over";
	// The seats on the floor behind the baskets and in the far corners,
	// stood on it in front of the boards.
	if (arena.courtside) {
		const standOf = (end: Side | undefined) => {
			const own = end === undefined ? undefined : f.crowd.ends?.[end];
			return own && own.up > f.crowd.up ? own : f.crowd;
		};
		drawCourtside(
			ctx,
			cam,
			arena.courtside,
			(end) => standOf(end).up,
			(end) => standOf(end).wave,
			f.now ?? t,
			(home) => {
				// (For a couple of seconds after a basket by their team.)
				const c = recentFx(tl, t, ["cheer"], 2200);
				return c?.team !== undefined && (c.team === 1) === home;
			},
		);
	}
	// The scorer's table, the people behind it hidden from the waist down,
	// their monitors on it.
	const crewAll = f.crew ?? [];
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
	// (None when the floor picture has its own - see CourtStyle.floorLines.)
	if (f.lineColor) {
		drawCourtLines(ctx, cam, f.lineColor);
	}
	// The lights over the far stands, shining back off the floor.
	drawGlints(ctx, cam);
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
	const huddles = chairsAt(tl, t);
	const chairFade = (h: { t0: number; t1: number }) =>
		Math.max(0, Math.min(1, (t - h.t0) / CHAIR_FADE, (h.t1 - t) / CHAIR_FADE));
	for (const h of huddles) {
		ctx.globalAlpha = chairFade(h);
		for (const { at } of h.seats) {
			drawShadow(ctx, cam, at.x, at.y, 0.95, 0.7);
		}
		ctx.globalAlpha = 1;
	}
	const balls = [ball, ...(f.moment.balls ?? [])];
	for (const b of balls) {
		drawShadow(ctx, cam, b.x, b.y, 0.45, Math.max(0, 1 - b.z / 14) * 0.9);
	}

	type Item = { depth: number; draw: () => void };
	const items: Item[] = [];
	for (const { st, body, look } of [...onCourt, ...benched]) {
		items.push({
			depth: depthOf(cam, { x: st.x, y: st.y, z: 3 }),
			draw: () => {
				drawSprite(
					ctx,
					f.scratch,
					cam,
					st,
					body,
					look,
					1,
					f.sprites,
					// (Not a man sat in his chair.)
					st.anim === "sit" ? 0 : SHINE,
				);
			},
		});
	}
	for (const c of crew) {
		items.push({
			depth: depthOf(cam, { x: c.st.x, y: c.st.y, z: 3 }),
			draw: () => {
				drawSprite(
					ctx,
					f.scratch,
					cam,
					c.st,
					c.body,
					c.look,
					1,
					f.sprites,
					SHINE,
				);
			},
		});
	}
	for (const h of huddles) {
		const a = chairFade(h);
		const trim = f.warmups[h.team];
		for (const { pid, at } of h.seats) {
			const height = seatHeight(f.bodyFor(pid));
			const back = {
				x: at.x - Math.cos(at.yaw) * 0.72,
				y: at.y - Math.sin(at.yaw) * 0.72,
			};
			for (const [part, p, nudge] of [
				["seat", at, 0.05],
				["back", back, 0],
			] as const) {
				items.push({
					depth: depthOf(cam, { x: p.x, y: p.y, z: 3 }) + nudge,
					draw: () => {
						ctx.globalAlpha = a;
						drawChair(ctx, cam, at, height, trim, part);
						ctx.globalAlpha = 1;
					},
				});
			}
		}
	}
	// In somebody's hands it is drawn with him (see drawSprite); at the rim,
	// with the rim (in front of its back, behind its front).
	const loose = balls.filter(
		(b) =>
			b.holder === undefined ||
			!players.some((p) => p.pid === b.holder && p.holding),
	);
	const atRim = loose.map((b) => ballAtRim(b));
	// (The period's clock run out: the lights round both boards on.)
	const out = isFinal(tl, t) || isBreak(tl, t);
	for (const side of [0, 1] as const) {
		const rx = side === 0 ? 5.25 : COURT_W - 5.25;
		const fx: HoopFx = {
			swish: fxLevel(tl, t, "swish", 520, side),
			clank: fxLevel(tl, t, "clank", 420, side),
			dunk: fxLevel(tl, t, "dunk", 1100, side),
			big: recentFx(tl, t, ["dunk"], 1100, side)?.big,
			out,
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
					atRim.includes(side)
						? () => {
								loose.forEach((b, i) => {
									if (atRim[i] === side) {
										drawBall(
											ctx,
											cam,
											b,
											b.roll ?? 0,
											false,
											b.rollWay,
											b.face,
										);
									}
								});
							}
						: undefined,
					f.arena.stanchion,
				);
			},
		});
	}
	loose.forEach((b, i) => {
		if (atRim[i] === undefined) {
			items.push({
				depth: depthOf(cam, b) - 0.3,
				draw: () => {
					// (In the floor too, as far under it as it is over it -
					// fainter the higher it is.)
					const shine = SHINE * (1 - b.z / BALL_SHINE_Z);
					if (shine > 0.01) {
						ctx.globalAlpha = shine;
						drawBall(
							ctx,
							cam,
							{ ...b, z: -b.z },
							b.roll ?? 0,
							true,
							b.rollWay,
							b.face,
						);
						ctx.globalAlpha = 1;
					}
					drawBall(ctx, cam, b, b.roll ?? 0, false, b.rollWay, b.face);
				},
			});
		}
	});
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
		const face = f.score ? jumboScored(jumbo, f.score) : jumbo;
		drawJumbo(ctx, cam, face, (plane, img) => {
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

	// Whoever has the ball, named under his feet - following the play, not
	// looking round the building, in on a huddle or showing it again.
	const holder =
		ball.holder === undefined || f.moment.warm || f.away
			? undefined
			: players.find((p) => p.pid === ball.holder);
	if (holder) {
		const look = f.lookFor(holder.pid);
		drawNameTag(ctx, cam, holder, look, f.textScale, f.tagClear);
	}
};

// Over his head: higher than the tallest man's (feet).
const TAG_OVER = 7.6;

const drawNameTag = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	st: PlayerState,
	look: Look,
	scale: number,
	clear: { x0: number; x1: number; y0: number }[] = [],
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
	// Kept in the picture: at its edge, along from under him; down on the
	// near sideline, up over his feet.
	const x = Math.round(
		Math.max(scale, Math.min(cam.viewW - total - scale, feet.x - total / 2)),
	);
	const under = Math.round(
		Math.min(feet.y + Math.max(5 * scale, 0.6 * feet.k), cam.viewH - h - scale),
	);
	// (Behind the score bug or the line over it, over his head instead.)
	const behind = clear.some(
		(b) =>
			x < b.x1 * cam.viewW &&
			x + total > b.x0 * cam.viewW &&
			under + h > b.y0 * cam.viewH,
	);
	const y = behind
		? Math.max(
				scale,
				Math.round(
					project(cam, { x: st.x, y: st.y, z: TAG_OVER }).y - h - 2 * scale,
				),
			)
		: under;
	ctx.fillStyle = "rgba(10, 10, 14, 0.82)";
	// The notch, if there is room below him for it.
	if (y - 3 * scale >= feet.y) {
		ctx.beginPath();
		ctx.moveTo(Math.round(feet.x) - 3 * scale, y);
		ctx.lineTo(Math.round(feet.x), y - 3 * scale);
		ctx.lineTo(Math.round(feet.x) + 3 * scale, y);
		ctx.closePath();
		ctx.fill();
	}
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
	"shootKick",
	"shootSplit",
	"shootPush",
	"shootLeanL",
	"shootLeanR",
	"shootHigh",
	"shootHighKick",
	"shootHighSplit",
	"shootQuick",
	"shootQuickKick",
	"shootQuickSplit",
	"setShot",
	"heave",
	"fade",
	"hook",
	"layup",
	"fingerRoll",
	"floater",
	"powerLayup",
	"scoop",
	"reverseLayup",
	"dunk",
	"dunk1",
	"tomahawk",
	"windmill",
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
	// Free throws: in close on the shooter, the lane and the rim - tighter
	// than the whole floor, with nothing going on out by the sidelines (see
	// courtFit).
	const beat = beatAt(tl, m.t);
	if (beat && (beat.type === "ft" || beat.type === "missFt")) {
		const left = ball.x < COURT_W / 2;
		const width = fit
			? Math.max(FT_MIN_W, fit.min * FT_SHARE)
			: narrow
				? FT_WIDTH_NARROW
				: FT_WIDTH;
		return {
			x: left ? FT_X : COURT_W - FT_X,
			width,
			y: fit ? fit.y(width) : 24,
		};
	}
	// On the ball - but once the game is over, on the players: the
	// handshakes, the bench out onto the floor, not the ball thrown up and
	// left lying under a basket.
	const on = onBall(m.t, beat);
	let minX = Infinity;
	let maxX = -Infinity;
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
	if (n === 0 || on > 0.5) {
		minX = Math.min(minX, ball.x);
		maxX = Math.max(maxX, ball.x);
	}
	const mid = n > 0 ? sum / n : ball.x;
	// Wide enough that the players read small on the floor, the way a
	// cartoon game shows them - in closer on them once it is over.
	const loose = narrow ? 56 : 72;
	const minW = loose * on + (fit?.min ?? loose) * (1 - on);
	const maxW = narrow ? 68 : 88;
	let width = Math.min(maxW, Math.max(minW, maxX - minX + 18));
	let x = (ball.x * 0.62 + mid * 0.38) * on + mid * (1 - on);
	// Ahead of the play: on toward where the ball is going.
	if (on > 0) {
		const d = aheadX(tl, Math.min(tl.end, m.t + LEAD_MS)) - ball.x;
		x += Math.sign(d) * Math.max(0, Math.abs(d) - LEAD_FROM) * LEAD_K * on;
	}
	// A shot going up: push in on the shooter - eased in as he goes up and
	// back out as he comes down, not a jump at either end.
	const shooter = m.players.find((p) => SHOOTING.has(p.anim));
	if (shooter) {
		const u = Math.max(
			0,
			Math.min(1, shooter.phase / 0.3, (1 - shooter.phase) / 0.3),
		);
		const k = u * u * (3 - 2 * u);
		width *= 1 - 0.08 * k;
		x = x * (1 - 0.4 * k) + shooter.x * 0.4 * k;
	}
	width = Math.max(width, fit?.min ?? 0);
	// (The ball kept in the picture while the game is on.)
	const room = width / 2 - 7;
	x += (Math.min(ball.x + room, Math.max(ball.x - room, x)) - x) * on;
	return framed(x, width);
};

// LEADING THE PLAY: an operator shows where the play is going, not only
// where it is - on a break, the floor ahead of the man with the ball and
// the rim he is going to, not him out at the edge of the picture with the
// rest of it still to come into it. The camera's aim goes on toward where
// the ball will be this far (ms of the game) on - the whole game is staged
// before it is shown, so that is known - by this much of the way there.
// (On the camera's own lag behind its aim - see operate - that keeps a man
// pushing the ball up the floor a little behind the middle of the picture.)
// (Only where it is going a good way: the ball moved round the arc, or a
// dribble one way and back, not chased ahead of time.)
const LEAD_MS = 800;
const LEAD_FROM = 6;
const LEAD_K = 0.8;
let leadBodyOf: Body | undefined;
const leadBody = (): Body => (leadBodyOf ??= bodyOf());
// (Where the ball will be, worked out at whole steps of the game this far
// apart and eased between - not afresh every frame, the man with it and his
// hands and all: the camera eases after its aim anyway.)
const LEAD_STEP_MS = 100;
const leadSamples = new WeakMap<CourtTimeline, Map<number, number>>();
const aheadX = (tl: CourtTimeline, t: number): number => {
	let samples = leadSamples.get(tl);
	if (!samples) {
		samples = new Map();
		leadSamples.set(tl, samples);
	}
	const at = (k: number) => {
		let x = samples.get(k);
		if (x === undefined) {
			x = evalBall(tl, Math.min(tl.end, k), leadBody).x;
			samples.set(k, x);
			// (Only the last few: the picture only goes on.)
			if (samples.size > 8) {
				samples.delete(samples.keys().next().value!);
			}
		}
		return x;
	};
	const k0 = Math.floor(t / LEAD_STEP_MS) * LEAD_STEP_MS;
	const u = (t - k0) / LEAD_STEP_MS;
	const x0 = at(k0);
	return u > 0 ? x0 + (at(k0 + LEAD_STEP_MS) - x0) * u : x0;
};

// A free throw's shot: about two thirds as wide as the whole floor's (but
// never narrower than FT_MIN_W feet, and these widths without one), and
// where along the floor from the baseline - the rim and the shooter at the
// line both well in it, and the baseline and the seats behind it in the
// picture past the rim, the way the side camera has a man at the line.
const FT_SHARE = 0.66;
const FT_MIN_W = 30;
const FT_WIDTH = 42;
const FT_WIDTH_NARROW = 34;
const FT_X = 10;

// How much the main camera is on the ball (0 to 1): all the way while the
// game is on, easing off it from the final buzzer - whatever is done with
// the ball then, the players are worth looking at.
const DEAD_MS = 1500;
const onBall = (t: number, beat: Beat | undefined): number => {
	if (beat?.type !== "gameOver") {
		return 1;
	}
	const u = Math.min(1, Math.max(0, (t - beat.actionStart) / DEAD_MS));
	return 1 - u * u * (3 - 2 * u);
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

// THE SLOW-MOTION REPLAY of a dunk, from courtside: on the man who put it
// in and the rim he went to - all of him, his feet near the foot of the
// picture and the iron near its top, as tight as that lets it be - not the
// ball off in a wide picture, half of it the crowd over the rim and the
// players along the bottom with their feet cut off. (Of a three, on him
// till the ball leaves his hands; then on the ball and the rim, as wide as
// both stay in it, closing in on the rim as it gets there.)
//
// A dunk's is seen from two angles, the way a broadcast cuts between its
// cameras: from the side through the run-up, and round under the rim, from
// the corner past the baseline, for the slam - him rising up the lane at
// the camera, the backboard square over him (see BASELINE_RIG).
//
// `aspect` is the picture's width over its height.
const SLAM_FROM = 650;
export const replayRig = (r: Replay | undefined): Rig =>
	r?.slam !== undefined && r.at >= r.slam - SLAM_FROM
		? BASELINE_RIG
		: REPLAY_RIG;
const REPLAY_ROOM = 5;
// The most of the picture's height (floor to iron) and width what has to
// be in it may take up, and how high over the floor the top of the rim is.
const REPLAY_FILL = 0.8;
// Where its middle goes (of 100, down the picture): a little low, for more
// room over the iron than under his feet.
const REPLAY_MIDDLE = 53;
const IRON_Z = RIM_Z + 0.4;
export const replayAim = (
	m: Moment,
	r: Replay | undefined,
	aspect: number,
): Shot => {
	const { ball } = m;
	const rim: Side = r?.rim ?? (ball.x < COURT_W / 2 ? 0 : 1);
	const rimX = rim === 0 ? 5.25 : COURT_W - 5.25;
	const him =
		r?.pid === undefined ? undefined : m.players.find((p) => p.pid === r.pid);
	// The ball, wherever it is - with whoever passes it to him, on its way
	// up to him at the rim - and him while it is his, or once he is on his
	// way in to the rim; not a shooter left behind out at the line once it
	// has gone.
	const onHim =
		him !== undefined &&
		(ball.holder === him.pid ||
			Math.hypot(him.x - ball.x, him.y - ball.y) < 4 ||
			Math.hypot(him.x - rimX, him.y - 25) < 17);
	// What has to be in the picture: the floor under each (at the foot of
	// it), and over each the rim, his head and the ball (at the top).
	const feet: Pt3[] = [
		{ x: rimX, y: 25, z: 0 },
		{ x: ball.x, y: ball.y, z: 0 },
	];
	const tops: Pt3[] = [
		{ x: rimX, y: 25, z: IRON_Z },
		{ ...ball, z: ball.z + 0.6 },
	];
	if (onHim) {
		feet.push({ x: him.x, y: him.y, z: 0 });
		tops.push({ x: him.x, y: him.y, z: him.z + 7.2 });
	}
	let lo = Infinity;
	let hi = -Infinity;
	let ys = 0;
	for (const p of feet) {
		lo = Math.min(lo, p.x);
		hi = Math.max(hi, p.x);
		ys += p.y;
	}
	const rig = replayRig(r);
	const side = rig === REPLAY_RIG;
	let x = (lo + hi) / 2;
	let y = Math.min(44, Math.max(6, ys / feet.length));
	// Never so tight that the floor and the iron can't both be in it.
	let width = Math.max(
		18,
		IRON_Z * aspect * 1.45,
		side ? hi - lo + 2 * REPLAY_ROOM : 0,
	);
	let z = 5;
	// Over, up or down until what has to be in it is in the middle of it,
	// and wider if it doesn't fit (the picture 100 high) - or, round under
	// the rim, as tight as it fits (it starts from no idea how wide that is).
	const w = aspect * 100;
	for (let i = 0; i < (side ? 4 : 6); i++) {
		const cam = makeCamera({ x, width, y, z }, w, 100, rig);
		let left = Infinity;
		let right = -Infinity;
		let top = Infinity;
		let bottom = -Infinity;
		for (const p of [...feet, ...tops]) {
			const q = project(cam, p);
			// (Room for a man's arms either side.)
			left = Math.min(left, q.x - 2 * q.k);
			right = Math.max(right, q.x + 2 * q.k);
			if (p.z === 0) {
				bottom = Math.max(bottom, q.y);
			} else {
				top = Math.min(top, q.y);
			}
		}
		const k = project(cam, { x, y, z }).k;
		// Across the picture: along the floor from the side; from the
		// corner, the way the camera's right is.
		const across = ((left + right) / 2 - w / 2) / k;
		x += across * (side ? 1 : cam.right.x);
		y += side ? 0 : across * cam.right.y;
		z -= ((top + bottom) / 2 - REPLAY_MIDDLE) / k;
		const over = Math.max(
			(right - left) / (w * REPLAY_FILL),
			(bottom - top) / (100 * REPLAY_FILL),
		);
		if (over > 1 || !side) {
			width *= over;
		}
	}
	return { x, width, y, z };
};

// WHAT THE BOARDS SHOW: a shout for the home side's big play, DEFENSE
// (blinking) while the road team has the ball in play, otherwise the team
// and the chants in turn - over a break, the team and the chants, and once
// the game is over, the team alone.
const AMBIENT: BoardScreen[] = ["name", "letsGo", "name", "noise"];
export const boardAt = (tl: CourtTimeline, t: number): BoardScreen => {
	const roar = recentFx(tl, t, ["roar"], 3200);
	if (roar?.team === 1 && roar.what) {
		return roar.what;
	}
	if (recentFx(tl, t, ["block"], 2400)?.team === 1) {
		return "block";
	}
	const beat = beatAt(tl, t);
	if (beat?.type === "gameOver") {
		return "name";
	}
	const stopped =
		arenaShotAt(tl, t) !== undefined ||
		beat?.type === "endOfPeriod" ||
		beat?.type === "timeout";
	if (!stopped && offenseAt(tl, t) === 0) {
		return Math.floor(t / 420) % 2 ? "defense2" : "defense";
	}
	// Crunch time with the ball: the building as loud as it gets.
	if (!stopped && tensionAt(tl, t) >= 0.5) {
		return Math.floor(t / 2600) % 2 ? "letsGo" : "noise";
	}
	return AMBIENT[Math.floor(t / 8000) % AMBIENT.length]!;
};

// THE LOOK ROUND THE BUILDING over a break (see ArenaShot): the whole arena
// from up high, panning slowly; or low in the seats, looking up at the
// crowd and the banners. Over a timeout, in place of the whole arena: in
// close on the huddle of the side with the ball, drawing up what it will
// run - from courtside, pushing slowly in.
const ARENA_RIG: Rig = { back: 60, high: 22, slide: 0.5, upright: false };
const HUDDLE_RIG: Rig = { back: 24, high: 14, slide: 0.92, upright: false };
export const arenaAim = (
	tl: CourtTimeline,
	t: number,
	narrow: boolean,
): { shot: Shot; rig: Rig } | undefined => {
	const s = arenaShotAt(tl, t);
	if (!s) {
		return undefined;
	}
	// (The last, once the game is over, runs on past the end: panned across
	// by then, and held there.)
	const t1 = Math.min(s.t1, tl.end);
	const u = Math.min(1, Math.max(0, (t - s.t0) / Math.max(1, t1 - s.t0)));
	const huddle = s.kind === "wide" ? huddleOf(tl, s.t0) : undefined;
	if (huddle !== undefined) {
		const k = u * u * (3 - 2 * u);
		return {
			shot: {
				x: benchX(huddle) + (huddle === 0 ? 1 : -1) * (1 - k),
				width: (narrow ? 22 : 27) - 4 * k,
				y: HUDDLE_Y,
				z: 3.5,
			},
			rig: HUDDLE_RIG,
		};
	}
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

// The huddle to go in close on for a look at t0, if it is one over a
// timeout: the side the ball goes back to (see the whistle that stopped
// play) - as often as not the one that called it.
const huddleOf = (tl: CourtTimeline, t0: number): Side | undefined => {
	if (beatAt(tl, t0)?.type !== "timeout") {
		return undefined;
	}
	const stop = recentFx(tl, t0, ["whistle"], 20_000);
	return stop?.call === "stop" ? stop.team : undefined;
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
