import { depthOf, project, type Camera } from "../court3d/camera.ts";
import { makeCourtRng } from "../courtRng.ts";
import type { Shot } from "./camera.ts";
import { CREW, officialId, type CrewPart } from "./crew.ts";
import type { Cheer } from "./director.ts";
import {
	dist,
	MID_X,
	MID_Y,
	SIDE_ROOM,
	TUNNEL_H,
	TUNNEL_W,
	TUNNEL_X,
	type Pt,
	type Side,
} from "./geometry.ts";
import type { Move } from "./poses.ts";
import { Man } from "./postgame.ts";
import { LOWER, UPPER } from "./stadium.ts";

// THE TEAMS OUT, before the opening kickoff (overtime has its coin toss: see
// toss.ts): the visitors out already, on their sideline; smoke pouring out
// of the home team's tunnel across the field, and the home team out of it
// through the smoke at a run, four abreast, the stadium on its feet as the
// first of them come - out across the grass and round onto the field, along
// it to in front of their bench, all in a crowd there, jumping, until the
// picture cuts to the kickoff, everybody set for it.

// Smoke out of the tunnel this long (ms) before the first of them, and on
// this long after the last.
const SMOKE_LEAD = 1800;
const SMOKE_TAIL = 1500;
// Out four abreast, this far apart (feet), a row every this long (ms), all
// at this pace (feet a second) - none run up the back of the man ahead.
const ABREAST = 4;
const LANE = 3.5;
const ROW_MS = 320;
const PACE = 17.5;
// Their way: out of the dark this far back in the tunnel, straight out of
// it to this far off the field, then round onto it and along it this far
// in - to the crowd they make in front of their bench, round this spot.
const FROM_IN = 2;
const OUT_TO = -20;
const ALONG_Y = 16;
const MOB: Pt = { x: MID_X - 40, y: 18 };
// The crowd: a man every this far (feet), in an oval this much longer along
// the field than across it - each run in to his place in it from this far
// short of it.
const MOB_GAP = 3;
const MOB_LONG = 1.5;
const MOB_IN = 10;
// Jumping there, turned in, now and then a fist pumped; the picture cut away
// this long after the last of them gets there.
const LEAP_MS = 800;
const PUMP_MS = 1300;
const PUMP = 0.15;
const CUT_AFTER = 2200;
// The crowd up as the first of them come out, and held up until this long
// after the last is in with the rest.
const ROAR_AFTER = 1500;
// The crew stood by on the field, watching: this far along from the middle
// of it, this far toward the near side, this far apart.
const CREW_ALONG = 6;
const CREW_IN = 12;
const CREW_GAP = 3.6;

// A puff of the smoke: from when, from where along the tunnel and how high,
// how fast it rolls out and aside (feet a second), how long it lasts (ms),
// how big it starts and how fast it grows.
export type Puff = {
	t: number;
	x: number;
	z: number;
	vx: number;
	vy: number;
	life: number;
	r0: number;
	grow: number;
};

export type RunOut = {
	team: Side;
	// The men out of the tunnel: each drawn from his first leg on. The crew
	// until the cut.
	men: CrewPart[];
	crew: CrewPart[];
	// The first of them out, the last; the picture's cut to the kick.
	out: number;
	last: number;
	cut: number;
	smoke: Puff[];
	cheer: Cheer;
};

const toward = (a: Pt, b: Pt) => Math.atan2(b.y - a.y, b.x - a.x);

// The way out, a point at a time: straight out of the tunnel, a quarter turn
// round onto the field, along it to the crowd.
const R = ALONG_Y - OUT_TO;
const WAY: Pt[] = [
	{ x: TUNNEL_X, y: -SIDE_ROOM - FROM_IN },
	...Array.from({ length: 9 }, (_, k) => {
		const a = (k / 8) * (Math.PI / 2);
		return { x: TUNNEL_X + R * (1 - Math.cos(a)), y: OUT_TO + R * Math.sin(a) };
	}),
	{ x: MOB.x - MOB_GAP * 4 * MOB_LONG - MOB_IN, y: ALONG_Y },
];
// (Each point of it this far to the right of the way it goes.)
const wayOff = (off: number): Pt[] =>
	WAY.map((p, k) => {
		const a = WAY[Math.max(0, k - 1)]!;
		const b = WAY[Math.min(WAY.length - 1, k + 1)]!;
		const l = dist(a, b) || 1;
		return {
			x: p.x + (off * (b.y - a.y)) / l,
			y: p.y - (off * (b.x - a.x)) / l,
		};
	});
// How far it is along the way to each point, and the point that far along
// it (on past its end to the middle of the crowd).
const SHOT_WAY = [...WAY, MOB];
const AT = SHOT_WAY.reduce<number[]>(
	(a, p, k) => [...a, k === 0 ? 0 : a[k - 1]! + dist(SHOT_WAY[k - 1]!, p)],
	[],
);
const alongWay = (s: number): Pt => {
	const k = AT.findIndex((d) => d >= s);
	if (k <= 0) {
		return k === 0 ? SHOT_WAY[0]! : SHOT_WAY.at(-1)!;
	}
	const u = (s - AT[k - 1]!) / (AT[k]! - AT[k - 1]! || 1);
	const a = SHOT_WAY[k - 1]!;
	const b = SHOT_WAY[k]!;
	return { x: a.x + (b.x - a.x) * u, y: a.y + (b.y - a.y) * u };
};

// The places in the crowd, the far end of it first.
const mobSpots = (n: number, r: () => number): Pt[] => {
	const spots: Pt[] = [];
	const reach = Math.ceil(Math.sqrt(n)) + 2;
	for (let j = -reach; j <= reach; j++) {
		for (let i = -reach * 2; i <= reach * 2; i++) {
			spots.push({
				x: (i + (j % 2 === 0 ? 0 : 0.5)) * MOB_GAP,
				y: j * MOB_GAP * 0.87,
			});
		}
	}
	return spots
		.sort(
			(a, b) =>
				Math.hypot(a.x / MOB_LONG, a.y) - Math.hypot(b.x / MOB_LONG, b.y),
		)
		.slice(0, n)
		.map((p) => ({
			x: MOB.x + p.x + (r() - 0.5) * 0.5,
			y: MOB.y + p.y + (r() - 0.5) * 0.5,
		}))
		.sort((a, b) => b.x - a.x);
};

export const stageRunOut = ({
	team,
	pids,
	t0,
	r,
}: {
	team: Side;
	pids: number[];
	t0: number;
	r: () => number;
}): RunOut => {
	const order = pids
		.map((pid) => ({ pid, k: r() }))
		.sort((a, b) => a.k - b.k)
		.map((p) => p.pid);
	const out = t0 + SMOKE_LEAD;
	const spots = mobSpots(order.length, r);
	const men: { pid: number; man: Man; spot: Pt }[] = [];
	for (let row = 0; row * ABREAST < order.length; row++) {
		const inRow = order.slice(row * ABREAST, (row + 1) * ABREAST);
		const lanes = inRow.map((_, j) => (j - (inRow.length - 1) / 2) * LANE);
		// (The row's places in the crowd matched to its lanes, the far side's
		// to the far side.)
		const places = spots
			.slice(row * ABREAST, row * ABREAST + inRow.length)
			.sort((a, b) => a.y - b.y);
		inRow.forEach((pid, j) => {
			const way = wayOff(lanes[j]! + (r() - 0.5) * 0.6);
			const at = out + row * ROW_MS + r() * 80;
			const pace = PACE;
			const man = new Man(
				team,
				[
					{
						t0: at,
						t1: at,
						from: way[0]!,
						to: way[0]!,
						move: "run",
						yaw: Math.PI / 2,
					},
				],
				pace,
			);
			for (const p of way.slice(1)) {
				man.on(p);
			}
			// (Lanes from the far side across, the far side's lane on the
			// right as they come out.)
			const spot = places[inRow.length - 1 - j]!;
			man.walk(spot);
			men.push({ pid, man, spot });
		});
	}
	const last = Math.max(out, ...men.map((m) => m.man.legs[0]!.t0));
	const there = Math.max(out, ...men.map((m) => m.man.t));
	const cut = there + CUT_AFTER;
	for (const { man, spot } of men) {
		const face = dist(spot, MOB) > 1 ? toward(spot, MOB) : r() * Math.PI * 2;
		while (man.t + LEAP_MS <= cut) {
			const pump = r() < PUMP && man.t + PUMP_MS <= cut;
			man.act(pump ? "fistPump" : "leap", pump ? PUMP_MS : LEAP_MS, face);
		}
		man.hold(cut, "stand", face);
	}

	// The smoke: billows out of the mouth of the tunnel every so often, and a
	// bank of it low along the grass, spreading out either side.
	const smoke: Puff[] = [];
	for (let t = t0; t < last + SMOKE_TAIL; t += 70 + r() * 50) {
		const low = r() < 0.4;
		smoke.push({
			t,
			x: TUNNEL_X + (r() - 0.5) * TUNNEL_W * 0.85,
			z: low ? 0.6 + r() * 1.6 : 1.5 + r() * (TUNNEL_H - 4),
			vx: (r() - 0.5) * (low ? 9 : 4),
			vy: low ? 6 + r() * 4 : 9 + r() * 6,
			life: low ? 5000 + r() * 2200 : 3400 + r() * 2000,
			r0: 2.5 + r() * 2,
			grow: low ? 2.4 + r() * 1.4 : 1.6 + r() * 1.4,
		});
	}

	// The crew stood together on the field, watching them come.
	const crew = CREW.map((o, j) => {
		const off = j - (CREW.length - 1) / 2;
		const p = {
			x: MID_X + CREW_ALONG + off * CREW_GAP + (r() - 0.5) * 0.8,
			y: MID_Y + CREW_IN + Math.abs(off) * 0.8 + (r() - 0.5) * 0.8,
		};
		return {
			pid: officialId(o),
			team: 0 as Side,
			legs: [
				{
					t0,
					t1: cut,
					from: p,
					to: p,
					move: "stand" as Move,
					yaw: toward(p, MOB) + (r() - 0.5) * 0.3,
				},
			],
		};
	});

	return {
		team,
		men: men.map(({ pid, man }) => ({ pid, team, legs: man.legs })),
		crew,
		out,
		last,
		cut,
		smoke,
		cheer: { t: out, side: team, big: true, hold: there - out + ROAR_AFTER },
	};
};

// The officials' play legs from the cut on, after their wait until it.
export const runOutCrew = (ro: RunOut, crew: CrewPart[]) => {
	for (const part of crew) {
		const before = ro.crew.find((m) => m.pid === part.pid);
		if (before) {
			part.legs.unshift(...before.legs);
		}
	}
};

// THE CAMERA ON IT: on the tunnel, the smoke coming out; then after them,
// from the first of them to the last, all of them in the picture; on the
// crowd of them at the end, wider, and up over them to take in their
// sideline and the stands on their feet behind it - as the last of them
// come in, over this far of the way.
const SHOT_LEAST = 74;
const SHOT_ROOM = 46;
const SHOT_UP = 3;
const SHOT_END = 96;
const SHOT_END_UP = -14;
const SHOT_IN = 60;
export const runOutShot = (ro: RunOut, t: number): Shot => {
	const end = AT.at(-1)!;
	const lead = Math.min(end, (PACE * Math.max(0, t - ro.out)) / 1000);
	const tail = Math.min(end, (PACE * Math.max(0, t - ro.last)) / 1000);
	const a = alongWay(lead);
	const b = alongWay(tail);
	const u = Math.max(0, Math.min(1, (tail - (end - SHOT_IN)) / SHOT_IN));
	const e = u * u * (3 - 2 * u);
	return {
		x: (a.x + b.x) / 2,
		y: (a.y + b.y) / 2 + SHOT_UP + (SHOT_END_UP - SHOT_UP) * e,
		width: Math.max(
			SHOT_LEAST + (SHOT_END - SHOT_LEAST) * e,
			SHOT_ROOM + Math.abs(a.x - b.x) * 1.2,
			SHOT_ROOM + Math.abs(a.y - b.y) * 0.8,
		),
	};
};

// THE SHOW OF IT, as the camera sees it: the smoke, each puff rolling out of
// the tunnel and on across the grass, slowing, rising, spreading, thinning
// away - among the men, those nearer drawn over it; fire up out of the
// cannons either side of the tunnel as the first of them come, and again;
// and all round the stands, flashes going off.
const SMOKE = "232,234,240";
const SMOKE_ALPHA = 0.55;
const SMOKE_IN = 250;
const SMOKE_RISE = 1.1;
// The cannons: this far out either side of the tunnel's middle, this far
// off the wall; fired this long (ms) from the first of them out (each burst
// this long, this high - feet - at its height), this far round.
const CANNON_OUT = TUNNEL_W / 2 + 7;
const CANNON_OFF = 3;
const BURSTS = [-150, 1350, 2900];
const BURST_MS = 850;
const FLAME_H = 15;
const FLAME_W = 2.6;
// Flashes: this many going off in the stands every this long (ms), at most -
// the more of them the louder it is.
const FLASHES = 120;
const FLASH_MS = 90;
const FLASH_FT = 0.3;
type Item = { depth: number; draw: () => void };

// How loud it is (0 to 1): up as the first of them come out, held, and down.
const roarOf = (ro: RunOut, t: number) => {
	const e = t - ro.cheer.t;
	const hold = ro.cheer.hold ?? 0;
	return e < 0
		? 0
		: e < 300
			? e / 300
			: e < hold
				? 1
				: Math.max(0, 1 - (e - hold) / 1400);
};

export const runOutGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	ro: RunOut | undefined,
	t: number,
): Item[] => {
	if (!ro || t >= ro.cut) {
		return [];
	}
	const items: Item[] = [];
	const y0 = -SIDE_ROOM;
	for (const p of ro.smoke) {
		const a = t - p.t;
		if (a < 0 || a >= p.life) {
			continue;
		}
		const u = a / p.life;
		const s = a / 1000;
		const at = {
			x: p.x + p.vx * s,
			y: y0 + p.vy * s * (1 - u / 2),
			z: p.z + SMOKE_RISE * s,
		};
		const alpha =
			SMOKE_ALPHA * Math.pow(1 - u, 1.6) * Math.min(1, a / SMOKE_IN);
		const rad = p.r0 + p.grow * s;
		items.push({
			depth: depthOf(cam, { x: at.x, y: at.y, z: 0 }),
			draw: () => {
				const c = project(cam, at);
				const px = rad * c.k;
				if (px < 0.5) {
					return;
				}
				const g = ctx.createRadialGradient(c.x, c.y, 0, c.x, c.y, px);
				g.addColorStop(0, `rgba(${SMOKE},${alpha})`);
				g.addColorStop(0.55, `rgba(${SMOKE},${alpha * 0.55})`);
				g.addColorStop(1, `rgba(${SMOKE},0)`);
				ctx.fillStyle = g;
				ctx.fillRect(c.x - px, c.y - px, px * 2, px * 2);
			},
		});
	}
	for (const side of [-1, 1]) {
		const base = { x: TUNNEL_X + side * CANNON_OUT, y: y0 + CANNON_OFF, z: 0 };
		const burst = BURSTS.map((b) => t - ro.out - b).find(
			(e) => e >= 0 && e < BURST_MS,
		);
		items.push({
			depth: depthOf(cam, base),
			draw: () => {
				drawCannon(ctx, cam, base, burst, t + side * 37);
			},
		});
	}
	const roar = roarOf(ro, t);
	if (roar > 0) {
		items.push({
			depth: Infinity,
			draw: () => {
				drawFlashes(ctx, cam, t, roar);
			},
		});
	}
	return items;
};

// A cannon on the grass - and, firing, the fire up out of it: up fast, held
// flickering, and down; the glow of it on the grass round it.
const drawCannon = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	base: { x: number; y: number; z: number },
	burst: number | undefined,
	flicker: number,
) => {
	const c = project(cam, base);
	const k = c.k;
	ctx.fillStyle = "#1b1c20";
	ctx.fillRect(c.x - 0.9 * k, c.y - 1.1 * k, 1.8 * k, 1.1 * k);
	if (burst === undefined) {
		return;
	}
	const up = Math.min(1, burst / 120) * Math.min(1, (BURST_MS - burst) / 300);
	const h =
		FLAME_H *
		up *
		(0.92 + 0.08 * Math.sin(flicker / 23) * Math.sin(flicker / 61));
	const sway = Math.sin(flicker / 47) * 0.5;
	const foot = c.y - 1.1 * k;
	const tipX = c.x + sway * k;
	const tipY = foot - h * k;
	const w = FLAME_W * k * (0.6 + 0.4 * up);
	ctx.save();
	ctx.globalCompositeOperation = "lighter";
	const glow = ctx.createRadialGradient(c.x, foot, 0, c.x, foot, 9 * k);
	glow.addColorStop(0, `rgba(255,150,40,${0.45 * up})`);
	glow.addColorStop(1, "rgba(255,120,30,0)");
	ctx.fillStyle = glow;
	ctx.fillRect(c.x - 9 * k, foot - 9 * k, 18 * k, 18 * k);
	const g = ctx.createLinearGradient(0, foot, 0, tipY);
	g.addColorStop(0, "rgba(255,250,215,0.95)");
	g.addColorStop(0.3, "rgba(255,196,70,0.9)");
	g.addColorStop(0.68, "rgba(240,96,24,0.7)");
	g.addColorStop(1, "rgba(190,40,10,0)");
	ctx.fillStyle = g;
	ctx.beginPath();
	ctx.moveTo(c.x - w / 2, foot);
	ctx.bezierCurveTo(
		c.x - w,
		foot - h * k * 0.45,
		tipX - w * 0.2,
		tipY + h * k * 0.2,
		tipX,
		tipY,
	);
	ctx.bezierCurveTo(
		tipX + w * 0.2,
		tipY + h * k * 0.2,
		c.x + w,
		foot - h * k * 0.45,
		c.x + w / 2,
		foot,
	);
	ctx.closePath();
	ctx.fill();
	ctx.restore();
};

// Flashes going off about the stands: each a spark of white a moment, here
// and there in the lower bowl and the upper deck, more of them the louder it
// is.
const drawFlashes = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	t: number,
	roar: number,
) => {
	const b = Math.floor(t / FLASH_MS);
	ctx.save();
	ctx.globalCompositeOperation = "lighter";
	for (const k of [b - 1, b]) {
		const r = makeCourtRng(`flash|${k}`);
		const n = Math.round(FLASHES * roar);
		for (let i = 0; i < n; i++) {
			const P = r() < 0.65 ? LOWER : UPPER;
			const at = r();
			const down = r();
			const from = k * FLASH_MS + r() * FLASH_MS;
			const e = t - from;
			if (e < 0 || e >= FLASH_MS) {
				continue;
			}
			const p = {
				x: P.origin.x + P.alongX.x * at * P.w + P.alongY.x * down * P.h,
				y: P.origin.y + P.alongX.y * at * P.w + P.alongY.y * down * P.h,
				z: P.origin.z + P.alongX.z * at * P.w + P.alongY.z * down * P.h,
			};
			const c = project(cam, p);
			if (c.x < -10 || c.x > cam.viewW + 10 || c.y < -10 || c.y > cam.viewH) {
				continue;
			}
			const px = Math.max(1, FLASH_FT * c.k) * 3;
			const a = 1 - e / FLASH_MS;
			const g = ctx.createRadialGradient(c.x, c.y, 0, c.x, c.y, px);
			g.addColorStop(0, `rgba(255,255,255,${a})`);
			g.addColorStop(0.3, `rgba(235,240,255,${a * 0.6})`);
			g.addColorStop(1, "rgba(235,240,255,0)");
			ctx.fillStyle = g;
			ctx.fillRect(c.x - px, c.y - px, px * 2, px * 2);
		}
	}
	ctx.restore();
};
