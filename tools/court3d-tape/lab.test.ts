// A TOOL, not a check: THE MOVEMENT LAB. A man's legs and body close up, as
// the 3D court draws him - from his side, the camera keeping pace with him
// at his own height, a grid on the floor and where each of his feet came
// down left marked on it (right red, left blue: a foot that slides while it
// is down drags its mark into a smear). Moments of each kind are found in a
// real game: setting off flat out, pulling up, cutting, sliding with his
// man, backpedaling, jogging, walking.
//
//   LAB_GAME=/dir/real_11.json LAB_DIR=/out [LAB_POSSESSION=1] \
//     npx vitest run --project basketball tools/court3d-tape/lab.test.ts
//   (then, for each clip: ffmpeg -f rawvideo -pix_fmt rgba -s 480x360 \
//     -r 30 -i /out/<clip>.rgba /out/<clip>.mp4)
//
// Writes each clip's frames as raw RGBA (480 x 360, 30 a second) to
// /out/<kind>_<n>.rgba, and /out/clips.json: what each shows - who, when,
// how fast he went, which steps the court drew.
//
// LAB_CLIPS=/other/clips.json films those same moments again (the same
// men, the same moments - to set two versions side by side). LAB_PLAIN=1
// leaves the men their old stepping (no gaits of their own - see
// possession/gait.ts) on the new possession sim's paths. LAB_MEASURE=1
// films nothing: it writes /out/feet.json, how his feet go over the whole
// game (see measure).
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import process from "node:process";
import { test, vi } from "vitest";
import {
	cameraAt,
	project,
	type Camera,
} from "../../src/ui/views/LiveGame/court3d/camera.ts";
import {
	compileCourt,
	type CourtTimeline,
} from "../../src/ui/views/LiveGame/court3d/director.ts";
import {
	bodyPoint,
	evalPlayer,
	fastAt,
	poseOf,
	withBody,
	type PlayerState,
} from "../../src/ui/views/LiveGame/court3d/evaluate.ts";
import {
	kitsFor,
	type Look,
} from "../../src/ui/views/LiveGame/court3d/figure.ts";
import {
	bodyOf,
	skeleton,
	type Body,
} from "../../src/ui/views/LiveGame/court3d/poses.ts";
import { sculpt } from "../../src/ui/views/LiveGame/court3d/sculpt.ts";

const W = 480;
const H = 360;
const FPS = 30;
// The camera: this far off his side (feet), this high, looking at his
// middle - easing round to keep square to the way he goes.
const SIDE = 20;
const EYE = 3.6;
const LOOK_AT = 3.2;
const WIDTH = 15;
// (A foot with its ankle this near where it sits on the floor is down.)
const DOWN = 0.08;
const SAMPLE_MS = 50;

// His height by where he plays (inches): the lab shows a typical man of it.
const HEIGHT: Record<string, number> = {
	PG: 75,
	G: 76,
	SG: 77,
	GF: 78,
	SF: 79,
	F: 80,
	PF: 81,
	FC: 82,
	C: 84,
};

type Sample = {
	t: number;
	x: number;
	y: number;
	v: number;
	heading: number;
	yaw: number;
	anim: string;
};
type Clip = {
	kind: string;
	pid: number;
	t0: number;
	t1: number;
	vMost: number;
	anims: string[];
};

const wrap = (a: number) => a - 2 * Math.PI * Math.round(a / (2 * Math.PI));

const samplesOf = (tl: CourtTimeline, pid: number, t0: number, t1: number) => {
	const out: Sample[] = [];
	let prev: PlayerState | undefined;
	for (let t = t0; t <= t1; t += SAMPLE_MS) {
		const st = evalPlayer(tl, pid, t);
		if (!st.shown || fastAt(tl, t) > 1.001) {
			prev = undefined;
			continue;
		}
		if (prev) {
			const dx = st.x - prev.x;
			const dy = st.y - prev.y;
			out.push({
				t,
				x: st.x,
				y: st.y,
				v: Math.hypot(dx, dy) / (SAMPLE_MS / 1000),
				heading: Math.atan2(dy, dx),
				yaw: st.yaw,
				anim: st.anim,
			});
		}
		prev = st;
	}
	return out;
};

// Moments of each kind in one man's samples: where it happens (ms).
const KINDS: Record<string, (s: Sample[], i: number) => boolean> = {
	// Flat out, from near standing a second before.
	sprint: (s, i) =>
		s[i]!.v > 17 && s.slice(Math.max(0, i - 24), i).some((q) => q.v < 4),
	// From a run to standing within a second and a half.
	stop: (s, i) =>
		s[i]!.v < 1 &&
		s.slice(Math.max(0, i - 30), i).some((q) => q.v > 12) &&
		s.slice(i - 3, i).every((q) => q.v < 4),
	// Turned through more than 70 degrees in half a second, going 8 ft/s
	// and more either side.
	cut: (s, i) => {
		const a = s[i - 5];
		const b = s[i + 5];
		return (
			!!a &&
			!!b &&
			a.v > 8 &&
			b.v > 8 &&
			Math.abs(wrap(b.heading - a.heading)) > (70 * Math.PI) / 180
		);
	},
	slide: (s, i) =>
		s[i]!.anim === "shuffle" &&
		s[i]!.v > 5 &&
		s.slice(i, i + 10).every((q) => q.anim === "shuffle"),
	backpedal: (s, i) =>
		(s[i]!.anim === "back" || s[i]!.anim === "slide") &&
		s[i]!.v > 5 &&
		s.slice(i, i + 10).every((q) => q.anim === s[i]!.anim),
	jog: (s, i) =>
		s[i]!.anim === "jog" &&
		s.slice(i, i + 30).every((q) => q.anim === "jog" && q.v > 6),
	walk: (s, i) =>
		s[i]!.anim === "walk" &&
		s.slice(i, i + 40).every((q) => q.anim === "walk" && q.v > 2.5),
};
const PER_KIND = 4;
// How much of a clip comes before the moment, and after (ms).
const BEFORE = 1600;
const AFTER = 1400;

// The floor: grey wood, a line every two feet.
const FLOOR: [number, number, number] = [196, 160, 112];
const LINE: [number, number, number] = [150, 118, 80];
const SKY: [number, number, number] = [40, 44, 56];

const put = (
	d: Uint8ClampedArray,
	x: number,
	y: number,
	c: [number, number, number],
) => {
	const xi = Math.round(x);
	const yi = Math.round(y);
	if (xi < 0 || yi < 0 || xi >= W || yi >= H) {
		return;
	}
	const o = (yi * W + xi) * 4;
	d[o] = c[0];
	d[o + 1] = c[1];
	d[o + 2] = c[2];
	d[o + 3] = 255;
};
const dot = (
	d: Uint8ClampedArray,
	x: number,
	y: number,
	r: number,
	c: [number, number, number],
) => {
	for (let dy = -r; dy <= r; dy++) {
		for (let dx = -r; dx <= r; dx++) {
			if (dx * dx + dy * dy <= r * r) {
				put(d, x + dx, y + dy, c);
			}
		}
	}
};

const background = (cam: Camera, at: { x: number; y: number }) => {
	const d = new Uint8ClampedArray(W * H * 4);
	// (Floor below the horizon: where a ray from the camera meets z = 0.)
	for (let y = 0; y < H; y++) {
		for (let x = 0; x < W; x++) {
			const o = (y * W + x) * 4;
			const ry = (cam.cy - y) / cam.f;
			const rx = (x - cam.cx) / cam.f;
			const dz = cam.fwd.z + cam.up.z * ry + cam.right.z * rx;
			const c = dz < -1e-3 ? FLOOR : SKY;
			d[o] = c[0];
			d[o + 1] = c[1];
			d[o + 2] = c[2];
			d[o + 3] = 255;
		}
	}
	const x0 = Math.floor(at.x / 2) * 2;
	const y0 = Math.floor(at.y / 2) * 2;
	for (let k = -16; k <= 16; k++) {
		for (let s = -32; s <= 32; s += 0.05) {
			for (const p of [
				{ x: x0 + k * 2, y: y0 + s, z: 0 },
				{ x: x0 + s, y: y0 + k * 2, z: 0 },
			]) {
				const q = project(cam, p);
				if (q.depth > 1) {
					put(d, q.x, q.y, LINE);
				}
			}
		}
	}
	return d;
};

const over = (d: Uint8ClampedArray, img: { data: Uint8ClampedArray }) => {
	const s = img.data;
	for (let i = 0; i < W * H; i++) {
		const a = s[i * 4 + 3]! / 255;
		if (a > 0) {
			for (let c = 0; c < 3; c++) {
				d[i * 4 + c] = d[i * 4 + c]! * (1 - a) + s[i * 4 + c]! * a;
			}
		}
	}
};

const RIGHT: [number, number, number] = [220, 40, 40];
const LEFT: [number, number, number] = [40, 90, 230];

const film = (
	tl: CourtTimeline,
	clip: Clip,
	body: Body,
	look: Look,
	out: string,
) => {
	const frames: Uint8ClampedArray[] = [];
	const marks: { x: number; y: number; c: [number, number, number] }[] = [];
	// The camera keeps square to the way he goes over the whole clip (his
	// side), easing along with him.
	const s = samplesOf(tl, clip.pid, clip.t0, clip.t1);
	let hx = 0;
	let hy = 0;
	for (const q of s) {
		hx += Math.cos(q.heading) * q.v;
		hy += Math.sin(q.heading) * q.v;
	}
	const way = Math.hypot(hx, hy) > 1e-3 ? Math.atan2(hy, hx) : 0;
	// (From the side nearer the main camera: the floor's y toward it.)
	let nx = -Math.sin(way);
	let ny = Math.cos(way);
	if (ny < 0) {
		nx = -nx;
		ny = -ny;
	}
	let cx: number | undefined;
	let cy: number | undefined;
	for (let t = clip.t0; t <= clip.t1; t += 1000 / FPS) {
		const st = evalPlayer(tl, clip.pid, t);
		const me = withBody(st, body);
		cx = cx === undefined ? me.x : cx + (me.x - cx) * 0.25;
		cy = cy === undefined ? me.y : cy + (me.y - cy) * 0.25;
		const cam = cameraAt(
			{ x: cx + nx * SIDE, y: cy + ny * SIDE, z: EYE },
			{ x: cx, y: cy, width: WIDTH, z: LOOK_AT },
			W,
			H,
			false,
			{ narrow: 0.5, wide: 0.5 },
		);
		const d = background(cam, { x: cx, y: cy });
		const sk = skeleton(body, poseOf(me));
		for (const [leg, c] of [
			[sk.legR, RIGHT],
			[sk.legL, LEFT],
		] as const) {
			const f = bodyPoint(me, leg.end);
			if (f.z - body.ankleH < DOWN) {
				marks.push({ x: f.x, y: f.y, c });
			}
		}
		for (const m of marks) {
			const q = project(cam, { x: m.x, y: m.y, z: 0 });
			dot(d, q.x, q.y, 1, m.c);
		}
		const fig = sculpt(cam, me, body, look, 1, 0, 0, W, H);
		over(d, fig.img);
		dot(d, fig.head.x, fig.head.y, Math.round(fig.head.r), [70, 48, 36]);
		frames.push(d);
	}
	const all = new Uint8Array(frames.length * W * H * 4);
	frames.forEach((f, i) => all.set(f, i * W * H * 4));
	writeFileSync(out, all);
	return frames.length;
};

// HOW HIS FEET GO, over the whole game: how many times a second a foot comes
// down at each pace (feet a second, by 2 ft/s), what share of the time a
// foot down is sliding along the floor, and each time he pulls up from a run
// to standing, how many steps it takes him and how long.
const PACES = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22];
const MEASURE_MS = 1000 / 30;
const measure = (
	tl: CourtTimeline,
	players: { pid: number; pos?: string }[],
	end: number,
) => {
	const time = PACES.map(() => 0);
	const falls = PACES.map(() => 0);
	const anims = PACES.map(() => new Map<string, number>());
	let downMs = 0;
	let skateMs = 0;
	const stops: { from: number; steps: number; ms: number }[] = [];
	for (const p of players) {
		if (!tl.tracks.has(p.pid)) {
			continue;
		}
		const body = bodyOf(HEIGHT[p.pos ?? ""] ?? 78);
		let prev:
			| {
					x: number;
					y: number;
					feet: { x: number; y: number; down: boolean }[];
			  }
			| undefined;
		// (Pulling up: from when he was last going 10 ft/s and more.)
		let run: { t: number; v: number; steps: number } | undefined;
		let still = 0;
		for (let t = 0; t <= end; t += MEASURE_MS) {
			const st = evalPlayer(tl, p.pid, t);
			// (Off the floor running is a stride's flight - his feet up, then
			// one down: counted.)
			if (!st.shown || fastAt(tl, t) > 1.001) {
				prev = undefined;
				run = undefined;
				continue;
			}
			const me = withBody(st, body);
			const sk = skeleton(body, poseOf(me));
			const feet = [sk.legR, sk.legL].map((leg) => {
				const f = bodyPoint(me, leg.end);
				return { x: f.x, y: f.y, down: f.z - body.ankleH < DOWN };
			});
			if (prev) {
				const v =
					Math.hypot(me.x - prev.x, me.y - prev.y) / (MEASURE_MS / 1000);
				const bin = Math.min(PACES.length - 1, Math.floor(v / 2));
				time[bin]! += MEASURE_MS;
				const a = anims[bin]!;
				a.set(st.anim, (a.get(st.anim) ?? 0) + 1);
				let came = 0;
				feet.forEach((f, i) => {
					const was = prev!.feet[i]!;
					if (f.down && !was.down) {
						came += 1;
					}
					if (f.down && was.down) {
						downMs += MEASURE_MS;
						// (Sliding: going a foot a second and more along the floor.)
						if (
							Math.hypot(f.x - was.x, f.y - was.y) / (MEASURE_MS / 1000) >
							1
						) {
							skateMs += MEASURE_MS;
						}
					}
				});
				falls[bin]! += came;
				if (v >= 10) {
					run = { t, v, steps: 0 };
					still = 0;
				} else if (run) {
					run.steps += came;
					still = v < 1 ? still + MEASURE_MS : 0;
					if (still >= 300) {
						stops.push({ from: run.v, steps: run.steps, ms: t - 300 - run.t });
						run = undefined;
					} else if (t - run.t > 4000) {
						run = undefined;
					}
				}
			}
			prev = { x: me.x, y: me.y, feet };
		}
	}
	const median = (xs: number[]) =>
		xs.length ? [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]! : 0;
	return {
		cadence: PACES.map((pace, i) => ({
			pace: `${pace - 2}-${pace}`,
			seconds: Math.round(time[i]! / 1000),
			stepsPerSecond:
				time[i]! > 0 ? +(falls[i]! / (time[i]! / 1000)).toFixed(2) : 0,
			anims: Object.fromEntries(
				[...anims[i]!.entries()].sort((a, b) => b[1] - a[1]).slice(0, 4),
			),
		})),
		footSliding: +(skateMs / Math.max(1, downMs)).toFixed(3),
		stops: {
			n: stops.length,
			stepsMedian: median(stops.map((s) => s.steps)),
			msMedian: median(stops.map((s) => s.ms)),
		},
	};
};

test.skipIf(!process.env.LAB_GAME)(
	"movement lab",
	() => {
		vi.stubGlobal(
			"ImageData",
			class {
				data: Uint8ClampedArray;
				width: number;
				height: number;
				constructor(width: number, height: number) {
					this.width = width;
					this.height = height;
					this.data = new Uint8ClampedArray(width * height * 4);
				}
			},
		);
		const dir = process.env.LAB_DIR ?? ".";
		mkdirSync(dir, { recursive: true });
		const { events, players } = JSON.parse(
			readFileSync(process.env.LAB_GAME!, "utf8"),
		);
		const tl = compileCourt({
			events,
			players,
			gid: 1,
			step: true,
			possession: !!process.env.LAB_POSSESSION,
		});
		if (process.env.LAB_PLAIN) {
			delete tl.gaits;
		}
		const kits = kitsFor(undefined, undefined);
		const end = Math.max(
			...[...tl.tracks.values()].flatMap((tr) => tr.moves.map((m) => m.t1)),
		);
		// (LAB_PROBE=pid:t0:t1 - each frame of him, as numbers.)
		if (process.env.LAB_PROBE) {
			const [pid, t0, t1] = process.env.LAB_PROBE.split(":").map(Number);
			const p = (players as { pid: number; pos?: string }[]).find(
				(q) => q.pid === pid,
			)!;
			const body = bodyOf(HEIGHT[p.pos ?? ""] ?? 78);
			let prev: PlayerState | undefined;
			const rows: string[] = [];
			for (let t = t0!; t <= t1!; t += 1000 / FPS) {
				const st = evalPlayer(tl, pid!, t);
				const me = withBody(st, body);
				const sk = skeleton(body, poseOf(me));
				const v = prev ? Math.hypot(st.x - prev.x, st.y - prev.y) * FPS : 0;
				const feet = [sk.legR, sk.legL].map((leg) =>
					(bodyPoint(me, leg.end).z - body.ankleH).toFixed(2),
				);
				const apart = Math.abs(sk.legR.end.f - sk.legL.end.f).toFixed(2);
				rows.push(
					[
						Math.round(t),
						st.anim,
						st.phase.toFixed(2),
						v.toFixed(1),
						st.stepLen?.map((x) => x.toFixed(2)).join("/") ?? "-",
						feet.join("/"),
						apart,
						sk.pelvis.u.toFixed(2),
					].join("\t"),
				);
				prev = st;
			}
			writeFileSync(`${dir}/probe.tsv`, rows.join("\n"));
			return;
		}
		if (process.env.LAB_MEASURE) {
			writeFileSync(
				`${dir}/feet.json`,
				JSON.stringify(measure(tl, players, end), null, 1),
			);
			return;
		}
		const clips: Clip[] = process.env.LAB_CLIPS
			? JSON.parse(readFileSync(process.env.LAB_CLIPS, "utf8"))
			: [];
		const found: Record<string, number> = {};
		for (const p of process.env.LAB_CLIPS
			? []
			: (players as { pid: number; team: 0 | 1; pos?: string }[])) {
			if (!tl.tracks.has(p.pid)) {
				continue;
			}
			const s = samplesOf(tl, p.pid, 0, end);
			for (const [kind, hit] of Object.entries(KINDS)) {
				let last = -Infinity;
				for (let i = 30; i < s.length - 30; i++) {
					if ((found[kind] ?? 0) >= PER_KIND) {
						break;
					}
					// (One of a kind each man, and each well apart in time.)
					if (s[i]!.t - last < 60_000 || !hit(s, i)) {
						continue;
					}
					// (All of it live, at real speed.)
					const t0 = s[i]!.t - BEFORE;
					const t1 = s[i]!.t + AFTER;
					const span = s.filter((q) => q.t >= t0 && q.t <= t1);
					if (span.length < ((t1 - t0) / SAMPLE_MS) * 0.95) {
						continue;
					}
					last = s[i]!.t;
					found[kind] = (found[kind] ?? 0) + 1;
					clips.push({
						kind,
						pid: p.pid,
						t0,
						t1,
						vMost: Math.max(...span.map((q) => q.v)),
						anims: [...new Set(span.map((q) => q.anim))],
					});
					break;
				}
			}
		}
		const byKind: Record<string, number> = {};
		const written: (Clip & { file: string; frames: number; hgt: number })[] =
			[];
		for (const clip of clips) {
			const n = (byKind[clip.kind] = (byKind[clip.kind] ?? 0) + 1);
			const p = (players as { pid: number; team: 0 | 1; pos?: string }[]).find(
				(q) => q.pid === clip.pid,
			)!;
			const hgt = HEIGHT[p.pos ?? ""] ?? 78;
			const look: Look = {
				kit: kits[p.team]!,
				skin: "#8d5524",
				hair: "#1f1612",
				jerseyNumber: String(clip.pid % 100),
				name: "",
				lastName: "",
				wordmark: "",
			};
			const file = `${clip.kind}_${n}`;
			const frames = film(tl, clip, bodyOf(hgt), look, `${dir}/${file}.rgba`);
			written.push({
				kind: clip.kind,
				pid: clip.pid,
				t0: clip.t0,
				t1: clip.t1,
				vMost: clip.vMost,
				anims: clip.anims,
				file,
				frames,
				hgt,
			});
		}
		writeFileSync(`${dir}/clips.json`, JSON.stringify(written, null, 1));
	},
	3_600_000,
);
