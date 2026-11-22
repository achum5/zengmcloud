import type { Authored } from "./authored.ts";
import type { Rig } from "./camera.ts";
import { dress, faceFor, type CrewMember } from "./crew.ts";
import type { CourtTimeline, Move, Track } from "./director.ts";
import { evalPlayer, withBody, type PlayerState } from "./evaluate.ts";
import { dist, type Pt, type Pt3 } from "./geometry.ts";
import { ANIMS, HUG_APART, HUG_MS, HUG_SIDE, type AnimName } from "./poses.ts";

// DRAFT NIGHT ON THE FLOOR (see draftVenue.ts for the room itself, and
// DraftBroadcast.tsx for the graphics over it): the building dressed for the
// draft - the floor full of the green room's tables, a stage at the far end
// with the picking team's colors all over its LED wall - and a pick made the
// way the broadcast shows one. The commissioner at the podium; his name read
// out; the cameras on him at his family's table as he gets up and hugs his
// mother; down the aisle and up the steps, the smoke going off either side;
// across the stage to shake the commissioner's hand, and the two of them
// turned to the photographers, him waving.
//
// All of it from the one timeline: the same pick looks the same every time.

// ---- the room (feet: x along the old court, y from the far side toward the
// cameras, z up) -------------------------------------------------------------

// The stage, at the far end of the floor where the far sideline was: its back
// against the LED wall, its front facing the tables.
export const STAGE = { x0: 28, x1: 66, y0: -10, y1: 0, z: 3.5 };
// Its steps, down the middle of its front: so many of them, out to here.
export const STEPS = { x0: 44.5, x1: 49.5, y1: 4.8, n: 4 };
// The podium, stage left: its middle, how wide, deep and tall (above the
// stage).
export const PODIUM = { x: 36, y: -4.6, w: 3.2, d: 2, h: 4.1 };
// The green room: round tables in rows across the floor, an aisle up the
// middle to the steps - each this big, the chairs round it this far out.
export const TABLE_R = 2.5;
export const TABLE_H = 2.5;
export const SEAT_R = 3.3;
export const AISLE_X = (STEPS.x0 + STEPS.x1) / 2;
const ROWS = [12, 20, 28, 36, 44];
const COLS = [4, 13, 22, 31, 40, 54, 63, 72, 81, 90];
export const TABLES: Pt[] = ROWS.flatMap((y) => COLS.map((x) => ({ x, y })));
// His family's: second row, on the aisle.
export const HIS_TABLE: Pt = { x: 40, y: 20 };
// Round each table, the chairs (radians: 0 toward the right of the room,
// a quarter turn on toward the cameras). At his, him, his mother and his
// father (see the cast) and one more - none at the near side, where the
// camera on him looks in across the table.
const SEATS = [-150, -90, -30, 30, 150].map((d) => (d * Math.PI) / 180);
const HIS_SEATS = [160].map((d) => (d * Math.PI) / 180);
const HIM_AT = (-50 * Math.PI) / 180;
const MOM_AT = (-100 * Math.PI) / 180;
const DAD_AT = (-150 * Math.PI) / 180;
export const seatAt = (table: Pt, a: number): Pt & { yaw: number } => ({
	x: table.x + SEAT_R * Math.cos(a),
	y: table.y + SEAT_R * Math.sin(a),
	// (Facing in, toward the middle of the table.)
	yaw: a + Math.PI,
});
export const seatsOf = (table: Pt): number[] =>
	table === HIS_TABLE || (table.x === HIS_TABLE.x && table.y === HIS_TABLE.y)
		? HIS_SEATS
		: SEATS;
// Where the smoke goes up from, either side of the bottom of the steps.
export const JETS: Pt3[] = [
	{ x: STEPS.x0 - 1.6, y: STEPS.y1 - 0.6, z: 0 },
	{ x: STEPS.x1 + 1.6, y: STEPS.y1 - 0.6, z: 0 },
];

// How high the floor is under a spot: the stage's floor, its steps (a ramp
// up them, near enough under a man's feet), the floor.
export const groundAt = (x: number, y: number): number => {
	if (x >= STAGE.x0 && x <= STAGE.x1 && y >= STAGE.y0 && y <= STAGE.y1) {
		return STAGE.z;
	}
	if (x >= STEPS.x0 && x <= STEPS.x1 && y > STAGE.y1 && y <= STEPS.y1) {
		return (STAGE.z * (STEPS.y1 - y)) / (STEPS.y1 - STAGE.y1);
	}
	return 0;
};

// ---- the pick, beat by beat (ms) --------------------------------------------

const SEAT = seatAt(HIS_TABLE, HIM_AT);
const MOM_SEAT = seatAt(HIS_TABLE, MOM_AT);
const DAD_SEAT = seatAt(HIS_TABLE, DAD_AT);
// Up out of his chair, he steps out from the table toward the aisle, turns to
// his mother, and she comes round to him - and they hug, his face to the
// camera over her shoulder (see HUG_SIDE: each over to his own right of the
// other).
const HUGGED: Pt = { x: 45, y: 16.2 };
const HUG_YAW = (110 * Math.PI) / 180;
const MOM_HUG: Pt = {
	x: HUGGED.x + HUG_APART * Math.cos(HUG_YAW) - HUG_SIDE * Math.sin(HUG_YAW),
	y: HUGGED.y + HUG_APART * Math.sin(HUG_YAW) + HUG_SIDE * Math.cos(HUG_YAW),
};
// (Round the back of his chair to him.)
const MOM_VIA: Pt = { x: 41.6, y: 14.7 };
// His way up: out to the aisle, up it to the steps, up them, and across the
// stage to where he meets the commissioner.
const AISLE_IN: Pt = { x: AISLE_X - 0.4, y: 14.6 };
const STEPS_FOOT: Pt = { x: AISLE_X, y: STEPS.y1 + 0.6 };
const STEPS_TOP: Pt = { x: AISLE_X, y: STAGE.y1 - 0.3 };
export const MARK: Pt = { x: AISLE_X + 0.6, y: -3.6 };
// The commissioner: behind the podium, and where he comes to meet him - and
// where he stands beside him for the pictures.
const PODIUM_SPOT: Pt = { x: PODIUM.x, y: PODIUM.y - PODIUM.d / 2 - 1.1 };
// (Him turned three-quarters to the camera in front of the stage.)
const MEET_YAW = (135 * Math.PI) / 180;
const COMM_MEET: Pt = {
	x: MARK.x + HUG_APART * Math.cos(MEET_YAW) - HUG_SIDE * Math.sin(MEET_YAW),
	y: MARK.y + HUG_APART * Math.sin(MEET_YAW) + HUG_SIDE * Math.cos(MEET_YAW),
};
const COMM_PHOTO: Pt = { x: MARK.x - 2.1, y: MARK.y - 0.1 };

// Walking (feet a second): his pace, up the steps, and theirs.
const PACE = 5.2;
const STAIR_PACE = 3.6;
const STAGE_PACE = 4.2;
const OTHERS_PACE = 3.6;
const legs = (a: Pt, b: Pt, pace: number) => (dist(a, b) / pace) * 1000;
const CHAIR_UP_MS = 900;

export const DRAFT_NIGHT = (() => {
	// The commissioner reads it out from the podium: the room first, then him.
	const podium = 1800;
	const call = 4800;
	const stand = call + 350;
	const stood = stand + CHAIR_UP_MS;
	const stepped = stood + legs(SEAT, HUGGED, 3.2);
	const momUp = stand + 150;
	const momStood = momUp + CHAIR_UP_MS;
	const momVia = momStood + legs(MOM_SEAT, MOM_VIA, OTHERS_PACE);
	const momThere = momVia + legs(MOM_VIA, MOM_HUG, OTHERS_PACE);
	const dadUp = stand + 450;
	const hug = Math.max(stepped, momThere) + 100;
	const leave = hug + HUG_MS + 150;
	const atSteps = leave + legs(HUGGED, STEPS_FOOT, PACE);
	const onStage = atSteps + legs(STEPS_FOOT, STEPS_TOP, STAIR_PACE);
	const arrive = onStage + legs(STEPS_TOP, MARK, STAGE_PACE);
	const meet = arrive + 320;
	const photo = meet + HUG_MS;
	const wave = photo + 450;
	const ms = wave + 1700 + 1300;
	// The commissioner sets off to be there just before him.
	const commGo = arrive - 250 - legs(PODIUM_SPOT, COMM_MEET, OTHERS_PACE);
	return {
		podium,
		call,
		stand,
		stood,
		stepped,
		momUp,
		momStood,
		momVia,
		momThere,
		dadUp,
		hug,
		leave,
		atSteps,
		onStage,
		arrive,
		meet,
		photo,
		wave,
		commGo,
		ms,
	};
})();
const N = DRAFT_NIGHT;

// When his table is on its feet, and when the room claps: his name read, and
// him up onto the stage.
export const tableUpAt = (t: number): boolean => t >= N.call + 200;
export const clappingAt = (t: number): boolean =>
	(t >= N.call && t < N.call + 3200) ||
	(t >= N.onStage - 300 && t < N.onStage + 3000);
// The smoke: as he comes up the steps.
export const JET_FROM = N.onStage - 900;
export const JET_MS = 1900;

// ---- the people ---------------------------------------------------------

// The commissioner and the pick's mother and father: drawn as those who work
// a game are (their faces, their clothes), below any of their pids.
export const COMMISSIONER = -201;
export const MOTHER = -202;
export const FATHER = -203;
const castFor = (pid: number, colors: string[] | undefined): CrewMember[] => [
	{
		pid: COMMISSIONER,
		role: "staff",
		team: 1,
		hgt: 72,
		weight: 200,
		face: faceFor("draft|commissioner", false),
		dress: dress("#1b1f2b", "#1b1f2b", {
			sleeves: "long",
			shirt: "#f2f2ee",
			tie: "#24365c",
		}),
	},
	{
		pid: MOTHER,
		role: "staff",
		team: 1,
		hgt: 65,
		weight: 140,
		face: faceFor(`draft|mother|${pid}`, true),
		dress: dress(
			colors?.[0] && colors[0] !== "#000000" ? colors[0] : "#7a2d8c",
			"#16161a",
			{ sleeves: "long" },
		),
	},
	{
		pid: FATHER,
		role: "staff",
		team: 1,
		hgt: 74,
		weight: 215,
		face: faceFor(`draft|father|${pid}`, false),
		dress: dress("#2b2f38", "#2b2f38", {
			sleeves: "long",
			shirt: "#cfdcef",
			tie: colors?.[1] ?? "#6b1f2a",
		}),
	},
];

const yawTo = (from: Pt, to: Pt) => Math.atan2(to.y - from.y, to.x - from.x);
const smooth = (u: number) => {
	const v = Math.min(1, Math.max(0, u));
	return v * v * (3 - 2 * v);
};
const lerp = (a: Pt, b: Pt, u: number): Pt => ({
	x: a.x + (b.x - a.x) * u,
	y: a.y + (b.y - a.y) * u,
});
const turn = (a: number, b: number, u: number) => {
	let d = b - a;
	while (d > Math.PI) {
		d -= Math.PI * 2;
	}
	while (d < -Math.PI) {
		d += Math.PI * 2;
	}
	return a + d * smooth(u);
};
const loopPhase = (anim: AnimName, t: number, seed: number) => {
	const a = ANIMS[anim];
	return a.kind === "loop" ? (t / 1000) * (a.fps / a.n) + seed : t / 1000;
};
// One of them at t: stood (or sat) somewhere doing something, walking from
// one spot to the next, eased from what he was doing before.
type Beat = {
	t0: number;
	t1: number;
	at: Pt;
	to?: Pt;
	yaw: number;
	anim: AnimName;
};
const castState = (
	pid: number,
	beats: Beat[],
	t: number,
	ground: (x: number, y: number) => number,
): PlayerState => {
	let i = beats.findIndex((b) => t < b.t1);
	if (i < 0) {
		i = beats.length - 1;
	}
	const b = beats[i]!;
	const u = Math.min(1, Math.max(0, (t - b.t0) / Math.max(1, b.t1 - b.t0)));
	const walking = b.to !== undefined;
	const at = walking ? lerp(b.at, b.to!, u) : b.at;
	const prev = beats[i - 1];
	const yaw = walking
		? yawTo(b.at, b.to!)
		: prev
			? turn(
					prev.to ? yawTo(prev.at, prev.to) : prev.yaw,
					b.yaw,
					(t - b.t0) / 350,
				)
			: b.yaw;
	const anim = b.anim;
	const phase =
		ANIMS[anim].kind === "act"
			? u
			: walking
				? (t / 1000) * 0.85
				: loopPhase(anim, t, pid * 0.37);
	const since = t - b.t0;
	const st: PlayerState = {
		pid,
		team: 1,
		shown: true,
		x: at.x,
		y: at.y,
		z: ground(at.x, at.y),
		aloft: 0,
		yaw,
		anim,
		phase,
		moving: walking,
	};
	// (Out of what he was doing, over a moment: an act at its end.)
	if (prev && since < 250) {
		return {
			...st,
			from: {
				anim: prev.anim,
				phase:
					ANIMS[prev.anim].kind === "act"
						? 1
						: prev.to
							? (b.t0 / 1000) * 0.85
							: loopPhase(prev.anim, b.t0, pid * 0.37),
				w: 1 - smooth(since / 250),
			},
		};
	}
	return st;
};

// ---- the cameras ------------------------------------------------------------

// Each a camera stood somewhere in the room, looking at a spot, so wide
// across there (feet) - on a squarer picture (a phone), in closer.
type Cam = { at: Pt3; look: Pt3; width: number };
const shotOf = (c: Cam): ReturnType<Authored["aim"]> => ({
	shot: { x: c.look.x, y: c.look.y, z: c.look.z, width: c.width },
	rig: { back: 0, high: 0, slide: 0, upright: false, at: c.at } as Rig,
});
const mix3 = (a: Pt3, b: Pt3, u: number): Pt3 => ({
	x: a.x + (b.x - a.x) * u,
	y: a.y + (b.y - a.y) * u,
	z: a.z + (b.z - a.z) * u,
});
// The camera in front of the stage, looking up at it: the stage's own
// photographers either side of it.
const STAGE_CAM: Pt3 = { x: 49, y: 22, z: 8.5 };
// The cuts: off his table once he is out in the aisle; off the camera
// following him up it once he is up the steps.
const TABLE_CUT = N.leave + 900;
const STAGE_CUT = N.onStage;

export const draftCameraAt = (
	t: number,
	narrow: boolean,
	him: PlayerState | undefined,
): Cam => {
	const k = narrow ? 0.82 : 1;
	if (t < N.podium) {
		// The room: the whole stage, the tables in front of it.
		const u = smooth(t / N.podium);
		return {
			at: mix3({ x: 41, y: 84, z: 27 }, { x: 44, y: 78, z: 24 }, u),
			look: { x: 47, y: 2, z: 5 },
			width: 74 * k,
		};
	}
	if (t < N.call) {
		// The commissioner at the podium, the team's colors behind him.
		const u = smooth((t - N.podium) / (N.call - N.podium));
		return {
			at: { x: PODIUM.x + 3.5, y: 12, z: 7.8 },
			look: { x: PODIUM.x, y: PODIUM.y - 1, z: 7.4 },
			width: (15 - 1.5 * u) * k,
		};
	}
	if (t < TABLE_CUT) {
		// In on him at his table, across it: up out of his chair, and the hug.
		const u = smooth((t - N.stand) / (N.hug - N.stand));
		return {
			at: { x: 40.4, y: 26.5, z: 6.4 },
			// (His head up off the bar along the bottom of the picture.)
			look: mix3(
				{ x: 42, y: 17.4, z: 3.9 },
				{ x: HUGGED.x - 0.9, y: HUGGED.y, z: 5.1 },
				u,
			),
			width: (11.5 + 1.5 * u) * k,
		};
	}
	if (t < STAGE_CUT) {
		// Behind him up the aisle, the stage ahead of him.
		const st = him ?? { x: AISLE_X, y: 10 };
		return {
			at: { x: AISLE_X + 2, y: st.y + 15, z: 10 },
			look: { x: st.x, y: st.y - 8, z: 4.6 },
			width: 22 * k,
		};
	}
	// In front of the stage, looking up at it: him across it, the handshake,
	// the two of them for the pictures.
	const u = smooth((t - STAGE_CUT) / (N.ms - STAGE_CUT));
	return {
		at: STAGE_CAM,
		look: { x: MARK.x - 0.9, y: MARK.y - 0.4, z: 7.6 },
		width: (19 - 3 * u) * k,
	};
};

// The photographers' flashes: round his table as he gets up, and all along
// the front of the stage once he is up on it.
const FLASH_MS = 80;
const unit = (n: number): number => {
	let h = Math.imul(n ^ 0x9e3779b9, 0x85ebca6b);
	h ^= h >>> 15;
	h = Math.imul(h, 0x2c1b3c6d);
	h ^= h >>> 12;
	return (h >>> 0) / 4294967296;
};
export const draftFlashesAt = (t: number): Pt3[] => {
	const slot = Math.floor(t / FLASH_MS);
	const out: Pt3[] = [];
	const table = t >= N.call + 300 && t < N.leave + 800;
	const stage = t >= N.meet && t < N.ms - 400;
	if (!table && !stage) {
		return out;
	}
	const share = stage ? (t >= N.photo ? 0.5 : 0.3) : 0.22;
	for (let k = 0; k < 3; k++) {
		if (unit(slot * 3 + k) >= share) {
			continue;
		}
		const a = unit(slot * 7 + k + 1000);
		const b = unit(slot * 13 + k + 2000);
		out.push(
			stage
				? {
						x: MARK.x - 9 + a * 16,
						y: STEPS.y1 + 1.2 + b * 2.2,
						z: 5.4 + unit(slot * 5 + k) * 1.1,
					}
				: {
						x: HUGGED.x + 1.5 + a * 3.5,
						y: HUGGED.y + 3 + b * 4,
						z: 5.4 + unit(slot * 5 + k) * 0.9,
					},
		);
	}
	return out;
};

// Nowhere the cameras look.
const NO_BALL = { x: -400, y: -400, z: 0.39 };

export const stageDraftNight = (
	pid: number,
	colors?: string[],
): Pick<
	Authored,
	"show" | "ms" | "moment" | "aim" | "crew" | "flashes" | "cast" | "castAt"
> => {
	const walk = (t0: number, from: Pt, to: Pt, pace: number): Move => ({
		t0,
		t1: t0 + legs(from, to, pace),
		from: { ...from },
		to: { ...to },
		anim: "walk",
	});
	const ahead = (from: Pt, yaw: number): Pt => ({
		x: from.x + Math.cos(yaw) * 6,
		y: from.y + Math.sin(yaw) * 6,
	});
	const STAGE_LOOK: Pt = { x: STAGE_CAM.x, y: STAGE_CAM.y };
	const moves: Move[] = [
		walk(N.stood, SEAT, HUGGED, 3.2),
		walk(N.leave, HUGGED, STEPS_FOOT, PACE),
		walk(N.atSteps, STEPS_FOOT, STEPS_TOP, STAIR_PACE),
		walk(N.onStage, STEPS_TOP, MARK, STAGE_PACE),
	];
	const track: Track = {
		pid,
		team: 1,
		start: { x: SEAT.x, y: SEAT.y },
		moves,
		acts: [
			{ t0: 0, t1: N.stand, anim: "chairSit", look: ahead(SEAT, SEAT.yaw) },
			{
				t0: N.stand,
				t1: N.stood,
				anim: "chairUp",
				look: ahead(SEAT, SEAT.yaw),
			},
			{
				t0: N.stepped,
				t1: N.hug,
				anim: "easy",
				look: ahead(HUGGED, HUG_YAW),
			},
			{
				t0: N.hug,
				t1: N.hug + HUG_MS,
				anim: "dapHug",
				look: ahead(HUGGED, HUG_YAW),
			},
			{ t0: N.arrive, t1: N.meet, anim: "easy", look: ahead(MARK, MEET_YAW) },
			{ t0: N.meet, t1: N.photo, anim: "dapHug", look: ahead(MARK, MEET_YAW) },
			{ t0: N.photo, t1: N.wave, anim: "easy", look: STAGE_LOOK },
			{ t0: N.wave, t1: N.wave + 1700, anim: "waveBall", look: STAGE_LOOK },
			{ t0: N.wave + 1700, t1: N.ms + 60_000, anim: "easy", look: STAGE_LOOK },
		],
		arms: [],
		faces: [],
		looks: [
			[0, ahead(SEAT, SEAT.yaw)],
			[N.stepped, ahead(HUGGED, HUG_YAW)],
			[N.arrive, ahead(MARK, MEET_YAW)],
			[N.photo, STAGE_LOOK],
		],
		shown: [[0, true]],
	};
	const tl: CourtTimeline = {
		tracks: new Map([[pid, track]]),
		ball: [],
		// The building on its feet as his name is read, clapping after - and
		// again as he comes up onto the stage.
		fx: [
			{ kind: "roar", t: N.call, team: 1 },
			{ kind: "cheer", t: N.call + 2600, team: 1 },
			{ kind: "roar", t: N.onStage, team: 1 },
		],
		beats: [],
		poss: [[0, 1]],
		cuts: [],
		shots: [],
		tension: [],
		fast: [],
		warmup: true,
		end: N.ms,
	};

	const momBeats: Beat[] = [
		{ t0: 0, t1: N.momUp, at: MOM_SEAT, yaw: MOM_SEAT.yaw, anim: "chairSit" },
		{
			t0: N.momUp,
			t1: N.momStood,
			at: MOM_SEAT,
			yaw: MOM_SEAT.yaw,
			anim: "chairUp",
		},
		{
			t0: N.momStood,
			t1: N.momVia,
			at: MOM_SEAT,
			to: MOM_VIA,
			yaw: 0,
			anim: "walk",
		},
		{
			t0: N.momVia,
			t1: N.momThere,
			at: MOM_VIA,
			to: MOM_HUG,
			yaw: 0,
			anim: "walk",
		},
		{
			t0: N.momThere,
			t1: N.hug,
			at: MOM_HUG,
			yaw: HUG_YAW + Math.PI,
			anim: "easy",
		},
		{
			t0: N.hug,
			t1: N.hug + HUG_MS,
			at: MOM_HUG,
			yaw: HUG_YAW + Math.PI,
			anim: "dapHug",
		},
		{
			t0: N.hug + HUG_MS,
			t1: N.ms + 60_000,
			at: MOM_HUG,
			yaw: yawTo(MOM_HUG, AISLE_IN),
			anim: "clap",
		},
	];
	const dadBeats: Beat[] = [
		{ t0: 0, t1: N.dadUp, at: DAD_SEAT, yaw: DAD_SEAT.yaw, anim: "chairSit" },
		{
			t0: N.dadUp,
			t1: N.dadUp + CHAIR_UP_MS,
			at: DAD_SEAT,
			yaw: DAD_SEAT.yaw,
			anim: "chairUp",
		},
		{
			t0: N.dadUp + CHAIR_UP_MS,
			t1: N.leave,
			at: DAD_SEAT,
			yaw: yawTo(DAD_SEAT, HUGGED),
			anim: "clap",
		},
		{
			t0: N.leave,
			t1: N.ms + 60_000,
			at: DAD_SEAT,
			yaw: yawTo(DAD_SEAT, AISLE_IN),
			anim: "clap",
		},
	];
	const commBeats: Beat[] = [
		{
			t0: 0,
			t1: N.podium,
			at: PODIUM_SPOT,
			yaw: Math.PI / 2,
			anim: "easy",
		},
		{
			t0: N.podium,
			t1: N.call + 600,
			at: PODIUM_SPOT,
			yaw: Math.PI / 2,
			anim: "talk",
		},
		{
			t0: N.call + 600,
			t1: N.commGo,
			at: PODIUM_SPOT,
			yaw: Math.PI / 2 - 0.25,
			anim: "clap",
		},
		{
			t0: N.commGo,
			t1: N.arrive - 250,
			at: PODIUM_SPOT,
			to: COMM_MEET,
			yaw: 0,
			anim: "walk",
		},
		{ t0: N.arrive - 250, t1: N.meet, at: COMM_MEET, yaw: 0, anim: "ready" },
		{ t0: N.meet, t1: N.photo, at: COMM_MEET, yaw: 0, anim: "dapHug" },
		{
			t0: N.photo,
			t1: N.photo + legs(COMM_MEET, COMM_PHOTO, 3),
			at: COMM_MEET,
			to: COMM_PHOTO,
			yaw: 0,
			anim: "walk",
		},
		{
			t0: N.photo + legs(COMM_MEET, COMM_PHOTO, 3),
			t1: N.ms + 60_000,
			at: COMM_PHOTO,
			yaw: yawTo(COMM_PHOTO, STAGE_LOOK),
			anim: "easy",
		},
	];

	return {
		show: { tl, parts: [tl] },
		ms: N.ms,
		moment: (t, bodyFor) => {
			const st = evalPlayer(tl, pid, t);
			return {
				t,
				players: [
					withBody(
						{ ...st, z: st.z + groundAt(st.x, st.y), aloft: 0 },
						bodyFor(pid),
					),
				],
				ball: NO_BALL,
			};
		},
		aim: (t, narrow, m) => shotOf(draftCameraAt(t, narrow, m.players[0])),
		// Nobody working a game: no officials, coaches or photographers of
		// theirs - only its own people.
		crew: () => false,
		flashes: draftFlashesAt,
		cast: castFor(pid, colors),
		castAt: (t) => [
			castState(COMMISSIONER, commBeats, t, groundAt),
			castState(MOTHER, momBeats, t, groundAt),
			castState(FATHER, dadBeats, t, groundAt),
		],
	};
};

// The chairs he and his mother and father get up out of, drawn where they
// sit (see draftVenue).
export const CHAIRS = [SEAT, MOM_SEAT, DAD_SEAT];
