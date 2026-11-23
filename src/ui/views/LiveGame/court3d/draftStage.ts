import type { Race } from "../../../../common/types.ts";
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
// No two picks quite the same (see DraftTake): where his family's table is,
// who is at it with him, how he takes his name being read, how he goes up
// and what he does up there, and where the cameras are - each picked from
// who he is, so his pick plays the same every time it is watched.

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
// Round each table, the chairs (radians: 0 toward the right of the room,
// a quarter turn on toward the cameras).
export const SEATS = [-150, -90, -30, 30, 150].map((d) => (d * Math.PI) / 180);
export const seatAt = (table: Pt, a: number): Pt & { yaw: number } => ({
	x: table.x + SEAT_R * Math.cos(a),
	y: table.y + SEAT_R * Math.sin(a),
	// (Facing in, toward the middle of the table.)
	yaw: a + Math.PI,
});
// A family's table, and who is sat round it (see DraftRoom): its chairs,
// the ones its own people get up out of drawn as chairs, the rest with
// somebody in them - none at the near side, where the camera on him looks
// in across the table.
export type FamilyTable = {
	table: Pt;
	guests: number[];
	chairs: (Pt & { yaw: number })[];
};
// What the room needs to know of a scene (see draftVenue.ts): the families'
// tables in it, when the one whose name is read is up on its feet, when the
// room claps, and when the smoke goes up.
export type DraftRoom = {
	families: FamilyTable[];
	upAt: (t: number) => boolean;
	clapAt: (t: number) => boolean;
	jetFrom: number;
};
const sameSpot = (a: Pt, b: Pt) => a.x === b.x && a.y === b.y;
export const familyAt = (room: DraftRoom, table: Pt) =>
	room.families.find((f) => sameSpot(f.table, table));
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

// ---- the takes ---------------------------------------------------------------

// HOW HIS PICK GOES: his family's table on the aisle - either side of it,
// in the front three rows; how he takes his name being read (straight up, a
// moment sat there taking it in, his hands on his head, both arms at the
// sky, a fist pumped); who is with him, and which of them comes round to
// hug him; his pace up the aisle; what he does for the cameras once the
// commissioner has had him; and where the cameras are for the room, the
// podium and the stage.
export type DraftReact = "up" | "beat" | "hands" | "sky" | "pump";
export type DraftKin = "mom" | "dad" | "brother" | "none";
export type DraftTake = {
	side: 1 | -1;
	row: -1 | 0 | 1;
	react: DraftReact;
	hugger: "mom" | "dad";
	other: DraftKin;
	pose: AnimName;
	pace: number;
	open: -1 | 0 | 1;
	podiumCam: 1 | -1;
	stageCam: -1 | 0 | 1;
};
const REACTS: DraftReact[] = ["up", "up", "beat", "hands", "sky", "pump"];
const REACT_ANIM: Partial<Record<DraftReact, AnimName>> = {
	hands: "headHands",
	sky: "skyPoint",
	pump: "fistPump",
};
const REACT_MS: Record<DraftReact, number> = {
	up: 0,
	beat: 0,
	hands: 1300,
	sky: 1200,
	pump: 1100,
};
const FAMILIES: ["mom" | "dad", DraftKin][] = [
	["mom", "dad"],
	["mom", "dad"],
	["mom", "brother"],
	["mom", "none"],
	["dad", "mom"],
	["dad", "none"],
];
export const POSES: AnimName[] = [
	"waveBall",
	"point",
	"chestPound",
	"skyPoint",
	"thanksBoth",
	"armsWide",
];
const unit = (n: number): number => {
	let h = Math.imul(n ^ 0x9e3779b9, 0x85ebca6b);
	h ^= h >>> 15;
	h = Math.imul(h, 0x2c1b3c6d);
	h ^= h >>> 12;
	return (h >>> 0) / 4294967296;
};
export const takeOf = (pid: number): DraftTake => {
	const r = (k: number) => unit(Math.imul(pid + 7, 2654435761) ^ (k * 40503));
	const of = <T>(a: readonly T[], k: number): T =>
		a[Math.min(a.length - 1, Math.floor(r(k) * a.length))]!;
	const [hugger, other] = of(FAMILIES, 3);
	return {
		side: of([1, -1] as const, 1),
		row: of([-1, 0, 0, 1] as const, 2),
		react: of(REACTS, 4),
		hugger,
		other,
		pose: of(POSES, 5),
		pace: 4.7 + r(6) * 0.9,
		open: of([-1, 0, 1] as const, 7),
		podiumCam: of([1, -1] as const, 8),
		stageCam: of([-1, 0, 1] as const, 9),
	};
};

// ---- where it all is, for a take -------------------------------------------

// Laid out for a table on the near side of the aisle, second row - and for
// any other, moved back or forward a row and turned over the aisle.
const HOME_TABLE: Pt = { x: 40, y: 20 };
const ROW_STEP = 8;
const HIM_AT = (-50 * Math.PI) / 180;
const KIN_AT = (-100 * Math.PI) / 180;
const OTHER_AT = (-150 * Math.PI) / 180;
const GUESTS_AT = [(160 * Math.PI) / 180];
// Up out of his chair, he steps out from the table toward the aisle, turns to
// his mother, and she comes round to him - and they hug, his face to the
// camera over her shoulder (see HUG_SIDE: each over to his own right of the
// other). On the far side of the aisle, that hug turned over the aisle would
// be left shoulder to left: she is out first, there, and he comes to her
// (see kinFirst).
const HUGGED_AT: Pt = { x: 45, y: 16.2 };
const HUGGED_OVER_AT: Pt = { x: 44.4, y: 15.9 };
const HUG_YAW = (110 * Math.PI) / 180;
// (Round the back of his chair to him.)
const VIA_AT: Pt = { x: 41.6, y: 14.7 };
const AISLE_IN_AT: Pt = { x: AISLE_X - 0.4, y: 14.6 };
// The camera across his table, and where it looks.
const TABLE_CAM_AT: Pt = { x: 40.4, y: 26.5 };
const TABLE_LOOK_AT: Pt = { x: 42, y: 17.4 };

const hugPartner = (at: Pt, yaw: number): Pt => ({
	x: at.x + HUG_APART * Math.cos(yaw) - HUG_SIDE * Math.sin(yaw),
	y: at.y + HUG_APART * Math.sin(yaw) + HUG_SIDE * Math.cos(yaw),
});

export const placesOf = (take: Pick<DraftTake, "side" | "row">) => {
	const dy = take.row * ROW_STEP;
	const mx = (x: number) => (take.side === 1 ? x : 2 * AISLE_X - x);
	const at = (p: Pt): Pt => ({ x: mx(p.x), y: p.y + dy });
	const turned = (a: number) => (take.side === 1 ? a : Math.PI - a);
	const table = at(HOME_TABLE);
	const kinFirst = take.side === -1;
	const hugged = at(kinFirst ? HUGGED_OVER_AT : HUGGED_AT);
	const hugYaw = turned(HUG_YAW);
	return {
		table,
		seat: seatAt(table, turned(HIM_AT)),
		kinSeat: seatAt(table, turned(KIN_AT)),
		otherSeat: seatAt(table, turned(OTHER_AT)),
		guests: GUESTS_AT.map(turned),
		hugged,
		hugYaw,
		kinHug: hugPartner(hugged, hugYaw),
		kinVia: at(VIA_AT),
		aisleIn: at(AISLE_IN_AT),
		tableCam: at(TABLE_CAM_AT),
		tableLook: at(TABLE_LOOK_AT),
		side: take.side,
		kinFirst,
	};
};
export type DraftPlaces = ReturnType<typeof placesOf>;

// His family's table, as the room has it (see DraftRoom).
export const familyOf = (g: DraftPlaces): FamilyTable => ({
	table: g.table,
	guests: g.guests,
	chairs: [g.seat, g.kinSeat, g.otherSeat],
});

// ---- the pick, beat by beat (ms) --------------------------------------------

// His way up: out to the aisle, up it to the steps, up them, and across the
// stage to where he meets the commissioner.
const STEPS_FOOT: Pt = { x: AISLE_X, y: STEPS.y1 + 0.6 };
const STEPS_TOP: Pt = { x: AISLE_X, y: STAGE.y1 - 0.3 };
export const MARK: Pt = { x: AISLE_X + 0.6, y: -3.6 };
// The commissioner: behind the podium, and where he comes to meet him - and
// where he stands beside him for the pictures.
const PODIUM_SPOT: Pt = { x: PODIUM.x, y: PODIUM.y - PODIUM.d / 2 - 1.1 };
// (Him turned three-quarters to the camera in front of the stage.)
const MEET_YAW = (135 * Math.PI) / 180;
const COMM_MEET = hugPartner(MARK, MEET_YAW);
const COMM_PHOTO: Pt = { x: MARK.x - 2.1, y: MARK.y - 0.1 };

// Walking (feet a second): up the steps, across the stage, and everybody
// else's (his own up the aisle is the take's).
const STAIR_PACE = 3.6;
const STAGE_PACE = 4.2;
const OTHERS_PACE = 3.6;
const legs = (a: Pt, b: Pt, pace: number) => (dist(a, b) / pace) * 1000;
const CHAIR_UP_MS = 900;
const POSE_MS = 1700;

// The two moments every pick shares - the broadcast's graphics go by them
// (see DraftBroadcast.tsx): the commissioner up at the podium, and his name.
export const DRAFT_CALL = { podium: 1800, call: 4800 };

export const nightOf = (take: DraftTake, g: DraftPlaces) => {
	// The commissioner reads it out from the podium: the room first, then him.
	const { podium, call } = DRAFT_CALL;
	const react = REACT_MS[take.react];
	const stand = call + (take.react === "beat" ? 1500 : 350);
	const stood = stand + CHAIR_UP_MS;
	const reacted = stood + react;
	// The one who hugs him up out of their chair and round the back of his
	// to him - never across his way: waiting there till he is past (or, the
	// far side of the aisle, there first, and him coming to them).
	const kinUp = g.kinFirst
		? call + 400
		: take.react === "beat"
			? call + 500
			: stand + 150 + Math.max(0, react - 300);
	const kinStood = kinUp + CHAIR_UP_MS;
	const kinVia = kinStood + legs(g.kinSeat, g.kinVia, OTHERS_PACE);
	const kinLeg = legs(g.kinVia, g.kinHug, OTHERS_PACE);
	const myLeg = legs(g.seat, g.hugged, 3.2);
	const walkOff = g.kinFirst ? Math.max(reacted, kinVia + kinLeg) : reacted;
	const stepped = walkOff + myLeg;
	const kinGo = g.kinFirst ? kinVia : Math.max(kinVia, stepped + 200 - kinLeg);
	const kinThere = kinGo + kinLeg;
	const otherUp = call + 800;
	const hug = Math.max(stepped, kinThere) + 100;
	const leave = hug + HUG_MS + 150;
	// (A step or two from the front row: at a walk, still.)
	const atSteps = leave + Math.max(1100, legs(g.hugged, STEPS_FOOT, take.pace));
	const onStage = atSteps + legs(STEPS_FOOT, STEPS_TOP, STAIR_PACE);
	const arrive = onStage + legs(STEPS_TOP, MARK, STAGE_PACE);
	const meet = arrive + 320;
	const photo = meet + HUG_MS;
	const pose = photo + 450;
	const ms = pose + POSE_MS + 1300;
	// The commissioner sets off to be there just before him.
	const commGo = arrive - 250 - legs(PODIUM_SPOT, COMM_MEET, OTHERS_PACE);
	return {
		podium,
		call,
		stand,
		stood,
		reacted,
		walkOff,
		stepped,
		kinUp,
		kinStood,
		kinVia,
		kinGo,
		kinThere,
		otherUp,
		hug,
		leave,
		atSteps,
		onStage,
		arrive,
		meet,
		photo,
		pose,
		commGo,
		ms,
	};
};
export type DraftNight = ReturnType<typeof nightOf>;

// When the room claps: his name read, and him up onto the stage.
const clapOf =
	(N: DraftNight) =>
	(t: number): boolean =>
		(t >= N.call && t < N.call + 3200) ||
		(t >= N.onStage - 300 && t < N.onStage + 3000);
// The smoke: as he comes up the steps.
export const JET_MS = 1900;

// ---- the people ---------------------------------------------------------

// The commissioner and the pick's family: drawn as those who work a game are
// (their faces, their clothes), below any of their pids - his family looking
// like his family.
export const COMMISSIONER = -201;
export const MOTHER = -202;
export const FATHER = -203;
export const BROTHER = -204;
const KIN_PID: Record<Exclude<DraftKin, "none">, number> = {
	mom: MOTHER,
	dad: FATHER,
	brother: BROTHER,
};
export const commissioner = (): CrewMember => ({
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
});
// Dressed for the night as they would be whoever took him - they came not
// knowing (see the room between picks, draftClock.ts).
const DRESSES = [
	"#7a2d8c",
	"#1f4f8f",
	"#a3243b",
	"#1d6b5a",
	"#c9a227",
	"#e8e4dc",
];
const TIES = ["#6b1f2a", "#24365c", "#3c4a2a", "#5a3a6e", "#8a6d1f"];
export const kinOf = (
	who: Exclude<DraftKin, "none">,
	pid: number,
	race: Race | undefined,
	as: number = KIN_PID[who],
): CrewMember => {
	const main = DRESSES[Math.floor(unit(pid * 31 + 5) * DRESSES.length)]!;
	const tie = TIES[Math.floor(unit(pid * 17 + 9) * TIES.length)]!;
	if (who === "mom") {
		return {
			pid: as,
			role: "staff",
			team: 1,
			hgt: 65,
			weight: 140,
			face: faceFor(`draft|mother|${pid}`, true, race),
			dress: dress(main, "#16161a", { sleeves: "long" }),
		};
	}
	if (who === "dad") {
		return {
			pid: as,
			role: "staff",
			team: 1,
			hgt: 74,
			weight: 215,
			face: faceFor(`draft|father|${pid}`, false, race),
			dress: dress("#2b2f38", "#2b2f38", {
				sleeves: "long",
				shirt: "#cfdcef",
				tie,
			}),
		};
	}
	return {
		pid: as,
		role: "staff",
		team: 1,
		hgt: 75,
		weight: 190,
		face: faceFor(`draft|brother|${pid}`, false, race),
		dress: dress("#20232b", "#20232b", {
			sleeves: "long",
			shirt: "#f2f2ee",
			tie: main,
		}),
	};
};

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
export type Beat = {
	t0: number;
	t1: number;
	at: Pt;
	to?: Pt;
	yaw: number;
	anim: AnimName;
};
export const castState = (
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
export type Cam = { at: Pt3; look: Pt3; width: number };
export const shotOf = (c: Cam): ReturnType<Authored["aim"]> => ({
	shot: { x: c.look.x, y: c.look.y, z: c.look.z, width: c.width },
	rig: { back: 0, high: 0, slide: 0, upright: false, at: c.at } as Rig,
});
export const mix3 = (a: Pt3, b: Pt3, u: number): Pt3 => ({
	x: a.x + (b.x - a.x) * u,
	y: a.y + (b.y - a.y) * u,
	z: a.z + (b.z - a.z) * u,
});
// The room, from the back of it: from either corner or down the middle.
export const OPENING: Record<-1 | 0 | 1, [Pt3, Pt3]> = {
	[-1]: [
		{ x: 16, y: 80, z: 25 },
		{ x: 22, y: 75, z: 22 },
	],
	0: [
		{ x: 41, y: 84, z: 27 },
		{ x: 44, y: 78, z: 24 },
	],
	1: [
		{ x: 80, y: 80, z: 25 },
		{ x: 73, y: 75, z: 22 },
	],
};
// The camera in front of the stage, looking up at it: the stage's own
// photographers either side of it.
const stageCamOf = (take: DraftTake): Pt3 => ({
	x: 49 + 2.5 * take.stageCam,
	y: 22,
	z: 8.5,
});

export const draftCameraAt = (
	t: number,
	narrow: boolean,
	him: PlayerState | undefined,
	take: DraftTake,
	g: DraftPlaces,
	N: DraftNight,
): Cam => {
	const k = narrow ? 0.82 : 1;
	if (t < N.podium) {
		// The room: the whole stage, the tables in front of it.
		const u = smooth(t / N.podium);
		const [from, to] = OPENING[take.open];
		return {
			at: mix3(from, to, u),
			look: { x: 47, y: 2, z: 5 },
			width: 74 * k,
		};
	}
	if (t < N.call) {
		// The commissioner at the podium, the team's colors behind him.
		const u = smooth((t - N.podium) / (N.call - N.podium));
		return {
			at: {
				x: PODIUM.x + (take.podiumCam === 1 ? 3.5 : -4),
				y: 12,
				z: 7.8,
			},
			look: { x: PODIUM.x, y: PODIUM.y - 1, z: 7.4 },
			width: (15 - 1.5 * u) * k,
		};
	}
	// The cuts: off his table once he is out in the aisle (or up the steps
	// already, from the front row); off the camera following him up it once
	// he is up them.
	const tableCut = Math.min(N.leave + 900, N.atSteps);
	if (t < tableCut) {
		// In on him at his table, across it: up out of his chair, and the hug.
		const u = smooth((t - N.stand) / (N.hug - N.stand));
		return {
			at: { x: g.tableCam.x, y: g.tableCam.y, z: 6.4 },
			// (His head up off the bar along the bottom of the picture.)
			look: mix3(
				{ x: g.tableLook.x, y: g.tableLook.y, z: 3.9 },
				{ x: g.hugged.x - 0.9 * g.side, y: g.hugged.y, z: 5.1 },
				u,
			),
			width: (11.5 + 1.5 * u) * k,
		};
	}
	if (t < N.onStage) {
		// Behind him up the aisle, the stage ahead of him.
		const st = him ?? { x: AISLE_X, y: 10 };
		return {
			at: { x: AISLE_X + 2 * g.side, y: st.y + 15, z: 10 },
			look: { x: st.x, y: st.y - 8, z: 4.6 },
			width: 22 * k,
		};
	}
	// In front of the stage, looking up at it: him across it, the handshake,
	// the two of them for the pictures.
	const u = smooth((t - N.onStage) / (N.ms - N.onStage));
	return {
		at: stageCamOf(take),
		look: { x: MARK.x - 0.9, y: MARK.y - 0.4, z: 7.6 },
		width: (19 - 3 * u) * k,
	};
};

// The photographers' flashes: round his table as he gets up, and all along
// the front of the stage once he is up on it.
const FLASH_MS = 80;
export const flashesOf =
	(g: DraftPlaces, N: DraftNight) =>
	(t: number): Pt3[] => {
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
							x: g.hugged.x + g.side * (1.5 + a * 3.5),
							y: g.hugged.y + 3 + b * 4,
							z: 5.4 + unit(slot * 5 + k) * 0.9,
						},
			);
		}
		return out;
	};

// Nowhere the cameras look.
export const NO_BALL = { x: -400, y: -400, z: 0.39 };

export const stageDraftNight = (
	pid: number,
	race?: Race,
	take: DraftTake = takeOf(pid),
): Pick<
	Authored,
	"show" | "ms" | "moment" | "aim" | "crew" | "flashes" | "cast" | "castAt"
> & {
	take: DraftTake;
	night: DraftNight;
	places: DraftPlaces;
	room: DraftRoom;
} => {
	const g = placesOf(take);
	const N = nightOf(take, g);
	const SEAT = g.seat;
	const walk = (t0: number, t1: number, from: Pt, to: Pt): Move => ({
		t0,
		t1,
		from: { ...from },
		to: { ...to },
		anim: "walk",
	});
	const ahead = (from: Pt, yaw: number): Pt => ({
		x: from.x + Math.cos(yaw) * 6,
		y: from.y + Math.sin(yaw) * 6,
	});
	const stageCam = stageCamOf(take);
	const STAGE_LOOK: Pt = { x: stageCam.x, y: stageCam.y };
	const TABLE_CAM: Pt = g.tableCam;
	const moves: Move[] = [
		walk(N.walkOff, N.stepped, SEAT, g.hugged),
		walk(N.leave, N.atSteps, g.hugged, STEPS_FOOT),
		walk(N.atSteps, N.onStage, STEPS_FOOT, STEPS_TOP),
		walk(N.onStage, N.arrive, STEPS_TOP, MARK),
	];
	const reactAnim = REACT_ANIM[take.react];
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
			...(reactAnim
				? [{ t0: N.stood, t1: N.reacted, anim: reactAnim, look: TABLE_CAM }]
				: []),
			// (Waiting on them to come out to him.)
			...(N.walkOff > N.reacted
				? [
						{
							t0: N.reacted,
							t1: N.walkOff,
							anim: "easy" as const,
							look: g.kinHug,
						},
					]
				: []),
			{
				t0: N.stepped,
				t1: N.hug,
				anim: "easy",
				look: ahead(g.hugged, g.hugYaw),
			},
			{
				t0: N.hug,
				t1: N.hug + HUG_MS,
				anim: "dapHug",
				look: ahead(g.hugged, g.hugYaw),
			},
			{ t0: N.arrive, t1: N.meet, anim: "easy", look: ahead(MARK, MEET_YAW) },
			{ t0: N.meet, t1: N.photo, anim: "dapHug", look: ahead(MARK, MEET_YAW) },
			{ t0: N.photo, t1: N.pose, anim: "easy", look: STAGE_LOOK },
			{ t0: N.pose, t1: N.pose + POSE_MS, anim: take.pose, look: STAGE_LOOK },
			{
				t0: N.pose + POSE_MS,
				t1: N.ms + 60_000,
				anim: "easy",
				look: STAGE_LOOK,
			},
		],
		arms: [],
		faces: [],
		looks: [
			[0, ahead(SEAT, SEAT.yaw)],
			...(reactAnim ? ([[N.stood, TABLE_CAM]] as [number, Pt][]) : []),
			[N.stepped, ahead(g.hugged, g.hugYaw)],
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

	// Who comes round to hug him, from the chair beside his.
	const kinPid = KIN_PID[take.hugger];
	const kinBeats: Beat[] = [
		{
			t0: 0,
			t1: N.kinUp,
			at: g.kinSeat,
			yaw: g.kinSeat.yaw,
			anim: "chairSit",
		},
		{
			t0: N.kinUp,
			t1: N.kinStood,
			at: g.kinSeat,
			yaw: g.kinSeat.yaw,
			anim: "chairUp",
		},
		{
			t0: N.kinStood,
			t1: N.kinVia,
			at: g.kinSeat,
			to: g.kinVia,
			yaw: 0,
			anim: "walk",
		},
		...(N.kinGo > N.kinVia
			? [
					{
						t0: N.kinVia,
						t1: N.kinGo,
						at: g.kinVia,
						yaw: yawTo(g.kinVia, g.hugged),
						anim: "clap" as const,
					},
				]
			: []),
		{
			t0: N.kinGo,
			t1: N.kinThere,
			at: g.kinVia,
			to: g.kinHug,
			yaw: 0,
			anim: "walk",
		},
		{
			t0: N.kinThere,
			t1: N.hug,
			at: g.kinHug,
			yaw: g.hugYaw + Math.PI,
			anim: "easy",
		},
		{
			t0: N.hug,
			t1: N.hug + HUG_MS,
			at: g.kinHug,
			yaw: g.hugYaw + Math.PI,
			anim: "dapHug",
		},
		{
			t0: N.hug + HUG_MS,
			t1: N.ms + 60_000,
			at: g.kinHug,
			yaw: yawTo(g.kinHug, g.aisleIn),
			anim: "clap",
		},
	];
	// And whoever else is with them: up on their feet, clapping him (his
	// brother more than clapping).
	const otherPid = take.other === "none" ? undefined : KIN_PID[take.other];
	const cheer: AnimName = take.other === "brother" ? "cheer" : "clap";
	const otherBeats: Beat[] = [
		{
			t0: 0,
			t1: N.otherUp,
			at: g.otherSeat,
			yaw: g.otherSeat.yaw,
			anim: "chairSit",
		},
		{
			t0: N.otherUp,
			t1: N.otherUp + CHAIR_UP_MS,
			at: g.otherSeat,
			yaw: g.otherSeat.yaw,
			anim: "chairUp",
		},
		{
			t0: N.otherUp + CHAIR_UP_MS,
			t1: N.leave,
			at: g.otherSeat,
			yaw: yawTo(g.otherSeat, g.hugged),
			anim: cheer,
		},
		{
			t0: N.leave,
			t1: N.ms + 60_000,
			at: g.otherSeat,
			yaw: yawTo(g.otherSeat, g.aisleIn),
			anim: cheer,
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
		aim: (t, narrow, m) =>
			shotOf(draftCameraAt(t, narrow, m.players[0], take, g, N)),
		// Nobody working a game: no officials, coaches or photographers of
		// theirs - only its own people.
		crew: () => false,
		flashes: flashesOf(g, N),
		cast: [
			commissioner(),
			kinOf(take.hugger, pid, race),
			...(take.other === "none" ? [] : [kinOf(take.other, pid, race)]),
		],
		castAt: (t) => [
			castState(COMMISSIONER, commBeats, t, groundAt),
			castState(kinPid, kinBeats, t, groundAt),
			...(otherPid === undefined
				? []
				: [castState(otherPid, otherBeats, t, groundAt)]),
		],
		take,
		night: N,
		places: g,
		room: {
			families: [familyOf(g)],
			upAt: (t) => t >= N.call + 200,
			clapAt: clapOf(N),
			jetFrom: N.onStage - 900,
		},
	};
};
