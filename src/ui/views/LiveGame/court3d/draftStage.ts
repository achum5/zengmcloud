import type { Race } from "../../../../common/types.ts";
import type { Authored } from "./authored.ts";
import type { Rig } from "./camera.ts";
import { dress, faceFor, type CrewMember } from "./crew.ts";
import type { CourtTimeline, Move, Track } from "./director.ts";
import { evalPlayer, withBody, type PlayerState } from "./evaluate.ts";
import { dist, type Pt, type Pt3 } from "./geometry.ts";
import {
	ANIMS,
	EMBRACE,
	HUG_APART,
	HUG_MS,
	HUG_SIDE,
	type AnimName,
} from "./poses.ts";

// DRAFT NIGHT ON THE FLOOR (see draftVenue.ts for the room itself, and
// DraftBroadcast.tsx for the graphics over it): the building dressed for the
// draft - the floor full of the green room's tables, a stage at the far end
// with the picking team's colors all over its LED wall - and a pick made the
// way the broadcast shows one. The commissioner at the podium; his name read
// out; the cameras on him at his family's table from the stage side - the
// photographers and the room behind him - as he gets up and hugs his family,
// one and then the other; down the aisle toward the stage, the smoke going
// off either side, his cap handed him at the foot of the steps and put on;
// up them and into the commissioner's arms, seen over the commissioner's
// shoulder; and the two of them turned to the photographers, him waving.
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
	{ x: STEPS.x0 - 2.6, y: STEPS.y1 - 0.6, z: 0 },
	{ x: STEPS.x1 + 2.6, y: STEPS.y1 - 0.6, z: 0 },
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
// in the front three rows; how he takes his name being read (see MOODS); who
// is with him, and which of them comes round to hug him; his pace up the
// aisle; what he does for the cameras once the commissioner has had him;
// and where the cameras are for the room, the podium and the stage.
export type DraftKin = "mom" | "dad" | "brother" | "none";
export type DraftMood =
	| "composed"
	| "overcome"
	| "overjoyed"
	| "firedUp"
	| "faith"
	| "stunned"
	| "relieved";
export type DraftTake = {
	side: 1 | -1;
	row: -1 | 0 | 1;
	// How it hits him: what he does in his chair as his name is read, and up
	// on his feet after (see SITS, UPS).
	mood: DraftMood;
	sit: AnimName;
	up: AnimName;
	hugger: "mom" | "dad";
	other: DraftKin;
	pose: AnimName;
	pace: number;
	open: -1 | 0 | 1;
	podiumCam: 1 | -1;
	stageCam: -1 | 0 | 1;
	// Which side of the steps the cap is handed him, the camera on him
	// coming up the aisle there too.
	footCam: 1 | -1;
};
// HOW IT HITS HIM, as it hits the men in the green room on the broadcasts:
// most of them composed - a smile, a nod, the jacket done up; plenty
// overcome, bent over in tears; some beside themselves, or fired up, or
// thanking God; some stunned, some just relieved it is over. (So many in
// twenty.)
const MOODS: [DraftMood, number][] = [
	["composed", 5],
	["overcome", 4],
	["overjoyed", 3],
	["firedUp", 2],
	["faith", 2],
	["stunned", 2],
	["relieved", 2],
];
// What he does with it in his chair...
export const SITS: Record<DraftMood, AnimName[]> = {
	composed: ["sitSmile", "sitStunned", "sitHeart"],
	overcome: ["sitBow", "sitFaceHands", "sitHandEyes"],
	overjoyed: ["sitJolt", "sitClap", "sitLaugh"],
	firedUp: ["sitFist", "sitJolt"],
	faith: ["sitPray", "sitHeart"],
	stunned: ["sitHandMouth", "sitStunned", "sitHandEyes"],
	relieved: ["sitLeanBack", "sitSmile", "sitLaugh"],
};
// ...and up on his feet.
export const UPS: Record<DraftMood, AnimName[]> = {
	composed: ["upButton", "upWave", "upClap", "upHeart"],
	overcome: ["upWipeEyes", "upFaceHands", "upHeart", "upButton"],
	overjoyed: ["upArmsOut", "upClap", "upHandsHead", "upSky"],
	firedUp: ["upFistPump", "upRoar", "upChestThump"],
	faith: ["upSky", "upPointUp", "upPray"],
	stunned: ["upHandsHead", "upHandMouth", "upButton"],
	relieved: ["upExhale", "upHandsHead", "upButton"],
};
// (Grinning after - all but those overcome.)
const GRINS: Record<DraftMood, boolean> = {
	composed: true,
	overcome: false,
	overjoyed: true,
	firedUp: true,
	faith: true,
	stunned: true,
	relieved: true,
};
// How long each takes (ms).
const BEAT_MS: Partial<Record<AnimName, number>> = {
	sitSmile: 1300,
	sitBow: 2300,
	sitFaceHands: 2200,
	sitHandMouth: 1800,
	sitHandEyes: 2000,
	sitPray: 2200,
	sitLeanBack: 1700,
	sitClap: 1300,
	sitFist: 1100,
	sitStunned: 1800,
	sitLaugh: 1600,
	sitHeart: 1600,
	sitJolt: 1100,
	upButton: 1500,
	upHandsHead: 1600,
	upHandMouth: 1600,
	upWipeEyes: 1800,
	upSky: 1500,
	upPointUp: 1600,
	upFistPump: 1200,
	upRoar: 1400,
	upChestThump: 1400,
	upHeart: 1600,
	upArmsOut: 1300,
	upClap: 1300,
	upPray: 1700,
	upExhale: 1500,
	upWave: 1400,
	upFaceHands: 1900,
};
const beatOf = (anim: AnimName) => BEAT_MS[anim] ?? 1400;
const FAMILIES: ["mom" | "dad", DraftKin][] = [
	["mom", "dad"],
	["mom", "dad"],
	["mom", "brother"],
	["mom", "none"],
	["dad", "mom"],
	["dad", "none"],
];
// For the cameras on the stage.
export const POSES: AnimName[] = [
	"upWave",
	"stagePoint",
	"upHeart",
	"upSky",
	"stageFist",
	"upClap",
	"stageThanks",
	"upArmsOut",
	"upChestThump",
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
	const mood = of(
		MOODS.flatMap(([m, n]) => Array<DraftMood>(n).fill(m)),
		4,
	);
	return {
		side: of([1, -1] as const, 1),
		row: of([-1, 0, 0, 1] as const, 2),
		mood,
		sit: of(SITS[mood], 11),
		up: of(UPS[mood], 12),
		hugger,
		other,
		pose: of(POSES, 5),
		pace: 4.7 + r(6) * 0.9,
		open: of([-1, 0, 1] as const, 7),
		podiumCam: of([1, -1] as const, 8),
		stageCam: of([-1, 0, 1] as const, 9),
		footCam: of([1, -1] as const, 10),
	};
};

// ---- where it all is, for a take -------------------------------------------

// Laid out in this frame for one table - and for his own turned end for end
// about its row (him on the far side of it from the stage, facing it, the
// room behind him), moved back or forward a row, and over the aisle for the
// far side of it.
const HOME_TABLE: Pt = { x: 40, y: 20 };
const ROW_STEP = 8;
const HIM_AT = (-50 * Math.PI) / 180;
const KIN_AT = (-100 * Math.PI) / 180;
const OTHER_AT = (-150 * Math.PI) / 180;
const GUESTS_AT = [(160 * Math.PI) / 180];
// Up out of his chair, he steps out from the table toward the aisle, turns to
// his mother, and she comes round to him - and they hug, his face to the
// camera over her shoulder (see embraceOf). Turned end for end only (this
// side of the aisle), she is out first, and he comes to her (see
// kinFirst).
const HUGGED_AT: Pt = { x: 45, y: 16.2 };
const HUGGED_OVER_AT: Pt = { x: 44.4, y: 15.9 };
const HUG_YAW = (110 * Math.PI) / 180;
// (Round the back of his chair to him.)
const VIA_AT: Pt = { x: 41.6, y: 14.7 };
const AISLE_IN_AT: Pt = { x: AISLE_X - 0.4, y: 14.6 };
// Whoever else is with them comes round behind the chairs to him after, for
// a hug of their own (behind him too, this side of the aisle); and the one
// who hugged him first steps back out of his way to the steps, if they are
// in it.
const OTHER_VIA_AT: Pt[] = [
	{ x: 37.2, y: 15.3 },
	{ x: 42.2, y: 15 },
	{ x: 44.8, y: 14 },
];
const OTHER_VIA_OVER_AT: Pt[] = [
	{ x: 37.2, y: 15.3 },
	{ x: 42.2, y: 15.1 },
];
const KIN_ASIDE_OVER_AT: Pt = { x: 43.2, y: 18.1 };
const OTHER_IN = 1;
const KIN_ASIDE = 1.6;
// The camera across his table from the stage side, and where it looks.
const TABLE_CAM_AT: Pt = { x: 40.4, y: 26.5 };
const TABLE_LOOK_AT: Pt = { x: 42, y: 17.4 };
// The photographers crowded in behind him, in the aisle a row back (across
// it from the aisle's middle, and back from his table's) - far enough back
// for a camera between them and him as he turns to hug the next of them.
const PRESS_AT: Pt[] = [
	{ x: -2.6, y: 11.4 },
	{ x: 0, y: 11.8 },
	{ x: 2.6, y: 11.4 },
	{ x: -3.8, y: 13.4 },
	{ x: -1.3, y: 13.8 },
	{ x: 1.3, y: 13.6 },
	{ x: 3.8, y: 13.3 },
];

const hugPartner = (at: Pt, yaw: number): Pt => ({
	x: at.x + HUG_APART * Math.cos(yaw) - HUG_SIDE * Math.sin(yaw),
	y: at.y + HUG_APART * Math.sin(yaw) + HUG_SIDE * Math.cos(yaw),
});
// Held in a long hug (see poses.ts HOLD): his mother a good deal shorter
// than him, him bent over her - his father or his brother much of a height,
// chest to chest; and where they stand for it, facing him - so far out in
// front of him, and over to his left.
// (His side of a hug, grinning.)
const grinned = (anim: AnimName, grin: boolean): AnimName =>
	!grin
		? anim
		: anim === "embrace"
			? "embraceGrin"
			: anim === "embraceMan"
				? "embraceManGrin"
				: anim;
export const embraceOf = (
	who: DraftKin,
): { him: AnimName; them: AnimName; apart: number; left: number } =>
	who === "mom" || who === "none"
		? { him: "embrace", them: "embraceUp", apart: EMBRACE.apart, left: 0 }
		: {
				him: "embraceMan",
				them: "embraceManUp",
				apart: EMBRACE.manApart,
				left: EMBRACE.manSide,
			};
const hugAt = (at: Pt, yaw: number, who: DraftKin, right = 0): Pt => {
	const e = embraceOf(who);
	// (His left: see evaluate.ts bodyPoint.)
	const left = e.left - right;
	return {
		x: at.x + e.apart * Math.cos(yaw) + left * Math.sin(yaw),
		y: at.y + e.apart * Math.sin(yaw) - left * Math.cos(yaw),
	};
};
// (Out first, she waits a step over from there - toward the aisle, out of
// his way - for him to come out to her, and steps in to him as he gets
// there.)
const KIN_WAIT = 1.2;
const STEP_IN_MS = 450;

export const placesOf = (
	take: Pick<DraftTake, "side" | "row" | "hugger" | "other">,
) => {
	const dy = take.row * ROW_STEP;
	const at = (p: Pt): Pt => ({
		x: take.side === 1 ? p.x : 2 * AISLE_X - p.x,
		y: 2 * HOME_TABLE.y + dy - p.y,
	});
	const turned = (a: number) => (take.side === 1 ? -a : Math.PI + a);
	const table = at(HOME_TABLE);
	const kinFirst = take.side === 1;
	const hugged = at(kinFirst ? HUGGED_OVER_AT : HUGGED_AT);
	const hugYaw = turned(HUG_YAW);
	// (Each of them coming in to him head on, the last step or two: not
	// round past his back, nor across in front of him.)
	const kinHug = hugAt(hugged, hugYaw, take.hugger);
	const otherHug = hugAt(hugged, hugYaw + Math.PI, take.other);
	const otherIn = {
		x: otherHug.x - Math.cos(hugYaw) * OTHER_IN,
		y: otherHug.y - Math.sin(hugYaw) * OTHER_IN,
	};
	return {
		table,
		seat: seatAt(table, turned(HIM_AT)),
		kinSeat: seatAt(table, turned(KIN_AT)),
		otherSeat: seatAt(table, turned(OTHER_AT)),
		guests: GUESTS_AT.map(turned),
		hugged,
		hugYaw,
		kinHug,
		kinWait: kinFirst
			? hugAt(hugged, hugYaw, take.hugger, KIN_WAIT)
			: {
					x: kinHug.x + Math.cos(hugYaw) * OTHER_IN,
					y: kinHug.y + Math.sin(hugYaw) * OTHER_IN,
				},
		kinVia: at(VIA_AT),
		aisleIn: at(AISLE_IN_AT),
		otherVia: [
			...(kinFirst ? OTHER_VIA_OVER_AT : OTHER_VIA_AT).map(at),
			otherIn,
		],
		otherHug,
		kinAside: kinFirst
			? at(KIN_ASIDE_OVER_AT)
			: hugAt(hugged, hugYaw, take.hugger, KIN_ASIDE),
		press: PRESS_AT.map((p) => ({
			x: AISLE_X + p.x * take.side,
			y: table.y + p.y,
		})),
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

// His way up: out to the aisle, up it to the steps - the cap handed him at
// the foot of them - up them, and across the stage to where he meets the
// commissioner.
const STEPS_TOP: Pt = { x: AISLE_X, y: STAGE.y1 - 0.3 };
export const MARK: Pt = { x: AISLE_X + 0.6, y: -3.6 };
// Where the cap is handed him, at the foot of the steps - by the one with
// it, beside them (the take's side of them).
const capAt = (take: Pick<DraftTake, "footCam">): Pt => ({
	x: AISLE_X + 0.5 * take.footCam,
	y: STEPS.y1 + 0.5,
});
const staffAt = (take: Pick<DraftTake, "footCam">): Pt => ({
	x: AISLE_X + 2.7 * take.footCam,
	y: STEPS.y1 + 0.2,
});
// The commissioner: behind the podium, and where he comes to meet him -
// between him and the back of the stage, him facing it - and where he
// stands beside him for the pictures.
const PODIUM_SPOT: Pt = { x: PODIUM.x, y: PODIUM.y - PODIUM.d / 2 - 1.1 };
const MEET_YAW = (-130 * Math.PI) / 180;
const COMM_MEET = hugPartner(MARK, MEET_YAW);
const COMM_PHOTO: Pt = { x: MARK.x - 2.1, y: MARK.y - 0.1 };
// (Round behind him to there.)
const COMM_VIA: Pt = { x: MARK.x - 1.6, y: MARK.y - 1.9 };
// The pictures done, him off at the back of the stage - round behind the
// podium's backdrop - and the commissioner back to the podium past the
// front of it, for the next.
const EXIT_VIA: Pt = { x: PODIUM.x + 7.6, y: STAGE.y0 + 1 };
const EXIT: Pt = { x: PODIUM.x - 2.5, y: STAGE.y0 + 0.85 };
const COMM_BACK: Pt = { x: PODIUM.x + 4, y: PODIUM_SPOT.y };
// The photographers in front of the stage, either side of the steps and
// clear of the smoke.
const PIT: Pt[] = [
	{ x: 34.6, y: 6.4 },
	{ x: 37.2, y: 7.3 },
	{ x: 39.8, y: 6.6 },
	{ x: 54.2, y: 6.7 },
	{ x: 56.8, y: 7.4 },
	{ x: 59.4, y: 6.5 },
];

// Walking (feet a second): up the steps, across the stage, and everybody
// else's (his own up the aisle is the take's).
const STAIR_PACE = 3.6;
const STAGE_PACE = 4.2;
const OTHERS_PACE = 3.6;
const legs = (a: Pt, b: Pt, pace: number) => (dist(a, b) / pace) * 1000;
const path = (pts: Pt[], pace: number) => {
	let ms = 0;
	for (let i = 1; i < pts.length; i++) {
		ms += legs(pts[i - 1]!, pts[i]!, pace);
	}
	return ms;
};
const CHAIR_UP_MS = 900;
// (His name read, a moment before it hits him.)
const SIT_AFTER_MS = 200;
const POSE_MS = 1700;
// (And off, at a walk.)
const OFF_PACE = 3.4;
// The hugs at the table: held, the first longer; and the turn between them.
const EMBRACE_MS = 3800;
const EMBRACE2_MS = 3200;
const TURN_MS = 450;
// (The one who hugged him first, a step or two back out of his way.)
const ASIDE_MS = 900;
// The cap: handed him, and on.
const HAND_MS = 700;
const CAP_MS = 1200;

// The two moments every pick shares - the broadcast's graphics go by them
// (see DraftBroadcast.tsx): the commissioner up at the podium, and his name.
export const DRAFT_CALL = { podium: 2400, call: 7400 };

export const nightOf = (take: DraftTake, g: DraftPlaces) => {
	// The commissioner reads it out from the podium: the room first, then him.
	const { podium, call } = DRAFT_CALL;
	// A moment in his chair with it, and up; and a moment on his feet.
	const sat = call + SIT_AFTER_MS;
	const stand = sat + beatOf(take.sit);
	const stood = stand + CHAIR_UP_MS;
	const react = beatOf(take.up);
	const reacted = stood + react;
	// The one who hugs him up out of their chair and round the back of his
	// to him - never across his way: waiting there till he is past (or, this
	// side of the aisle, there first, and him coming to them).
	// (Not before he is getting up himself: not stood in front of him while
	// it hits him.)
	const kinUp = g.kinFirst
		? Math.max(call + 400, stand - 400)
		: stand + 150 + Math.max(0, react - 300);
	const kinStood = kinUp + CHAIR_UP_MS;
	const kinVia = kinStood + legs(g.kinSeat, g.kinVia, OTHERS_PACE);
	const kinLeg = legs(g.kinVia, g.kinWait, OTHERS_PACE);
	const inLeg = legs(g.kinWait, g.kinHug, OTHERS_PACE);
	const myLeg = legs(g.seat, g.hugged, 3.2);
	const walkOff = g.kinFirst ? Math.max(reacted, kinVia + kinLeg) : reacted;
	const stepped = walkOff + myLeg;
	// (Across his way to it halfway along hers: not till he is there.)
	const kinGo = g.kinFirst
		? kinVia
		: Math.max(kinVia, stepped + 200 - kinLeg - inLeg, stepped - kinLeg / 2);
	const kinThere = kinGo + kinLeg;
	const stepIn = g.kinFirst ? stepped - 150 : kinThere;
	const inThere = stepIn + (g.kinFirst ? STEP_IN_MS : inLeg);
	const otherUp = call + 800;
	const hug = Math.max(stepped, inThere) + 100;
	const hugEnd = hug + EMBRACE_MS;
	// Whoever else is with them round behind the table to him as he lets
	// go, and he turns to them.
	const two = take.other !== "none";
	const otherLeg = two
		? path([g.otherSeat, ...g.otherVia, g.otherHug], OTHERS_PACE)
		: 0;
	const hug2 = two
		? Math.max(hugEnd + TURN_MS, otherUp + CHAIR_UP_MS + otherLeg + 150)
		: hugEnd;
	const otherGo = hug2 - 150 - otherLeg;
	const hug2End = two ? hug2 + EMBRACE2_MS : hugEnd;
	// (And off once the first is back out of his way.)
	const leave = Math.max(hug2End + 150, hugEnd + ASIDE_MS);
	// Off to the steps - at a walk, still, from the front row.
	const atFoot = leave + Math.max(1100, legs(g.hugged, capAt(take), take.pace));
	// Turned to the one with his cap, it handed him, and on.
	const handFrom = atFoot + 250;
	const handed = handFrom + HAND_MS;
	const capOn = handed + CAP_MS * 0.45;
	const atSteps = handed + CAP_MS;
	const onStage = atSteps + legs(capAt(take), STEPS_TOP, STAIR_PACE);
	const arrive = onStage + legs(STEPS_TOP, MARK, STAGE_PACE);
	const meet = arrive + 320;
	const photo = meet + HUG_MS;
	const pose = photo + 600;
	// Off the back of the stage, the commissioner back to the podium - cut
	// away from as he gets to the back of it.
	const off = pose + POSE_MS + 300;
	const offVia = off + legs(MARK, EXIT_VIA, OFF_PACE);
	const commBack = off + 250;
	const ms = offVia + 450;
	// The commissioner sets off to be there just before him.
	const commGo = arrive - 250 - legs(PODIUM_SPOT, COMM_MEET, OTHERS_PACE);
	return {
		podium,
		call,
		sat,
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
		stepIn,
		inThere,
		otherUp,
		otherGo,
		hug,
		hugEnd,
		hug2,
		hug2End,
		leave,
		atFoot,
		handFrom,
		handed,
		capOn,
		atSteps,
		onStage,
		arrive,
		meet,
		photo,
		pose,
		off,
		offVia,
		commBack,
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
// The smoke: as he comes up the aisle to the steps.
export const JET_MS = 1900;

// ---- the people ---------------------------------------------------------

// The commissioner and the pick's family: drawn as those who work a game are
// (their faces, their clothes), below any of their pids - his family looking
// like his family.
export const COMMISSIONER = -201;
export const MOTHER = -202;
export const FATHER = -203;
export const BROTHER = -204;
// The one with his cap; the photographers round his table, and in front of
// the stage.
export const STAFFER = -205;
const PRESS_PID = -220;
const PIT_PID = -230;
const KIN_PID: Record<Exclude<DraftKin, "none">, number> = {
	mom: MOTHER,
	dad: FATHER,
	brother: BROTHER,
};
// The commissioner: bald, in thin silver-rimmed glasses, smiling; a navy
// pinstriped suit, a pale blue shirt, a dark navy tie.
const commissionerFace = () => {
	const face = faceFor("draft|commissioner", false, "white");
	return {
		...face,
		fatness: 0.15,
		teamColors: ["#aeb5be", "#c4c9cf", "#1c2436"] as [string, string, string],
		head: { id: "head4", shave: "rgba(0,0,0,0)" },
		hair: { id: "bald", color: "#5e5248", flip: false },
		facialHair: { id: "none" },
		glasses: { id: "glasses2-secondary" },
		eye: { id: "eye5", angle: 0 },
		eyebrow: { id: "eyebrow8", angle: 0 },
		nose: { id: "nose6", flip: false, size: 0.9 },
		mouth: { id: "smile3", flip: false },
		smileLine: { id: "line1", size: 1 },
	};
};
export const commissioner = (): CrewMember => ({
	pid: COMMISSIONER,
	role: "staff",
	team: 1,
	hgt: 72,
	weight: 185,
	face: commissionerFace(),
	dress: dress("#1c2436", "#1c2436", {
		sleeves: "long",
		shirt: "#bcd2ea",
		tie: "#1a2140",
		pin: "#5f6b84",
		trousers: true,
	}),
});
export const staffer = (): CrewMember => ({
	pid: STAFFER,
	role: "staff",
	team: 1,
	hgt: 68,
	weight: 170,
	face: faceFor("draft|staffer", true),
	dress: dress("#17181c", "#17181c", { sleeves: "long", trousers: true }),
});
// Dark clothes, a camera each.
const PRESS_TOPS = ["#18181c", "#262a33", "#3a3a3d", "#1f2a24", "#2d2230"];
const PRESS_PANTS = ["#18181a", "#4b4334", "#2b2e35", "#3c3f46"];
const photographer = (pid: number, seed: number): CrewMember => ({
	pid,
	role: "photo",
	team: 1,
	hgt: 66 + unit(seed * 3 + 1) * 8,
	weight: 150 + unit(seed * 3 + 2) * 60,
	face: faceFor(`draft|press|${seed}`, unit(seed * 3) < 0.3),
	dress: dress(
		PRESS_TOPS[Math.floor(unit(seed * 5 + 3) * PRESS_TOPS.length)]!,
		PRESS_PANTS[Math.floor(unit(seed * 5 + 4) * PRESS_PANTS.length)]!,
		{
			sleeves: unit(seed * 7) < 0.5 ? "short" : "long",
			camera: true,
			trousers: true,
		},
	),
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
			dress: dress(main, "#16161a", { sleeves: "long", trousers: true }),
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
				trousers: true,
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
			trousers: true,
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
// The building, from high up in the stands at the back of it: from either
// corner or down the middle - the stage small at the far end, the floor full
// in front of it.
export const OPENING: Record<-1 | 0 | 1, [Pt3, Pt3]> = {
	[-1]: [
		{ x: 6, y: 92, z: 44 },
		{ x: 12, y: 88, z: 42 },
	],
	0: [
		{ x: 40, y: 98, z: 47 },
		{ x: 46, y: 93, z: 45 },
	],
	1: [
		{ x: 88, y: 92, z: 44 },
		{ x: 82, y: 88, z: 42 },
	],
};
export const OPENING_LOOK: Pt3 = { x: 47, y: 6, z: 4 };
export const OPENING_WIDTH = 104;
// Before a pick is read, and between picks: down on the floor among the
// tables, looking up - at the board (the picks on it, the team on the clock
// under them), at the wall over the stage, or at the team's tower - the rig
// glaring over it all, the room's heads along the bottom.
const BOARD_CAMS: Record<
	-1 | 0 | 1,
	{ at: [Pt3, Pt3]; look: [Pt3, Pt3]; width: number }
> = {
	// Up at the board.
	[-1]: {
		at: [
			{ x: 36, y: 25.5, z: 4.2 },
			{ x: 35, y: 24.5, z: 4.6 },
		],
		look: [
			{ x: 20, y: -8, z: 19 },
			{ x: 22, y: -8, z: 18.5 },
		],
		width: 52,
	},
	// Up the aisle at the wall.
	0: {
		at: [
			{ x: AISLE_X - 1.5, y: 27.5, z: 3.8 },
			{ x: AISLE_X - 0.5, y: 26, z: 4.2 },
		],
		look: [
			{ x: AISLE_X - 2, y: STAGE.y0, z: 15 },
			{ x: AISLE_X, y: STAGE.y0, z: 17 },
		],
		width: 56,
	},
	// From over to the side, across at the stage and the board beyond it.
	1: {
		at: [
			{ x: 59.5, y: 25, z: 4.2 },
			{ x: 58.5, y: 24, z: 4.6 },
		],
		look: [
			{ x: 30, y: -8, z: 16 },
			{ x: 32, y: -8, z: 18 },
		],
		width: 58,
	},
};
export const boardCam = (side: -1 | 0 | 1, u: number, k: number): Cam => {
	const c = BOARD_CAMS[side];
	return {
		at: mix3(c.at[0], c.at[1], u),
		look: mix3(c.look[0], c.look[1], u),
		width: (c.width - 3 * u) * k,
	};
};
// The commissioner at the podium, square on from out in the room on a long
// lens: chest up, the podium's top along the bottom of the picture, the
// team's colors either side of him.
export const podiumCam = (side: 1 | -1, u: number, k: number): Cam => ({
	at: { x: PODIUM.x + 0.8 * side, y: 26, z: 8.6 },
	look: { x: PODIUM.x, y: PODIUM_SPOT.y, z: 7.75 },
	width: (8 - 0.6 * u) * k,
});
// The camera in front of the stage, looking up at it: the photographers in
// front of it in the bottom of the picture.
const stageCamOf = (take: DraftTake): Pt3 => ({
	x: 49 + 2.5 * take.stageCam,
	y: 22,
	z: 8.5,
});
// Up on the stage to the side of the steps, looking down the aisle at him
// coming up it - the room behind him - and at the cap handed him: over the
// shoulder of the one handing it.
const footCamOf = (take: DraftTake): Pt3 => ({
	x: AISLE_X + 6 * take.footCam,
	y: -2,
	z: 10.2,
});
// At the back of the stage, looking out at the room: him up the steps and
// across to the commissioner, and the two of them hugging - him over the
// commissioner's shoulder.
const BACK_CAM: Pt3 = { x: MARK.x + 2.2, y: STAGE.y0 + 0.6, z: 11.2 };
// Low on the floor past the far end of the stage, looking back along it: the
// two of them before the logo, the podium beyond - and him off the back of
// it.
const SIDE_CAM: Pt3 = { x: STAGE.x1 + 3.5, y: 7, z: 4.6 };

// Over the shoulder of the one he hugs, behind them and a little over to
// his right - his head is down past theirs to his right (see poses.ts
// HOLD) - at his head's height: his face, their head and shoulders, his
// arms round them. Pushed in as they hold it (`held`, 0 to 1), and back
// out as he lets go (`out`).
const HUG_CAM = {
	back: 5.2,
	push: 0.9,
	right: 1.4,
	up: 0.1,
	down: 1.15,
	width: 8.2,
	tight: 1.2,
	loose: 3.5,
};
// (Cut to it just before they come together.)
const HUG_CUT_MS = 250;
// Close on him sat at his table: how high it looks, and how wide (feet).
// (Out from it as he gets up, from just before - so long, ms.)
const SAT_CAM = { z: 3.5, width: 7, lead: 250, out: 750 };
export const hugCamOf = (
	at: Pt,
	yaw: number,
	who: DraftKin,
	held: number,
	out: number,
	k: number,
): Cam => {
	const e = embraceOf(who);
	const man = e.him !== "embrace";
	// (His head, in his own frame - see HOLD and MAN_HOLD.)
	const head = man
		? { f: 0.66, s: -0.37, u: 5.6 }
		: { f: 1.08, s: -0.39, u: 5.39 };
	const c = Math.cos(yaw);
	const sn = Math.sin(yaw);
	const pt = (f: number, s: number) => ({
		x: at.x + f * c + s * sn,
		y: at.y + f * sn - s * c,
	});
	const back = e.apart + HUG_CAM.back - HUG_CAM.push * held + 1.2 * out;
	return {
		at: { ...pt(back, -HUG_CAM.right), z: head.u + HUG_CAM.up },
		look: {
			...pt(head.f, head.s * 0.6),
			z: head.u - HUG_CAM.down - 0.4 * out,
		},
		width: (HUG_CAM.width - HUG_CAM.tight * held + HUG_CAM.loose * out) * k,
	};
};

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
		return boardCam(take.open, smooth(t / N.podium), k);
	}
	if (t < N.call) {
		return podiumCam(
			take.podiumCam,
			smooth((t - N.podium) / (N.call - N.podium)),
			k,
		);
	}
	const st = him ?? { x: g.seat.x, y: g.seat.y, z: 0 };
	const hugFrom = N.hug - HUG_CUT_MS;
	if (t < hugFrom) {
		// In on him at his table, from the stage side of it: close on him in
		// his chair as it hits him; out as he gets up out of it, and out to
		// them - the photographers crowded in behind him.
		const u = smooth((t - N.stand) / (hugFrom - N.stand));
		const up = smooth((t - N.stand + SAT_CAM.lead) / SAT_CAM.out);
		const wide = {
			look: mix3(
				{ x: g.tableLook.x, y: g.tableLook.y, z: 3.3 },
				{ x: g.hugged.x - 0.4 * g.side, y: g.hugged.y, z: 5 },
				u,
			),
			width: 10.5 + 2 * u,
		};
		return {
			at: { x: g.tableCam.x, y: g.tableCam.y, z: 5.8 },
			look: mix3(
				{ x: g.tableLook.x, y: g.tableLook.y, z: SAT_CAM.z },
				wide.look,
				up,
			),
			width: (SAT_CAM.width + (wide.width - SAT_CAM.width) * up) * k,
		};
	}
	if (t < N.leave + 500) {
		// Tight on his face over their shoulder as they hug, pushing in;
		// and the next of them, over theirs - and out a little as he lets
		// go and goes.
		const two = take.other !== "none";
		const second = two && t >= N.hugEnd;
		const from = second ? N.hugEnd : hugFrom;
		const last = second || !two;
		const held = smooth((t - from) / ((second ? N.hug2End : N.hugEnd) - from));
		const out = last
			? smooth((t - (second ? N.hug2End : N.hugEnd) + 700) / 1400)
			: 0;
		return hugCamOf(
			g.hugged,
			second ? g.hugYaw + Math.PI : g.hugYaw,
			second ? take.other : take.hugger,
			held,
			out,
			k,
		);
	}
	if (t < N.atSteps) {
		// From the stage, down the aisle at him coming up it - and at the cap
		// handed him at the foot of the steps.
		const at = footCamOf(take);
		const d = Math.hypot(st.x - at.x, st.y - at.y);
		return {
			at,
			look: { x: st.x, y: st.y, z: st.z + 4.4 },
			width: Math.min(19, Math.max(9.5, d * 0.68)) * k,
		};
	}
	if (t < N.photo) {
		// From the back of the stage: him up onto it toward the camera, the
		// room behind him, and over the commissioner's shoulder as they hug.
		const d = Math.hypot(st.x - BACK_CAM.x, st.y - BACK_CAM.y);
		const settled = smooth((t - N.arrive) / 600);
		return {
			at: BACK_CAM,
			look: mix3(
				{ x: st.x, y: st.y, z: st.z + 4.6 },
				{ x: MARK.x, y: MARK.y, z: STAGE.z + 5.4 },
				settled,
			),
			width: Math.min(15, Math.max(10, d * 1.1)) * k,
		};
	}
	const sideFrom = N.off - 300;
	if (t >= sideFrom) {
		// From the end of the stage, low: him off at the back of it past the
		// podium, the commissioner back to it, the logo over them.
		const u = smooth((t - sideFrom) / (N.ms - sideFrom));
		return {
			at: SIDE_CAM,
			look: mix3(
				{ x: MARK.x - 1, y: MARK.y - 0.2, z: STAGE.z + 4.8 },
				{ x: st.x - 2, y: st.y, z: STAGE.z + 5 },
				u,
			),
			width: (22 + 3 * u) * k,
		};
	}
	// In front of the stage, looking up at it: the two of them for the
	// pictures, the photographers' backs in front.
	const u = smooth((t - N.photo) / (sideFrom - N.photo));
	return {
		at: stageCamOf(take),
		look: { x: MARK.x - 0.9, y: MARK.y - 0.4, z: 7.4 },
		width: (18 - 2.5 * u) * k,
	};
};

// The photographers' flashes: those round his table as he gets up, and
// those in front of the stage once he is up on it.
const FLASH_MS = 80;
export const flashesOf =
	(g: DraftPlaces, N: DraftNight) =>
	(t: number): Pt3[] => {
		const slot = Math.floor(t / FLASH_MS);
		const out: Pt3[] = [];
		const table = t >= N.call + 300 && t < N.leave + 800;
		const stage = t >= N.meet && t < N.off;
		if (!table && !stage) {
			return out;
		}
		const from = stage ? PIT : g.press;
		const aim = stage ? MARK : g.hugged;
		const share = stage ? (t >= N.photo ? 0.5 : 0.3) : 0.3;
		for (let k = 0; k < 3; k++) {
			if (unit(slot * 3 + k) >= share) {
				continue;
			}
			const p = from[Math.floor(unit(slot * 7 + k + 1000) * from.length)]!;
			const a = yawTo(p, aim);
			out.push({
				x: p.x + Math.cos(a) * 0.7,
				y: p.y + Math.sin(a) * 0.7,
				z: 5.5 + unit(slot * 5 + k) * 0.3,
			});
		}
		return out;
	};

// Nowhere the cameras look.
export const NO_BALL = { x: 5000, y: 20, z: 0.39 };

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
	const STAFF = staffAt(take);
	const CAP = capAt(take);
	const two = take.other !== "none";
	const moves: Move[] = [
		walk(N.walkOff, N.stepped, SEAT, g.hugged),
		walk(N.leave, N.atFoot, g.hugged, CAP),
		walk(N.atSteps, N.onStage, CAP, STEPS_TOP),
		walk(N.onStage, N.arrive, STEPS_TOP, MARK),
		walk(N.off, N.offVia, MARK, EXIT_VIA),
		walk(N.offVia, N.offVia + legs(EXIT_VIA, EXIT, OFF_PACE), EXIT_VIA, EXIT),
	];
	const grin = GRINS[take.mood];
	const easy: AnimName = grin ? "easyGrin" : "easy";
	const hug1 = embraceOf(take.hugger);
	const hug2 = embraceOf(take.other);
	const hugLook = ahead(g.hugged, g.hugYaw);
	const hug2Look = ahead(g.hugged, g.hugYaw + Math.PI);
	const track: Track = {
		pid,
		team: 1,
		start: { x: SEAT.x, y: SEAT.y },
		moves,
		acts: [
			{ t0: 0, t1: N.sat, anim: "tableSit", look: ahead(SEAT, SEAT.yaw) },
			{ t0: N.sat, t1: N.stand, anim: take.sit, look: ahead(SEAT, SEAT.yaw) },
			{
				t0: N.stand,
				t1: N.stood,
				anim: grin ? "tableUpGrin" : "tableUp",
				look: ahead(SEAT, SEAT.yaw),
			},
			{ t0: N.stood, t1: N.reacted, anim: take.up, look: TABLE_CAM },
			// (Waiting on them to come out to him.)
			...(N.walkOff > N.reacted
				? [
						{
							t0: N.reacted,
							t1: N.walkOff,
							anim: easy,
							look: g.kinHug,
						},
					]
				: []),
			{ t0: N.stepped, t1: N.hug, anim: easy, look: hugLook },
			{ t0: N.hug, t1: N.hugEnd, anim: grinned(hug1.him, grin), look: hugLook },
			...(two
				? [
						{
							t0: N.hugEnd,
							t1: N.hug2,
							anim: easy,
							look: hug2Look,
						},
						{
							t0: N.hug2,
							t1: N.hug2End,
							anim: grinned(hug2.him, grin),
							look: hug2Look,
						},
					]
				: []),
			// The cap: turned to her, handed it, and on.
			{ t0: N.atFoot, t1: N.handFrom, anim: "easy", look: STAFF },
			{ t0: N.handFrom, t1: N.handed, anim: "dap", look: STAFF },
			{ t0: N.handed, t1: N.atSteps, anim: "capOn", look: STEPS_TOP },
			{ t0: N.arrive, t1: N.meet, anim: "easy", look: ahead(MARK, MEET_YAW) },
			{ t0: N.meet, t1: N.photo, anim: "dapHug", look: ahead(MARK, MEET_YAW) },
			{ t0: N.photo, t1: N.pose, anim: "easyGrin", look: STAGE_LOOK },
			{ t0: N.pose, t1: N.pose + POSE_MS, anim: take.pose, look: STAGE_LOOK },
			{ t0: N.pose + POSE_MS, t1: N.off, anim: "easyGrin", look: STAGE_LOOK },
		],
		arms: [],
		faces: [],
		looks: [
			[0, ahead(SEAT, SEAT.yaw)],
			[N.stood, TABLE_CAM],
			[N.stepped, hugLook],
			...(two ? ([[N.hugEnd, hug2Look]] as [number, Pt][]) : []),
			[N.atFoot, STAFF],
			[N.handed, STEPS_TOP],
			[N.arrive, ahead(MARK, MEET_YAW)],
			[N.photo, STAGE_LOOK],
			[N.off, EXIT],
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
			to: g.kinWait,
			yaw: 0,
			anim: "walk",
		},
		...(N.stepIn > N.kinThere
			? [
					{
						t0: N.kinThere,
						t1: N.stepIn,
						at: g.kinWait,
						yaw: yawTo(g.kinWait, g.hugged),
						anim: "clap" as const,
					},
				]
			: []),
		{
			t0: N.stepIn,
			t1: N.inThere,
			at: g.kinWait,
			to: g.kinHug,
			yaw: 0,
			anim: "walk",
		},
		{
			t0: N.inThere,
			t1: N.hug,
			at: g.kinHug,
			yaw: g.hugYaw + Math.PI,
			anim: "easy",
		},
		{
			t0: N.hug,
			t1: N.hugEnd,
			at: g.kinHug,
			yaw: g.hugYaw + Math.PI,
			anim: hug1.them,
		},
		{
			t0: N.hugEnd,
			t1: N.hugEnd + ASIDE_MS,
			at: g.kinHug,
			to: g.kinAside,
			yaw: 0,
			anim: "walk",
		},
		{
			t0: N.hugEnd + ASIDE_MS,
			t1: N.leave,
			at: g.kinAside,
			yaw: yawTo(g.kinAside, g.hugged),
			anim: "clap",
		},
		{
			t0: N.leave,
			t1: N.ms + 60_000,
			at: g.kinAside,
			yaw: yawTo(g.kinAside, g.aisleIn),
			anim: "clap",
		},
	];
	// And whoever else is with them: up on their feet, clapping him (his
	// brother more than clapping) - and round to him for a hug of their own.
	const otherPid = take.other === "none" ? undefined : KIN_PID[take.other];
	const cheer: AnimName = take.other === "brother" ? "cheer" : "clap";
	const otherWay = [g.otherSeat, ...g.otherVia, g.otherHug];
	const otherLegs: Beat[] = [];
	{
		let t = N.otherGo;
		for (let i = 1; i < otherWay.length; i++) {
			const a = otherWay[i - 1]!;
			const b = otherWay[i]!;
			const t1 = t + legs(a, b, OTHERS_PACE);
			otherLegs.push({ t0: t, t1, at: a, to: b, yaw: 0, anim: "walk" });
			t = t1;
		}
	}
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
			t1: N.otherGo,
			at: g.otherSeat,
			yaw: yawTo(g.otherSeat, g.hugged),
			anim: cheer,
		},
		...otherLegs,
		{
			t0: otherLegs.at(-1)?.t1 ?? N.otherGo,
			t1: N.hug2,
			at: g.otherHug,
			yaw: g.hugYaw,
			anim: "easy",
		},
		{
			t0: N.hug2,
			t1: N.hug2End,
			at: g.otherHug,
			yaw: g.hugYaw,
			anim: hug2.them,
		},
		{
			t0: N.hug2End,
			t1: N.ms + 60_000,
			at: g.otherHug,
			yaw: yawTo(g.otherHug, g.aisleIn),
			anim: cheer,
		},
	];
	const commAround = path([COMM_MEET, COMM_VIA, COMM_PHOTO], 3);
	const commBackAt = N.commBack + legs(COMM_PHOTO, COMM_BACK, 3);
	const commHome = commBackAt + legs(COMM_BACK, PODIUM_SPOT, 3);
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
		{
			t0: N.arrive - 250,
			t1: N.meet,
			at: COMM_MEET,
			yaw: MEET_YAW + Math.PI,
			anim: "ready",
		},
		{
			t0: N.meet,
			t1: N.photo,
			at: COMM_MEET,
			yaw: MEET_YAW + Math.PI,
			anim: "dapHug",
		},
		{
			t0: N.photo,
			t1: N.photo + legs(COMM_MEET, COMM_VIA, 3),
			at: COMM_MEET,
			to: COMM_VIA,
			yaw: 0,
			anim: "walk",
		},
		{
			t0: N.photo + legs(COMM_MEET, COMM_VIA, 3),
			t1: N.photo + commAround,
			at: COMM_VIA,
			to: COMM_PHOTO,
			yaw: 0,
			anim: "walk",
		},
		{
			t0: N.photo + commAround,
			t1: N.commBack,
			at: COMM_PHOTO,
			yaw: yawTo(COMM_PHOTO, STAGE_LOOK),
			anim: "easy",
		},
		// And back to the podium.
		{
			t0: N.commBack,
			t1: commBackAt,
			at: COMM_PHOTO,
			to: COMM_BACK,
			yaw: 0,
			anim: "walk",
		},
		{
			t0: commBackAt,
			t1: commHome,
			at: COMM_BACK,
			to: PODIUM_SPOT,
			yaw: 0,
			anim: "walk",
		},
		{
			t0: commHome,
			t1: N.ms + 60_000,
			at: PODIUM_SPOT,
			yaw: Math.PI / 2,
			anim: "easy",
		},
	];
	// The cap held for him at the foot of the steps, and handed him.
	const staffBeats: Beat[] = [
		{
			t0: 0,
			t1: N.handFrom,
			at: STAFF,
			yaw: yawTo(STAFF, { x: AISLE_X, y: STEPS.y1 + 8 }),
			anim: "easy",
		},
		{
			t0: N.handFrom,
			t1: N.handed,
			at: STAFF,
			yaw: yawTo(STAFF, CAP),
			anim: "dap",
		},
		{
			t0: N.handed,
			t1: N.ms + 60_000,
			at: STAFF,
			yaw: yawTo(STAFF, MARK),
			anim: "clap",
		},
	];
	// The photographers: crowded in behind his table, cameras up from his
	// name on; and in front of the stage, from when he is up on it.
	const pressBeats = (at: Pt, aim: Pt, from: number): Beat[] => [
		{ t0: 0, t1: from, at, yaw: yawTo(at, aim), anim: "easy" },
		{
			t0: from,
			t1: N.ms + 60_000,
			at,
			yaw: yawTo(at, aim),
			anim: "shootStand",
		},
	];
	const press = g.press.map((at, i) => ({
		pid: PRESS_PID - i,
		beats: pressBeats(at, g.hugged, N.call + 300 + i * 170),
	}));
	const pit = PIT.map((at, i) => ({
		pid: PIT_PID - i,
		beats: pressBeats(at, MARK, N.atSteps + i * 150),
	}));

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
			staffer(),
			...press.map((p) => photographer(p.pid, pid * 11 + p.pid)),
			...pit.map((p) => photographer(p.pid, p.pid)),
		],
		castAt: (t) => [
			castState(COMMISSIONER, commBeats, t, groundAt),
			castState(kinPid, kinBeats, t, groundAt),
			...(otherPid === undefined
				? []
				: [castState(otherPid, otherBeats, t, groundAt)]),
			castState(STAFFER, staffBeats, t, groundAt),
			...press.map((p) => castState(p.pid, p.beats, t, groundAt)),
			...pit.map((p) => castState(p.pid, p.beats, t, groundAt)),
		],
		take,
		night: N,
		places: g,
		room: {
			families: [familyOf(g)],
			upAt: (t) => t >= N.call + 200,
			clapAt: clapOf(N),
			jetFrom: N.atFoot - 2600,
		},
	};
};
