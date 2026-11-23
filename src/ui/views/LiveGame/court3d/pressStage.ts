import type { Race } from "../../../../common/types.ts";
import { makeCourtRng } from "../courtRng.ts";
import type { Authored } from "./authored.ts";
import { crewBody, dress, faceFor, type CrewMember } from "./crew.ts";
import type { CourtTimeline } from "./director.ts";
import {
	castState,
	NO_BALL,
	shotOf,
	type Beat,
	type Cam,
} from "./draftStage.ts";
import { bodyPoint, poseOf, withBody, type PlayerState } from "./evaluate.ts";
import { shade } from "./figure.ts";
import type { Pt, Pt3 } from "./geometry.ts";
import { skeleton, type AnimName, type Body } from "./poses.ts";

// THE PRESS CONFERENCE THAT INTRODUCES HIM (see the worker's
// pressConference.ts for what it is made from, pressVenue.ts for the room,
// PressBroadcast.tsx for the graphics over it): a team showing off who it
// traded for or signed. The room: a step-and-repeat in the team's colors,
// a low riser in front of it, the photographers crowded in on the floor.
// Him - and any others it got in the same move - stood on the riser holding
// his new jersey out in front of him by its shoulders, its back to the
// cameras (his name and number), in his own clothes; the general manager and
// the coach either side of him, the owner too sometimes, a ball in their
// hands now and then; and at the table, the microphones in front of them, the
// general manager saying a few words and him answering.
//
// No two the same (see PressTake): the order of it, who is up there with him
// and where, how he holds it up, the backdrop, which of its jerseys, the
// cameras - each picked from the move and the team, so it plays the same
// every time it is watched.

// ---- the room (feet: x along the old court, y from the far side toward the
// cameras, z up) -------------------------------------------------------------

export const CX = 47;
// The step-and-repeat, standing on the floor facing the cameras.
export const BACKDROP = { x0: CX - 21, x1: CX + 21, y: 8, z0: 0, z1: 11.5 };
// The riser in front of it, and how high.
export const RISER = { x0: CX - 13.5, x1: CX + 13.5, y0: 8.2, y1: 17, h: 0.8 };
// Where they stand for the pictures, and sit at the table.
const ROW_Y = 11.6;
export const SEAT_Y = 10.6;
// The table in front of the chairs: from so far in front of them, so deep,
// so high (over the riser).
export const TABLE = { from: 1.0, depth: 2.3, h: 2.45 };
// Or a podium for whoever is speaking, the others stood either side of it:
// where its middle is, how wide, deep and high (over the riser).
export const PODIUM = { x: CX, y: 11.7, w: 2.2, d: 1.1, h: 3.5 };
// Where the one speaking at it stands, and how far apart the rest stand
// beside it.
export const AT_PODIUM: Pt = { x: CX, y: 10.6 };
const BESIDE_GAP = 2.9;
const BESIDE_FROM = 3.3;
// How far apart the chairs at it are, and how far past the end ones it goes.
const SEAT_GAP = 3.1;
const TABLE_PAST = 1.7;

export const groundAt = (x: number, y: number): number =>
	x >= RISER.x0 && x <= RISER.x1 && y >= RISER.y0 && y <= RISER.y1
		? RISER.h
		: 0;

// ---- the take -----------------------------------------------------------------

export type ExecKind = "gm" | "coach" | "owner";
// The cameras through the pictures: from the back of the room, the whole of
// it; in closer, all of them; tight on him and his jersey; from off to the
// side; and from down among the photographers.
export type PhotoShot = "wide" | "medium" | "close" | "angle" | "pit";
export type PressTake = {
	// The table first and then the pictures, or the other way round.
	order: "talkFirst" | "photoFirst";
	// The execs either side of him, the nearest first.
	left: ExecKind[];
	right: ExecKind[];
	// A ball in the execs' hands for the pictures.
	balls: boolean;
	// His jersey held at his chest, or up higher.
	hold: "holdJersey" | "holdJerseyHigh";
	// The pictures come in on it already held up, or on it brought up.
	heldFirst: boolean;
	// Which side of the room the cameras favor.
	side: 1 | -1;
	shots: PhotoShot[];
	// The talk: sat at a table, or stood at a podium; from the back on all of
	// it, then the two-shot or straight to him.
	talkAt: "table" | "podium";
	twoShot: boolean;
	// Who they look at through the pictures: the photographers to their
	// right, in front, to their left.
	looks: (-1 | 0 | 1)[];
	// The backdrop's look (see pressVenue.ts), and which of its jerseys.
	backdrop: 0 | 1 | 2 | 3;
	edition: "home" | "road";
	photoMs: number;
	talkMs: number;
};

const pickOf =
	(rng: () => number) =>
	<T>(of: readonly T[]): T =>
		of[Math.min(of.length - 1, Math.floor(rng() * of.length))]!;

export const takeOf = (eid: number, tid: number): PressTake => {
	const rng = makeCourtRng(`press|${eid}|${tid}`);
	const pick = pickOf(rng);
	const order = rng() < 0.7 ? "talkFirst" : "photoFirst";
	// The GM and the coach, most of the time; the owner in for one of them
	// now and then, or with them both.
	const crew = pick<ExecKind[]>([
		["gm", "coach"],
		["gm", "coach"],
		["gm", "coach"],
		["coach", "gm"],
		["owner", "gm"],
		["gm", "owner"],
		["gm", "coach", "owner"],
		["owner", "gm", "coach"],
	]);
	const left = crew.length === 3 ? [crew[0]!, crew[2]!] : [crew[0]!];
	const right = [crew[1]!];
	const side = rng() < 0.5 ? 1 : -1;
	const opener = pick<PhotoShot>(["wide", "wide", "pit", "pit", "medium"]);
	const middle = pick<PhotoShot[]>([
		["medium", "close"],
		["close", "medium"],
		["close", "angle"],
		["medium", "close", "angle"],
		["pit", "close"],
	]).filter((s) => s !== opener);
	const shots: PhotoShot[] = [opener, ...middle];
	if (shots.at(-1) === "close" && rng() < 0.6) {
		shots.push(pick<PhotoShot>(["wide", "medium", "angle"]));
	}
	const take: PressTake = {
		order,
		left,
		right,
		balls: rng() < 0.4,
		hold: rng() < 0.6 ? "holdJersey" : "holdJerseyHigh",
		heldFirst: order === "photoFirst" ? rng() < 0.5 : rng() < 0.2,
		side,
		shots,
		twoShot: rng() < 0.55,
		looks: pick<(-1 | 0 | 1)[]>([
			[0, -1, 0, 1, 0],
			[0, 1, 0, -1, 0],
			[-1, 0, 1, 0],
			[1, 0, -1, 0],
			[0, 1, -1, 0],
		]),
		backdrop: pick([0, 0, 1, 2, 3] as const),
		edition: rng() < 0.62 ? "home" : "road",
		photoMs: Math.round(8600 + rng() * 1600),
		talkMs: Math.round(6400 + rng() * 1200),
		talkAt: rng() < 0.35 ? "podium" : "table",
	};
	// (Stood at a podium, a little longer: the general manager has more to
	// say up there.)
	if (take.talkAt === "podium") {
		take.talkMs += 1800;
	}
	return take;
};

// ---- the people -------------------------------------------------------------

export const EXEC_PID: Record<ExecKind, number> = {
	gm: -301,
	coach: -302,
	owner: -303,
};
const PRESS_PID = -320;

// Suits most of them; the coach in the team's quarter-zip sometimes.
const SUITS = [
	"#1b2436",
	"#24262c",
	"#141416",
	"#2e3340",
	"#3a3f4a",
	"#33302c",
];
const SHIRTS = ["#f2f2ee", "#f2f2ee", "#d9e3ee", "#cfdcef", "#e9e6df"];
const TIES = ["#6b1f2a", "#1e2c52", "#9aa0a8", "#2f5d4a", "#5a3a6e", "#141416"];

// The team's people - the same faces for the same team over a few seasons
// (a front office lasts a while).
export const execOf = (
	kind: ExecKind,
	tid: number,
	season: number,
	colors: [string, string, string],
): CrewMember => {
	const seed = `press|${kind}|${tid}|${Math.floor(season / 5)}`;
	const rng = makeCourtRng(seed);
	const pick = pickOf(rng);
	const female =
		rng() < (kind === "owner" ? 0.15 : kind === "gm" ? 0.08 : 0.02);
	const hgt = female
		? 64 + rng() * 6
		: kind === "owner"
			? 67 + rng() * 6
			: kind === "coach"
				? 71 + rng() * 9
				: 70 + rng() * 7;
	const weight = (female ? 125 : 165) + rng() * 70;
	const suit = pick(SUITS);
	const shirt = pick(SHIRTS);
	const tie = rng() < (kind === "owner" ? 0.35 : 0.65) ? pick(TIES) : undefined;
	const zip = kind === "coach" && rng() < 0.4;
	const teamTop = shade(colors[0], -0.08);
	return {
		pid: EXEC_PID[kind],
		role: "staff",
		team: 1,
		hgt,
		weight,
		face: faceFor(seed, female),
		dress: zip
			? dress(teamTop, pick(["#18181a", "#2b2e35", "#3c3f46"]), {
					sleeves: "long",
					trousers: true,
				})
			: dress(suit, suit, {
					sleeves: "long",
					shirt,
					...(tie ? { tie } : {}),
					trousers: true,
				}),
	};
};

// Dark clothes, a camera each.
const PRESS_TOPS = ["#18181c", "#262a33", "#3a3a3d", "#1f2a24", "#2d2230"];
const PRESS_PANTS = ["#18181a", "#4b4334", "#2b2e35", "#3c3f46"];
const photographer = (pid: number, seed: string): CrewMember => {
	const rng = makeCourtRng(seed);
	const pick = pickOf(rng);
	return {
		pid,
		role: "photo",
		team: 1,
		hgt: 65 + rng() * 9,
		weight: 145 + rng() * 70,
		face: faceFor(seed, rng() < 0.3),
		dress: dress(pick(PRESS_TOPS), pick(PRESS_PANTS), {
			sleeves: rng() < 0.5 ? "short" : "long",
			camera: true,
			trousers: true,
		}),
	};
};

// ---- where they all are -------------------------------------------------------

export type Placed = { pid: number; x: number; y: number };

// Stood in a row along the riser for the pictures: the execs on the
// outside, the players in the middle - the best of them in the middle of
// those (see the worker: best first).
export const lineupOf = (take: PressTake, pids: number[]) => {
	const mid = [...pids];
	// (0 1 2 3 -> 2 0 1 3; 0 1 2 -> 1 0 2.)
	const order =
		mid.length === 3
			? [mid[1]!, mid[0]!, mid[2]!]
			: mid.length >= 4
				? [mid[2]!, mid[0]!, mid[1]!, mid[3]!, ...mid.slice(4)]
				: mid;
	const row: { pid: number; exec?: ExecKind }[] = [
		...[...take.left].reverse().map((k) => ({ pid: EXEC_PID[k], exec: k })),
		...order.map((pid) => ({ pid })),
		...take.right.map((k) => ({ pid: EXEC_PID[k], exec: k })),
	];
	const gaps = row.slice(1).map((b, i) => {
		const a = row[i]!;
		return a.exec && b.exec ? 2.7 : a.exec || b.exec ? 3.0 : 3.4;
	});
	const span = gaps.reduce((s, g) => s + g, 0);
	let x = CX - span / 2;
	return row.map((r, i) => {
		if (i > 0) {
			x += gaps[i - 1]!;
		}
		// (The execs a little back off his shoulder.)
		return { ...r, x, y: ROW_Y + (r.exec ? -0.35 : 0) };
	});
};
export type Lineup = ReturnType<typeof lineupOf>;

// Sat at the table: the general manager beside him (the coach the other
// side), the owner stood off at the end of the riser.
export const seatsOf = (take: PressTake, pids: number[]) => {
	const sitting = [...take.left, ...take.right].filter((k) => k !== "owner");
	const leftK = sitting.find((k) => k === "gm") ?? sitting[0];
	const rightK = sitting.find((k) => k !== leftK);
	const flip = take.left[0] === "coach";
	const ends = flip ? [rightK, leftK] : [leftK, rightK];
	const row: { pid: number; exec?: ExecKind }[] = [
		...(ends[0] ? [{ pid: EXEC_PID[ends[0]], exec: ends[0] }] : []),
		...pids.map((pid) => ({ pid })),
		...(ends[1] ? [{ pid: EXEC_PID[ends[1]], exec: ends[1] }] : []),
	];
	const x0 = CX - ((row.length - 1) * SEAT_GAP) / 2;
	return row.map((r, i) => ({ ...r, x: x0 + i * SEAT_GAP, y: SEAT_Y }));
};
export type Seats = ReturnType<typeof seatsOf>;

// Stood at the podium: who is beside it, either side - the players on one
// side (the best of them nearest), the execs on the other - and after the
// general manager has had his say, him at it and the general manager in his
// place.
export const besideOf = (take: PressTake, pids: number[]) => {
	const execs = [...take.left, ...take.right];
	const gm = execs.includes("gm") ? "gm" : execs[0]!;
	const rest = execs.filter((k) => k !== gm);
	const side = take.side;
	const at = (i: number, way: number): Pt => ({
		x: AT_PODIUM.x + way * (BESIDE_FROM + i * BESIDE_GAP),
		y: AT_PODIUM.y - 0.2 - 0.25 * i,
	});
	return {
		gm: EXEC_PID[gm],
		gmKind: gm,
		players: pids.map((pid, i) => ({ pid, at: at(i, side) })),
		rest: rest.map((k, i) => ({ pid: EXEC_PID[k], exec: k, at: at(i, -side) })),
	};
};
export const tableOf = (seats: Seats) => ({
	x0: seats[0]!.x - TABLE_PAST,
	x1: seats.at(-1)!.x + TABLE_PAST,
	y0: SEAT_Y + TABLE.from,
	y1: SEAT_Y + TABLE.from + TABLE.depth,
});

// The photographers for the pictures - a row down on a knee in front, a row
// stood behind them - and at the table, off to either side of it.
const KNEEL_ROW = [-6.4, 3.2, 7.6];
const STAND_ROW = [-10.2, -4.2, 3.4, 9.6];
const SIDE_SPOTS: Pt[] = [
	{ x: RISER.x0 - 2.4, y: 10.5 },
	{ x: RISER.x0 - 3.2, y: 13.6 },
	{ x: RISER.x1 + 2.4, y: 10.8 },
	{ x: RISER.x1 + 3.1, y: 14 },
];
const pitOf = (take: PressTake) => {
	const rng = makeCourtRng(`press|pit|${take.side}|${take.photoMs}`);
	return [
		...KNEEL_ROW.map((dx, i) => ({
			// (The one in the middle off the side the cameras favor.)
			at: {
				x: CX + (i === 1 ? dx * take.side : dx) + (rng() - 0.5) * 1.2,
				y: 20.6 + rng() * 0.8,
			},
			kneel: true,
		})),
		...STAND_ROW.map((dx) => ({
			at: { x: CX + dx + (rng() - 0.5) * 1.4, y: 23.4 + rng() * 1.2 },
			kneel: false,
		})),
	];
};

// ---- the timeline -------------------------------------------------------------

const yawTo = (from: Pt, to: Pt) => Math.atan2(to.y - from.y, to.x - from.x);
const FACING = Math.PI / 2;
// (Toward the cameras, a little in toward the middle of the room.)
const toRoom = (at: Pt) => yawTo(at, { x: CX, y: 40 });
// Looking off at the photographers to one side or the other.
const LOOK_TURN = 0.26;

export type PressPlan = {
	photo: { t0: number; t1: number };
	talk: { t0: number; t1: number };
	// When the jerseys come up, and are held up.
	up: number;
	held: number;
	// Who is talking at the table, from when to when.
	gmTalk: [number, number];
	himTalk: [number, number];
	ms: number;
};
const UP_MS = 900;
export const planOf = (take: PressTake): PressPlan => {
	const photoFirst = take.order === "photoFirst";
	const photo = photoFirst
		? { t0: 0, t1: take.photoMs }
		: { t0: take.talkMs, t1: take.talkMs + take.photoMs };
	const talk = photoFirst
		? { t0: take.photoMs, t1: take.photoMs + take.talkMs }
		: { t0: 0, t1: take.talkMs };
	const up = take.heldFirst ? photo.t0 - UP_MS : photo.t0 + 1300;
	const gmShare = take.talkAt === "podium" ? 0.45 : photoFirst ? 0.3 : 0.42;
	const gmEnd = talk.t0 + Math.round((talk.t1 - talk.t0) * gmShare);
	return {
		photo,
		talk,
		up,
		held: up + UP_MS,
		gmTalk: [talk.t0, gmEnd],
		himTalk: [gmEnd + 250, talk.t1],
		ms: Math.max(photo.t1, talk.t1),
	};
};

// ---- the cameras ----------------------------------------------------------------

// Each a camera stood somewhere in the room, looking at a spot, so wide
// across there (feet) - on a squarer picture (a phone), in closer.
const photoCam = (
	shot: PhotoShot,
	take: PressTake,
	row: Lineup,
	star: Placed,
	u: number,
	k: number,
): Cam => {
	const s = take.side;
	const x0 = row[0]!.x;
	const x1 = row.at(-1)!.x;
	const span = x1 - x0;
	const midX = (x0 + x1) / 2;
	// (A slow push in, all through it.)
	const push = 1 - 0.07 * u;
	switch (shot) {
		case "wide":
			return {
				at: { x: midX + 3.5 * s, y: 40, z: 9.4 },
				look: { x: midX + 0.3 * s, y: ROW_Y, z: 4.7 },
				width: Math.max(18, span + 9) * push * k,
			};
		case "medium":
			return {
				at: { x: midX + 1.6 * s, y: 31, z: 8.3 },
				look: { x: midX, y: ROW_Y, z: 5.1 },
				width: Math.max(11, span + 3.4) * push * k,
			};
		case "close":
			return {
				at: { x: star.x + 0.9 * s, y: 25.5, z: 6.9 },
				look: { x: star.x + 0.1 * s, y: ROW_Y + 0.8, z: 5.35 },
				width: 6.4 * push * k,
			};
		case "angle":
			return {
				at: { x: midX + 15.5 * s, y: 26, z: 7.2 },
				look: { x: midX - 1.2 * s, y: ROW_Y, z: 5.1 },
				width: Math.max(12, span * 0.85 + 4) * push * k,
			};
		case "pit":
			return {
				at: { x: midX - 1.2 * s, y: 30, z: 5.9 },
				look: { x: midX + 0.3 * s, y: ROW_Y, z: 5 },
				width: Math.max(13, span + 5) * push * k,
			};
	}
};

// At the table: from the back of the room on all of it; across from off to
// the side on him and the general manager; tight on him at his microphone.
const talkCam = (
	shot: "table" | "two" | "him",
	take: PressTake,
	seats: Seats,
	him: Placed,
	gm: Placed | undefined,
	u: number,
	k: number,
): Cam => {
	const s = take.side;
	const x0 = seats[0]!.x;
	const x1 = seats.at(-1)!.x;
	const midX = (x0 + x1) / 2;
	const push = 1 - 0.06 * u;
	const sz = RISER.h + 4.25;
	if (shot === "table") {
		return {
			at: { x: midX + 2.2 * s, y: 38, z: 7.4 },
			look: { x: midX + 0.2 * s, y: SEAT_Y + 1.2, z: RISER.h + 3.4 },
			width: Math.max(14, x1 - x0 + 8) * push * k,
		};
	}
	if (shot === "two" && gm) {
		const mx = (him.x + gm.x) / 2;
		const toward = Math.sign(gm.x - him.x) || 1;
		return {
			at: { x: mx - 9 * toward, y: 25, z: 6.1 },
			look: { x: mx + 0.4 * toward, y: SEAT_Y + 0.6, z: sz },
			width: 9 * push * k,
		};
	}
	return {
		at: { x: him.x + 1.5 * s, y: 23, z: 5.8 },
		look: { x: him.x + 0.15 * s, y: SEAT_Y + 0.5, z: sz - 0.2 },
		width: 7.4 * push * k,
	};
};

// Or at the podium: from the back of the room on all of them; across from
// off to the side on him at it and the general manager stood beside it;
// tight on whoever is at it.
const podiumCam = (
	shot: "table" | "two" | "him",
	take: PressTake,
	xs: number[],
	gm: Pt,
	// How tall whoever is at it is (feet).
	tall: number,
	u: number,
	k: number,
): Cam => {
	const s = take.side;
	const x0 = Math.min(...xs);
	const x1 = Math.max(...xs);
	const midX = (x0 + x1) / 2;
	const push = 1 - 0.06 * u;
	const sz = RISER.h + tall * 0.8;
	if (shot === "table") {
		return {
			at: { x: midX + 2.2 * s, y: 38, z: 7.8 },
			look: { x: midX + 0.2 * s, y: AT_PODIUM.y + 1, z: RISER.h + 4 },
			width: Math.max(14, x1 - x0 + 7) * push * k,
		};
	}
	if (shot === "two") {
		const mx = (AT_PODIUM.x + gm.x) / 2;
		const toward = Math.sign(gm.x - AT_PODIUM.x) || 1;
		return {
			at: { x: mx - 9 * toward, y: 25, z: sz + 0.9 },
			look: { x: mx + 0.4 * toward, y: AT_PODIUM.y + 0.6, z: sz },
			width: 9.6 * push * k,
		};
	}
	return {
		at: { x: AT_PODIUM.x + 1.5 * s, y: 23, z: sz + 0.7 },
		look: { x: AT_PODIUM.x + 0.15 * s, y: AT_PODIUM.y + 0.5, z: sz },
		width: 7 * push * k,
	};
};

const smooth = (u: number) => {
	const v = Math.min(1, Math.max(0, u));
	return v * v * (3 - 2 * v);
};

// ---- the flashes ----------------------------------------------------------------

const unit = (n: number): number => {
	let h = Math.imul(n ^ 0x9e3779b9, 0x85ebca6b);
	h ^= h >>> 15;
	h = Math.imul(h, 0x2c1b3c6d);
	h ^= h >>> 12;
	return (h >>> 0) / 4294967296;
};
const FLASH_MS = 80;

// ---- all of it ------------------------------------------------------------------

export type PressMade = {
	eid: number;
	season: number;
	team: { tid: number; colors: [string, string, string] };
	// (Their heights, inches: for the cameras in close on him.)
	players: { pid: number; race?: Race; hgt?: number }[];
};

// Where everybody's hands are, as the picture last had them: for the jerseys
// and the balls in them (see pressVenue.ts).
export type Hands = { r: Pt3; l: Pt3 };
export type PressSeen = {
	// The players holding up their jerseys at t, and their hands.
	jerseys: (t: number) => { pid: number; hands: Hands }[];
	// The execs with a ball in their hands at t, and where it is.
	balls: (t: number) => Pt3[];
	// The table, the chairs at it, at t - or the podium.
	table: (t: number) => boolean;
	podium: (t: number) => boolean;
};

const MAX_ON_STAGE = 4;

export const stagePress = (
	made: PressMade,
	take: PressTake = takeOf(made.eid, made.team.tid),
): Pick<
	Authored,
	"show" | "ms" | "moment" | "aim" | "crew" | "flashes" | "cast" | "castAt"
> & {
	take: PressTake;
	plan: PressPlan;
	lineup: Lineup;
	seats: Seats;
	seen: PressSeen;
} => {
	const pids = made.players.slice(0, MAX_ON_STAGE).map((p) => p.pid);
	const P = planOf(take);
	const row = lineupOf(take, pids);
	const seats = seatsOf(take, pids);
	const execs = [...take.left, ...take.right];
	const star: Placed = row.find((r) => r.pid === pids[0])!;
	const starSeat: Placed = seats.find((r) => r.pid === pids[0])!;
	const gmSeat: Placed | undefined =
		seats.find((r) => r.exec === "gm") ?? seats.find((r) => r.exec);
	const talker = gmSeat?.pid;

	// Through the pictures: all of them looking at the photographers in
	// front, then off to one side, and the other.
	const lookTimes = (() => {
		const from = P.held + 600;
		const n = take.looks.length;
		const each = Math.max(900, (P.photo.t1 - from) / n);
		return take.looks.map((d, i) => ({
			t: i === 0 ? P.photo.t0 : from + i * each,
			d,
		}));
	})();
	const photoBeats = (
		at: Pt,
		pid: number,
		anims: { t0: number; anim: AnimName }[],
	): Beat[] => {
		// Every change of anim or of look a beat of its own.
		const marks = [
			...anims.map((a) => a.t0),
			...lookTimes.map((l) => l.t + ((pid * 37) % 5) * 40),
		]
			.filter((t) => t >= P.photo.t0 && t < P.photo.t1)
			.sort((a, b) => a - b);
		const uniq = [...new Set([P.photo.t0, ...marks])];
		return uniq.map((t0, i) => {
			const anim = [...anims].reverse().find((a) => a.t0 <= t0)!.anim;
			const look = [...lookTimes].reverse().find((l) => l.t <= t0)?.d ?? 0;
			return {
				t0,
				t1: uniq[i + 1] ?? P.photo.t1,
				at,
				yaw: toRoom(at) - look * LOOK_TURN,
				anim,
			};
		});
	};
	const heldAnim: AnimName = take.hold;
	const playerPhoto = (r: Placed): Beat[] =>
		photoBeats(
			r,
			r.pid,
			take.heldFirst && P.up < P.photo.t0
				? [{ t0: P.photo.t0, anim: heldAnim }]
				: [
						{ t0: P.photo.t0, anim: "jerseyLow" },
						{ t0: P.up, anim: "jerseyUp" },
						{ t0: P.held, anim: heldAnim },
					],
		);
	const execPhoto = (r: Placed, kind: ExecKind): Beat[] => {
		const anim: AnimName = take.balls
			? kind === "owner"
				? "execFolded"
				: "execBall"
			: kind === "coach"
				? "execFolded"
				: "easyGrin";
		// (Their hands free: clapping him as his jersey comes up.)
		const claps = !take.balls && P.up >= P.photo.t0;
		return photoBeats(r, r.pid, [
			{ t0: P.photo.t0, anim },
			...(claps
				? [
						{ t0: P.up + 150, anim: "clap" as const },
						{ t0: P.up + 1450, anim },
					]
				: []),
		]);
	};
	const talkBeats = (r: Placed): Beat[] => {
		const { t0, t1 } = P.talk;
		const talks =
			r.pid === talker ? P.gmTalk : r.pid === star.pid ? P.himTalk : undefined;
		const yaw = FACING + (r.x - CX) * -0.012;
		if (!talks) {
			return [{ t0, t1, at: r, yaw, anim: "sitListen" }];
		}
		const out: Beat[] = [];
		if (talks[0] > t0) {
			out.push({ t0, t1: talks[0], at: r, yaw, anim: "sitListen" });
		}
		out.push({ t0: talks[0], t1: talks[1], at: r, yaw, anim: "sitTalk" });
		if (talks[1] < t1) {
			out.push({ t0: talks[1], t1, at: r, yaw, anim: "sitListen" });
		}
		return out;
	};
	// The owner, not at the table: stood off the end of the riser.
	const OWNER_ASIDE: Pt = {
		x: take.left.includes("owner") ? RISER.x0 + 1.6 : RISER.x1 - 1.6,
		y: SEAT_Y + 0.4,
	};
	const ownerTalk = (): Beat[] => [
		{
			t0: P.talk.t0,
			t1: P.talk.t1,
			at: OWNER_ASIDE,
			yaw: yawTo(OWNER_ASIDE, starSeat) + 0.6,
			anim: "execFolded",
		},
	];
	// Or stood at the podium and either side of it: the general manager at
	// it first, then him in his place - swapped on the cut to him (see
	// talkCuts).
	const podium = take.talkAt === "podium";
	const beside = besideOf(take, pids);
	const swap = P.gmTalk[1] - 500;
	// (Turned a little in toward it.)
	const besideYaw = (at: Pt) => FACING - 0.42 * Math.sign(AT_PODIUM.x - at.x);
	const standTalk = (pid: number): Beat[] => {
		const { t0, t1 } = P.talk;
		const mine =
			beside.players.find((p) => p.pid === pid) ??
			beside.rest.find((p) => p.pid === pid);
		const at = mine?.at ?? { x: CX + 60, y: 40 };
		if (pid === beside.gm) {
			const after = beside.players[0]!.at;
			return [
				{ t0, t1: swap, at: AT_PODIUM, yaw: FACING, anim: "podiumTalk" },
				{ t0: swap, t1, at: after, yaw: besideYaw(after), anim: "execFolded" },
			];
		}
		if (pid === star.pid) {
			return [
				{ t0, t1: swap, at, yaw: besideYaw(at), anim: "easyGrin" },
				{ t0: swap, t1, at: AT_PODIUM, yaw: FACING, anim: "podiumTalk" },
			];
		}
		const exec = beside.rest.find((p) => p.pid === pid)?.exec;
		const anim: AnimName = exec === "coach" ? "execFolded" : "easyGrin";
		return [{ t0, t1, at, yaw: besideYaw(at), anim }];
	};
	// Each one's two parts, kept apart: across the cut between them nobody
	// is eased from sitting to standing.
	type Parts = { photo: Beat[]; talk: Beat[] };
	const whole = (photo: Beat[], talk: Beat[]): Parts => ({ photo, talk });
	const beats = new Map<number, Parts>();
	for (const r of row) {
		const seat = seats.find((s) => s.pid === r.pid);
		const photo = r.exec ? execPhoto(r, r.exec) : playerPhoto(r);
		const talk = podium
			? standTalk(r.pid)
			: seat
				? talkBeats(seat)
				: r.exec === "owner"
					? ownerTalk()
					: talkBeats({ pid: r.pid, x: r.x, y: SEAT_Y });
		beats.set(r.pid, whole(photo, talk));
	}
	// The photographers: on their knees and on their feet in front of the
	// riser for the pictures; off to the sides at the table.
	const pit = pitOf(take);
	const press = pit.map((p, i) => {
		const pid = PRESS_PID - i;
		const aim = row[Math.floor(unit(i * 13 + made.eid) * row.length)]!;
		const side = SIDE_SPOTS[i % SIDE_SPOTS.length]!;
		const sideAim = podium
			? AT_PODIUM
			: seats[i % 2 === 0 ? 0 : seats.length - 1]!;
		const photo: Beat[] = [
			{
				t0: P.photo.t0,
				t1: P.photo.t1,
				at: p.at,
				yaw: yawTo(p.at, aim),
				anim: p.kneel ? "kneelShoot" : "shootStand",
			},
		];
		// (Three of them about at the table: the rest gone.)
		const talk: Beat[] = [
			{
				t0: P.talk.t0,
				t1: P.talk.t1,
				at: i < SIDE_SPOTS.length ? side : { x: CX + 60, y: 40 },
				yaw: yawTo(side, sideAim),
				anim: i % 2 === 0 ? "shootStand" : "easy",
			},
		];
		return { pid, beats: whole(photo, talk), kneel: p.kneel };
	});

	const tl: CourtTimeline = {
		tracks: new Map(),
		ball: [],
		fx: [],
		beats: [],
		poss: [[0, 1]],
		cuts: [],
		shots: [],
		tension: [],
		fast: [],
		warmup: true,
		end: P.ms,
	};

	// What the picture last had of them, for their hands (see PressSeen).
	const bodies = new Map<number, Body>();
	const execBodies = new Map(
		execs.map((k) => [
			EXEC_PID[k],
			crewBody(execOf(k, made.team.tid, made.season, made.team.colors)),
		]),
	);
	const photoLast = P.photo.t1 >= P.talk.t1;
	const partOf = (parts: Parts, t: number) => {
		if (t >= P.photo.t0 && t < P.photo.t1) {
			return parts.photo;
		}
		if (t >= P.talk.t0 && t < P.talk.t1) {
			return parts.talk;
		}
		// (Before it starts, the first; held at its end, the last.)
		return t >= P.ms === photoLast ? parts.photo : parts.talk;
	};
	const stateOf = (pid: number, t: number) =>
		castState(pid, partOf(beats.get(pid)!, t), t, groundAt);
	const handsOf = (st: PlayerState, body: Body): Hands => {
		const placed = withBody(st, body);
		const sk = skeleton(body, poseOf(placed));
		return {
			r: bodyPoint(placed, sk.armR.end),
			l: bodyPoint(placed, sk.armL.end),
		};
	};
	const inPhoto = (t: number) => t >= P.photo.t0 && t < P.photo.t1;
	const talking = (t: number) =>
		!inPhoto(t) && !(t >= P.ms && take.order === "talkFirst");
	const seen: PressSeen = {
		jerseys: (t) =>
			inPhoto(t) || (t >= P.ms && take.order === "talkFirst")
				? pids.flatMap((pid) => {
						const body = bodies.get(pid);
						return body ? [{ pid, hands: handsOf(stateOf(pid, t), body) }] : [];
					})
				: [],
		balls: (t) =>
			take.balls && (inPhoto(t) || (t >= P.ms && take.order === "talkFirst"))
				? execs
						.filter((k) => k !== "owner")
						.map((k) => {
							const pid = EXEC_PID[k];
							const st = withBody(stateOf(pid, t), execBodies.get(pid)!);
							const sk = skeleton(execBodies.get(pid)!, poseOf(st));
							const h = sk.armR.end;
							return bodyPoint(st, { f: h.f + 0.12, s: h.s, u: h.u + 0.42 });
						})
				: [],
		table: (t) => !podium && talking(t),
		podium: (t) => podium && talking(t),
	};

	const k0 = (narrow: boolean) => (narrow ? 0.82 : 1);
	const photoCuts = (() => {
		const n = take.shots.length;
		const len = P.photo.t1 - P.photo.t0;
		// (The first and last held a little longer.)
		const w = take.shots.map((_, i) => (i === 0 || i === n - 1 ? 1.25 : 1));
		const sum = w.reduce((a, b) => a + b, 0);
		let t = P.photo.t0;
		return take.shots.map((shot, i) => {
			const t0 = t;
			t += (len * w[i]!) / sum;
			return { shot, t0, t1: i === n - 1 ? P.photo.t1 : t };
		});
	})();
	type TalkCut = { shot: "table" | "two" | "him"; t0: number; t1: number };
	const talkCuts = ((): TalkCut[] => {
		const { t0, t1 } = P.talk;
		const first = swap;
		const mid = P.himTalk[0] + 1400;
		if (podium) {
			// (All of them; in on the general manager at it; and him.)
			const gmIn = t0 + Math.round((first - t0) * 0.5);
			return [
				{ shot: "table", t0, t1: gmIn },
				{ shot: "him", t0: gmIn, t1: first },
				take.twoShot
					? { shot: "two", t0: first, t1: mid }
					: { shot: "table", t0: first, t1: first + 1700 },
				{ shot: "him", t0: take.twoShot ? mid : first + 1700, t1 },
			];
		}
		return take.twoShot && gmSeat
			? [
					{ shot: "table", t0, t1: first },
					{ shot: "two", t0: first, t1: mid },
					{ shot: "him", t0: mid, t1 },
				]
			: [
					{ shot: "table", t0, t1: first },
					{ shot: "him", t0: first, t1 },
				];
	})();
	const gmTall =
		execOf(beside.gmKind, made.team.tid, made.season, made.team.colors).hgt /
		12;
	const starTall = (made.players[0]?.hgt ?? 78) / 12;
	const standXs = [
		AT_PODIUM.x,
		...beside.players.map((p) => p.at.x),
		...beside.rest.map((p) => p.at.x),
	];
	const cameraAt = (t: number, narrow: boolean): Cam => {
		const k = k0(narrow);
		const tt = Math.min(t, P.ms - 1);
		if (inPhoto(tt)) {
			const c = photoCuts.find((c) => tt < c.t1) ?? photoCuts.at(-1)!;
			return photoCam(
				c.shot,
				take,
				row,
				star,
				smooth((tt - c.t0) / (c.t1 - c.t0)),
				k,
			);
		}
		const c = talkCuts.find((c) => tt < c.t1) ?? talkCuts.at(-1)!;
		if (podium) {
			return podiumCam(
				c.shot,
				take,
				standXs,
				beside.players[0]!.at,
				tt < swap ? gmTall : starTall,
				smooth((tt - c.t0) / (c.t1 - c.t0)),
				k,
			);
		}
		return talkCam(
			c.shot,
			take,
			seats,
			starSeat,
			gmSeat,
			smooth((tt - c.t0) / (c.t1 - c.t0)),
			k,
		);
	};

	// The flashes: going off all through the pictures, more once they are
	// held up - from the cameras of the photographers in front; at the table,
	// now and then from the sides.
	const flashes = (t: number): Pt3[] => {
		const slot = Math.floor(t / FLASH_MS);
		const out: Pt3[] = [];
		const photo = inPhoto(t);
		const share = photo ? (t >= P.held ? 0.42 : 0.2) : 0.05;
		for (let i = 0; i < 3; i++) {
			if (unit(slot * 3 + i) >= share) {
				continue;
			}
			const n = Math.floor(unit(slot * 7 + i + 1000) * press.length);
			const p = press[n]!;
			const st = castState(p.pid, partOf(p.beats, t), t, groundAt);
			if (st.x > CX + 40) {
				continue;
			}
			out.push(
				bodyPoint(st, {
					f: 0.9,
					s: 0,
					u: p.kneel && photo ? 3.4 : 5.45,
				}),
			);
		}
		return out;
	};

	return {
		show: { tl, parts: [tl] },
		ms: P.ms,
		moment: (t, bodyFor) => ({
			t,
			players: pids.map((pid) => {
				const body = bodyFor(pid);
				bodies.set(pid, body);
				return withBody(stateOf(pid, t), body);
			}),
			ball: NO_BALL,
		}),
		aim: (t, narrow) => shotOf(cameraAt(t, narrow)),
		crew: () => false,
		flashes,
		cast: [
			...execs.map((k) =>
				execOf(k, made.team.tid, made.season, made.team.colors),
			),
			...press.map((p) =>
				photographer(p.pid, `press|photo|${made.eid}|${p.pid}`),
			),
		],
		castAt: (t) => [
			...execs.map((k) => stateOf(EXEC_PID[k], t)),
			...press.map((p) => castState(p.pid, partOf(p.beats, t), t, groundAt)),
		],
		take,
		plan: P,
		lineup: row,
		seats,
		seen,
	};
};
