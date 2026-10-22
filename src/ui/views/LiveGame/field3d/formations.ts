import type { Group } from "./personnel.ts";
import type { Move } from "./poses.ts";

// HOW THE TWENTY-TWO LINE UP, the way they do on Sundays - in feet, measured
// off the ball, so a play from the left hash looks like one.
//
// The offense first: five linemen with two feet between each and the next,
// the guards' and tackles' heads at the center's hip, the tight end two feet
// outside his tackle; the quarterback under center or five yards back in the
// gun; the backs where each set puts them; the receivers split out by where
// they stand from the sideline, not from the ball - the wide one a few yards
// outside the numbers, the field side wider than the boundary when the ball
// is on a hash. Then the defense, aligned on them: the front shaded on the
// linemen, the linebackers five yards off, a corner over each wide man, the
// nickel over the slot, the safeties two-high over the hashes or one deep in
// the middle.

// A man's spot before the snap, the way a coach draws it: `depth` feet behind
// the line on his own side (his feet), `across` feet to the offense's right
// of the ball - or, split out, `wide` feet in from the sideline on his side
// (`side`: -1 the offense's left, 1 its right). `pos` the group the sim fills
// it from (see personnel.ts).
export type Align = {
	role: Role;
	pos: Group;
	depth: number;
	across?: number;
	wide?: { side: -1 | 1; ft: number };
	move: Move;
	// His stance the other way round (a lineman on the left down on his
	// left hand).
	mirrored?: boolean;
};

export type Role =
	// The offense.
	| "C"
	| "LG"
	| "RG"
	| "LT"
	| "RT"
	| "TE"
	| "TE2"
	| "QB"
	| "RB"
	| "FB"
	| "X"
	| "Z"
	| "SL"
	| "SL2"
	// The defense.
	| "DE"
	| "DT"
	| "NT"
	| "SAM"
	| "MIKE"
	| "WILL"
	| "BUCK"
	| "CB"
	| "NB"
	| "FS"
	| "SS"
	// The kicking game.
	| "K"
	| "P"
	| "H"
	| "LS"
	| "KR"
	| "PR"
	| "UP"
	| "GUNNER"
	| "JAMMER"
	| "WING"
	| "COVER"
	| "FRONT"
	| "RUSH"
	| "WALL";

// The line: the center over the ball, guards and tackles shaded back off it.
export const C_DEPTH = 1.4;
const G_ACROSS = 4.6;
const G_DEPTH = 2.6;
const T_ACROSS = 9.3;
const T_DEPTH = 3.0;
const TE_ACROSS = 14;
// Five yards back in the gun; the pistol four, with the back behind him.
const GUN_DEPTH = 15;
const PISTOL_DEPTH = 12;
const UNDER_DEPTH = 3.3;

const line = (): Align[] => [
	{ role: "C", pos: "OL", depth: C_DEPTH, across: 0, move: "center" },
	{
		role: "LG",
		pos: "OL",
		depth: G_DEPTH,
		across: -G_ACROSS,
		move: "threePoint",
		mirrored: true,
	},
	{
		role: "RG",
		pos: "OL",
		depth: G_DEPTH,
		across: G_ACROSS,
		move: "threePoint",
	},
	{
		role: "LT",
		pos: "OL",
		depth: T_DEPTH,
		across: -T_ACROSS,
		move: "threePoint",
		mirrored: true,
	},
	{
		role: "RT",
		pos: "OL",
		depth: T_DEPTH,
		across: T_ACROSS,
		move: "threePoint",
	},
];

const te = (side: -1 | 1, role: Role = "TE"): Align => ({
	role,
	pos: "TE",
	depth: T_DEPTH,
	across: side * TE_ACROSS,
	move: "threePoint",
	...(side < 0 ? { mirrored: true } : {}),
});
const wideOut = (
	role: Role,
	side: -1 | 1,
	ft: number,
	onLine: boolean,
): Align => ({
	role,
	pos: "WR",
	depth: onLine ? 1.6 : 4.8,
	wide: { side, ft },
	move: "receiverStance",
	...(side < 0 ? { mirrored: true } : {}),
});
// A slot receiver: off the line, halfway out to the wide man.
const slot = (role: Role, side: -1 | 1, across: number): Align => ({
	role,
	pos: "WR",
	depth: 4.8,
	across: side * across,
	move: "receiverStance",
	...(side < 0 ? { mirrored: true } : {}),
});

export type OffenseSet = {
	name: string;
	slots: Align[];
	// Nobody in the backfield to protect.
	empty?: boolean;
	// Extra bodies instead of receivers: short yardage, the goal line.
	heavy?: boolean;
	// The quarterback in the gun (or the pistol) rather than under center.
	gun?: boolean;
};

// Eleven personnel in the gun: the set most football is played from.
const SHOTGUN: OffenseSet = {
	name: "Shotgun",
	gun: true,
	slots: [
		...line(),
		te(1),
		{ role: "QB", pos: "QB", depth: GUN_DEPTH, across: 0, move: "shotgun" },
		{
			role: "RB",
			pos: "RB",
			depth: GUN_DEPTH,
			across: -4.8,
			move: "backStance",
		},
		wideOut("X", -1, 27, true),
		wideOut("Z", 1, 33, false),
		slot("SL", -1, 24),
	],
};

// Trips: three receivers to the field.
const TRIPS: OffenseSet = {
	name: "Trips",
	gun: true,
	slots: [
		...line(),
		te(-1),
		{ role: "QB", pos: "QB", depth: GUN_DEPTH, across: 0, move: "shotgun" },
		{
			role: "RB",
			pos: "RB",
			depth: GUN_DEPTH,
			across: -4.8,
			move: "backStance",
		},
		wideOut("Z", 1, 24, true),
		slot("SL", 1, 20),
		slot("SL2", 1, 32),
	],
};

// Empty: five out, nobody home.
const EMPTY: OffenseSet = {
	name: "Empty",
	gun: true,
	empty: true,
	slots: [
		...line(),
		{ role: "QB", pos: "QB", depth: GUN_DEPTH, across: 0, move: "shotgun" },
		wideOut("X", -1, 27, true),
		wideOut("Z", 1, 24, true),
		{ ...slot("SL", -1, 18), pos: "TE" },
		slot("SL", 1, 31),
		{ ...slot("SL2", 1, 19), pos: "RB" },
	],
};

// The pistol: the quarterback four yards back, the back right behind him.
const PISTOL: OffenseSet = {
	name: "Pistol",
	gun: true,
	slots: [
		...line(),
		te(1),
		{ role: "QB", pos: "QB", depth: PISTOL_DEPTH, across: 0, move: "shotgun" },
		{ role: "RB", pos: "RB", depth: 21, across: 0, move: "backStance" },
		wideOut("X", -1, 27, true),
		wideOut("Z", 1, 33, false),
		slot("SL", -1, 24),
	],
};

// Singleback, under center: a tight end, three receivers, the back seven
// yards deep.
const SINGLEBACK: OffenseSet = {
	name: "Singleback",
	slots: [
		...line(),
		te(1),
		{
			role: "QB",
			pos: "QB",
			depth: UNDER_DEPTH,
			across: 0,
			move: "underCenter",
		},
		{ role: "RB", pos: "RB", depth: 21, across: 0, move: "backStance" },
		wideOut("X", -1, 27, true),
		wideOut("Z", 1, 33, false),
		slot("SL", -1, 24),
	],
};

// Ace: two tight ends, two receivers.
const ACE: OffenseSet = {
	name: "Ace",
	slots: [
		...line(),
		te(1),
		te(-1, "TE2"),
		{
			role: "QB",
			pos: "QB",
			depth: UNDER_DEPTH,
			across: 0,
			move: "underCenter",
		},
		{ role: "RB", pos: "RB", depth: 21, across: 0, move: "backStance" },
		wideOut("X", -1, 30, true),
		wideOut("Z", 1, 33, false),
	],
};

// The I: a fullback in front of the tailback. Downhill football.
const I_FORM: OffenseSet = {
	name: "I-Form",
	slots: [
		...line(),
		te(1),
		{
			role: "QB",
			pos: "QB",
			depth: UNDER_DEPTH,
			across: 0,
			move: "underCenter",
		},
		{ role: "RB", pos: "RB", depth: 21.5, across: 0, move: "backStance" },
		{ role: "FB", pos: "RB", depth: 12.5, across: 0, move: "backStance" },
		wideOut("X", -1, 30, true),
		wideOut("Z", 1, 33, false),
	],
};

// Heavy: two tight ends and a fullback, one receiver to keep a corner
// honest. Short yardage and the goal line.
const HEAVY: OffenseSet = {
	name: "Heavy",
	heavy: true,
	slots: [
		...line(),
		te(1),
		te(-1, "TE2"),
		{
			role: "QB",
			pos: "QB",
			depth: UNDER_DEPTH,
			across: 0,
			move: "underCenter",
		},
		{ role: "RB", pos: "RB", depth: 21.5, across: 0, move: "backStance" },
		{ role: "FB", pos: "RB", depth: 12.5, across: 3, move: "backStance" },
		wideOut("X", -1, 36, true),
	],
};

// Victory: the quarterback under center to take a knee, a back either side
// of him to keep anyone off, one deep in case.
const VICTORY: OffenseSet = {
	name: "Victory",
	heavy: true,
	slots: [
		...line(),
		te(1),
		te(-1, "TE2"),
		{
			role: "QB",
			pos: "QB",
			depth: UNDER_DEPTH,
			across: 0,
			move: "underCenter",
		},
		{ role: "RB", pos: "RB", depth: 24, across: 0, move: "safetyStance" },
		{ role: "FB", pos: "RB", depth: 7, across: -4, move: "backStance" },
		{ role: "SL", pos: "WR", depth: 7, across: 4, move: "backStance" },
	],
};

export const OFFENSE_SETS = {
	shotgun: SHOTGUN,
	trips: TRIPS,
	empty: EMPTY,
	pistol: PISTOL,
	singleback: SINGLEBACK,
	ace: ACE,
	iForm: I_FORM,
	heavy: HEAVY,
	victory: VICTORY,
} satisfies Record<string, OffenseSet>;
export type OffenseSetName = keyof typeof OFFENSE_SETS;

// What the offense lines up in: the situation decides most of it - nobody
// runs empty on the goal line - and the rest is variety, so a drive does not
// look like the same snap eleven times. (`r` a number from 0 to 1, seeded by
// the play.)
export const chooseOffenseSet = (
	{
		running,
		down,
		toGo,
		scrimmage,
	}: { running: boolean; down: number; toGo: number; scrimmage: number },
	r: number,
): OffenseSetName => {
	if (scrimmage >= 97 || (toGo <= 1 && running)) {
		return r < 0.65 ? "heavy" : "iForm";
	}
	if (toGo <= 2) {
		return r < 0.4 ? "heavy" : r < 0.7 ? "iForm" : "ace";
	}
	if (running) {
		return r < 0.3
			? "singleback"
			: r < 0.55
				? "shotgun"
				: r < 0.7
					? "pistol"
					: r < 0.85
						? "iForm"
						: "ace";
	}
	if (down >= 3 && toGo >= 7) {
		return r < 0.3 ? "empty" : r < 0.6 ? "trips" : "shotgun";
	}
	return r < 0.45
		? "shotgun"
		: r < 0.62
			? "trips"
			: r < 0.74
				? "singleback"
				: r < 0.86
					? "pistol"
					: r < 0.94
						? "ace"
						: "empty";
};

// ---- the defense -----------------------------------------------------------

// Where the offense stands, for the defense to align on: each man's spot
// across the field (feet to the offense's right of the ball) and how far off
// the line.
export type Offset = { role: Role; pos: Group; across: number; depth: number };

// The defense's call before the snap: its front and whether it shows two
// safeties deep or one. The fronts: four down and three linebackers (base),
// three down and four (threeFour), five defensive backs (nickel), six
// (dime), and the goal line's six down.
export type DefenseCall = {
	front: "base" | "threeFour" | "nickel" | "dime" | "goalLine";
	shell: "two" | "one";
	// Up in the receivers' faces at the line rather than off.
	press: boolean;
};

// Off the ball (feet, on its own side): a lineman's feet, a linebacker, a
// corner off or pressed, a safety deep.
const DL_DEPTH = 3.4;
const LB_DEPTH = 15;
const CB_OFF = 20;
const CB_PRESS = 3.2;
const NB_DEPTH = 15;
const S_DEEP = 39;
const S_MIDDLE = 45;
const S_BOX = 24;

// The defense, aligned on the offense: the front shaded on the linemen, the
// linebackers over the box, a corner on the widest man each side, the nickel
// and dime over the slots, the safeties two-high over the hashes or one deep
// with the other down in the box to the strength. `hashes` the spots across
// the field (feet to the offense's right of the ball) of the two hash marks,
// left and right.
export const alignDefense = (
	offense: Offset[],
	call: DefenseCall,
	hashes: [number, number],
): Align[] => {
	const out: Align[] = [];
	const at = (
		role: Role,
		pos: Group,
		across: number,
		depth: number,
		move: Move,
		mirrored = false,
	) =>
		out.push({
			role,
			pos,
			depth,
			across,
			move,
			...(mirrored ? { mirrored: true } : {}),
		});
	// The strength: the side with the tight end (or more receivers).
	const tes = offense.filter((o) => o.pos === "TE" && Math.abs(o.across) < 20);
	const weight = offense.reduce(
		(s, o) => (o.role === "QB" || o.role === "C" ? s : s + Math.sign(o.across)),
		0,
	);
	const strong: -1 | 1 =
		tes.length === 1
			? (Math.sign(tes[0]!.across) as -1 | 1)
			: weight < 0
				? -1
				: 1;
	const tackle = (side: -1 | 1) =>
		Math.max(
			...offense
				.filter(
					(o) => o.pos !== "WR" && o.depth < 6 && Math.sign(o.across) === side,
				)
				.map((o) => Math.abs(o.across)),
			T_ACROSS,
		);
	if (call.front === "goalLine") {
		for (const [i, a] of [
			-T_ACROSS - 2.5,
			-G_ACROSS - 1.8,
			-1.8,
			1.8,
			G_ACROSS + 1.8,
			T_ACROSS + 2.5,
		].entries()) {
			at(
				i === 0 || i === 5 ? "DE" : "DT",
				"DL",
				a,
				DL_DEPTH - 0.4,
				"dlThree",
				a < 0,
			);
		}
		at("MIKE", "LB", -4, 10, "backerStance");
		at("WILL", "LB", 4, 10, "backerStance");
	} else if (call.front === "threeFour") {
		// Three down: the nose head up on the center, the ends in the
		// tackles' inside shoulders; the outside linebackers up on the edge,
		// the inside two over the guards.
		at("DE", "DL", -(T_ACROSS - 1.3), DL_DEPTH, "dlThree", true);
		at("NT", "DL", 0, DL_DEPTH - 0.2, "dlThree");
		at("DE", "DL", T_ACROSS - 1.3, DL_DEPTH, "dlThree");
		at("SAM", "LB", strong * (tackle(strong) + 4.5), 4.5, "backerStance");
		at(
			"WILL",
			"LB",
			-strong * (tackle(-strong as -1 | 1) + 4.5),
			4.5,
			"backerStance",
		);
		at("MIKE", "LB", strong * G_ACROSS, LB_DEPTH, "backerStance");
		at("BUCK", "LB", -strong * G_ACROSS, LB_DEPTH, "backerStance");
	} else {
		// Four down: the ends outside the tackles (or the tight end), a
		// three-technique on the strong guard's outside shoulder, a
		// one-technique on the center's weak shade.
		const endAt = (side: -1 | 1) => side * (tackle(side) + 2.4);
		at("DE", "DL", endAt(-1), DL_DEPTH, "dlThree", true);
		at("DT", "DL", -strong * 1.9, DL_DEPTH, "dlThree", strong > 0);
		at("DT", "DL", strong * (G_ACROSS + 1.9), DL_DEPTH, "dlThree", strong < 0);
		at("DE", "DL", endAt(1), DL_DEPTH, "dlThree");
		if (call.front === "base") {
			at("MIKE", "LB", strong * 1.5, LB_DEPTH, "backerStance");
			at("WILL", "LB", -strong * 10.5, LB_DEPTH - 1, "backerStance");
			at(
				"SAM",
				"LB",
				strong * (tackle(strong) + 2),
				LB_DEPTH - 3,
				"backerStance",
			);
		} else if (call.front === "nickel") {
			at("MIKE", "LB", strong * 4.5, LB_DEPTH, "backerStance");
			at("WILL", "LB", -strong * 5.5, LB_DEPTH, "backerStance");
		} else {
			at("MIKE", "LB", 0, LB_DEPTH + 1, "backerStance");
		}
	}
	// The receivers, outside in each side: a corner on the widest, the
	// nickel (and dime) on the next ones in.
	const receivers = (side: -1 | 1) =>
		offense
			.filter(
				(o) =>
					Math.sign(o.across) === side &&
					o.role !== "C" &&
					o.role !== "QB" &&
					(o.pos === "WR" || Math.abs(o.across) > TE_ACROSS + 3),
			)
			.sort((a, b) => Math.abs(b.across) - Math.abs(a.across));
	const nickels = call.front === "nickel" ? 1 : call.front === "dime" ? 2 : 0;
	let nbLeft = nickels;
	const slotted: Offset[] = [];
	for (const side of [-1, 1] as const) {
		const rs = receivers(side);
		const outside = rs[0];
		if (call.front === "goalLine") {
			at(
				"CB",
				"CB",
				outside ? outside.across - side * 1 : side * 30,
				outside ? 7 : 10,
				"cornerStance",
			);
			continue;
		}
		if (outside) {
			// Off, seven yards deep and a step outside him; pressed, right on
			// him, a step inside.
			const across = outside.across + side * (call.press ? -1.2 : 1.5);
			at(
				"CB",
				"CB",
				across,
				call.press ? CB_PRESS : CB_OFF,
				"cornerStance",
				side < 0,
			);
		} else {
			at("CB", "CB", side * 36, CB_OFF + 6, "cornerStance", side < 0);
		}
		slotted.push(...rs.slice(1));
	}
	slotted.sort((a, b) => Math.abs(a.across) - Math.abs(b.across));
	for (const r of slotted) {
		if (nbLeft <= 0) {
			break;
		}
		at(
			"NB",
			"CB",
			r.across - Math.sign(r.across) * 2.5,
			NB_DEPTH - 3,
			"cornerStance",
			r.across < 0,
		);
		nbLeft -= 1;
	}
	for (; nbLeft > 0; nbLeft--) {
		at("NB", "CB", (nbLeft % 2 === 0 ? -1 : 1) * 22, NB_DEPTH, "backerStance");
	}
	if (call.front !== "goalLine") {
		if (call.shell === "two") {
			at("FS", "S", hashes[0] - 2, S_DEEP, "safetyStance");
			at("SS", "S", hashes[1] + 2, S_DEEP + 2, "safetyStance");
		} else {
			at("FS", "S", 0, S_MIDDLE, "safetyStance");
			at("SS", "S", strong * 15, S_BOX, "backerStance");
		}
	} else {
		at("SS", "S", -strong * 9, 13, "backerStance");
	}
	return out;
};

// The defense's answer to the offense on the field and the down: three
// receivers get nickel, four dime, two (or a heavy set) a base front - four
// down or three - and the goal line everybody in the box; two-high or
// one-high, press or off, by the throw of `r` (0 to 1, seeded by the play).
export const chooseDefenseCall = (
	set: OffenseSet,
	{ down, toGo, scrimmage }: { down: number; toGo: number; scrimmage: number },
	r: number,
): DefenseCall => {
	const wides = set.slots.filter(
		(s) => s.pos === "WR" || s.wide !== undefined,
	).length;
	const r2 = (r * 7.31) % 1;
	const r3 = (r * 13.17) % 1;
	const front: DefenseCall["front"] =
		scrimmage >= 97 || (set.heavy && toGo <= 2)
			? "goalLine"
			: set.heavy || wides <= 2
				? r3 < 0.35
					? "threeFour"
					: "base"
				: set.empty || wides >= 4 || (down >= 3 && toGo >= 10)
					? "dime"
					: "nickel";
	return {
		front,
		shell:
			front === "goalLine"
				? "one"
				: r < (down >= 3 && toGo >= 7 ? 0.65 : 0.5)
					? "two"
					: "one",
		press: r2 < 0.3,
	};
};

// ---- the kicking game ------------------------------------------------------

// A kickoff, today's way: the kicker at the ball on its tee, the other ten
// strung across the receiving team's 40 - five either side - and the
// receiving team's nine in their setup zone five to ten yards off them, two
// returners back in the landing zone. `depth` here is off the ball's own
// yard line, `across` off the middle of the field.
// (The sim's coverage: five linebackers, three safeties, two corners - the
// corners outside, the safeties next in.)
const KICK_LINE = [-66, -52, -38, -24, -10, 10, 24, 38, 52, 66];
const KICK_LINE_POS: Group[] = [
	"CB",
	"S",
	"LB",
	"LB",
	"LB",
	"LB",
	"LB",
	"S",
	"S",
	"CB",
];
export const KICKOFF_COVER = (lineAt: number): Align[] => [
	{ role: "K", pos: "K", depth: 24, across: -6, move: "stand" },
	...KICK_LINE.map((across, i): Align => ({
		role: "COVER",
		pos: KICK_LINE_POS[i]!,
		depth: -lineAt,
		across,
		move: "receiverStance",
		...(across < 0 ? { mirrored: true } : {}),
	})),
];
// The receiving team (its depths, like any defense's, off the ball on its
// own side): its setup zone `zone` feet off the ball's yard line, the
// returners `back` feet. (The sim's: five linebackers and four safeties in
// front of two returners.)
const SETUP = [-60, -45, -30, -15, 0, 15, 30, 45, 60];
const SETUP_POS: Group[] = ["S", "S", "LB", "LB", "LB", "LB", "LB", "S", "S"];
export const KICKOFF_RECEIVE = (zone: number, back: number): Align[] => [
	...SETUP.map((across, i): Align => ({
		role: "FRONT",
		pos: SETUP_POS[i]!,
		depth: zone + (i % 2) * 6,
		across,
		move: "backerStance",
	})),
	{ role: "KR", pos: "KR", depth: back, across: -12, move: "safetyStance" },
	{ role: "KR", pos: "KR", depth: back + 9, across: 10, move: "safetyStance" },
];

// The spread punt: the snapper, two guards and two tackles tight, the
// gunners split wide on the line, a shield of three five yards back, the
// punter fifteen yards deep. (The sim's: seven linemen, a back - the
// personal protector in the middle of the shield - two corners, the punter.)
export const PUNT_TEAM: Align[] = [
	{ role: "LS", pos: "OL", depth: C_DEPTH, across: 0, move: "center" },
	{
		role: "LG",
		pos: "OL",
		depth: 2.2,
		across: -4,
		move: "threePoint",
		mirrored: true,
	},
	{ role: "RG", pos: "OL", depth: 2.2, across: 4, move: "threePoint" },
	{ role: "LT", pos: "OL", depth: 2.6, across: -8.2, move: "backerStance" },
	{ role: "RT", pos: "OL", depth: 2.6, across: 8.2, move: "backerStance" },
	{ role: "UP", pos: "OL", depth: 16, across: -5.5, move: "backerStance" },
	{ role: "UP", pos: "RB", depth: 16, across: 0, move: "backerStance" },
	{ role: "UP", pos: "OL", depth: 16, across: 5.5, move: "backerStance" },
	{
		role: "GUNNER",
		pos: "CB",
		depth: 1.6,
		wide: { side: -1, ft: 24 },
		move: "receiverStance",
		mirrored: true,
	},
	{
		role: "GUNNER",
		pos: "CB",
		depth: 1.6,
		wide: { side: 1, ft: 24 },
		move: "receiverStance",
	},
	{ role: "P", pos: "P", depth: 45, across: 0, move: "shotgun" },
];
// The return: six up on the line, a man on each gunner, two holding up
// behind them, the returner where the punt will come down (`back` feet).
// (The sim's: six linemen, four safeties, the returner.)
export const PUNT_RETURN = (back: number): Align[] => [
	...[-14, -7, -2, 2, 7, 14].map((across): Align => ({
		role: "RUSH",
		pos: "DL",
		depth: DL_DEPTH,
		across,
		move: Math.abs(across) < 5 ? "dlThree" : "backerStance",
		...(across < 0 ? { mirrored: true } : {}),
	})),
	{
		role: "JAMMER",
		pos: "S",
		depth: 3,
		wide: { side: -1, ft: 21 },
		move: "cornerStance",
	},
	{
		role: "JAMMER",
		pos: "S",
		depth: 3,
		wide: { side: 1, ft: 21 },
		move: "cornerStance",
	},
	{ role: "WALL", pos: "S", depth: 21, across: -9, move: "backerStance" },
	{ role: "WALL", pos: "S", depth: 24, across: 9, move: "backerStance" },
	{ role: "PR", pos: "PR", depth: back, across: 0, move: "safetyStance" },
];

// A field goal or a try: the snapper and a wall of eight shoulder to
// shoulder, a wing on each end, the holder down on a knee seven yards back,
// the kicker behind him and off to the side. (The sim's: nine linemen, the
// punter holding, the kicker.)
export const KICK_TEAM: Align[] = [
	{ role: "LS", pos: "OL", depth: C_DEPTH, across: 0, move: "center" },
	...[-3.6, 3.6, -7.2, 7.2, -10.8, 10.8].map((across): Align => ({
		role: "FRONT",
		pos: "OL",
		depth: 2.4,
		across,
		move: "threePoint",
		...(across < 0 ? { mirrored: true } : {}),
	})),
	{ role: "WING", pos: "OL", depth: 4.2, across: -13.6, move: "backerStance" },
	{ role: "WING", pos: "OL", depth: 4.2, across: 13.6, move: "backerStance" },
	{ role: "H", pos: "P", depth: 21, across: 0, move: "holdKneel" },
	{ role: "K", pos: "K", depth: 29, across: -6, move: "kickReady" },
];
// The block: everybody at the line but a couple, a man up high in the
// middle to leap at it. (The sim's: six linemen, three linebackers, two
// safeties.)
export const KICK_BLOCK: Align[] = [
	...[-14, -10, -6, -2, 2, 6, 10, 14].map((across): Align => ({
		role: "RUSH",
		pos: Math.abs(across) < 12 ? "DL" : "LB",
		depth: DL_DEPTH,
		across,
		move: "dlThree",
		...(across < 0 ? { mirrored: true } : {}),
	})),
	{ role: "CB", pos: "S", depth: 3.4, across: -20, move: "cornerStance" },
	{ role: "CB", pos: "S", depth: 3.4, across: 20, move: "cornerStance" },
	{ role: "MIKE", pos: "LB", depth: 10, across: 0, move: "backerStance" },
];
