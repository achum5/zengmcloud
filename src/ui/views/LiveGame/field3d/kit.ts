import type { HelmetLook } from "../../../../common/types.ts";
import type { HairCut, HairStyle } from "../court3d/faces.ts";
import { makeCourtRng } from "../courtRng.ts";
import { luminance, shade } from "./color.ts";

// WHAT A FOOTBALL PLAYER WEARS: his team's uniform - helmet, jersey, pants,
// socks - and his own gear on top of it, decided by who he is and what he
// plays: a lineman's cage of a facemask and taped hands, a receiver's open
// mask and gloves, the quarterback's bare throwing hand.
//
// The pro way round: the home team in its color, the visitors in white -
// both in the same helmet they always wear.

export type FieldKit = {
	helmet: string;
	// Down the middle of the helmet, front to back: its color, and the thin
	// edge either side of it (none, if unset).
	helmetStripe?: string;
	helmetStripeEdge?: string;
	facemask: string;
	jersey: string;
	// The band round the neck.
	collar: string;
	number: string;
	numberEdge: string;
	// His name across his back.
	name: string;
	// Bands round each sleeve, from the shoulder down.
	sleeveStripes: string[];
	pants: string;
	// Down the outside of each leg, from the front back.
	pantsStripes: string[];
	sock: string;
	// Bands round the top of the socks.
	sockStripes: string[];
	// An official's shirt: white, striped up and down in this.
	stripes?: string;
	// A vest over the shirt, the chain crew's.
	vest?: string;
};

// The facemask: two bars open in front (a quarterback, a receiver, a
// kicker's), three with a strut up the middle (a back, a linebacker), or a
// lineman's full cage.
export type MaskStyle = "open" | "bars" | "cage";

export type FieldGear = {
	mask: MaskStyle;
	// A tinted shield across his eyes - or a clear one, all but unseen.
	visor?: "dark" | "clear";
	// Eye black, a smear under each eye.
	eyeBlack?: true;
	cleat: string;
	cleatSole: string;
	// The swoosh of color up their side.
	cleatAccent: string;
	// Gloves, both hands - the palms a shade of their own - or his hands
	// taped (a lineman's), or bare (his throwing hand, a kicker's).
	glove?: { color: string; palm: string; hands: "R" | "L" | "RL" };
	tape?: boolean;
	// A sleeve down his arm, to the wrist, and a band round his wrist.
	sleeve?: { arms: "R" | "L" | "RL"; color: string };
	wrist?: { arms: "R" | "L" | "RL"; color: string };
	// A towel tucked in his pants at the front.
	towel?: boolean;
};

export type FieldLook = {
	kit: FieldKit;
	gear: FieldGear;
	skin: string;
	jerseyNumber: string;
	lastName: string;
	// The team's logo, for the sides of his helmet.
	logo?: HTMLCanvasElement;
	// One of the officials: a cap, not a helmet (in the kit's helmet color),
	// no pads.
	official?: true;
	// (A coach's) a headset on over his cap.
	headset?: true;
	// His hair, for when he has his helmet off - and off, in his hand.
	hair?: string;
	bare?: true;
	// And the rest of his own head as his face has it, then: how his hair
	// is cut and stands; his beard, and his brows (both in this color); how
	// big his nose and ears are (1 about average).
	head?: BareHead;
	// A cheerleader: no pads, no cap - her hair up in a ponytail - a crop
	// top and a skirt, bare arms and legs, and a pom-pom in each hand in
	// these colors.
	cheer?: { poms: [string, string] };
};

export type BareHead = {
	cut: HairCut;
	style: HairStyle;
	whiskers: string;
	jaw?: boolean;
	chin?: boolean;
	lip?: boolean;
	burns?: boolean;
	nose?: number;
	ear?: number;
};

export type FieldTeamColors = [string, string, string];

const WHITE = "#f4f2ec";
const INK = "#18181c";
const MASK_GRAY = "#a2a5ab";

const contrast = (a: string, b: string) =>
	Math.abs(luminance(a) - luminance(b));

// The first of `colors` that shows on `on`, or `or`.
const showing = (colors: string[], on: string, or: string, by = 0.2) =>
	colors.find((c) => contrast(c, on) > by) ?? or;

// A team's uniform: at home its jersey in its main color - its darkest, if
// the main one is too light to be a home jersey - in white pants; on the
// road a white jersey over pants in its main color. Its helmet the same
// either way: its main color with a stripe down the middle in the others.
// Or any of it as the team has set it (see HelmetLook) - what it has not
// set picked to go with what it has.
export const fieldKit = (
	colors: FieldTeamColors,
	edition: "home" | "away",
	own: HelmetLook = {},
): FieldKit => {
	const [c0, c1, c2] = colors;
	const darkest = [...colors].sort((x, y) => luminance(x) - luminance(y))[0]!;
	const main = luminance(c0) < 0.72 ? c0 : darkest;
	const home = edition === "home";
	const ownJersey = home ? own.homeJersey : own.awayJersey;
	const jersey = ownJersey || (home ? main : WHITE);
	const others = [c1, c2, c0].filter((c) => c !== jersey);
	// (On a jersey of its own, what shows on it.)
	const onIt = luminance(jersey) > 0.55 ? INK : WHITE;
	const number =
		(home ? own.homeNumber : own.awayNumber) ||
		(home
			? showing(others, jersey, WHITE, 0.25)
			: ownJersey
				? showing([main, ...others], jersey, onIt, 0.25)
				: main);
	const numberEdge = showing(
		[c1, c2, c0, home ? WHITE : INK].filter(
			(c) => c !== number && c !== jersey,
		),
		number,
		shade(jersey, -0.45),
		0.15,
	);
	// Light pants under a dark jersey at home - the team's second color if it
	// is a light one, a gold or a silver, else white; on the road, the
	// team's own color, or white if that is too light to tell from white.
	const pants =
		(home ? own.homePants : own.awayPants) ||
		(home
			? luminance(c1) > 0.5 && contrast(c1, jersey) > 0.25
				? c1
				: WHITE
			: luminance(main) < 0.6
				? main
				: WHITE);
	const stripeOn = (on: string) =>
		[c0, c1, c2].filter((c) => contrast(c, on) > 0.18).slice(0, 2);
	const helmet = own.shell || c0;
	const helmetStripe =
		own.stripe ??
		[c1, c2, WHITE].find((c) => c !== helmet && contrast(c, helmet) > 0.2);
	const stripeEdge =
		own.stripeEdge ??
		[c2, c1, WHITE, INK].find(
			(c) =>
				c !== helmetStripe && helmetStripe && contrast(c, helmetStripe) > 0.2,
		);
	return {
		helmet,
		...(helmetStripe ? { helmetStripe } : {}),
		...(helmetStripe && stripeEdge ? { helmetStripeEdge: stripeEdge } : {}),
		facemask: own.facemask || MASK_GRAY,
		jersey,
		collar:
			home || ownJersey
				? showing(
						[c1, c2, WHITE].filter((c) => c !== jersey),
						jersey,
						onIt,
					)
				: main,
		number,
		numberEdge,
		name: number,
		sleeveStripes: stripeOn(jersey),
		pants,
		pantsStripes: stripeOn(pants),
		sock: home ? main : luminance(main) < 0.6 ? main : darkest,
		sockStripes: [],
	};
};

// What the two teams wear, [away, home] as the court has them.
export const fieldKitsFor = (
	away: FieldTeamColors | undefined,
	home: FieldTeamColors | undefined,
	helmets: [HelmetLook?, HelmetLook?] = [],
): [FieldKit, FieldKit] => [
	fieldKit(away ?? ["#1d3461", "#f28c28", "#ffffff"], "away", helmets[0]),
	fieldKit(home ?? ["#8c1d40", "#f2c14e", "#ffffff"], "home", helmets[1]),
];

const LINEMEN = new Set(["OL", "DL"]);
const BACKS = new Set(["RB", "TE", "LB"]);
const KICKERS = new Set(["K", "P"]);

// A player's own gear, the same every game: decided by who he is and what
// he plays, in his team's colors that day.
// How many (not the kickers) wear eye black.
const EYE_BLACK = 0.3;
export const fieldGearFor = (
	pid: number,
	pos: string,
	kit: FieldKit,
): FieldGear => {
	const rng = makeCourtRng(`fieldGear|${pid}`);
	const pick = <T>(xs: T[]): T => xs[Math.floor(rng() * xs.length)]!;
	const lineman = LINEMEN.has(pos);
	const kicker = KICKERS.has(pos);
	const mask: MaskStyle = lineman
		? "cage"
		: BACKS.has(pos)
			? rng() < 0.75
				? "bars"
				: "cage"
			: rng() < 0.85
				? "open"
				: "bars";
	// Black cleats or white, the team's color up their side (or the other
	// of black and white, if it would not show).
	const cleat = rng() < 0.55 ? INK : WHITE;
	const cleatAccent = showing(
		[kit.jersey, kit.pants, kit.helmet],
		cleat,
		cleat === INK ? WHITE : INK,
		0.25,
	);
	const gear: FieldGear = {
		mask,
		cleat,
		cleatSole: cleat === INK ? "#2a2a2e" : "#dcdad4",
		cleatAccent,
	};
	if (lineman) {
		// Linemen: gloves in black or gray, or just tape round the hands.
		if (rng() < 0.6) {
			const color = rng() < 0.6 ? INK : "#6c6f75";
			gear.glove = { color, palm: shade(color, 0.12), hands: "RL" };
		} else {
			gear.tape = true;
		}
	} else if (pos === "QB") {
		// The quarterback's throwing hand bare - sometimes a glove on the
		// other.
		if (rng() < 0.35) {
			gear.glove = {
				color: rng() < 0.5 ? INK : WHITE,
				palm: "#8a8d93",
				hands: "L",
			};
		}
	} else if (!kicker) {
		// Everybody else in gloves: the team's color, black or white, the
		// palms gray - grippy.
		const color = pick([kit.jersey, INK, WHITE, kit.helmet]);
		gear.glove = {
			color,
			palm: shade(color, luminance(color) > 0.5 ? -0.18 : 0.2),
			hands: "RL",
		};
	}
	if (!kicker && rng() < 0.32) {
		gear.sleeve = {
			arms: rng() < 0.75 ? "RL" : rng() < 0.5 ? "R" : "L",
			color: pick([INK, WHITE, kit.jersey]),
		};
	}
	if (!gear.sleeve && rng() < 0.25) {
		gear.wrist = { arms: "RL", color: pick([INK, WHITE, kit.jersey]) };
	}
	if (!lineman && rng() < 0.22) {
		gear.visor = rng() < 0.45 ? "dark" : "clear";
	}
	if (["QB", "RB", "WR", "CB", "S"].includes(pos) && rng() < 0.22) {
		gear.towel = true;
	}
	if (!kicker && rng() < EYE_BLACK) {
		gear.eyeBlack = true;
	}
	return gear;
};

// THE OFFICIALS: the striped shirt, black pants with a white stripe down
// the leg, black shoes, and a cap - the referee's white, the rest black.
const OFFICIAL_SKIN = [
	"#f0c8a8",
	"#e2b08a",
	"#cc9670",
	"#a87050",
	"#7c5038",
	"#5c3a28",
];
export const officialLook = (
	referee: boolean,
	number: string,
	seed: string,
): FieldLook => {
	const rng = makeCourtRng(`official|${seed}`);
	return {
		kit: {
			helmet: referee ? WHITE : INK,
			facemask: INK,
			jersey: WHITE,
			collar: INK,
			number: INK,
			numberEdge: WHITE,
			name: INK,
			sleeveStripes: [],
			pants: INK,
			pantsStripes: [WHITE],
			sock: INK,
			sockStripes: [],
			stripes: INK,
		},
		gear: {
			mask: "open",
			cleat: INK,
			cleatSole: "#2a2a2e",
			cleatAccent: INK,
		},
		skin: OFFICIAL_SKIN[Math.floor(rng() * OFFICIAL_SKIN.length)]!,
		jerseyNumber: number,
		lastName: "",
		official: true,
	};
};

// THE CHAIN CREW: a bright vest over a white shirt, black pants, a black cap.
const VEST = "#f4581c";
export const chainCrewLook = (seed: string): FieldLook => {
	const rng = makeCourtRng(`chain|${seed}`);
	return {
		kit: {
			helmet: INK,
			facemask: INK,
			jersey: WHITE,
			vest: VEST,
			collar: WHITE,
			number: INK,
			numberEdge: WHITE,
			name: INK,
			sleeveStripes: [],
			pants: INK,
			pantsStripes: [],
			sock: INK,
			sockStripes: [],
		},
		gear: {
			mask: "open",
			cleat: INK,
			cleatSole: "#2a2a2e",
			cleatAccent: INK,
		},
		skin: OFFICIAL_SKIN[Math.floor(rng() * OFFICIAL_SKIN.length)]!,
		jerseyNumber: "",
		lastName: "",
		official: true,
	};
};

// A COACH: his team's pullover in its main color, sleeves to the wrist, dark
// pants, a cap - most of them a headset on over it (the head coach always).
const HEADSETS = 0.7;
export const coachLook = (
	kit: FieldKit,
	seed: string,
	head = false,
): FieldLook => {
	const rng = makeCourtRng(`coach|${seed}`);
	const main = kit.helmet;
	return {
		kit: {
			helmet: luminance(main) < 0.2 ? main : shade(main, -0.25),
			facemask: INK,
			jersey: main,
			collar: kit.helmetStripe ?? WHITE,
			number: INK,
			numberEdge: WHITE,
			name: INK,
			sleeveStripes: [],
			pants: rng() < 0.6 ? "#2b2c31" : "#b9a682",
			pantsStripes: [],
			sock: INK,
			sockStripes: [],
		},
		gear: {
			mask: "open",
			cleat: INK,
			cleatSole: "#2a2a2e",
			cleatAccent: INK,
			sleeve: { arms: "RL", color: main },
		},
		skin: OFFICIAL_SKIN[Math.floor(rng() * OFFICIAL_SKIN.length)]!,
		jerseyNumber: "",
		lastName: "",
		official: true,
		...(rng() < HEADSETS || head ? { headset: true } : {}),
	};
};

// A TRAINER, or the team doctor, out to a man hurt: a polo in the team's
// main color, khakis, a cap.
export const trainerLook = (kit: FieldKit, seed: string): FieldLook => {
	const rng = makeCourtRng(`trainer|${seed}`);
	const main = kit.helmet;
	return {
		kit: {
			helmet: luminance(main) < 0.2 ? main : shade(main, -0.25),
			facemask: INK,
			jersey: main,
			collar: kit.helmetStripe ?? WHITE,
			number: INK,
			numberEdge: WHITE,
			name: INK,
			sleeveStripes: [],
			pants: rng() < 0.7 ? "#b9a682" : "#2b2c31",
			pantsStripes: [],
			sock: INK,
			sockStripes: [],
		},
		gear: {
			mask: "open",
			cleat: rng() < 0.5 ? INK : WHITE,
			cleatSole: "#2a2a2e",
			cleatAccent: INK,
		},
		skin: OFFICIAL_SKIN[Math.floor(rng() * OFFICIAL_SKIN.length)]!,
		jerseyNumber: "",
		lastName: "",
		official: true,
	};
};

// A PHOTOGRAPHER: the tan vest over a dark shirt, sleeves to the wrist,
// dark or khaki pants, a cap.
const SHIRTS = ["#1d1e22", "#2b3040", "#3b3d42", "#45392f"];
const CAPS = [INK, "#2b3040", "#5b5d62", WHITE];
export const photographerLook = (seed: string): FieldLook => {
	const rng = makeCourtRng(`photographer|${seed}`);
	const pick = (from: string[]) => from[Math.floor(rng() * from.length)]!;
	const shirt = pick(SHIRTS);
	return {
		kit: {
			helmet: pick(CAPS),
			facemask: INK,
			jersey: shirt,
			vest: "#bba47a",
			collar: shirt,
			number: INK,
			numberEdge: WHITE,
			name: INK,
			sleeveStripes: [],
			pants: pick(["#2b2c31", "#b9a682", "#34405a"]),
			pantsStripes: [],
			sock: INK,
			sockStripes: [],
		},
		gear: {
			mask: "open",
			cleat: pick([INK, WHITE, "#6b5844"]),
			cleatSole: "#2a2a2e",
			cleatAccent: INK,
			sleeve: { arms: "RL", color: shirt },
		},
		skin: OFFICIAL_SKIN[Math.floor(rng() * OFFICIAL_SKIN.length)]!,
		jerseyNumber: "",
		lastName: "",
		official: true,
	};
};

// A TV CAMERAMAN: black from head to foot but for his skin - a polo, pants,
// a cap - or khakis.
export const tvCrewLook = (seed: string): FieldLook => {
	const rng = makeCourtRng(`tv|${seed}`);
	return {
		kit: {
			helmet: INK,
			facemask: INK,
			jersey: "#202125",
			collar: "#202125",
			number: INK,
			numberEdge: WHITE,
			name: INK,
			sleeveStripes: [],
			pants: rng() < 0.7 ? "#1d1e22" : "#b9a682",
			pantsStripes: [],
			sock: INK,
			sockStripes: [],
		},
		gear: {
			mask: "open",
			cleat: INK,
			cleatSole: "#2a2a2e",
			cleatAccent: INK,
		},
		skin: OFFICIAL_SKIN[Math.floor(rng() * OFFICIAL_SKIN.length)]!,
		jerseyNumber: "",
		lastName: "",
		official: true,
	};
};

// A CHEERLEADER: the home team's - a crop top in its jersey color, trimmed in
// its collar's, a skirt to match; white sneakers and socks; pom-poms in the
// two.
const CHEER_HAIR = [
	"#1b120c",
	"#3a2414",
	"#6b4a2b",
	"#8a5a2e",
	"#c9a05a",
	"#e2c27c",
];
export const cheerLook = (kit: FieldKit, seed: string): FieldLook => {
	const rng = makeCourtRng(`cheer|${seed}`);
	const top = kit.jersey;
	const trim =
		kit.collar === top ? showing([kit.number, WHITE], top, INK) : kit.collar;
	return {
		kit: {
			helmet: top,
			facemask: INK,
			jersey: top,
			collar: trim,
			number: INK,
			numberEdge: WHITE,
			name: INK,
			sleeveStripes: [],
			pants: top,
			pantsStripes: [],
			sock: WHITE,
			sockStripes: [],
		},
		gear: {
			mask: "open",
			cleat: WHITE,
			cleatSole: "#d4d2cc",
			cleatAccent: trim,
		},
		skin: OFFICIAL_SKIN[Math.floor(rng() * OFFICIAL_SKIN.length)]!,
		hair: CHEER_HAIR[Math.floor(rng() * CHEER_HAIR.length)]!,
		jerseyNumber: "",
		lastName: "",
		cheer: { poms: [top, trim] },
	};
};
