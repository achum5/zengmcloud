import type { CourtStyle } from "./types.ts";

// THE GAME'S OWN TEAMS' HOME FLOORS: one for every team the game comes with,
// laid out the way an NBA floor is - the wood, the lane painted in the
// team's colors, the colored border round the floor with the team written
// on the baselines, the logo at center court, the second logo out on the
// wings - each its own, from its own logos and colors.
//
// A team plays on its stock floor till it has a court of its own: the
// court editor opens on it, and a team that changes it keeps its change.
// The floor is worked out from the team as it is - its colors, its name,
// its logos - so a team recolored or renamed has its floor follow.
//
// Every spot a stock floor puts something is clear of everything else on
// it, the lines included (see stockCourts.test), and of where a league's
// own pictures go (inside each three-point line toward the corners).

// Wood stains.
const WOODS = {
	blonde: "#dfc495",
	maple: "#d4ad74",
	natural: "#c9a165",
	honey: "#c18f52",
	amber: "#b37a43",
	pecan: "#9a6a43",
	walnut: "#7a5236",
	espresso: "#4e3426",
	ash: "#b8aa93",
	smoke: "#8f8577",
} as const;
type Wood = keyof typeof WOODS;

// A color on the floor: one of the team's (by its place in the team's
// colors), the darkest of them taken darker still, or a color outright.
type Ink = 0 | 1 | 2 | "dark" | "white" | "black" | `#${string}` | Wood;

type Design = {
	wood: Wood;
	// Inside the three-point lines, another stain.
	inside?: Wood;
	pattern?: CourtStyle["floorPattern"];
	paint?: Ink;
	trim?: Ink;
	apron: Ink;
	text: Ink;
	lines?: Ink;
	circle?: Ink;
	// The center logo, as a share of its 30 ft box.
	logo: number;
	// The second logo out on each wing, near the far sideline: its size, as a
	// share of its 13 ft box.
	wings?: number;
	// The baselines: the team name on both, or the city at one end and the
	// name at the other.
	rails?: "split";
	// The city written across center court, above the logo.
	center?: "region";
	lettering?: CourtStyle["lettering"];
};

// Where the wing logos sit: out past the three-point line, toward the far
// sideline, a stride short of center court (feet from where the secondary
// slot is laid out - see LiveCourt).
export const WING = { dx: 6.1, dy: -17 } as const;

const DESIGNS: Record<string, Design> = {
	ABQ: {
		wood: "maple",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
	},
	AMS: {
		wood: "honey",
		pattern: "chevron",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 2,
		logo: 0.62,
		wings: 0.55,
	},
	ANC: {
		wood: "blonde",
		paint: 0,
		apron: "#0f2f45",
		text: 0,
		lines: "#0f2f45",
		circle: 0,
		logo: 0.36,
		wings: 0.55,
	},
	ATH: {
		wood: "pecan",
		inside: "natural",
		trim: 1,
		apron: "dark",
		text: 1,
		circle: 2,
		logo: 0.36,
		wings: 0.55,
		lettering: "serif",
	},
	ATL: {
		wood: "honey",
		pattern: "chevron",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		logo: 0.7,
		lettering: "italic",
	},
	AUS: {
		wood: "amber",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 2,
		circle: 2,
		logo: 0.36,
		wings: 0.55,
	},
	BAL: {
		wood: "pecan",
		inside: "maple",
		paint: 2,
		trim: 0,
		apron: 2,
		text: 0,
		rails: "split",
		logo: 0.66,
		wings: 0.6,
		lettering: "serif",
	},
	BEI: {
		wood: "honey",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 2,
		rails: "split",
		logo: 0.72,
		wings: 0.55,
	},
	BEL: {
		wood: "natural",
		paint: 1,
		trim: 2,
		apron: 1,
		text: "white",
		logo: 0.74,
		wings: 0.55,
		lettering: "italic",
	},
	BER: {
		wood: "natural",
		paint: 0,
		trim: 2,
		apron: 1,
		text: 2,
		rails: "split",
		logo: 0.64,
		wings: 0.55,
	},
	BKN: {
		wood: "smoke",
		pattern: "chevron",
		paint: 0,
		apron: 0,
		text: 1,
		circle: 2,
		logo: 0.36,
		wings: 0.55,
	},
	BOG: {
		wood: "natural",
		paint: 0,
		trim: 1,
		apron: "dark",
		text: 1,
		logo: 0.6,
		wings: 0.6,
	},
	BOI: {
		wood: "ash",
		paint: 0,
		trim: 2,
		apron: 1,
		text: 2,
		logo: 0.62,
		wings: 0.6,
		lettering: "italic",
	},
	BOS: {
		wood: "natural",
		paint: 2,
		trim: 1,
		apron: 2,
		text: 0,
		rails: "split",
		logo: 0.68,
		wings: 0.6,
	},
	BRU: {
		wood: "natural",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
	},
	BUE: {
		wood: "maple",
		paint: 0,
		trim: 2,
		apron: "dark",
		text: "white",
		logo: 0.66,
		wings: 0.55,
		lettering: "italic",
	},
	BUF: {
		wood: "natural",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.66,
		wings: 0.55,
	},
	CAI: {
		wood: "amber",
		inside: "honey",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		logo: 0.62,
		wings: 0.55,
		lettering: "serif",
	},
	CGY: {
		wood: "amber",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 2,
		logo: 0.66,
		wings: 0.55,
		lettering: "serif",
	},
	CHA: {
		wood: "maple",
		paint: 1,
		trim: 0,
		apron: 1,
		text: "white",
		rails: "split",
		logo: 0.64,
		wings: 0.55,
		lettering: "serif",
	},
	CHI: {
		wood: "walnut",
		inside: "pecan",
		paint: 0,
		trim: 2,
		apron: 1,
		text: 2,
		logo: 0.6,
		wings: 0.55,
		lettering: "serif",
	},
	CHW: {
		wood: "maple",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		circle: 1,
		rails: "split",
		logo: 0.36,
		wings: 0.6,
	},
	CIN: {
		wood: "walnut",
		paint: 1,
		apron: 0,
		text: 1,
		logo: 0.7,
		wings: 0.58,
		lettering: "italic",
	},
	CLB: {
		wood: "natural",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
	},
	CLE: {
		wood: "espresso",
		inside: "walnut",
		paint: 2,
		trim: 1,
		apron: 0,
		text: 1,
		lines: 1,
		logo: 0.62,
		lettering: "serif",
	},
	CPH: {
		wood: "blonde",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		lines: 0,
		logo: 0.66,
		wings: 0.55,
		lettering: "italic",
	},
	DAL: {
		wood: "natural",
		paint: 1,
		trim: 0,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.7,
		wings: 0.55,
	},
	DEL: {
		wood: "honey",
		pattern: "chevron",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		logo: 0.64,
		wings: 0.55,
		lettering: "serif",
	},
	DEN: {
		wood: "natural",
		inside: "maple",
		paint: 0,
		apron: 1,
		text: 2,
		circle: 1,
		logo: 0.36,
		wings: 0.55,
	},
	DET: {
		wood: "ash",
		pattern: "diagonal",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 2,
		rails: "split",
		logo: 0.78,
		wings: 0.55,
		lettering: "italic",
	},
	EDM: {
		wood: "ash",
		paint: 1,
		trim: 0,
		apron: 1,
		text: "white",
		circle: 1,
		logo: 0.36,
		wings: 0.55,
	},
	HAW: {
		wood: "blonde",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		lines: 0,
		logo: 0.64,
		wings: 0.55,
		lettering: "italic",
	},
	HOU: {
		wood: "maple",
		paint: 0,
		trim: "white",
		apron: "dark",
		text: "white",
		center: "region",
		logo: 0.62,
	},
	IND: {
		wood: "honey",
		inside: "maple",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
		lettering: "serif",
	},
	IST: {
		wood: "amber",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.6,
		wings: 0.55,
		lettering: "serif",
	},
	JAX: {
		wood: "natural",
		paint: 2,
		trim: 0,
		apron: 2,
		text: 0,
		rails: "split",
		logo: 0.64,
		wings: 0.6,
	},
	JKT: {
		wood: "pecan",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.66,
		wings: 0.58,
	},
	JNB: {
		wood: "amber",
		paint: 2,
		trim: 1,
		apron: 0,
		text: 1,
		circle: 1,
		logo: 0.36,
		wings: 0.55,
	},
	KAU: {
		wood: "natural",
		pattern: "parquet",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
		lettering: "serif",
	},
	KC: {
		wood: "amber",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
		lettering: "serif",
	},
	KEN: {
		wood: "natural",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
	},
	KYV: {
		wood: "maple",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
	},
	LA: {
		wood: "natural",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		logo: 0.75,
		wings: 0.58,
		lettering: "italic",
	},
	LAE: {
		wood: "maple",
		paint: 1,
		apron: "#1c1c1c",
		text: 1,
		center: "region",
		logo: 0.62,
	},
	LAG: {
		wood: "natural",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 2,
		circle: 2,
		logo: 0.36,
		wings: 0.55,
	},
	LIM: {
		wood: "honey",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
		lettering: "serif",
	},
	LIS: {
		wood: "natural",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		circle: 1,
		logo: 0.36,
		wings: 0.55,
		lettering: "serif",
	},
	LON: {
		wood: "honey",
		pattern: "chevron",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		circle: 2,
		logo: 0.36,
		wings: 0.55,
		lettering: "serif",
	},
	LV: {
		wood: "maple",
		paint: 2,
		trim: 1,
		apron: 2,
		text: 1,
		circle: 1,
		logo: 0.36,
		wings: 0.55,
	},
	MAD: {
		wood: "amber",
		paint: 1,
		trim: 2,
		apron: 1,
		text: 0,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
		lettering: "serif",
	},
	MCM: {
		wood: "ash",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		rails: "split",
		logo: 0.64,
		wings: 0.55,
	},
	MEM: {
		wood: "espresso",
		paint: 2,
		trim: 1,
		apron: 0,
		text: 2,
		rails: "split",
		logo: 0.8,
		lettering: "italic",
	},
	MIA: {
		wood: "blonde",
		paint: 1,
		trim: 0,
		apron: 0,
		text: "white",
		lines: 0,
		logo: 0.6,
		wings: 0.55,
		lettering: "italic",
	},
	MIL: {
		wood: "natural",
		inside: "blonde",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.64,
		wings: 0.6,
		lettering: "serif",
	},
	MIN: {
		wood: "ash",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		logo: 0.7,
		wings: 0.55,
	},
	MLB: {
		wood: "honey",
		paint: 2,
		trim: 0,
		apron: 1,
		text: 0,
		circle: 2,
		logo: 0.36,
		wings: 0.58,
	},
	MNL: {
		wood: "natural",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.72,
		wings: 0.55,
	},
	MON: {
		wood: "natural",
		pattern: "parquet",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.7,
	},
	MOS: {
		wood: "natural",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		logo: 0.64,
		wings: 0.55,
	},
	MXC: {
		wood: "amber",
		paint: 1,
		trim: 2,
		apron: 1,
		text: 2,
		rails: "split",
		logo: 0.72,
		wings: 0.55,
	},
	NJ: {
		wood: "natural",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		logo: 0.72,
		wings: 0.58,
		lettering: "italic",
	},
	NOL: {
		wood: "pecan",
		paint: 0,
		trim: 1,
		apron: 2,
		text: 1,
		logo: 0.72,
		wings: 0.55,
		lettering: "italic",
	},
	NSH: {
		wood: "honey",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.62,
		wings: 0.58,
		lettering: "italic",
	},
	NYC: {
		wood: "natural",
		paint: 0,
		trim: 1,
		apron: 0,
		text: "white",
		rails: "split",
		logo: 0.64,
		wings: 0.55,
		lettering: "serif",
	},
	OAK: {
		wood: "pecan",
		inside: "natural",
		paint: 0,
		apron: 2,
		text: "white",
		circle: 0,
		logo: 0.36,
		wings: 0.6,
	},
	OKC: {
		wood: "honey",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 2,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
	},
	OMA: {
		wood: "amber",
		inside: "honey",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		logo: 0.68,
		wings: 0.58,
		lettering: "serif",
	},
	ORL: {
		wood: "maple",
		paint: 0,
		trim: 2,
		apron: 2,
		text: "white",
		logo: 0.62,
		wings: 0.55,
	},
	OTT: {
		wood: "walnut",
		paint: 2,
		trim: 1,
		apron: 0,
		text: 1,
		logo: 0.62,
		wings: 0.55,
	},
	PAR: {
		wood: "honey",
		pattern: "parquet",
		paint: 0,
		trim: 1,
		apron: 0,
		text: "white",
		rails: "split",
		logo: 0.64,
		wings: 0.55,
		lettering: "serif",
	},
	PHI: {
		wood: "natural",
		paint: 0,
		trim: 1,
		apron: 2,
		text: "white",
		logo: 0.62,
		wings: 0.6,
	},
	PHO: {
		wood: "amber",
		inside: "honey",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		rails: "split",
		logo: 0.72,
		wings: 0.6,
	},
	PIT: {
		wood: "natural",
		paint: 1,
		trim: 0,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
	},
	POR: {
		wood: "walnut",
		inside: "pecan",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 2,
		logo: 0.62,
		wings: 0.55,
		lettering: "serif",
	},
	PRO: {
		wood: "smoke",
		paint: 2,
		trim: 1,
		apron: 0,
		text: 1,
		circle: 1,
		logo: 0.36,
		wings: 0.55,
		lettering: "serif",
	},
	QUE: {
		wood: "honey",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		circle: 1,
		rails: "split",
		logo: 0.36,
		wings: 0.55,
		lettering: "serif",
	},
	RAL: {
		wood: "natural",
		paint: 0,
		trim: 2,
		apron: 2,
		text: 0,
		rails: "split",
		logo: 0.66,
		wings: 0.55,
		lettering: "italic",
	},
	RIO: {
		wood: "maple",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		logo: 0.64,
		wings: 0.55,
		lettering: "italic",
	},
	ROM: {
		wood: "amber",
		pattern: "diagonal",
		paint: 0,
		trim: 1,
		apron: 2,
		text: 1,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
		lettering: "serif",
	},
	RYD: {
		wood: "maple",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		center: "region",
		logo: 0.62,
		lettering: "serif",
	},
	SA: {
		wood: "honey",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		circle: 1,
		logo: 0.36,
		wings: 0.55,
		lettering: "italic",
	},
	SAC: {
		wood: "pecan",
		inside: "honey",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
		lettering: "serif",
	},
	SD: {
		wood: "blonde",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		lines: 0,
		rails: "split",
		logo: 0.6,
	},
	SEA: {
		wood: "ash",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		circle: 1,
		logo: 0.36,
		wings: 0.55,
	},
	SEL: {
		wood: "maple",
		paint: 0,
		trim: 1,
		apron: 2,
		text: 1,
		logo: 0.62,
		wings: 0.55,
		lettering: "italic",
	},
	SF: {
		wood: "maple",
		pattern: "chevron",
		paint: 0,
		trim: 1,
		apron: 0,
		text: "white",
		logo: 0.66,
		wings: 0.55,
		lettering: "italic",
	},
	SGO: {
		wood: "natural",
		paint: 0,
		trim: 2,
		apron: 1,
		text: 2,
		circle: 0,
		logo: 0.36,
		wings: 0.6,
	},
	SIN: {
		wood: "maple",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.66,
		wings: 0.55,
	},
	SJ: {
		wood: "natural",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 2,
		rails: "split",
		logo: 0.64,
		wings: 0.55,
		lettering: "serif",
	},
	STL: {
		wood: "ash",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		logo: 0.72,
		wings: 0.6,
		lettering: "italic",
	},
	STO: {
		wood: "ash",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		logo: 0.68,
		wings: 0.6,
		lettering: "italic",
	},
	SYD: {
		wood: "honey",
		paint: 1,
		trim: 0,
		apron: 1,
		text: 0,
		logo: 0.62,
		wings: 0.55,
	},
	THR: {
		wood: "honey",
		paint: 1,
		trim: 2,
		apron: 1,
		text: 0,
		circle: 2,
		logo: 0.36,
		wings: 0.55,
		lettering: "serif",
	},
	TLV: {
		wood: "maple",
		paint: 1,
		trim: 2,
		apron: 1,
		text: 0,
		logo: 0.64,
		wings: 0.55,
	},
	TOK: {
		wood: "blonde",
		paint: 1,
		trim: 2,
		apron: 2,
		text: 1,
		lines: 2,
		rails: "split",
		logo: 0.66,
		wings: 0.58,
	},
	TOR: {
		wood: "natural",
		paint: 0,
		trim: 2,
		apron: 2,
		text: 1,
		rails: "split",
		logo: 0.64,
		wings: 0.55,
	},
	TPA: {
		wood: "natural",
		paint: 1,
		trim: 2,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.7,
		wings: 0.55,
	},
	TPE: {
		wood: "natural",
		paint: 0,
		trim: 2,
		apron: 0,
		text: 1,
		circle: 2,
		logo: 0.36,
		wings: 0.55,
	},
	UTA: {
		wood: "maple",
		paint: 1,
		trim: 2,
		apron: 1,
		text: 2,
		rails: "split",
		logo: 0.62,
		wings: 0.52,
		lettering: "serif",
	},
	VAN: {
		wood: "pecan",
		inside: "natural",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		center: "region",
		logo: 0.72,
	},
	VB: {
		wood: "honey",
		paint: 2,
		trim: 0,
		apron: 2,
		text: 0,
		rails: "split",
		logo: 0.6,
		wings: 0.5,
		lettering: "serif",
	},
	WAS: {
		wood: "natural",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		circle: 0,
		rails: "split",
		logo: 0.36,
		wings: 0.55,
		lettering: "serif",
	},
	WAW: {
		wood: "honey",
		pattern: "parquet",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.66,
		wings: 0.55,
		lettering: "serif",
	},
	WPG: {
		wood: "ash",
		paint: 0,
		trim: 1,
		apron: 0,
		text: 1,
		rails: "split",
		logo: 0.62,
		wings: 0.55,
		lettering: "serif",
	},
};

export const STOCK_COURT_TEAMS = Object.keys(DESIGNS);

// (Colors, worked with as 0-1 red, green, blue.)
const rgb = (hex: string): [number, number, number] => {
	const h = hex.replace("#", "");
	const full =
		h.length === 3
			? h
					.split("")
					.map((c) => c + c)
					.join("")
			: h.padEnd(6, "0").slice(0, 6);
	const n = Number.parseInt(full, 16);
	return Number.isFinite(n)
		? [((n >> 16) & 255) / 255, ((n >> 8) & 255) / 255, (n & 255) / 255]
		: [0, 0, 0];
};
const hex = ([r, g, b]: [number, number, number]) =>
	`#${[r, g, b]
		.map((c) =>
			Math.round(Math.max(0, Math.min(1, c)) * 255)
				.toString(16)
				.padStart(2, "0"),
		)
		.join("")}`;
const luminance = (color: string) => {
	const [r, g, b] = rgb(color).map((c) =>
		c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4,
	) as [number, number, number];
	return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};
export const contrast = (a: string, b: string) => {
	const la = luminance(a);
	const lb = luminance(b);
	return (Math.max(la, lb) + 0.05) / (Math.min(la, lb) + 0.05);
};
// How far apart two colors look, 0 (the same) to 1 (black and white) - the
// "redmean" weighting, close enough to the eye for telling paint from wood.
export const apart = (a: string, b: string) => {
	const [r1, g1, b1] = rgb(a);
	const [r2, g2, b2] = rgb(b);
	const rm = (r1 + r2) / 2;
	return (
		Math.sqrt(
			(2 + rm) * (r1 - r2) ** 2 +
				4 * (g1 - g2) ** 2 +
				(3 - rm) * (b1 - b2) ** 2,
		) / 3
	);
};
// The first of these that stands apart from a color - for paint laid on
// the wood, a trim round the paint.
const distinct = (on: string, wanted: string, others: string[], least = 0.12) =>
	[wanted, ...others].find((c) => apart(c, on) >= least) ?? wanted;
const darken = (color: string, by: number) =>
	hex(rgb(color).map((c) => c * (1 - by)) as [number, number, number]);

// The best of these to read against a color: the first that stands out
// from it well enough, else whichever stands out most.
const readable = (on: string, wanted: string, others: string[], least = 3) => {
	if (contrast(wanted, on) >= least) {
		return wanted;
	}
	let best = wanted;
	for (const c of others) {
		if (contrast(c, on) >= least) {
			return c;
		}
		if (contrast(c, on) > contrast(best, on)) {
			best = c;
		}
	}
	return best;
};

const LINES = "#f8f5f0";

export type StockTeam = {
	region?: string;
	name?: string;
	colors?: [string, string, string];
	imgURL?: string;
	imgURLSmall?: string;
};

// The game's own team a team is, by the logo it has on: one of the game's
// own logos, or none.
export const stockCourtKey = (t: StockTeam): string | undefined => {
	const m = /^\/img\/logos-primary\/([A-Z]+)\.svg$/.exec(t.imgURL ?? "");
	return m && Object.hasOwn(DESIGNS, m[1]!) ? m[1] : undefined;
};

// A team's stock floor, if it is one of the game's own teams.
export const stockCourt = (t: StockTeam): CourtStyle | undefined => {
	const key = stockCourtKey(t);
	if (key === undefined) {
		return undefined;
	}
	const d = DESIGNS[key]!;
	const colors = t.colors ?? ["#333333", "#ffffff", "#999999"];
	const darkest = [...colors].sort((a, b) => luminance(a) - luminance(b))[0]!;
	const ink = (i: Ink): string =>
		typeof i === "number"
			? (colors[i] ?? darkest)
			: i === "dark"
				? luminance(darkest) > 0.05
					? darken(darkest, 0.55)
					: darkest
				: i === "white"
					? "#ffffff"
					: i === "black"
						? "#141414"
						: i in WOODS
							? WOODS[i as Wood]
							: i;
	const floor = WOODS[d.wood];
	const inside = d.inside ? WOODS[d.inside] : undefined;
	const apron = ink(d.apron);
	const court: CourtStyle = {
		floor,
		...(inside ? { insideFloor: inside } : {}),
		...(d.pattern && d.pattern !== "hardwood"
			? { floorPattern: d.pattern }
			: {}),
		apron,
		apronText: readable(apron, ink(d.text), [...colors, "#ffffff", "#141414"]),
		// (Lines that stand out from the wood - white, the default, unless the
		// floor is too light for it.)
		lines: readable(
			floor,
			d.lines !== undefined ? ink(d.lines) : LINES,
			[LINES, darkest, "#141414"],
			1.5,
		),
	};
	if (d.paint !== undefined) {
		// (Paint that stands out from the wood it is laid on.)
		court.paint = distinct(inside ?? floor, ink(d.paint), [darkest, ...colors]);
	}
	if (d.trim !== undefined) {
		court.paintTrim = distinct(court.paint ?? inside ?? floor, ink(d.trim), [
			...colors,
			"#ffffff",
			"#141414",
		]);
	}
	if (d.circle !== undefined) {
		court.centerCircle = distinct(floor, ink(d.circle), [darkest, ...colors]);
	}
	const region = (t.region ?? "").toUpperCase();
	const name = (t.name ?? "").toUpperCase();
	if (d.rails === "split" && region && name) {
		court.railText = region;
		court.railText2 = name;
	}
	if (d.center === "region" && region) {
		court.centerText = region;
		court.centerTextColor = readable(floor, apron, [...colors, "#141414"]);
	}
	if (d.lettering && d.lettering !== "block") {
		court.lettering = d.lettering;
	}
	court.adjust = { logo: { scale: d.logo } };
	if (d.wings !== undefined && t.imgURLSmall) {
		court.secondaryLogoURL = t.imgURLSmall;
		court.adjust.secondary = { scale: d.wings, ...WING, opacity: 0.95 };
	}
	return court;
};

// The court a team's floor is drawn from: its own, or its stock floor.
export const courtOf = (t: StockTeam & { court?: CourtStyle }) =>
	t.court ?? stockCourt(t);
