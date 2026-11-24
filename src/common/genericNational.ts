import { NIGHTS } from "./nationalBroadcasts.ts";
import type {
	NationalBroadcast,
	ScoreBugPiece,
	ScoreBugStyle,
} from "./types.ts";

// THE NATIONAL NETWORK A LEAGUE HAS WITHOUT ITS OWN (see NationalBroadcast,
// and the team looks file's "national"): our own, with nothing of a real
// one's - its logo a ball on a screen, its bug drawn here. Every night's
// biggest game, and the playoffs and the Finals.

const HEAVY = "National Heavy";
const BOLD = "National Bold";
const DARK = "#0d1520";
const LIGHT = "#e9edf2";

// (Each piece's letters, baseline to cap height, on the middle of what they
// sit on: a team's panel above its stripe, the bar, the strip.)
const side = (who: "away" | "home", x: number): ScoreBugPiece[] => [
	{ show: "box", x, y: 0, w: 440, h: 96, background: `${who}0` },
	{ show: "box", x, y: 90, w: 440, h: 6, background: `${who}1` },
	{ show: `${who}Logo`, x: x + 18, y: 12, w: 72, h: 72 },
	{
		show: `${who}Abbrev`,
		x: x + 104,
		y: -2.7,
		w: 160,
		h: 96,
		color: `${who}Ink`,
		size: 60,
		font: HEAVY,
		spacing: 1,
	},
	{
		show: `${who}Score`,
		x: x + 250,
		y: -2.5,
		w: 166,
		h: 96,
		color: `${who}Ink`,
		size: 84,
		font: HEAVY,
		align: "right",
	},
	{
		show: `${who}Record`,
		x: x + 20,
		y: 96,
		w: 400,
		h: 54,
		color: LIGHT,
		size: 34,
		font: BOLD,
		text: "{rec} ({rank} IN {CONF})",
		sub: 28,
		when: "noBonus",
	},
	{
		show: `${who}Timeouts`,
		x: x + 20,
		y: 120,
		w: 220,
		h: 6,
		color: LIGHT,
		off: "transparent",
		spacing: 12,
		when: "bonus",
	},
	{
		show: `${who}Bonus`,
		x: x + 240,
		y: 96,
		w: 180,
		h: 54,
		color: "#ffcc4d",
		size: 34,
		font: BOLD,
		align: "right",
		text: "BONUS",
	},
];

const scoreBug: ScoreBugStyle = {
	width: 1600,
	height: 150,
	span: 0.52,
	place: "center",
	bottom: 0.016,
	shadow: "none",
	font: HEAVY,
	fonts: [
		{
			name: HEAVY,
			src: "/fonts/saira-extra-condensed/saira-extra-condensed-latin-800-normal.woff2",
		},
		{
			name: BOLD,
			src: "/fonts/saira-extra-condensed/saira-extra-condensed-latin-700-normal.woff2",
		},
	],
	pieces: [
		{ show: "box", x: 0, y: 0, w: 1600, h: 96, background: DARK },
		{
			show: "box",
			x: 0,
			y: 96,
			w: 1600,
			h: 54,
			background: "rgba(13,21,32,.9)",
		},
		{
			show: "image",
			x: 30,
			y: 10,
			w: 120,
			h: 76,
			image: "/img/national-tv-dark.svg",
		},
		...side("away", 180),
		...side("home", 620),
		{ show: "box", x: 618, y: 0, w: 4, h: 96, background: DARK },
		{
			show: "period",
			x: 1076,
			y: 0.9,
			w: 110,
			h: 96,
			color: "#c9d1dc",
			size: 54,
			font: HEAVY,
			align: "center",
			ordinal: "sup",
			upper: true,
		},
		{
			show: "clock",
			x: 1186,
			y: 0.5,
			w: 250,
			h: 96,
			color: "#ffffff",
			size: 70,
			font: HEAVY,
			align: "center",
			spacing: 1,
		},
		{
			show: "box",
			x: 1452,
			y: 18,
			w: 124,
			h: 60,
			background: "#1e2a3a",
			radius: 8,
		},
		{
			show: "shotClock",
			x: 1452,
			y: 18,
			w: 124,
			h: 60,
			color: "#ffcc4d",
			size: 50,
			font: HEAVY,
			align: "center",
			radius: 8,
			low: "#ffffff",
			lowBackground: "#c8322b",
		},
		{
			show: "series",
			x: 1076,
			y: 96.5,
			w: 500,
			h: 54,
			color: LIGHT,
			size: 32,
			font: BOLD,
			align: "right",
			spacing: 1,
			text: "{DAY} NIGHT HOOPS",
			undated: "GAME OF THE NIGHT",
		},
	],
};

export const GENERIC_NATIONAL: NationalBroadcast = {
	network: "National TV",
	nights: NIGHTS,
	finals: true,
	logo: "/img/national-tv.svg",
	logoDark: "/img/national-tv-dark.svg",
	scoreBug,
};
