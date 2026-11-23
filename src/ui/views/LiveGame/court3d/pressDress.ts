import { makeCourtRng } from "../courtRng.ts";
import { lightness, streetClothes, type Look, type Outfit } from "./figure.ts";

// DRESSED FOR THE PRESS CONFERENCE (see pressStage.ts): his own clothes, not
// a uniform - nothing of them in his new team's colors (he came in them
// before he had it). A sweater, a quarter-zip, a T-shirt, a jacket over a
// T-shirt, a tracksuit; a suit now and then, with a tie or open at the neck;
// jeans, chinos, trousers; sneakers mostly. Picked for him and the move, so
// each time he is introduced he has something else on, and the same each
// time that one is watched.

type Fit = {
	top: string;
	// The zip of a quarter-zip.
	trim?: string;
	pants: string;
	shoe: string;
	sole: string;
	outfit: Outfit;
};

const KNITS = [
	"#e8e1d2", // cream
	"#9a9ca1", // heather gray
	"#3a3c42", // charcoal
	"#161618", // black
	"#1f2a44", // navy
	"#5b5d3a", // olive
	"#b08a5a", // camel
	"#5e1a26", // burgundy
	"#234235", // forest
	"#f2f1ed", // white
	"#5a3e2b", // brown
	"#4d6280", // slate blue
];
const TEES = [
	"#f2f2ef",
	"#f2f2ef",
	"#151517",
	"#151517",
	"#a9abb0",
	"#e6dfcf",
	"#55583a",
	"#1f2a44",
	"#c8b896",
];
const JACKETS = [
	"#18181a", // black
	"#1d2538", // navy
	"#33353b", // charcoal
	"#a8835a", // camel
	"#4a4d33", // olive
	"#3e5878", // denim
	"#201a17", // leather
	"#ddd5c3", // cream
	"#4d1a22", // burgundy
];
const SUITS = [
	"#1b2436",
	"#24262c",
	"#141416",
	"#2e3340",
	"#5e1a26",
	"#1f3f8f",
	"#c9ccd1",
	"#a98a62",
	"#1d4d3a",
	"#e8e1d2",
];
const SHIRTS = ["#f2f2ee", "#f2f2ee", "#d9e3ee", "#141416"];
const TIES = ["#6b1f2a", "#1e2c52", "#9aa0a8", "#2f5d4a", "#141416", "#7a4a2a"];
const JEANS = ["#263750", "#263750", "#3e5677", "#6d84a3", "#1c1d21"];
const PANTS = [
	"#b39f78", // khaki
	"#4e5137", // olive
	"#d8d0bd", // cream
	"#5d6066", // gray
	"#151517", // black
	"#2a2c33", // charcoal
];
const SNEAKERS: [string, string][] = [
	["#f2f2f0", "#dcdcd8"],
	["#f2f2f0", "#dcdcd8"],
	["#f2f2f0", "#dcdcd8"],
	["#18181a", "#f0f0ee"],
	["#18181a", "#2a2a2c"],
	["#9a9ca1", "#ececea"],
	["#5a3a22", "#2a1d14"],
];

type Kind =
	| "sweater"
	| "quarterZip"
	| "tee"
	| "jacket"
	| "tracksuit"
	| "suitTie"
	| "suitOpen";
const KINDS: [Kind, number][] = [
	["sweater", 20],
	["quarterZip", 12],
	["tee", 13],
	["jacket", 21],
	["tracksuit", 7],
	["suitTie", 14],
	["suitOpen", 13],
];

// How far apart two colors are (0 to about 440).
const rgbOf = (hex: string): [number, number, number] => {
	const n = Number.parseInt(hex.replace("#", "").slice(0, 6), 16);
	return Number.isFinite(n)
		? [(n >> 16) & 255, (n >> 8) & 255, n & 255]
		: [0, 0, 0];
};
const apart = (a: string, b: string) => {
	const [r0, g0, b0] = rgbOf(a);
	const [r1, g1, b1] = rgbOf(b);
	return Math.hypot(r0 - r1, g0 - g1, b0 - b1);
};
// (Nothing next to his skin's color on him: he would look bare.)
const SKIN_APART = 70;

export const pressFitOf = (
	pid: number,
	eid: number,
	skin?: string,
): Fit & { kind: Kind } => {
	const rng = makeCourtRng(`press|fit|${pid}|${eid}`);
	const pick0 = <T>(of: readonly T[]) =>
		of[Math.min(of.length - 1, Math.floor(rng() * of.length))]!;
	const pick = <T>(of: readonly T[]): T => {
		const got = pick0(of);
		if (!skin || typeof got !== "string" || apart(got, skin) >= SKIN_APART) {
			return got;
		}
		const ok = of.filter(
			(c) => typeof c !== "string" || apart(c, skin) >= SKIN_APART,
		);
		return ok.length > 0 ? pick0(ok) : got;
	};
	const total = KINDS.reduce((s, [, n]) => s + n, 0);
	let roll = rng() * total;
	let kind: Kind = "sweater";
	for (const [k, n] of KINDS) {
		roll -= n;
		if (roll < 0) {
			kind = k;
			break;
		}
	}
	const [shoe, sole] = pick(SNEAKERS);
	const casualPants = () => (rng() < 0.6 ? pick(JEANS) : pick(PANTS));
	// (Never all one dark: pants a shade off a dark top.)
	const off = (top: string) => {
		let pants = casualPants();
		for (let i = 0; i < 4 && pants === top; i++) {
			pants = casualPants();
		}
		return pants;
	};
	switch (kind) {
		case "sweater": {
			const top = pick(KNITS);
			return {
				kind,
				top,
				pants: off(top),
				shoe,
				sole,
				outfit: { sleeves: "long", plain: true, trousers: true },
			};
		}
		case "quarterZip": {
			const top = pick(KNITS);
			const zip =
				rng() < 0.5 ? top : lightness(top) > 0.5 ? "#2a2b30" : "#c9c9c6";
			return {
				kind,
				top,
				trim: zip,
				pants: off(top),
				shoe,
				sole,
				outfit: { sleeves: "long", trousers: true },
			};
		}
		case "tee": {
			const top = pick(TEES);
			return {
				kind,
				top,
				pants: off(top),
				shoe,
				sole,
				outfit: { sleeves: "short", plain: true, trousers: true },
			};
		}
		case "jacket": {
			const top = pick(JACKETS);
			const tee = pick(TEES.filter((t) => t !== top));
			return {
				kind,
				top,
				pants: off(top),
				shoe,
				sole,
				outfit: { sleeves: "long", shirt: tee, trousers: true },
			};
		}
		case "tracksuit": {
			const top = pick(["#161618", "#3a3c42", "#1f2a44", "#9a9ca1", "#234235"]);
			return {
				kind,
				top,
				pants: top,
				shoe,
				sole,
				outfit: { sleeves: "long", plain: true, trousers: true },
			};
		}
		case "suitTie":
		case "suitOpen": {
			const suit = pick(SUITS);
			const shirt =
				lightness(suit) > 0.6 ? pick(["#f2f2ee", "#141416"]) : pick(SHIRTS);
			const tie =
				kind === "suitTie"
					? pick(TIES.filter((t) => t !== suit && t !== shirt))
					: undefined;
			const dress = rng() < 0.55;
			return {
				kind,
				top: suit,
				pants: suit,
				shoe: dress ? (rng() < 0.7 ? "#141416" : "#3a2618") : shoe,
				sole: dress ? "#2a2a2c" : sole,
				outfit: {
					sleeves: "long",
					shirt,
					...(tie ? { tie } : {}),
					trousers: true,
				},
			};
		}
	}
};

// His look for it: his own clothes, nothing of the uniform on him.
export const pressLook = (who: number, eid: number, look: Look): Look => {
	const fit = pressFitOf(who, eid, look.skin);
	const worn = streetClothes(who);
	const out: Look = {
		...look,
		kit: {
			...worn.kit,
			jersey: fit.top,
			trim: fit.trim ?? fit.top,
			number: fit.top,
			numberEdge: fit.top,
			shorts: fit.pants,
			stripe: fit.pants,
			sock: fit.pants,
			shoe: fit.shoe,
			sole: fit.sole,
		},
		gear: {
			tights: { legs: "RL", color: fit.pants },
			shoe: fit.shoe,
			sole: fit.sole,
			sock: fit.pants,
		},
		outfit: fit.outfit,
		jerseyNumber: "",
		lastName: "",
		wordmark: "",
		street: true,
	};
	delete out.kitArt;
	delete out.crest;
	delete out.cap;
	// (Drawn as the people about him are: the player's own body is made in
	// his shorts.)
	delete out.real;
	delete out.faceTex;
	return out;
};
