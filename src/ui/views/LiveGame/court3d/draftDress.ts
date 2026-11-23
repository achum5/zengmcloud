import { makeCourtRng } from "../courtRng.ts";
import type { Cap } from "./cap.ts";
import { streetClothes, type Look } from "./figure.ts";

// DRESSED FOR THE DRAFT (see draftStage.ts, draftClock.ts): the suit he
// came in - his own, chosen long before anyone took him, so nothing of it in
// his new team's colors - the same one every time the night is shown
// (decided by who he is). Trousers to his shoes, socks to match them; and
// once he has been taken, his new team's cap.

type Suit = {
	suit: string;
	shirt: string;
	// None: open at the neck.
	tie?: string;
	shoe: string;
	pin?: string;
};

// Most in the dark suits anybody wears; plenty in a color of their own.
const DARK = ["#1b2436", "#24262c", "#141416", "#2e3340", "#3a3f4a"];
const BOLD = [
	"#5e1a26", // burgundy
	"#1f3f8f", // royal blue
	"#1d4d3a", // emerald
	"#e8e1d2", // cream
	"#c9ccd1", // light gray
	"#a98a62", // tan
	"#4a2c5a", // plum
	"#8fb3d9", // powder blue
	"#f2f1ed", // white
	"#6b6f76", // slate
	"#3c2a22", // chocolate
];
const TIES = [
	"#6b1f2a",
	"#1e2c52",
	"#9aa0a8",
	"#c9a227",
	"#2f5d4a",
	"#5a3a6e",
	"#141416",
	"#7a4a2a",
	"#b9a6d6",
	"#d8d4cc",
];
const SHIRTS_FOR_DARK = ["#f2f2ee", "#f2f2ee", "#d9e3ee", "#141416"];
const PALE = new Set(["#e8e1d2", "#c9ccd1", "#8fb3d9", "#f2f1ed", "#a98a62"]);

const suitOf = (pid: number): Suit => {
	const rng = makeCourtRng(`draft|suit|${pid}`);
	const pick = <T>(of: readonly T[]) => of[Math.floor(rng() * of.length)]!;
	const bold = rng() < 0.42;
	const suit = bold ? pick(BOLD) : pick(DARK);
	const pale = PALE.has(suit);
	// All one color, shirt and tie too - or a white shirt, a black one, a
	// pale blue one.
	const tonal = !pale && rng() < 0.12;
	const shirt = tonal
		? suit
		: pale
			? pick(["#f2f2ee", "#141416", "#f2f2ee"])
			: pick(SHIRTS_FOR_DARK);
	// A tie of his own - or none, open at the neck.
	let tie: string | undefined;
	if (tonal) {
		tie = rng() < 0.5 ? suit : undefined;
	} else if (rng() >= 0.22) {
		tie =
			shirt === "#141416"
				? pick(["#141416", "#9aa0a8", "#c9a227"])
				: pick(TIES.filter((t) => t !== suit && t !== shirt));
	}
	const shoeRoll = rng();
	const shoe =
		shoeRoll < 0.6
			? "#141416"
			: shoeRoll < 0.85
				? pale
					? "#6a4528"
					: "#3a2618"
				: "#f2f2f0";
	return {
		suit,
		shirt,
		...(tie ? { tie } : {}),
		shoe,
		...(!bold && rng() < 0.18 ? { pin: "#8a93a6" } : {}),
	};
};

const draftSuit = (pid: number): Pick<Look, "kit" | "gear" | "outfit"> => {
	const worn = streetClothes(pid);
	const { suit, shirt, tie, shoe, pin } = suitOf(pid);
	const sole = shoe === "#f2f2f0" ? "#d8d8d4" : "#2a2a2c";
	return {
		kit: {
			...worn.kit,
			jersey: suit,
			trim: suit,
			number: suit,
			numberEdge: suit,
			shorts: suit,
			stripe: suit,
			sock: suit,
			shoe,
			sole,
		},
		gear: { tights: { legs: "RL", color: suit }, shoe, sole, sock: suit },
		outfit: {
			sleeves: "long",
			shirt,
			...(tie ? { tie } : {}),
			...(pin ? { pin } : {}),
			trousers: true,
		},
	};
};

// His look for the night: the cap (if he has been taken) on hair close to
// his head, unless it hangs down past it.
export const draftLook = (who: number, look: Look, cap?: Cap): Look => {
	const out: Look = {
		...look,
		...draftSuit(who),
		jerseyNumber: "",
		lastName: "",
		wordmark: "",
		street: true,
		...(cap
			? {
					cap,
					cut:
						look.cut === "long"
							? "long"
							: look.cut === "bald"
								? "bald"
								: "short",
				}
			: {}),
	};
	delete out.kitArt;
	delete out.crest;
	delete out.style;
	// (Drawn as the commissioner and his family are: the player's own body is
	// made in his shorts.)
	delete out.real;
	delete out.faceTex;
	return out;
};
