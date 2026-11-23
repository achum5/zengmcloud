import type { Cap } from "./cap.ts";
import { streetClothes, type Look } from "./figure.ts";

// DRESSED FOR THE DRAFT (see draftStage.ts, draftClock.ts): a dark suit, a
// white shirt and a tie - in his new team's color, and its cap on his head,
// once he has been taken.
const SUITS = ["#1d2333", "#24262c", "#151517", "#2c3140"];
const TIES = ["#6b1f2a", "#24365c", "#3c4a2a", "#5a3a6e", "#8a6d1f", "#2f5d6e"];

const draftSuit = (
	pid: number,
	tie: string,
): Pick<Look, "kit" | "gear" | "outfit"> => {
	const worn = streetClothes(pid);
	const suit = SUITS[Math.abs(pid) % SUITS.length]!;
	const shoe = "#141416";
	const sole = "#2a2a2c";
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
		outfit: { sleeves: "long", shirt: "#f2f2ee", tie },
	};
};

// His look for the night: the cap (if he has been taken) on hair close to
// his head, unless it hangs down past it.
export const draftLook = (who: number, look: Look, cap?: Cap): Look => {
	const out: Look = {
		...look,
		...draftSuit(who, cap?.crown ?? TIES[Math.abs(who) % TIES.length]!),
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
	return out;
};
