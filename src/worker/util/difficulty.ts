import g from "./g.ts";
import { realisticFrontOfficesOn } from "../core/realisticFrontOffice/isOn.ts";

// The difficulty knob used for TRADES (AI valuation of your assets). Falls back
// to the league's overall `difficulty` when no trade-specific override is set.
// Not with realistic front offices, where difficulty does nothing.
export const tradeDifficulty = (): number =>
	realisticFrontOfficesOn()
		? 0
		: (g.get("difficultyTrade") ?? g.get("difficulty"));

// The difficulty knob used for SIGNINGS (free-agent mood / contract demands).
// Falls back to the league's overall `difficulty` when no signing-specific
// override is set. (Spectator handling stays at the call site, matching the
// existing moodComponents behavior.)
export const signingDifficulty = (): number =>
	g.get("difficultySigning") ?? g.get("difficulty");
