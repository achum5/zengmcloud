import { g } from "../../util/index.ts";
import { vanillaPlusSettings } from "../vanillaPlus/settings.ts";

// Whether this league runs the new front offices (docs/realistic-front-offices.md).
// Basketball only; every stock code path that behaves differently checks this.
export const realisticFrontOfficesOn = () =>
	__SPORT === "basketball" && !!g.get("realisticFrontOffices");

// Whether CPU teams decide by the smart CPU's worth (docs/smart-cpu.md): a
// switch on top of the mode (League Settings), in place of vanilla+.
export const smartCpuOn = () =>
	realisticFrontOfficesOn() && !!g.get("smartCpu");

// The smart CPU on vanilla's rules: the Smart CPU switch with the new front
// offices off. Free agency, re-signing, the trade screens and the trade
// difficulty stay vanilla's; CPU teams trade among themselves, sign, re-sign,
// draft and cut by the smart CPU's worth, and turn down a human's offer that
// vanilla would take if it goes against their direction or helps a rival
// contender more than them (smartCpu/humanTrades.ts).
export const smartVanillaOn = () =>
	__SPORT === "basketball" &&
	!!g.get("smartCpu") &&
	!g.get("realisticFrontOffices");

// Whether CPU teams decide by the smart CPU's worth, on either rules.
export const smartBrainOn = () => smartCpuOn() || smartVanillaOn();

// Whether trades run on the mode's own title-odds engine: not once vanilla+
// is on (core/vanillaPlus), which trades on vanilla's, or the smart CPU.
export const realisticTradesOn = () =>
	realisticFrontOfficesOn() && !vanillaPlusSettings.on && !smartCpuOn();

// Whether trades run on vanilla+ (docs/vanilla-plus.md).
export const vanillaPlusOn = () =>
	realisticFrontOfficesOn() && vanillaPlusSettings.on && !smartCpuOn();

// Whether its free agency and re-signing are the offer market's: whenever the
// mode is on (owner's call). There was a switch for stock's free agency with
// the mode's front offices; with every team on the same terms, a bad CPU team
// could sign nobody (no free agent of 50 or better in 43 summers of 2 arena
// leagues) and leagues split far more than stock's.
export const realisticFreeAgencyOn = () => realisticFrontOfficesOn();

// How much more players test free agency when their contracts run out, the
// same for every team, human or CPU (owner's call, in place of stock's extra
// testing for CPU teams' players only) - except players finishing rookie
// deals. In player.moodInfo's units; tuned in the arena so top free agents
// re-sign about as often as in the NBA (~57%).
export const MARKET_TESTING = 10;
