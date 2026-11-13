import { g } from "../../util/index.ts";
import { vanillaPlusSettings } from "../vanillaPlus/settings.ts";

// Whether this league runs the new front offices (docs/realistic-front-offices.md).
// Basketball only; every stock code path that behaves differently checks this.
export const realisticFrontOfficesOn = () =>
	__SPORT === "basketball" && !!g.get("realisticFrontOffices");

// Whether trades run on the mode's own title-odds engine: not once vanilla+
// is on (core/vanillaPlus), which trades on vanilla's.
export const realisticTradesOn = () =>
	realisticFrontOfficesOn() && !vanillaPlusSettings.on;

// Whether trades run on vanilla+ (docs/vanilla-plus.md).
export const vanillaPlusOn = () =>
	realisticFrontOfficesOn() && vanillaPlusSettings.on;

// Whether CPU teams decide by the smart CPU's worth (docs/smart-cpu.md): a
// hidden switch on top of the mode, for test leagues.
export const smartCpuOn = () =>
	realisticFrontOfficesOn() && !!g.get("smartCpu");

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
