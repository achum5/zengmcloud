import { bySport } from "./sportFunctions.ts";

const PERFORMANCE_FRACTION_DECREASE_PER_DAY = bySport({
	baseball: 0.025,
	basketball: 0.025,
	football: 0.05,
	hockey: 0.025,
});

const playThroughInjuriesFactor = (playThroughInjuries: number) => {
	return 1 - PERFORMANCE_FRACTION_DECREASE_PER_DAY * playThroughInjuries;
};

// On the NBA calendar, days left instead of games: prorated, a day counting
// for the classic days the calendar's clock passes in one (`rate`, about half
// in the regular season), so a player plays as well as he would in a classic
// league with the same time left.
export const playThroughInjuriesDayFactor = (days: number, rate: number) =>
	playThroughInjuriesFactor(days * rate);

// A team's Play Through Injuries setting in days on the NBA calendar (the
// user's teams; see playThroughDays) and the classic days a calendar day is
// worth there, for prorating the performance.
export type PlayThroughDays = { days: number; rate: number };

// How far off a game is: the classic days that pass before it (injury
// ticks, see injuryTicksUntil) and the calendar days counted down before it
// (see calendarDaysUntil). Tonight's game: none of either.
export type GameAhead = { games: number; days: number };

const TONIGHT: GameAhead = { games: 0, days: 0 };

// How a player stands for a game `ahead` of now, with his team's setting:
// healed by then, playing through, or out. Healed by his classic days, as the
// sim heals him. With the setting in days on the NBA calendar (`byDays`),
// whether he plays through and how well go by the days he'll have left that
// day (the days shown now, less the days counted down before it - never under
// one while he's hurt, as the game shows them); else by his classic days left
// against the classic setting.
export const injuryAtGame = (
	injury: { gamesRemaining: number; daysRemaining?: number },
	setting: number,
	byDays: PlayThroughDays | undefined,
	ahead: GameAhead = TONIGHT,
) => {
	const gamesLeft = injury.gamesRemaining - ahead.games;
	if (gamesLeft <= 0) {
		return { healthy: true, plays: true, factor: 1 };
	}
	if (byDays && injury.daysRemaining !== undefined) {
		const daysLeft = Math.max(1, injury.daysRemaining - ahead.days);
		return {
			healthy: false,
			plays: daysLeft <= byDays.days,
			factor: playThroughInjuriesDayFactor(daysLeft, byDays.rate),
		};
	}
	return {
		healthy: false,
		plays: gamesLeft <= setting,
		factor: playThroughInjuriesFactor(gamesLeft),
	};
};

// Whether a hurt player plays tonight, and how well (see injuryAtGame).
export const playingThroughInjury = (
	injury: { gamesRemaining: number; daysRemaining?: number },
	setting: number,
	byDays: PlayThroughDays | undefined,
) => {
	const { healthy, plays, factor } = injuryAtGame(injury, setting, byDays);
	return { playingThrough: !healthy && plays, factor };
};

export default playThroughInjuriesFactor;
