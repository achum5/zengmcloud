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

// Whether a hurt player plays tonight, and how well: by the classic games he
// has left against the classic setting - or, with the setting in days on the
// NBA calendar (`byDays`), by his days left against it, prorated.
export const playingThroughInjury = (
	injury: { gamesRemaining: number; daysRemaining?: number },
	setting: number,
	byDays: { days: number; rate: number } | undefined,
) => {
	const daysLeft = byDays ? injury.daysRemaining : undefined;
	if (byDays && daysLeft !== undefined) {
		return {
			playingThrough: injury.gamesRemaining > 0 && daysLeft <= byDays.days,
			factor: playThroughInjuriesDayFactor(daysLeft, byDays.rate),
		};
	}
	return {
		playingThrough:
			injury.gamesRemaining > 0 && injury.gamesRemaining <= setting,
		factor: playThroughInjuriesFactor(injury.gamesRemaining),
	};
};

export default playThroughInjuriesFactor;
