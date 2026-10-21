// THE TRADE PENALTY, priced. Players dislike teams that trade a lot: the game
// keeps a count of players each team has traded away - this season's at 0.75,
// last season's at 0.5, the one before at 0.25 - and past 5 every further one
// lowers every player's mood towards that team (player/moodComponents.ts,
// "trades"). Mood decides re-signing, so a trade costs a team some chance of
// keeping everyone whose contract runs out while the count still remembers it.

// Players a team has traded away: this season, last season, the one before.
export type TradedAway = readonly [number, number, number];

const WEIGHTS = [0.75, 0.5, 0.25] as const;

// The mood component for this many (normalized) players traded away.
const tradesComponent = (normalized: number) =>
	Math.min(0, -(normalized - 5) / 4);

// How much more a team's players will mind its trading when the contract of
// one of them runs out `yearsAhead` seasons from now (0 is this coming
// re-signing), if it trades away `extra` more players now. Assumes no other
// trades in between.
export const moodShiftFromTrading = (
	tradedAway: TradedAway,
	extra: number,
	yearsAhead: number,
) => {
	if (extra <= 0 || yearsAhead < 0 || yearsAhead >= WEIGHTS.length) {
		return 0;
	}
	let normalized = 0;
	for (let k = 0; k + yearsAhead < WEIGHTS.length; k++) {
		normalized += WEIGHTS[k + yearsAhead]! * tradedAway[k]!;
	}
	const weight = WEIGHTS[yearsAhead]!;
	return (
		tradesComponent(normalized + weight * extra) - tradesComponent(normalized)
	);
};

// A re-signing chance moved by that much mood, on the mood model's own curve
// (player.moodInfo: probWilling = 1 / (1 + exp(-0.7 * mood))).
export const shiftRetention = (retention: number, moodShift: number) => {
	if (moodShift === 0 || retention >= 1) {
		return retention;
	}
	const p = Math.min(1 - 1e-6, Math.max(1e-6, retention));
	const logit = Math.log(p / (1 - p)) + 0.7 * moodShift;
	return 1 / (1 + Math.exp(-logit));
};
