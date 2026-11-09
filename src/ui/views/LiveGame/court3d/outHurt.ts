type Injury = {
	type?: string;
	gamesRemaining?: number;
	newThisGame?: boolean;
	playingThrough?: boolean;
};

// Out hurt from before the game - not hurt in it, nor playing through it:
// he never plays tonight (as the box score has him at the start: a man hurt
// in this game has what he had going into it kept as injuryAtStart).
export const outHurt = (p: {
	injury?: Injury;
	injuryAtStart?: Injury;
	min?: number;
}): boolean => {
	const start = p.injury?.newThisGame ? p.injuryAtStart : p.injury;
	return (
		start !== undefined &&
		(start.gamesRemaining ?? 0) > 0 &&
		!start.playingThrough &&
		!((p.min ?? 0) > 0)
	);
};
