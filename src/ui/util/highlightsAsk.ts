// A game asked for as its highlights (the league home's button) rather than
// in full: the live game opened next plays its key plays, whatever this
// browser has saved (see LiveGame/court3d/keyPlays.ts). Good for one game,
// opened within a few seconds - an ask nothing took up is forgotten.
let askedAt = 0;

export const askForHighlights = () => {
	askedAt = Date.now();
};

export const takeHighlightsAsk = (): boolean => {
	const asked = askedAt > 0 && Date.now() - askedAt < 15_000;
	askedAt = 0;
	return asked;
};
