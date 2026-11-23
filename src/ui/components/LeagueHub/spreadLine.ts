// A game's line as a scoreboard writes it: the favorite and its spread
// ("BOS -5.5"), or "PK" for a pick'em. `spread` is from the home team's
// side, positive when home is favored (Game.spread).
export const spreadLine = (
	spread: number | undefined,
	homeAbbrev: string,
	awayAbbrev: string,
) => {
	if (spread === undefined || !Number.isFinite(spread)) {
		return undefined;
	}
	if (spread === 0) {
		return "PK";
	}
	const points = Math.abs(spread).toLocaleString("en-US", {
		maximumFractionDigits: 1,
	});
	return `${spread > 0 ? homeAbbrev : awayAbbrev} -${points}`;
};
