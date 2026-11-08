import type { GamecastInfo } from "../../../../worker/views/liveGameInfo.ts";

type Team = { tid: number; abbrev?: string };

// Each side's wins in a series [first, second] from its meetings with
// scores - leaving out the game being watched, if `before`.
export const seriesWins = (
	info: GamecastInfo,
	first: number,
	second: number,
	before = false,
): [number, number] => {
	const wins = (tid: number) =>
		info.series.filter((s) => {
			if (!s.pts || (before && s.now)) {
				return false;
			}
			const i = s.tids[0] === tid ? 0 : 1;
			return s.pts[i] > s.pts[i === 0 ? 1 : 0];
		}).length;
	return [wins(first), wins(second)];
};

// "BOS leads 2-1", "Tied 1-1" - or, the series won, "BOS wins 4-2".
export const recordText = (
	w: [number, number],
	teams: [Team, Team],
	toWin?: number,
): string => {
	if (w[0] === w[1]) {
		return `Tied ${w[0]}-${w[1]}`;
	}
	const k = w[0] > w[1] ? 0 : 1;
	const hi = Math.max(w[0], w[1]);
	const lo = Math.min(w[0], w[1]);
	return `${teams[k].abbrev} ${toWin !== undefined && hi >= toWin ? "wins" : "leads"} ${hi}-${lo}`;
};

// A playoff game's round, which game of the series it is, and the series as
// it stood going into it - "1st round · Game 3 · BOS leads 2-1" - for the
// score bug. Undefined outside the playoffs.
export const seriesLine = (
	info: GamecastInfo | undefined,
	playoffs: boolean,
	// Away first, as the score bug has them.
	teams: [Team, Team],
): string | undefined => {
	if (!playoffs || !info || info.series.length === 0) {
		return undefined;
	}
	const k = info.series.findIndex((s) => s.now);
	const round = info.bracket?.find((r) => r.series.some((s) => s.now));
	const w = seriesWins(info, teams[0].tid, teams[1].tid, true);
	const parts = [
		round ? round.name.charAt(0).toUpperCase() + round.name.slice(1) : "",
		k >= 0 ? `Game ${k + 1}` : "",
		w[0] + w[1] > 0 ? recordText(w, teams, round?.toWin) : "",
	].filter((x) => x !== "");
	return parts.length > 0 ? parts.join(" · ") : undefined;
};
