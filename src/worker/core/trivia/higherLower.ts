import { g } from "../../util/index.ts";
import { getTriviaPool } from "./pool.ts";
import { mergedSeasons } from "./criteria.ts";

// Higher or Lower, ported from ZenGM Grids' higher-or-lower game: pick a stat
// category, then keep choosing which of two players ranks higher (or lower,
// for draft position). One wrong pick ends the run. The worker ships every
// player's value for every category; the streak game itself runs in the UI.

export type HigherLowerPlayer = {
	pid: number;
	name: string;
	years: string;
	values: Record<string, number | undefined>;
};

const round1 = (x: number) => Math.round(x * 10) / 10;
const pct = (made: number, att: number, minAtt: number) =>
	att >= minAtt ? Math.round((1000 * made) / att) / 10 : undefined;

export const buildHigherLowerPool = async (): Promise<HigherLowerPlayer[]> => {
	const pool = await getTriviaPool();
	const numTeams = g.get("numActiveTeams");

	const out: HigherLowerPlayer[] = [];
	for (const p of pool.players) {
		if (p.tot.gp <= 0) {
			continue;
		}

		// Season bests (per-game rates, min 20 games that season).
		let bestPpg = 0;
		let bestRpg = 0;
		let bestApg = 0;
		for (const [, s] of mergedSeasons(p)) {
			if (s.gp >= 20) {
				bestPpg = Math.max(bestPpg, s.pts / s.gp);
				bestRpg = Math.max(bestRpg, s.trb / s.gp);
				bestApg = Math.max(bestApg, s.ast / s.gp);
			}
		}

		const avgEligible = p.tot.gp >= 50;

		out.push({
			pid: p.pid,
			name: p.name,
			years: `${p.firstSeason}-${p.lastSeason}`,
			values: {
				careerPts: p.tot.pts,
				careerTrb: p.tot.trb,
				careerAst: p.tot.ast,
				careerStl: p.tot.stl,
				careerBlk: p.tot.blk,
				careerTp: p.tot.tp,
				careerGp: p.tot.gp,
				careerMin: Math.round(p.tot.min),
				seasons: p.tot.seasons,
				ppg: avgEligible ? round1(p.tot.pts / p.tot.gp) : undefined,
				rpg: avgEligible ? round1(p.tot.trb / p.tot.gp) : undefined,
				apg: avgEligible ? round1(p.tot.ast / p.tot.gp) : undefined,
				spg: avgEligible ? round1(p.tot.stl / p.tot.gp) : undefined,
				bpg: avgEligible ? round1(p.tot.blk / p.tot.gp) : undefined,
				fgPct: pct(p.tot.fg, p.tot.fga, 500),
				ftPct: pct(p.tot.ft, p.tot.fta, 250),
				tpPct: pct(p.tot.tp, p.tot.tpa, 250),
				bestPpg: bestPpg > 0 ? round1(bestPpg) : undefined,
				bestRpg: bestRpg > 0 ? round1(bestRpg) : undefined,
				bestApg: bestApg > 0 ? round1(bestApg) : undefined,
				highPts: p.gameHigh.pts > 0 ? p.gameHigh.pts : undefined,
				highTrb: p.gameHigh.trb > 0 ? p.gameHigh.trb : undefined,
				highAst: p.gameHigh.ast > 0 ? p.gameHigh.ast : undefined,
				// Overall draft position - the one category where LOWER is better.
				draftPick:
					p.draft.round >= 1
						? (p.draft.round - 1) * numTeams + p.draft.pick
						: undefined,
			},
		});
	}

	return out;
};
