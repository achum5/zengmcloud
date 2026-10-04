import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";

// PRESTIGE
//
// Moves slowly toward what a program has earned lately: winning, NCAA
// tournament runs, and the quality of its incoming class. Blue bloods have a
// floor. Facilities follow prestige, slower still. AI coaches stay put while
// things go well and get let go when they don't (there are no coach
// characters, just how long the current one has been there).

// Credit for an NCAA tournament run: playoffRoundsWon is -1 for missing it,
// 0 for losing the first game, 6 for a title.
const TOURNEY_SCORE = [0.55, 0.65, 0.75, 0.85, 0.92, 0.97, 1];

export const collegeUpdatePrestige = async () => {
	const season = g.get("season");
	const rate = g.get("collegePrestigeRate");
	const coach = g.get("collegeCoach");

	// Incoming class quality: where each school's freshmen rank among all
	// freshmen.
	const freshmen = (
		await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
	)
		.filter((p) => p.collegeYear0 === season)
		.sort((a, b) => b.value - a.value);
	const classScores = new Map<number, number[]>();
	for (const [i, p] of freshmen.entries()) {
		const list = classScores.get(p.tid) ?? [];
		list.push(1 - i / Math.max(1, freshmen.length));
		classScores.set(p.tid, list);
	}

	for (const t of await idb.cache.teams.getAll()) {
		if (t.disabled) {
			continue;
		}
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsByTidSeason", [
			t.tid,
			season,
		]);
		if (!ts) {
			continue;
		}
		const games = ts.won + ts.lost;
		const winp = games > 0 ? ts.won / games : 0.5;
		const win = helpers.bound((winp - 0.3) / 0.55, 0, 1);
		const tourney =
			ts.playoffRoundsWon < 0
				? 0
				: TOURNEY_SCORE[
						Math.min(ts.playoffRoundsWon, TOURNEY_SCORE.length - 1)
					]!;
		const classList = classScores.get(t.tid) ?? [];
		const classScore =
			classList.length > 0
				? classList.reduce((a, b) => a + b, 0) / classList.length
				: 0.3;
		const target = 100 * (0.45 * win + 0.35 * tourney + 0.2 * classScore);

		const prestige = t.prestige ?? 30;
		const next = helpers.bound(
			prestige + (target - prestige) * 0.12 * rate,
			Math.max(1, t.collegePrestigeFloor ?? 1),
			100,
		);
		t.prestige = Math.round(next * 10) / 10;

		const facilities = t.collegeFacilities ?? prestige;
		t.collegeFacilities =
			Math.round((facilities + (t.prestige - facilities) * 0.05) * 10) / 10;

		if (coach?.tid !== t.tid) {
			// A season well short of what the program expects can cost the
			// coach his job.
			const fired = target < prestige - 15 && Math.random() < 0.35;
			t.collegeCoachYears = fired ? 0 : (t.collegeCoachYears ?? 0) + 1;
		}

		await idb.cache.teams.put(t);
	}
};
