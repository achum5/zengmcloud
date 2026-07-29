import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { bySport } from "../../../common/sportFunctions.ts";
import { getTeamInfoBySeason } from "../../util/getTeamInfoBySeason.ts";

// The player card the trivia games open when you tap a player you've named.
//
// A trimmed player page rather than a link to one: leaving the page mid-grid
// loses the board, so the card comes to you. Everything here is display-only
// and goes through playersPlus, which means fuzz and the hide-ones-digit mode
// apply exactly as they do everywhere else.

const PROFILE_STATS = bySport({
	basketball: [
		"gp",
		"gs",
		"min",
		"pts",
		"trb",
		"ast",
		"stl",
		"blk",
		"tov",
		"fgp",
		"tpp",
		"ftp",
	],
	default: ["gp", "keyStats"],
});

export type TriviaPlayerProfile = Awaited<
	ReturnType<typeof getTriviaPlayerProfile>
>;

export const getTriviaPlayerProfile = async (pid: number) => {
	const pRaw = await idb.getCopy.players({ pid }, "noCopyCache");
	if (!pRaw) {
		return undefined;
	}

	const p = await idb.getCopy.playersPlus(pRaw, {
		attrs: [
			"pid",
			"name",
			"tid",
			"abbrev",
			"age",
			"hgt",
			"weight",
			"born",
			"college",
			"draft",
			"face",
			"imgURL",
			"awards",
			"jerseyNumber",
			"experience",
			"hof",
			"retiredYear",
		],
		ratings: ["season", "abbrev", "tid", "age", "ovr", "pot", "pos", "skills"],
		stats: ["season", "tid", "abbrev", "age", ...PROFILE_STATS],
		showRookies: true,
		fuzz: true,
		mergeStats: "totAndTeams",
	});

	if (!p) {
		return undefined;
	}

	// A player with no games in a season has nothing to show for it.
	const stats = (p.stats ?? []).filter((row: any) => (row.gp ?? 0) > 0);

	// The uniform the header is drawn in: the team they're on now, or the one
	// they played the most minutes for. Same rule the grid cards use, so a
	// player looks the same wherever you meet him.
	let cardTid = p.tid as number;
	if (cardTid < 0) {
		const minByTid = new Map<number, number>();
		for (const row of stats) {
			if (row.tid >= 0) {
				minByTid.set(row.tid, (minByTid.get(row.tid) ?? 0) + (row.min ?? 0));
			}
		}
		let best = -1;
		let bestMin = -1;
		for (const [tid, min] of minByTid) {
			if (min > bestMin) {
				best = tid;
				bestMin = min;
			}
		}
		cardTid = best;
	}
	const team = cardTid >= 0 ? await idb.cache.teams.get(cardTid) : undefined;

	// Awards collapsed to "5x All-Star", newest first within a type, ordered by
	// how much the honor is worth rather than alphabetically.
	const RANK: Record<string, number> = {
		"Inducted into the Hall of Fame": 0,
		"Won Championship": 1,
		"Most Valuable Player": 2,
		"Finals MVP": 3,
	};
	const byType = new Map<string, number[]>();
	for (const award of p.awards ?? []) {
		const seasons = byType.get(award.type) ?? [];
		seasons.push(award.season);
		byType.set(award.type, seasons);
	}
	const awards = [...byType.entries()]
		.map(([type, seasons]) => ({
			type,
			count: seasons.length,
			seasons: seasons.sort((a, b) => a - b),
		}))
		.sort(
			(a, b) =>
				(RANK[a.type] ?? 50) - (RANK[b.type] ?? 50) ||
				b.count - a.count ||
				a.type.localeCompare(b.type),
		);

	const currentTeam =
		p.tid >= 0
			? await getTeamInfoBySeason(p.tid as number, g.get("season"))
			: undefined;

	return {
		pid: p.pid as number,
		name: p.name as string,
		tid: p.tid as number,
		pos: (p.ratings?.at(-1)?.pos ?? "") as string,
		age: p.age as number,
		hgt: p.hgt as number,
		weight: p.weight as number,
		bornYear: p.born?.year as number,
		bornLoc: p.born?.loc as string,
		college: p.college as string,
		draft: p.draft,
		experience: p.experience as number,
		hof: !!p.hof,
		retiredYear: p.retiredYear as number,
		jerseyNumber: p.jerseyNumber as string | undefined,
		face: p.face,
		imgURL: p.imgURL as string | undefined,
		colors: team?.colors,
		jersey: team?.jersey,
		teamName: currentTeam
			? `${currentTeam.region} ${currentTeam.name}`
			: undefined,
		awards,
		ratings: (p.ratings ?? []).map((row: any) => ({
			season: row.season,
			ovr: row.ovr,
			pot: row.pot,
			pos: row.pos,
		})),
		stats,
		careerStats: p.careerStats,
		statKeys: PROFILE_STATS,
	};
};
