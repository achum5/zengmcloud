import type {
	CardStatRow,
	CardSubject,
} from "../../common/tradingCardPrompt.ts";
import { idb } from "../db/index.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";
import g from "./g.ts";

// Assembles everything a card prompt needs about one player as of one season.
//
// "As of" is load-bearing. A card printed in a season cannot know what came
// after it, so the stat grid stops at the depicted season and so does the honors
// list - otherwise a 1997 card lists a 2003 MVP award and the illusion dies.

const div = (numerator: number, denominator: number): number =>
	denominator > 0 ? numerator / denominator : 0;

const pct = (made: number, attempted: number): number =>
	attempted > 0 ? (100 * made) / attempted : 0;

type Totals = {
	gp: number;
	gs: number;
	min: number;
	pts: number;
	trb: number;
	ast: number;
	stl: number;
	blk: number;
	fg: number;
	fga: number;
	tp: number;
	tpa: number;
	ft: number;
	fta: number;
};

const TOTAL_KEYS: (keyof Totals)[] = [
	"gp",
	"gs",
	"min",
	"pts",
	"trb",
	"ast",
	"stl",
	"blk",
	"fg",
	"fga",
	"tp",
	"tpa",
	"ft",
	"fta",
];

const emptyTotals = (): Totals =>
	Object.fromEntries(TOTAL_KEYS.map((key) => [key, 0])) as Totals;

const perGame = (totals: Totals): Omit<CardStatRow, "season" | "abbrev"> => ({
	gp: Math.round(totals.gp),
	gs: Math.round(totals.gs),
	min: div(totals.min, totals.gp),
	pts: div(totals.pts, totals.gp),
	trb: div(totals.trb, totals.gp),
	ast: div(totals.ast, totals.gp),
	stl: div(totals.stl, totals.gp),
	blk: div(totals.blk, totals.gp),
	fgp: pct(totals.fg, totals.fga),
	tpp: pct(totals.tp, totals.tpa),
	ftp: pct(totals.ft, totals.fta),
});

export const getTradingCardSubject = async (
	pid: number,
	season: number,
): Promise<CardSubject | undefined> => {
	const raw = await idb.getCopy.players({ pid }, "noCopyCache");
	if (!raw) {
		return undefined;
	}

	// Totals rather than per-game, so the career line through this season is a
	// real weighted total and not an average of averages.
	const p = await idb.getCopy.playersPlus(raw, {
		attrs: [
			"pid",
			"name",
			"born",
			"college",
			"draft",
			"face",
			"hgt",
			"weight",
			"jerseyNumber",
			"awards",
			"tid",
		],
		ratings: ["season", "pos"],
		stats: [
			"season",
			"abbrev",
			"tid",
			"jerseyNumber",
			"gp",
			"gs",
			"min",
			"pts",
			"trb",
			"ast",
			"stl",
			"blk",
			"fg",
			"fga",
			"tp",
			"tpa",
			"ft",
			"fta",
		],
		statType: "totals",
		playoffs: false,
		regularSeason: true,
		mergeStats: "totOnly",
		fuzz: true,
		// A card back is a record, so it shows real ratings-free numbers; nothing
		// here is a rating, but opt out so no future stat gets coarsened.
		coarsenRatings: false,
	});
	if (!p) {
		return undefined;
	}

	const allStats: any[] = Array.isArray(p.stats) ? p.stats : [];
	const throughSeason = allStats
		.filter((row) => row.season <= season && row.gp > 0)
		.sort((a, b) => a.season - b.season);

	const stats: CardStatRow[] = throughSeason.map((row) => ({
		season: row.season,
		abbrev: row.abbrev ?? "",
		...perGame(row as Totals),
	}));

	const careerTotals = emptyTotals();
	for (const row of throughSeason) {
		for (const key of TOTAL_KEYS) {
			careerTotals[key] += row[key] ?? 0;
		}
	}
	const career = stats.length > 1 ? perGame(careerTotals) : undefined;

	// The team the card puts him on: whoever he actually played for that season,
	// falling back to where he is now for a card depicting a season he sat out.
	const seasonRow = throughSeason.findLast((row) => row.season === season);
	const tid: number = seasonRow?.tid ?? p.tid;
	const teamInfo =
		tid >= 0 ? await getTeamInfoBySeason(tid, season) : undefined;

	const ratingsRows: any[] = Array.isArray(p.ratings) ? p.ratings : [];
	const ratingsRow =
		ratingsRows.findLast((row) => row.season <= season) ?? ratingsRows[0];

	const awards: string[] = [];
	for (const award of (raw.awards ?? []) as {
		season: number;
		type: string;
	}[]) {
		if (award.season <= season) {
			awards.push(`${award.type} (${award.season})`);
		}
	}

	const draftTeam =
		p.draft?.tid !== undefined && p.draft.tid >= 0
			? await getTeamInfoBySeason(p.draft.tid, p.draft.year)
			: undefined;

	return {
		name: p.name,
		pos: ratingsRow?.pos ?? "",
		jerseyNumber: seasonRow?.jerseyNumber ?? p.jerseyNumber,
		heightIn: p.hgt ?? 0,
		weightLbs: p.weight ?? 0,
		age:
			p.born?.year !== undefined && season >= p.born.year
				? season - p.born.year
				: undefined,
		bornYear: p.born?.year,
		bornLoc: p.born?.loc,
		college: p.college,
		draft:
			p.draft?.year !== undefined
				? {
						year: p.draft.year,
						round: p.draft.round ?? 0,
						pick: p.draft.pick ?? 0,
						teamName: draftTeam
							? `${draftTeam.region} ${draftTeam.name}`
							: undefined,
					}
				: undefined,
		teamName: teamInfo ? `${teamInfo.region} ${teamInfo.name}` : "Free Agents",
		teamColors: teamInfo?.colors,
		season,
		face: p.face,
		awards,
		stats,
		career,
	};
};

// Which seasons a card can depict for this player: every season he has a stats
// or ratings row for, plus the current one so a card can be made the moment he
// is drafted.
export const getTradingCardSeasons = (raw: {
	stats?: { season: number }[];
	ratings?: { season: number }[];
}): number[] => {
	const seasons = new Set<number>();
	for (const row of raw.stats ?? []) {
		seasons.add(row.season);
	}
	for (const row of raw.ratings ?? []) {
		seasons.add(row.season);
	}
	if (seasons.size === 0) {
		seasons.add(g.get("season"));
	}
	return [...seasons].sort((a, b) => b - a);
};
