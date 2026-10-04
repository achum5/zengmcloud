import {
	LEAGUE_DATABASE_VERSION,
	PHASE,
	PLAYER,
} from "../../../common/constants.ts";
import { idb } from "../../db/index.ts";
import connectLeague from "../../db/connectLeague.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import type { Player } from "../../../common/types.ts";

// THE PRO HAND-OFF
//
// Players who declare for the draft can go on to a pro league, two ways:
// exported as a draft class file (the pro league's Draft Scouting page
// imports it), or sent automatically to a linked league as its next draft
// class. Ratings are already on the pro scale, so nothing is rescaled - just
// ages and seasons shifted to the pro league's calendar.

export const collegeDraftees = async (season: number) =>
	(await idb.getCopies.players({ retiredYear: season }, "noCopyCache"))
		.filter((p) => p.collegeExit === "draft")
		.sort((a, b) => b.value - a.value);

// A college player as a pro draft prospect in `draftYear`, `collegeSeason`
// being the season he left school.
const toProspect = (p: Player, draftYear: number, collegeSeason: number) => {
	const shift = draftYear - collegeSeason;
	const t = g.get("teamInfoCache")[p.stats.at(-1)?.tid ?? -1];
	const ratings = { ...p.ratings.at(-1)!, season: draftYear };
	const prospect: Record<string, unknown> = {
		...helpers.deepCopy(p),
		tid: PLAYER.UNDRAFTED,
		born: { ...p.born, year: p.born.year + shift },
		college: t ? `${t.region} ${t.name}` : p.college,
		draft: {
			round: 0,
			pick: 0,
			tid: -1,
			originalTid: -1,
			year: draftYear,
			pot: ratings.pot,
			ovr: ratings.ovr,
			skills: ratings.skills,
		},
		ratings: [ratings],
		retiredYear: Infinity,
		stats: [],
		statsTids: [],
		transactions: [],
		awards: [],
		salaries: [],
		injury: { type: "Healthy", gamesRemaining: 0 },
		injuries: [],
		contract: { amount: 0, exp: draftYear },
		yearsFreeAgent: 0,
		numDaysFreeAgent: 0,
		gamesUntilTradable: 0,
		ptModifier: 1,
	};
	for (const key of [
		"pid",
		"jerseyNumber",
		"collegeYear0",
		"collegeExit",
		"collegeProfile",
		"collegePromises",
		"collegeRetention",
		"collegeStars",
		"recruiting",
		"diedYear",
	]) {
		delete prospect[key];
	}
	return prospect;
};

// A league file with just this draft class, for a pro league's "upload draft
// class" button.
export const collegeExportDraftClass = async (season: number) => {
	const players = await collegeDraftees(season);
	return {
		version: LEAGUE_DATABASE_VERSION,
		startingSeason: season,
		players: players.map((p) => toProspect(p, season, season)),
	};
};

export const collegeLinkableLeagues = async () => {
	const lid = g.get("lid");
	return (await idb.meta.getAll("leagues"))
		.filter((l) => l.lid !== lid)
		.map((l) => ({ lid: l.lid, name: l.name }));
};

// Replace a linked league's next draft class with this season's draftees.
export const collegeSendToLinkedLeague = async (season: number) => {
	const lid = g.get("collegeLinkedLid");
	if (lid === undefined) {
		return "No league is linked.";
	}
	const meta = await idb.meta.get("leagues", lid);
	if (!meta) {
		return "The linked league no longer exists.";
	}
	const players = await collegeDraftees(season);
	if (players.length === 0) {
		return "No one declared for the draft.";
	}

	const db = await connectLeague(lid);
	try {
		const attrs = db.transaction("gameAttributes").store;
		const proSeason = (await attrs.get("season"))?.value as number;
		const proPhase = (await attrs.get("phase"))?.value as number;
		// Its next draft: this season's if it hasn't happened yet.
		const draftYear =
			proPhase <= PHASE.DRAFT_LOTTERY ? proSeason : proSeason + 1;

		const tx = db.transaction("players", "readwrite");
		const store = tx.store;
		for (const p of await store.index("tid").getAll(PLAYER.UNDRAFTED)) {
			if (p.draft.year === draftYear && !p.real) {
				await store.delete(p.pid);
			}
		}
		for (const p of players) {
			await store.add(toProspect(p, draftYear, season) as any);
		}
		await tx.done;

		logEvent({
			type: "playoffs",
			text: `${players.length} players headed to the ${draftYear} draft in ${meta.name}.`,
			showNotification: true,
			tids: [g.get("userTid")],
			score: 10,
		});
	} finally {
		db.close();
	}
};
