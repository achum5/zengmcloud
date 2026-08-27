import { PHASE } from "../../../common/constants.ts";
import setContract from "./setContract.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import type { Phase, Player, PlayerContract } from "../../../common/types.ts";
import fuzzRating from "./fuzzRating.ts";
import genJerseyNumber from "./genJerseyNumber.ts";
import setJerseyNumber from "./setJerseyNumber.ts";
import { isSport } from "../../../common/sportFunctions.ts";

// HOW HIGH A POTENTIAL IS WORTH ANNOUNCING, and the number to quote when one
// is. Undefined means say nothing.
export const HIGH_UPSIDE_POT = 60;

// THE NUMBER IS THE SCOUTED ONE. playersPlus says it plainly - any user-facing
// rating is fuzzed, any internal one is not - and this used to read straight
// off the ratings row, so a notification was worth more than the scouting
// department that is supposed to produce it. The threshold reads the same
// fuzzed value, so the potential quoted is always the one that qualified
// rather than a second, truer number leaking alongside it.
//
// AND NOTHING IS SAID AT ALL UNDER "No Visible Player Ratings", which is not
// the same as printing the sentence without the number. It only ever fires for
// high potential, so its arrival IS the rating: a league that hid ratings would
// have every promising free agent announced as one, which gives away more than
// the number did. Same reason the Most Progs leaderboard drops its rows
// outright under that setting instead of blanking a column.
export const highUpsideSigningPot = (
	ratings: { pot: number; fuzz: number } | undefined,
): number | undefined => {
	if (!ratings || g.get("challengeNoRatings")) {
		return undefined;
	}
	const pot = fuzzRating(ratings.pot, ratings.fuzz);
	return pot >= HIGH_UPSIDE_POT ? pot : undefined;
};

const sign = async (
	p: Player,
	tid: number,
	contract: PlayerContract,
	phase: Phase,
) => {
	const isRookie =
		p.stats.length === 0 &&
		p.draft.year === g.get("season") &&
		p.draft.tid === tid;

	p.tid = tid;
	p.numDaysFreeAgent = 0;
	p.gamesUntilTradable = isRookie ? 0 : Math.round(0.17 * g.get("numGames")); // 14 for basketball, 3 for football
	delete p.numPlayersTradedAwayNormalized;

	// Handle stats if the season is in progress. Otherwise, not needed until next season.
	if (phase <= PHASE.PLAYOFFS) {
		setJerseyNumber(p, await genJerseyNumber(p));
	}

	let score = p.valueFuzz - 45;
	if (isSport("football")) {
		score -= 7;
	}
	score = Math.round(helpers.bound(score, 0, Infinity));

	setContract(p, contract, true);
	const resigning =
		phase === PHASE.RESIGN_PLAYERS && p.draft.year !== g.get("season");
	const eventType = resigning ? "reSigned" : "freeAgent";
	const eid = await logEvent({
		type: eventType,
		showNotification: false,
		pids: [p.pid],
		tids: [p.tid],
		score,
		contract: p.contract,
	});

	const freeAgent = !resigning && !isRookie;
	if (freeAgent) {
		if (!p.transactions) {
			p.transactions = [];
		}
		p.transactions.push({
			season: g.get("season"),
			phase: g.get("phase"),
			tid: p.tid,
			type: "freeAgent",
			eid,
		});

		// A heads-up for league-mates that a promising player just came off the
		// board - see highUpsideSigningPot for who qualifies. Notification only
		// (saveToDb: false): the freeAgent event above is already the recorded
		// transaction.
		const pot = highUpsideSigningPot(p.ratings.at(-1));
		if (pot !== undefined) {
			const t = g.get("teamInfoCache")[tid];
			logEvent({
				type: "freeAgent",
				text: `<a href="${helpers.leagueUrl(["player", p.pid])}">${
					p.firstName
				} ${p.lastName}</a> (${pot} pot) signed with the <a href="${helpers.leagueUrl(
					["roster", `${t?.abbrev}_${tid}`, g.get("season")],
				)}">${t ? `${t.region} ${t.name}` : "team"}</a>.`,
				showNotification: true,
				saveToDb: false,
				pids: [p.pid],
				tids: [tid],
			});
		}
	}

	return eid;
};

export default sign;
