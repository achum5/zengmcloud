import { PHASE } from "../../../common/constants.ts";
import setContract from "./setContract.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import type { Phase, Player, PlayerContract } from "../../../common/types.ts";
import genJerseyNumber from "./genJerseyNumber.ts";
import setJerseyNumber from "./setJerseyNumber.ts";
import { isSport } from "../../../common/sportFunctions.ts";

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

		// Notify whenever a high-upside free agent (60+ potential) comes off the
		// board, whichever team lands them - a heads-up for league-mates that a
		// promising player just got signed. Notification only (saveToDb: false):
		// the freeAgent event above is already the recorded transaction.
		const pot = p.ratings.at(-1)?.pot ?? 0;
		if (pot >= 60) {
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
