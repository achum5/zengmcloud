import { chancePercent } from "../../../common/signingHow.ts";
import type { Player, SigningHow } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { team } from "../index.ts";
import { checkFor, ownTid, stagesOn } from "./stages.ts";

// THE HISTORY (owner's call, 2026-11-10: "history is saved and users can be
// very knowledgeable about it"). Under the contract rules every signing's
// event says how it happened - the rule it fit under (cap room, Bird rights,
// a mid-level, the minimum), an option, a prove-it deal, an offer sheet
// matched or not, an extension - and options decided are events of their
// own ("contract"). Transactions, the news and each
// player's page show them as they show any event.

// How a team may sign him for `amount` now, by the rules - for a signing that
// was allowed by rules other than the check made as free agency runs
// (re-signing, roster fills). Undefined with the rules off.
export const howFor = async (
	p: Player,
	tid: number,
	amount: number,
): Promise<SigningHow | undefined> => {
	if (!stagesOn()) {
		return undefined;
	}
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		return undefined;
	}
	const check = checkFor({
		t,
		p,
		payroll: await team.getPayroll(tid),
		amount,
		rosterSize: (await idb.cache.players.indexGetAll("playersByTid", tid))
			.length,
	});
	return check.ok
		? { path: ownPath(check.path, ownTid(p) === tid) }
		: undefined;
};

// His own team signs him by his Bird rights, under the cap or over it: that
// is what lets it, whatever its room (and what the NBA calls it).
export const ownPath = (
	path: NonNullable<SigningHow["path"]>,
	own: boolean,
): NonNullable<SigningHow["path"]> => (own && path === "room" ? "bird" : path);

const playerLink = (p: Pick<Player, "pid" | "firstName" | "lastName">) =>
	`<a href="${helpers.leagueUrl(["player", p.pid])}">${p.firstName} ${p.lastName}</a>`;

const teamLink = (tid: number) => {
	const t = g.get("teamInfoCache")[tid];
	return `<a href="${helpers.leagueUrl([
		"roster",
		`${t?.abbrev}_${tid}`,
		g.get("season"),
	])}">${t?.name ?? "???"}</a>`;
};

const scoreOf = (p: Pick<Player, "valueFuzz">) =>
	Math.round(helpers.bound(p.valueFuzz - 45, 0, Infinity));

// An option decided as the re-signing phase opens.
export const logOption = async ({
	p,
	tid,
	option,
	exercised,
	amount,
	season,
}: {
	p: Player;
	tid: number;
	option: "player" | "team";
	exercised: boolean;
	amount: number;
	season: number;
}) => {
	const money = `${helpers.formatCurrency(amount / 1000, "M")} for ${season}`;
	const text =
		option === "player"
			? exercised
				? `${playerLink(p)} picked up his player option with the ${teamLink(tid)} (${money}).`
				: `${playerLink(p)} declined his player option with the ${teamLink(tid)}.`
			: exercised
				? `The ${teamLink(tid)} picked up their team option on ${playerLink(p)} (${money}).`
				: `The ${teamLink(tid)} declined their team option on ${playerLink(p)}.`;
	await logEvent({
		type: "contract",
		text,
		showNotification: false,
		pids: [p.pid],
		tids: [tid],
		score: scoreOf(p),
	});
};

// A human team's re-signing offer he turned down, to test free agency - or
// that no longer fit the cap rules when it was decided.
export const logReSignRefused = async ({
	p,
	tid,
	amount,
	exp,
	chance,
	fits,
}: {
	p: Player;
	tid: number;
	amount: number;
	exp: number;
	chance: number;
	fits: boolean;
}) => {
	const money = `${helpers.formatCurrency(amount / 1000, "M")}/year through ${exp}`;
	await logEvent({
		type: "contract",
		text: fits
			? `${playerLink(p)} turned down an offer from the ${teamLink(tid)}: ${money} (${chancePercent(chance)} odds).`
			: `${playerLink(p)} became a free agent: an offer from the ${teamLink(tid)} (${money}) did not fit the cap rules.`,
		showNotification: false,
		pids: [p.pid],
		tids: [tid],
		score: scoreOf(p),
	});
};
