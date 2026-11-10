import { PHASE, PLAYER } from "../../../common/constants.ts";
import type {
	FaDayResultItem,
	FaDayResults,
	Negotiation,
	Player,
} from "../../../common/types.ts";
import { range } from "../../../common/utils.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import {
	offerChance,
	offersOpen,
	reSignChance,
	reSigningWith,
} from "./freeAgency.ts";
import { realisticFreeAgencyOn } from "./isOn.ts";
import { askFor, offerYears, wantedYears } from "./offers.ts";
import type { ContractHolder } from "./contractOf.ts";
import { inStages, ownTid, stageIndex, STAGES } from "./stages.ts";

// WHAT A HUMAN TEAM SEES OF THE OFFER MARKET (owner's calls: no instant
// signing - a human's offer waits for the player's decision like anyone's;
// blind bidding; the offer screen shows your odds). The offers themselves
// live on the team, as every team's do (t.offerBoard, freeAgency.ts); this
// is the reading side for the offer screen, the Free Agents page and the
// re-signing list.

const firstSeason = () =>
	g.get("season") + (g.get("phase") <= PHASE.AFTER_TRADE_DEADLINE ? 0 : 1);

// A player drafted this season who has no rookie contract yet. He signs as in
// stock, as CPU teams' draft picks do.
export const unsignedDraftPick = (
	p: Pick<Player, "draft">,
	negotiation: Pick<Negotiation, "resigning"> | undefined,
) =>
	!!negotiation?.resigning &&
	g.get("phase") === PHASE.RESIGN_PLAYERS &&
	p.draft.year === g.get("season");

// No instant signing: a contract is offered, and he decides. Unsigned draft
// picks sign as in stock.
export const instantSigningRefusal = async (pid: number) => {
	if (!realisticFreeAgencyOn()) {
		return undefined;
	}
	const p = await idb.cache.players.get(pid);
	const negotiation = await idb.cache.negotiations.get(pid);
	if (p && unsignedDraftPick(p, negotiation)) {
		return undefined;
	}
	return "Make him an offer instead.";
};

export type OfferTerm = {
	years: number;
	exp: number;
	// His asking price for this length, in thousands.
	ask: number;
};

// Every length the league allows, with his asking price for it.
export const offerTerms = (p: ContractHolder & Pick<Player, "ratings">) => {
	const minContract = g.get("minContract");
	const maxContract = g.get("maxContract");
	return range(g.get("minContractLength"), g.get("maxContractLength") + 1).map(
		(years): OfferTerm => ({
			years,
			exp: firstSeason() + years - 1,
			ask: helpers.bound(
				helpers.roundContract(askFor(p, years)),
				minContract,
				maxContract,
			),
		}),
	);
};

// The length he wants, within what the league allows.
export const wantedTerm = (p: ContractHolder & Pick<Player, "ratings">) => {
	const terms = offerTerms(p);
	const years = helpers.bound(
		wantedYears(p),
		terms[0]!.years,
		terms.at(-1)!.years,
	);
	return terms.find((term) => term.years === years)!;
};

// The chance he takes this offer: against his other offers and waiting in
// free agency, or against testing the market when his own team re-signs him.
export const chanceOfOffer = async (
	tid: number,
	offer: {
		pid: number;
		amount: number;
		exp: number;
		raise?: number;
		option?: "player" | "team";
	},
) => {
	const p = await idb.cache.players.get(offer.pid);
	if (!p || p.tid !== PLAYER.FREE_AGENT) {
		return 0;
	}
	if (await reSigningWith(tid, p.pid)) {
		return reSignChance(p, tid, offer);
	}
	return offerChance(tid, offer);
};

// The team's standing offers to players who are still free agents, in its
// order of priority. Offers to players who signed are dropped from the board
// at the end of each day; until then they are not shown.
export const liveOffers = async (tid: number) => {
	const t = await idb.cache.teams.get(tid);
	const out: { p: Player; amount: number; exp: number }[] = [];
	for (const offer of t?.offerBoard ?? []) {
		const p = await idb.cache.players.get(offer.pid);
		if (p && p.tid === PLAYER.FREE_AGENT) {
			out.push({ p, amount: offer.amount, exp: offer.exp });
		}
	}
	return out;
};

// Free agency in stages: the stage he decides in, short ("Day 1", "Open
// market"), and the number to beat - the leading offer's money and years,
// never who made it. Undefined when free agency is not in stages.
export const stageOf = (
	p: Pick<
		Player,
		| "faDecides"
		| "faLead"
		| "stats"
		| "faRenounced"
		| "faRestricted"
		| "faOfferSheet"
		| "faMatch"
		| "contracts"
	>,
	userTid?: number,
) => {
	if (!inStages()) {
		return undefined;
	}
	// The user's own free agent (Bird rights, cap hold), and any offer sheet
	// for him to match - its team public, as in the NBA.
	const own = userTid !== undefined && ownTid(p) === userTid;
	const sheet = own ? p.faOfferSheet : undefined;
	// Once his stage has passed, he decides in each one after it.
	const decides = Math.max(
		p.faDecides ?? STAGES.indexOf("Open market"),
		stageIndex(),
	);
	return {
		decides,
		decidesLabel: STAGES[decides]!.replace("Decision day", "Day"),
		toBeat: p.faLead
			? { amount: p.faLead.amount, years: offerYears(p.faLead.exp) }
			: undefined,
		own,
		restricted: own && !!p.faRestricted,
		sheet: sheet
			? {
					amount: sheet.amount,
					years: offerYears(sheet.exp),
					abbrev: g.get("teamInfoCache")[sheet.tid]?.abbrev ?? "",
				}
			: undefined,
		match: own && !!p.faMatch,
	};
};

// The offer screen for one player: his asking price at each length, the
// team's offer standing (if any) and its place among the team's offers, and
// the chance of the offer shown - the standing one, or his ask for the length
// he wants.
export const offerScreen = async (tid: number, pid: number) => {
	const p = await idb.cache.players.get(pid);
	if (!p) {
		return "Player not found.";
	}
	if (p.tid !== PLAYER.FREE_AGENT) {
		return `${p.firstName} ${p.lastName} is not a free agent.`;
	}
	const reSigning = await reSigningWith(tid, pid);
	if (!reSigning && !offersOpen()) {
		return "Free agents can't be offered contracts now.";
	}
	const terms = offerTerms(p);
	const wanted = wantedTerm(p);
	const offers = await liveOffers(tid);
	const i = offers.findIndex((offer) => offer.p.pid === pid);
	const standing = i >= 0 ? offers[i] : undefined;
	const shown = standing
		? { amount: standing.amount, exp: standing.exp }
		: { amount: wanted.ask, exp: wanted.exp };
	return {
		reSigning,
		terms,
		wantedYears: wanted.years,
		current: standing
			? {
					amount: standing.amount,
					exp: standing.exp,
					years: offerYears(standing.exp),
				}
			: undefined,
		rank: i >= 0 ? i + 1 : undefined,
		numOffers: offers.length,
		minContract: g.get("minContract"),
		maxContract: g.get("maxContract"),
		chance: await chanceOfOffer(tid, { pid, ...shown }),
		stage: stageOf(p, tid),
	};
};

// The team's offers for the Free Agents page, with each one's chance today.
export const offersList = async (tid: number) => {
	const out: {
		pid: number;
		name: string;
		amount: number;
		exp: number;
		years: number;
		chance: number;
	}[] = [];
	for (const { p, amount, exp } of await liveOffers(tid)) {
		out.push({
			pid: p.pid,
			name: `${p.firstName} ${p.lastName}`,
			amount,
			exp,
			years: offerYears(exp),
			chance: await chanceOfOffer(tid, { pid: p.pid, amount, exp }),
		});
	}
	return out;
};

// The re-signing list's offers: for each of the team's players whose
// contract ran out, its offer (undefined: he is let go) and its chance, and
// his ask at each length.
export const reSigningOffers = async (tid: number, players: Player[]) => {
	const t = await idb.cache.teams.get(tid);
	const negotiations = await idb.cache.negotiations.getAll();
	const out = new Map<
		number,
		{
			terms: OfferTerm[];
			wantedYears: number;
			offer: { amount: number; exp: number; years: number } | undefined;
			chance: number | undefined;
		}
	>();
	for (const p of players) {
		const negotiation = negotiations.find(
			(n) => n.pid === p.pid && n.tid === tid,
		);
		if (!negotiation?.resigning || unsignedDraftPick(p, negotiation)) {
			continue;
		}
		const offer = t?.offerBoard?.find((x) => x.pid === p.pid);
		out.set(p.pid, {
			terms: offerTerms(p),
			wantedYears: wantedTerm(p).years,
			offer: offer
				? { amount: offer.amount, exp: offer.exp, years: offerYears(offer.exp) }
				: undefined,
			chance: offer ? await reSignChance(p, tid, offer) : undefined,
		});
	}
	return out;
};

// The most recent day's signings in the offer market, for the Free Agents
// page: who signed where, every offer he had with its odds, and the roll.
// Only decisions are recorded, so no offer is seen before one is made.
export const lastChoices = async () => {
	const season = g.get("season");
	let rows: FaDayResults[] = await idb.cache.faDayResults.getAll();
	try {
		rows = [
			...rows,
			...(((await (idb.league as any).getAll("faDayResults")) ??
				[]) as FaDayResults[]),
		];
	} catch {
		// Not in the database yet.
	}
	let latest: FaDayResults | undefined;
	for (const row of rows) {
		if (
			(row.season === season || row.season === season - 1) &&
			row.items.some((item) => item.type === "choice") &&
			(!latest || row.at > latest.at)
		) {
			latest = row;
		}
	}
	if (!latest) {
		return undefined;
	}
	// In stages, the stage they came in (daysLeft counts the stages down).
	const stage =
		inStages() &&
		latest.season === season &&
		latest.daysLeft >= 1 &&
		latest.daysLeft <= STAGES.length
			? STAGES[STAGES.length - latest.daysLeft]
			: undefined;
	return {
		daysLeft: latest.daysLeft,
		stage,
		at: latest.at,
		items: latest.items.filter(
			(item): item is Extract<FaDayResultItem, { type: "choice" }> =>
				item.type === "choice",
		),
	};
};

// Every day or stage of a free agency's signings, oldest first, labeled by
// stage - the history the Free Agents page browses. A free agency runs from
// the end of `season` through the next regular season.
export const choiceHistory = async (season: number) => {
	const byKey = new Map<string, FaDayResults>();
	try {
		for (const row of ((await (idb.league as any).getAll("faDayResults")) ??
			[]) as FaDayResults[]) {
			byKey.set(row.key, row);
		}
	} catch {
		// Not in the database yet.
	}
	for (const row of await idb.cache.faDayResults.getAll()) {
		byKey.set(row.key, row);
	}
	const out: {
		key: string;
		label: string;
		at: number;
		items: Extract<FaDayResultItem, { type: "choice" }>[];
	}[] = [];
	for (const row of byKey.values()) {
		const parts = row.key.split("-");
		if (parts.length !== 3) {
			continue;
		}
		const phase = Number(parts[1]);
		const offseason = phase > PHASE.PLAYOFFS;
		if (offseason ? row.season !== season : row.season !== season + 1) {
			continue;
		}
		const items = row.items.filter(
			(item): item is Extract<FaDayResultItem, { type: "choice" }> =>
				item.type === "choice",
		);
		if (items.length === 0) {
			continue;
		}
		out.push({
			key: row.key,
			label:
				row.stage !== undefined
					? STAGES[row.stage]!
					: offseason
						? `${row.daysLeft} ${helpers.plural("day", row.daysLeft)} left`
						: "Season",
			at: row.at,
			items,
		});
	}
	return out.sort((a, b) => a.at - b.at);
};
