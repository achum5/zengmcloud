import { PHASE } from "../../../common/constants.ts";
import {
	getContractExp,
	getCurrentContract,
} from "../../../common/contracts.ts";
import { uniformSeed } from "../../../common/random.ts";
import type { Contract, Player, Team } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import {
	getProjectedContractAmounts,
	projectNextContract,
} from "../../util/projectedContracts.ts";
import { team } from "../index.ts";
import {
	amountsWithRaise,
	cbaSettings,
	checkSigning,
	hardCap,
	maxFor,
	serviceYears,
} from "./cba.ts";
import { extensionChance, extensionDecision } from "./freeAgency.ts";
import { aiTids, currentOutlook, loadTeam, type TeamState } from "./market.ts";
import { askFor, askingAs } from "./offers.ts";
import { stagesOn } from "./stages.ts";

// EXTENSIONS (CBA-lite rule 9, docs/free-agency-overhaul.md): in the last
// season of his contract, during the regular season, a team may add seasons
// to its player's deal - Bird rights, up to 5 more years, raises within 8%,
// an option on the final season, no more than his tier's max, and next
// season's payroll within the hard cap. He takes it or waits for free agency
// with odds, as when re-signing, on a roll fixed for him for the season: the
// same offer again changes nothing, a better one may do it. Turned down this
// many times in a season, a team gets no more answers until the next.
export const extensionSettings = {
	patience: 3,
};

export type ExtensionOffer = {
	amount: number;
	// Seasons past this one.
	years: number;
	raise?: number;
	option?: "player" | "team";
};

export const extensionsOpen = () => {
	const phase = g.get("phase");
	return (
		stagesOn() &&
		(phase === PHASE.REGULAR_SEASON || phase === PHASE.AFTER_TRADE_DEADLINE)
	);
};

// On a team, in the last season of his contract, and not in his first
// season since the draft.
export const canExtend = (
	p: Pick<Player, "tid" | "draft" | "contractDemand" | "contracts">,
) => {
	const season = g.get("season");
	if (!extensionsOpen() || p.tid < 0 || p.draft.year >= season - 1) {
		return false;
	}
	const contract = getCurrentContract(p);
	return !!contract && getContractExp(contract) === season;
};

export const extensionLengths = () => {
	const out: number[] = [];
	const most = Math.min(cbaSettings.yearsOwn, g.get("maxContractLength"));
	for (let years = 1; years <= most; years++) {
		out.push(years);
	}
	return out;
};

// What he would ask as a free agent this summer, and for how long: his
// projected next contract (the auction his re-signing ask comes from), within
// his tier's max.
export const extensionAsk = async (p: Player) => {
	const next = projectNextContract(p, await getProjectedContractAmounts());
	const lengths = extensionLengths();
	return {
		amount: helpers.bound(
			helpers.roundContract(next.amount),
			g.get("minContract"),
			maxFor(serviceYears(p)),
		),
		years: helpers.bound(next.years, lengths[0]!, lengths.at(-1)!),
	};
};

// Why a team may not extend him on these terms, if it may not.
export const extensionError = async (
	p: Player,
	tid: number,
	offer: ExtensionOffer,
) => {
	if (p.tid !== tid || !canExtend(p)) {
		return "He can't be extended now.";
	}
	const lengths = extensionLengths();
	if (!lengths.includes(offer.years)) {
		return `An extension is 1 to ${lengths.at(-1)} years.`;
	}
	if (
		!(offer.amount >= g.get("minContract")) ||
		offer.amount > g.get("maxContract")
	) {
		return "That is not a contract the league allows.";
	}
	if (Math.abs(offer.raise ?? 0) > cbaSettings.raiseOwn + 1e-9) {
		return `Raises are limited to ${Math.round(cbaSettings.raiseOwn * 100)}% a year.`;
	}
	if (offer.option && offer.years < 2) {
		return "An option needs a deal of two years or more.";
	}
	// Bird rights: over the cap is fine, past his max or the hard cap next
	// season is not - no roster exception, he is on the roster already.
	const check = checkSigning({
		tid,
		payroll: await team.getPayroll(tid, g.get("season") + 1),
		amount: offer.amount,
		service: serviceYears(p),
		own: true,
		midLevelUsed: true,
		rosterSize: Infinity,
	});
	return check.ok ? undefined : `${check.reason}.`;
};

// The new seasons on the end of his contract, with the raise, and the option
// if any; a rookie contract with an extension is no longer one.
export const applyExtension = (contract: Contract, offer: ExtensionOffer) => {
	contract.amounts = [
		...contract.amounts,
		...amountsWithRaise(offer.amount, offer.years, offer.raise ?? 0).map(
			(amount) => helpers.roundContract(amount),
		),
	];
	if (offer.option && offer.years >= 2) {
		contract.option = offer.option;
	} else {
		delete contract.option;
	}
	if (contract.type === "rookie") {
		contract.type = "unknown";
	}
};

const extend = async (p: Player, tid: number, offer: ExtensionOffer) => {
	const contract = getCurrentContract(p)!;
	const before = contract.amounts.length;
	applyExtension(contract, offer);
	await idb.cache.players.put(p);
	const added = contract.amounts.slice(before);
	await logEvent({
		type: "reSigned",
		showNotification: false,
		pids: [p.pid],
		tids: [tid],
		score: Math.round(helpers.bound(p.valueFuzz - 45, 0, Infinity)),
		contract: { amount: offer.amount, exp: getContractExp(contract) },
		// An extension is by his Bird rights (extensionError checked the rest).
		how: {
			extension: true,
			path: "bird",
			...(contract.option ? { option: contract.option } : {}),
			years: added.length,
			total: added.reduce((sum, amount) => sum + amount, 0),
		},
	});
};

// His answer, fixed for the season.
export const extensionRoll = (p: Pick<Player, "pid">) =>
	uniformSeed(p.pid * 6_421 + g.get("season"));

const WONT_TALK = "He won't talk about an extension again this season.";

const declinesOf = (t: Pick<Team, "extensionDeclines"> | undefined) =>
	(t?.extensionDeclines ?? []).filter((x) => x.season === g.get("season"));

// The extension editor: his ask at each length, the chance of an offer (his
// ask for the length he wants, if none), why it can't be made, if it can't,
// and how many the team has had turned down.
export const extensionProps = async (
	tid: number,
	pid: number,
	offer?: ExtensionOffer,
) => {
	const p = await idb.cache.players.get(pid);
	if (!p || p.tid !== tid || !canExtend(p)) {
		return "He can't be extended now.";
	}
	const ask = await extensionAsk(p);
	const asking = askingAs(p, ask);
	const most = maxFor(serviceYears(p));
	const terms = extensionLengths().map((years) => ({
		years,
		exp: g.get("season") + years,
		ask: helpers.bound(
			helpers.roundContract(askFor(asking, years)),
			g.get("minContract"),
			most,
		),
	}));
	const wanted = terms.find((term) => term.years === ask.years) ?? terms[0]!;
	const shown = offer ?? { amount: wanted.ask, years: wanted.years };
	const declined =
		declinesOf(await idb.cache.teams.get(tid)).find((x) => x.pid === pid)
			?.count ?? 0;
	return {
		terms,
		wantedYears: wanted.years,
		minContract: g.get("minContract"),
		maxContract: Math.min(most, g.get("maxContract")),
		maxRaise: cbaSettings.raiseOwn,
		chance: await extensionChance(p, ask, shown),
		error:
			declined >= extensionSettings.patience
				? WONT_TALK
				: await extensionError(p, tid, shown),
		declined,
		patience: extensionSettings.patience,
	};
};

// A team's offer to extend him, answered now: the error if it can't be made,
// or whether he took it.
export const offerExtension = async (
	tid: number,
	pid: number,
	offer: ExtensionOffer,
) => {
	const p = await idb.cache.players.get(pid);
	const t = await idb.cache.teams.get(tid);
	if (!p || !t) {
		return "Player not found.";
	}
	const declines = declinesOf(t);
	const before = declines.find((x) => x.pid === pid)?.count ?? 0;
	if (before >= extensionSettings.patience) {
		return WONT_TALK;
	}
	const error = await extensionError(p, tid, offer);
	if (error !== undefined) {
		return error;
	}
	const chance = await extensionChance(p, await extensionAsk(p), offer);
	const accepted = extensionRoll(p) < chance;
	const declined = accepted ? before : before + 1;
	if (accepted) {
		await extend(p, tid, offer);
	} else {
		t.extensionDeclines = [
			...declines.filter((x) => x.pid !== pid),
			{ pid, season: g.get("season"), count: declined },
		];
		await idb.cache.teams.put(t);
	}
	return {
		accepted,
		chance,
		declined,
		patience: extensionSettings.patience,
	};
};

// Once a season, as the regular season opens: each CPU team offers to extend
// its players in the last season of their contracts, best first, as it
// re-signs them (extensionDecision), and each answers on his roll.
export const cpuExtensions = async () => {
	if (!extensionsOpen()) {
		return;
	}
	const minContract = g.get("minContract");
	const outlook = await currentOutlook();
	for (const tid of await aiTids()) {
		const players = (await idb.cache.players.indexGetAll("playersByTid", tid))
			.filter((p) => canExtend(p))
			.sort((a, b) => b.value - a.value || a.pid - b.pid);
		// The team as it stands, again after each extension.
		let t: TeamState | undefined;
		for (const p of players) {
			const payroll = await team.getPayroll(tid, g.get("season") + 1);
			const cap = Math.min(
				maxFor(serviceYears(p)),
				g.get("maxContract"),
				hardCap(tid) - payroll,
			);
			if (cap < minContract) {
				continue;
			}
			t ??= await loadTeam(outlook, tid);
			const offer = await extensionDecision(
				outlook,
				t,
				p,
				await extensionAsk(p),
				extensionLengths(),
				cap,
			);
			if (
				offer &&
				extensionRoll(p) < offer.chance &&
				(await extensionError(p, tid, offer)) === undefined
			) {
				await extend(p, tid, { amount: offer.amount, years: offer.years });
				t = undefined;
			}
		}
	}
};
