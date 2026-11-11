import { extensionSettings } from "./extensions.ts";
import { PHASE, PHASE_TEXT, PLAYER } from "../../../common/constants.ts";
import { howMethod, howPhrases } from "../../../common/signingHow.ts";
import type { CapStatusKey } from "../../../common/signingHow.ts";
import { getCurrentContract } from "../../../common/contracts.ts";
import type { EventBBGM, Player, SigningHow } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { getHardCap } from "../../util/getHardCap.ts";
import { team } from "../index.ts";
import { cbaSettings, hardCap, maxFor, midLevelAmount } from "./cba.ts";
import {
	offerSettings,
	PLAYER_OPTION_VALUE,
	TEAM_OPTION_COST,
} from "./offers.ts";
import {
	inStages,
	midLevelUsed,
	ownTid,
	stageIndex,
	STAGES,
	stageSettings,
	stagesOn,
} from "./stages.ts";

// WHAT THE PAGES SHOW OF THE CONTRACT RULES (owner's call, 2026-11-10:
// "very nicely laid out on every single page involving it ... as little
// complicated as possible"). A team's cap status - where it stands against
// the cap, the luxury tax and the hard cap, and its exception - and the
// league's rules as numbers. Everything in thousands, as stored.

export type CapException =
	| { kind: "midLevel"; amount: number }
	| { kind: "used" }
	| { kind: "none" };

export type TeamCapStatus = {
	tid: number;
	abbrev: string;
	name: string;
	payroll: number;
	// Negative: over the cap.
	capSpace: number;
	hardCap: number;
	hardCapRoom: number;
	status: CapStatusKey;
	exception: CapException;
};

// Where a payroll stands: under the cap, over it, over the luxury tax line
// ("apron"), or at the hard cap (no more than a minimum deal left under it).
export const capStatusOf = (tid: number, payroll: number): CapStatusKey => {
	if (hardCap(tid) - payroll < g.get("minContract")) {
		return "hardCap";
	}
	if (payroll > g.get("luxuryPayroll")) {
		return "apron";
	}
	if (payroll > g.get("salaryCap")) {
		return "over";
	}
	return "under";
};

// The mid-level a team has left this season, within its hard cap.
export const exceptionOf = (
	t: { tid: number; midLevelSeason?: number },
	payroll: number,
): CapException => {
	if (midLevelUsed(t)) {
		return { kind: "used" };
	}
	const mle = Math.min(midLevelAmount(), hardCap(t.tid) - payroll);
	return mle >= g.get("minContract")
		? { kind: "midLevel", amount: mle }
		: { kind: "none" };
};

export const teamCapStatus = async (
	tid: number,
): Promise<TeamCapStatus | undefined> => {
	if (!stagesOn()) {
		return undefined;
	}
	const t = await idb.cache.teams.get(tid);
	const info = g.get("teamInfoCache")[tid];
	const payroll = await team.getPayroll(tid);
	const ceiling = hardCap(tid);
	return {
		tid,
		abbrev: info?.abbrev ?? "",
		name: info ? `${info.region} ${info.name}` : "",
		payroll,
		capSpace: g.get("salaryCap") - payroll,
		hardCap: ceiling,
		hardCapRoom: ceiling - payroll,
		status: capStatusOf(tid, payroll),
		exception: exceptionOf(t ?? { tid }, payroll),
	};
};

// Every active team's cap status, for the league tracker.
export const leagueCapTracker = async () => {
	const out: TeamCapStatus[] = [];
	for (const t of await idb.cache.teams.getAll()) {
		if (t.disabled) {
			continue;
		}
		const status = await teamCapStatus(t.tid);
		if (status) {
			out.push(status);
		}
	}
	return out;
};

const pct = (share: number) => Math.round(share * 1000) / 10;

// The league's rules as numbers, for the Contract Rules page.
export const contractRules = () => {
	const salaryCap = g.get("salaryCap");
	const maxLength = g.get("maxContractLength");
	const defaultHardCap =
		g.get("luxuryPayroll") + cbaSettings.hardCapOverTax * salaryCap;
	// Teams the league's own, lower hard cap binds.
	const ownHardCaps: { tid: number; abbrev: string; amount: number }[] = [];
	for (const t of g.get("teamInfoCache").entries()) {
		const [tid, info] = t;
		if (info.disabled) {
			continue;
		}
		const amount = getHardCap(tid);
		if (amount < defaultHardCap) {
			ownHardCaps.push({ tid, abbrev: info.abbrev, amount });
		}
	}
	return {
		salaryCap,
		luxuryTax: g.get("luxuryPayroll"),
		hardCap: defaultHardCap,
		hardCapPct: pct(cbaSettings.hardCapOverTax),
		ownHardCaps,
		minContract: g.get("minContract"),
		maxContract: maxFor(0),
		maxContractPct: pct(maxFor(0) / salaryCap),
		midLevel: midLevelAmount(),
		midLevelPct: pct(cbaSettings.midLevel),
		maxLength,
		raiseOwn: pct(cbaSettings.raiseOwn),
		raiseOther: pct(cbaSettings.raiseOther),
		proveItPct: pct(stageSettings.discountShare),
		proveItYears: stageSettings.discountYears,
		// For the How it works tab.
		luxuryTaxRate: g.get("luxuryTax"),
		minRosterSize: g.get("minRosterSize"),
		maxRosterSize: g.get("maxRosterSize"),
		lateLongPct: pct(stageSettings.lateLongShare),
		playerOptionPct: pct(PLAYER_OPTION_VALUE),
		teamOptionPct: pct(TEAM_OPTION_COST),
		overAskKneePct: pct(offerSettings.overAskKnee - 1),
		extensionYears: maxLength,
		extensionsOn: extensionSettings.on,
		tier1: stageSettings.tier1,
		tier2: stageSettings.tier2,
		stages: [...STAGES],
		stage: inStages() ? stageIndex() : undefined,
	};
};

export type SigningRow = {
	eid: number;
	pid: number;
	name: string;
	tid: number;
	abbrev: string;
	season: number;
	when: string;
	type: "freeAgent" | "reSigned" | "extension" | "rookie";
	how?: SigningHow;
	years: number;
	total: number;
	amount: number;
	exp: number;
};

// The season's signings, from their events: who, where, when, and how.
export const signingsLog = async (season: number, tid?: number) => {
	const events = await idb.merged.events.getAll(
		{ season },
		{
			filter: (event: EventBBGM) =>
				(event.type === "freeAgent" || event.type === "reSigned") &&
				(tid === undefined || event.tids?.[0] === tid),
		},
	);
	const rows: SigningRow[] = [];
	for (const event of events) {
		if (
			(event.type !== "freeAgent" && event.type !== "reSigned") ||
			!event.contract
		) {
			continue;
		}
		const pid = event.pids[0];
		const p = await idb.merged.players.get(pid);
		const how = event.how;
		const phase = event.revert?.phase ?? event.phase;
		const when =
			how?.stage !== undefined
				? (STAGES[how.stage] ?? "")
				: how?.extension
					? "Season"
					: phase !== undefined
						? helpers.upperCaseFirstLetter(PHASE_TEXT[phase])
						: "";
		const start =
			phase !== undefined && phase > PHASE.PLAYOFFS
				? event.season + 1
				: event.season;
		const years = how?.years ?? Math.max(1, event.contract.exp - start + 1);
		rows.push({
			eid: event.eid,
			pid,
			name: p ? `${p.firstName} ${p.lastName}` : "???",
			tid: event.tids[0],
			abbrev: g.get("teamInfoCache")[event.tids[0]]?.abbrev ?? "???",
			season: event.season,
			when,
			type: how?.extension
				? "extension"
				: event.contract.rookie
					? "rookie"
					: event.type,
			how,
			years,
			total: how?.total ?? event.contract.amount * years,
			amount: event.contract.amount,
			exp: event.contract.exp,
		});
	}
	return rows.reverse();
};

// A player's contract under the rules, for his page: his deal in one line,
// what he'll be when it's up and who holds his Bird rights, his tier, each
// season's markers, and every contract he has signed and how.
type SigningEvent = Extract<EventBBGM, { type: "freeAgent" | "reSigned" }>;

export const contractCard = (
	p: Player,
	events: readonly EventBBGM[],
): ContractCard | undefined => {
	if (!stagesOn() || p.tid === PLAYER.RETIRED || p.tid < PLAYER.FREE_AGENT) {
		return undefined;
	}
	const abbrevOf = (tid: number | undefined) =>
		tid === undefined || tid < 0
			? undefined
			: g.get("teamInfoCache")[tid]?.abbrev;
	const signings = new Map<number, SigningEvent>();
	for (const event of events) {
		if (event.type === "freeAgent" || event.type === "reSigned") {
			signings.set(event.eid, event);
		}
	}

	// Extensions, by the season his deal ran through once each was signed.
	const extensions = new Map<number, SigningEvent>();
	for (const event of signings.values()) {
		if (event.type === "reSigned" && event.how?.extension && event.contract) {
			extensions.set(event.contract.exp, event);
		}
	}

	// Each season's markers: the option on a deal's last season, prove-it.
	const marks: Record<number, ("PO" | "TO" | "Prove-it")[]> = {};
	const history: ContractCard["history"] = [];
	let deal: ContractCard["deal"];
	const current = p.tid >= 0 ? getCurrentContract(p) : undefined;
	for (const c of p.contracts) {
		const exp = c.start + c.amounts.length - 1;
		for (let season = c.start; season <= exp; season++) {
			const list = (marks[season] ??= []);
			if (c.discount) {
				list.push("Prove-it");
			}
			if (season === exp && c.option) {
				list.push(c.option === "player" ? "PO" : "TO");
			}
		}
		// The seasons added by extensions, latest first, peeled off the end:
		// what is left is the deal as signed.
		const added: SigningEvent[] = [];
		let signedExp = exp;
		for (;;) {
			const extension = extensions.get(signedExp);
			const years = extension?.how?.years ?? 0;
			if (!extension || years <= 0 || signedExp - years < c.start) {
				break;
			}
			added.unshift(extension);
			signedExp -= years;
		}
		const event = c.eid !== undefined ? signings.get(c.eid) : undefined;
		const how = event?.how;
		const tid =
			event?.tids[0] ??
			(c.type === "rookie" ? p.draft.tid : c.payments[0]?.tid);
		const years = how?.years ?? signedExp - c.start + 1;
		history.push({
			season: event?.season ?? c.start,
			tid: tid ?? -1,
			abbrev: abbrevOf(tid) ?? "",
			years,
			total:
				how?.total ??
				c.amounts.slice(0, years).reduce((sum, amount) => sum + amount, 0),
			method:
				c.type === "rookie" || event?.contract?.rookie
					? "Rookie"
					: how
						? (howMethod(how)?.short ?? "")
						: "",
			how,
		});
		for (const extension of added) {
			history.push({
				season: extension.season,
				tid: extension.tids[0],
				abbrev: abbrevOf(extension.tids[0]) ?? "",
				years: extension.how?.years ?? 0,
				total: extension.how?.total ?? 0,
				method: "Ext",
				how: extension.how,
			});
		}

		// His deal now: the latest extension if he has one, or the deal as
		// signed.
		if (c === current) {
			const extension = added.at(-1);
			const signedHow = extension?.how ?? how;
			deal = {
				years: extension ? (extension.how?.years ?? 0) : years,
				total: extension
					? (extension.how?.total ?? 0)
					: c.amounts.reduce((sum, amount) => sum + amount, 0),
				exp,
				signed: extension?.season ?? event?.season,
				verb: extension ? "extended" : "signed",
				abbrev: abbrevOf(extension?.tids[0] ?? tid ?? p.tid) ?? "",
				method:
					!extension && c.type === "rookie"
						? "rookie deal"
						: signedHow
							? howPhrases({
									path: signedHow.path,
									offerSheet: signedHow.offerSheet,
								}).join(", ") || undefined
							: undefined,
				restricted: c.type === "rookie",
				bird: c.discount ? undefined : abbrevOf(p.tid),
			};
		}
	}
	history.sort((a, b) => a.season - b.season);

	const bird = p.tid === PLAYER.FREE_AGENT ? abbrevOf(ownTid(p)) : deal?.bird;
	return {
		deal,
		freeAgent:
			p.tid === PLAYER.FREE_AGENT
				? {
						restricted:
							!!p.faRestricted ||
							(g.get("phase") === PHASE.RESIGN_PLAYERS &&
								p.contracts.at(-1)?.type === "rookie"),
					}
				: undefined,
		bird,
		marks,
		history,
	};
};

export type ContractCard = {
	deal?: {
		years: number;
		total: number;
		exp: number;
		signed?: number;
		verb: "signed" | "extended";
		abbrev: string;
		method?: string;
		restricted: boolean;
		bird?: string;
	};
	freeAgent?: { restricted: boolean };
	bird?: string;
	marks: Record<number, ("PO" | "TO" | "Prove-it")[]>;
	history: {
		season: number;
		tid: number;
		abbrev: string;
		years: number;
		total: number;
		method: string;
		how?: SigningHow;
	}[];
};
