import { PHASE, PHASE_TEXT, PLAYER } from "../../../common/constants.ts";
import { howMethod, howPhrases } from "../../../common/signingHow.ts";
import type { CapStatusKey } from "../../../common/signingHow.ts";
import { getCurrentContract } from "../../../common/contracts.ts";
import type { EventBBGM, Player, SigningHow } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { getHardCap } from "../../util/getHardCap.ts";
import { team } from "../index.ts";
import {
	apron,
	cbaSettings,
	hardCap,
	maxFor,
	midLevelAmount,
	serviceYears,
	taxpayerMidLevelAmount,
} from "./cba.ts";
import { contractOf } from "./contractOf.ts";
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
// the cap, the apron and the hard cap, its exception, its cap holds - and the
// league's rules as numbers. Everything in thousands, as stored.

export type CapException =
	| { kind: "midLevel" | "taxpayerMidLevel"; amount: number }
	| { kind: "used" }
	| { kind: "none" };

export type CapHoldRow = {
	pid: number;
	name: string;
	amount: number;
	restricted: boolean;
};

export type TeamCapStatus = {
	tid: number;
	abbrev: string;
	name: string;
	payroll: number;
	holds: CapHoldRow[];
	holdsTotal: number;
	// Cap space after holds (negative: over the cap).
	capSpace: number;
	apronRoom: number;
	hardCap: number;
	hardCapRoom: number;
	status: CapStatusKey;
	exception: CapException;
	// Whether the holds can be renounced now (free agency in stages).
	canRenounce: boolean;
};

// Where a payroll stands: under the cap (after holds), over it, over the
// apron, or at the hard cap (no more than a minimum deal left under it).
export const capStatusOf = (
	tid: number,
	payroll: number,
	holds = 0,
): CapStatusKey => {
	if (hardCap(tid) - payroll < g.get("minContract")) {
		return "hardCap";
	}
	if (payroll > apron()) {
		return "apron";
	}
	if (payroll + holds > g.get("salaryCap")) {
		return "over";
	}
	return "under";
};

// The exception a team over the cap has left this season: the mid-level
// while it would stay under the apron, the taxpayer mid-level past it.
export const exceptionOf = (
	t: { tid: number; midLevelSeason?: number },
	payroll: number,
): CapException => {
	if (midLevelUsed(t)) {
		return { kind: "used" };
	}
	const minContract = g.get("minContract");
	const mle = Math.min(midLevelAmount(), apron() - payroll);
	const tmle = Math.min(taxpayerMidLevelAmount(), hardCap(t.tid) - payroll);
	if (mle >= tmle && mle >= minContract) {
		return { kind: "midLevel", amount: mle };
	}
	if (tmle >= minContract) {
		return { kind: "taxpayerMidLevel", amount: tmle };
	}
	return { kind: "none" };
};

// Holds count while a team's own players are unsigned: re-signing, and free
// agency in stages.
const holdsShown = () => g.get("phase") === PHASE.RESIGN_PLAYERS || inStages();

export const teamCapStatus = async (
	tid: number,
	pool?: readonly Player[],
): Promise<TeamCapStatus | undefined> => {
	if (!stagesOn()) {
		return undefined;
	}
	const t = await idb.cache.teams.get(tid);
	const info = g.get("teamInfoCache")[tid];
	const payroll = await team.getPayroll(tid);
	const holds: CapHoldRow[] = [];
	if (holdsShown()) {
		const freeAgents =
			pool ??
			(await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT));
		for (const p of freeAgents) {
			if (p.tid === PLAYER.FREE_AGENT && ownTid(p) === tid) {
				holds.push({
					pid: p.pid,
					name: `${p.firstName} ${p.lastName}`,
					amount: contractOf(p).amount,
					restricted: !!p.faRestricted,
				});
			}
		}
		holds.sort((a, b) => b.amount - a.amount);
	}
	const holdsTotal = holds.reduce((sum, row) => sum + row.amount, 0);
	const ceiling = hardCap(tid);
	return {
		tid,
		abbrev: info?.abbrev ?? "",
		name: info ? `${info.region} ${info.name}` : "",
		payroll,
		holds,
		holdsTotal,
		capSpace: g.get("salaryCap") - payroll - holdsTotal,
		apronRoom: apron() - payroll,
		hardCap: ceiling,
		hardCapRoom: ceiling - payroll,
		status: capStatusOf(tid, payroll, holdsTotal),
		exception: exceptionOf(t ?? { tid }, payroll),
		canRenounce: inStages(),
	};
};

// Every active team's cap status, for the league tracker.
export const leagueCapTracker = async () => {
	const pool = await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	);
	const out: TeamCapStatus[] = [];
	for (const t of await idb.cache.teams.getAll()) {
		if (t.disabled) {
			continue;
		}
		const status = await teamCapStatus(t.tid, pool);
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
	const maxContract = g.get("maxContract");
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
	const tiers = cbaSettings.maxTiers.map((tier, i) => {
		const next = cbaSettings.maxTiers[i + 1];
		return {
			seasons:
				next === undefined
					? `${tier.service}+`
					: `${tier.service}-${next.service - 1}`,
			amount: maxFor(tier.service),
			pct: pct(maxFor(tier.service) / salaryCap),
		};
	});
	return {
		salaryCap,
		apron: apron(),
		hardCap: defaultHardCap,
		hardCapPct: pct(cbaSettings.hardCapOverTax),
		ownHardCaps,
		minContract: g.get("minContract"),
		maxContract,
		midLevel: midLevelAmount(),
		midLevelPct: pct(cbaSettings.midLevel),
		taxpayerMidLevel: taxpayerMidLevelAmount(),
		taxpayerMidLevelPct: pct(cbaSettings.taxpayerMidLevel),
		tiers,
		years: {
			own: Math.min(cbaSettings.yearsOwn, maxLength),
			other: Math.min(cbaSettings.yearsOther, maxLength),
			minimum: Math.min(cbaSettings.yearsMinimum, maxLength),
			midLevel: Math.min(cbaSettings.midLevelYears, maxLength),
			taxpayerMidLevel: Math.min(cbaSettings.taxpayerYears, maxLength),
		},
		raiseOwn: pct(cbaSettings.raiseOwn),
		raiseOther: pct(cbaSettings.raiseOther),
		proveItPct: pct(stageSettings.discountShare),
		proveItYears: stageSettings.discountYears,
		extensionYears: Math.min(cbaSettings.yearsOwn, maxLength),
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

	const service = serviceYears(p);
	const bird = p.tid === PLAYER.FREE_AGENT ? abbrevOf(ownTid(p)) : deal?.bird;
	return {
		deal,
		freeAgent:
			p.tid === PLAYER.FREE_AGENT
				? {
						restricted:
							!!p.faRestricted ||
							(g.get("phase") === PHASE.RESIGN_PLAYERS &&
								p.contracts.at(-1)?.type === "rookie" &&
								!p.faRenounced),
					}
				: undefined,
		bird,
		service,
		maxPct: Math.round((100 * maxFor(service)) / g.get("salaryCap")),
		max: maxFor(service),
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
	service: number;
	maxPct: number;
	max: number;
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
