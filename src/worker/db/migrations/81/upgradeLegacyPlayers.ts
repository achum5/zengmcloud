import { getContractExp } from "../../../../common/contracts.ts";
import type { Phase } from "../../../../common/types.ts";
import {
	applyLegacyReleasedPlayers,
	convertLegacyContracts,
	type LegacyReleasedPlayer,
	type ReleaseEventInfo,
	type SigningEventInfo,
} from "../../../core/contract/convertLegacyContracts.ts";
import { updateOwedByTids } from "../../../core/contract/index.ts";

// The pieces of the version 81 upgrade (p.contract, p.salaries and the
// releasedPlayers store becoming p.contracts, p.contractDemand and
// p.owedByTids) that are needed outside the database upgrade too. A synced
// league can still receive players in the old shape after this device has
// upgraded - from a league mate whose device is still on an older build, from
// changes published before the upgrade that this device is only now catching
// up on, or from a checkpoint taken before it - and they have to be converted
// the same way the upgrade converted the ones already on this device.

export type LeagueStateForUpgrade = {
	fractionCurrentSeasonPaid: number;
	phase: Phase;
	season: number;
};

// A player record from before version 81. A record with p.contracts is
// current, even if it somehow still carries p.contract too.
export const isLegacyPlayerRecord = (p: unknown) => {
	if (!p || typeof p !== "object") {
		return false;
	}
	const record = p as Record<string, unknown>;
	return (
		!Array.isArray(record.contracts) &&
		(record.contract !== undefined || record.salaries !== undefined)
	);
};

// The old fields left on a record that is already in the current shape, like
// one a device on an older build received and then wrote back. Returns whether
// anything was removed.
export const removeStrayLegacyFields = (p: any) => {
	if (!p || typeof p !== "object" || !Array.isArray(p.contracts)) {
		return false;
	}
	let changed = false;
	for (const key of ["contract", "salaries"]) {
		if (key in p) {
			delete p[key];
			changed = true;
		}
	}
	return changed;
};

const pushToMap = <T>(map: Map<number, T[]>, key: number, value: T) => {
	let values = map.get(key);
	if (!values) {
		values = [];
		map.set(key, values);
	}
	values.push(value);
};

// Fraction of regular season games played so far in the current season, averaged over all teams
export const getFractionCurrentSeasonPaid = (
	teamSeasons: {
		won: number;
		lost: number;
		tied?: number;
		otl?: number;
	}[],
	numGames: number,
) => {
	if (teamSeasons.length === 0 || numGames <= 0) {
		return 0;
	}

	let gp = 0;
	for (const ts of teamSeasons) {
		gp += ts.won + ts.lost + (ts.tied ?? 0) + (ts.otl ?? 0);
	}

	return Math.min(1, gp / (teamSeasons.length * numGames));
};

export type LegacyContractInfoByPid = {
	releasedPlayersByPid: Map<number, LegacyReleasedPlayer[]>;
	signingEventsByPid: Map<number, SigningEventInfo[]>;
	releaseEventsByPid: Map<number, ReleaseEventInfo[]>;
};

export const createLegacyContractInfoByPid = (): LegacyContractInfoByPid => ({
	releasedPlayersByPid: new Map(),
	signingEventsByPid: new Map(),
	releaseEventsByPid: new Map(),
});

// Rows of the old releasedPlayers store
export const addLegacyReleasedPlayer = (
	info: LegacyContractInfoByPid,
	row: unknown,
) => {
	const rp = row as Partial<LegacyReleasedPlayer> | undefined;
	if (
		typeof rp?.pid === "number" &&
		typeof rp.tid === "number" &&
		typeof rp.contract?.amount === "number" &&
		typeof rp.contract.exp === "number"
	) {
		pushToMap(info.releasedPlayersByPid, rp.pid, {
			pid: rp.pid,
			tid: rp.tid,
			contract: {
				amount: rp.contract.amount,
				exp: rp.contract.exp,
			},
		});
	}
};

// Signing and release events say when contracts started and ended
export const addLegacyEvent = (info: LegacyContractInfoByPid, event: any) => {
	const pid = event?.pids?.[0];
	const tid = event?.tids?.[0];
	if (typeof pid !== "number" || typeof tid !== "number") {
		return;
	}

	if (
		(event.type === "freeAgent" || event.type === "reSigned") &&
		event.contract &&
		typeof event.eid === "number"
	) {
		pushToMap(info.signingEventsByPid, pid, {
			eid: event.eid,
			type: event.type,
			season: event.season,
			phase: event.phase,
			tid,
			amount: event.contract.amount,
			exp: event.contract.exp,
		});
	} else if (event.type === "release") {
		pushToMap(info.releaseEventsByPid, pid, {
			season: event.season,
			phase: event.phase,
			tid,
		});
	}
};

// Convert one old-shape player record in place. See convertLegacyContracts.
export const upgradeLegacyPlayer = (
	p: any,
	leagueState: LeagueStateForUpgrade,
	info: LegacyContractInfoByPid,
) => {
	const { contracts, contractDemand } = convertLegacyContracts({
		p,
		leagueState,
		releasedPlayers: info.releasedPlayersByPid.get(p.pid) ?? [],
		releaseEvents: info.releaseEventsByPid.get(p.pid) ?? [],
		signingEvents: info.signingEventsByPid.get(p.pid) ?? [],
	});

	delete p.contract;
	delete p.salaries;
	p.contracts = contracts;
	delete p.contractDemand;
	if (contractDemand) {
		p.contractDemand = contractDemand;
	}
	updateOwedByTids(p);
};

// Same release, already recorded on one of his contracts
const hasRelease = (contracts: any[], rp: LegacyReleasedPlayer) =>
	contracts.some(
		(contract) =>
			contract.endedEarly?.tid === rp.tid &&
			contract.amounts?.[0] === rp.contract.amount &&
			getContractExp(contract) === rp.contract.exp,
	);

// What this device already knows about a player's releases, from his current
// record. An old-shape record of him has no releasedPlayers rows with it (they
// were their own store, and those rows rode in whichever change made the
// release), so without this, converting it would forget that a team still
// owes him money.
export const addReleasesFromCurrentRecord = (
	info: LegacyContractInfoByPid,
	current: any,
) => {
	if (!current || !Array.isArray(current.contracts)) {
		return;
	}

	for (const contract of current.contracts) {
		const tid = contract.endedEarly?.tid;
		if (typeof tid !== "number" || !Array.isArray(contract.amounts)) {
			continue;
		}

		const rp: LegacyReleasedPlayer = {
			pid: current.pid,
			tid,
			contract: {
				amount: contract.amounts[0],
				exp: getContractExp(contract),
			},
		};
		const existing = info.releasedPlayersByPid.get(current.pid) ?? [];
		if (
			existing.some(
				(row) =>
					row.tid === rp.tid &&
					row.contract.amount === rp.contract.amount &&
					row.contract.exp === rp.contract.exp,
			)
		) {
			continue;
		}
		pushToMap(info.releasedPlayersByPid, current.pid, rp);
		pushToMap(info.releaseEventsByPid, current.pid, {
			season: contract.endedEarly.season,
			phase: contract.endedEarly.phase,
			tid,
		});
	}
};

// Apply old releasedPlayers rows to a current-shape record, in place. Returns
// whether anything changed. Rows already recorded on his contracts are skipped,
// so applying the same release twice is harmless.
export const applyLegacyReleasesToCurrentRecord = (
	p: any,
	leagueState: LeagueStateForUpgrade,
	releasedPlayers: LegacyReleasedPlayer[],
	releaseEvents: ReleaseEventInfo[],
) => {
	const toApply = releasedPlayers.filter((rp) => !hasRelease(p.contracts, rp));
	if (toApply.length === 0) {
		return false;
	}

	applyLegacyReleasedPlayers({
		contractDemand: p.contractDemand,
		contracts: p.contracts,
		leagueState,
		releasedPlayers: toApply,
		releaseEvents,
		tid: p.tid,
	});
	updateOwedByTids(p);
	return true;
};
