import { idb } from "../../db/index.ts";

// Where CPU teams' standing trade proposals to humans are kept: on the CPU
// team (proposals.ts makes them). Apart so the trade screen can keep them
// current without the market.

type Stored = NonNullable<
	Awaited<ReturnType<typeof idb.cache.teams.get>>
>["tradeProposals"];

const stillHas = async (tid: number, pids: number[], dpids: number[]) => {
	for (const pid of pids) {
		if ((await idb.cache.players.get(pid))?.tid !== tid) {
			return false;
		}
	}
	for (const dpid of dpids) {
		if ((await idb.cache.draftPicks.get(dpid))?.tid !== tid) {
			return false;
		}
	}
	return true;
};

const standing = async (cpuTid: number, list: NonNullable<Stored>) => {
	const kept = [];
	for (const x of list) {
		if (
			(await stillHas(cpuTid, x.give.pids, x.give.dpids)) &&
			(await stillHas(x.tid, x.get.pids, x.get.dpids))
		) {
			kept.push(x);
		}
	}
	return kept;
};

// A proposal is off once any of its assets has moved.
export const dropMovedProposals = async () => {
	for (const t of await idb.cache.teams.getAll()) {
		if (!t.tradeProposals) {
			continue;
		}
		const kept = await standing(t.tid, t.tradeProposals);
		if (kept.length !== t.tradeProposals.length) {
			t.tradeProposals = kept.length > 0 ? kept : undefined;
			await idb.cache.teams.put(t);
		}
	}
};

// The proposals standing to a human team.
export const proposalsTo = async (humanTid: number) => {
	const out: {
		tid: number;
		give: { pids: number[]; dpids: number[] };
		get: { pids: number[]; dpids: number[] };
	}[] = [];
	for (const t of await idb.cache.teams.getAll()) {
		const x = t.tradeProposals?.find((y) => y.tid === humanTid);
		if (x && (await standing(t.tid, [x])).length > 0) {
			out.push({ tid: t.tid, give: x.give, get: x.get });
		}
	}
	return out;
};

// A proposal's assets, both sides, as one string.
export const proposalKey = (x: {
	give: { pids: readonly number[]; dpids: readonly number[] };
	get: { pids: readonly number[]; dpids: readonly number[] };
}) =>
	[
		[...x.give.pids].sort((a, b) => a - b).join(),
		[...x.give.dpids].sort((a, b) => a - b).join(),
		[...x.get.pids].sort((a, b) => a - b).join(),
		[...x.get.dpids].sort((a, b) => a - b).join(),
	].join("|");

// How many turned-down proposals a team remembers.
const DECLINED_KEPT = 10;

// A human team turns down a CPU team's proposal: it is off, and the CPU team
// won't make it again.
export const declineProposal = async (cpuTid: number, humanTid: number) => {
	const t = await idb.cache.teams.get(cpuTid);
	const x = t?.tradeProposals?.find((y) => y.tid === humanTid);
	if (!t || !x) {
		return;
	}
	const rest = t.tradeProposals!.filter((y) => y !== x);
	t.tradeProposals = rest.length > 0 ? rest : undefined;
	t.declinedTradeProposals = [
		...(t.declinedTradeProposals ?? []),
		{ tid: humanTid, key: proposalKey(x) },
	].slice(-DECLINED_KEPT);
	await idb.cache.teams.put(t);
};

export const wasDeclined = (
	t: { declinedTradeProposals?: { tid: number; key: string }[] },
	humanTid: number,
	key: string,
) =>
	(t.declinedTradeProposals ?? []).some(
		(x) => x.tid === humanTid && x.key === key,
	);
