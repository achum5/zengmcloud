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
