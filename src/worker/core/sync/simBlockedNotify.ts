import { g, local } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { getSyncEngine } from "./engineHolder.ts";

// A user team that's breaking a roster-size limit, so a sim can't run.
type Blocker = { tid: number; over: boolean; count: number; limit: number };

// Signature of the roster block we last announced to the room, so a sim that
// keeps getting skipped for the SAME reason (e.g. an auto-play timer firing
// every 20 min while a roster stays over the cap) pings the room only once.
// Re-announces when a DIFFERENT team blocks; cleared on any successful sim
// (clearRosterBlockNotice) so a block that's fixed and later reappears pings
// again.
let lastSignature: string | undefined;

// Called on a successful sim so the next block (even the same team again) is
// announced fresh rather than deduped against a stale signature.
export const clearRosterBlockNotice = () => {
	lastSignature = undefined;
};

const rosterBlockers = async (): Promise<Blocker[]> => {
	// Mirror checkRosterSizes' "user team, actively managed" condition: under the
	// built-in unattended auto-play (autoPlayUntil) or spectator mode, user
	// rosters are auto-pruned and never block, so there's nothing to announce.
	if (local.autoPlayUntil || g.get("spectator")) {
		return [];
	}
	const maxRosterSize = g.get("maxRosterSize");
	const minRosterSize = g.get("minRosterSize");
	const blockers: Blocker[] = [];
	for (const tid of g.get("userTids")) {
		const players = await idb.cache.players.indexGetAll("playersByTid", tid);
		const n = players.length;
		if (n > maxRosterSize) {
			blockers.push({ tid, over: true, count: n, limit: maxRosterSize });
		} else if (n < minRosterSize) {
			blockers.push({ tid, over: false, count: n, limit: minRosterSize });
		}
	}
	return blockers;
};

const teamName = (tid: number): string => {
	const info = g.get("teamInfoCache")[tid];
	if (!info) {
		return "A team";
	}
	return `${info.region} ${info.name}`.trim() || info.abbrev || "A team";
};

// When a scheduled (or manual) sim is skipped because a user team is over (or
// under) the roster limit, announce it to the room so everyone knows why the
// timer came and went - otherwise a follower just sees no sim and no reason.
// No-op outside a sync room, and deduped so a persistent block pings once.
export const notifyRosterBlockedSim = async () => {
	const engine = getSyncEngine();
	if (!engine) {
		return;
	}
	const blockers = await rosterBlockers();
	if (blockers.length === 0) {
		// The block cleared between the sim's own check and here; nothing to say.
		lastSignature = undefined;
		return;
	}

	const signature = blockers
		.map((b) => `${b.tid}:${b.over ? "over" : "under"}:${b.count}`)
		.sort()
		.join("|");
	if (signature === lastSignature) {
		return;
	}
	lastSignature = signature;

	const describe = (b: Blocker) =>
		b.over
			? `${teamName(b.tid)} have ${b.count} players (max ${b.limit})`
			: `${teamName(b.tid)} have ${b.count} players (min ${b.limit})`;

	const title =
		blockers.length > 1
			? "Sim skipped — roster limits"
			: blockers[0]!.over
				? "Sim skipped — roster over the limit"
				: "Sim skipped — roster under the limit";

	const allOver = blockers.every((b) => b.over);
	const fix = allOver
		? "Trim the roster to the limit and the next scheduled sim will run."
		: "Fix the roster and the next scheduled sim will run.";
	const body = `${blockers.map(describe).join("; ")}. ${fix}`;

	try {
		await engine.publishNotification({
			title,
			body,
			targetTids: null,
			path: "roster",
		});
	} catch {
		// A missed announcement is harmless; it re-announces when the signature
		// next changes.
	}
};
