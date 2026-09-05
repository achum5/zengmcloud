import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";
import { sanitizeRotation } from "../../common/rotation.ts";
import {
	generateRotation,
	type RotationCandidate,
} from "../core/team/generateRotation.ts";
import addFirstNameShort from "../util/addFirstNameShort.ts";

// THE ROTATION PAGE: a team's plan, and the men it is drawn from.
//
// What the page edits is the plan on the team row. What it shows is either
// that plan or, for a team that has left the rotation to the coach, the plan
// the coach would draw up today - so taking control starts from something
// sensible rather than a blank grid, and handing control back does not lose
// what was drawn.

const updateRotation = async (
	{ abbrev, tid }: ViewInput<"rotation">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("gameAttributes") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("team") ||
		tid !== state.tid
	) {
		const enabled = g.get("rotationPlans");
		const numPeriods = g.get("numPeriods");
		const periodLength = g.get("quarterLength");
		const numPlayersOnCourt = g.get("numPlayersOnCourt");

		const editable =
			g.get("userTids").includes(tid) && !g.get("spectator") && enabled;

		const t = await idb.cache.teams.get(tid);
		if (!t) {
			throw new Error("Invalid tid");
		}

		const playersRaw = await idb.cache.players.indexGetAll("playersByTid", tid);
		const players = addFirstNameShort(
			await idb.getCopies.playersPlus(playersRaw, {
				attrs: [
					"pid",
					"firstName",
					"lastName",
					"injury",
					"ptModifier",
					"rosterOrder",
					"watch",
				],
				ratings: ["pos", "ovr", "skills"],
				stats: ["min", "gp", "jerseyNumber"],
				season: g.get("season"),
				showNoStats: true,
				showRookies: true,
				fuzz: true,
			}),
		);
		players.sort((a, b) => a.rosterOrder - b.rosterOrder);

		// The coach's own ranking, which is what the sim substitutes by. Taken
		// from the raw rows because the display copies are fuzzed.
		const valueByPid = new Map(playersRaw.map((p) => [p.pid, p.valueNoPot]));
		const candidates: RotationCandidate[] = players.map((p) => ({
			pid: p.pid,
			value: valueByPid.get(p.pid) ?? 0,
			ptModifier: p.ptModifier,
			injured: p.injury.gamesRemaining > 0,
		}));

		const generated = generateRotation(candidates, {
			numPeriods,
			periodLength,
			numPlayersOnCourt,
		});

		const stored = sanitizeRotation(
			t.rotation,
			new Set(players.map((p) => p.pid)),
			numPeriods,
		);
		const auto = stored?.auto ?? true;

		return {
			abbrev,
			auto,
			editable,
			enabled,
			generated,
			numPeriods,
			numPlayersOnCourt,
			periodLength,
			players,
			// A team on auto shows what the coach would do; a team in control
			// shows its own plan, seeded from the coach's when it has none yet.
			stints:
				!auto && stored && stored.stints.length > 0 ? stored.stints : generated,
			tid,
		};
	}
};

export default updateRotation;
