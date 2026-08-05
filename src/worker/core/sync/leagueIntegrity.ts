import { PHASE } from "../../../common/constants.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";

// Catastrophe detection for a whole league: is this database so obviously
// broken that nothing should trust it?
//
// This is NOT legality checking. It doesn't know a sport's minimum roster size
// or care about salary caps. It exists for one scenario, because it happened:
// a league whose rosters had been stripped to two players a team passed every
// guard in the sync system - it simmed, it published, it was restored from -
// because every guard asked "is this device in the right POSITION?" and none
// asked "is this database a LEAGUE?". Broken data spreads exactly as fast as
// good data. This is where that stops.
//
// Deliberately conservative: a rule that fires on any legitimate league state
// would quarantine healthy devices, which is worse than the disease. So the
// floor is five to a side - no basketball game can be played below it, no
// sport's roster legitimately sits there mid-season, and a strip like the one
// that motivated this leaves teams at two or three.

const ROSTER_FLOOR = 5;

// Rosters are only judged in the phases where teams actually hold full
// rosters. Everything from the draft lottery on is offseason churn - rosters
// legitimately thin out through re-signing and free agency - and the two
// draft phases (fantasy, expansion) empty rosters BY DESIGN.
const phaseWithFullRosters = (phase: number): boolean =>
	phase >= PHASE.PRESEASON && phase <= PHASE.PLAYOFFS;

// A gameAttributes row's value, unwrapping the [{ start, value }] shape some
// attributes use.
const gaValue = (rows: any[], key: string): unknown => {
	const row = rows.find((r) => r?.key === key);
	if (!row) {
		return undefined;
	}
	const { value } = row;
	if (
		Array.isArray(value) &&
		value.length > 0 &&
		value[0]?.start !== undefined
	) {
		return value.at(-1).value;
	}
	return value;
};

// The pure form, working on plain arrays, so the SAME rule judges a live
// database and a snapshot payload about to overwrite one.
export const findIntegrityProblems = ({
	players,
	teams,
	phase,
}: {
	players: { tid: number }[];
	teams: { tid: number; disabled?: boolean }[];
	phase: number | undefined;
}): string[] => {
	const problems: string[] = [];

	if (teams.length === 0) {
		problems.push("no teams");
	}
	if (players.length === 0) {
		problems.push("no players");
	}
	if (problems.length > 0 || phase === undefined) {
		return problems;
	}

	if (!phaseWithFullRosters(phase)) {
		return problems;
	}

	const countByTid = new Map<number, number>();
	for (const p of players) {
		if (p.tid >= 0) {
			countByTid.set(p.tid, (countByTid.get(p.tid) ?? 0) + 1);
		}
	}

	const shorted: string[] = [];
	for (const t of teams) {
		if (t.disabled) {
			continue;
		}
		const count = countByTid.get(t.tid) ?? 0;
		if (count < ROSTER_FLOOR) {
			shorted.push(`team ${t.tid} has ${count}`);
		}
	}
	if (shorted.length > 0) {
		problems.push(
			`rosters stripped below ${ROSTER_FLOOR} players in a phase where games are played (${shorted.slice(0, 5).join(", ")}${shorted.length > 5 ? `, and ${shorted.length - 5} more` : ""})`,
		);
	}

	return problems;
};

// Judge a snapshot payload's stores before they are allowed to overwrite a
// local league. gameAttributes rows come as {key, value} pairs.
export const findPayloadIntegrityProblems = (stores: {
	players?: unknown[];
	teams?: unknown[];
	gameAttributes?: unknown[];
}): string[] => {
	const ga = Array.isArray(stores.gameAttributes) ? stores.gameAttributes : [];
	const phase = gaValue(ga, "phase");
	return findIntegrityProblems({
		players: (stores.players ?? []) as { tid: number }[],
		teams: (stores.teams ?? []) as { tid: number; disabled?: boolean }[],
		phase: typeof phase === "number" ? phase : undefined,
	});
};

// Judge THIS device's league. Reads the cache (players held there are exactly
// the non-retired ones, which is what roster counting wants), so it is cheap
// enough to run as a preflight before every sim and every snapshot publish.
export const checkLeagueIntegrity = async (): Promise<string[]> => {
	let phase: number | undefined;
	try {
		phase = g.get("phase");
	} catch {
		return [];
	}
	const [players, teams] = await Promise.all([
		idb.cache.players.getAll(),
		idb.cache.teams.getAll(),
	]);
	// An empty store here is not a stripped league, it is no league: a cache
	// mid-load, a test harness, or a state so broken a sim would fail on its
	// own in the first step. The device-side check exists for the dangerous
	// middle - a league intact enough to sim and broken enough to poison the
	// room - and the payload-side validators still refuse empty snapshots.
	if (players.length === 0 || teams.length === 0) {
		return [];
	}
	return findIntegrityProblems({ players, teams, phase });
};
