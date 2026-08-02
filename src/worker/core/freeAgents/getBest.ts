import { g } from "../../util/index.ts";
import type { PlayerWithoutKey } from "../../../common/types.ts";
import { DRAFT_BY_TEAM_OVR } from "../../../common/constants.ts";
import { getTeamOvrDiffs } from "../draft/runPicks.ts";
import { last, orderBy } from "../../../common/utils.ts";
import { bySport } from "../../../common/sportFunctions.ts";

// In some sports, extra check for certain important rare positions in case the only one was traded away. These should only be positions with weird unique skills, where you can't replace them easily with another position. Value is the number of players that should be at each position.
export const KEY_POSITIONS_NEEDED = bySport<Record<string, number> | undefined>(
	{
		baseball: undefined,
		basketball: undefined,
		football: { QB: 2, K: 1, P: 1 },
		hockey: { G: 2 },
	},
);

// Find the best available free agent for a team.
// playersAvailable should be sorted - best players first, worst players last.
// If payroll is not supplied, don't do salary cap check (like when creating new league).
const getBest = <T extends PlayerWithoutKey>(
	playersOnRoster: T[],
	playersAvailable: T[],
	payroll?: number,
	// A secondary hard cap for this team (thousands), or Infinity if not bound.
	hardCap: number = Infinity,
): T | void => {
	const maxRosterSize = g.get("maxRosterSize");
	const minContract = g.get("minContract");
	const salaryCap = g.get("salaryCap");
	const salaryCapType = g.get("salaryCapType");
	const numActiveTeams = g.get("numActiveTeams");

	let playersSorted: T[];
	if (DRAFT_BY_TEAM_OVR) {
		// playersAvailable is sorted by value. So if we hit a player at a minimum contract at a position, no player with lower value needs to be considered
		//
		// That reasoning is only sound on a value-sorted list, and callers do not
		// all provide one - posture-driven free agency orders by fit, which can put
		// a cheap player at a position of need ahead of a much better one and so
		// prune the better one away entirely. Sorting here makes the pruning safe
		// for any caller; it is a no-op when the list already arrived in value
		// order, and the result is re-sorted by team-ovr improvement below either
		// way, so nothing downstream can tell the difference.
		const byValue = orderBy(playersAvailable, "value", "desc");
		const seenMinContractAtPos = new Set();
		const playersAvailableFiltered = byValue.filter((p) => {
			const pos = last(p.ratings).pos;
			if (seenMinContractAtPos.has(pos)) {
				return false;
			}

			if (p.contract.amount <= minContract && p.injury.gamesRemaining === 0) {
				seenMinContractAtPos.add(pos);
			}

			return true;
		});

		const teamOvrDiffs = getTeamOvrDiffs(
			playersOnRoster,
			playersAvailableFiltered,
		);
		const wrapper = playersAvailableFiltered.map((p, i) => ({
			p,
			teamOvrDiff: teamOvrDiffs[i]!,
		}));
		playersSorted = orderBy(wrapper, (x) => x.teamOvrDiff, "desc").map(
			(x) => x.p,
		);
	} else {
		playersSorted = playersAvailable;
	}

	const skipSalaryCapCheck =
		salaryCapType === "none" && Math.random() < 2 / numActiveTeams;

	let keyPositionsNeededCache: Set<string> | undefined;
	const getKeyPositionsNeeded = () => {
		if (KEY_POSITIONS_NEEDED) {
			if (keyPositionsNeededCache) {
				return keyPositionsNeededCache;
			}

			const allKeyPositionsNeeded = Object.keys(KEY_POSITIONS_NEEDED);
			const positionCounts: Record<
				"injured" | "healthy",
				Record<string, number>
			> = {
				injured: {},
				healthy: {},
			};

			for (const p of playersOnRoster) {
				const pos = last(p.ratings).pos;
				const injured = p.injury.gamesRemaining > 0;
				const object = positionCounts[injured ? "injured" : "healthy"];
				object[pos] ??= 0;
				object[pos] += 1;
			}

			keyPositionsNeededCache = new Set(
				allKeyPositionsNeeded.filter((pos) => {
					const injured = positionCounts.injured[pos] ?? 0;
					const healthy = positionCounts.healthy[pos] ?? 0;

					// If we already have 4 injured ones, maybe don't sign another? idk
					if (injured >= 4) {
						return false;
					}

					return (
						healthy === 0 || healthy + injured < KEY_POSITIONS_NEEDED[pos]!
					);
				}),
			);

			return keyPositionsNeededCache;
		}
	};

	for (const p of playersSorted) {
		const salaryCapCheck =
			payroll === undefined ||
			skipSalaryCapCheck ||
			p.contract.amount + payroll <= salaryCap;

		// Secondary hard cap: a bound team may only cross it with a minimum
		// contract, and only until its roster is full (the trade rule prevents
		// those minimum guys being stockpiled to take on salary over the cap).
		const wouldExceedHardCap =
			payroll !== undefined && p.contract.amount + payroll > hardCap;
		const hardCapOk =
			!wouldExceedHardCap ||
			(p.contract.amount <= minContract &&
				playersOnRoster.length < maxRosterSize);

		// Don't sign minimum contract players to fill out the roster
		const shouldAddPlayerNormal =
			salaryCapCheck && p.contract.amount > minContract;
		const shouldAddPlayerMinContract =
			p.contract.amount <= minContract &&
			playersOnRoster.length < maxRosterSize - 2;

		// If none of the other checks were true and we can afford this player and it's at a position we have nobody at (like hockey goalie), go for it
		const shouldAddPlayerPosition =
			p.injury.gamesRemaining === 0 &&
			!shouldAddPlayerNormal &&
			!shouldAddPlayerMinContract &&
			(salaryCapCheck || p.contract.amount <= minContract) &&
			getKeyPositionsNeeded()?.has(last(p.ratings).pos);

		if (
			(shouldAddPlayerNormal ||
				shouldAddPlayerPosition ||
				shouldAddPlayerMinContract) &&
			hardCapOk
		) {
			return p;
		}
	}
};

export default getBest;
