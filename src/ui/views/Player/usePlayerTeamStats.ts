import { useEffect, useRef, useState } from "react";
import { toWorker } from "../../util/toWorker.ts";

// Per-team career totals for the current player, fetched once and shared by
// every stat table on the page (they all render the same team subtotal rows).
// Undefined while loading / for a single-team career (then no team rows show).
export type PlayerTeamStats = {
	tid: number;
	careerStats: any;
	careerStatsPlayoffs: any;
	careerStatsCombined: any;
}[];

export const usePlayerTeamStats = (
	pid: number,
	// p.stats.length - refetch when a sim adds a new season row.
	numStatsRows: number,
): PlayerTeamStats | undefined => {
	const [teamStats, setTeamStats] = useState<PlayerTeamStats | undefined>();
	const loadCount = useRef(0);

	useEffect(() => {
		loadCount.current += 1;
		const myCount = loadCount.current;
		let cancelled = false;

		setTeamStats(undefined);
		(async () => {
			try {
				const result = await toWorker("main", "getPlayerTeamStats", { pid });
				if (!cancelled && loadCount.current === myCount) {
					setTeamStats((result as PlayerTeamStats | undefined) ?? []);
				}
			} catch (error) {
				console.error("Failed to load per-team stats", error);
				if (!cancelled && loadCount.current === myCount) {
					setTeamStats([]);
				}
			}
		})();

		return () => {
			cancelled = true;
		};
	}, [pid, numStatsRows]);

	return teamStats;
};
