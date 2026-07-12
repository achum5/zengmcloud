import { useEffect, useRef, useState } from "react";
import { toWorker } from "../../util/toWorker.ts";

type SelectedStats = {
	careerStats: any;
	careerStatsCombined: any;
	careerStatsPlayoffs: any;
};

// Backs the "select rows to subtotal them" feature on a player's stat tables.
// The user checks season rows; this holds the selected set and fetches their
// aggregated totals from the worker (correct rate stats, any non-contiguous
// selection). Selection is per stat table and resets when the player changes.
export const useSelectedSeasonsFooter = (pid: number) => {
	const [selected, setSelected] = useState<Set<number>>(() => new Set());
	const [data, setData] = useState<SelectedStats | undefined>();
	const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");
	const loadCount = useRef(0);

	// Reset when switching players.
	const prevPid = useRef(pid);
	if (prevPid.current !== pid) {
		prevPid.current = pid;
		if (selected.size > 0) {
			setSelected(new Set());
		}
		if (data !== undefined) {
			setData(undefined);
		}
		if (status !== "idle") {
			setStatus("idle");
		}
	}

	const selectedKey = [...selected].sort((a, b) => a - b).join(",");

	useEffect(() => {
		if (selected.size === 0) {
			setData(undefined);
			setStatus("idle");
			return;
		}

		loadCount.current += 1;
		const myCount = loadCount.current;
		let cancelled = false;

		// Small debounce so rapid checkbox toggles coalesce into one fetch.
		const timeout = setTimeout(async () => {
			setStatus("loading");
			try {
				const p = (await toWorker("main", "getPlayerSelectedStats", {
					pid,
					seasons: [...selected],
				})) as SelectedStats | undefined;
				if (cancelled || loadCount.current !== myCount) {
					return;
				}
				if (p) {
					setData(p);
					setStatus("idle");
				} else {
					setStatus("error");
				}
			} catch (error) {
				console.error("Failed to load selected-seasons stats", error);
				if (!cancelled && loadCount.current === myCount) {
					setStatus("error");
				}
			}
		}, 150);

		return () => {
			cancelled = true;
			clearTimeout(timeout);
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [pid, selectedKey]);

	const toggle = (season: number) => {
		setSelected((prev) => {
			const next = new Set(prev);
			if (next.has(season)) {
				next.delete(season);
			} else {
				next.add(season);
			}
			return next;
		});
	};

	const clear = () => setSelected(new Set());

	return { selected, data, status, toggle, clear };
};
