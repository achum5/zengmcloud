import { useCallback, useState } from "react";
import { helpers } from "./helpers.ts";
import type { DataTableRow } from "../components/DataTable/index.tsx";
import type { FooterRow } from "../components/DataTable/Footer.tsx";

// Selection state for a game-log table: which game rows are currently
// highlighted. Reused by every game-log table that wants a highlighted-averages
// summary (each table keeps its own selection).
export const useGameLogSelection = () => {
	const [selectedKeys, setSelectedKeys] = useState<Set<DataTableRow["key"]>>(
		() => new Set(),
	);

	const onToggle = useCallback((key: DataTableRow["key"]) => {
		setSelectedKeys((prev) => {
			const next = new Set(prev);
			if (next.has(key)) {
				next.delete(key);
			} else {
				next.add(key);
			}
			return next;
		});
	}, []);

	const clear = useCallback(() => {
		setSelectedKeys(new Set());
	}, []);

	return { selectedKeys, onToggle, clear };
};

type GameWithStats = { stats: Record<string, number | undefined> };

const sumStat = (games: GameWithStats[], key: string) =>
	games.reduce((acc, g) => acc + (g.stats[key] ?? 0), 0);

// The correct per-game average of a single stat across the selected games.
// Counting stats are a straight mean; rate stats (shooting percentages) are
// recomputed from the summed makes/attempts, since averaging already-computed
// percentages is wrong (a 1/1 game and a 2/10 game don't average to a real FG%).
const averageGameStat = (
	games: GameWithStats[],
	stat: string,
): number | undefined => {
	const n = games.length;
	if (n === 0) {
		return undefined;
	}

	switch (stat) {
		case "fgp": {
			const d = sumStat(games, "fga");
			return d > 0 ? (100 * sumStat(games, "fg")) / d : 0;
		}
		case "tpp": {
			const d = sumStat(games, "tpa");
			return d > 0 ? (100 * sumStat(games, "tp")) / d : 0;
		}
		case "ftp": {
			const d = sumStat(games, "fta");
			return d > 0 ? (100 * sumStat(games, "ft")) / d : 0;
		}
		case "tsp": {
			const d = 2 * (sumStat(games, "fga") + 0.44 * sumStat(games, "fta"));
			return d > 0 ? (100 * sumStat(games, "pts")) / d : 0;
		}
		case "efg": {
			const d = sumStat(games, "fga");
			return d > 0
				? (100 * (sumStat(games, "fg") + 0.5 * sumStat(games, "tp"))) / d
				: 0;
		}
		default: {
			// If the stat is absent from every selected game, leave the cell blank
			// rather than inventing a 0.
			if (games.every((g) => g.stats[stat] === undefined)) {
				return undefined;
			}
			return sumStat(games, stat) / n;
		}
	}
};

// A "table-primary" summary row for the TOP of a game-log table showing the
// per-game averages of the highlighted games. Game-log tables all share the
// leading column layout [#, Team, @, Opp, Result, ...], so the label lives in
// the Result column (index 4) where there's room; the stat columns line up
// because the row's data is indexed by original column position, exactly like
// the per-game rows.
export const gameLogAveragesRow = (
	selectedGames: GameWithStats[],
	stats: string[],
	numCols: number,
	onClear: () => void,
): FooterRow => {
	const prefixCount = numCols - stats.length;
	const data: FooterRow["data"] = new Array(numCols).fill(null);

	data[4] = {
		value: (
			<div className="d-flex align-items-center gap-1">
				<button
					type="button"
					className="btn-close btn-close-sm"
					style={{ fontSize: "0.6rem" }}
					title="Clear selection"
					onClick={onClear}
				/>
				<span>
					Averages ({selectedGames.length} game
					{selectedGames.length === 1 ? "" : "s"})
				</span>
			</div>
		),
	};

	for (const [si, stat] of stats.entries()) {
		const avg = averageGameStat(selectedGames, stat);
		data[prefixCount + si] =
			avg === undefined ? null : helpers.roundStat(avg, stat, false);
	}

	return { classNames: "table-primary", data };
};
