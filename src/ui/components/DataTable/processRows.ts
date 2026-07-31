import type { DataTableRow, Props, SortBy } from "./index.tsx";
import { normalizeIntl } from "../../../common/normalizeIntl.ts";
import { orderBy } from "../../../common/utils.ts";
import {
	coarsenRating,
	isCoarsenedRatingCol,
} from "../../../common/coarsenRating.ts";
import createFilterFunction from "./createFilterFunction.ts";
import getSearchVal from "./getSearchVal.tsx";
import getSortVal from "./getSortVal.tsx";
import type { State } from "./loadStateFromCache.ts";

// Last resort for a table that shows ratings but has no Name column to break
// ties with. A fixed pseudo-random number per row, from its key (the pid, for
// a player table) - FNV-1a, which scatters consecutive keys, and consecutive
// keys are exactly the correlation being hidden.
const scrambleKey = (key: DataTableRow["key"]) => {
	const str = String(key);
	let hash = 2166136261;
	for (let i = 0; i < str.length; i++) {
		hash ^= str.charCodeAt(i);
		hash = Math.imul(hash, 16777619);
	}
	return hash >>> 0;
};

export const processRows = ({
	coarseRatings,
	cols,
	rankCol,
	rows,
	state,
}: {
	// The "hide ratings ones digit" mode is on, so every rating column is a
	// ten-way tie and the tiebreak has to come from somewhere that isn't the
	// hidden digit.
	coarseRatings?: boolean;
	state: State;
} & Pick<Props, "cols" | "rankCol" | "rows">) => {
	const filterFunctions = state.enableFilters
		? state.filters.map((filter, i) =>
				createFilterFunction(
					filter,
					cols[i] ? cols[i].sortType : undefined,
					cols[i] ? cols[i].searchType : undefined,
				),
			)
		: [];
	const skipFiltering = state.searchText === "" && !state.enableFilters;
	const searchText = normalizeIntl(state.searchText);
	const rowsFiltered = skipFiltering
		? rows
		: rows.filter((row) => {
				// Search
				if (state.searchText !== "") {
					let found = false;

					for (let i = 0; i < row.data.length; i++) {
						// cols[i] might be undefined if number of columns in a table changed
						if (cols[i]?.noSearch) {
							continue;
						}

						if (
							normalizeIntl(getSearchVal(row.data[i], false)).includes(
								searchText,
							)
						) {
							found = true;
							break;
						}
					}

					if (!found) {
						return false;
					}
				}

				// Filter
				if (state.enableFilters) {
					for (let i = 0; i < row.data.length; i++) {
						// cols[i] might be undefined if number of columns in a table changed
						if (cols[i]?.noSearch) {
							continue;
						}

						if (
							filterFunctions[i] &&
							filterFunctions[i]!(row.data[i]) === false
						) {
							return false;
						}
					}
				}

				return true;
			});

	let rowsOrdered;
	if (state.sortBys === undefined) {
		rowsOrdered = rowsFiltered;
	} else {
		const colIndex = (sortBy: SortBy) => {
			const i = sortBy[0];
			if (typeof i !== "number" || i >= cols.length) {
				return 0;
			}
			return i;
		};

		const valueOf = (row: DataTableRow, i: number) =>
			getSortVal(row.data[i >= row.data.length ? 0 : i], cols[i]!.sortType);

		const sortKeys: ((row: DataTableRow) => any)[] = [];
		const orders: ("asc" | "desc")[] = [];

		for (const sortBy of state.sortBys) {
			const i = colIndex(sortBy);
			const order = sortBy[1];

			// A row exempt from the coarsening (a draft prospect, when the setting
			// spares them) still carries exact ratings, so its 78 sits in the same
			// column as everyone else's 7. Compared straight, 78 beats every real 8
			// and the whole draft class floats to the top of the table.
			//
			// So sort the exempt rows by the decade they'd have shown, drop them
			// below the players actually showing that digit, and only then let their
			// exact number order them against each other - which is the whole point
			// of exempting them. "Below" means later in the list whichever way the
			// column is pointing, so that middle key never flips with the order.
			if (
				coarseRatings &&
				isCoarsenedRatingCol(cols[i]?.key) &&
				rowsFiltered.some((row) => row.coarseExempt)
			) {
				const decade = (row: DataTableRow) => {
					const value = valueOf(row, i);
					if (row.coarseExempt && typeof value === "number") {
						return coarsenRating(value);
					}
					return value;
				};
				sortKeys.push(
					decade,
					(row) => (row.coarseExempt ? 1 : 0),
					(row) => valueOf(row, i),
				);
				orders.push(order, "asc", order);
				continue;
			}

			sortKeys.push((row) => valueOf(row, i));
			orders.push(order);
		}

		// Sorting by a coarsened rating puts everyone in a decade on the same
		// number, and every one of them SHOULD tie - that's the mode working. But
		// a stable sort then breaks those ties by the order the rows arrived in,
		// which is the order the worker read them out of the database, and draft
		// classes are written to it best-first. So "sort by Ovr" quietly ranked
		// the 6s from 69 down to 60: the exact digit the mode exists to hide.
		//
		// Break the tie by name instead. Alphabetical says nothing about a rating,
		// it's the same order clicking the Name header gives, and it's stable, so
		// the table doesn't reshuffle between renders. Always ascending, whichever
		// way the ratings are pointing - if the names flipped too, reading the two
		// directions against each other would hand the digit straight back.
		const tieBreak =
			coarseRatings &&
			state.sortBys.some((sortBy) =>
				isCoarsenedRatingCol(cols[sortBy[0]]?.key),
			);
		const nameIndex = tieBreak
			? cols.findIndex((col) => col.key === "Name")
			: -1;

		const tieBreakKey =
			nameIndex >= 0
				? (row: DataTableRow) =>
						getSortVal(row.data[nameIndex], cols[nameIndex]!.sortType)
				: // No name to sort on, so fall back to scattering the tie. Never
					// leave it to the incoming order, which is the leak.
					(row: DataTableRow) => scrambleKey(row.key);

		rowsOrdered = orderBy(
			rowsFiltered,
			tieBreak ? [...sortKeys, tieBreakKey] : sortKeys,
			tieBreak ? [...orders, "asc"] : orders,
		);
	}

	const colOrderFiltered = state.colOrder.filter(
		({ hidden, colIndex }) => !hidden && cols[colIndex],
	);

	return rowsOrdered.map((row, i) => {
		return {
			...row,
			data: colOrderFiltered.map(({ colIndex }) =>
				colIndex === rankCol ? i + 1 : row.data[colIndex],
			),
		};
	});
};
