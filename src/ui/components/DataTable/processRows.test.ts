import { assert, describe, test } from "vitest";
import { processRows } from "./processRows.ts";
import type { Col, DataTableRow, SortBy } from "./index.tsx";
import type { State } from "./loadStateFromCache.ts";

// In "hide ratings ones digit" mode every player in a decade shows the same
// number, so sorting by Ovr is a ten-way tie. A stable sort breaks that tie by
// the order the rows arrived in - and the worker reads players out of the
// database in pid order, which within a draft class is best-first. Sorting by
// Ovr therefore used to rank the 6s from 69 down to 60, which is the exact
// digit the mode exists to hide.

const cols: Col[] = [
	{ key: "Name", title: "Name" },
	{ key: "Ovr", title: "Ovr", sortType: "number" },
	{ key: "Age", title: "Age", sortType: "number" },
];

// Everyone shows a 6, listed in the order the worker would hand them over:
// descending true ovr, which is what must not survive to the screen.
const rows: DataTableRow[] = [1, 2, 3, 4, 5, 6, 7, 8].map((pid) => ({
	key: pid,
	data: [`Player ${pid}`, 6, 25],
}));

const state = (sortBys: SortBy[]): State =>
	({
		colOrder: cols.map((_, colIndex) => ({ colIndex })),
		currentPage: 1,
		enableFilters: false,
		filters: [],
		hideAllControls: false,
		perPage: 100,
		prevName: "test",
		searchText: "",
		showSelectColumnsModal: false,
		sortBys,
		stickyCols: 0,
	}) as unknown as State;

const order = (coarseRatings: boolean, sortBys: SortBy[]) =>
	processRows({
		coarseRatings,
		cols,
		rows,
		state: state(sortBys),
	}).map((row) => row.key);

const BY_OVR: SortBy[] = [[1, "desc"]];
const BY_AGE: SortBy[] = [[2, "desc"]];

describe("coarsened rating ties", () => {
	test("without the mode, ties keep the order they came in", () => {
		assert.deepEqual(order(false, BY_OVR), [1, 2, 3, 4, 5, 6, 7, 8]);
	});

	test("with the mode, sorting by a rating scrambles the tie", () => {
		const scrambled = order(true, BY_OVR);
		assert.notDeepEqual(scrambled, [1, 2, 3, 4, 5, 6, 7, 8]);
		// Everyone is still there, exactly once.
		assert.deepEqual([...scrambled].sort(), [1, 2, 3, 4, 5, 6, 7, 8]);
	});

	test("the scramble is stable, so the table doesn't jitter", () => {
		assert.deepEqual(order(true, BY_OVR), order(true, BY_OVR));
	});

	// Flipping the arrow reverses the ratings, not the players inside a decade -
	// otherwise the two directions read against each other and give the digit
	// back.
	test("the within-decade order is the same ascending and descending", () => {
		assert.deepEqual(order(true, BY_OVR), order(true, [[1, "asc"]]));
	});

	// Sorting by something that isn't a rating leaks nothing, and the incoming
	// order there is usually meaningful (roster order, draft order).
	test("a non-rating column is left alone even in the mode", () => {
		assert.deepEqual(order(true, BY_AGE), [1, 2, 3, 4, 5, 6, 7, 8]);
	});

	test("the real ratings still sort ahead of the tiebreak", () => {
		const mixed: DataTableRow[] = [
			{ key: 1, data: ["a", 5, 25] },
			{ key: 2, data: ["b", 7, 25] },
			{ key: 3, data: ["c", 6, 25] },
		];
		const sorted = processRows({
			coarseRatings: true,
			cols,
			rows: mixed,
			state: state(BY_OVR),
		}).map((row) => row.data[1]);
		assert.deepEqual(sorted, [7, 6, 5]);
	});
});
