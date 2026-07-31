import { assert, describe, test } from "vitest";
import { processRows } from "./processRows.ts";
import type { Col, DataTableRow, SortBy } from "./index.tsx";
import type { State } from "./loadStateFromCache.ts";

// In "hide ratings ones digit" mode every player in a decade shows the same
// number, and every one of them should tie. But a stable sort breaks that tie
// by the order the rows arrived in - and the worker reads players out of the
// database in pid order, which within a draft class is best-first. Sorting by
// Ovr therefore used to rank the 6s from 69 down to 60, which is the exact
// digit the mode exists to hide. The tiebreak is the player's name.

const cols: Col[] = [
	{ key: "Name", title: "Name" },
	{ key: "Ovr", title: "Ovr", sortType: "number" },
	{ key: "Age", title: "Age", sortType: "number" },
];

// Everyone shows a 6, listed the way the worker would hand them over: best
// true rating first, which is what must not survive to the screen.
const NAMES_BY_HIDDEN_RATING = [
	"Young Zeb",
	"Adams Mike",
	"Turner Cole",
	"Baker Ray",
	"Nolan Pete",
	"Cole Ivan",
];

const rows: DataTableRow[] = NAMES_BY_HIDDEN_RATING.map((name, i) => ({
	key: i + 1,
	data: [name, 6, 25],
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

const names = (coarseRatings: boolean, sortBys: SortBy[], rowsIn = rows) =>
	processRows({
		coarseRatings,
		cols,
		rows: rowsIn,
		state: state(sortBys),
	}).map((row) => row.data[0]);

const BY_OVR: SortBy[] = [[1, "desc"]];
const BY_AGE: SortBy[] = [[2, "desc"]];

const ALPHABETICAL = [...NAMES_BY_HIDDEN_RATING].sort();

describe("coarsened rating ties", () => {
	test("without the mode, ties keep the order they came in", () => {
		assert.deepEqual(names(false, BY_OVR), NAMES_BY_HIDDEN_RATING);
	});

	test("with the mode, everyone on the same number sorts by name", () => {
		assert.deepEqual(names(true, BY_OVR), ALPHABETICAL);
	});

	// Flipping the arrow reverses the ratings, not the players inside a decade -
	// two directions read against each other would give the digit back.
	test("names stay A-Z whichever way the ratings point", () => {
		assert.deepEqual(names(true, [[1, "asc"]]), ALPHABETICAL);
	});

	// Sorting by something that isn't a rating leaks nothing, and the incoming
	// order there is usually meaningful (roster order, draft order).
	test("a non-rating column is left alone even in the mode", () => {
		assert.deepEqual(names(true, BY_AGE), NAMES_BY_HIDDEN_RATING);
	});

	test("the ratings still sort ahead of the tiebreak", () => {
		const mixed: DataTableRow[] = [
			{ key: 1, data: ["Zeb", 5, 25] },
			{ key: 2, data: ["Yves", 7, 25] },
			{ key: 3, data: ["Xan", 6, 25] },
			{ key: 4, data: ["Abe", 5, 25] },
		];
		assert.deepEqual(names(true, BY_OVR, mixed), ["Yves", "Xan", "Abe", "Zeb"]);
	});

	// Nothing to alphabetize by, but the incoming order still can't be allowed
	// to stand - it's the leak.
	test("a table with no Name column scatters the tie instead", () => {
		const nameless: Col[] = [{ key: "Ovr", title: "Ovr", sortType: "number" }];
		const namelessRows: DataTableRow[] = [1, 2, 3, 4, 5, 6, 7, 8].map(
			(pid) => ({
				key: pid,
				data: [6],
			}),
		);
		const keys = processRows({
			coarseRatings: true,
			cols: nameless,
			rows: namelessRows,
			state: {
				...state([[0, "desc"]]),
				colOrder: [{ colIndex: 0 }],
			},
		}).map((row) => row.key);
		assert.notDeepEqual(keys, [1, 2, 3, 4, 5, 6, 7, 8]);
		assert.deepEqual([...keys].sort(), [1, 2, 3, 4, 5, 6, 7, 8]);
	});
});

// The prospects exemption spares undrafted players from the coarsening, so
// Player Ratings shows them at full resolution next to everyone else's tens
// digit. Compared as plain numbers, a prospect's 78 beats every real 8 and the
// entire draft class sat above the league.
describe("rows exempt from the coarsening", () => {
	// Deliberately interleaved on the way in, so nothing here can pass on the
	// incoming order alone.
	const mixed: DataTableRow[] = [
		{ key: 1, data: ["Prospect 78", 78, 20], coarseExempt: true },
		{ key: 2, data: ["Real 8", 8, 28] },
		{ key: 3, data: ["Prospect 71", 71, 19], coarseExempt: true },
		{ key: 4, data: ["Real 7", 7, 27] },
		{ key: 5, data: ["Prospect 85", 85, 20], coarseExempt: true },
		{ key: 6, data: ["Real 8 also", 8, 26] },
	];

	test("a prospect lands under the players showing his tens digit", () => {
		assert.deepEqual(names(true, BY_OVR, mixed), [
			"Real 8",
			"Real 8 also",
			"Prospect 85",
			"Real 7",
			"Prospect 78",
			"Prospect 71",
		]);
	});

	// Flipping the arrow reverses the decades and reverses the prospects inside
	// one, but a prospect stays BELOW the real players on his number - otherwise
	// he'd jump above them just by clicking the header twice.
	test("still under them with the arrow flipped", () => {
		assert.deepEqual(names(true, [[1, "asc"]], mixed), [
			"Real 7",
			"Prospect 71",
			"Prospect 78",
			"Real 8",
			"Real 8 also",
			"Prospect 85",
		]);
	});

	// Exact ratings are what the exemption is FOR, so ordering prospects by
	// their real number hides nothing.
	test("prospects on the same digit rank by their exact rating", () => {
		const class2005: DataTableRow[] = [
			{ key: 1, data: ["Bravo", 71, 20], coarseExempt: true },
			{ key: 2, data: ["Alpha", 79, 19], coarseExempt: true },
			{ key: 3, data: ["Charlie", 75, 20], coarseExempt: true },
		];
		assert.deepEqual(names(true, BY_OVR, class2005), [
			"Alpha",
			"Charlie",
			"Bravo",
		]);
	});

	// The exemption only exists because the mode is on. With it off nobody is
	// coarsened, so there is nothing to fold back.
	test("without the mode the exact numbers just sort", () => {
		assert.deepEqual(names(false, BY_OVR, mixed), [
			"Prospect 85",
			"Prospect 78",
			"Prospect 71",
			"Real 8",
			"Real 8 also",
			"Real 7",
		]);
	});

	// Age isn't a rating - it was never coarsened, so it sorts on its face value
	// for prospect and veteran alike.
	test("a non-rating column is untouched", () => {
		assert.deepEqual(names(true, BY_AGE, mixed), [
			"Real 8",
			"Real 7",
			"Real 8 also",
			"Prospect 78",
			"Prospect 85",
			"Prospect 71",
		]);
	});
});
