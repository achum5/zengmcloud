import { assert, describe, test } from "vitest";
import { getCols } from "./getCols.ts";
import goatFormula from "../worker/util/goatFormula.ts";

// EVERY STAT THE GOAT LAB OFFERS HAS A COLUMN.
//
// Most pages name the columns they want, but the GOAT lab hands out every stat
// the sport has as a formula variable and asks getCols to label them - and
// getCols throws on a name it does not know. So a stat added for one corner of
// one page does not degrade the lab, it takes the whole page down with
// "Unknown column". That is what "stat:orapmPct" did.
//
// A stat with nothing to say to a table stays out of the list instead, the way
// minAvailable does: see BANNED_STAT_VARIABLES.
describe("getCols knows every GOAT stat", () => {
	test("no GOAT variable is missing a column", () => {
		const missing = goatFormula.STAT_VARIABLES.filter((stat) => {
			try {
				getCols([`stat:${stat}`]);
				return false;
			} catch {
				return true;
			}
		});
		assert.deepStrictEqual(missing, [], "stats with no column");
	});
});
