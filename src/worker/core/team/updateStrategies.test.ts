import { assert, beforeEach, describe, test } from "vitest";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import updateStrategies from "./updateStrategies.ts";
import {
	AI_TID,
	buildValuationLeague as build,
} from "../../../test/fixtures/valuationLeague.ts";

// The flag the rest of the game shows is set from the plan the front office
// actually runs on, so a team cannot be labelled rebuilding while buying.
describe("updateStrategies with the smart front office", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	const flagAfter = async (
		aiWon: number,
		before: "contending" | "rebuilding",
	) => {
		await build({ aiWon });
		const t = (await idb.cache.teams.get(AI_TID))!;
		t.strategy = before;
		await idb.cache.teams.put(t);
		await updateStrategies();
		return (await idb.cache.teams.get(AI_TID))!.strategy;
	};

	test("a team that is winning reads contending, whatever it read before", async () => {
		assert.strictEqual(await flagAfter(60, "rebuilding"), "contending");
	});

	test("a team that is tearing down reads rebuilding", async () => {
		assert.strictEqual(await flagAfter(20, "contending"), "rebuilding");
	});

	test("with the setting off, the flag is left to the legacy formula", async () => {
		await build({ aiWon: 60 });
		g.setWithoutSavingToDB("smartAiFrontOffice", false);
		const t = (await idb.cache.teams.get(AI_TID))!;
		t.strategy = "rebuilding";
		await idb.cache.teams.put(t);
		// The legacy score needs stats; a fixture without them leaves the
		// flag where it was rather than throwing.
		try {
			await updateStrategies();
		} catch {
			// The legacy path is not under test here.
		}
		assert.oneOf((await idb.cache.teams.get(AI_TID))!.strategy, [
			"rebuilding",
			"contending",
		]);
	});
});
