import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../test/helpers.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { team } from "../core/index.ts";
import setGameAttributes from "../core/league/setGameAttributes.ts";
import loadGameAttributes from "../core/league/loadGameAttributes.ts";
import { changeTracker } from "../db/changeTracker.ts";
import updateSettings from "./settings.ts";

// A League Setting is only real if it survives the trip: shown by the view,
// written by a save, and still there when the league is loaded again. This
// pins that whole path for "Pause Sim On Days" (simStopDays), which is a
// string game attribute added after the fact - exactly the kind that gets
// dropped by a missing entry in one of the several lists a setting has to
// appear in.

const stubLeagueDb = () => {
	const store = {
		index: () => store,
		getAll: async () => [],
		get: async () => undefined,
		async *iterate() {},
	};
	(idb as any).league = {
		getAll: async () => [],
		get: async () => undefined,
		transaction: () => ({
			store,
			objectStore: () => store,
			done: Promise.resolve(),
		}),
	};
};

const build = async () => {
	resetG();
	const teams: any[] = [];
	for (let tid = 0; tid < 4; tid++) {
		teams.push(
			team.generate({
				tid,
				cid: tid % 2,
				did: tid % 2,
				region: `R${tid}`,
				name: `N${tid}`,
				abbrev: `T${tid}`,
				pop: 2,
				imgURL: "",
			} as any),
		);
	}
	await resetCache({ teams });
	stubLeagueDb();
};

describe("a league setting survives the round trip", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("Pause Sim On Days is shown, saved, and reloaded", async () => {
		await build();

		// Shown: the view has to actually return the key, or the form renders a
		// blank field with nothing behind it.
		const before: any = await updateSettings(undefined, ["firstRun"] as any);
		assert.ok(
			Object.hasOwn(before.initialSettings, "simStopDays"),
			"the settings view must return simStopDays",
		);
		assert.strictEqual(before.initialSettings.simStopDays, "");

		// Saved: a real gameAttributes ROW, not just an in-memory default.
		await setGameAttributes({ simStopDays: "15, 41, deadline" });
		const row = await idb.cache.gameAttributes.get("simStopDays" as any);
		assert.ok(row, "saving must write a gameAttributes row");
		assert.strictEqual((row as any).value, "15, 41, deadline");

		// Reloaded: what a fresh league load puts back into g.
		delete (g as any).simStopDays;
		await loadGameAttributes();
		assert.strictEqual(g.get("simStopDays"), "15, 41, deadline");

		// And shown again, which is where the user looks.
		const after: any = await updateSettings(undefined, ["firstRun"] as any);
		assert.strictEqual(after.initialSettings.simStopDays, "15, 41, deadline");
	});

	test("clearing it back to blank also persists", async () => {
		await build();
		await setGameAttributes({ simStopDays: "15" });
		await setGameAttributes({ simStopDays: "" });
		delete (g as any).simStopDays;
		await loadGameAttributes();
		assert.strictEqual(g.get("simStopDays"), "");
	});
});
