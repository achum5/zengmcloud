import { assert, describe, test } from "vitest";
import {
	applyFaceChanges,
	appearanceForSeason,
	appearancesDiffer,
	faceChanges,
	latestAppearanceSeason,
	lookStepped,
	recordSeasonLook,
	revertAppearance,
	steppedFields,
	type PlayerAppearance,
} from "./playerAppearance.ts";

// A face with the parts these rules care about: a hair style (steps), a beard
// (steps) and two gradual dials.
const face = (
	hair: string,
	{
		beard = "none",
		smile = 1,
		fatness = 0.2,
	}: { beard?: string; smile?: number; fatness?: number } = {},
) =>
	({
		hair: { id: hair, color: "#272421" },
		facialHair: { id: beard },
		smileLine: { id: "line1", size: smile },
		fatness,
	}) as any;

const hairIn = (p: any, season: number, latest?: number) =>
	appearanceForSeason(p, season, latest).face!.hair.id;

describe("appearanceForSeason", () => {
	const p = {
		face: face("bald"),
		appearances: [
			{ season: 2010, face: face("afro") },
			{ season: 2016, face: face("short") },
		] as PlayerAppearance[],
	};

	test("an earlier season resolves to the history", () => {
		assert.strictEqual(hairIn(p, 2010, 2020), "afro");
		assert.strictEqual(hairIn(p, 2015, 2020), "afro");
		assert.strictEqual(hairIn(p, 2016, 2020), "short");
		assert.strictEqual(hairIn(p, 2019, 2020), "short");
	});

	test("the latest season is p.face - one answer, not two", () => {
		// The bug this replaces: the newest entry and p.face both claimed the
		// current season, and the gallery showed one while the page showed the
		// other.
		assert.strictEqual(hairIn(p, 2020, 2020), "bald");
		assert.strictEqual(hairIn(p, 2030, 2020), "bald");
	});

	test("a season before the record falls back to the earliest look", () => {
		assert.strictEqual(hairIn(p, 2004, 2020), "afro");
	});

	test("no history means the player always looked like he does now", () => {
		const plain = { face: face("curly"), imgURL: "" };
		assert.strictEqual(hairIn(plain, 2011, 2020), "curly");
		assert.strictEqual(
			appearanceForSeason(plain, undefined).face!.hair.id,
			"curly",
		);
	});

	test("a photo is carried per season just like a face", () => {
		const withPhoto = {
			imgURL: "now.png",
			appearances: [{ season: 2010, imgURL: "then.png" }],
		};
		assert.strictEqual(
			appearanceForSeason(withPhoto, 2012, 2020).imgURL,
			"then.png",
		);
		assert.strictEqual(
			appearanceForSeason(withPhoto, 2020, 2020).imgURL,
			"now.png",
		);
	});

	test("a season that changed only a little layers onto the one before", () => {
		const layered = {
			face: face("short", { smile: 1.3 }),
			appearances: [
				{ season: 2010, face: face("afro", { smile: 1 }) },
				{ season: 2011, changes: { smileLine: { size: 1.1 } } },
				{ season: 2012, changes: { hair: { id: "short" } } },
			] as PlayerAppearance[],
		};
		const at2011 = appearanceForSeason(layered, 2011, 2020).face!;
		assert.strictEqual(at2011.hair.id, "afro");
		assert.strictEqual(at2011.smileLine.size, 1.1);
		// Untouched fields come through from the whole face.
		assert.strictEqual(at2011.smileLine.id, "line1");
		const at2014 = appearanceForSeason(layered, 2014, 2020).face!;
		assert.strictEqual(at2014.hair.id, "short");
		assert.strictEqual(at2014.hair.color, "#272421");
		assert.strictEqual(at2014.smileLine.size, 1.1);
	});
});

describe("latestAppearanceSeason", () => {
	test("is the newest ratings season", () => {
		assert.strictEqual(
			latestAppearanceSeason([
				{ season: 2012 },
				{ season: 2019 },
				{ season: 2015 },
			]),
			2019,
		);
		assert.isUndefined(latestAppearanceSeason([]));
	});
});

describe("faceChanges", () => {
	test("only what changed, one level deep, and it applies back", () => {
		const a = face("afro", { smile: 1 });
		const b = face("afro", { smile: 1.2, beard: "goatee1" });
		const changes = faceChanges(a, b)!;
		assert.deepStrictEqual(changes, {
			facialHair: { id: "goatee1" },
			smileLine: { size: 1.2 },
		});
		assert.deepStrictEqual(applyFaceChanges(a, changes), b);
		assert.isUndefined(faceChanges(a, structuredClone(a)));
	});

	test("applying does not touch the face it started from", () => {
		const a = face("afro");
		applyFaceChanges(a, { hair: { id: "bald" } });
		assert.strictEqual(a.hair.id, "afro");
	});
});

describe("steps and gradual aging", () => {
	test("the slow dials alone are not a step", () => {
		const a = face("afro", { smile: 1, fatness: 0.2 });
		const b = face("afro", { smile: 1.1, fatness: 0.21 });
		assert.deepStrictEqual(steppedFields(a, b), []);
		assert.isFalse(lookStepped({ face: a }, { face: b }));
	});

	test("a beard or a haircut is", () => {
		const a = face("afro");
		assert.deepStrictEqual(
			steppedFields(a, face("afro", { beard: "goatee1" })),
			["facialHair.id"],
		);
		assert.isTrue(lookStepped({ face: a }, { face: face("short") }));
		assert.isTrue(lookStepped({ face: a }, { face: a, imgURL: "x.png" }));
	});
});

describe("recordSeasonLook", () => {
	test("an empty history starts with a whole face, back to when he arrived", () => {
		const history = recordSeasonLook({
			appearances: undefined,
			season: 2015,
			firstSeason: 2010,
			look: { face: face("afro") },
		})!;
		assert.deepStrictEqual(
			history.map((e) => [e.season, e.face?.hair.id]),
			[[2010, "afro"]],
		);
	});

	test("after that, a season keeps only what changed", () => {
		// Every season moves a little now, so every season has an entry - and
		// it has to be small.
		let history = recordSeasonLook({
			appearances: undefined,
			season: 2010,
			look: { face: face("afro", { smile: 1 }) },
		});
		history = recordSeasonLook({
			appearances: history,
			season: 2011,
			look: { face: face("afro", { smile: 1.08 }) },
		});
		const entry = history!.at(-1)!;
		assert.strictEqual(entry.season, 2011);
		assert.isUndefined(entry.face);
		assert.deepStrictEqual(entry.changes, { smileLine: { size: 1.08 } });
		assert.isBelow(JSON.stringify(entry).length, 80);
	});

	test("a season that looked like the one before writes nothing", () => {
		const history: PlayerAppearance[] = [{ season: 2010, face: face("afro") }];
		assert.isUndefined(
			recordSeasonLook({
				appearances: history,
				season: 2013,
				look: { face: face("afro") },
			}),
		);
	});

	test("re-recording a season replaces it rather than stacking", () => {
		const history = recordSeasonLook({
			appearances: [
				{ season: 2010, face: face("afro") },
				{ season: 2015, changes: { hair: { id: "bald" } } },
			],
			season: 2015,
			look: { face: face("dreads") },
		})!;
		assert.strictEqual(history.length, 2);
		assert.strictEqual(hairIn({ appearances: history }, 2015), "dreads");
	});

	test("an edit to an old season carries forward until something sets it again", () => {
		const history = recordSeasonLook({
			appearances: [
				{ season: 2010, face: face("afro") },
				{ season: 2011, changes: { smileLine: { size: 1.1 } } },
				{ season: 2013, changes: { hair: { id: "short" } } },
			],
			season: 2010,
			look: { face: face("dreads") },
		})!;
		const p = { face: face("short"), appearances: history };
		assert.strictEqual(hairIn(p, 2010, 2020), "dreads");
		assert.strictEqual(hairIn(p, 2012, 2020), "dreads");
		assert.strictEqual(hairIn(p, 2013, 2020), "short");
	});
});

// PUTTING ONE SEASON'S STEP BACK - the gallery's undo. The report behind it is
// a 23-year-old who grew a mustache and a pair of flared chops.
describe("revertAppearance", () => {
	// Rookie in 2010; a goatee comes in 2013 and fills to a beard in 2016; the
	// smile lines deepen a little every season throughout.
	const career = (): PlayerAppearance[] => [
		{ season: 2010, face: face("afro", { smile: 1 }) },
		{ season: 2011, changes: { smileLine: { size: 1.1 } } },
		{ season: 2012, changes: { smileLine: { size: 1.2 } } },
		{
			season: 2013,
			changes: { facialHair: { id: "goatee1" }, smileLine: { size: 1.3 } },
		},
		{ season: 2014, changes: { smileLine: { size: 1.4 } } },
		{ season: 2015, changes: { smileLine: { size: 1.5 } } },
		{
			season: 2016,
			changes: { facialHair: { id: "beard1" }, smileLine: { size: 1.6 } },
		},
	];
	const live = () => ({ face: face("afro", { beard: "beard1", smile: 1.8 }) });

	test("the step goes, until the season something set it again", () => {
		const out = revertAppearance({
			appearances: career(),
			season: 2013,
			current: live(),
			latestSeason: 2018,
		})!;
		const p = { ...out.current, appearances: out.appearances };
		const beard = (season: number) =>
			appearanceForSeason(p, season, 2018).face!.facialHair.id;
		assert.strictEqual(beard(2013), "none");
		assert.strictEqual(beard(2015), "none");
		assert.strictEqual(beard(2016), "beard1");
		// The slow aging that season is kept.
		assert.strictEqual(
			appearanceForSeason(p, 2013, 2018).face!.smileLine.size,
			1.3,
		);
		// The beard that came later is still live, so the live face is left.
		assert.strictEqual(out.current.face!.facialHair.id, "beard1");
	});

	test("undoing a step that is still on the live face moves the live face", () => {
		const out = revertAppearance({
			appearances: career(),
			season: 2016,
			current: live(),
			latestSeason: 2018,
		})!;
		assert.strictEqual(out.current.face!.facialHair.id, "goatee1");
		// And only the step: his lines stay as deep as they are.
		assert.strictEqual(out.current.face!.smileLine.size, 1.8);
	});

	test("undoing this season's step changes the live face and nothing else", () => {
		const history = career();
		const out = revertAppearance({
			appearances: history,
			season: 2018,
			current: { face: face("short", { beard: "beard1", smile: 1.8 }) },
			latestSeason: 2018,
		})!;
		assert.strictEqual(out.current.face!.hair.id, "afro");
		assert.deepStrictEqual(out.appearances, history);
	});

	test("a season of gradual aging alone has nothing to undo", () => {
		assert.isUndefined(
			revertAppearance({
				appearances: career(),
				season: 2014,
				current: live(),
				latestSeason: 2018,
			}),
		);
	});

	test("the first season has nothing behind it", () => {
		assert.isUndefined(
			revertAppearance({
				appearances: career(),
				season: 2010,
				current: live(),
				latestSeason: 2018,
			}),
		);
	});

	test("a player who never changed has no history to undo", () => {
		assert.isUndefined(
			revertAppearance({
				appearances: undefined,
				season: 2013,
				current: live(),
				latestSeason: 2018,
			}),
		);
	});

	test("it is idempotent - reverting the same season twice is a no-op", () => {
		const once = revertAppearance({
			appearances: career(),
			season: 2013,
			current: live(),
			latestSeason: 2018,
		})!;
		assert.isUndefined(
			revertAppearance({
				appearances: once.appearances,
				season: 2013,
				current: once.current,
				latestSeason: 2018,
			}),
		);
	});

	test("older saves' whole-face entries still revert", () => {
		const out = revertAppearance({
			appearances: [
				{ season: 2010, face: face("rookie") },
				{ season: 2013, face: face("chops") },
				{ season: 2016, face: face("beard") },
			],
			season: 2013,
			current: { face: face("beard") },
			latestSeason: 2020,
		})!;
		const p = { ...out.current, appearances: out.appearances };
		assert.strictEqual(hairIn(p, 2013, 2020), "rookie");
		assert.strictEqual(hairIn(p, 2015, 2020), "rookie");
		assert.strictEqual(hairIn(p, 2016, 2020), "beard");
	});
});

describe("appearancesDiffer", () => {
	test("by value, either half", () => {
		assert.isFalse(
			appearancesDiffer({ face: face("afro") }, { face: face("afro") }),
		);
		assert.isTrue(
			appearancesDiffer({ face: face("afro") }, { face: face("bald") }),
		);
		assert.isTrue(appearancesDiffer({ imgURL: "a" }, { imgURL: "b" }));
	});
});
