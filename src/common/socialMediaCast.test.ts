import { assert, describe, test } from "vitest";
import { mediaCastAccounts } from "./socialMediaCast.ts";
import type { ImplicitTeam } from "./socialAccounts.ts";

const teams: ImplicitTeam[] = Array.from({ length: 30 }, (_, tid) => ({
	tid,
	region: `Region${tid}`,
	name: `Name${tid}`,
	abbrev: `T${tid}`,
	disabled: false,
}));

describe("the media and fan cast", () => {
	const cast = mediaCastAccounts(teams);

	test("every city has a beat writer, radio, three fans and a film room", () => {
		for (const t of teams) {
			const local = cast.filter((a) => a.tid === t.tid);
			assert.deepStrictEqual(
				local.map((a) => a.archetypeId).sort(),
				[
					"analytics",
					"beatWriter",
					"casualFan",
					"doomerFan",
					"homerFan",
					"localRadio",
				],
				`team ${t.tid}`,
			);
		}
	});

	test("two national insiders, so somebody can be first", () => {
		const insiders = cast.filter(
			(a) => a.tid === undefined && a.archetypeId === "insider",
		);
		assert.strictEqual(insiders.length, 2);
		assert.notStrictEqual(insiders[0]!.name, insiders[1]!.name);
	});

	test("ids and names are unique, and the same on every call", () => {
		const ids = cast.map((a) => a.id);
		assert.strictEqual(new Set(ids).size, ids.length);
		const names = cast.map((a) => a.name);
		assert.strictEqual(new Set(names).size, names.length);
		assert.deepStrictEqual(mediaCastAccounts(teams), cast);
	});

	test("the casual fan reads as a person, not a press office", () => {
		const casual = cast.find((a) => a.id === "m:cast:casual:3")!;
		assert.strictEqual(casual.archetypeId, "casualFan");
		assert.match(casual.name, /Region3|Name3|T3/);
	});
});
