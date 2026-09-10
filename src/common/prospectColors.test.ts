import { assert, describe, test } from "vitest";
import defaultColleges from "../worker/data/defaultColleges.ts";
import { PLAYER } from "./constants.ts";
import {
	COLLEGE_COLORS,
	COUNTRY_COLORS,
	prospectUniform,
} from "./prospectColors.ts";

describe("prospect colors", () => {
	test("every college the game can assign has colors", () => {
		const missing = Object.keys(defaultColleges).filter(
			(college) => !COLLEGE_COLORS[college],
		);
		assert.deepStrictEqual(missing, []);
	});

	test("every color is a hex triple", () => {
		for (const colors of [
			...Object.values(COLLEGE_COLORS),
			...Object.values(COUNTRY_COLORS),
		]) {
			assert.strictEqual(colors.length, 3);
			for (const c of colors) {
				assert.match(c, /^#[\da-f]{6}$/);
			}
		}
	});

	test("a prospect wears his college, else his country", () => {
		assert.deepStrictEqual(
			prospectUniform({
				tid: PLAYER.UNDRAFTED,
				college: "Duke",
				born: { loc: "Ohio, USA" },
			})?.colors,
			COLLEGE_COLORS.Duke,
		);
		assert.deepStrictEqual(
			prospectUniform({
				tid: PLAYER.UNDRAFTED,
				college: "",
				born: { loc: "Vilnius, Lithuania" },
			})?.colors,
			COUNTRY_COLORS.Lithuania,
		);
		// An unknown college falls through to the country.
		assert.deepStrictEqual(
			prospectUniform({
				tid: PLAYER.UNDRAFTED,
				college: "Oak Hill Academy",
				born: { loc: "Tokyo, Japan" },
			})?.colors,
			COUNTRY_COLORS.Japan,
		);
	});

	test("only draft prospects get one", () => {
		assert.strictEqual(
			prospectUniform({ tid: 0, college: "Duke", born: { loc: "USA" } }),
			undefined,
		);
		assert.strictEqual(
			prospectUniform({
				tid: PLAYER.FREE_AGENT,
				college: "Duke",
				born: { loc: "USA" },
			}),
			undefined,
		);
	});
});
