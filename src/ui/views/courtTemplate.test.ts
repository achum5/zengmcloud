import { describe, expect, test } from "vitest";
import {
	FLOOR_TEMPLATE_H,
	FLOOR_TEMPLATE_W,
	isWideFloor,
} from "./courtTemplate.ts";

describe("isWideFloor", () => {
	test("a 118 x 66 ft picture, at any size it is saved", () => {
		expect(isWideFloor(2360, 1320)).toBe(true);
		expect(isWideFloor(1888, 1056)).toBe(true);
		// (Scaled to a round width, its height rounded.)
		expect(isWideFloor(2080, 1163)).toBe(true);
	});

	test("the template's own shape, and other pictures, are not", () => {
		expect(isWideFloor(FLOOR_TEMPLATE_W, FLOOR_TEMPLATE_H)).toBe(false);
		expect(isWideFloor(1920, 1080)).toBe(false);
		expect(isWideFloor(2048, 1024)).toBe(false);
		expect(isWideFloor(0, 0)).toBe(false);
	});
});
