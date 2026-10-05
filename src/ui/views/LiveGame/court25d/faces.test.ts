import { assert, describe, test } from "vitest";
import { hairCut } from "./faces.ts";

describe("2.5D faces", () => {
	// Seen from the side or from behind, his hair is drawn the way it sits on
	// his head: cropped close, standing up off it, or hanging down.
	test("every haircut sits on the back of his head the way it should", () => {
		assert.strictEqual(hairCut(undefined), "bald");
		assert.strictEqual(hairCut("bald"), "bald");
		for (const id of ["crop", "crop-fade", "short", "short-fade", "cornrows"]) {
			assert.strictEqual(hairCut(id), "short", id);
		}
		for (const id of ["curlyFade1", "fauxhawk-fade", "spike2", "tall-fade"]) {
			assert.strictEqual(hairCut(id), "short", id);
		}
		for (const id of ["afro", "afro2", "high", "curly", "curly3", "messy"]) {
			assert.strictEqual(hairCut(id), "big", id);
		}
		for (const id of ["dreads", "longHair", "female3"]) {
			assert.strictEqual(hairCut(id), "long", id);
		}
	});
});
