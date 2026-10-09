import { assert, describe, test } from "vitest";
import type { FaceConfig } from "facesjs";
import { hairCut, profileOf } from "./faces.ts";

describe("3D faces", () => {
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

	// Side on, his face in profile keeps what it has from the front: the
	// beard on it, where it grows, his headband, eye black.
	test("his profile has the beard, headband and eye black his face has", () => {
		const face = (facialHair: string, accessories = "none") =>
			({
				facialHair: { id: facialHair },
				accessories: { id: accessories },
			}) as FaceConfig;
		const colors: [string, string, string] = ["#007a33", "#ba9653", "#fff"];
		assert.deepEqual(profileOf(face("none")), {});
		assert.deepEqual(profileOf(face("beard1")), {
			lip: true,
			jaw: true,
			chin: true,
			burns: true,
		});
		const goatee = profileOf(face("goatee1-stache"));
		assert.isTrue(goatee.chin && goatee.lip);
		assert.isFalse(goatee.jaw);
		const stache = profileOf(face("mustache1"));
		assert.isTrue(stache.lip);
		assert.isFalse(stache.chin || stache.jaw);
		assert.isTrue(profileOf(face("sideburns2")).burns);
		assert.isFalse(profileOf(face("sideburns2")).lip);
		assert.deepEqual(profileOf(face("none", "headband-high"), colors).band, {
			high: true,
			color: "#007a33",
			stripe: "#ba9653",
		});
		assert.isTrue(profileOf(face("none", "eye-black")).eyeBlack);
		assert.isUndefined(profileOf(face("none", "eye-black")).band);
	});
});
