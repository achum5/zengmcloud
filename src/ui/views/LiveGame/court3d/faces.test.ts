import { assert, describe, test } from "vitest";
import type { FaceConfig } from "facesjs";
import { hairCut, hairStyle, profileOf } from "./faces.ts";

describe("3D faces", () => {
	// Seen from the side or from behind, his hair is drawn the way it sits on
	// his head: cropped close, standing up off it, or hanging down.
	test("every haircut sits on the back of his head the way it should", () => {
		assert.strictEqual(hairCut(undefined), "bald");
		assert.strictEqual(hairCut("bald"), "bald");
		for (const id of ["crop", "crop-fade", "short", "short-fade", "cornrows"]) {
			assert.strictEqual(hairCut(id), "short", id);
		}
		// Standing up off his head, but his ears showing.
		for (const id of ["curlyFade1", "fauxhawk-fade", "spike2", "tall-fade"]) {
			assert.strictEqual(hairCut(id), "short", id);
		}
		for (const id of ["high", "curly", "curly3", "messy", "dreads"]) {
			assert.strictEqual(hairCut(id), "short", id);
		}
		for (const id of ["afro", "shaggy1", "emo"]) {
			assert.strictEqual(hairCut(id), "big", id);
		}
		for (const id of ["longHair", "female3", "female11"]) {
			assert.strictEqual(hairCut(id), "long", id);
		}
		// Tied up in a bun.
		assert.strictEqual(hairCut("female8"), "short");
	});

	// From the side and behind, the shape of it as his face has it.
	test("his hair keeps its shape from every side", () => {
		assert.strictEqual(hairStyle("high").top, "flat");
		assert.isAbove(hairStyle("high").height, hairStyle("crop").height);
		assert.isTrue(hairStyle("tall-fade").fade);
		assert.strictEqual(hairStyle("curly2").top, "curly");
		assert.strictEqual(hairStyle("spike3").top, "spiky");
		assert.isTrue(hairStyle("faux-hawk").strip);
		assert.isTrue(hairStyle("fauxhawk-fade").strip);
		assert.isTrue(hairStyle("cornrows").rows);
		assert.isTrue(hairStyle("short-bald").crown);
		assert.isTrue(hairStyle("short-fade-2").thin);
		assert.isTrue(hairStyle("dreads").bun);
		assert.isTrue(hairStyle("female8").bun);
		assert.deepEqual(hairStyle("parted"), hairStyle(undefined));
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

	// And the shape of it: his nose as big as it is, his ears, how full.
	test("his profile is as big-nosed and full-faced as his face", () => {
		const face = (nose: string, size: number, fatness: number) =>
			({
				nose: { id: nose, size },
				ear: { id: "ear1", size: 1.3 },
				fatness,
			}) as FaceConfig;
		const small = profileOf(face("small", 0.5, 0.1));
		const big = profileOf(face("nose4", 1.2, 0.9));
		const honker = profileOf(face("honker", 1.2, 0.9));
		assert.isBelow(small.nose!, big.nose!);
		assert.isBelow(big.nose!, honker.nose!);
		assert.isBelow(small.full!, big.full!);
		assert.strictEqual(big.ear, 1.3);
	});
});
