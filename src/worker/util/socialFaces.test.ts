import { svgsIndex } from "facesjs";
import { assert, describe, test } from "vitest";
import {
	CIVILIAN_CLOTHES,
	CIVILIAN_OUTFITS,
} from "../../common/civilianClothes.ts";
import {
	MALE_ONLY_BODY,
	MALE_ONLY_EAR,
	roleFor,
	socialAccountPicture,
	socialFace,
} from "./socialFaces.ts";

const ARCHETYPES = [
	"insider",
	"nationalPundit",
	"beatWriter",
	"analytics",
	"capNerd",
	"draftHead",
	"historian",
	"localRadio",
	"aggregator",
	"troll",
	"homerFan",
	"doomerFan",
	"casualFan",
];

const TEAM: [string, string, string] = ["#0c2340", "#c8102e", "#ffffff"];

const everyone = () => {
	const out: {
		id: string;
		archetypeId: string;
		face: any;
		age: number;
		gender: string;
	}[] = [];
	for (const archetypeId of ARCHETYPES) {
		for (let i = 0; i < 40; i++) {
			const id = `m:${archetypeId}-${i}`;
			out.push({ id, archetypeId, ...socialFace(id, archetypeId) });
		}
	}
	return out;
};

describe("faces for the cast", () => {
	test("the same account is the same person, every time and everywhere", () => {
		// The whole design rests on this: nothing is stored, so two devices
		// agree only because the derivation is pure. If it ever picks up a
		// Math.random or a clock, two people in one league stop seeing the same
		// room and there is nothing in the save to reconcile.
		for (const archetypeId of ["insider", "casualFan", "historian"]) {
			for (let i = 0; i < 25; i++) {
				const id = `m:${archetypeId}-${i}`;
				assert.deepStrictEqual(
					socialFace(id, archetypeId).face,
					socialFace(id, archetypeId).face,
				);
				assert.deepStrictEqual(
					socialAccountPicture(id, archetypeId, TEAM),
					socialAccountPicture(id, archetypeId, TEAM),
				);
			}
		}
	});

	test("two accounts are two different people", () => {
		const seen = new Set<string>();
		for (const p of everyone()) {
			seen.add(JSON.stringify(p.face));
		}
		// 520 accounts; duplicates are possible by chance but a collapse is not.
		assert.isAbove(seen.size, 500, `only ${seen.size} distinct faces`);
	});

	test("nobody in the press box is dressed for a game", () => {
		// facesjs keeps headbands and eye black in `accessories`, which is
		// exactly the thing this was supposed to stop - a beat writer in a
		// headband is the roster-with-captions look in one detail.
		const COURT = new Set(["headband", "headband-high", "eye-black", "facemask"]);
		for (const p of everyone()) {
			assert.isFalse(
				COURT.has(p.face.accessories.id),
				`${p.id} turned up in ${p.face.accessories.id}`,
			);
			assert.isFalse(
				COURT.has(p.face.glasses.id),
				`${p.id} is wearing a ${p.face.glasses.id}`,
			);
		}
	});

	test("only fans wear a cap", () => {
		const caps = new Set(["hat", "hat2", "hat3"]);
		for (const p of everyone()) {
			if (!caps.has(p.face.accessories.id)) {
				continue;
			}
			assert.isDefined(
				roleFor(p.archetypeId).caps,
				`${p.id} is a ${p.archetypeId} in a cap`,
			);
		}
	});

	test("everybody is wearing something that exists", () => {
		for (const p of everyone()) {
			const picture = socialAccountPicture(p.id, p.archetypeId, TEAM);
			assert.isDefined(
				CIVILIAN_CLOTHES[picture.jersey!],
				`${p.id} is wearing ${picture.jersey}, which is not in the wardrobe`,
			);
			assert.lengthOf(picture.colors!, 3);
		}
		// And every outfit a role can name is real, including the ones no
		// sample happened to draw.
		for (const archetypeId of ARCHETYPES) {
			for (const outfit of roleFor(archetypeId).wardrobe) {
				assert.include(CIVILIAN_OUTFITS, outfit, `${archetypeId} wardrobe`);
				assert.isDefined(CIVILIAN_CLOTHES[`civ-${outfit}`]);
			}
		}
	});

	test("a woman is not drawn out of the men's catalogue", () => {
		// The library draws no female eye lines or brow lines and no female
		// facial hair, so those would be a man's art on a woman's face.
		for (const p of everyone()) {
			if (p.gender !== "female") {
				continue;
			}
			assert.strictEqual(p.face.facialHair.id, "none", `${p.id} has a beard`);
			assert.strictEqual(p.face.head.shave, "rgba(0,0,0,0)", `${p.id} shave`);
			for (const type of ["hair", "eye", "eyebrow", "head"] as const) {
				assert.match(
					p.face[type].id,
					/^female/,
					`${p.id} ${type} is ${p.face[type].id}`,
				);
			}
			assert.notInclude(MALE_ONLY_BODY, p.face.body.id, `${p.id} body`);
			assert.notInclude(MALE_ONLY_EAR, p.face.ear.id, `${p.id} ear`);
		}
	});

	test("the cast is not all men, and not all one age", () => {
		const all = everyone();
		const women = all.filter((p) => p.gender === "female").length;
		assert.isAbove(women / all.length, 0.2, "share of women");
		assert.isBelow(women / all.length, 0.5, "share of women");
		// A historian should read older than a troll, or the age band is doing
		// nothing.
		const mean = (a: string) => {
			const xs = all.filter((p) => p.archetypeId === a).map((p) => p.age);
			return xs.reduce((s, x) => s + x, 0) / xs.length;
		};
		assert.isAbove(mean("historian"), mean("troll") + 20);
		assert.isAbove(mean("insider"), mean("aggregator") + 8);
	});

	test("our copy of the library's gender table still matches it", async () => {
		// svgsGenders is not exported and the package's exports map forbids a
		// deep import, so socialFaces derives it - female art is prefixed
		// "female", and the handful of male-only ids in types with no female
		// art are written out. That derivation can rot if facesjs redraws its
		// catalogue, so check it against the real table, loaded by path because
		// the exports map only blocks the bare specifier.
		const url = new URL(
			"../../../node_modules/facesjs/build/svgs-index.js",
			import.meta.url,
		).href;
		const mod: any = await import(/* @vite-ignore */ url);
		const genders = mod.svgsGenders;

		const femaleTagged = (type: string, id: string) =>
			genders[type][(svgsIndex as any)[type].indexOf(id)] === "female";

		// 1. "starts with female" still means female-tagged, and vice versa -
		//    except blush, which is the library's one exception and is not a
		//    feature this module ever picks.
		for (const type of Object.keys(genders)) {
			for (const id of (svgsIndex as any)[type] as string[]) {
				if (type === "miscLine" && id === "blush") {
					continue;
				}
				assert.strictEqual(
					femaleTagged(type, id),
					id.startsWith("female"),
					`${type}.${id} is tagged ${genders[type][(svgsIndex as any)[type].indexOf(id)]}`,
				);
			}
		}

		// 2. The written-out male-only lists are still exactly right.
		const maleOnly = (type: string) =>
			((svgsIndex as any)[type] as string[]).filter(
				(id) => genders[type][(svgsIndex as any)[type].indexOf(id)] === "male",
			);
		assert.deepStrictEqual(maleOnly("body").toSorted(), [...MALE_ONLY_BODY].toSorted());
		assert.deepStrictEqual(maleOnly("ear").toSorted(), [...MALE_ONLY_EAR].toSorted());

		// 3. The types a woman is drawn from all still HAVE female art, which
		//    is what lets the male-only list stay this short.
		for (const type of ["hair", "eye", "eyebrow", "head", "hairBg"]) {
			assert.isAbove(
				((svgsIndex as any)[type] as string[]).filter((id) =>
					id.startsWith("female"),
				).length,
				0,
				`${type} has no female art any more`,
			);
		}
	});
});
