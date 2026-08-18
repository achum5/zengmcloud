import { generate, type FaceConfig } from "facesjs";
import { idb } from "../db/index.ts";
import type { PlayerWithoutKey, Race } from "../../common/types.ts";
import { DEFAULT_JERSEY } from "../../common/constants.ts";
import g from "./g.ts";
import { defaultGameAttributes } from "../../common/defaultGameAttributes.ts";
import { bySport, isSport } from "../../common/sportFunctions.ts";
import { applyRealisticFace } from "./realisticFaces.ts";

export const generateFace = (
	options:
		| { race?: Race; relative?: undefined; age?: number; pid?: number }
		| {
				race?: undefined;
				relative?: FaceConfig;
				age?: number;
				pid?: number;
		  } = {},
) => {
	let overrides: any;

	if (isSport("baseball")) {
		const [jersey, accessory] = DEFAULT_JERSEY.split(":");
		overrides = {
			jersey: {
				id: jersey,
			},
			accessories: {
				id: accessory,
			},
		};
	} else {
		overrides = {
			jersey: {
				id: DEFAULT_JERSEY,
			},
		};
	}

	if (!isSport("basketball")) {
		overrides.glasses = {
			id: "none",
		};
	}

	// Careful, because this can be called from the team editor before a league is created
	const gender = Object.hasOwn(g, "gender")
		? g.get("gender")
		: defaultGameAttributes.gender;

	const { age, pid, ...faceOptions } = options;

	let face = generate(overrides, {
		gender,
		...faceOptions,
	});

	const allowEyeBlack = bySport({
		baseball: true, // Doesn't matter, gets replaced by hat
		basketball: false,
		football: true,
		hockey: false,
	});

	while (
		// Baseball hat is only for baseball
		(!isSport("baseball") && face.accessories.id.startsWith("hat")) ||
		(!allowEyeBlack && face.accessories.id === "eye-black") ||
		face.accessories.id === "santa-hat"
	) {
		face = generate(overrides, {
			gender,
			...faceOptions,
		});
	}

	// Age-aware features, style groups and per-player colors. Basketball only:
	// the style groups were classified by eye against a basketball league, and
	// the other sports cover their faces with helmets and hats anyway.
	// Same guard as gender above: this runs from the team editor before any
	// league exists, where g has nothing in it.
	const realisticFaces = Object.hasOwn(g, "realisticFaces")
		? g.get("realisticFaces")
		: defaultGameAttributes.realisticFaces;

	if (isSport("basketball") && realisticFaces) {
		applyRealisticFace(face, { age: age ?? 25, race: options.race, pid });
	}

	return face;
};

export const upgradeFace = async (p: PlayerWithoutKey) => {
	// TEMP DISABLE WITH ESLINT 9 UPGRADE eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
	if (!p.face || !p.face.accessories) {
		// @ts-expect-error
		p.face2 = p.face;
		p.face = generateFace();
		await idb.cache.players.put(p);
	}
};
