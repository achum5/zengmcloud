import { generate, type FaceConfig } from "facesjs";
import { idb } from "../db/index.ts";
import type { PlayerWithoutKey, Race } from "../../common/types.ts";
import { DEFAULT_JERSEY } from "../../common/constants.ts";
import g from "./g.ts";
import { defaultGameAttributes } from "../../common/defaultGameAttributes.ts";
import { bySport, isSport } from "../../common/sportFunctions.ts";
import {
	applyFaceAgingHistory,
	applyRealisticFace,
	familySeed,
	inferRaceFromFace,
} from "./realisticFaces.ts";

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

	// A relative's face is DEEP-COPIED from the player he is related to, and the
	// parts facesjs leaves alone are the whole point: same skin, same hair
	// colour, three quarters of the same features. Two things have to change
	// downstream because of that.
	//
	// The first is race. facesjs works out a relative's race by matching his
	// skin colour against its own palette, exactly - and the per-player colour
	// nudge below moves every face off that palette, so for any player in a
	// realistic-faces league the match fails and the son comes back with hair
	// drawn from a randomly-raced face. Reading the race back off the father's
	// skin and handing it to the age pass fixes the hair without handing it to
	// facesjs, which would make it regenerate the colours and lose the family.
	//
	// The second is the colour nudge itself: see `keepColors` below.
	const inherited = faceOptions.relative !== undefined;
	const race =
		options.race ??
		(inherited ? inferRaceFromFace(faceOptions.relative!) : undefined);

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

	// Put the family's colours back. facesjs re-rolls skin and hair from the
	// palette whenever it can work out the relative's race, and whether it can
	// depends on whether the per-player nudge happened to round that particular
	// face onto an exact palette value - so a son inherited his father's skin
	// most of the time and a stranger's the rest of it. The colours a son gets
	// from his father ARE the resemblance; they are not left to a coin toss
	// inside the library.
	const relative = faceOptions.relative;
	if (relative) {
		if (relative.body?.color) {
			face.body.color = relative.body.color;
		}
		if (relative.hair?.color) {
			face.hair.color = relative.hair.color;
		}
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
		applyRealisticFace(face, {
			age: age ?? 25,
			race,
			pid,
			keepColors: inherited,
		});
	}

	return face;
};

// A FACE BUILT AT ONE AGE, FOR A PLAYER WHO IS NOW ANOTHER.
//
// Players are generated as prospects and then developed for years - a whole
// league at creation, a draft class inside its own generator - and the face
// stayed the prospect's. Every new league opened on 34-year-olds with the
// beardless, unlined faces of nineteen-year-olds. This replays the years
// between, one at a time, with the same rules a played season uses, so the
// face arrives where a played career would have taken it. Nothing when the
// mode is off (facesjs ignores age anyway), when there is no face, or when
// there are no years to replay.
export const catchUpFace = (
	p: {
		face?: FaceConfig;
		born: { year: number };
		pid?: number;
		relatives?: { pid: number }[];
	},
	fromAge: number,
) => {
	const realisticFaces = Object.hasOwn(g, "realisticFaces")
		? g.get("realisticFaces")
		: defaultGameAttributes.realisticFaces;
	if (!isSport("basketball") || !realisticFaces || !p.face) {
		return;
	}
	const currentAge = g.get("season") - p.born.year;
	if (!Number.isFinite(currentAge) || currentAge <= fromAge) {
		return;
	}
	applyFaceAgingHistory({
		face: p.face,
		rookieAge: fromAge,
		currentAge,
		pid: p.pid,
		familyPid: familySeed(p.pid, p.relatives),
		race: inferRaceFromFace(p.face),
	});
};

export const upgradeFace = async (p: PlayerWithoutKey) => {
	// TEMP DISABLE WITH ESLINT 9 UPGRADE eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
	if (!p.face || !p.face.accessories) {
		// @ts-expect-error
		p.face2 = p.face;
		// His own age and id, so a face made on the fly for a 33-year-old is
		// a 33-year-old's.
		p.face = generateFace({
			age: Object.hasOwn(g, "season")
				? g.get("season") - p.born.year
				: undefined,
			pid: p.pid,
		});
		await idb.cache.players.put(p);
	}
};
