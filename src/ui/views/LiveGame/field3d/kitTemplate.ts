import { ART_H, ART_W, type KitArt } from "../court3d/kitArt.ts";
import { closeGaps, letteringBox, templateOf } from "../court3d/kitTemplate.ts";
import { footballBody } from "./body.ts";
import {
	MAIN_RIG,
	makeFieldCamera,
	REPLAY_RIG,
	type FieldRig,
} from "./camera.ts";
import { fieldKitsFor, type FieldLook } from "./kit.ts";
import type { Move } from "./poses.ts";
import { FIELD_LETTERING, wrapsFor } from "./sculpt.ts";
import { markSeen, type FieldPlayerState } from "./sprite.ts";

// THE TEMPLATE A TEAM'S UNIFORM IS PAINTED ON, for football: the court's
// picture (see court3d/kitTemplate.ts), its lower band his pants, marked for
// what shows on a football player - his sleeves out of the tops of the
// jersey's sides, his pants on down past his knees.

// Him seen from all round, from up in the stands and from low down: standing,
// down in his stance (his back and seat), up for a catch (his sides).
const VIEWS: [FieldRig, Move, number, number][] = [
	[MAIN_RIG, "stand", 0, 12],
	[MAIN_RIG, "threePoint", 0, 8],
	[MAIN_RIG, "leapCatch", 0.5, 8],
	[REPLAY_RIG, "stand", 0, 8],
	[REPLAY_RIG, "receiverStance", 0, 8],
];

const seenOnHim = async (): Promise<Uint8Array> => {
	const seen = new Uint8Array(ART_W * ART_H);
	const body = footballBody();
	const art: KitArt = {
		id: "template",
		data: new Uint8ClampedArray(ART_W * ART_H * 4),
		scale: 1,
	};
	const kit = fieldKitsFor(undefined, undefined)[1];
	const look: FieldLook = {
		kit,
		gear: {
			mask: "open",
			cleat: "#ffffff",
			cleatSole: "#ffffff",
			cleatAccent: "#18181c",
		},
		kitArt: art,
		skin: "#8d5524",
		jerseyNumber: "",
		lastName: "",
	};
	for (const [rig, move, phase, turns] of VIEWS) {
		const cam = makeFieldCamera(
			{ x: 180, width: 9, y: 80, z: 3 },
			600,
			800,
			rig,
		);
		for (let i = 0; i < turns; i++) {
			const st: FieldPlayerState = {
				id: 1,
				team: 1,
				x: 180,
				y: 80,
				z: 0,
				yaw: ((i + 0.5) / turns) * Math.PI * 2,
				move,
				phase,
			};
			markSeen(cam, st, body, look, seen);
			// A breath between turns, so the page doesn't stall.
			await new Promise((resolve) => setTimeout(resolve, 0));
		}
	}
	return closeGaps(seen);
};

export const fieldKitTemplate = async (): Promise<HTMLCanvasElement> => {
	const seen = await seenOnHim();
	// His number on his chest; his name and number on his back.
	const body = footballBody();
	const { jersey } = wrapsFor(body);
	const L = FIELD_LETTERING;
	return templateOf(
		seen,
		(
			[
				[L.number, true],
				[L.name, false],
				[L.backNumber, false],
			] as const
		).map(([at, front]) =>
			letteringBox(jersey, at, front, body, body.shoulderW * 1.15),
		),
		"PANTS",
	);
};
