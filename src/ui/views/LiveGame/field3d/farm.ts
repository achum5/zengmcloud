import {
	farmHandOf,
	makeFarmOf,
	sendPixels,
	type FarmCodec,
	type FarmOf,
	type FromHandOf,
	type MadeOf,
	type Pixels,
	type ToHandOf,
} from "../court3d/farm.ts";
import type { FieldLook } from "./kit.ts";
import { shapeField, type FieldJob, type FieldShaped } from "./sprite.ts";

// NEW PICTURES SCULPTED OFF THE PAGE'S THREAD - the court's farm (see
// court3d/farm.ts), for football: a man sculpted by a hand off the page - a
// worker of its own (see src/field) - all of him (a football player's head
// is sculpted with the rest of him), kept as it comes back. A look goes
// with its uniform's picture and its team's logo, for the sides of his
// helmet.

// Who he is, the frame it was asked for in, his size then and whether the
// ball was drawn with him.
type Whose = { id: number; frame: number; scale: number; held: boolean };
type SentShape = Omit<FieldShaped, "pixels"> & { pixels: Pixels };
const FIELD: FarmCodec<FieldLook, FieldJob, FieldShaped, SentShape> = {
	split: ({ logo, kitArt, ...rest }) => ({
		rest,
		...(kitArt ? { art: kitArt } : {}),
		...(logo ? { pic: logo } : {}),
	}),
	join: (rest, art, pic) => ({
		...(rest as Omit<FieldLook, "logo" | "kitArt">),
		...(pic ? { logo: pic } : {}),
		...(art ? { kitArt: art } : {}),
	}),
	shape: (job, look) => shapeField(job, look),
	pack: ({ pixels, ...sh }, transfer) => ({
		...sh,
		pixels: sendPixels(pixels, transfer),
	}),
	unpack: ({ pixels, ...sh }) => ({ ...sh, pixels: new ImageData(...pixels) }),
};

export type FieldMade = MadeOf<FieldLook, FieldJob, FieldShaped, Whose>;
export type FieldFarm = FarmOf<FieldLook, FieldJob, FieldShaped, Whose>;
export type ToFieldHand = ToHandOf<FieldJob>;
export type FromFieldHand = FromHandOf<SentShape>;

// Another instance of the worker the hands are (see src/field).
export const spawnFieldWorker = () =>
	new Worker(
		__NODE_ENV === "production"
			? `/gen/field-${window.bbgmVersion}.js`
			: "/gen/field.js",
		{ type: "module" },
	);

export const makeFieldFarm = (spawn: () => Worker, hands: number): FieldFarm =>
	makeFarmOf(spawn, hands, FIELD);

export const fieldFarmHand = (
	post: (msg: { farm: FromFieldHand }, transfer?: ArrayBuffer[]) => void,
) => farmHandOf(post, FIELD);
