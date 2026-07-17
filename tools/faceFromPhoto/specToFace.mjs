// Shared: turn a compact "perception spec" into a real faces.js FaceConfig.
// Used by both the single-face builder and the batch converter, and by the
// validator, so the mapping + id-checking lives in exactly one place.
//
// A spec is the small thing a vision step (a Claude chat, or the heuristic)
// produces per player:
//   {
//     gender, race,                       // seed coherent defaults for unset slots
//     colors: { skin, hair, shave },      // free hex, set exactly
//     shape:  { fatness, nose, ear },     // free numbers
//     slots:  { head, hair, facialHair, glasses, accessories,
//               eye, eyebrow, nose, mouth, ear }   // discrete menu picks (ids)
//   }
// Anything omitted stays as generate()'s coherent random default, so a sparse
// or partly-wrong spec still yields a well-formed face.

import { generate, svgsIndex } from "facesjs";

// A slot value may be a bare id ("head7") or an object ({ id, angle, ... }).
const asObj = (v) => (typeof v === "string" ? { id: v } : { ...v });

// Validate every id against the real option list; drop unknowns (with a
// collected warning) so a hallucinated id can never reach the renderer.
export const specToFace = (spec, { onWarn } = {}) => {
	const warn = (m) => onWarn?.(m);
	const overrides = {};

	for (const [slot, raw] of Object.entries(spec.slots ?? {})) {
		if (!(slot in svgsIndex)) {
			warn(`unknown slot "${slot}" — ignored`);
			continue;
		}
		const v = asObj(raw);
		if (v.id !== undefined && !svgsIndex[slot].includes(v.id)) {
			warn(`invalid ${slot} id "${v.id}" — using a default instead`);
			delete v.id; // keep any angle/size, let generate() pick the id
		}
		if (Object.keys(v).length > 0) {
			overrides[slot] = { ...(overrides[slot] ?? {}), ...v };
		}
	}

	if (spec.colors?.skin) {
		overrides.body = { ...(overrides.body ?? {}), color: spec.colors.skin };
	}
	if (spec.colors?.hair) {
		overrides.hair = { ...(overrides.hair ?? {}), color: spec.colors.hair };
	}
	if (spec.colors?.shave !== undefined) {
		overrides.head = { ...(overrides.head ?? {}), shave: spec.colors.shave };
	}
	if (spec.shape?.fatness !== undefined) {
		overrides.fatness = spec.shape.fatness;
	}
	if (spec.shape?.nose !== undefined) {
		overrides.nose = { ...(overrides.nose ?? {}), size: spec.shape.nose };
	}
	if (spec.shape?.ear !== undefined) {
		overrides.ear = { ...(overrides.ear ?? {}), size: spec.shape.ear };
	}

	// generate() fills every unset slot coherently; our overrides stamp identity.
	return generate(overrides, {
		gender: spec.gender ?? "male",
		race: spec.race ?? "white",
	});
};
