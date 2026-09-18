import { svgs } from "facesjs";
import { buildJerseySvg, type UniformSpec } from "../../common/uniform.ts";
import { CIVILIAN_CLOTHES } from "../../common/civilianClothes.ts";

// faces.js draws jerseys by looking an id up in its svg table, and the table
// is a plain object - so a custom uniform becomes a real jersey by building
// its SVG string and registering it under a synthetic id. The library then
// draws it exactly like a preset: right z-order, right body-size transform,
// and it survives screenshots because nothing about the render path changes.

const hash = (s: string): string => {
	// djb2. The key length rides along so a 32-bit collision also has to match
	// on length before two different uniforms could share an id.
	let h = 5381;
	for (let i = 0; i < s.length; i++) {
		h = ((h << 5) + h + s.charCodeAt(i)) | 0;
	}
	return `${(h >>> 0).toString(36)}x${s.length.toString(36)}`;
};

// The civilian wardrobe goes in once, here, because this module is the one
// that owns writing into the library's table and it is imported by the only
// component that draws a face. A suit is not per-league or per-colour the way
// a uniform is - the colours arrive as teamColors at render time - so there is
// nothing to key and nothing to build lazily.
for (const [id, svg] of Object.entries(CIVILIAN_CLOTHES)) {
	(svgs.jersey as unknown as Record<string, string>)[id] = svg;
}

const registered = new Map<string, string>();

export const registerUniformJersey = (
	spec: UniformSpec,
	colors: [string, string, string],
): string => {
	const key = JSON.stringify([spec, colors]);
	let id = registered.get(key);
	if (id === undefined) {
		id = `uniform${hash(key)}`;
		(svgs.jersey as unknown as Record<string, string>)[id] = buildJerseySvg(
			spec,
			colors,
			id,
		);
		registered.set(key, id);
	}
	return id;
};
