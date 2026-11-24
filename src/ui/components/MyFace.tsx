import type { FaceConfig } from "facesjs";
import { Face } from "facesjs/react";
import fastDeepEqual from "fast-deep-equal";
import { useMemo, useState } from "react";
import { DEFAULT_JERSEY, DEFAULT_TEAM_COLORS } from "../../common/constants.ts";
import { parseUniform } from "../../common/uniform.ts";
import { registerUniformJersey } from "../util/uniformJersey.ts";
import { stubbleShave } from "../../common/stubbleShade.ts";

const isChristmas = () => {
	const now = new Date();
	return now.getMonth() === 11 && now.getDate() === 25;
};

// What faces.js is told on top of the face itself: team colors, uniform,
// stubble tint. Shared with anything else that draws a face (the face
// converter's review sheets), so a face looks the same everywhere.
export const faceOverrides = (
	colors: [string, string, string],
	jersey: string,
	shave: string | undefined,
) => {
	let o;
	if (__SPORT === "baseball") {
		const [jerseyId, accessoryId] = jersey.split(":");
		o = {
			teamColors: colors,
			jersey: { id: jerseyId! },
			accessories: { id: accessoryId! } as { id: string },
		};
	} else {
		// A custom uniform travels inside the jersey string. Build its SVG,
		// register it into the faces.js table, and point the override at it -
		// from here on it is a preset like any other.
		const spec = parseUniform(jersey);
		o = {
			teamColors: colors,
			jersey: { id: spec ? registerUniformJersey(spec, colors) : jersey },
		} as {
			teamColors: [string, string, string];
			jersey: { id: string };
			accessories?: { id: string };
		};
	}

	if (isChristmas()) {
		o.accessories = { id: "santa-hat" };
	}
	if (shave !== undefined) {
		(o as any).head = { shave };
	}
	return o;
};

export const MyFace = ({
	colors = DEFAULT_TEAM_COLORS,
	face,
	jersey = DEFAULT_JERSEY,
	lazy,
}: {
	colors?: [string, string, string];
	face: FaceConfig;
	jersey?: string;
	lazy?: boolean;
}) => {
	// Face redraws the whole SVG, which is slow, whenever face or overrides is a new object. Data from the worker is a new object on every update even if nothing changed, so keep the previous face while its contents are the same.
	const [stableFace, setStableFace] = useState(face);
	if (face !== stableFace && !fastDeepEqual(face, stableFace)) {
		setStableFace(face);
	}

	// Stubble tinted to the hair (see stubbleShade). Keyed on the two inputs so
	// a face facesjs has already merged the tint into does not re-render.
	const shave = stubbleShave(stableFace?.head?.shave, stableFace?.hair?.color);

	const overrides = useMemo(
		() => faceOverrides(colors, jersey, shave),
		// colors is an array; depend on its members so a fresh array with the same
		// values doesn't needlessly re-generate the face.
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[colors?.[0], colors?.[1], colors?.[2], jersey, shave],
	);

	return (
		<Face
			face={stableFace}
			ignoreDisplayErrors
			lazy={lazy}
			overrides={overrides}
			style={{
				aspectRatio: "2/3",
			}}
		/>
	);
};
