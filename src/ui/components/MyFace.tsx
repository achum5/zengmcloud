import { useMemo } from "react";
import type { FaceConfig } from "facesjs";
import { Face } from "facesjs/react";
import { DEFAULT_JERSEY, DEFAULT_TEAM_COLORS } from "../../common/constants.ts";
import { isSport } from "../../common/sportFunctions.ts";

const isChristmas = () => {
	const now = new Date();
	return now.getMonth() === 11 && now.getDate() === 25;
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
	// facesjs <Face> regenerates the whole face SVG (an imperative display() in a
	// useLayoutEffect) whenever this `overrides` object changes identity. Building
	// it fresh each render made every face re-generate on every re-render - most
	// visibly during a live sim, where it competes with the on-court ball
	// animation. Memoize it so the face only re-generates when colors/jersey
	// actually change.
	const overrides = useMemo(() => {
		let o;
		if (isSport("baseball")) {
			const [jerseyId, accessoryId] = jersey.split(":");
			o = {
				teamColors: colors,
				jersey: { id: jerseyId! },
				accessories: { id: accessoryId! } as { id: string },
			};
		} else {
			o = {
				teamColors: colors,
				jersey: { id: jersey },
			} as {
				teamColors: [string, string, string];
				jersey: { id: string };
				accessories?: { id: string };
			};
		}

		if (isChristmas()) {
			o.accessories = { id: "santa-hat" };
		}
		return o;
		// colors is an array; depend on its members so a fresh array with the same
		// values doesn't needlessly re-generate the face.
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [colors?.[0], colors?.[1], colors?.[2], jersey]);

	return (
		<Face
			face={face}
			ignoreDisplayErrors
			lazy={lazy}
			overrides={overrides}
			style={{
				aspectRatio: "2/3",
			}}
		/>
	);
};
