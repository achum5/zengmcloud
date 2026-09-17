import { useId, useMemo } from "react";
import type { FaceConfig } from "facesjs";
import { DEFAULT_JERSEY, DEFAULT_TEAM_COLORS } from "../../common/constants.ts";
import { parseUniform, presetToSpec } from "../../common/uniform.ts";
import {
	buildFullBodySvg,
	FULL_BODY_VIEWBOX,
} from "../../common/uniformFullBody.ts";
import { MyFace } from "./MyFace.tsx";

// The whole player: the faces.js face slotted over an SVG body wearing the
// team's uniform. The face canvas is 400x600 with the seam at y=600; the body
// canvas shares its x axis, so laying the face over the top of a 400x1300
// frame lines the two up exactly. The face's opaque bottom edge hides the
// join.
//
// Basketball only - the body is drawn in a tank top and shorts.
export const FullBodyPlayer = ({
	colors = DEFAULT_TEAM_COLORS,
	face,
	jersey = DEFAULT_JERSEY,
	jerseyNumber,
	hgt,
}: {
	colors?: [string, string, string];
	face: FaceConfig;
	jersey?: string;
	jerseyNumber?: string;
	hgt?: number;
}) => {
	// Ids inside the SVG string (clip paths, patterns) must not collide between
	// two players on screen at once. useId can contain characters SVG ids
	// dislike, so strip to alphanumerics.
	const idBase = `fb${useId().replaceAll(/[^\dA-Za-z]/g, "")}`;

	// Depend on the array's members and the face's fields rather than the
	// objects, so a fresh array or face with the same values doesn't rebuild
	// the body.
	const [c0, c1, c2] = colors;
	const skinColor = face.body.color;
	const bodySize = face.body.size;

	const body = useMemo(() => {
		const teamColors: [string, string, string] = [c0, c1, c2];
		// An uncustomized team still gets a full body: its preset converts to an
		// equivalent spec on the fly.
		const spec = parseUniform(jersey) ?? presetToSpec(jersey, teamColors);
		return buildFullBodySvg({
			spec,
			teamColors,
			skinColor,
			bodySize,
			hgt,
			jerseyNumber,
			idBase,
		});
	}, [jersey, c0, c1, c2, skinColor, bodySize, hgt, jerseyNumber, idBase]);

	return (
		<div
			style={{ position: "relative", aspectRatio: "400/1300" }}
			className="mx-auto"
		>
			<svg
				viewBox={FULL_BODY_VIEWBOX}
				style={{
					position: "absolute",
					inset: 0,
					width: "100%",
					height: "100%",
				}}
				// Built by buildFullBodySvg, which sanitizes everything it embeds.
				dangerouslySetInnerHTML={{ __html: body }}
			/>
			<div style={{ position: "absolute", top: 0, left: 0, width: "100%" }}>
				<MyFace colors={colors} face={face} jersey={jersey} />
			</div>
		</div>
	);
};
