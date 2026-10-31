import { useEffect, useMemo, useRef, useState } from "react";
import type { HelmetLook, StadiumLook } from "../../../../common/types.ts";
import { logoPicture } from "../court3d/logoPicture.ts";
import {
	drawPreview,
	makePreviewScratch,
	paintPreview,
	type PreviewTeam,
} from "./preview.ts";

// The stadium page's picture of a team's stadium and helmet (see preview.ts),
// redrawn as they are changed - the stadium painted again only when it is
// what changed.

const W = 480;
const H = 270;
// (As the 3D field reads it: see Field3D.)
const END_ZONE_PX = 512;

const StadiumPreview = ({
	team,
	stadium,
	helmet,
	sky,
}: {
	team: {
		abbrev?: string;
		name?: string;
		region?: string;
		colors?: [string, string, string];
		imgURL?: string;
	};
	stadium: StadiumLook;
	helmet: HelmetLook;
	sky: "day" | "night";
}) => {
	const canvas = useRef<HTMLCanvasElement>(null);
	const scratch = useMemo(() => makePreviewScratch(), []);
	const [logo, setLogo] = useState<HTMLCanvasElement>();
	useEffect(() => {
		let alive = true;
		void logoPicture(team.imgURL).then((made) => {
			if (alive) {
				setLogo(made);
			}
		});
		return () => {
			alive = false;
		};
	}, [team.imgURL]);

	// (The team's own end zone picture, if it has one.)
	const [endPicture, setEndPicture] = useState<HTMLCanvasElement>();
	const endURL = stadium.endZoneURL;
	useEffect(() => {
		let alive = true;
		void logoPicture(endURL, END_ZONE_PX).then((made) => {
			if (alive) {
				setEndPicture(made);
			}
		});
		return () => {
			alive = false;
		};
	}, [endURL]);

	const [c0, c1, c2] = team.colors ?? [];
	const shown: PreviewTeam = useMemo(
		() => ({
			abbrev: team.abbrev,
			name: team.name,
			region: team.region,
			...(c0 && c1 && c2 ? { colors: [c0, c1, c2] } : {}),
			stadium,
		}),
		[team.abbrev, team.name, team.region, c0, c1, c2, stadium],
	);
	const paint = useMemo(
		() => paintPreview(shown, sky, logo, endPicture),
		[shown, sky, logo, endPicture],
	);

	useEffect(() => {
		const ctx = canvas.current?.getContext("2d");
		if (ctx) {
			drawPreview(ctx, scratch, paint, { ...shown, helmet }, logo);
		}
	}, [paint, shown, helmet, logo, scratch]);

	return (
		<canvas
			ref={canvas}
			width={W}
			height={H}
			aria-label="Stadium preview"
			className="rounded"
			style={{
				width: "100%",
				aspectRatio: `${W} / ${H}`,
				imageRendering: "pixelated",
				background: "#07060a",
			}}
		/>
	);
};

export default StadiumPreview;
