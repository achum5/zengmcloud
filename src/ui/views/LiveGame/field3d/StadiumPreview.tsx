import { useEffect, useMemo, useRef, useState } from "react";
import type { HelmetLook, StadiumLook } from "../../../../common/types.ts";
import { logoPicture } from "../court3d/logoPicture.ts";
import {
	drawPreview,
	makePreviewScratch,
	paintPreview,
	type PreviewTeam,
} from "./preview.ts";
import type { DecalPicture } from "./fieldPaint.ts";

// The stadium page's picture of a team's stadium and helmet (see preview.ts),
// redrawn as they are changed - the stadium painted again only when it is
// what changed.

const W = 480;
const H = 270;
// (As the 3D field reads them: see Field3D.)
const END_ZONE_PX = 512;
const DECAL_PX = 512;

const StadiumPreview = ({
	team,
	stadium,
	helmet,
	sky,
	field = false,
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
	// The whole field instead, nobody on it (see drawPreview).
	field?: boolean;
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

	// (The team's own pictures, if it has them: see StadiumLook.)
	const [own, setOwn] = useState<{
		end?: HTMLCanvasElement;
		mid?: HTMLCanvasElement;
		secondary?: HTMLCanvasElement;
	}>({});
	const endURL = stadium.endZoneURL;
	const midURL = stadium.midLogoURL;
	const secondURL = stadium.secondaryLogoURL;
	useEffect(() => {
		let alive = true;
		void Promise.all([
			logoPicture(endURL, END_ZONE_PX),
			logoPicture(midURL),
			logoPicture(secondURL, END_ZONE_PX),
		]).then(([end, mid, secondary]) => {
			if (alive) {
				setOwn({ end, mid, secondary });
			}
		});
		return () => {
			alive = false;
		};
	}, [endURL, midURL, secondURL]);

	// (The league's decals on it, if any: see CourtDecals.)
	const [decals, setDecals] = useState<DecalPicture[]>([]);
	const decalsKey = JSON.stringify(stadium.decals ?? null);
	useEffect(() => {
		let alive = true;
		void Promise.all(
			(stadium.decals ?? []).map(async (d) => {
				const picture = await logoPicture(d.href, DECAL_PX);
				return picture ? [{ picture, adjust: d.adjust, pair: d.pair }] : [];
			}),
		).then((made) => {
			if (alive) {
				setDecals(made.flat());
			}
		});
		return () => {
			alive = false;
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [decalsKey]);

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
		() => paintPreview(shown, sky, logo, { ...own, decals }),
		[shown, sky, logo, own, decals],
	);

	useEffect(() => {
		const ctx = canvas.current?.getContext("2d");
		if (ctx) {
			drawPreview(ctx, scratch, paint, { ...shown, helmet }, logo, field);
		}
	}, [paint, shown, helmet, logo, scratch, field]);

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
