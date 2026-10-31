import type { HelmetLook } from "../../../../common/types.ts";
import { depthOf } from "../court3d/camera.ts";
import { makeScratch, type Scratch } from "../court3d/sprite.ts";
import { footballBody } from "./body.ts";
import { makeFieldCamera, type FieldRig } from "./camera.ts";
import {
	fieldStyleFor,
	paintEndZone,
	paintMidLogo,
	paintNumbers,
	paintTurf,
	type FieldTeam,
} from "./fieldPaint.ts";
import { FIELD_W, GOAL_R } from "./geometry.ts";
import { fieldGearFor, fieldKit, type FieldLook } from "./kit.ts";
import { drawFieldFrame, type FieldPaint } from "./scene.ts";
import { drawFieldPlayer, type FieldPlayerState } from "./sprite.ts";
import { paintStadium } from "./stadium.ts";
import { venueFor } from "./venue.ts";

// A TEAM'S STADIUM AND HELMET, SHOWN AS ITS SETTINGS HAVE THEM: from down
// on the field near the sideline, two of its players in front - one in the
// home uniform, one in the road - the stands and the sky (or the roof)
// behind, for a page to set them on (see EditTeamStadium).

export type PreviewTeam = FieldTeam & { helmet?: HelmetLook };

// (No stadium shadow here: the afternoon as it is in the open.)
export const paintPreview = (
	team: PreviewTeam,
	sky: "day" | "night" | undefined,
	logo: HTMLCanvasElement | undefined,
): FieldPaint => {
	const picked = venueFor(team, { gid: 0, primeTime: sky === "night" });
	const venue = { ...picked, shadow: undefined };
	const style = fieldStyleFor(team, undefined, false, venue);
	return {
		style,
		turf: paintTurf(style, "preview"),
		endZones: [paintEndZone(style, 0), paintEndZone(style, 1)],
		numbers: paintNumbers(style),
		...(logo && team.stadium?.midLogo !== false
			? { logo: paintMidLogo(logo) }
			: {}),
		stadium: paintStadium(team, undefined, "preview", undefined, logo, venue),
	};
};

const SKINS = ["#8d5524", "#e0ac69"];
// The camera at eye level just off the near sideline, looking across the
// field at the stands, an end zone off to the right; the two men a few steps
// in front of it.
const RIG: FieldRig = { back: 22, high: 6.5, slide: 1, upright: false };
const SHOT = { x: GOAL_R - 34, width: 240, y: -50, z: 11 };
const AT_Y = FIELD_W - 7;
const MEN = [
	{ dx: -3.2, yaw: Math.PI / 2 - 0.75, edition: "home", num: "12" },
	{ dx: 3.2, yaw: Math.PI / 2 + 0.75, edition: "away", num: "88" },
] as const;

export const makePreviewScratch = (): Scratch => makeScratch();

export const drawPreview = (
	ctx: CanvasRenderingContext2D,
	scratch: Scratch,
	paint: FieldPaint,
	team: PreviewTeam,
	logo: HTMLCanvasElement | undefined,
) => {
	const { width: w, height: h } = ctx.canvas;
	const cam = makeFieldCamera(SHOT, w, h, RIG);
	const colors = team.colors ?? ["#8c1d40", "#f2c14e", "#ffffff"];
	const body = footballBody(74, 215);
	const items = MEN.map((m, k) => {
		const kit = fieldKit(colors, m.edition, team.helmet);
		const look: FieldLook = {
			kit,
			gear: fieldGearFor(k + 1, "WR", kit),
			skin: SKINS[k]!,
			jerseyNumber: m.num,
			lastName: "",
			...(logo && team.helmet?.logo !== false ? { logo } : {}),
		};
		const st: FieldPlayerState = {
			id: k + 1,
			team: 1,
			x: SHOT.x + m.dx,
			y: AT_Y,
			z: 0,
			yaw: m.yaw,
			move: "stand",
			phase: 0,
		};
		return {
			depth: depthOf(cam, { x: st.x, y: st.y, z: 0 }),
			draw: () => {
				drawFieldPlayer(ctx, scratch, cam, st, body, look, 1);
			},
		};
	});
	drawFieldFrame({ ctx, cam, paint, items });
};
