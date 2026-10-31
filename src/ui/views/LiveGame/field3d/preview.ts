import type { HelmetLook } from "../../../../common/types.ts";
import { depthOf } from "../court3d/camera.ts";
import { makeScratch, type Scratch } from "../court3d/sprite.ts";
import { footballBody } from "./body.ts";
import { makeFieldCamera, type FieldRig } from "./camera.ts";
import {
	fieldStyleFor,
	midLogoDecal,
	paintDecals,
	secondaryDecal,
	paintEndZone,
	paintMidLogo,
	paintNumbers,
	paintTurf,
	type DecalPicture,
	type FieldTeam,
} from "./fieldPaint.ts";
import { FIELD_W, GOAL_R, MID_X, MID_Y } from "./geometry.ts";
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
	// Its own pictures - for the end zones, midfield (the logo, left out),
	// and the 25-yard lines - and the league's decals on it (see
	// StadiumLook, paintDecals).
	pictures: {
		end?: HTMLCanvasElement;
		mid?: HTMLCanvasElement;
		secondary?: HTMLCanvasElement;
		field?: HTMLCanvasElement;
		decals?: DecalPicture[];
	} = {},
): FieldPaint => {
	const picked = venueFor(team, { gid: 0, primeTime: sky === "night" });
	const venue = { ...picked, shadow: undefined };
	const style = fieldStyleFor(team, undefined, false, venue);
	// (A picture of the whole field: the logo on it only if asked for - see
	// Field3D.)
	const mid =
		team.stadium?.midLogo !== false
			? (pictures.mid ?? (pictures.field ? undefined : logo))
			: undefined;
	const own = team.stadium?.adjust;
	const decals = [
		...(mid && own?.logo ? [midLogoDecal(mid, own.logo)] : []),
		...(pictures.secondary
			? [secondaryDecal(pictures.secondary, own?.secondary)]
			: []),
		...(pictures.decals ?? []),
	];
	return {
		style,
		turf: paintTurf(style, "preview"),
		endZones: [
			paintEndZone(style, 0, pictures.end),
			paintEndZone(style, 1, pictures.end),
		],
		numbers: paintNumbers(style),
		...(mid && !own?.logo ? { logo: paintMidLogo(mid) } : {}),
		...(decals.length > 0 ? { decals: paintDecals(decals) } : {}),
		...(pictures.field
			? {
					field: pictures.field,
					ownLines: team.stadium?.fieldLines === "picture",
				}
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

// Or the whole field from the broadcast's camera, nobody on it (for the
// league's decals: see CourtDecals).
const FIELD_SHOT = { x: MID_X, y: MID_Y, width: 330 };

export const makePreviewScratch = (): Scratch => makeScratch();

export const drawPreview = (
	ctx: CanvasRenderingContext2D,
	scratch: Scratch,
	paint: FieldPaint,
	team: PreviewTeam,
	logo: HTMLCanvasElement | undefined,
	field = false,
) => {
	const { width: w, height: h } = ctx.canvas;
	if (field) {
		const cam = makeFieldCamera(FIELD_SHOT, w, h);
		drawFieldFrame({ ctx, cam, paint, items: [] });
		return;
	}
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
