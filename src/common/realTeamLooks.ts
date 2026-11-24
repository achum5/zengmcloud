import * as z from "zod";
import type {
	CourtStyle,
	JerseySkinIds,
	RealTeamInfo,
	ScoreBugStyle,
} from "./types.ts";
import { checkCourt, courtURLs } from "./courtFile.ts";
import { parseScoreBug } from "./scoreBug.ts";

// A TEAM'S LOOK FOR A SEASON, FROM REAL TEAM INFO: its 3D uniforms (the home
// and away jersey pictures - see JerseySkin), its court and its score bug,
// the real ones season by season. A season's entry holds from that season
// until the next one listed, and is put on each new season in every real
// players league (see applyRealTeamLooks in worker/util/courtPictures.ts).
//
// Its pictures are links (https://...) or written in as data URLs, which
// are kept in the league when the look is put on, so the team record names
// them only by id. Not "pic:" ids: those name pictures kept in one league.

// (The league's picture store's limit, see PICTURE_MAX.)
const PICTURE_MAX = 700_000;
const LINK = /^https?:\/\/\S{1,2000}$/;
const IMAGE = /^data:image\/(?:png|jpeg|webp);base64,/;
// A jersey picture's swatches are read back exactly: lossless only.
const PNG = /^data:image\/png;base64,/;
const FONT = /^data:font\/(?:woff2|woff|ttf|otf);base64,/;

const isPicture = (url: string, written: RegExp) =>
	LINK.test(url) || (url.length <= PICTURE_MAX && written.test(url));

const PICTURE_PROBLEM =
	"should be a link (https://...) or a picture written in (a data URL)";

const jerseyPicture = z.string().refine((url) => isPicture(url, PNG), {
	message: `A jersey ${PICTURE_PROBLEM}, a PNG.`,
});

export const realJerseySkinsSchema = z.object({
	home: jerseyPicture.exactOptional(),
	away: jerseyPicture.exactOptional(),
});

export const realCourtSchema = z
	.unknown()
	.transform((value, ctx): CourtStyle => {
		const court = checkCourt(value);
		if (typeof court === "string") {
			ctx.addIssue({ code: "custom", message: `court: ${court}` });
			return z.NEVER;
		}
		for (const url of courtURLs(court)) {
			if (!isPicture(url, IMAGE)) {
				ctx.addIssue({
					code: "custom",
					message: `court: a picture ${PICTURE_PROBLEM}.`,
				});
				return z.NEVER;
			}
		}
		return court;
	});

export const realScoreBugSchema = z
	.unknown()
	.transform((value, ctx): ScoreBugStyle => {
		const bug = parseScoreBug(value);
		if (typeof bug === "string") {
			ctx.addIssue({ code: "custom", message: `scoreBug: ${bug}` });
			return z.NEVER;
		}
		const urls = [
			...(bug.image === undefined ? [] : [{ url: bug.image, font: false }]),
			...bug.pieces.flatMap((p) =>
				p.image === undefined ? [] : [{ url: p.image, font: false }],
			),
			...(bug.fonts ?? []).map((f) => ({ url: f.src, font: true })),
		];
		for (const { url, font } of urls) {
			if (!isPicture(url, font ? FONT : IMAGE)) {
				ctx.addIssue({
					code: "custom",
					message: `scoreBug: a ${font ? "font" : "picture"} ${PICTURE_PROBLEM}.`,
				});
				return z.NEVER;
			}
		}
		return bug;
	});

export type RealTeamLooks = {
	jerseySkins?: JerseySkinIds;
	court?: CourtStyle;
	scoreBug?: ScoreBugStyle;
};

export const REAL_LOOK_KEYS = ["jerseySkins", "court", "scoreBug"] as const;

// A team's look for a season: with `exactSeason`, only what is listed for
// that very season; otherwise each part as last listed up to it (the team's
// own entry, under its seasons, before any of them). Undefined: nothing.
export const realTeamLooks = (
	realTeamInfo: RealTeamInfo | undefined,
	srID: string | undefined,
	season: number,
	{ exactSeason = false }: { exactSeason?: boolean } = {},
): RealTeamLooks | undefined => {
	const root = srID === undefined ? undefined : realTeamInfo?.[srID];
	if (!root) {
		return;
	}
	const listed = Object.keys(root.seasons ?? {})
		.map((x) => Number.parseInt(x))
		.filter((x) => !Number.isNaN(x) && x <= season)
		.sort((a, b) => a - b);
	const entries = exactSeason
		? listed.includes(season)
			? [root.seasons![season]!]
			: []
		: [root, ...listed.map((x) => root.seasons![x]!)];
	const out: RealTeamLooks = {};
	for (const entry of entries) {
		for (const key of REAL_LOOK_KEYS) {
			if (entry[key] !== undefined) {
				(out as any)[key] = entry[key];
			}
		}
	}
	return Object.keys(out).length > 0 ? out : undefined;
};
