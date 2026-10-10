import { idb } from "../db/index.ts";
import g from "./g.ts";
import type {
	CourtDecal,
	CourtDecalPlaced,
	CourtStyle,
} from "../../common/types.ts";

// THE PICTURES A COURT IS PAINTED WITH.
//
// A picture uploaded for one of a court's slots is kept once, by a hash of
// it, in the league's synced picture store - the one the 3D uniforms live
// in - and the court names it "pic:<id>": so it travels in the league file
// and the team record stays small. Whatever draws a court is handed it with
// its pictures filled in (see resolveCourt).
export const PIC = "pic:";
export const COURT_PICTURE_KEYS = [
	"logoURL",
	"trophyURL",
	"secondaryLogoURL",
	"sidelineImageURL",
	"baselineImageURL",
	"cornerLogoURL",
	"benchImageURL",
	"railImageURL",
] as const satisfies readonly (keyof CourtStyle)[];

// A picture as a data URL, at most this long (the store syncs a row as one
// document).
export const PICTURE_MAX = 700_000;
const PICTURE_TYPES = /^data:image\/(png|jpeg|webp);base64,[\w+/]+=*$/;

// A picture by id. The store isn't kept in memory: a picture is in the
// cache only if it was written lately, otherwise on disk.
export const pictureById = async (id: string) =>
	(await idb.cache.jerseySkins.get(id)) ??
	(await idb.league.get("jerseySkins", id));

// Keep a picture, once - its id.
export const storePicture = async (
	url: string,
	types: RegExp = PICTURE_TYPES,
): Promise<string> => {
	if (url.length > PICTURE_MAX || !types.test(url)) {
		throw new Error("Invalid picture");
	}
	const digest = await crypto.subtle.digest(
		"SHA-256",
		new TextEncoder().encode(url),
	);
	const id = Array.from(new Uint8Array(digest).slice(0, 10), (b) =>
		b.toString(16).padStart(2, "0"),
	).join("");
	if (!(await pictureById(id))) {
		await idb.cache.jerseySkins.put({ id, url, at: Date.now() });
	}
	return id;
};

// The pictures a court names.
export const courtPictureIds = (court: CourtStyle | undefined): string[] => {
	const ids: string[] = [];
	for (const key of COURT_PICTURE_KEYS) {
		const v = court?.[key];
		if (typeof v === "string" && v.startsWith(PIC)) {
			ids.push(v.slice(PIC.length));
		}
	}
	return ids;
};

// Them, by id - those there are.
export const courtPictures = async (
	court: CourtStyle | undefined,
): Promise<Record<string, string>> => {
	const out: Record<string, string> = {};
	for (const id of courtPictureIds(court)) {
		try {
			const row = await pictureById(id);
			if (row) {
				out[id] = row.url;
			}
		} catch {
			// Cosmetic: that slot goes without.
		}
	}
	return out;
};

// A court with its pictures filled in, ready to draw - a picture no longer
// there leaves its slot empty.
export const resolveCourt = async (
	court: CourtStyle | undefined,
): Promise<CourtStyle | undefined> => {
	if (!court || courtPictureIds(court).length === 0) {
		return court;
	}
	const pictures = await courtPictures(court);
	const out: CourtStyle = { ...court };
	for (const key of COURT_PICTURE_KEYS) {
		const v = out[key];
		if (typeof v === "string" && v.startsWith(PIC)) {
			const url = pictures[v.slice(PIC.length)];
			if (url === undefined) {
				delete out[key];
			} else {
				out[key] = url;
			}
		}
	}
	return out;
};

// The pictures the league's decals name.
export const decalPictureIds = (decals: readonly CourtDecal[]): string[] =>
	decals
		.map((d) => d.image)
		.filter((u) => u.startsWith(PIC))
		.map((u) => u.slice(PIC.length));

// THE LEAGUE'S DECALS FOR A GAME: those laid down on its occasion - every
// game, opening night (the first day of the regular season), the playoffs,
// the finals - in its season, their pictures filled in.
export const decalsForGame = async (game: {
	season: number;
	day?: number;
	playoffs?: boolean;
	finals?: boolean;
}): Promise<CourtDecalPlaced[]> => {
	const all = g.get("courtDecals") ?? [];
	const on = all.filter(
		(d) =>
			(d.from === undefined || game.season >= d.from) &&
			(d.to === undefined || game.season <= d.to) &&
			(d.when === "always" ||
				(d.when === "openingNight" && !game.playoffs && game.day === 1) ||
				(d.when === "playoffs" && game.playoffs === true) ||
				(d.when === "finals" && game.finals === true)),
	);
	const out: CourtDecalPlaced[] = [];
	for (const d of on) {
		let href: string | undefined = d.image;
		if (href.startsWith(PIC)) {
			try {
				href = (await pictureById(href.slice(PIC.length)))?.url;
			} catch {
				href = undefined;
			}
		}
		if (href) {
			out.push({
				href,
				...(d.adjust ? { adjust: d.adjust } : {}),
				...(d.pair ? { pair: true } : {}),
			});
		}
	}
	return out;
};

// A court with the game's decals laid on it.
export const withDecals = (
	court: CourtStyle | undefined,
	decals: CourtDecalPlaced[],
): CourtStyle | undefined =>
	decals.length === 0 ? court : { ...court, decals };

// Of these pictures, remove any nothing uses any more - no team's uniform,
// no team's court, no decal. (A replay that showed one goes without it.)
export const prunePictures = async (ids: Iterable<string>) => {
	const teams = await idb.cache.teams.getAll();
	const used = new Set<string>(decalPictureIds(g.get("courtDecals") ?? []));
	for (const t of teams) {
		for (const id of [t.jerseySkins?.home, t.jerseySkins?.away]) {
			if (id !== undefined) {
				used.add(id);
			}
		}
		for (const id of courtPictureIds(t.court)) {
			used.add(id);
		}
	}
	for (const id of new Set(ids)) {
		if (!used.has(id)) {
			await idb.cache.jerseySkins.delete(id);
		}
	}
};
