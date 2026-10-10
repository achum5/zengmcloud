import { idb } from "../db/index.ts";
import type { CourtStyle } from "../../common/types.ts";

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

// Of these pictures, remove any nothing uses any more - no team's uniform,
// no team's court. (A replay that showed one goes without it.)
export const prunePictures = async (ids: Iterable<string>) => {
	const teams = await idb.cache.teams.getAll();
	const used = new Set<string>();
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
