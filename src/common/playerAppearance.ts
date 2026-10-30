// A PLAYER'S LOOK, SEASON BY SEASON.
//
// Faces age (see worker/util/realisticFaces.ts), so the 34-year-old with a
// full beard and lines round his eyes was a clean-shaven rookie, and the
// appearance gallery on his page shows every season of that.
//
// ONE SOURCE OF TRUTH PER SEASON, split in exactly two:
//
//  - `p.face` / `p.imgURL` is the look for the player's LATEST season - this
//    season while he plays, his last season once he has retired (a retired
//    face never changes again). Everything that shows a player now reads it.
//  - `p.appearances` is the look for every EARLIER season, and only those.
//
// Nothing else answers the question, and appearanceForSeason is the only
// thing that asks it. The old version let the newest history entry and p.face
// both claim the current season: aging moved p.face a little every year but
// only wrote history on the years something big happened, so the gallery's
// last face and the face on the rest of the page drifted apart, and a season's
// face could change after it was over.
//
// STORED AS CHANGES. A face moves a little almost every season now - lines
// deepen, ears and noses lengthen, the jaw fills out - so every season needs
// its own entry. A whole face is ~800 bytes and twenty of them a career is
// real weight on a synced league, so after the first entry each one keeps only
// the fields that changed that season (usually five numbers, ~120 bytes),
// applied on top of the look before it. Whole-face entries from older saves
// still read exactly as they did.
//
// An empty history means "always looked like p.face", which is every player in
// a league that has never aged a face.

import type { FaceConfig } from "facesjs";

// The fields that changed in one season: a top-level face key mapped either to
// its new value or, for an object like `hair`, to just the sub-fields that
// changed.
export type FaceChanges = Record<string, unknown>;

export type PlayerAppearance = {
	// The season this entry describes. It stays in effect until the next
	// entry's season.
	season: number;
	// The whole look as of this season - the first entry, and every entry an
	// older save wrote.
	face?: FaceConfig;
	// Or just what changed this season.
	changes?: FaceChanges;
	// A photo in effect from this season. On a `changes` entry, absent means
	// unchanged.
	imgURL?: string;
};

export type Look = { face?: FaceConfig; imgURL?: string };

type PlayerLike = Look & { appearances?: PlayerAppearance[] };

const isPlainObject = (x: unknown): x is Record<string, unknown> =>
	typeof x === "object" && x !== null && !Array.isArray(x);

const same = (a: unknown, b: unknown) =>
	JSON.stringify(a ?? null) === JSON.stringify(b ?? null);

// What `to` changed relative to `from`, one level deep. Undefined if nothing.
export const faceChanges = (
	from: FaceConfig | undefined,
	to: FaceConfig | undefined,
): FaceChanges | undefined => {
	if (!to) {
		return undefined;
	}
	const a = (from ?? {}) as Record<string, unknown>;
	const b = to as unknown as Record<string, unknown>;
	const changes: FaceChanges = {};
	for (const key of Object.keys(b)) {
		const before = a[key];
		const after = b[key];
		if (isPlainObject(before) && isPlainObject(after)) {
			const sub: Record<string, unknown> = {};
			for (const subKey of Object.keys(after)) {
				if (!same(before[subKey], after[subKey])) {
					sub[subKey] = after[subKey];
				}
			}
			if (Object.keys(sub).length > 0) {
				changes[key] = sub;
			}
		} else if (!same(before, after)) {
			changes[key] = after;
		}
	}
	return Object.keys(changes).length > 0 ? changes : undefined;
};

export const applyFaceChanges = (
	face: FaceConfig,
	changes: FaceChanges,
): FaceConfig => {
	const out = structuredClone(face) as unknown as Record<string, unknown>;
	for (const [key, value] of Object.entries(changes)) {
		const current = out[key];
		out[key] =
			isPlainObject(current) && isPlainObject(value)
				? { ...current, ...structuredClone(value) }
				: structuredClone(value);
	}
	return out as unknown as FaceConfig;
};

const sorted = (history: PlayerAppearance[]) =>
	history.toSorted((a, b) => a.season - b.season);

// The look the history describes for `season`, built forward from its first
// entry. A season before the first entry gets the first entry's look - closer
// to the truth than today's face. `fallback` is what a history that somehow
// starts with a `changes` entry is applied to.
const historyLook = (
	history: PlayerAppearance[],
	season: number,
	fallback: Look,
): Look => {
	let look: Look | undefined;
	for (const entry of sorted(history)) {
		if (look !== undefined && entry.season > season) {
			break;
		}
		if (entry.face) {
			look = { face: entry.face, imgURL: entry.imgURL };
		} else {
			const base = look ?? fallback;
			look = {
				face:
					entry.changes && base.face
						? applyFaceChanges(base.face, entry.changes)
						: base.face,
				imgURL: entry.imgURL !== undefined ? entry.imgURL : base.imgURL,
			};
		}
	}
	return look ?? fallback;
};

// A player's latest season, for the rule at the top: the newest season he has
// ratings for - this season while he plays, his last once he has retired.
export const latestAppearanceSeason = (
	ratings: readonly { season: number }[] | undefined,
): number | undefined => {
	let latest: number | undefined;
	for (const row of ratings ?? []) {
		if (latest === undefined || row.season > latest) {
			latest = row.season;
		}
	}
	return latest;
};

// What this player looked like in `season`.
//
// `latestSeason` is his latest season - see the top of the file. From it on,
// the answer is p.face; before it, the history. Left out, the history answers
// every season it covers.
export const appearanceForSeason = (
	p: PlayerLike,
	season: number | undefined,
	latestSeason?: number,
): Look => {
	const current = { face: p.face, imgURL: p.imgURL };
	const history = p.appearances;
	if (
		season === undefined ||
		!Array.isArray(history) ||
		history.length === 0 ||
		(latestSeason !== undefined && season >= latestSeason)
	) {
		return current;
	}
	return historyLook(history, season, current);
};

// Do two looks differ in any way that would show on screen?
export const appearancesDiffer = (a: Look, b: Look): boolean =>
	(a.imgURL ?? "") !== (b.imgURL ?? "") || !same(a.face, b.face);

// THE SLOW PART OF AGING, which is not a change anyone would undo. These move
// a hair every season (see realisticFaces.ts); everything else on a face moves
// in steps - a beard comes in, a line appears, the hair is cut.
export const GRADUAL_FACE_FIELDS: readonly string[] = [
	"fatness",
	"smileLine.size",
	"ear.size",
	"nose.size",
	"eye.angle",
];

const GRADUAL = new Set(GRADUAL_FACE_FIELDS);

// The fields that took a step between two looks, as "key" or "key.sub".
export const steppedFields = (
	from: FaceConfig | undefined,
	to: FaceConfig | undefined,
): string[] => {
	const changes = faceChanges(from, to) ?? {};
	const fields: string[] = [];
	for (const [key, value] of Object.entries(changes)) {
		const paths =
			isPlainObject(value) && isPlainObject((from as any)?.[key])
				? Object.keys(value).map((sub) => `${key}.${sub}`)
				: [key];
		fields.push(...paths.filter((path) => !GRADUAL.has(path)));
	}
	return fields;
};

// Did the look take a step - something you could see and might want back -
// between these two seasons? Gradual aging alone does not count.
export const lookStepped = (a: Look, b: Look): boolean =>
	(a.imgURL ?? "") !== (b.imgURL ?? "") ||
	steppedFields(a.face, b.face).length > 0;

// RECORD WHAT A PAST SEASON LOOKED LIKE.
//
// After this, the history answers `look` for `season`. Seasons after it that
// set nothing of their own carry the new look forward, which is what an edit
// to an old season should do. Returns the new history, or undefined when it
// already said so.
//
// `firstSeason` is where an empty history starts: the look being recorded has
// been in effect since then, as far as anyone knows.
export const recordSeasonLook = ({
	appearances,
	season,
	look,
	firstSeason = season,
}: {
	appearances: PlayerAppearance[] | undefined;
	season: number;
	look: Look;
	firstSeason?: number;
}): PlayerAppearance[] | undefined => {
	if (!look.face) {
		return undefined;
	}
	const history = Array.isArray(appearances) ? sorted(appearances) : [];

	if (history.length === 0) {
		return [
			{
				season: Math.min(firstSeason, season),
				face: structuredClone(look.face),
				imgURL: look.imgURL,
			},
		];
	}

	if (!appearancesDiffer(look, historyLook(history, season, look))) {
		return undefined;
	}

	// What the season would look like without an entry of its own.
	const others = history.filter((entry) => entry.season !== season);
	const first = others[0];
	let entry: PlayerAppearance | undefined;
	if (!first || first.season > season) {
		entry = {
			season,
			face: structuredClone(look.face),
			imgURL: look.imgURL,
		};
	} else {
		const before = historyLook(others, season, look);
		const changes = faceChanges(before.face, look.face);
		const imgChanged = (before.imgURL ?? "") !== (look.imgURL ?? "");
		if (changes || imgChanged) {
			entry = { season, changes };
			if (imgChanged) {
				entry.imgURL = look.imgURL ?? "";
			}
		}
	}

	return sorted(entry ? [...others, entry] : others);
};

// PUT ONE SEASON'S STEP BACK, from the gallery.
//
// Most years a face only ages a hair; the year it takes a step - a beard, a
// line, a haircut - is occasionally one you would rather it had not. Undoing
// it puts the fields that stepped that season back to what they were the
// season before, for as long as nothing later set them again - so undoing the
// year a goatee came in removes the goatee until the season it filled into a
// beard, and the slow aging of every season stays where it was.
//
// When the step is still on the live face (nothing after it touched those
// fields), p.face moves with it - the gallery and the rest of the page show
// the same man.
//
// Returns undefined when that season took no step.
export const revertAppearance = ({
	appearances,
	season,
	current,
	latestSeason,
}: {
	appearances: PlayerAppearance[] | undefined;
	season: number;
	// p.face / p.imgURL.
	current: Look;
	latestSeason: number;
}):
	| { appearances: PlayerAppearance[] | undefined; current: Look }
	| undefined => {
	const history = Array.isArray(appearances) ? sorted(appearances) : [];
	if (history.length === 0) {
		return undefined;
	}
	const player = { ...current, appearances: history };
	const before = appearanceForSeason(player, season - 1, latestSeason);
	const at = appearanceForSeason(player, season, latestSeason);
	if (!before.face || !at.face) {
		return undefined;
	}
	const fields = steppedFields(before.face, at.face);
	const imgStepped = (before.imgURL ?? "") !== (at.imgURL ?? "");
	if (fields.length === 0 && !imgStepped) {
		return undefined;
	}

	const valueAt = (face: FaceConfig, path: string) => {
		const [key, sub] = path.split(".") as [string, string | undefined];
		const top = (face as any)[key];
		return sub === undefined ? top : top?.[sub];
	};
	const restore = (face: FaceConfig): FaceConfig => {
		const out = structuredClone(face) as any;
		for (const path of fields) {
			const [key, sub] = path.split(".") as [string, string | undefined];
			const value = structuredClone(valueAt(before.face!, path));
			if (sub === undefined) {
				out[key] = value;
			} else {
				out[key] = { ...out[key], [sub]: value };
			}
		}
		return out;
	};

	if (season >= latestSeason) {
		return {
			appearances: history,
			current: {
				face: restore(current.face ?? at.face),
				imgURL: imgStepped ? before.imgURL : current.imgURL,
			},
		};
	}

	// The fields this undo reaches: the stepped ones, until a later season
	// sets them again. A later whole-face entry sets everything.
	const later = history.filter(
		(entry) => entry.season > season && entry.season < latestSeason,
	);
	const setLater = (path: string) =>
		later.some((entry) => {
			if (entry.face) {
				return true;
			}
			const [key, sub] = path.split(".") as [string, string | undefined];
			const value = entry.changes?.[key];
			return sub === undefined
				? value !== undefined
				: isPlainObject(value) && sub in value;
		});

	const next = recordSeasonLook({
		appearances: history,
		season,
		look: {
			face: restore(at.face),
			imgURL: imgStepped ? before.imgURL : at.imgURL,
		},
	});
	if (!next) {
		return undefined;
	}

	let nextCurrent = current;
	const stillLive = fields.filter(
		(path) =>
			!setLater(path) &&
			current.face !== undefined &&
			same(valueAt(current.face, path), valueAt(at.face!, path)),
	);
	if (current.face && stillLive.length > 0) {
		const out = structuredClone(current.face) as any;
		for (const path of stillLive) {
			const [key, sub] = path.split(".") as [string, string | undefined];
			const value = structuredClone(valueAt(before.face, path));
			if (sub === undefined) {
				out[key] = value;
			} else {
				out[key] = { ...out[key], [sub]: value };
			}
		}
		nextCurrent = { ...current, face: out };
	}
	if (imgStepped && !later.some((entry) => entry.imgURL !== undefined)) {
		nextCurrent = { ...nextCurrent, imgURL: before.imgURL };
	}

	return { appearances: next, current: nextCurrent };
};
