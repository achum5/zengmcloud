import { adjustProblem } from "./courtDecals.ts";
import type {
	CourtImageAdjust,
	CourtPair,
	CourtStyle,
	CourtVersion,
	CourtVersionName,
	CourtVersionPicture,
} from "./types.ts";

// A COURT'S VERSIONS (see CourtVersion): which there are, the pictures one
// has on, and a court file's checked.

export const COURT_VERSIONS: readonly CourtVersionName[] = [
	"homeOpener",
	"playoffs",
	"finals",
];

// The version a game is played on: the finals', a playoff game's, or the
// home team's first home game of the regular season's.
export const versionOfGame = (game: {
	playoffs?: boolean;
	finals?: boolean;
	homeOpener?: boolean;
}): CourtVersionName | undefined =>
	game.finals
		? "finals"
		: game.playoffs
			? "playoffs"
			: game.homeOpener
				? "homeOpener"
				: undefined;

// The pictures a court has on for a version - the finals, having none of
// their own, the playoffs'.
export const versionPictures = (
	court: CourtStyle | undefined,
	version: CourtVersionName | undefined,
): CourtVersionPicture[] => {
	const versions = court?.versions;
	if (!versions || version === undefined) {
		return [];
	}
	const own = versions[version];
	if (version === "finals" && own === undefined) {
		return versions.playoffs?.pictures ?? [];
	}
	return own?.pictures ?? [];
};

// Every picture a court's versions name.
export const allVersionPictures = (
	court: CourtStyle | undefined,
): CourtVersionPicture[] =>
	COURT_VERSIONS.flatMap((v) => court?.versions?.[v]?.pictures ?? []);

// A court's versions with their uploaded pictures ("pic:<id>") filled in,
// from these by id - one no longer there left out.
export const withVersionPictures = (
	versions: NonNullable<CourtStyle["versions"]>,
	pictures: Record<string, string>,
): NonNullable<CourtStyle["versions"]> => {
	const out: NonNullable<CourtStyle["versions"]> = {};
	for (const name of COURT_VERSIONS) {
		const v = versions[name];
		if (v === undefined) {
			continue;
		}
		out[name] = v.pictures
			? {
					pictures: v.pictures.flatMap((p) => {
						if (!p.image.startsWith(PIC)) {
							return [p];
						}
						const url = pictures[p.image.slice(PIC.length)];
						return url === undefined ? [] : [{ ...p, image: url }];
					}),
				}
			: {};
	}
	return out;
};
const PIC = "pic:";

// The other one of a pair: across center court, mirrored - or round it, the
// same way up ("diagonal").
export const pairedAdjust = (
	adjust: CourtImageAdjust | undefined,
	pair: CourtPair,
): CourtImageAdjust =>
	pair === "diagonal"
		? { ...adjust, dx: -(adjust?.dx ?? 0), dy: -(adjust?.dy ?? 0) }
		: { ...adjust, dx: -(adjust?.dx ?? 0), rotate: -(adjust?.rotate ?? 0) };

export const pairProblem = (
	value: unknown,
	where: string,
): string | undefined =>
	value === undefined ||
	value === true ||
	value === false ||
	value === "diagonal"
		? undefined
		: `"${where}" should be true, false or "diagonal".`;

// A court file's "versions", checked field by field: them, or why not.
export const parseVersions = (
	raw: unknown,
): Partial<Record<CourtVersionName, CourtVersion>> | string => {
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
		return `"versions" should be an object.`;
	}
	const out: Partial<Record<CourtVersionName, CourtVersion>> = {};
	for (const [name, v] of Object.entries(raw)) {
		if (!(COURT_VERSIONS as readonly string[]).includes(name)) {
			return `Unknown version "${name}" in "versions" (one of ${COURT_VERSIONS.join(", ")}).`;
		}
		const at = `versions.${name}`;
		if (typeof v !== "object" || v === null || Array.isArray(v)) {
			return `"${at}" should be an object.`;
		}
		for (const key of Object.keys(v)) {
			if (key !== "pictures") {
				return `"${at}": unknown field "${key}".`;
			}
		}
		const list = (v as { pictures?: unknown }).pictures;
		const pictures: CourtVersionPicture[] = [];
		if (list !== undefined) {
			if (!Array.isArray(list)) {
				return `"${at}.pictures" should be a list.`;
			}
			for (const [i, p] of list.entries()) {
				const pat = `${at}.pictures[${i}]`;
				if (typeof p !== "object" || p === null || Array.isArray(p)) {
					return `"${pat}" should be an object.`;
				}
				for (const key of Object.keys(p)) {
					if (!["image", "adjust", "pair"].includes(key)) {
						return `"${pat}": unknown field "${key}".`;
					}
				}
				const { image, adjust, pair } = p as Record<string, unknown>;
				if (typeof image !== "string" || image === "") {
					return `"${pat}.image" should be a URL.`;
				}
				const problem =
					(adjust === undefined
						? undefined
						: adjustProblem(adjust, `${pat}.adjust`)) ??
					pairProblem(pair, `${pat}.pair`);
				if (problem) {
					return problem;
				}
				pictures.push({
					image,
					...(adjust !== undefined
						? { adjust: adjust as CourtImageAdjust }
						: {}),
					...(pair ? { pair: pair as CourtPair } : {}),
				});
			}
		}
		out[name as CourtVersionName] = list === undefined ? {} : { pictures };
	}
	return out;
};
