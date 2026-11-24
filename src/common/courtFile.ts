import type { CourtStyle } from "./types.ts";
import { COURT_VERSIONS, parseVersions } from "./courtVersions.ts";
import { NOT_TEAM_LOOKS } from "./scoreBug.ts";

// A COURT AS JSON - the court editor's file, a team looks file's "court",
// a Real Team Info season's - checked field by field.

export const PICTURE_KEYS = [
	"floorImageURL",
	"logoURL",
	"trophyURL",
	"secondaryLogoURL",
	"sidelineImageURL",
	"baselineImageURL",
	"cornerLogoURL",
	"benchImageURL",
	"railImageURL",
] as const satisfies readonly (keyof CourtStyle)[];

const STYLE_STRINGS = new Set<string>([
	"floor",
	"lines",
	"paint",
	"apron",
	"apronText",
	"paintTrim",
	"insideFloor",
	"centerCircle",
	"railText",
	"railText2",
	"centerText",
	"centerTextColor",
	"benchText",
	"benchTextColor",
	...PICTURE_KEYS,
]);
const PATTERNS = new Set([
	"hardwood",
	"parquet",
	"diagonal",
	"chevron",
	"solid",
]);
const LETTERING = new Set(["block", "serif", "italic"]);
const SLOTS = new Set<string>([
	"floor",
	"logo",
	"trophy",
	"secondary",
	"sideline",
	"bench",
	"baseline",
	"corner",
	"rail",
	"cover",
]);

// A court, checked field by field - or why not.
export const checkCourt = (raw: unknown): CourtStyle | string => {
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
		return "Expected a JSON object.";
	}
	if ("teamLooks" in raw) {
		return NOT_TEAM_LOOKS;
	}
	for (const [key, value] of Object.entries(raw)) {
		if (STYLE_STRINGS.has(key)) {
			if (typeof value !== "string") {
				return `"${key}" should be text.`;
			}
		} else if (key === "floorPattern") {
			if (typeof value !== "string" || !PATTERNS.has(value)) {
				return `"floorPattern" should be one of ${[...PATTERNS].join(", ")}.`;
			}
		} else if (key === "lettering") {
			if (typeof value !== "string" || !LETTERING.has(value)) {
				return `"lettering" should be one of ${[...LETTERING].join(", ")}.`;
			}
		} else if (key === "floorLines") {
			if (value !== "picture") {
				return `"floorLines" should be "picture".`;
			}
		} else if (key === "hideRailText") {
			if (typeof value !== "boolean") {
				return `"hideRailText" should be true or false.`;
			}
		} else if (key === "arenaAds") {
			if (!Array.isArray(value) || value.length > 4) {
				return `"arenaAds" should be a list of up to four ads.`;
			}
			for (const ad of value) {
				if (typeof ad !== "object" || ad === null || Array.isArray(ad)) {
					return `Each of "arenaAds" should be an object.`;
				}
				for (const [k, v] of Object.entries(ad)) {
					if (!["text", "sub", "bg", "fg", "imageURL"].includes(k)) {
						return `Unknown field "${k}" in "arenaAds".`;
					}
					if (typeof v !== "string") {
						return `"arenaAds.${k}" should be text.`;
					}
				}
			}
		} else if (key === "versions") {
			const versions = parseVersions(value);
			if (typeof versions === "string") {
				return versions;
			}
		} else if (key === "adjust") {
			if (typeof value !== "object" || value === null) {
				return `"adjust" should be an object.`;
			}
			for (const [slot, a] of Object.entries(value)) {
				if (!SLOTS.has(slot)) {
					return `Unknown slot "${slot}" in "adjust".`;
				}
				if (typeof a !== "object" || a === null) {
					return `"adjust.${slot}" should be an object.`;
				}
				for (const [k, v] of Object.entries(a)) {
					if (k === "fit") {
						if (v !== "contain" && v !== "fill") {
							return `"adjust.${slot}.fit" should be "contain" or "fill".`;
						}
					} else if (
						!["scale", "opacity", "dx", "dy", "rotate"].includes(k) ||
						typeof v !== "number" ||
						!Number.isFinite(v)
					) {
						return `"adjust.${slot}.${k}" isn't a number setting.`;
					}
				}
			}
		} else {
			return `Unknown field "${key}".`;
		}
	}
	const out = { ...(raw as CourtStyle) };
	if ("versions" in raw) {
		out.versions = parseVersions(
			(raw as { versions: unknown }).versions,
		) as CourtStyle["versions"];
	}
	return out;
};

// Every picture a court names - its slots', its sponsors', its versions'.
export const courtURLs = (court: CourtStyle): string[] => [
	...PICTURE_KEYS.map((key) => court[key]).filter(
		(url): url is string => typeof url === "string",
	),
	...(court.arenaAds ?? []).flatMap((a) => (a.imageURL ? [a.imageURL] : [])),
	...COURT_VERSIONS.flatMap(
		(name) => court.versions?.[name]?.pictures?.map((p) => p.image) ?? [],
	),
];

// Every picture a court names, rewritten.
export const mapCourtURLs = async (
	court: CourtStyle,
	f: (url: string) => Promise<string>,
): Promise<CourtStyle> => {
	const out = { ...court };
	for (const key of PICTURE_KEYS) {
		const url = out[key];
		if (typeof url === "string") {
			out[key] = await f(url);
		}
	}
	if (out.arenaAds) {
		out.arenaAds = await Promise.all(
			out.arenaAds.map(async (a) =>
				a.imageURL ? { ...a, imageURL: await f(a.imageURL) } : a,
			),
		);
	}
	if (out.versions) {
		const versions: NonNullable<CourtStyle["versions"]> = {};
		for (const name of COURT_VERSIONS) {
			const v = out.versions[name];
			if (v === undefined) {
				continue;
			}
			versions[name] = v.pictures
				? {
						pictures: await Promise.all(
							v.pictures.map(async (p) => ({
								...p,
								image: await f(p.image),
							})),
						),
					}
				: {};
		}
		out.versions = versions;
	}
	return out;
};
