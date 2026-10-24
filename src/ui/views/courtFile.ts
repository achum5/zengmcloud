import type { CourtStyle } from "../../common/types.ts";
import { PIC } from "../components/CourtImageField.tsx";
import { toWorker } from "../util/toWorker.ts";

// A COURT AS A FILE: the court editor's JSON, checked field by field, and
// the pictures in it kept in the league as uploads are.

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
]);

// A court typed or pasted in as JSON, checked field by field - or why not.
export const parseCourt = (text: string): CourtStyle | string => {
	let raw: unknown;
	try {
		raw = JSON.parse(text);
	} catch {
		return "Not valid JSON.";
	}
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
		return "Expected a JSON object.";
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
		} else if (key === "floorLines") {
			if (value !== "picture") {
				return `"floorLines" should be "picture".`;
			}
		} else if (key === "hideRailText") {
			if (typeof value !== "boolean") {
				return `"hideRailText" should be true or false.`;
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
	return raw as CourtStyle;
};

// A court's pictures that are written into it (data URLs) kept in the
// league, the court naming them by id - those too big to keep stay in it.
// The ids kept, and the pictures by id.
export const keepCourtPictures = async (
	court: CourtStyle,
): Promise<{
	court: CourtStyle;
	ids: string[];
	pictures: Record<string, string>;
}> => {
	const out = { ...court };
	const ids: string[] = [];
	const pictures: Record<string, string> = {};
	for (const key of PICTURE_KEYS) {
		const url = out[key];
		if (typeof url === "string" && url.startsWith("data:image/")) {
			try {
				const id = await toWorker("main", "storeCourtPicture", url);
				ids.push(id);
				pictures[id] = url;
				out[key] = `${PIC}${id}`;
			} catch {}
		}
	}
	return { court: out, ids, pictures };
};
