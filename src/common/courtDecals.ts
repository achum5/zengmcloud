import type { CourtDecal, CourtImageAdjust } from "./types.ts";

// THE LEAGUE'S COURT DECALS, CHECKED: from JSON typed or pasted in, or sent
// to be saved - each field what it should be, or why not.

const WHEN = new Set(["always", "openingNight", "playoffs", "finals"]);
const NUMBERS = new Set(["scale", "opacity", "dx", "dy", "rotate"]);

// An adjust object's first problem, if any (see CourtImageAdjust).
export const adjustProblem = (
	value: unknown,
	where: string,
): string | undefined => {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		return `"${where}" should be an object.`;
	}
	for (const [k, v] of Object.entries(value)) {
		if (k === "fit") {
			if (v !== "contain" && v !== "fill") {
				return `"${where}.fit" should be "contain" or "fill".`;
			}
		} else if (
			!NUMBERS.has(k) ||
			typeof v !== "number" ||
			!Number.isFinite(v)
		) {
			return `"${where}.${k}" isn't a number setting.`;
		}
	}
	return undefined;
};

export const parseCourtDecals = (raw: unknown): CourtDecal[] | string => {
	if (!Array.isArray(raw)) {
		return "Expected a list of decals.";
	}
	const out: CourtDecal[] = [];
	for (const [i, d] of raw.entries()) {
		const at = `decal ${i + 1}`;
		if (typeof d !== "object" || d === null || Array.isArray(d)) {
			return `${at} should be an object.`;
		}
		const decal: Record<string, unknown> = { ...d };
		for (const key of Object.keys(decal)) {
			if (!["image", "when", "from", "to", "adjust", "pair"].includes(key)) {
				return `${at}: unknown field "${key}".`;
			}
		}
		if (typeof decal.image !== "string" || decal.image === "") {
			return `${at}: "image" should be a URL.`;
		}
		if (typeof decal.when !== "string" || !WHEN.has(decal.when)) {
			return `${at}: "when" should be one of ${[...WHEN].join(", ")}.`;
		}
		for (const key of ["from", "to"] as const) {
			const v = decal[key];
			if (v !== undefined && (typeof v !== "number" || !Number.isInteger(v))) {
				return `${at}: "${key}" should be a season.`;
			}
		}
		if (decal.pair !== undefined && typeof decal.pair !== "boolean") {
			return `${at}: "pair" should be true or false.`;
		}
		if (decal.adjust !== undefined) {
			const problem = adjustProblem(decal.adjust, `${at}.adjust`);
			if (problem) {
				return problem;
			}
		}
		out.push({
			image: decal.image,
			when: decal.when as CourtDecal["when"],
			...(decal.from !== undefined ? { from: decal.from as number } : {}),
			...(decal.to !== undefined ? { to: decal.to as number } : {}),
			...(decal.adjust !== undefined
				? { adjust: decal.adjust as CourtImageAdjust }
				: {}),
			...(decal.pair ? { pair: true } : {}),
		});
	}
	return out;
};
