import type { HelmetLook, StadiumLook } from "./types.ts";

// A FOOTBALL TEAM'S STADIUM AND HELMET SETTINGS, kept to what they can be:
// anything else - an unknown key, a value no option has, a color that is
// not one - dropped, and nothing left, nothing at all.

const HEX = /^#[\da-f]{6}$/i;

const ROOFS = new Set(["open", "dome"]);
const TURFS = new Set(["grass", "artificial"]);
const END_ZONES = new Set(["solid", "green", "striped"]);
const SKIES = new Set(["day", "night"]);
export const END_ZONE_TEXT_MAX = 24;

export const cleanStadium = (raw: unknown): StadiumLook | undefined => {
	if (!raw || typeof raw !== "object") {
		return undefined;
	}
	const r = raw as Record<string, unknown>;
	const out: StadiumLook = {};
	if (typeof r.roof === "string" && ROOFS.has(r.roof)) {
		out.roof = r.roof as StadiumLook["roof"];
	}
	if (typeof r.turf === "string" && TURFS.has(r.turf)) {
		out.turf = r.turf as StadiumLook["turf"];
	}
	if (typeof r.endZones === "string" && END_ZONES.has(r.endZones)) {
		out.endZones = r.endZones as StadiumLook["endZones"];
	}
	if (typeof r.sky === "string" && SKIES.has(r.sky)) {
		out.sky = r.sky as StadiumLook["sky"];
	}
	for (const key of [
		"turfColor",
		"endZoneColor",
		"endZoneTextColor",
		"seatColor",
	] as const) {
		const v = r[key];
		if (typeof v === "string" && HEX.test(v)) {
			out[key] = v;
		}
	}
	if (typeof r.endZoneText === "string") {
		out.endZoneText = r.endZoneText.trim().slice(0, END_ZONE_TEXT_MAX);
	}
	if (r.midLogo === false) {
		out.midLogo = false;
	}
	return Object.keys(out).length > 0 ? out : undefined;
};

export const cleanHelmet = (raw: unknown): HelmetLook | undefined => {
	if (!raw || typeof raw !== "object") {
		return undefined;
	}
	const r = raw as Record<string, unknown>;
	const out: HelmetLook = {};
	for (const key of [
		"shell",
		"facemask",
		"homeJersey",
		"homeNumber",
		"homePants",
		"awayJersey",
		"awayNumber",
		"awayPants",
	] as const) {
		const v = r[key];
		if (typeof v === "string" && HEX.test(v)) {
			out[key] = v;
		}
	}
	// (A stripe and its edge may be "", for none.)
	for (const key of ["stripe", "stripeEdge"] as const) {
		const v = r[key];
		if (typeof v === "string" && (v === "" || HEX.test(v))) {
			out[key] = v;
		}
	}
	if (r.logo === false) {
		out.logo = false;
	}
	return Object.keys(out).length > 0 ? out : undefined;
};
