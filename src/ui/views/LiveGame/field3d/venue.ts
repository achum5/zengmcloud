import { makeCourtRng } from "../courtRng.ts";
import type { FieldTeam } from "./fieldPaint.ts";
import { FIELD_L, FIELD_W, type Pt } from "./geometry.ts";

// EACH TEAM'S HOME, the same every game it plays there: open to the sky or
// under a roof (about a third of the NFL's teams play indoors), real grass
// mowed in stripes or artificial turf (always, under a roof), and its end
// zones painted solid in its color, left green with its name in its colors,
// or painted and striped corner to corner - as the team has set it (see
// StadiumLook), and what it has not, picked for it from who it is. And the
// game's kickoff: under the lights in prime time - two of the league's
// better teams (see worker primeTime.ts) - in the afternoon otherwise, the
// sun behind the camera and, as often as not, the shadow of the stands there
// reaching out across the field.

export type Venue = {
	roof: "open" | "dome";
	turf: "grass" | "artificial";
	endZones: "solid" | "green" | "striped";
	sky: "day" | "night";
	// How far across the field (feet from the far sideline) that shadow
	// reaches, at each end line; none, all in the sun.
	shadow?: [number, number];
};

const DOMES = 0.3;
const ARTIFICIAL_OUTDOORS = 0.4;
const SHADOWED = 0.65;
const SHADOW_SLANT = 60;

// `game`, the game being shown (its own kickoff, if the team has not set
// one); none, a night game.
export const venueFor = (
	home: FieldTeam | undefined,
	game?: { gid: number; primeTime?: boolean },
): Venue => {
	const own = home?.stadium ?? {};
	const rng = makeCourtRng(
		`venue|${home?.region ?? ""}|${home?.name ?? ""}|${home?.abbrev ?? ""}`,
	);
	// (All drawn whatever the team has set, so what it has not stays as it
	// was.)
	const rolls = [rng(), rng(), rng()] as const;
	const roof = own.roof ?? (rolls[0] < DOMES ? "dome" : "open");
	const turf =
		own.turf ??
		(roof === "dome" || rolls[1] < ARTIFICIAL_OUTDOORS
			? "artificial"
			: "grass");
	const endZones =
		own.endZones ??
		(rolls[2] < 0.6 ? "solid" : rolls[2] < 0.85 ? "green" : "striped");
	const sky =
		own.sky ?? (game === undefined || game.primeTime ? "night" : "day");
	let shadow: [number, number] | undefined;
	if (sky === "day" && roof === "open" && game !== undefined) {
		const r = makeCourtRng(`shadow|${game.gid}`);
		if (r() < SHADOWED) {
			const mid = 25 + r() * (FIELD_W - 10);
			const slant = (r() - 0.5) * SHADOW_SLANT;
			shadow = [mid - slant / 2, mid + slant / 2];
		}
	}
	return { roof, turf, endZones, sky, ...(shadow ? { shadow } : {}) };
};

// Whether a point on the field is in that shadow.
export const inShadow = (shadow: [number, number] | undefined, p: Pt) =>
	!!shadow && p.y > shadow[0] + (p.x / FIELD_L) * (shadow[1] - shadow[0]);
