import { makeCourtRng } from "../courtRng.ts";
import type { FieldTeam } from "./fieldPaint.ts";
import { FIELD_L, FIELD_W, type Pt } from "./geometry.ts";

// EACH TEAM'S HOME, the same every game it plays there: open to the sky or
// under a roof (about a third of the NFL's teams play indoors), real grass
// mowed in stripes or artificial turf (always, under a roof), and its end
// zones painted solid in its color, left green with its name in its colors,
// or painted and striped corner to corner - the city at one end and the
// name at the other, or the name at both - as the team has set it (see
// StadiumLook), and what it has not, picked for it from who it is: a roof
// as its city's weather would call for one. And the game's kickoff: under
// the lights in prime time - two of the league's
// better teams (see worker primeTime.ts) - in the afternoon otherwise, the
// sun behind the camera and, as often as not, the shadow of the stands there
// reaching out across the field.

export type Venue = {
	roof: "open" | "dome";
	turf: "grass" | "artificial";
	endZones: "solid" | "green" | "striped";
	// The city lettered in the left end zone and the name in the right, not
	// the name in both.
	split?: boolean;
	// The end zones lettered in block capitals (left out), a serif or an
	// italic.
	lettering?: "serif" | "italic";
	sky: "day" | "night";
	// How far across the field (feet from the far sideline) that shadow
	// reaches, at each end line; none, all in the sun.
	shadow?: [number, number];
};

const DOMES = 0.3;
const SPLIT = 0.45;
// The stock fields lettered in a serif this often, and in an italic this
// often (block capitals otherwise).
const SERIF = 0.2;
const ITALIC = 0.2;
// The game's own cities (see common/teamInfos), each under a roof or open
// to the sky as a stadium there would be: a roof where the winters are
// hard or the summers hot (or the city has a dome famous enough), open
// air elsewhere. A city not among them, as the rolls have it.
const ROOFED = new Set([
	"Amsterdam",
	"Anchorage",
	"Atlanta",
	"Calgary",
	"Dallas",
	"Detroit",
	"Edmonton",
	"Houston",
	"Indianapolis",
	"Las Vegas",
	"Los Angeles",
	"McMurdo Station",
	"Melbourne",
	"Milwaukee",
	"Minneapolis",
	"Montreal",
	"Moscow",
	"New Orleans",
	"Ottawa",
	"Phoenix",
	"Quebec",
	"Riyadh",
	"San Antonio",
	"Seoul",
	"Singapore",
	"St. Louis",
	"Stockholm",
	"Taipei",
	"Tokyo",
	"Toronto",
	"Vancouver",
	"Warsaw",
	"Winnipeg",
]);
const OPEN_AIR = new Set([
	"Albuquerque",
	"Athens",
	"Austin",
	"Baltimore",
	"Beijing",
	"Belgrade",
	"Berlin",
	"Bogota",
	"Boise",
	"Boston",
	"Brooklyn",
	"Brussels",
	"Buenos Aires",
	"Buffalo",
	"Cairo",
	"Charlotte",
	"Chicago",
	"Cincinnati",
	"Cleveland",
	"Columbus",
	"Copenhagen",
	"Delhi",
	"Denver",
	"Hawaii",
	"Istanbul",
	"Jacksonville",
	"Jakarta",
	"Johannesburg",
	"Kansas City",
	"Kaunas",
	"Kentucky",
	"Kyiv",
	"Lagos",
	"Lima",
	"Lisbon",
	"London",
	"Madrid",
	"Manila",
	"Memphis",
	"Mexico City",
	"Miami",
	"Nashville",
	"New Jersey",
	"New York",
	"Oakland",
	"Oklahoma City",
	"Omaha",
	"Orlando",
	"Paris",
	"Philadelphia",
	"Pittsburgh",
	"Portland",
	"Providence",
	"Raleigh",
	"Rio de Janeiro",
	"Rome",
	"Sacramento",
	"San Diego",
	"San Francisco",
	"San Jose",
	"Santiago",
	"Seattle",
	"Sydney",
	"Tampa",
	"Tehran",
	"Tel Aviv",
	"Utah",
	"Virginia Beach",
	"Washington",
]);
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
	const rolls = [rng(), rng(), rng(), rng(), rng()] as const;
	const city = home?.region ?? "";
	const roof =
		own.roof ??
		(ROOFED.has(city)
			? "dome"
			: OPEN_AIR.has(city)
				? "open"
				: rolls[0] < DOMES
					? "dome"
					: "open");
	const turf =
		own.turf ??
		(roof === "dome" || rolls[1] < ARTIFICIAL_OUTDOORS
			? "artificial"
			: "grass");
	const endZones =
		own.endZones ??
		(rolls[2] < 0.6 ? "solid" : rolls[2] < 0.85 ? "green" : "striped");
	const split =
		own.endZoneText === undefined &&
		city !== "" &&
		city.toUpperCase() !== (home?.name ?? "").toUpperCase() &&
		rolls[3] < SPLIT;
	const lettering =
		own.lettering ??
		(rolls[4] < SERIF
			? "serif"
			: rolls[4] < SERIF + ITALIC
				? "italic"
				: "block");
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
	return {
		roof,
		turf,
		endZones,
		...(split ? { split } : {}),
		...(lettering !== "block" ? { lettering } : {}),
		sky,
		...(shadow ? { shadow } : {}),
	};
};

// Whether a point on the field is in that shadow.
export const inShadow = (shadow: [number, number] | undefined, p: Pt) =>
	!!shadow && p.y > shadow[0] + (p.x / FIELD_L) * (shadow[1] - shadow[0]);
