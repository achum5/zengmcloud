import geographicCoordinates from "./geographicCoordinates.ts";
import { CITY_ALIASES, CITY_LOCATIONS } from "./cityLocations.ts";

// WHERE A TEAM PLAYS, for travel. Most regions name a city on the map
// ("Atlanta", "St. Louis") or something that stands for one ("Golden
// State", "Utah", "Brooklyn"). A region that doesn't ("Gotham") can be given
// a home city in Manage Teams; that choice stays with the region it was made
// for, so a renamed or relocated team goes back to its region.

export type HomeCity = {
	// The region this was set for.
	region: string;
	city: string;
};

export type TeamLocation = {
	city: string;
	latitude: number;
	longitude: number;
};

const normalize = (text: string) =>
	text
		.normalize("NFD")
		.replaceAll(/[̀-ͯ]/g, "")
		.toLowerCase()
		.replaceAll("&", " and ")
		.replaceAll(/\bsaint\b/g, "st")
		.replaceAll(/\bft\b/g, "fort")
		.replaceAll(/[^\da-z]+/g, " ")
		.trim();

// Normalized name -> the city's name in CITY_LOCATIONS (or BBGM's own
// geographicCoordinates, whose regions keep their coordinates).
let index: Map<string, string> | undefined;
const getIndex = () => {
	if (!index) {
		index = new Map();
		for (const city of Object.keys(CITY_LOCATIONS)) {
			index.set(normalize(city), city);
		}
		for (const [alias, city] of Object.entries(CITY_ALIASES)) {
			index.set(normalize(alias), city);
		}
		for (const region of Object.keys(geographicCoordinates)) {
			const key = normalize(region);
			if (!index.has(key)) {
				index.set(key, region);
			}
		}
	}
	return index;
};

const coordsOf = (city: string) => {
	const own = geographicCoordinates[city];
	if (own) {
		return { latitude: own.latitude, longitude: own.longitude };
	}
	const known = CITY_LOCATIONS[city];
	return known ? { latitude: known[0], longitude: known[1] } : undefined;
};

// The city a name stands for: the name itself, or with its state or country
// ("Portland, Oregon"), or with words around it ("Greater Boston", "San
// Diego State").
export const findCity = (name: string | undefined): string | undefined => {
	if (name === undefined) {
		return;
	}
	const idx = getIndex();
	const whole = normalize(name);
	if (whole === "") {
		return;
	}
	const direct = idx.get(whole);
	if (direct !== undefined) {
		return direct;
	}

	const beforeComma = name.split(",")[0]!;
	if (beforeComma !== name) {
		const found = idx.get(normalize(beforeComma));
		if (found !== undefined) {
			return found;
		}
	}

	// The longest run of words that names a city.
	const words = whole.split(" ");
	for (let length = words.length - 1; length >= 1; length--) {
		for (let start = 0; start + length <= words.length; start++) {
			const found = idx.get(words.slice(start, start + length).join(" "));
			if (found !== undefined) {
				return found;
			}
		}
	}
};

export const cityLocation = (city: string): TeamLocation | undefined => {
	const coords = coordsOf(city);
	return coords ? { city, ...coords } : undefined;
};

export const teamLocation = ({
	region,
	homeCity,
}: {
	region?: string;
	homeCity?: HomeCity;
}): TeamLocation | undefined => {
	const name = homeCity && homeCity.region === region ? homeCity.city : region;
	const city = findCity(name);
	return city === undefined ? undefined : cityLocation(city);
};

// Every city a team can be put in, for picking one.
export const allCityNames = () =>
	Object.keys(CITY_LOCATIONS).sort((a, b) => a.localeCompare(b));
