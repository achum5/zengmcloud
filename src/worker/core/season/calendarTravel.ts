import geographicCoordinates from "../../../common/geographicCoordinates.ts";

type Coords = { latitude: number; longitude: number };

// Great-circle miles between two cities.
export const distanceMiles = (a: Coords, b: Coords) => {
	const toRad = Math.PI / 180;
	const dLat = (b.latitude - a.latitude) * toRad;
	const dLon = (b.longitude - a.longitude) * toRad;
	const h =
		Math.sin(dLat / 2) ** 2 +
		Math.cos(a.latitude * toRad) *
			Math.cos(b.latitude * toRad) *
			Math.sin(dLon / 2) ** 2;
	return 3959 * 2 * Math.asin(Math.min(1, Math.sqrt(h)));
};

// What a flight costs when one of the cities isn't on the map: an average
// NBA hop, so a league of made-up cities gets a sensible calendar, just not a
// travel-shaped one.
const UNKNOWN_MILES = 1000;

export const coordsOfRegion = (region: string | undefined) =>
	region === undefined ? undefined : geographicCoordinates[region];

// Miles between every pair of teams, by index.
export const distanceMatrix = (regions: (string | undefined)[]) => {
	const n = regions.length;
	const coords = regions.map(coordsOfRegion);
	const out = new Float64Array(n * n);
	for (let i = 0; i < n; i++) {
		for (let j = i + 1; j < n; j++) {
			const a = coords[i];
			const b = coords[j];
			const d = a && b ? distanceMiles(a, b) : UNKNOWN_MILES;
			out[i * n + j] = d;
			out[j * n + i] = d;
		}
	}
	return out;
};
