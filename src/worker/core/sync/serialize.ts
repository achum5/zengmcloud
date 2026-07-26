// JSON serialization that preserves Infinity / -Infinity / NaN. These appear in
// real game records (e.g. an active player's `retiredYear` is Infinity), and a
// plain JSON.stringify would turn them into null and corrupt the data on the
// receiving device. We store changesets as strings in Firestore anyway (to
// dodge its nested-array restrictions), so this is the single choke point.

const INF = "__Infinity__";
const NEG_INF = "__-Infinity__";
const NAN = "__NaN__";

const replacer = (_key: string, value: unknown) => {
	if (typeof value === "number") {
		if (value === Infinity) {
			return INF;
		}
		if (value === -Infinity) {
			return NEG_INF;
		}
		if (Number.isNaN(value)) {
			return NAN;
		}
	}
	return value;
};

const reviver = (_key: string, value: unknown) => {
	if (value === INF) {
		return Infinity;
	}
	if (value === NEG_INF) {
		return -Infinity;
	}
	if (value === NAN) {
		return NaN;
	}
	return value;
};

export const serializeChangeset = (changeset: unknown): string =>
	JSON.stringify(changeset, replacer);

export const deserializeChangeset = (serialized: string): any =>
	JSON.parse(serialized, reviver);

// --- Bulk payload compression -------------------------------------------------
//
// A single sim day's changeset is ~6 MB of JSON in a 30-team league: hundreds of
// whole player records, each repeating the same keys, each carrying that
// player's full career arrays. That is why one sim publishes ~20 chunk docs and
// why a device that falls behind has thousands of entries to page through.
//
// gzip typically shrinks that by ~10x. base64 gives a third of it back (+33%),
// but keeps the wire format a plain STRING - so chunk splitting, `payloadPart`,
// the outbox, and the activity page all keep working unchanged. Net is still a
// large reduction in both bytes AND the number of log entries, which is what
// actually makes catch-up fast.
//
// Only bulk (chunked) payloads are compressed. Small single-doc changesets stay
// plain: they save little, and leaving them alone keeps the transport's
// synchronous entry parsing untouched.
const GZIP_PREFIX = "GZ1:";

// Safari only got these in 16.4, so every use is feature-checked and falls back
// to plain JSON (the format is self-describing, so a mixed room still works).
const compressionSupported = () =>
	typeof CompressionStream !== "undefined" &&
	typeof DecompressionStream !== "undefined";

// Chunked so a multi-MB payload can't blow the argument limit on spread.
const bytesToBase64 = (bytes: Uint8Array): string => {
	let binary = "";
	const CHUNK = 0x8000;
	for (let i = 0; i < bytes.length; i += CHUNK) {
		binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
	}
	return btoa(binary);
};

const base64ToBytes = (base64: string): Uint8Array => {
	const binary = atob(base64);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i += 1) {
		bytes[i] = binary.charCodeAt(i);
	}
	return bytes;
};

// Compress an already-serialized changeset for shipping as bulk chunks. Returns
// the input unchanged when compression is unavailable or fails, so a publish can
// never be blocked by it - the reader detects the format either way.
export const compressSerialized = async (
	serialized: string,
): Promise<string> => {
	if (!compressionSupported()) {
		return serialized;
	}
	try {
		const stream = new Blob([serialized])
			.stream()
			.pipeThrough(new CompressionStream("gzip"));
		const buffer = await new Response(stream).arrayBuffer();
		return GZIP_PREFIX + bytesToBase64(new Uint8Array(buffer));
	} catch (error) {
		console.error("Sync payload compression failed; sending plain", error);
		return serialized;
	}
};

// Inverse of compressSerialized. A payload without the marker is plain JSON from
// an older client (or one without CompressionStream) and passes straight
// through, so both formats coexist in one room's log forever.
export const decompressSerialized = async (
	payload: string,
): Promise<string> => {
	if (!payload.startsWith(GZIP_PREFIX)) {
		return payload;
	}
	const stream = new Blob([base64ToBytes(payload.slice(GZIP_PREFIX.length))])
		.stream()
		.pipeThrough(new DecompressionStream("gzip"));
	return await new Response(stream).text();
};

// Is this payload compressed? Used to keep the "does it fit in one doc" sizing
// decision honest about what actually goes on the wire.
export const isCompressed = (payload: string): boolean =>
	payload.startsWith(GZIP_PREFIX);
