// A grid, as a short code you can paste to someone else so they play the exact
// same board.
//
// The code encodes the six CRITERIA, not the answers - team ids and achievement
// ids - so it stays a few dozen characters and resolves against whatever league
// the recipient is in. That also means a code is only meaningful inside the
// league it came from: tid 7 is a different franchise in someone else's league.
// Within a room, which is where these get passed around, it's exact.

export type GridStatOp = "gte" | "lte";
export type GridDecadeMode = "debut" | "played";

export type GridCodeRef =
	| { kind: "team"; tid: number }
	| { kind: "career" | "season"; id: string }
	| { kind: "stat"; spec: string; op: GridStatOp; value: number }
	| { kind: "decade"; mode: GridDecadeMode; decade: number };

// Bumped only if the ref shapes change in a way an old code can't express.
const VERSION = "1";

const FIELD = "~";
const REF = "|";

const encodeRef = (r: GridCodeRef): string => {
	switch (r.kind) {
		case "team":
			return `t${r.tid}`;
		case "career":
			return `c${r.id}`;
		case "season":
			return `s${r.id}`;
		case "stat":
			return `x${r.spec}${FIELD}${r.op === "lte" ? "l" : "g"}${FIELD}${r.value}`;
		case "decade":
			return `d${r.mode === "debut" ? "b" : "p"}${FIELD}${r.decade}`;
	}
};

const decodeRef = (s: string): GridCodeRef | undefined => {
	const tag = s[0];
	const rest = s.slice(1);
	if (tag === "t") {
		const tid = Number.parseInt(rest);
		return Number.isFinite(tid) ? { kind: "team", tid } : undefined;
	}
	if (tag === "c" || tag === "s") {
		return rest
			? { kind: tag === "c" ? "career" : "season", id: rest }
			: undefined;
	}
	if (tag === "x") {
		const [spec, op, value] = rest.split(FIELD);
		const parsed = Number.parseFloat(value ?? "");
		if (!spec || (op !== "g" && op !== "l") || !Number.isFinite(parsed)) {
			return undefined;
		}
		return {
			kind: "stat",
			spec,
			op: op === "l" ? "lte" : "gte",
			value: parsed,
		};
	}
	if (tag === "d") {
		const [mode, decade] = rest.split(FIELD);
		const parsed = Number.parseInt(decade ?? "");
		if ((mode !== "b" && mode !== "p") || !Number.isFinite(parsed)) {
			return undefined;
		}
		return {
			kind: "decade",
			mode: mode === "b" ? "debut" : "played",
			decade: parsed,
		};
	}
	return undefined;
};

// URL-safe base64, so a code survives being pasted into a chat, a URL or a
// spreadsheet without anything mangling a `+`, `/` or `=`.
const toBase64Url = (s: string) =>
	btoa(s).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");

const fromBase64Url = (s: string) => {
	const padded = s.replaceAll("-", "+").replaceAll("_", "/");
	return atob(padded + "=".repeat((4 - (padded.length % 4)) % 4));
};

export const encodeGridCode = (
	rows: GridCodeRef[],
	cols: GridCodeRef[],
): string =>
	toBase64Url(
		[VERSION, ...rows, ...cols]
			.map((r) => (typeof r === "string" ? r : encodeRef(r)))
			.join(REF),
	);

export const decodeGridCode = (
	code: string,
): { rows: GridCodeRef[]; cols: GridCodeRef[] } | undefined => {
	// Tolerate whatever a paste brings with it - spaces, newlines, a stray
	// wrapping quote - rather than making the player clean the code up by hand.
	const cleaned = code.trim().replaceAll(/[\s"']/g, "");
	if (!cleaned) {
		return undefined;
	}
	let raw: string;
	try {
		raw = fromBase64Url(cleaned);
	} catch {
		return undefined;
	}
	const parts = raw.split(REF);
	if (parts[0] !== VERSION || parts.length !== 7) {
		return undefined;
	}
	const refs: GridCodeRef[] = [];
	for (const part of parts.slice(1)) {
		const ref = decodeRef(part);
		if (!ref) {
			return undefined;
		}
		refs.push(ref);
	}
	return { rows: refs.slice(0, 3), cols: refs.slice(3, 6) };
};
