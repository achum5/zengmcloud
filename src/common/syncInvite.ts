// Multiplayer "invite codes" for bringing your own Firestore project.
//
// A shared league lives under leagues/{code} in ONE Firebase project, so
// everyone playing it must point at the same project. To let a host use their
// own Firestore (instead of the built-in project) while keeping joining as
// simple as pasting one string, an invite bundles the room code together with
// the host's Firebase web config (which is public by design - see
// firebaseConfig.ts) into a single opaque token.
//
// A plain room code (no prefix) is returned unchanged with no config, so it
// keeps using the default project exactly as before.

import type { FirebaseConfig } from "./firebaseConfig.ts";

// Versioned so the format can change later without silently mis-parsing old
// tokens. Bump the number if the payload shape changes.
const INVITE_PREFIX = "zgm1:";

const REQUIRED_KEYS: (keyof FirebaseConfig)[] = [
	"apiKey",
	"authDomain",
	"projectId",
	"storageBucket",
	"messagingSenderId",
	"appId",
];

export const isValidFirebaseConfig = (x: unknown): x is FirebaseConfig => {
	if (typeof x !== "object" || x === null) {
		return false;
	}
	const obj = x as Record<string, unknown>;
	return REQUIRED_KEYS.every(
		(key) => typeof obj[key] === "string" && obj[key] !== "",
	);
};

// UTF-8-safe base64, working in both window and worker contexts (btoa/atob only
// handle Latin-1 directly).
const toBase64 = (s: string): string => {
	const bytes = new TextEncoder().encode(s);
	let binary = "";
	for (const b of bytes) {
		binary += String.fromCharCode(b);
	}
	return btoa(binary);
};

const fromBase64 = (s: string): string => {
	const binary = atob(s);
	const bytes = Uint8Array.from(binary, (ch) => ch.charCodeAt(0));
	return new TextDecoder().decode(bytes);
};

export const looksLikeSyncInvite = (input: string): boolean =>
	input.trim().startsWith(INVITE_PREFIX);

export const encodeSyncInvite = (
	code: string,
	config: FirebaseConfig,
): string => {
	const trimmed = code.trim();
	if (!trimmed) {
		throw new Error("A league code is required.");
	}
	if (!isValidFirebaseConfig(config)) {
		throw new Error("Invalid Firebase config.");
	}
	return INVITE_PREFIX + toBase64(JSON.stringify({ c: trimmed, f: config }));
};

// Turn a pasted string into a room code plus (for an invite) the project config.
// A plain code returns { code } with no config - the default project. A
// malformed invite throws, so a corrupted paste is reported rather than silently
// treated as a (nonexistent) plain room code.
export const decodeSyncInvite = (
	input: string,
): { code: string; config?: FirebaseConfig } => {
	const trimmed = input.trim();
	if (!trimmed.startsWith(INVITE_PREFIX)) {
		return { code: trimmed };
	}

	let parsed: unknown;
	try {
		parsed = JSON.parse(fromBase64(trimmed.slice(INVITE_PREFIX.length)));
	} catch {
		throw new Error("This invite code is invalid or corrupted.");
	}

	const obj = parsed as { c?: unknown; f?: unknown };
	if (
		typeof obj.c !== "string" ||
		obj.c.trim() === "" ||
		!isValidFirebaseConfig(obj.f)
	) {
		throw new Error("This invite code is invalid or corrupted.");
	}

	return { code: obj.c.trim(), config: obj.f };
};
