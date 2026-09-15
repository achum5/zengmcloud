import type { FirebaseConfig } from "./firebaseConfig.ts";
import { isValidFirebaseConfig } from "./syncInvite.ts";

// READING WHAT IS ACTUALLY ON THE USER'S CLIPBOARD.
//
// The Firebase console does not offer a web config to download. It shows a
// JavaScript snippet and a Copy button, so what a person pastes here is
// whatever that button gave them:
//
//   const firebaseConfig = {
//     apiKey: "AIza…",
//     authDomain: "my-league.firebaseapp.com",
//     …
//   };
//
// Unquoted keys, trailing semicolon, often the surrounding `import` lines and
// an `initializeApp` call too. That is not JSON, and demanding JSON means
// asking someone to hand-edit a config before the app will look at it - the
// exact kind of friction that makes a setup step feel like a wall. So: pull the
// fields out of whatever was pasted, and validate strictly afterwards.

const FIELDS = [
	"apiKey",
	"authDomain",
	"projectId",
	"storageBucket",
	"messagingSenderId",
	"appId",
] as const;

// A SERVICE ACCOUNT KEY IS NOT THIS.
//
// The console's other downloadable JSON - the one that IS a file, so the one a
// person hunting for "my Firebase config file" finds first - is a service
// account private key. It grants full admin access to the whole project,
// bypassing every security rule. Pasting it into a web app would publish it to
// everyone in the league.
//
// So this is checked before anything else and reported in as many words, rather
// than failing as "invalid config" and leaving someone to try harder.
const SERVICE_ACCOUNT_MARKERS = [
	"private_key",
	"client_email",
	"service_account",
];

export type ParsedFirebaseConfig =
	| { ok: true; config: FirebaseConfig }
	| { ok: false; error: string; isServiceAccount?: true };

const readField = (text: string, field: string): string | undefined => {
	// key: "value" | 'value' | "key": "value" - any quoting the console or a
	// hand-edit might produce.
	const match = new RegExp(
		String.raw`["']?\b${field}\b["']?\s*:\s*["']([^"']*)["']`,
	).exec(text);
	return match?.[1];
};

export const parseFirebaseConfig = (input: string): ParsedFirebaseConfig => {
	const text = input.trim();
	if (text === "") {
		return { ok: false, error: "Paste your Firebase config to continue." };
	}

	if (SERVICE_ACCOUNT_MARKERS.some((marker) => text.includes(marker))) {
		return {
			ok: false,
			isServiceAccount: true,
			error:
				"That's a service account key, which gives full admin access to your project - don't share it with anyone. You want the web config instead: Project settings → General → Your apps → Config.",
		};
	}

	const found: Record<string, string> = {};
	for (const field of FIELDS) {
		const value = readField(text, field);
		if (value !== undefined && value !== "") {
			found[field] = value;
		}
	}

	if (Object.keys(found).length === 0) {
		return {
			ok: false,
			error:
				"Couldn't find a Firebase config in that. Copy the whole snippet from Project settings → General → Your apps → Config.",
		};
	}

	const missing = FIELDS.filter((field) => found[field] === undefined);
	if (missing.length > 0) {
		return {
			ok: false,
			error: `That config is missing ${missing.join(", ")}. Copy the whole snippet, not just part of it.`,
		};
	}

	const config = found as FirebaseConfig;
	if (!isValidFirebaseConfig(config)) {
		return { ok: false, error: "That config isn't usable." };
	}

	return { ok: true, config };
};

// Deep links into the console for the project a config names, so a failing
// setup step goes straight to the page that fixes it instead of describing
// where to click.
export const consoleUrls = (projectId: string) => ({
	auth: `https://console.firebase.google.com/project/${projectId}/authentication/providers`,
	firestore: `https://console.firebase.google.com/project/${projectId}/firestore`,
	rules: `https://console.firebase.google.com/project/${projectId}/firestore/rules`,
});
