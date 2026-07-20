import { assert, test } from "vitest";
import {
	decodeSyncInvite,
	encodeSyncInvite,
	isValidFirebaseConfig,
	looksLikeSyncInvite,
} from "./syncInvite.ts";
import type { FirebaseConfig } from "./firebaseConfig.ts";

const config: FirebaseConfig = {
	apiKey: "test-api-key",
	authDomain: "example.firebaseapp.com",
	projectId: "example-project",
	storageBucket: "example-project.appspot.com",
	messagingSenderId: "123456789",
	appId: "1:123456789:web:abcdef",
};

test("a plain code decodes to itself with no config", () => {
	const result = decodeSyncInvite("smith-dynasty");
	assert.strictEqual(result.code, "smith-dynasty");
	assert.strictEqual(result.config, undefined);
});

test("a plain code is trimmed", () => {
	assert.strictEqual(
		decodeSyncInvite("  smith-dynasty  ").code,
		"smith-dynasty",
	);
});

test("looksLikeSyncInvite only true for the prefix", () => {
	assert.strictEqual(looksLikeSyncInvite("smith-dynasty"), false);
	assert.strictEqual(
		looksLikeSyncInvite(encodeSyncInvite("room", config)),
		true,
	);
});

test("encode then decode round-trips the code and config", () => {
	const invite = encodeSyncInvite("smith-dynasty", config);
	assert.strictEqual(looksLikeSyncInvite(invite), true);

	const result = decodeSyncInvite(invite);
	assert.strictEqual(result.code, "smith-dynasty");
	assert.deepStrictEqual(result.config, config);
});

test("encode trims the code and rejects an empty one", () => {
	assert.strictEqual(
		decodeSyncInvite(encodeSyncInvite("  room  ", config)).code,
		"room",
	);
	assert.throws(() => encodeSyncInvite("   ", config));
});

test("encode rejects an incomplete config", () => {
	const bad = { ...config, appId: "" };
	assert.throws(() => encodeSyncInvite("room", bad as FirebaseConfig));
});

test("a corrupted invite throws rather than being treated as a plain code", () => {
	assert.throws(() => decodeSyncInvite("zgm1:not-valid-base64!!!"));
});

test("an invite with a missing config field is rejected", () => {
	const { messagingSenderId, ...partial } = config;
	// Hand-build a token with the same encoding but an invalid payload.
	const invite = decodeSyncInvite;
	assert.throws(() =>
		invite(
			"zgm1:" +
				btoa(
					unescape(
						encodeURIComponent(JSON.stringify({ c: "room", f: partial })),
					),
				),
		),
	);
});

test("isValidFirebaseConfig guards the required string fields", () => {
	assert.strictEqual(isValidFirebaseConfig(config), true);
	assert.strictEqual(isValidFirebaseConfig(null), false);
	assert.strictEqual(isValidFirebaseConfig({}), false);
	assert.strictEqual(isValidFirebaseConfig({ ...config, projectId: 5 }), false);
});
