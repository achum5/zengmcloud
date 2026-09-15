import { assert, describe, test } from "vitest";
import { consoleUrls, parseFirebaseConfig } from "./parseFirebaseConfig.ts";

// What the Firebase console's Copy button actually puts on the clipboard.
const CONSOLE_SNIPPET = `// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
// TODO: Add SDKs for Firebase products that you want to use

const firebaseConfig = {
  apiKey: "AIzaSyCUvEh1yMuJ1aq-LfZHVI_ty7MOb64CXuE",
  authDomain: "my-league.firebaseapp.com",
  projectId: "my-league",
  storageBucket: "my-league.firebasestorage.app",
  messagingSenderId: "446695548992",
  appId: "1:446695548992:web:3be29f048e582ebd6457a5"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);`;

describe("parseFirebaseConfig", () => {
	test("reads the console's snippet exactly as it is copied", () => {
		const result = parseFirebaseConfig(CONSOLE_SNIPPET);
		assert.ok(result.ok, result.ok ? "" : result.error);
		assert.deepStrictEqual(result.config, {
			apiKey: "AIzaSyCUvEh1yMuJ1aq-LfZHVI_ty7MOb64CXuE",
			authDomain: "my-league.firebaseapp.com",
			projectId: "my-league",
			storageBucket: "my-league.firebasestorage.app",
			messagingSenderId: "446695548992",
			appId: "1:446695548992:web:3be29f048e582ebd6457a5",
		});
	});

	test("reads plain JSON too", () => {
		const json = JSON.stringify({
			apiKey: "k",
			authDomain: "d",
			projectId: "p",
			storageBucket: "b",
			messagingSenderId: "m",
			appId: "a",
		});
		const result = parseFirebaseConfig(json);
		assert.ok(result.ok);
		assert.strictEqual(result.config.projectId, "p");
	});

	test("an extra field the console sometimes adds is ignored", () => {
		const result = parseFirebaseConfig(
			CONSOLE_SNIPPET.replace(
				'appId: "1:446695548992:web:3be29f048e582ebd6457a5"',
				'appId: "1:446695548992:web:3be29f048e582ebd6457a5",\n  measurementId: "G-ABC123"',
			),
		);
		assert.ok(result.ok);
		assert.strictEqual(
			Object.keys(result.config).includes("measurementId"),
			false,
		);
	});

	// The dangerous paste. This one IS a downloadable file, so it is the thing a
	// person hunting for "my Firebase config file" finds first - and it is a
	// private key that bypasses every security rule.
	test("a service account key is refused, and said so by name", () => {
		const serviceAccount = JSON.stringify({
			type: "service_account",
			project_id: "my-league",
			private_key_id: "abc",
			private_key:
				"-----BEGIN PRIVATE KEY-----\nMIIE…\n-----END PRIVATE KEY-----\n",
			client_email: "firebase-adminsdk@my-league.iam.gserviceaccount.com",
		});
		const result = parseFirebaseConfig(serviceAccount);
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.isServiceAccount);
		assert.ok(!result.ok && result.error.includes("service account"));
	});

	test("half a config names what is missing", () => {
		const result = parseFirebaseConfig(`{
			"apiKey": "k",
			"projectId": "p"
		}`);
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.error.includes("authDomain"));
		assert.ok(!result.ok && result.error.includes("appId"));
	});

	test("something that is not a config at all says so", () => {
		const result = parseFirebaseConfig("hello world");
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.error.includes("Couldn't find"));
	});

	test("empty input asks for the config rather than erroring", () => {
		const result = parseFirebaseConfig("   ");
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.error.includes("Paste"));
	});

	// A field present but blank is not a field.
	test("a blank value counts as missing", () => {
		const result = parseFirebaseConfig(
			CONSOLE_SNIPPET.replace('projectId: "my-league"', 'projectId: ""'),
		);
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.error.includes("projectId"));
	});
});

describe("consoleUrls", () => {
	test("each link lands on the page that fixes its step", () => {
		const urls = consoleUrls("my-league");
		assert.ok(urls.auth.includes("/my-league/authentication/providers"));
		assert.ok(urls.rules.includes("/my-league/firestore/rules"));
		assert.ok(urls.firestore.endsWith("/my-league/firestore"));
	});
});
