import { assert, test } from "vitest";
import {
	getActiveFirebaseConfig,
	getFirebaseApp,
	setActiveFirebaseConfig,
} from "./firebaseApp.ts";
import { firebaseConfig } from "./firebaseConfig.ts";

test("with no custom config, uses the built-in project's default app", () => {
	setActiveFirebaseConfig(undefined);
	assert.strictEqual(getActiveFirebaseConfig(), firebaseConfig);

	const app1 = getFirebaseApp();
	const app2 = getFirebaseApp();
	// Same instance reused (memoized), unnamed default app, built-in project.
	assert.strictEqual(app1, app2);
	assert.strictEqual(app1.name, "[DEFAULT]");
	assert.strictEqual(app1.options.projectId, firebaseConfig.projectId);
});

test("a custom config gets a distinct named app; reset returns to default", () => {
	const custom = {
		apiKey: "k",
		authDomain: "d",
		projectId: "custom-project-xyz",
		storageBucket: "b",
		messagingSenderId: "m",
		appId: "a",
	};

	setActiveFirebaseConfig(custom);
	const customApp = getFirebaseApp();
	assert.strictEqual(customApp.options.projectId, "custom-project-xyz");
	assert.notStrictEqual(customApp.name, "[DEFAULT]");
	// Same custom config reuses the same app.
	assert.strictEqual(getFirebaseApp(), customApp);

	setActiveFirebaseConfig(undefined);
	const backToDefault = getFirebaseApp();
	assert.strictEqual(backToDefault.name, "[DEFAULT]");
	assert.strictEqual(backToDefault.options.projectId, firebaseConfig.projectId);
	assert.notStrictEqual(backToDefault, customApp);
});
