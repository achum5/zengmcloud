import { assert, describe, test } from "vitest";
import { isIOSUserAgent } from "./Modal.tsx";

// A few representative user agents. maxTouchPoints matters only for iPadOS,
// which reports a desktop-Mac UA and is otherwise indistinguishable.
const UA = {
	iphone:
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
	ipadLegacy:
		"Mozilla/5.0 (iPad; CPU OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1",
	ipadOS:
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
	mac: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
	androidChrome:
		"Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36",
	windows:
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
};

describe("isIOSUserAgent", () => {
	test("iPhone is iOS", () => {
		assert.strictEqual(isIOSUserAgent(UA.iphone, 5), true);
	});

	test("legacy iPad (real iPad UA) is iOS", () => {
		assert.strictEqual(isIOSUserAgent(UA.ipadLegacy, 5), true);
	});

	test("iPadOS masquerading as Mac counts as iOS only when it has a touch screen", () => {
		// The whole reason the maxTouchPoints branch exists.
		assert.strictEqual(isIOSUserAgent(UA.ipadOS, 5), true);
		assert.strictEqual(isIOSUserAgent(UA.ipadOS, 0), false);
	});

	test("a real desktop Mac (no touch) is NOT iOS", () => {
		assert.strictEqual(isIOSUserAgent(UA.mac, 0), false);
	});

	test("Android and Windows are not iOS, even with touch points", () => {
		assert.strictEqual(isIOSUserAgent(UA.androidChrome, 5), false);
		assert.strictEqual(isIOSUserAgent(UA.windows, 10), false);
	});
});
