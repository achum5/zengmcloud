import { afterEach, assert, describe, test } from "vitest";
import { setSyncEngine } from "../sync/engineHolder.ts";
import { smartDecidesHere } from "./decidesHere.ts";

// Only one device makes the CPU's moves in a shared league (docs/smart-cpu.md,
// shared leagues): a league mate's last draft pick or own game that ends a day
// runs on their phone, and the CPU's trades and signings wait for the device
// that decides.
describe("smartDecidesHere", () => {
	afterEach(() => {
		setSyncEngine(undefined);
	});

	const engine = (authority: boolean, advance: boolean) =>
		({
			isAuthority: () => authority,
			inAdvance: () => advance,
		}) as any;

	test("single player decides", () => {
		setSyncEngine(undefined);
		assert.isTrue(smartDecidesHere());
	});

	test("the device in charge of simming decides", () => {
		setSyncEngine(engine(true, false));
		assert.isTrue(smartDecidesHere());
	});

	test("a league mate's device running a step it claimed decides", () => {
		setSyncEngine(engine(false, true));
		assert.isTrue(smartDecidesHere());
	});

	test("a league mate's own pick or own game does not", () => {
		setSyncEngine(engine(false, false));
		assert.isFalse(smartDecidesHere());
	});
});
