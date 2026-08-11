import { assert, describe, test } from "vitest";
import {
	appendPhaseForensics,
	formatPhaseForensics,
	MAX_PHASE_FORENSICS,
	type PhaseForensicsEntry,
} from "./phaseForensics.ts";

const entry = (overrides: Partial<PhaseForensicsEntry> = {}) => ({
	at: 1_700_000_000_000,
	lid: 26,
	season: 2007,
	from: 5,
	to: 8,
	source: "playMenu.untilFreeAgency",
	engine: false,
	authority: false,
	...overrides,
});

describe("appendPhaseForensics", () => {
	test("appends in order", () => {
		const out = appendPhaseForensics(
			[entry({ to: 6 })],
			entry({ to: 7 }) as PhaseForensicsEntry,
		);
		assert.strictEqual(out.length, 2);
		assert.strictEqual(out[1]!.to, 7);
	});

	test("stays bounded, dropping the oldest", () => {
		let list: PhaseForensicsEntry[] | undefined;
		for (let i = 0; i < MAX_PHASE_FORENSICS + 10; i++) {
			list = appendPhaseForensics(
				list,
				entry({ at: i }) as PhaseForensicsEntry,
			);
		}
		assert.strictEqual(list!.length, MAX_PHASE_FORENSICS);
		assert.strictEqual(list!.at(-1)!.at, MAX_PHASE_FORENSICS + 9);
		assert.strictEqual(list![0]!.at, 10, "oldest entries fall off the front");
	});

	test("a corrupted prior value is treated as empty, never thrown on", () => {
		const out = appendPhaseForensics(
			"garbage" as unknown as PhaseForensicsEntry[],
			entry() as PhaseForensicsEntry,
		);
		assert.strictEqual(out.length, 1);
	});
});

describe("formatPhaseForensics", () => {
	test("names the transition, the click, and the sync state", () => {
		// The field incident's exact question: WHAT ran the phase forward, and
		// did the device think it was synced at the time?
		const text = formatPhaseForensics([entry() as PhaseForensicsEntry]);
		assert.ok(text.includes("phase 5->8"), text);
		assert.ok(text.includes("source=playMenu.untilFreeAgency"), text);
		assert.ok(text.includes("engine=false"), text);
		assert.ok(text.includes("authority=false"), text);
	});

	test("an empty log says so instead of vanishing", () => {
		assert.ok(formatPhaseForensics(undefined).includes("none recorded"));
		assert.ok(formatPhaseForensics([]).includes("none recorded"));
	});
});
