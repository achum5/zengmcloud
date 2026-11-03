import { assert, describe, test } from "vitest";
import { checkRepairFile, REPAIR_FILE_KIND } from "./applyRepair.ts";

const file = (over: Record<string, unknown> = {}) => ({
	kind: REPAIR_FILE_KIND,
	room: "abc",
	expectedVersion: 10,
	changes: [
		{ store: "players", id: 1, type: "put", value: { pid: 1 } },
		{ store: "events", id: 5, type: "delete" },
	],
	...over,
});

const here = (over: Record<string, unknown> = {}) => ({
	room: "abc",
	applied: 10,
	caughtUp: true,
	inCharge: true,
	...over,
});

describe("checkRepairFile", () => {
	test("made for this room and version, on the device in charge: ok", () => {
		assert.isTrue(checkRepairFile(file(), here()).ok);
	});

	test("not a repair file", () => {
		assert.isFalse(checkRepairFile({ hello: 1 }, here()).ok);
		assert.isFalse(checkRepairFile(undefined, here()).ok);
	});

	test("another room's repair", () => {
		assert.isFalse(checkRepairFile(file(), here({ room: "xyz" })).ok);
	});

	test("the league has moved on since it was made", () => {
		assert.isFalse(checkRepairFile(file(), here({ applied: 11 })).ok);
		assert.isFalse(checkRepairFile(file(), here({ caughtUp: false })).ok);
	});

	test("only the device in charge of simming", () => {
		assert.isFalse(checkRepairFile(file(), here({ inCharge: false })).ok);
	});

	test("stores a repair has no business touching", () => {
		assert.isFalse(
			checkRepairFile(
				file({
					changes: [{ store: "schedule", id: 1, type: "put", value: {} }],
				}),
				here(),
			).ok,
		);
	});
});
