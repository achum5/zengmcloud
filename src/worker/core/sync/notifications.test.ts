import { assert, beforeEach, describe, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { PHASE } from "../../../common/constants.ts";
import { g } from "../../util/index.ts";
import { buildNotification } from "./notifications.ts";
import type { Changeset } from "./changeset.ts";

const opts = { isHost: true, authorName: "Alex" };

const playerPut = (pid: number, tid: number): Changeset["changes"][number] => ({
	store: "players",
	id: pid,
	type: "put",
	value: { pid, tid },
});

const phasePut = (phase: number): Changeset["changes"][number] => ({
	store: "gameAttributes",
	id: "phase",
	type: "put",
	value: { key: "phase", value: phase },
});

describe("buildNotification", () => {
	beforeEach(() => {
		resetG();
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
	});

	test("host sim in a playable phase → generic sim notice", () => {
		const notif = buildNotification("playMenu.day", { changes: [] }, opts);
		assert.strictEqual(notif?.title, "Sim complete");
	});

	test("host sim that reaches a human phase → your turn", () => {
		const notif = buildNotification(
			"playMenu.week",
			{ changes: [phasePut(PHASE.DRAFT)] },
			opts,
		);
		assert.strictEqual(notif?.title, "Your league needs you");
	});

	test("non-host never announces a sim", () => {
		const notif = buildNotification(
			"playMenu.day",
			{ changes: [phasePut(PHASE.DRAFT)] },
			{ ...opts, isHost: false },
		);
		assert.strictEqual(notif, undefined);
	});

	test("manual phase advance to a human phase → your turn", () => {
		const notif = buildNotification(
			"main.draftLottery",
			{ changes: [phasePut(PHASE.DRAFT_LOTTERY)] },
			opts,
		);
		assert.strictEqual(notif?.title, "Your league needs you");
	});

	test("players moving to two teams → trade", () => {
		const notif = buildNotification(
			"main.proposeTrade",
			{ changes: [playerPut(1, 4), playerPut(2, 7)] },
			opts,
		);
		assert.strictEqual(notif?.title, "Trade completed");
		assert.ok(notif?.body.includes("Alex"));
	});

	test("a single roster change → roster move", () => {
		const notif = buildNotification(
			"main.signFreeAgent",
			{ changes: [playerPut(1, 4)] },
			opts,
		);
		assert.strictEqual(notif?.title, "Roster move");
	});

	test("a non-roster change → no notification", () => {
		const notif = buildNotification(
			"main.updateGameAttributes",
			{ changes: [{ store: "teams", id: 3, type: "put", value: { tid: 3 } }] },
			opts,
		);
		assert.strictEqual(notif, undefined);
	});
});
