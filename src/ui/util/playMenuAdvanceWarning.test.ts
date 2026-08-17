import { assert, describe, test } from "vitest";
import { PHASE } from "../../common/constants.ts";
import {
	getPlayMenuAdvanceWarning,
	type PlayMenuAdvanceWarning,
} from "./playMenuAdvanceWarning.ts";
import type { MpPhaseReady } from "../../common/types.ts";

// vitest's chai `assert` doesn't narrow a union, so the kind check that lets the
// tests read the fields off a warning is a throw rather than an assert.
const ofKind = <Kind extends PlayMenuAdvanceWarning["kind"]>(
	warning: PlayMenuAdvanceWarning | undefined,
	kind: Kind,
): Extract<PlayMenuAdvanceWarning, { kind: Kind }> => {
	if (warning?.kind !== kind) {
		throw new Error(`Expected a ${kind} warning, got ${warning?.kind}`);
	}
	return warning as Extract<PlayMenuAdvanceWarning, { kind: Kind }>;
};

const readyState = ({
	phase,
	readyTeams,
	totalTeams,
	teams,
}: {
	phase: number;
	readyTeams: number;
	totalTeams: number;
	teams?: MpPhaseReady["teams"];
}): MpPhaseReady => ({
	phase,
	readyTeams,
	totalTeams,
	ready: false,
	myUntilStep: undefined,
	nextStep: { number: 1, label: "Next" },
	onClockUser: false,
	waypoints: [],
	options: [],
	teams: teams ?? [],
});

const warn = (
	id: string,
	mpPhaseReady: MpPhaseReady | undefined,
	{ mpSyncActive = true, url }: { mpSyncActive?: boolean; url?: string } = {},
) =>
	getPlayMenuAdvanceWarning({
		option: { id, label: id, url },
		mpSyncActive,
		mpPhaseReady,
	});

describe("no warning at all", () => {
	test("solo league - every Play menu item stays one click", () => {
		assert.isUndefined(
			warn("untilResignPlayers", undefined, { mpSyncActive: false }),
		);
		assert.isUndefined(
			warn(
				"untilFreeAgency",
				readyState({
					phase: PHASE.RESIGN_PLAYERS,
					readyTeams: 0,
					totalTeams: 3,
				}),
				{ mpSyncActive: false },
			),
		);
	});

	test("a url item only navigates", () => {
		assert.isUndefined(warn("viewDraft", undefined, { url: "/l/1/draft" }));
	});

	test("everyone is ready - the room advances on its own anyway", () => {
		assert.isUndefined(
			warn(
				"untilFreeAgency",
				readyState({
					phase: PHASE.RESIGN_PLAYERS,
					readyTeams: 3,
					totalTeams: 3,
				}),
			),
		);
		assert.isUndefined(
			warn(
				"onePick",
				readyState({ phase: PHASE.DRAFT, readyTeams: 2, totalTeams: 2 }),
			),
		);
	});

	test("simming inside a phase is nobody else's business", () => {
		// Playoffs and the regular season have no ready-up, and a day/week/month
		// does not leave the phase.
		for (const id of ["day", "week", "month", "untilEndOfRound", "dayLive"]) {
			assert.isUndefined(warn(id, undefined), id);
		}
	});

	test("regular season at the trade deadline - simming cannot cross it", () => {
		// The deadline arms a ready-up, but the ordinary sim path refuses to cross
		// (tradeDeadlineGate), so a day/week is not stepping over anyone.
		const atDeadline = readyState({
			phase: PHASE.REGULAR_SEASON,
			readyTeams: 1,
			totalTeams: 3,
		});
		assert.isUndefined(warn("day", atDeadline));
		assert.isUndefined(warn("week", atDeadline));
		assert.isUndefined(warn("untilTradeDeadline", atDeadline));
	});
});

describe("not everyone is readied up", () => {
	test("the accident: re-signing while the room is mid ready-up", () => {
		const warning = ofKind(
			warn(
				"untilResignPlayers",
				readyState({
					phase: PHASE.DRAFT_LOTTERY,
					readyTeams: 1,
					totalTeams: 3,
					teams: [
						{ tid: 0, name: "Boston", ready: true, onClock: false },
						{ tid: 1, name: "Sacramento", ready: false, onClock: false },
						{ tid: 2, name: "Phoenix", ready: false, onClock: false },
					],
				}),
			),
			"notReady",
		);
		assert.strictEqual(warning.readyTeams, 1);
		assert.strictEqual(warning.totalTeams, 3);
		assert.deepStrictEqual(warning.notReady, ["Sacramento", "Phoenix"]);
		assert.deepStrictEqual(warning.onClock, []);
	});

	test("a draft pick is a step of the stage", () => {
		const state = readyState({
			phase: PHASE.DRAFT,
			readyTeams: 1,
			totalTeams: 3,
			teams: [
				{ tid: 0, name: "Boston", ready: true, onClock: false },
				{ tid: 1, name: "Sacramento", ready: false, onClock: true },
				{ tid: 2, name: "Phoenix", ready: false, onClock: false },
			],
		});
		for (const id of ["onePick", "untilYourNextPick", "untilEnd"]) {
			assert.strictEqual(warn(id, state)?.kind, "notReady", id);
		}

		// The team picking is not a holdout - making the pick IS their ready.
		const warning = ofKind(warn("onePick", state), "notReady");
		assert.deepStrictEqual(warning.notReady, ["Phoenix"]);
		assert.deepStrictEqual(warning.onClock, ["Sacramento"]);
	});

	test("a free-agency day is a step of the stage", () => {
		const state = readyState({
			phase: PHASE.FREE_AGENCY,
			readyTeams: 2,
			totalTeams: 3,
		});
		assert.strictEqual(warn("day", state)?.kind, "notReady");
		assert.strictEqual(warn("week", state)?.kind, "notReady");
		// Leaving free agency entirely is a phase advance, so it warns too.
		assert.strictEqual(warn("untilPreseason", state)?.kind, "notReady");
	});

	test("a phase advance warns in any gated stage", () => {
		assert.strictEqual(
			warn(
				"untilRegularSeason",
				readyState({ phase: PHASE.PRESEASON, readyTeams: 0, totalTeams: 2 }),
			)?.kind,
			"notReady",
		);
		assert.strictEqual(
			warn(
				"untilFreeAgency",
				readyState({
					phase: PHASE.RESIGN_PLAYERS,
					readyTeams: 1,
					totalTeams: 2,
				}),
			)?.kind,
			"notReady",
		);
		assert.strictEqual(
			warn(
				"untilDraft",
				readyState({
					phase: PHASE.DRAFT_LOTTERY,
					readyTeams: 0,
					totalTeams: 2,
				}),
			)?.kind,
			"notReady",
		);
	});
});

describe("phases with no ready-up", () => {
	test("after the draft, re-signing still moves the whole league", () => {
		const warning = ofKind(
			warn("untilResignPlayers", undefined),
			"phaseAdvance",
		);
		assert.strictEqual(warning.action, "untilResignPlayers");
	});

	test("every other phase advance is covered too", () => {
		for (const id of [
			"untilRegularSeason",
			"untilPlayoffs",
			"throughPlayoffs",
			"untilDraft",
			"untilFreeAgency",
			"untilPreseason",
		]) {
			assert.strictEqual(warn(id, undefined)?.kind, "phaseAdvance", id);
		}
	});

	test("stopping is not advancing", () => {
		assert.isUndefined(warn("stop", undefined));
		assert.isUndefined(warn("stopAuto", undefined));
	});
});
