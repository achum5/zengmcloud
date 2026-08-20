import { PHASE } from "../../common/constants.ts";
import type { MpPhaseReady, Option } from "../../common/types.ts";

// WHEN A PLAY-MENU CLICK NEEDS A SECOND LOOK.
//
// In a shared league the Play menu sits a few pixels from the ready-up control,
// and both of them look like "the button that moves things along". They are not
// the same: readying up says "I'm done, go when everyone else is", while the
// Play menu RUNS it, now, for every team in the league, with no way back. On
// autopilot that is one hover apart, which is how a room ends up in the
// re-signing phase because someone meant to press Ready.
//
// So the Play menu asks first - but only where a wrong click actually costs
// something. Solo (unsynced) leagues and plain navigation items are untouched,
// and so is simming INSIDE a phase (a playoff day, a regular-season week) when
// nobody is waiting on anybody.

// Items that move the whole league from one phase to the next. Irreversible for
// everyone, in every phase, ready-up or no ready-up.
const PHASE_ADVANCE_IDS = new Set([
	"untilRegularSeason",
	"untilPlayoffs",
	"throughPlayoffs",
	"untilDraft",
	"untilResignPlayers",
	"untilFreeAgency",
	"untilPreseason",
]);

// Items that ARE a step of the current ready-up stage - the exact thing the
// room is waiting on each other for, so running one by hand steps over whoever
// has not readied up yet. Only the stages whose steps live in the Play menu
// appear here: the draft advances a pick at a time, free agency a day at a
// time. Preseason, the lottery and re-signing are single-step stages whose step
// IS a phase advance, already covered above.
//
// The regular season is here because of sim stops. It is only ever a gated
// stage while one is pending - the trade deadline, or a day the league has
// asked to pause before - and every sim item then runs straight into it. This
// dialog IS the way past: confirming it grants the one-shot permission that
// lets that sim cross (see tradeDeadlineGate). Without that the room can be
// stranded by one person who never readies up, which is a worse failure than
// somebody simming a day early.
const GATED_STEP_IDS: Partial<Record<number, Set<string>>> = {
	[PHASE.DRAFT]: new Set(["onePick", "untilYourNextPick", "untilEnd"]),
	[PHASE.FREE_AGENCY]: new Set(["day", "week"]),
	[PHASE.REGULAR_SEASON]: new Set([
		"day",
		"dayLive",
		"week",
		"month",
		"untilAllStarGame",
		"untilTradeDeadline",
		"untilPlayoffs",
	]),
};

export type PlayMenuAdvanceWarning =
	| {
			// A ready-up is running and somebody has not said go yet.
			kind: "notReady";
			action: string;
			readyTeams: number;
			totalTeams: number;
			// Teams yet to ready up, by name.
			notReady: string[];
			// Called out separately from the rest: in the draft, the team on the
			// clock making their pick IS their ready, so they are not a holdout in
			// the way the others are.
			onClock: string[];
	  }
	| {
			// No ready-up covers this phase - nobody is being stepped over - but it
			// still moves the whole league, so it is still worth a look.
			kind: "phaseAdvance";
			action: string;
	  };

export const getPlayMenuAdvanceWarning = ({
	option,
	mpSyncActive,
	mpPhaseReady,
}: {
	option: Pick<Option, "id" | "label" | "url">;
	mpSyncActive: boolean;
	mpPhaseReady: MpPhaseReady | undefined;
}): PlayMenuAdvanceWarning | undefined => {
	// A solo league is nobody else's league, so it keeps its one-click Play menu.
	// A url item just navigates.
	if (!mpSyncActive || option.url !== undefined) {
		return undefined;
	}

	const isPhaseAdvance = PHASE_ADVANCE_IDS.has(option.id);

	if (mpPhaseReady) {
		// Everyone is ready, so the evaluator is about to run this step of its own
		// accord. Clicking it by hand changes nothing and is not the mistake this
		// is here to catch.
		if (mpPhaseReady.readyTeams >= mpPhaseReady.totalTeams) {
			return undefined;
		}

		if (
			!isPhaseAdvance &&
			!GATED_STEP_IDS[mpPhaseReady.phase]?.has(option.id)
		) {
			return undefined;
		}

		const teams = mpPhaseReady.teams ?? [];
		return {
			kind: "notReady",
			action: option.label,
			readyTeams: mpPhaseReady.readyTeams,
			totalTeams: mpPhaseReady.totalTeams,
			notReady: teams.filter((t) => !t.ready && !t.onClock).map((t) => t.name),
			onClock: teams.filter((t) => !t.ready && t.onClock).map((t) => t.name),
		};
	}

	if (!isPhaseAdvance) {
		return undefined;
	}

	return {
		kind: "phaseAdvance",
		action: option.label,
	};
};
