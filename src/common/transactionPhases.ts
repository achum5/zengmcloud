import { PHASE } from "./constants.ts";
import { tradesBlocked } from "./tradesBlocked.ts";
import type { Phase } from "./types.ts";

// tradesAfterDeadline: God Mode reopened trading after the deadline
export const canTradeInPhase = (phase: Phase, tradesAfterDeadline?: boolean) =>
	phase >= 0 && !tradesBlocked(phase, tradesAfterDeadline);

// Re-signing your own players is handled separately
export const canSignFreeAgentsInPhase = (phase: Phase) =>
	phase >= 0 &&
	!(phase > PHASE.AFTER_TRADE_DEADLINE && phase <= PHASE.RESIGN_PLAYERS);

export type TradeBlockedReason =
	| "spectator"
	| "challengeNoTrades"
	| "otherTeamsWantToHire"
	| "phase";

// Why the user can't trade right now, or undefined if they can
export const getTradeBlockedReason = ({
	challengeNoTrades,
	gameOver,
	otherTeamsWantToHire,
	phase,
	spectator,
	tradesAfterDeadline,
}: {
	challengeNoTrades: boolean;
	gameOver: boolean;
	otherTeamsWantToHire: boolean;
	phase: Phase;
	spectator: boolean;
	tradesAfterDeadline: boolean | undefined;
}): TradeBlockedReason | undefined => {
	if (spectator) {
		return "spectator";
	}

	if (challengeNoTrades) {
		return "challengeNoTrades";
	}

	if (otherTeamsWantToHire) {
		return "otherTeamsWantToHire";
	}

	if (!canTradeInPhase(phase, tradesAfterDeadline) || gameOver) {
		return "phase";
	}
};
