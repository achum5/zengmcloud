import type { CollegeProfile, CollegeTalks } from "../../../common/college.ts";
import { helpers } from "../../util/index.ts";

// NIL NEGOTIATION
//
// The same haggling for recruits, portal players and returning players
// renegotiating. A school only sees a rough range for what a player wants.
// It makes an offer; at or above his number he takes it, otherwise he
// counters. Each counter costs a round of his patience - two for a lowball,
// which also costs interest for good - and a way-low offer ends talks on the
// spot. Out of patience, his last counter is final: meet it or lose him.
//
// Patience depends on his personality, how much he likes the school, and how
// many other schools have money on the table.

export const roundNil = (amount: number) =>
	amount >= 100 ? Math.round(amount / 5) * 5 : Math.max(0, Math.round(amount));

// The range a school sees around a player's true number. Not centered, so the
// middle isn't his number either.
export const nilRange = (ask: number): [number, number] => [
	roundNil(ask * (0.75 + 0.15 * Math.random())),
	Math.max(1, roundNil(ask * (1.1 + 0.15 * Math.random()))),
];

const WALK_AWAY = 0.6;
const LOWBALL = 0.85;

export const openTalks = (
	profile: CollegeProfile | undefined,
	interest: number,
	rivalOffers: number,
): CollegeTalks => {
	let patience = profile?.patience ?? 3;
	if (interest >= 85) {
		patience += 2;
	} else if (interest >= 70) {
		patience += 1;
	}
	if (rivalOffers >= 2) {
		patience -= 1;
	}
	return { patience: helpers.bound(patience, 1, 7), penalty: 0 };
};

export type OfferOutcome =
	| { type: "accepted"; amount: number }
	| { type: "countered"; counter: number; final: boolean; lowball: boolean }
	| { type: "walked" };

// Mutates talks.
export const respondToOffer = (
	talks: CollegeTalks,
	ask: number,
	amount: number,
): OfferOutcome => {
	if (talks.walked) {
		return { type: "walked" };
	}
	if (
		amount >= ask ||
		(talks.counter !== undefined && amount >= talks.counter)
	) {
		delete talks.counter;
		return { type: "accepted", amount };
	}

	const ratio = amount / Math.max(1, ask);
	const final = talks.counter !== undefined && talks.patience <= 0;
	const lowball = ratio < LOWBALL;
	if (ratio < WALK_AWAY || final || (lowball && talks.patience <= 1)) {
		talks.walked = true;
		talks.penalty += 10;
		delete talks.counter;
		return { type: "walked" };
	}

	talks.patience -= lowball ? 2 : 1;
	if (lowball) {
		talks.penalty += 3;
	}
	// Lowballers get a stiffer counter.
	const counter = roundNil(
		ask * (lowball ? 1.06 + 0.06 * Math.random() : 1 + 0.05 * Math.random()),
	);
	talks.counter = Math.max(counter, Math.ceil(ask));
	return {
		type: "countered",
		counter: talks.counter,
		final: talks.patience <= 0,
		lowball,
	};
};
