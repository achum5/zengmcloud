// WHO BOTHERS TO POST ABOUT WHAT.
//
// This is the step that decides whether the feed reads like a league or like a
// press release generator, and it is a casting problem rather than a writing
// one. Five hundred accounts times a night's events is thousands of possible
// posts; a day should show thirty to sixty. Which thousands get cut matters far
// more than how well any single one is phrased.
//
// Three rules do most of the work.
//
// PROXIMITY BEATS MAGNITUDE. A Boston beat writer would rather write four
// hundred words about a Celtics loss than one line about a 50-point night in
// Sacramento. A national account is the reverse. So interest is topic weight
// times salience times RELEVANCE, and relevance is what keeps thirty accounts
// from all covering the same marquee game.
//
// NOBODY POSTS TWICE ABOUT THE SAME THING, and almost nobody posts three times
// in a night. Caps are per account and per event, applied before the global
// limit, so a huge game cannot eat the whole day and a chatty account cannot
// drown out a quiet one with something better to say.
//
// THE SAME ACCOUNTS MUST NOT ALWAYS WIN. Pure scoring is stable, which means
// the identical cast covers every night forever - the most obvious way for a
// generated feed to feel mechanical. A seeded jitter breaks that up while
// staying identical on every device, since the feed is derived rather than
// stored.

import { hashSeed, rngFromSeed } from "./phrasePool.ts";
import type { ResolvedSocialAccount } from "./socialAccounts.ts";
import type { SocialEvent } from "./socialEvents.ts";

export type SocialCasting = {
	accountId: string;
	eventId: string;
	// The interest that won the slot, kept so the writer can pitch the post's
	// intensity to how much this account actually cares.
	interest: number;
};

// How much an account cares that this event involves these people, before
// topic and salience are considered.
//
// The numbers are ratios rather than points on purpose: doubling relevance
// should beat a mild topic preference and lose to a large one, which is what
// keeps a homer from covering a game his team was not in while still letting
// the insider break a trade anywhere in the league.
export const relevance = (
	account: ResolvedSocialAccount,
	event: SocialEvent,
): number => {
	// THIS IS ABOUT ME. A player account whose own pid is in the event is the
	// strongest signal there is, and it is what makes five hundred player
	// accounts worth having: each one is silent except about his own nights.
	if (account.pid !== undefined && event.pids.includes(account.pid)) {
		return 6;
	}

	const loyalty = account.personality.loyaltyTid ?? account.tid;
	const rivals = account.personality.rivalTids ?? [];

	if (loyalty !== undefined && event.tids.includes(loyalty)) {
		// A player whose TEAM is involved but who is not himself named cares,
		// but far less than about his own line.
		return account.kind === "player" ? 1.6 : 3;
	}
	if (rivals.some((tid) => event.tids.includes(tid))) {
		return 1.4;
	}

	// No connection. A national account still covers the league; anyone with a
	// team barely looks up. Without this asymmetry every beat writer in the
	// league covers every game, which is both wrong and unreadable.
	if (loyalty === undefined) {
		return 1;
	}
	// A player with no stake at all essentially never posts about it.
	return account.kind === "player" ? 0.04 : 0.15;
};

// Topic weights are relative within an account, so they are normalized against
// that account's own largest weight. Otherwise an account whose weights happen
// to be written small would lose every slot to one written large, which is a
// property of how someone typed the preset rather than of what it means.
export const topicPull = (
	account: ResolvedSocialAccount,
	event: SocialEvent,
): number => {
	const weights = account.personality.topics;
	const max = Math.max(...Object.values(weights));
	if (max <= 0) {
		return 0;
	}
	return (weights[event.topic] ?? 0) / max;
};

export const interest = (
	account: ResolvedSocialAccount,
	event: SocialEvent,
): number =>
	topicPull(account, event) * event.salience * relevance(account, event);

// Below this an account has no business posting, whatever the day looks like.
// Without a floor, a thin night would scrape the barrel and produce posts from
// accounts with no stake in anything that happened - which reads as filler and
// is exactly what the reactive-volume choice was meant to avoid.
const INTEREST_FLOOR = 0.08;

export type CastingLimits = {
	// How many posts the day should end up with, before replies.
	target: number;
	maxPerAccount?: number;
	maxPerEvent?: number;
};

// The jitter's job is to reshuffle who wins among candidates that are ALREADY
// close, never to promote an account with nothing to say over one with a real
// stake. A multiplicative band does that; an additive one would swamp the
// small scores at the bottom, which are precisely the marginal calls.
const JITTER = 0.35;

export const castDay = ({
	accounts,
	events,
	seed,
	limits,
}: {
	accounts: readonly ResolvedSocialAccount[];
	events: readonly SocialEvent[];
	// Stable per day: the same day must cast identically on every device.
	seed: string;
	limits: CastingLimits;
}): SocialCasting[] => {
	const maxPerAccount = limits.maxPerAccount ?? 2;
	const maxPerEvent = limits.maxPerEvent ?? 4;

	type Candidate = SocialCasting & { score: number };
	const candidates: Candidate[] = [];

	for (const account of accounts) {
		const { postiness } = account.personality;
		if (postiness <= 0) {
			continue;
		}
		// One draw per account per day decides whether it shows up at all, so a
		// low-postiness account is quiet for whole nights rather than posting a
		// weak line every single day. Seeded on the account and the day, so it
		// is the same everywhere and different tomorrow.
		const showUpRng = rngFromSeed(hashSeed(`${seed}|show|${account.id}`));
		if (showUpRng() > postiness) {
			continue;
		}

		for (const event of events) {
			const base = interest(account, event);
			if (base < INTEREST_FLOOR) {
				continue;
			}
			const rng = rngFromSeed(hashSeed(`${seed}|${account.id}|${event.id}`));
			const score = base * (1 - JITTER + 2 * JITTER * rng());
			candidates.push({
				accountId: account.id,
				eventId: event.id,
				interest: base,
				score,
			});
		}
	}

	const perAccount = new Map<string, number>();
	const perEvent = new Map<string, number>();
	const out: SocialCasting[] = [];

	candidates.sort(
		(a, b) =>
			b.score - a.score ||
			// Ties break on identity, never on array order, so two devices that
			// enumerated accounts differently still agree. Defensive rather than
			// load-bearing: the jitter is seeded on the account id, so two
			// candidates cannot actually score the same. Kept because the cost
			// is a string compare and the failure it prevents - a feed that
			// differs between league-mates - is invisible until someone
			// screenshots it.
			a.accountId.localeCompare(b.accountId) ||
			a.eventId.localeCompare(b.eventId),
	);

	for (const candidate of candidates) {
		if (out.length >= limits.target) {
			break;
		}
		const byAccount = perAccount.get(candidate.accountId) ?? 0;
		if (byAccount >= maxPerAccount) {
			continue;
		}
		const byEvent = perEvent.get(candidate.eventId) ?? 0;
		if (byEvent >= maxPerEvent) {
			continue;
		}
		perAccount.set(candidate.accountId, byAccount + 1);
		perEvent.set(candidate.eventId, byEvent + 1);
		out.push({
			accountId: candidate.accountId,
			eventId: candidate.eventId,
			interest: candidate.interest,
		});
	}

	return out;
};

// WHO ANSWERS BACK.
//
// Replies are cast against posts rather than events, and the rule is different
// in kind: a reply is about the POSTER as much as the subject. An account
// replies because it disagrees, because the post is about its team, or because
// it has history with whoever wrote it - never because the underlying game was
// close.
//
// Kept separate from castDay because the two must run in order: nothing can
// reply to a post that has not been cast yet, and letting one pass do both
// would make a reply's existence depend on iteration order.
export type SocialReplyCasting = {
	accountId: string;
	// The casting being replied to, identified by both halves since one event
	// can carry several posts.
	parentAccountId: string;
	parentEventId: string;
	kind: "reply" | "quote";
	heat: number;
};

export const replyAppetite = ({
	account,
	poster,
	event,
	// Prior friction between these two accounts, 0 to 1. Derived rather than
	// remembered - see socialFeuds.
	feud,
}: {
	account: ResolvedSocialAccount;
	poster: ResolvedSocialAccount;
	event: SocialEvent;
	feud: number;
}): number => {
	if (account.id === poster.id) {
		return 0;
	}
	const p = account.personality;
	// Interest in the subject sets the ceiling: nobody argues about a game they
	// would not have posted about.
	const subject = interest(account, event);
	if (subject < INTEREST_FLOOR) {
		return 0;
	}

	// How much this account engages with anyone at all. It MULTIPLIES rather
	// than adds, so an account written never to argue stays out of every fight
	// however provoking the post is - an earlier version added the provocation
	// on top, and a wire-service account ended up replying to trolls because
	// they were wrong, which is the one thing a wire service never does.
	const engagement = p.replyiness + p.quotiness;
	if (engagement <= 0) {
		return 0;
	}

	// Somebody being wrong is the reason most replies exist, and the impulse is
	// DIRECTIONAL: it belongs to the account with the higher bar correcting the
	// one with the lower. The reverse, a troll under a wire report, is appetite
	// for a fight rather than a point of order - that is feudiness, below.
	const correcting = Math.max(0, p.accuracy - poster.personality.accuracy);

	// Opposed outlooks on the same event. A doomer under a homer's post is the
	// most reliably entertaining thing a feed like this produces.
	const outlook = Math.abs(p.optimism - poster.personality.optimism) / 2;

	const provocation = correcting * 0.6 + outlook * 0.5 + feud * p.feudiness;

	return engagement * (1 + 2 * provocation) * Math.min(1, subject);
};
