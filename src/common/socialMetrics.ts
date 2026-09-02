// THE FURNITURE OF A TIMELINE.
//
// A post is more than its sentence. It has a clock time, it has like and reply
// counts, and some accounts have a checkmark. None of that changes what was
// said, and all of it is most of what makes a screenshot look real - a feed of
// bare text with no numbers on it reads as a transcript, not as a timeline.
//
// Every value here is DERIVED and stored nowhere: the same post computes the
// same time and the same counts on every device, from the account, the event
// and the day. They are decoration with a rule, which is the only kind of
// decoration that survives being looked at twice.

import { hashSeed, rngFromSeed } from "./phrasePool.ts";
import type { ResolvedSocialAccount } from "./socialAccounts.ts";

// WHAT AN ACCOUNT'S PICTURE IS. Derived, never uploaded: a player shows the
// face the league already generated for him, a franchise shows its logo, and
// everyone else gets a monogram tinted with their team's colour. Lives here
// rather than in the worker so the UI can name the type without reaching
// across the boundary.
export type AccountPicture = {
	// A facesjs config. Typed loosely on purpose - this module is pure and
	// has no business depending on the face library.
	face?: unknown;
	imgURL?: string;
	jersey?: string;
	colors?: [string, string, string];
	logoURL?: string;
};

// ---------------------------------------------------------------- REACH
//
// How many people would see this account at all. Not stored, not a real
// follower graph - a plausible number that is STABLE for an account and
// ordered the way it should be: national media above local media above a
// franchise's own account above a star player above a fan.

const BASE_REACH: Record<string, number> = {
	insider: 2_400_000,
	nationalPundit: 1_100_000,
	aggregator: 900_000,
	analytics: 320_000,
	capNerd: 210_000,
	draftHead: 260_000,
	historian: 180_000,
	beatWriter: 140_000,
	localRadio: 95_000,
	teamOfficial: 1_800_000,
	troll: 47_000,
	homerFan: 22_000,
	doomerFan: 18_000,
	player: 400_000,
};

// A player's reach follows his standing in the league, which the account
// already carries as postiness and optimism cannot - so it is passed in.
export const reachOf = (
	account: ResolvedSocialAccount,
	// 0 to 1, how good/notable the player is. Ignored for non-players.
	notability = 0.5,
): number => {
	const base = BASE_REACH[account.archetypeId] ?? 60_000;
	// A stable per-account multiplier between about 0.45 and 2.2, so two beat
	// writers are not the same size.
	const rng = rngFromSeed(hashSeed(`reach|${account.id}`));
	rng();
	const spread = 0.45 + rng() * 1.75;
	const scale =
		account.kind === "player"
			? // Stars are orders of magnitude above end-of-bench, which is the
				// single most recognisable thing about a real sports timeline.
				0.02 + notability ** 3 * 4
			: 1;
	return Math.round(base * spread * scale);
};

export const formatReach = (n: number): string => {
	if (n >= 1_000_000) {
		const m = n / 1_000_000;
		return `${m >= 10 ? Math.round(m) : m.toFixed(1)}M`;
	}
	if (n >= 1000) {
		const k = n / 1000;
		return `${k >= 10 ? Math.round(k) : k.toFixed(1)}K`;
	}
	return String(n);
};

// A checkmark goes to the accounts an institution would verify: the media
// outlets, the franchises, and the players. Not the fan accounts, which is
// exactly the distinction that makes the badge mean anything.
const UNVERIFIED = new Set(["homerFan", "doomerFan", "troll"]);
export const isVerified = (account: ResolvedSocialAccount): boolean =>
	!UNVERIFIED.has(account.archetypeId);

// ---------------------------------------------------------------- ENGAGEMENT
//
// Likes and replies, from reach and from how big the moment was. The shape
// that matters is the SKEW: most posts do nothing, and one in a while a post
// about a real moment does ten times the account's normal numbers. A feed
// where every post has similar counts looks generated at a glance.

export type Engagement = {
	likes: number;
	reposts: number;
	replies: number;
};

export const engagementFor = ({
	account,
	reach,
	salience,
	seed,
	// True when this is a reply rather than a post: answers get a fraction of
	// the attention the thing they answer does.
	isReply = false,
	// The parent post's likes. A big account replying to a small one really
	// does out-draw it, so this is a ceiling rather than parity - but without
	// one, a star's reply showed forty times the numbers of the post it was
	// answering, which reads as two unrelated posts rather than as a thread.
	parentLikes,
}: {
	account: ResolvedSocialAccount;
	reach: number;
	// 0 to 1, how big a deal the event was.
	salience: number;
	seed: string;
	isReply?: boolean;
	parentLikes?: number;
}): Engagement => {
	const rng = rngFromSeed(hashSeed(`eng|${seed}`));
	rng();

	// A long tail rather than a bell: rng^3 puts most posts near the floor and
	// lets a few run away, which is how engagement actually distributes.
	const luck = 0.25 + rng() ** 3 * 6;
	const base = reach * 0.004 * (0.4 + salience) * luck * (isReply ? 0.12 : 1);

	let likes = Math.max(0, Math.round(base));
	if (isReply && parentLikes !== undefined) {
		likes = Math.min(likes, Math.max(3, Math.round(parentLikes * 3)));
	}

	// Ratios drift by account: a troll gets argued with, a wire service does
	// not. Reply-heavy is the "ratio", and it belongs to the loud accounts.
	const argumentative =
		0.04 +
		account.personality.replyiness * 0.16 +
		(1 - account.personality.accuracy) * 0.1;
	return {
		likes,
		// Reposts are the rarest of the three: people like far more than they
		// pass along, and the first numbers had a franchise reposted nine
		// times for every reply it drew.
		reposts: Math.round(likes * (0.03 + rng() * 0.07)),
		// An answer in a thread does not itself collect a thread.
		replies: Math.round(
			likes * argumentative * (0.3 + rng()) * (isReply ? 0.3 : 1),
		),
	};
};

export const formatCount = (n: number): string => {
	if (n >= 1_000_000) {
		return `${(n / 1_000_000).toFixed(1)}M`;
	}
	if (n >= 10_000) {
		return `${Math.round(n / 1000)}K`;
	}
	if (n >= 1000) {
		return `${(n / 1000).toFixed(1)}K`;
	}
	return String(n);
};

// ---------------------------------------------------------------- THE CLOCK
//
// A game night runs from tip to about two hours after the last final, and the
// news of a day is scattered across the working hours before it. Posts about
// one game cluster tightly right after it, which is what a real timeline looks
// like: a wall of posts at 22:40 and almost nothing at 15:00.

export type PostTime = {
	// Minutes since midnight.
	minutes: number;
	label: string;
};

const pad = (n: number) => String(n).padStart(2, "0");

export const timeOf = ({
	// Position of the event within the day, ascending, as trimDayEvents left it.
	eventIndex,
	eventCount,
	// A game gets an evening slot; league news gets the working day before it.
	isGame,
	seed,
}: {
	eventIndex: number;
	eventCount: number;
	isGame: boolean;
	seed: string;
}): PostTime => {
	const rng = rngFromSeed(hashSeed(`time|${seed}`));
	rng();

	let minutes: number;
	if (isGame) {
		// A slate runs across the evening rather than all at once: the early
		// game is final around 21:45 and the late one closer to midnight, and
		// posts about each land within the hour after it. The first attempt
		// put every post between 22:45 and midnight, which read as one
		// enormous simultaneous event.
		const share = eventCount > 1 ? eventIndex / (eventCount - 1) : 0.5;
		const finish = 19 * 60 + 45 + share * 190;
		minutes = Math.round(finish + rng() ** 0.8 * 65);
	} else {
		// News lands during the day, weighted towards the afternoon.
		minutes = Math.round(9 * 60 + rng() ** 0.7 * 9 * 60);
	}
	minutes = Math.min(minutes, 24 * 60 - 1);

	const h = Math.floor(minutes / 60);
	const m = minutes % 60;
	const suffix = h >= 12 ? "PM" : "AM";
	const h12 = h % 12 === 0 ? 12 : h % 12;
	return { minutes, label: `${h12}:${pad(m)} ${suffix}` };
};
