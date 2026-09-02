// WHAT AN ACCOUNT IS LIKE, AS NUMBERS.
//
// The whole feed is derived rather than stored (nothing is written when a day
// sims; a day's posts are recomputed from the league on demand), so an
// account's personality has to be a small, serializable, PURE description that
// the generator can read. It cannot be a function, or a closure, or anything
// that would have to be reconstructed identically on every device.
//
// It is also the thing the user edits, individually or in batches, so every
// field here is something a person can reason about without reading code. That
// rules out opaque weights: each knob is named for the behavior it produces.
//
// ARCHETYPES ARE PRESETS, NOT CLASSES. An account stores an archetype id plus
// a sparse override, and the two are merged at read time. Editing an archetype
// therefore changes every account that inherits it - which is the batch edit -
// while an override changes exactly one account. Neither needs the other.

import type { MoodTrait } from "./types.ts";

// What an account talks about. Relative weights, not probabilities: the
// generator normalizes across whatever topics an actual day produced, so an
// account that only cares about trades stays silent on a quiet night rather
// than posting filler about a game it does not care about.
export type SocialTopicWeights = {
	// A game finished. The bread and butter.
	gameResult: number;
	// One player's line, good or bad.
	playerPerformance: number;
	// Someone got hurt.
	injury: number;
	trade: number;
	freeAgency: number;
	draft: number;
	// Standings movement, streaks, playoff races.
	standings: number;
	awards: number;
	// Career milestones, franchise records, retirements.
	milestone: number;
	// Contracts, cap space, luxury tax.
	money: number;
	// Efficiency numbers, ratings, "actually the box score says".
	analytics: number;
	// Unsourced speculation. Only accounts with low accuracy should carry
	// much of this, since it is where invented claims live.
	rumor: number;
	// Not about the league at all. The thing that makes a feed feel like a
	// feed rather than a wire service - and the thing that goes stale fastest,
	// so most accounts should have very little of it.
	offTopic: number;
};

export type SocialTone =
	| "wire" // Flat, factual, no adjectives. Insiders.
	| "beat" // Professional but human. Beat writers.
	| "hype" // Everything is the greatest thing ever.
	| "snark" // Dry, mocking, quote-posts other people.
	| "doom" // Everything is a disaster, especially good news.
	| "wonk" // Numbers first, sentences second.
	| "corporate" // Team accounts. Exclamation points, no opinions.
	| "unhinged"; // All caps, no punctuation, total conviction.

export type SocialPersonality = {
	topics: SocialTopicWeights;
	tone: SocialTone;

	// ---- VOICE. How the sentence is typed, after it is chosen. -------------
	// 0 writes like a group chat (lowercase, no terminal punctuation); 1 writes
	// like a copy desk. Applied as a post-processing pass so every template is
	// usable by every account rather than needing a variant per voice.
	formality: number;
	// 0 is a fragment, 1 will use two sentences and a subordinate clause.
	verbosity: number;
	// Chance of an emoji riding along.
	emoji: number;
	// Chance of SHOUTING a phrase for emphasis.
	caps: number;
	// Chance of a censored expletive. Off entirely at 0.
	profanity: number;

	// ---- BIAS. Who they are for, and how honest they are. ------------------
	// The team this account is a fan of, if any. Undefined for neutral media.
	// Stored on the account rather than here when it is derived from a player's
	// own team; here is where a user pins it deliberately.
	loyaltyTid?: number;
	rivalTids?: number[];
	// -1 assumes the worst about everything, +1 assumes the best.
	optimism: number;
	// THE HONESTY DIAL, and the most important one for not embarrassing the
	// feature. 1 means every number and claim is checked against the box score
	// before it posts. Below 1 the account may state opinions as fact and
	// speculate; it still may NOT invent numbers, because a feed that
	// misreports a score is broken rather than characterful. See the generator:
	// accuracy gates CLAIMS, never STATS.
	accuracy: number;

	// ---- BEHAVIOR. How much they show up. ---------------------------------
	// Chance of posting at all on a day that gave them something to react to.
	postiness: number;
	// Chance of replying to someone else's post rather than posting fresh.
	replyiness: number;
	// Chance of quote-posting rather than replying.
	quotiness: number;
	// How long they carry a grudge, and how readily they start one.
	feudiness: number;

	// Phrases this account reuses. Deliberately part of the personality rather
	// than the template pool, because a catchphrase is the fastest way for a
	// reader to recognize an account, and the fastest way for a user to make an
	// account theirs.
	catchphrases: string[];
};

// A sparse edit. Both the per-account override and the batch editor produce
// one of these; merging is the only way personality is ever assembled.
export type SocialPersonalityOverride = {
	topics?: Partial<SocialTopicWeights>;
} & Partial<Omit<SocialPersonality, "topics">>;

const NO_TOPICS: SocialTopicWeights = {
	gameResult: 0,
	playerPerformance: 0,
	injury: 0,
	trade: 0,
	freeAgency: 0,
	draft: 0,
	standings: 0,
	awards: 0,
	milestone: 0,
	money: 0,
	analytics: 0,
	rumor: 0,
	offTopic: 0,
};

export const TOPIC_KEYS = Object.keys(
	NO_TOPICS,
) as (keyof SocialTopicWeights)[];

const topics = (weights: Partial<SocialTopicWeights>): SocialTopicWeights => ({
	...NO_TOPICS,
	...weights,
});

// The neutral starting point every archetype is written as a diff from, so a
// new archetype only has to state what makes it different.
export const BASE_PERSONALITY: SocialPersonality = {
	topics: topics({ gameResult: 1, playerPerformance: 1 }),
	tone: "beat",
	formality: 0.7,
	verbosity: 0.5,
	emoji: 0,
	caps: 0,
	profanity: 0,
	optimism: 0,
	accuracy: 1,
	postiness: 0.5,
	replyiness: 0.15,
	quotiness: 0.1,
	feudiness: 0,
	catchphrases: [],
};

export type SocialArchetype = {
	id: string;
	label: string;
	// One line, shown in the editor so a user picking an archetype knows what
	// they are getting without reading the numbers.
	summary: string;
	personality: SocialPersonalityOverride;
};

// The built-in cast. Chosen to cover the shapes a real league timeline has:
// people who BREAK things, people who EXPLAIN things, people who REACT to
// things, and people who exist to be annoying about things. Anything the user
// adds is a copy of one of these with edits.
export const BUILT_IN_ARCHETYPES: SocialArchetype[] = [
	{
		id: "insider",
		label: "Insider",
		summary: "Breaks trades and signings. No opinions, all facts, terse.",
		personality: {
			tone: "wire",
			topics: topics({
				trade: 10,
				freeAgency: 10,
				injury: 6,
				draft: 5,
				money: 3,
				gameResult: 0,
				playerPerformance: 0,
			}),
			formality: 0.95,
			verbosity: 0.35,
			accuracy: 1,
			postiness: 1,
			replyiness: 0,
			quotiness: 0,
			catchphrases: ["Sources tell me", "ESPN Sources:"],
		},
	},
	{
		id: "beatWriter",
		label: "Beat writer",
		summary: "Covers one team every night. Professional, fair, thorough.",
		personality: {
			tone: "beat",
			topics: topics({
				gameResult: 8,
				playerPerformance: 7,
				injury: 6,
				standings: 4,
				trade: 4,
				analytics: 2,
				milestone: 3,
			}),
			formality: 0.85,
			verbosity: 0.75,
			postiness: 0.95,
			replyiness: 0.2,
			accuracy: 1,
		},
	},
	{
		id: "nationalPundit",
		label: "National pundit",
		summary: "Ranks everyone, declares eras over, starts arguments.",
		personality: {
			tone: "snark",
			topics: topics({
				gameResult: 5,
				playerPerformance: 6,
				standings: 6,
				awards: 5,
				trade: 4,
				rumor: 3,
			}),
			formality: 0.6,
			verbosity: 0.6,
			optimism: -0.2,
			accuracy: 0.55,
			postiness: 0.7,
			replyiness: 0.3,
			quotiness: 0.45,
			feudiness: 0.6,
		},
	},
	{
		id: "analytics",
		label: "Analytics account",
		summary: "Posts the efficiency numbers and corrects everyone else.",
		personality: {
			tone: "wonk",
			topics: topics({
				analytics: 10,
				playerPerformance: 6,
				gameResult: 3,
				standings: 4,
				money: 2,
			}),
			formality: 0.8,
			verbosity: 0.6,
			accuracy: 1,
			postiness: 0.6,
			replyiness: 0.5,
			quotiness: 0.5,
			feudiness: 0.3,
		},
	},
	{
		id: "homerFan",
		label: "Homer fan",
		summary:
			"One team can do no wrong. Loud when they win, louder when they lose.",
		personality: {
			tone: "hype",
			topics: topics({
				gameResult: 9,
				playerPerformance: 7,
				standings: 5,
				trade: 4,
				injury: 3,
				offTopic: 2,
			}),
			formality: 0.15,
			verbosity: 0.35,
			emoji: 0.5,
			caps: 0.35,
			optimism: 0.8,
			accuracy: 0.35,
			postiness: 0.85,
			replyiness: 0.4,
			quotiness: 0.35,
			feudiness: 0.5,
		},
	},
	{
		id: "doomerFan",
		label: "Doomer fan",
		summary: "Same team, opposite wiring. A win is a fluke, a loss is proof.",
		personality: {
			tone: "doom",
			topics: topics({
				gameResult: 9,
				playerPerformance: 6,
				standings: 5,
				trade: 5,
				injury: 5,
				money: 3,
			}),
			formality: 0.25,
			verbosity: 0.4,
			caps: 0.15,
			optimism: -0.85,
			accuracy: 0.45,
			postiness: 0.85,
			replyiness: 0.4,
			quotiness: 0.4,
			feudiness: 0.55,
		},
	},
	{
		id: "troll",
		label: "Troll",
		summary: "Exists to be annoying. Picks fights, rarely right, never sorry.",
		personality: {
			tone: "unhinged",
			topics: topics({
				gameResult: 6,
				playerPerformance: 5,
				rumor: 6,
				standings: 3,
				offTopic: 4,
			}),
			formality: 0.05,
			verbosity: 0.25,
			caps: 0.5,
			emoji: 0.3,
			profanity: 0.3,
			optimism: -0.3,
			accuracy: 0.15,
			postiness: 0.8,
			replyiness: 0.65,
			quotiness: 0.6,
			feudiness: 0.9,
		},
	},
	{
		id: "aggregator",
		label: "Aggregator",
		summary:
			"Stat lines and final scores, no commentary. The wire of the fan world.",
		personality: {
			tone: "wire",
			topics: topics({
				gameResult: 10,
				playerPerformance: 9,
				milestone: 5,
				standings: 3,
			}),
			formality: 0.7,
			verbosity: 0.3,
			accuracy: 1,
			postiness: 1,
			replyiness: 0,
			quotiness: 0.05,
		},
	},
	{
		id: "teamOfficial",
		label: "Team account",
		summary: "The franchise itself. Relentlessly positive, never critical.",
		personality: {
			tone: "corporate",
			topics: topics({
				gameResult: 8,
				playerPerformance: 6,
				milestone: 5,
				draft: 5,
				freeAgency: 5,
				awards: 6,
			}),
			formality: 0.6,
			verbosity: 0.35,
			emoji: 0.6,
			// Zero, alone among the archetypes. A franchise account is written
			// by a communications department, and random mid-sentence emphasis
			// is the one thing it never does - it reads as a typo, not as
			// excitement. Its enthusiasm comes out through emoji instead.
			caps: 0,
			optimism: 1,
			accuracy: 1,
			postiness: 0.9,
			replyiness: 0.05,
			quotiness: 0.1,
		},
	},
	{
		id: "player",
		label: "Player",
		summary: "A player posting in first person about his own nights.",
		personality: {
			tone: "hype",
			topics: topics({
				gameResult: 5,
				playerPerformance: 6,
				milestone: 6,
				awards: 6,
				trade: 3,
				offTopic: 3,
			}),
			formality: 0.2,
			verbosity: 0.3,
			emoji: 0.45,
			caps: 0.2,
			optimism: 0.5,
			accuracy: 0.8,
			// Low: most players are not posting most nights, which is what keeps
			// 500 accounts from drowning the feed.
			postiness: 0.16,
			replyiness: 0.25,
			quotiness: 0.15,
			feudiness: 0.2,
		},
	},
	{
		id: "capNerd",
		label: "Cap analyst",
		summary: "Contracts, tax bills, and what a team can actually afford.",
		personality: {
			tone: "wonk",
			topics: topics({
				money: 10,
				trade: 7,
				freeAgency: 8,
				draft: 3,
			}),
			formality: 0.85,
			verbosity: 0.7,
			accuracy: 1,
			postiness: 0.5,
			replyiness: 0.3,
			quotiness: 0.3,
		},
	},
	{
		id: "draftHead",
		label: "Draft analyst",
		summary: "Prospects, lottery odds, and who the tank is actually for.",
		personality: {
			tone: "beat",
			topics: topics({
				draft: 10,
				standings: 5,
				playerPerformance: 3,
				trade: 3,
			}),
			formality: 0.75,
			verbosity: 0.6,
			accuracy: 0.9,
			postiness: 0.55,
			replyiness: 0.25,
			quotiness: 0.25,
		},
	},
	{
		id: "historian",
		label: "Historian",
		summary: "Records, milestones, and what happened on this date.",
		personality: {
			tone: "beat",
			topics: topics({
				milestone: 10,
				awards: 6,
				standings: 3,
				analytics: 3,
			}),
			formality: 0.9,
			verbosity: 0.7,
			accuracy: 1,
			postiness: 0.45,
			replyiness: 0.1,
			quotiness: 0.15,
		},
	},
	{
		id: "localRadio",
		label: "Local radio",
		summary: "One market, maximum heat. Fires the coach every other week.",
		personality: {
			tone: "snark",
			topics: topics({
				gameResult: 8,
				playerPerformance: 5,
				trade: 6,
				standings: 5,
				rumor: 5,
				money: 3,
			}),
			formality: 0.4,
			verbosity: 0.5,
			caps: 0.2,
			optimism: -0.5,
			accuracy: 0.5,
			postiness: 0.8,
			replyiness: 0.45,
			quotiness: 0.5,
			feudiness: 0.7,
		},
	},
];

export const archetypeById = (id: string): SocialArchetype | undefined =>
	BUILT_IN_ARCHETYPES.find((a) => a.id === id);

// Merge a sparse edit over a base. Topics merge KEY BY KEY rather than
// wholesale: an override that bumps one topic must not silently zero the
// twelve it did not mention, which is exactly the trap a spread would set for
// anyone using the batch editor.
export const mergePersonality = (
	base: SocialPersonality,
	...overrides: (SocialPersonalityOverride | undefined)[]
): SocialPersonality => {
	let out = base;
	for (const override of overrides) {
		if (!override) {
			continue;
		}
		const { topics: topicOverride, ...rest } = override;
		out = {
			...out,
			...rest,
			topics: topicOverride ? { ...out.topics, ...topicOverride } : out.topics,
		};
	}
	return out;
};

// The finished personality for an account: the neutral base, then its
// archetype, then its own edits. Always in that order, so a per-account edit
// always wins over a batch one.
export const resolvePersonality = ({
	archetype,
	override,
}: {
	archetype: SocialArchetype | undefined;
	override: SocialPersonalityOverride | undefined;
}): SocialPersonality =>
	mergePersonality(BASE_PERSONALITY, archetype?.personality, override);

// A PLAYER'S OWN VOICE, derived rather than authored.
//
// Every player in the league gets an account, so nobody is going to hand-write
// five hundred personalities. The league already knows enough to differentiate
// them: mood traits say what a player cares about, and age and ability say how
// much standing he has to say it with. This turns that into knob deltas on top
// of the "player" archetype, so a 34-year-old max player who values Winning
// reads nothing like a 20-year-old rookie who values Fame.
//
// Deliberately small and monotonic. Every term here has to be explainable in
// one clause, because a user who does not like what a player sounds like will
// go looking for the reason.
export const personalityForPlayer = ({
	moodTraits,
	age,
	ovr,
	// Seasons of experience. A rookie posts differently from a veteran.
	experience,
}: {
	moodTraits: readonly MoodTrait[];
	age: number;
	ovr: number;
	experience: number;
}): SocialPersonalityOverride => {
	const traits = new Set(moodTraits);
	const override: SocialPersonalityOverride = {};
	const topicDelta: Partial<SocialTopicWeights> = {};

	// STANDING. A star is worth listening to and knows it; a fringe player
	// posts less and hedges more. Scaled off ovr because that is the league's
	// own measure of who matters.
	const star = Math.max(0, Math.min(1, (ovr - 45) / 25));
	override.postiness = 0.08 + 0.3 * star;
	override.verbosity = 0.25 + 0.2 * star;

	// FAME wants an audience: posts more, off-topic more, louder.
	if (traits.has("F")) {
		override.postiness = (override.postiness ?? 0.16) * 1.8;
		override.emoji = 0.6;
		override.caps = 0.3;
		topicDelta.offTopic = 6;
		topicDelta.awards = 8;
	}

	// MONEY talks about money, which in a basketball league means contracts.
	if (traits.has("$")) {
		topicDelta.money = 7;
		topicDelta.freeAgency = 5;
	}

	// WINNING talks about the standings and says less about himself.
	if (traits.has("W")) {
		topicDelta.standings = 7;
		topicDelta.gameResult = 8;
		topicDelta.playerPerformance = 3;
		override.optimism = 0.2;
	}

	// LOYALTY defends the team and the teammates, and holds grudges about both.
	if (traits.has("L")) {
		override.feudiness = 0.45;
		override.replyiness = 0.4;
		topicDelta.trade = 5;
	}

	// AGE. Young players are louder and less careful; veterans are measured.
	// Both ends are real, so this moves formality in both directions rather
	// than only penalizing one.
	if (age <= 23) {
		override.formality = 0.12;
		override.caps = Math.max(override.caps ?? 0.2, 0.3);
		override.accuracy = 0.7;
	} else if (age >= 32 || experience >= 12) {
		override.formality = 0.45;
		override.optimism = (override.optimism ?? 0.5) * 0.5;
		topicDelta.milestone = 6;
	}

	if (Object.keys(topicDelta).length > 0) {
		override.topics = topicDelta;
	}
	return override;
};
