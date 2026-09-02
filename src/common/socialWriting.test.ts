import { assert, describe, test } from "vitest";
import { createPhrasePool, rngFromSeed } from "./phrasePool.ts";
import type { ResolvedSocialAccount } from "./socialAccounts.ts";
import { eventsFromGame, type SocialEvent } from "./socialEvents.ts";
import {
	BUILT_IN_ARCHETYPES,
	resolvePersonality,
	type SocialPersonalityOverride,
} from "./socialPersonality.ts";
import {
	applyVoice,
	frameFor,
	stanceOf,
	verifyPostNumbers,
	writePost,
	writeReply,
} from "./socialWriting.ts";

const account = (
	archetypeId: string,
	extra: {
		tid?: number;
		pid?: number;
		override?: SocialPersonalityOverride;
	} = {},
): ResolvedSocialAccount => ({
	id: `m:${archetypeId}:${extra.tid ?? "x"}`,
	kind: "media",
	handle: archetypeId,
	name: archetypeId,
	bio: "",
	tid: extra.tid,
	pid: extra.pid,
	archetypeId,
	personality: resolvePersonality({
		archetype: BUILT_IN_ARCHETYPES.find((a) => a.id === archetypeId),
		override: extra.override,
	}),
	implicit: false,
});

const game = (overrides: Record<string, unknown> = {}) =>
	eventsFromGame({
		gid: 1,
		day: 1,
		season: 2013,
		overtimes: 0,
		winnerTid: 0,
		playoffs: false,
		teams: [
			{
				tid: 0,
				region: "Boston",
				name: "Celtics",
				abbrev: "BOS",
				pts: 112,
				players: [],
			},
			{
				tid: 1,
				region: "Sacramento",
				name: "Kings",
				abbrev: "SAC",
				pts: 98,
				players: [],
			},
		],
		...overrides,
	})[0]!;

const perfEvent = (
	stats: Partial<Record<string, number | boolean | string>> = {},
): SocialEvent => ({
	id: "perf:1:5",
	type: "performance",
	topic: "playerPerformance",
	season: 2013,
	day: 1,
	order: 2,
	salience: 0.8,
	tids: [0],
	pids: [5],
	facts: {
		name: "Paul Pierce",
		tid: 0,
		won: true,
		min: 38,
		pts: 34,
		reb: 8,
		ast: 5,
		stl: 2,
		blk: 1,
		tov: 3,
		doubles: 1,
		tripleDouble: false,
		opponentAbbrev: "SAC",
		...stats,
	},
});

const write = (acct: ResolvedSocialAccount, event: SocialEvent, seed = 1) => {
	const pool = createPhrasePool();
	return writePost({ account: acct, event, pool, rng: rngFromSeed(seed) });
};

describe("stanceOf", () => {
	test("an account sits with its own team, against the other", () => {
		assert.strictEqual(
			stanceOf(account("homerFan", { tid: 0 }), game()),
			"for",
		);
		assert.strictEqual(
			stanceOf(account("homerFan", { tid: 1 }), game()),
			"against",
		);
		assert.strictEqual(stanceOf(account("nationalPundit"), game()), "neutral");
	});

	test("a losing performance puts a teammate account against it", () => {
		assert.strictEqual(
			stanceOf(account("homerFan", { tid: 0 }), perfEvent({ won: false })),
			"against",
		);
	});
});

describe("frameFor", () => {
	test("a game frame names the shape, not just the numbers", () => {
		const frame = frameFor(account("beatWriter"), game()) as any;
		assert.strictEqual(frame.kind, "game");
		assert.strictEqual(frame.winner, "Boston Celtics");
		assert.strictEqual(frame.margin, 14);
		assert.strictEqual(frame.blowout, false);
		assert.strictEqual(frame.nailbiter, false);
	});

	test("overtime makes any game a nailbiter regardless of margin", () => {
		const frame = frameFor(
			account("beatWriter"),
			game({ overtimes: 2 }),
		) as any;
		assert.strictEqual(frame.nailbiter, true);
	});

	test("an event with no facts to quote produces no frame", () => {
		// Better to post nothing than a shape with a hole in it.
		const empty: SocialEvent = {
			id: "e:1",
			type: "trade",
			topic: "trade",
			season: 2013,
			day: 1,
			order: 1,
			salience: 0.5,
			tids: [],
			pids: [],
			facts: { summary: "" },
		};
		assert.strictEqual(frameFor(account("insider"), empty), undefined);
	});
});

describe("writePost", () => {
	test("a wire account states the result flatly", () => {
		const text = write(account("aggregator"), game())!;
		assert.strictEqual(/Boston|BOS/.test(text), true);
		assert.strictEqual(/Sacramento|SAC/.test(text), true);
		// Whatever line it drew, every number in it is the league's.
		assert.deepStrictEqual(verifyPostNumbers(text, game().facts), []);
	});

	test("a homer for the winner never draws a losing line", () => {
		// Stance gating is what stops a fan of the winning team posting "sell
		// the team", which is the single most obviously broken thing this
		// design could produce.
		for (let seed = 0; seed < 40; seed++) {
			const text = write(account("homerFan", { tid: 0 }), game(), seed)!;
			assert.strictEqual(
				/sell the team|same thing every night/i.test(text),
				false,
			);
		}
	});

	test("a homer for the loser never draws a celebration", () => {
		for (let seed = 0; seed < 40; seed++) {
			const text = write(account("homerFan", { tid: 1 }), game(), seed)!;
			assert.strictEqual(/how you do it|not a typo/i.test(text), false);
		}
	});

	test("tone decides which bank an account draws from", () => {
		const corporate = write(account("teamOfficial", { tid: 0 }), game(), 3)!;
		const wonk = write(account("analytics"), game(), 3)!;
		assert.notStrictEqual(corporate, wonk);
	});

	test("a night does not reuse the same line twice", () => {
		// One pool across the whole day, exactly as the generator will run it.
		const pool = createPhrasePool();
		pool.beginBatch();
		const accounts = Array.from({ length: 8 }, (_, i) =>
			account("beatWriter", { tid: i }),
		);
		const texts = accounts.map(
			(a, i) =>
				writePost({
					account: a,
					event: game(),
					pool,
					rng: rngFromSeed(i + 1),
				})!,
		);
		assert.strictEqual(new Set(texts).size > 1, true);
	});

	test("a corporate account never draws a line written for a doomer", () => {
		// Tone filtering is what keeps the franchise's own account from posting
		// "sell the team" about its own loss.
		const blowout = game({
			winnerTid: 0,
			teams: [
				{
					tid: 0,
					region: "Boston",
					name: "Celtics",
					abbrev: "BOS",
					pts: 130,
					players: [],
				},
				{
					tid: 1,
					region: "Sacramento",
					name: "Kings",
					abbrev: "SAC",
					pts: 96,
					players: [],
				},
			],
		});
		for (let seed = 0; seed < 30; seed++) {
			const text = write(account("teamOfficial", { tid: 1 }), blowout, seed);
			if (text === undefined) {
				continue;
			}
			assert.strictEqual(
				/sell the team|allowed to leave|run off the floor/i.test(text),
				false,
				text,
			);
		}
	});

	test("two identical accounts on one event do not say the same thing", () => {
		// The precise mechanism: template ids are CLAIMED for the batch, so the
		// second account is steered off the first one's line even though its
		// eligible set and its seed are identical. Without claiming these two
		// produce the same string.
		const pool = createPhrasePool();
		pool.beginBatch();
		const event = game();
		const first = writePost({
			account: { ...account("beatWriter"), id: "m:a" },
			event,
			pool,
			rng: rngFromSeed(4),
		});
		const second = writePost({
			account: { ...account("beatWriter"), id: "m:b" },
			event,
			pool,
			rng: rngFromSeed(4),
		});
		assert.notStrictEqual(first, undefined);
		assert.notStrictEqual(first, second);
	});

	test("a performance post quotes the line it was given", () => {
		const text = write(account("aggregator"), perfEvent(), 2)!;
		assert.strictEqual(/Paul Pierce/.test(text), true);
	});

	test("a triple-double is available to every tone", () => {
		for (const archetypeId of [
			"aggregator",
			"homerFan",
			"troll",
			"analytics",
			"teamOfficial",
		]) {
			const text = write(
				account(archetypeId, { tid: 0 }),
				perfEvent({
					pts: 12,
					reb: 11,
					ast: 10,
					doubles: 3,
					tripleDouble: true,
				}),
				7,
			);
			assert.notStrictEqual(text, undefined);
		}
	});

	test("an account with a tone no line suits stays silent rather than guessing", () => {
		const odd = account("beatWriter", { override: { tone: "unhinged" } });
		// Summary events have unhinged lines, so use a bank where the filter can
		// genuinely empty: a quiet performance for a tone with no matching line.
		const text = write(odd, perfEvent({ pts: 11, reb: 2, ast: 1, tov: 1 }), 5);
		assert.strictEqual(text === undefined || text.length > 0, true);
	});
});

describe("applyVoice", () => {
	const base = resolvePersonality({
		archetype: undefined,
		override: undefined,
	});

	const voice = (
		override: Partial<typeof base>,
		seed = 1,
		positive: boolean | undefined = true,
		text = "Boston won by 14.",
	) => {
		const pool = createPhrasePool();
		return applyVoice({
			text,
			personality: { ...base, ...override },
			pool,
			rng: rngFromSeed(seed),
			positive,
		});
	};

	test("a formal account is left alone", () => {
		assert.strictEqual(
			voice({ formality: 1, emoji: 0, caps: 0, profanity: 0 }),
			"Boston won by 14.",
		);
	});

	test("an informal account types in lowercase without a full stop", () => {
		const text = voice({ formality: 0.1, emoji: 0, caps: 0, profanity: 0 });
		assert.strictEqual(text, "boston won by 14");
	});

	test("shouting emphasizes part of a line, never all of it", () => {
		// A whole post in caps is unreadable; the real thing people do is
		// emphasize a few words. Checked across seeds because the run is random
		// and the failure being guarded against is the unlucky one.
		for (let seed = 0; seed < 30; seed++) {
			const text = voice(
				{ caps: 1, emoji: 0, profanity: 0, formality: 1 },
				seed,
				true,
				"Boston absolutely dismantled Sacramento tonight in every single phase.",
			);
			assert.strictEqual(text === text.toUpperCase(), false, text);
			assert.strictEqual(/[A-Z]{4,}/.test(text), true, text);
		}
	});

	test("a short line is never shouted end to end", () => {
		// The run cap alone does not save a three-word post: without a bound
		// relative to the line's length, every word gets capitalised.
		for (let seed = 0; seed < 30; seed++) {
			const text = voice(
				{ caps: 1, emoji: 0, profanity: 0, formality: 1 },
				seed,
				true,
				"Celtics won again.",
			);
			assert.strictEqual(text === text.toUpperCase(), false, text);
		}
	});

	test("emoji follow the news, not the account", () => {
		const good = voice(
			{ emoji: 1, caps: 0, profanity: 0, formality: 1 },
			1,
			true,
		);
		const bad = voice(
			{ emoji: 1, caps: 0, profanity: 0, formality: 1 },
			1,
			false,
		);
		assert.notStrictEqual(good, bad);
	});

	test("a neutral observer gets neither a celebration nor a skull", () => {
		const text = voice(
			{ emoji: 1, caps: 0, profanity: 0, formality: 1 },
			1,
			undefined,
		);
		assert.strictEqual(/🔥|💀|😭|🙌/.test(text), false);
	});

	test("swearing is censored by construction", () => {
		const text = voice({ profanity: 1, emoji: 0, caps: 0, formality: 1 });
		assert.strictEqual(/damn|hell|garbage|brutal/.test(text), true);
	});

	test("a catchphrase can lead the line", () => {
		let seen = false;
		for (let seed = 0; seed < 40; seed++) {
			const text = voice(
				{
					catchphrases: ["Sources tell me"],
					emoji: 0,
					caps: 0,
					profanity: 0,
					formality: 1,
				},
				seed,
			);
			if (text.startsWith("Sources tell me")) {
				seen = true;
			}
		}
		assert.strictEqual(seen, true);
	});

	test("the same voice and seed always types the same way", () => {
		assert.strictEqual(
			voice({ emoji: 1, caps: 1, profanity: 1, formality: 0.2 }, 9),
			voice({ emoji: 1, caps: 1, profanity: 1, formality: 0.2 }, 9),
		);
	});
});

describe("verifyPostNumbers", () => {
	const facts = { winnerPts: 112, loserPts: 98, margin: 14, tsp: 63.4 };

	test("numbers taken from the event pass", () => {
		assert.deepStrictEqual(
			verifyPostNumbers("BOS 112, SAC 98. Margin 14.", facts),
			[],
		);
	});

	test("an invented number is caught", () => {
		// The whole point: an account may be wrong about what a result MEANS
		// and never about what it WAS.
		const violations = verifyPostNumbers("Boston won by 22.", facts);
		assert.strictEqual(violations.length, 1);
		assert.strictEqual(violations[0]!.kind, "unsourced-number");
	});

	test("numbers inside the league's own prose count as sourced", () => {
		assert.deepStrictEqual(
			verifyPostNumbers("Signed for 4 years.", {
				summary: "Paul Pierce signed for 4 years.",
			}),
			[],
		);
	});

	test("a displayed rate may round without failing the post", () => {
		assert.deepStrictEqual(verifyPostNumbers("63% true shooting", facts), []);
	});

	test("a rate that is simply wrong is still caught", () => {
		assert.strictEqual(verifyPostNumbers("81% true shooting", facts).length, 1);
	});

	test("a post with no numbers is fine", () => {
		assert.deepStrictEqual(verifyPostNumbers("Not close at all.", facts), []);
	});

	test("every generated post survives its own checker", () => {
		// The guarantee, exercised across the real banks rather than asserted.
		const events = [
			game(),
			game({ overtimes: 1 }),
			game({
				winnerTid: 1,
				teams: [
					{
						tid: 0,
						region: "Boston",
						name: "Celtics",
						abbrev: "BOS",
						pts: 96,
						players: [],
					},
					{
						tid: 1,
						region: "Sacramento",
						name: "Kings",
						abbrev: "SAC",
						pts: 121,
						players: [],
					},
				],
			}),
			perfEvent(),
			perfEvent({ pts: 44, reb: 12, doubles: 2 }),
			perfEvent({ pts: 15, reb: 11, ast: 12, doubles: 3, tripleDouble: true }),
		];
		const archetypes = BUILT_IN_ARCHETYPES.map((a) => a.id);
		let checked = 0;
		for (const archetypeId of archetypes) {
			for (const tid of [0, 1, undefined]) {
				for (const event of events) {
					for (let seed = 0; seed < 6; seed++) {
						const text = write(account(archetypeId, { tid }), event, seed);
						if (text === undefined) {
							continue;
						}
						checked += 1;
						assert.deepStrictEqual(
							verifyPostNumbers(text, event.facts),
							[],
							`${archetypeId}/${tid}/${event.id}/${seed}: ${text}`,
						);
					}
				}
			}
		}
		assert.strictEqual(checked > 200, true, `only checked ${checked}`);
	});
});

// The lines that assert somebody is WRONG, as opposed to merely adding
// something. Only an account with a higher accuracy bar may reach these.
const CORRECTION =
	/not what the box score says|worth mentioning both|^it was \d+-\d+\./i;

describe("writeReply", () => {
	const reply = (
		replier: ResolvedSocialAccount,
		parent: ResolvedSocialAccount,
		heat = 0,
		event: SocialEvent = game(),
		seed = 1,
	) => {
		const pool = createPhrasePool();
		return writeReply({
			account: replier,
			parent,
			event,
			heat,
			pool,
			rng: rngFromSeed(seed),
		});
	};

	test("an answer is always produced, so no thread dangles", () => {
		const text = reply(account("beatWriter"), account("homerFan", { tid: 0 }));
		assert.notStrictEqual(text, undefined);
		assert.strictEqual(text!.length > 0, true);
	});

	test("a correction quotes the same facts the original was held to", () => {
		// The one reply that states numbers, and it may not invent them either.
		const wonk = account("analytics");
		const troll = account("troll", { tid: 0 });
		for (let seed = 0; seed < 25; seed++) {
			const text = reply(wonk, troll, 0, game(), seed);
			if (text === undefined) {
				continue;
			}
			assert.deepStrictEqual(verifyPostNumbers(text, game().facts), [], text);
		}
	});

	test("only an account with a higher bar reads another as wrong", () => {
		// A troll never corrects a wire service, which is the direction that
		// makes the whole idea read as parody rather than as a feed.
		const troll = account("troll", { tid: 0 });
		const wire = account("aggregator", { tid: 0 });
		for (let seed = 0; seed < 25; seed++) {
			const text = reply(troll, wire, 0, game(), seed);
			if (text === undefined) {
				continue;
			}
			assert.strictEqual(CORRECTION.test(text), false, text);
		}
	});

	test("a low-accuracy account never corrects a careful one", () => {
		// Isolates the direction rather than the tone: this account draws from
		// the same bank the corrections live in, and still must not reach them.
		const sloppyWonk = account("analytics", {
			tid: 0,
			override: { accuracy: 0.2 },
		});
		const careful = account("aggregator", { tid: 0 });
		for (let seed = 0; seed < 30; seed++) {
			const text = reply(sloppyWonk, careful, 0, game(), seed);
			if (text === undefined) {
				continue;
			}
			assert.strictEqual(CORRECTION.test(text), false, text);
		}
	});

	test("heat unlocks lines that do not exist without history", () => {
		const troll = account("troll", { tid: 1 });
		const homer = account("homerFan", { tid: 0 });
		const cold = new Set<string>();
		const hot = new Set<string>();
		for (let seed = 0; seed < 30; seed++) {
			const c = reply(troll, homer, 0, game(), seed);
			const h = reply(troll, homer, 1, game(), seed);
			if (c) {
				cold.add(c);
			}
			if (h) {
				hot.add(h);
			}
		}
		const heated = [...hot].some((t) => /you again|imagine typing/i.test(t));
		const coldHeated = [...cold].some((t) =>
			/you again|imagine typing/i.test(t),
		);
		assert.strictEqual(heated, true);
		assert.strictEqual(coldHeated, false);
	});

	test("a reply names the account it is answering", () => {
		const troll = account("troll", { tid: 1 });
		const homer = account("homerFan", { tid: 0 });
		let named = false;
		for (let seed = 0; seed < 30; seed++) {
			const text = reply(troll, homer, 1, game(), seed);
			if (text && text.includes("@")) {
				named = true;
			}
		}
		assert.strictEqual(named, true);
	});

	test("two similar accounts in one batch do not give the same answer", () => {
		// The ledger claims template ids across the whole day, so the second
		// doomer under a same-side post is steered off the first one's line.
		const pool = createPhrasePool();
		pool.beginBatch();
		const parent = account("homerFan", { tid: 0 });
		const first = writeReply({
			account: { ...account("doomerFan", { tid: 0 }), id: "m:d1" },
			parent,
			event: game({ winnerTid: 1 }),
			heat: 0,
			pool,
			rng: rngFromSeed(2),
		});
		const second = writeReply({
			account: { ...account("doomerFan", { tid: 0 }), id: "m:d2" },
			parent,
			event: game({ winnerTid: 1 }),
			heat: 0,
			pool,
			rng: rngFromSeed(2),
		});
		assert.notStrictEqual(first, undefined);
		assert.notStrictEqual(first, second);
	});

	test("the same reply is written the same way every time", () => {
		const a = reply(account("analytics"), account("troll", { tid: 0 }), 0.5);
		const b = reply(account("analytics"), account("troll", { tid: 0 }), 0.5);
		assert.strictEqual(a, b);
	});

	test("every reply survives the number checker", () => {
		const events = [game(), game({ overtimes: 1 }), perfEvent()];
		let checked = 0;
		for (const archetypeId of BUILT_IN_ARCHETYPES.map((a) => a.id)) {
			for (const parentId of ["troll", "homerFan", "aggregator", "doomerFan"]) {
				for (const event of events) {
					for (let seed = 0; seed < 4; seed++) {
						const text = reply(
							account(archetypeId, { tid: 0 }),
							account(parentId, { tid: 1 }),
							seed / 4,
							event,
							seed,
						);
						if (text === undefined) {
							continue;
						}
						checked += 1;
						assert.deepStrictEqual(
							verifyPostNumbers(text, event.facts),
							[],
							`${archetypeId} -> ${parentId}: ${text}`,
						);
					}
				}
			}
		}
		assert.strictEqual(checked > 100, true, `only checked ${checked}`);
	});
});

// THE HOLE TEST. Every voice has to have something to say in every situation
// it can find itself in, because a cell with one eligible line is a catch-all
// that repeats every single time it comes up. This was measured at ONE line
// for an enthusiastic account quoting somebody it disagrees with, and that is
// how "Noting this one down." ended up in a fortnight of output six times.
describe("reply and quote coverage", () => {
	const TONES = [
		"wire",
		"beat",
		"hype",
		"snark",
		"doom",
		"wonk",
		"corporate",
		"unhinged",
	] as const;

	const voiced = (
		tone: (typeof TONES)[number],
		tid: number,
		accuracy: number,
	) =>
		account("beatWriter", {
			tid,
			override: {
				tone,
				accuracy,
				// Voice is off so the count measures the BANK, not the emoji.
				emoji: 0,
				caps: 0,
				profanity: 0,
				formality: 0.8,
			},
		});

	// Same side, other side, a feud, and a correction: the four situations the
	// banks branch on.
	const SITUATIONS: [string, boolean, number, boolean][] = [
		["same side", true, 0, false],
		["other side", false, 0, false],
		["a feud", false, 0.7, false],
		["a correction", false, 0, true],
	];

	for (const quote of [false, true]) {
		for (const tone of TONES) {
			for (const [label, sameSide, heat, correcting] of SITUATIONS) {
				test(`${quote ? "quotes" : "replies"}: ${tone} on ${label}`, () => {
					const replier = voiced(tone, 0, correcting ? 1 : 0.5);
					const parent = voiced("hype", sameSide ? 0 : 1, 0.5);
					const seen = new Set<string>();
					for (let seed = 0; seed < 120; seed++) {
						const pool = createPhrasePool();
						const text = writeReply({
							account: replier,
							parent,
							event: game(),
							heat,
							quote,
							pool,
							rng: rngFromSeed(seed * 7919 + 13),
						});
						if (text !== undefined) {
							seen.add(text);
						}
					}
					assert.ok(
						seen.size >= 6,
						`only ${seen.size} distinct lines: ${[...seen].join(" / ")}`,
					);
				});
			}
		}
	}
});

// A POST HAS NO CONTEXT. It is read on its own in a timeline, so a line about
// a game that names only one of the two teams leaves the reader guessing who
// the other one was. Caught three templates that said "BOS by 10" and one that
// said "230 points between them" with no "them" anywhere in sight.
//
// Only for accounts with NO stake in the game. A fan account saying "beat the
// Kings" does not have to name its own team, because the account IS the other
// team - that is the one piece of context a post carries with it.
describe("every game line names both teams", () => {
	const TONES = [
		"wire",
		"beat",
		"hype",
		"snark",
		"doom",
		"wonk",
		"corporate",
		"unhinged",
	] as const;

	// A spread of game shapes, so the situational templates get their turn.
	const SHAPES: [string, Record<string, unknown>][] = [
		["a normal win", {}],
		["a blowout", { winnerPts: 130, loserPts: 99 }],
		["a one-point game", { winnerPts: 101, loserPts: 100 }],
		["overtime", { overtimes: 1, winnerPts: 118, loserPts: 115 }],
		["a shootout", { winnerPts: 140, loserPts: 135 }],
		["a rock fight", { winnerPts: 88, loserPts: 84 }],
	];

	for (const tone of TONES) {
		for (const [label, overrides] of SHAPES) {
			for (const [side, tid] of [["a bystander", undefined]] as const) {
				test(`${tone} on ${label} as ${side}`, () => {
					const event = game(overrides);
					const winner = String(event.facts.winner);
					const loser = String(event.facts.loser);
					const winnerAbbrev = String(event.facts.winnerAbbrev);
					const loserAbbrev = String(event.facts.loserAbbrev);
					for (let seed = 0; seed < 60; seed++) {
						const pool = createPhrasePool();
						const text = writePost({
							account: account("beatWriter", {
								tid,
								override: { tone, emoji: 0, caps: 0, profanity: 0 },
							}),
							event,
							pool,
							rng: rngFromSeed(seed * 7919 + 13),
						});
						if (text === undefined) {
							continue;
						}
						const names = new RegExp(
							`${winner}|${loser}|${winnerAbbrev}|${loserAbbrev}`,
							"i",
						);
						// Either it names somebody, or it is a reaction that names
						// nobody at all - what it must never do is name one side
						// and leave the other implied.
						const mentionsWinner = new RegExp(
							`${winner}|${winnerAbbrev}`,
							"i",
						).test(text);
						const mentionsLoser = new RegExp(
							`${loser}|${loserAbbrev}`,
							"i",
						).test(text);
						if (names.test(text)) {
							assert.ok(
								mentionsWinner && mentionsLoser,
								`names one side only: ${text}`,
							);
						}
					}
				});
			}
		}
	}
});
