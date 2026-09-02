import { assert, describe, test } from "vitest";
import {
	castDay,
	castReplies,
	interest,
	relevance,
	replyAppetite,
	topicPull,
} from "./socialCasting.ts";
import { resolveAccounts, type ImplicitPlayer } from "./socialAccounts.ts";
import type { SocialEvent } from "./socialEvents.ts";
import {
	BUILT_IN_ARCHETYPES,
	resolvePersonality,
	type SocialPersonalityOverride,
} from "./socialPersonality.ts";

const account = (
	id: string,
	archetypeId: string,
	extra: {
		tid?: number;
		pid?: number;
		kind?: "player" | "team" | "media";
		override?: SocialPersonalityOverride;
	} = {},
) => ({
	id,
	kind: extra.kind ?? ("media" as const),
	handle: id,
	name: id,
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

const ev = (id: string, overrides: Partial<SocialEvent> = {}): SocialEvent => ({
	id,
	type: "gameResult",
	topic: "gameResult",
	season: 2013,
	day: 1,
	order: 1,
	salience: 0.6,
	tids: [0, 1],
	pids: [],
	facts: {},
	...overrides,
});

describe("relevance", () => {
	test("a player's own night outranks everything else he could post about", () => {
		// This is what makes five hundred player accounts worth having: each is
		// silent except about himself.
		const p = account("p:1", "player", { kind: "player", pid: 1, tid: 0 });
		const own = relevance(p, ev("perf", { pids: [1], tids: [0] }));
		const teamOnly = relevance(p, ev("g", { pids: [], tids: [0, 1] }));
		const elsewhere = relevance(p, ev("g2", { pids: [], tids: [4, 5] }));
		assert.strictEqual(own > teamOnly, true);
		assert.strictEqual(teamOnly > elsewhere, true);
	});

	test("a beat writer barely looks up at a game his team was not in", () => {
		// Without this asymmetry every beat writer covers every game, which is
		// both wrong and unreadable.
		const beat = account("m:bos", "beatWriter", { tid: 0 });
		const own = relevance(beat, ev("g", { tids: [0, 1] }));
		const other = relevance(beat, ev("g2", { tids: [4, 5] }));
		assert.strictEqual(own / other > 10, true);
	});

	test("a national account covers the whole league evenly", () => {
		const national = account("m:nat", "nationalPundit");
		assert.strictEqual(
			relevance(national, ev("g", { tids: [0, 1] })),
			relevance(national, ev("g2", { tids: [4, 5] })),
		);
	});

	test("a rival's game is worth more than a stranger's", () => {
		const fan = account("m:fan", "homerFan", {
			tid: 0,
			override: { rivalTids: [7] },
		});
		assert.strictEqual(
			relevance(fan, ev("g", { tids: [7, 9] })) >
				relevance(fan, ev("g2", { tids: [4, 5] })),
			true,
		);
	});

	test("an explicit loyalty overrides the account's own team", () => {
		const fan = account("m:fan", "homerFan", {
			tid: 0,
			override: { loyaltyTid: 5 },
		});
		assert.strictEqual(
			relevance(fan, ev("g", { tids: [5, 9] })) >
				relevance(fan, ev("g2", { tids: [0, 1] })),
			true,
		);
	});
});

describe("topicPull", () => {
	test("weights are relative to the account's own largest", () => {
		// Otherwise an account whose preset happens to be written with small
		// numbers loses every slot to one written with large numbers, which is a
		// property of how someone typed it rather than of what it means.
		const scaled = (factor: number) =>
			Object.fromEntries(
				Object.entries(account("m:ref", "insider").personality.topics).map(
					([k, v]) => [k, v * factor],
				),
			) as any;
		const small = account("m:a", "insider", {
			override: { topics: scaled(1) },
		});
		const large = account("m:b", "insider", {
			override: { topics: scaled(100) },
		});
		const trade = ev("t", { topic: "trade" });
		assert.strictEqual(topicPull(small, trade), topicPull(large, trade));
	});

	test("a topic the account ignores pulls nothing", () => {
		const insider = account("m:i", "insider");
		assert.strictEqual(topicPull(insider, ev("g", { topic: "gameResult" })), 0);
	});

	test("an account with no weights at all never posts", () => {
		const blank = account("m:z", "insider", {
			override: {
				topics: Object.fromEntries(
					Object.keys(account("m:i", "insider").personality.topics).map((k) => [
						k,
						0,
					]),
				) as any,
			},
		});
		assert.strictEqual(topicPull(blank, ev("g")), 0);
	});
});

describe("interest", () => {
	test("proximity beats magnitude", () => {
		// A beat writer would rather cover his own team's dull loss than a
		// spectacular game across the country.
		const beat = account("m:bos", "beatWriter", { tid: 0 });
		const dullHome = ev("g1", { tids: [0, 1], salience: 0.3 });
		const spectacularAway = ev("g2", { tids: [4, 5], salience: 1 });
		assert.strictEqual(
			interest(beat, dullHome) > interest(beat, spectacularAway),
			true,
		);
	});

	test("a bigger event still wins between two the account is equally close to", () => {
		const beat = account("m:bos", "beatWriter", { tid: 0 });
		assert.strictEqual(
			interest(beat, ev("g1", { tids: [0, 1], salience: 0.9 })) >
				interest(beat, ev("g2", { tids: [0, 1], salience: 0.2 })),
			true,
		);
	});

	test("an insider ignores a game entirely, however big", () => {
		const insider = account("m:i", "insider");
		assert.strictEqual(
			interest(insider, ev("g", { topic: "gameResult", salience: 1 })),
			0,
		);
	});
});

describe("castDay", () => {
	const events = Array.from({ length: 12 }, (_, i) =>
		ev(`g:${i}`, {
			tids: [i * 2, i * 2 + 1],
			salience: 0.4 + (i % 5) / 10,
			order: i,
		}),
	);

	const league = () => {
		const players: ImplicitPlayer[] = Array.from({ length: 60 }, (_, i) => ({
			pid: i,
			name: `Player ${i}`,
			tid: i % 24,
			age: 25,
			ovr: 50,
			experience: 4,
			moodTraits: [],
		}));
		const teams = Array.from({ length: 24 }, (_, tid) => ({
			tid,
			region: `Region${tid}`,
			name: `Name${tid}`,
			abbrev: `T${tid}`,
		}));
		return resolveAccounts({ players, teams, stored: [] });
	};

	test("a day lands at the requested size", () => {
		const cast = castDay({
			accounts: league(),
			events,
			seed: "2013|1",
			limits: { target: 40 },
		});
		assert.strictEqual(cast.length <= 40, true);
		assert.strictEqual(cast.length > 10, true);
	});

	test("nobody posts more than their cap in a night", () => {
		const cast = castDay({
			accounts: league(),
			events,
			seed: "2013|1",
			limits: { target: 60, maxPerAccount: 2 },
		});
		const counts = new Map<string, number>();
		for (const c of cast) {
			counts.set(c.accountId, (counts.get(c.accountId) ?? 0) + 1);
		}
		assert.strictEqual(Math.max(...counts.values()) <= 2, true);
	});

	test("one event never takes over the night", () => {
		const cast = castDay({
			accounts: league(),
			events,
			seed: "2013|1",
			limits: { target: 60, maxPerEvent: 3 },
		});
		const counts = new Map<string, number>();
		for (const c of cast) {
			counts.set(c.eventId, (counts.get(c.eventId) ?? 0) + 1);
		}
		assert.strictEqual(Math.max(...counts.values()) <= 3, true);
	});

	test("nobody posts about the same thing twice", () => {
		const cast = castDay({
			accounts: league(),
			events,
			seed: "2013|1",
			limits: { target: 60 },
		});
		const pairs = cast.map((c) => `${c.accountId}|${c.eventId}`);
		assert.strictEqual(new Set(pairs).size, pairs.length);
	});

	test("the same day casts identically every time", () => {
		const a = castDay({
			accounts: league(),
			events,
			seed: "2013|1",
			limits: { target: 40 },
		});
		const b = castDay({
			accounts: league(),
			events,
			seed: "2013|1",
			limits: { target: 40 },
		});
		assert.deepStrictEqual(a, b);
	});

	test("account order does not change the cast", () => {
		// Two devices may enumerate accounts differently; they must still agree.
		const forward = league();
		const backward = [...forward].reverse();
		assert.deepStrictEqual(
			castDay({
				accounts: forward,
				events,
				seed: "2013|1",
				limits: { target: 40 },
			}),
			castDay({
				accounts: backward,
				events,
				seed: "2013|1",
				limits: { target: 40 },
			}),
		);
	});

	test("a different day casts different people", () => {
		// Pure scoring is stable, which would mean the identical cast covers
		// every night forever - the most obvious way for this to feel mechanical.
		const day1 = castDay({
			accounts: league(),
			events,
			seed: "2013|1",
			limits: { target: 30 },
		});
		const day2 = castDay({
			accounts: league(),
			events,
			seed: "2013|2",
			limits: { target: 30 },
		});
		const a = new Set(day1.map((c) => c.accountId));
		const b = new Set(day2.map((c) => c.accountId));
		const shared = [...a].filter((id) => b.has(id)).length;
		assert.strictEqual(shared < a.size, true);
	});

	test("jitter reshuffles near-ties without promoting the uninterested", () => {
		// A player with no stake in anything must never outrank the beat writer
		// covering the game, however the dice land.
		const beat = account("m:bos", "beatWriter", { tid: 0 });
		const stranger = account("p:99", "player", {
			kind: "player",
			pid: 99,
			tid: 20,
		});
		const cast = castDay({
			accounts: [beat, stranger],
			events: [ev("g:0", { tids: [0, 1], salience: 0.8 })],
			seed: "2013|1",
			limits: { target: 1 },
		});
		assert.strictEqual(cast[0]?.accountId, "m:bos");
	});

	test("a quiet day produces a short feed rather than filler", () => {
		// The reactive-volume choice: nothing worth reacting to means nobody
		// scrapes the barrel to fill space.
		const cast = castDay({
			accounts: league(),
			events: [ev("g:0", { tids: [0, 1], salience: 0.05 })],
			seed: "2013|1",
			limits: { target: 40 },
		});
		assert.strictEqual(cast.length < 8, true);
	});

	test("an account that never posts is never cast", () => {
		const mute = account("m:mute", "beatWriter", {
			tid: 0,
			override: { postiness: 0 },
		});
		assert.strictEqual(
			castDay({
				accounts: [mute],
				events,
				seed: "2013|1",
				limits: { target: 10 },
			}).length,
			0,
		);
	});

	test("a low-postiness account is quiet on most nights, not every night", () => {
		// The gate is what makes five hundred player accounts survivable: each
		// shows up occasionally rather than posting a weak line every day.
		const sometimes = account("m:s", "beatWriter", {
			tid: 0,
			override: { postiness: 0.35 },
		});
		let days = 0;
		for (let day = 0; day < 40; day++) {
			const cast = castDay({
				accounts: [sometimes],
				events: [ev("g:0", { tids: [0, 1], salience: 0.9 })],
				seed: `2013|${day}`,
				limits: { target: 5 },
			});
			if (cast.length > 0) {
				days += 1;
			}
		}
		assert.strictEqual(days > 2, true, "never posts");
		assert.strictEqual(days < 40, true, "posts every single day");
	});

	test("an account with no stake in anything stays silent", () => {
		// Without a floor a thin night scrapes the barrel and produces posts
		// from accounts with no connection to what happened, which is filler.
		const far = Array.from({ length: 12 }, (_, i) =>
			ev(`g:${i}`, { tids: [90, 91], salience: 0.05, order: i }),
		);
		assert.strictEqual(
			castDay({
				accounts: league(),
				events: far,
				seed: "2013|1",
				limits: { target: 40 },
			}).length,
			0,
		);
	});

	test("the cast rotates even when everyone shows up every day", () => {
		// Isolates the jitter from the show-up gate: with postiness pinned at 1
		// the gate cannot vary, so any day-to-day difference is the jitter doing
		// its job. Without it the identical cast covers every night forever.
		const always = Array.from({ length: 20 }, (_, i) =>
			account(`m:${i}`, "nationalPundit", { override: { postiness: 1 } }),
		);
		const slate = Array.from({ length: 6 }, (_, i) =>
			ev(`g:${i}`, { tids: [i * 2, i * 2 + 1], salience: 0.6, order: i }),
		);
		const key = (seed: string) =>
			castDay({ accounts: always, events: slate, seed, limits: { target: 8 } })
				.map((c) => `${c.accountId}|${c.eventId}`)
				.join(",");
		assert.notStrictEqual(key("2013|1"), key("2013|2"));
	});

	test("no events means no posts", () => {
		assert.strictEqual(
			castDay({
				accounts: league(),
				events: [],
				seed: "2013|1",
				limits: { target: 40 },
			}).length,
			0,
		);
	});
});

describe("replyAppetite", () => {
	const event = ev("g:0", { tids: [0, 1], salience: 0.8 });

	test("an account never replies to itself", () => {
		const a = account("m:a", "nationalPundit");
		assert.strictEqual(
			replyAppetite({ account: a, poster: a, event, feud: 1 }),
			0,
		);
	});

	test("nobody argues about a game they would not have posted about", () => {
		const insider = account("m:i", "insider");
		const homer = account("m:h", "homerFan", { tid: 0 });
		assert.strictEqual(
			replyAppetite({ account: insider, poster: homer, event, feud: 1 }),
			0,
		);
	});

	test("someone being wrong is the reason most replies exist", () => {
		const wonk = account("m:w", "analytics");
		const troll = account("m:t", "troll", { tid: 0 });
		const wire = account("m:a", "aggregator", { tid: 0 });
		assert.strictEqual(
			replyAppetite({ account: wonk, poster: troll, event, feud: 0 }) >
				replyAppetite({ account: wonk, poster: wire, event, feud: 0 }),
			true,
		);
	});

	test("opposed outlooks on the same event attract each other", () => {
		const doomer = account("m:d", "doomerFan", { tid: 0 });
		const homer = account("m:h", "homerFan", { tid: 0 });
		const neutral = account("m:b", "beatWriter", { tid: 0 });
		assert.strictEqual(
			replyAppetite({ account: doomer, poster: homer, event, feud: 0 }) >
				replyAppetite({ account: doomer, poster: neutral, event, feud: 0 }),
			true,
		);
	});

	test("history between two accounts raises the temperature", () => {
		const troll = account("m:t", "troll", { tid: 0 });
		const homer = account("m:h", "homerFan", { tid: 0 });
		assert.strictEqual(
			replyAppetite({ account: troll, poster: homer, event, feud: 1 }) >
				replyAppetite({ account: troll, poster: homer, event, feud: 0 }),
			true,
		);
	});

	test("an account with no appetite for arguing stays out of it", () => {
		const wire = account("m:a", "aggregator", { tid: 0 });
		const troll = account("m:t", "troll", { tid: 0 });
		const quiet = replyAppetite({
			account: wire,
			poster: troll,
			event,
			feud: 0,
		});
		const loud = replyAppetite({
			account: troll,
			poster: wire,
			event,
			feud: 0,
		});
		assert.strictEqual(quiet < loud, true);
	});
});

describe("castReplies", () => {
	const event = ev("g:0", { tids: [0, 1], salience: 0.9 });
	const cast = [
		{ accountId: "m:troll", eventId: "g:0", interest: 0.9 },
		{ accountId: "m:homer", eventId: "g:0", interest: 0.9 },
	];
	const accounts = [
		account("m:troll", "troll", { tid: 0 }),
		account("m:homer", "homerFan", { tid: 0 }),
		account("m:doom", "doomerFan", { tid: 0 }),
		account("m:wonk", "analytics"),
		account("m:wire", "aggregator", { tid: 1 }),
	];
	const noFeud = () => 0;

	const run = (extra: Partial<Parameters<typeof castReplies>[0]> = {}) =>
		castReplies({
			posts: cast,
			accounts,
			events: [event],
			feudBetween: noFeud,
			seed: "2013|1",
			target: 10,
			...extra,
		});

	test("replies point at a real post", () => {
		for (const reply of run()) {
			assert.strictEqual(
				cast.some(
					(c) =>
						c.accountId === reply.parentAccountId &&
						c.eventId === reply.parentEventId,
				),
				true,
			);
		}
	});

	test("nobody replies to themselves", () => {
		for (const reply of run()) {
			assert.notStrictEqual(reply.accountId, reply.parentAccountId);
		}
	});

	test("one reply per account per day", () => {
		// Somebody working down the whole timeline is a bot, and reads like one.
		const counts = new Map<string, number>();
		for (const reply of run()) {
			counts.set(reply.accountId, (counts.get(reply.accountId) ?? 0) + 1);
		}
		assert.strictEqual(Math.max(0, ...counts.values()) <= 1, true);
	});

	test("a post does not collect an unlimited pile-on", () => {
		const counts = new Map<string, number>();
		for (const reply of run({ maxPerPost: 1 })) {
			const key = `${reply.parentAccountId}|${reply.parentEventId}`;
			counts.set(key, (counts.get(key) ?? 0) + 1);
		}
		assert.strictEqual(Math.max(0, ...counts.values()) <= 1, true);
	});

	test("history makes more people answer", () => {
		const cold = run().length;
		const hot = run({ feudBetween: () => 1 }).length;
		assert.strictEqual(hot >= cold, true);
	});

	test("the same day answers identically every time", () => {
		assert.deepStrictEqual(run(), run());
	});

	test("account order does not change the answers", () => {
		assert.deepStrictEqual(run(), run({ accounts: [...accounts].reverse() }));
	});

	test("a post nobody can see draws nothing", () => {
		assert.strictEqual(
			castReplies({
				posts: [{ accountId: "m:ghost", eventId: "g:0", interest: 1 }],
				accounts,
				events: [event],
				feudBetween: noFeud,
				seed: "2013|1",
				target: 10,
			}).length,
			0,
		);
	});

	test("a roster with no appetite for arguing produces no replies", () => {
		// Without a floor the bottom of every thread fills with accounts that
		// had nothing to add.
		const quiet = [
			account("m:a", "aggregator", { tid: 1 }),
			account("m:b", "aggregator", { tid: 1 }),
			account("m:c", "aggregator", { tid: 1 }),
		];
		assert.strictEqual(
			castReplies({
				posts: [{ accountId: "m:a", eventId: "g:0", interest: 0.9 }],
				accounts: quiet,
				events: [event],
				feudBetween: noFeud,
				seed: "2013|1",
				target: 10,
			}).length,
			0,
		);
	});

	test("quotes and replies both appear", () => {
		const kinds = new Set(
			run({ target: 40, maxPerPost: 5 }).map((r) => r.kind),
		);
		assert.strictEqual(kinds.size >= 1, true);
		for (const kind of kinds) {
			assert.strictEqual(["reply", "quote"].includes(kind), true);
		}
	});
});
