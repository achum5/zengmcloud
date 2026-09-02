import { assert, describe, test } from "vitest";
import {
	BUILT_IN_ARCHETYPES,
	resolvePersonality,
} from "./socialPersonality.ts";
import { NO_QUIRKS } from "./socialQuirks.ts";
import type { ResolvedSocialAccount } from "./socialAccounts.ts";
import {
	engagementFor,
	formatCount,
	formatReach,
	isVerified,
	reachOf,
	timeOf,
} from "./socialMetrics.ts";

const account = (
	archetypeId: string,
	id = archetypeId,
): ResolvedSocialAccount => ({
	id,
	kind: archetypeId === "player" ? "player" : "media",
	handle: id,
	name: id,
	bio: "",
	archetypeId,
	personality: resolvePersonality({
		archetype: BUILT_IN_ARCHETYPES.find((a) => a.id === archetypeId),
		override: undefined,
	}),
	quirks: NO_QUIRKS,
	implicit: true,
});

describe("reach", () => {
	test("is stable for an account and different between accounts", () => {
		const a = account("beatWriter", "m:a");
		const b = account("beatWriter", "m:b");
		assert.strictEqual(reachOf(a), reachOf(a));
		assert.notStrictEqual(reachOf(a), reachOf(b));
	});

	test("orders the league the way it should be ordered", () => {
		// A national insider outdraws a local beat writer outdraws a fan, and
		// no amount of the per-account spread should be able to invert that.
		let insiderWins = 0;
		for (let i = 0; i < 40; i++) {
			const insider = reachOf(account("insider", `m:i${i}`));
			const beat = reachOf(account("beatWriter", `m:b${i}`));
			const fan = reachOf(account("homerFan", `m:f${i}`));
			if (insider > beat && beat > fan) {
				insiderWins += 1;
			}
		}
		assert.strictEqual(insiderWins, 40);
	});

	test("a star is an order of magnitude above a bench player", () => {
		const star = reachOf(account("player", "p:1"), 1);
		const bench = reachOf(account("player", "p:1"), 0.1);
		assert.ok(star > bench * 20, `${star} vs ${bench}`);
	});
});

describe("verification", () => {
	test("goes to institutions and players, never to fan accounts", () => {
		assert.strictEqual(isVerified(account("insider")), true);
		assert.strictEqual(isVerified(account("teamOfficial")), true);
		assert.strictEqual(isVerified(account("player")), true);
		assert.strictEqual(isVerified(account("homerFan")), false);
		assert.strictEqual(isVerified(account("doomerFan")), false);
		assert.strictEqual(isVerified(account("troll")), false);
	});
});

describe("engagement", () => {
	const of = (seed: string, salience = 0.6) =>
		engagementFor({
			account: account("beatWriter"),
			reach: 150_000,
			salience,
			seed,
		});

	test("is skewed, not uniform", () => {
		// Most posts do nothing and a few run away. A flat distribution is the
		// giveaway that the numbers were generated rather than earned.
		const likes = Array.from({ length: 300 }, (_, i) => of(`s${i}`).likes).sort(
			(a, b) => a - b,
		);
		const median = likes[150]!;
		const top = likes[297]!;
		assert.ok(top > median * 5, `top ${top} vs median ${median}`);
	});

	test("a bigger moment draws more than a smaller one", () => {
		let bigger = 0;
		for (let i = 0; i < 60; i++) {
			if (of(`x${i}`, 1).likes > of(`x${i}`, 0.1).likes) {
				bigger += 1;
			}
		}
		assert.strictEqual(bigger, 60);
	});

	test("a reply never runs away from the post it answers", () => {
		for (let i = 0; i < 80; i++) {
			const reply = engagementFor({
				account: account("player", "p:9"),
				reach: 3_000_000,
				salience: 0.9,
				seed: `r${i}`,
				isReply: true,
				parentLikes: 20,
			});
			assert.ok(reply.likes <= 60, `${reply.likes} on a 20-like post`);
		}
	});

	test("people like more than they repost", () => {
		for (let i = 0; i < 60; i++) {
			const e = of(`y${i}`);
			assert.ok(e.reposts <= e.likes, `${e.reposts} reposts, ${e.likes} likes`);
		}
	});
});

describe("the clock", () => {
	test("a slate spreads across the evening", () => {
		// Every post landing in the same twenty minutes reads as one
		// simultaneous event rather than as a night of basketball.
		const times: number[] = [];
		for (let i = 0; i < 12; i++) {
			times.push(
				timeOf({
					eventIndex: i,
					eventCount: 12,
					isGame: true,
					seed: `g${i}`,
				}).minutes,
			);
		}
		const spread = Math.max(...times) - Math.min(...times);
		assert.ok(spread > 120, `only ${spread} minutes apart`);
		assert.ok(Math.min(...times) >= 19 * 60, "a game finished before 7pm");
		assert.ok(Math.max(...times) < 24 * 60, "a game finished after midnight");
	});

	test("news lands during the day, games at night", () => {
		for (let i = 0; i < 40; i++) {
			const news = timeOf({
				eventIndex: i,
				eventCount: 40,
				isGame: false,
				seed: `n${i}`,
			});
			assert.ok(news.minutes >= 9 * 60 && news.minutes <= 18 * 60, news.label);
		}
	});

	test("the same post always gets the same time", () => {
		const args = {
			eventIndex: 3,
			eventCount: 9,
			isGame: true,
			seed: "stable",
		};
		assert.strictEqual(timeOf(args).label, timeOf(args).label);
	});
});

describe("formatting", () => {
	test("counts read the way a client writes them", () => {
		assert.strictEqual(formatCount(7), "7");
		assert.strictEqual(formatCount(1200), "1.2K");
		assert.strictEqual(formatCount(38_000), "38K");
		assert.strictEqual(formatCount(2_400_000), "2.4M");
		assert.strictEqual(formatReach(950), "950");
		assert.strictEqual(formatReach(302_000), "302K");
		assert.strictEqual(formatReach(1_500_000), "1.5M");
	});
});
