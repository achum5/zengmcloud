import { assert, beforeAll, describe, test } from "vitest";
import { KEY_PLAY_DEFAULTS } from "../court3d/keyPlays.ts";
import { compileField, type FieldTimeline } from "./director.ts";
import { planFieldKeyPlays } from "./keyPlays.ts";
import { makeFieldClock, onFieldCursor, stepField } from "./playback.ts";
import { simGames, type SimGame } from "./testGames.ts";

let game: SimGame;
let tl: FieldTimeline;

beforeAll(async () => {
	[game] = (await simGames([21])) as [SimGame];
	tl = compileField({ events: game.events, players: game.roster, gid: 1 });
}, 120_000);

const on = { ...KEY_PLAY_DEFAULTS, on: true };

describe("3D field key plays", () => {
	test("a reel in order: the run out, the plays, the finish, to about the length asked", () => {
		for (const minutes of [2, 5]) {
			const plan = planFieldKeyPlays(
				tl,
				game.events,
				{ ...on, minutes, closePoints: 0 },
				{ rate: 1, numPeriods: 4 },
			);
			assert.strictEqual(plan[0]!.t0, 0);
			assert.strictEqual(plan.at(-1)!.t1, tl.end);
			let ms = 0;
			plan.forEach((s, k) => {
				assert.isBelow(s.t0, s.t1);
				ms += s.t1 - s.t0;
				if (k > 0) {
					assert.isAbove(s.t0, plan[k - 1]!.t1);
					assert.isAtLeast(s.cursor, plan[k - 1]!.cursor);
				}
			});
			assert.isBelow(ms, minutes * 60_000 * 1.15, `${minutes} min`);
			assert.isAbove(ms, minutes * 60_000 * 0.5, `${minutes} min`);
		}
	}, 20_000);

	test("cut to a key play, its line is not up on the page before it happens", () => {
		const plan = planFieldKeyPlays(
			tl,
			game.events,
			{ ...on, minutes: 5 },
			{ rate: 1, numPeriods: 4 },
		);
		for (const s of plan) {
			for (const b of tl.beats) {
				if (b.i < s.cursor) {
					assert.isBelow(b.actionStart, s.t0, `line ${b.i}`);
				}
			}
		}
	}, 20_000);

	test("watched through: every stretch shown, the page on to the end with it", () => {
		const { events } = game;
		const plan = planFieldKeyPlays(
			tl,
			events,
			{ ...on, minutes: 3 },
			{ rate: 1, numPeriods: 4 },
		);
		// (The page: a line at a time when asked, and on to a cursor when the
		// field goes on past what it leaves out.)
		const lineAfter = (c: number) => {
			const b = tl.beats.find((x) => x.i >= c);
			return b ? b.i + 1 : events.length;
		};
		let cursor = 0;
		const input = {
			cursor,
			paused: false,
			rate: 1,
			follower: false,
			keyPlan: plan,
			eventsLength: events.length,
			onReady: () => {
				cursor = lineAfter(cursor);
			},
			onJump: (to: number) => {
				while (cursor < to) {
					cursor = lineAfter(cursor);
				}
			},
		};
		const s = makeFieldClock(false, 0);
		onFieldCursor(s, tl, { cursor, paused: false, skips: 0 });
		const seen = new Set<number>();
		let now = 0;
		let was = cursor;
		while (now < 3_600_000 && !(s.t >= tl.end && cursor >= events.length)) {
			now += 16;
			const before = s.t;
			stepField(s, { ...input, cursor }, tl, now);
			assert.isAtLeast(s.t, before);
			plan.forEach((x, k) => {
				if (s.t >= x.t0 && s.t < x.t1) {
					seen.add(k);
				}
			});
			if (cursor !== was) {
				onFieldCursor(s, tl, { cursor, paused: false, skips: 0 });
				was = cursor;
			}
		}
		assert.strictEqual(cursor, events.length);
		assert.isAtLeast(s.t, tl.end);
		assert.strictEqual(seen.size, plan.length);
		// (About as long as the stretches shown, a replay or two in a close
		// finish on top.)
		const ms = plan.reduce((a, x) => a + x.t1 - x.t0, 0);
		assert.isBelow(now, ms + 120_000);
	}, 60_000);
});
