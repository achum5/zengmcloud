import { assert, describe, test } from "vitest";
import { healedForward } from "./healInjuriesForward.ts";

const player = (pid: number, type: string, gamesRemaining: number) => ({
	pid,
	injury: { type, gamesRemaining },
});

describe("healedForward", () => {
	// THE BUG THIS EXISTS FOR. The rows handed to the line-makers come from
	// getCopies.players(..., "noCopyCache"), which returns the live cache
	// records. Healing them in place put a hypothetical about next week into
	// the actual league and published it to every device in the room.
	test("never touches what it was given", () => {
		const players = [
			player(1, "Sprained Ankle", 8),
			player(2, "Sore Elbow", 3),
			player(3, "Torn ACL", 90),
		];
		const before = JSON.stringify(players);
		healedForward(players, 8);
		assert.strictEqual(JSON.stringify(players), before);
	});

	test("a player still hurt on the day keeps his injury, minus the days", () => {
		const [p] = healedForward([player(1, "Torn ACL", 90)], 8);
		assert.deepStrictEqual(p!.injury, {
			type: "Torn ACL",
			gamesRemaining: 82,
		});
	});

	// The zombie state - a real injury type sitting on a zeroed counter - is
	// what made the field incident so hard to recognise. Everything that asks
	// "is he hurt" asks gamesRemaining, so he was instantly available, and the
	// next day's countdown then healed him for good.
	test("a player recovered by then is healthy, not a type at zero games", () => {
		for (const days of [8, 9, 40]) {
			const [p] = healedForward([player(1, "Sprained Ankle", 8)], days);
			assert.deepStrictEqual(
				p!.injury,
				{ type: "Healthy", gamesRemaining: 0 },
				`${days} days`,
			);
		}
	});

	test("today's game heals nobody", () => {
		const players = [player(1, "Sprained Ankle", 8)];
		const out = healedForward(players, 0);
		assert.deepStrictEqual(out[0]!.injury, {
			type: "Sprained Ankle",
			gamesRemaining: 8,
		});
		// Still a different array, so a caller sorting it cannot reorder the
		// caller's own list.
		assert.notStrictEqual(out, players);
	});

	test("a healthy player is passed straight through", () => {
		const players = [player(1, "Healthy", 0)];
		const out = healedForward(players, 5);
		assert.strictEqual(out[0], players[0]);
	});

	test("every player is accounted for, in order", () => {
		const players = [
			player(1, "Healthy", 0),
			player(2, "Sore Elbow", 3),
			player(3, "Torn ACL", 90),
		];
		assert.deepStrictEqual(
			healedForward(players, 4).map((p) => p.pid),
			[1, 2, 3],
		);
	});

	test("a nonsense day count is treated as no days at all", () => {
		const players = [player(1, "Sprained Ankle", 8)];
		for (const days of [-3, Number.NaN]) {
			assert.deepStrictEqual(healedForward(players, days)[0]!.injury, {
				type: "Sprained Ankle",
				gamesRemaining: 8,
			});
		}
	});
});
