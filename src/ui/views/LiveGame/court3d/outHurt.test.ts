import { assert, test } from "vitest";
import { outHurt } from "./outHurt.ts";

test("out hurt: hurt going into the game and not playing through it", () => {
	assert.isFalse(outHurt({ injury: { type: "Healthy", gamesRemaining: 0 } }));
	assert.isTrue(
		outHurt({ injury: { type: "Sprained Ankle", gamesRemaining: 3 } }),
	);
	// Playing through it, or on the floor tonight: he plays.
	assert.isFalse(
		outHurt({
			injury: {
				type: "Sprained Ankle",
				gamesRemaining: 3,
				playingThrough: true,
			},
		}),
	);
	assert.isFalse(
		outHurt({ injury: { type: "Sprained Ankle", gamesRemaining: 3 }, min: 12 }),
	);
	// Hurt in this game: what he had going into it is what counts.
	assert.isFalse(
		outHurt({
			injury: { type: "Torn ACL", gamesRemaining: 60, newThisGame: true },
		}),
	);
	assert.isTrue(
		outHurt({
			injury: { type: "Torn ACL", gamesRemaining: 60, newThisGame: true },
			injuryAtStart: { type: "Bruised Knee", gamesRemaining: 2 },
		}),
	);
});
