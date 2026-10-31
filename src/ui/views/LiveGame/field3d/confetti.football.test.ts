import { assert, test } from "vitest";
import { confettiFor } from "./confetti.ts";
import type { Play } from "./director.ts";

const HOME = ["#c8102e", "#ffb612", "#ffffff"];
const AWAY = ["#00338d", "#c60c30", "#ffffff"];
const over = (winner?: 0 | 1, title = true) =>
	({
		postgame: {
			from: 10_000,
			shake: 14_000,
			at: { x: 150, y: 80 },
			...(winner !== undefined ? { winner } : {}),
			clinch: { title },
		},
	}) as unknown as Play;

test("confetti for the championship won, in the winners' colors - and for nothing else", () => {
	const won = confettiFor(over(0), [AWAY, HOME])!;
	assert.isAtLeast(won.t, 10_000);
	assert.strictEqual(won.x, 150);
	assert.includeMembers(won.colors, AWAY.slice(0, 2));
	assert.notInclude(won.colors, HOME[0]);
	assert.includeMembers(
		confettiFor(over(1), [AWAY, HOME])!.colors,
		HOME.slice(0, 2),
	);
	// (Any other round won; any other game over; a game not over yet.)
	assert.isUndefined(confettiFor(over(1, false), [AWAY, HOME]));
	const plain = over(1);
	delete plain.postgame!.clinch;
	assert.isUndefined(confettiFor(plain, [AWAY, HOME]));
	assert.isUndefined(confettiFor({} as Play, [AWAY, HOME]));
	assert.isUndefined(confettiFor(undefined, [AWAY, HOME]));
});
