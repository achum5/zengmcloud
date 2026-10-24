import { assert, describe, test } from "vitest";
import { evalBall } from "./evaluate.ts";
import { ballTurn, channelDistance, HELD } from "./gameBall.ts";
import { bodyOf } from "./poses.ts";
import { compile } from "./testGame.ts";

const body = bodyOf();
const bodyFor = () => body;

const DEG = Math.PI / 180;

// A point on the ball, so far round from x toward y and toward z.
const at = (fromX: number, round: number): [number, number, number] => [
	Math.cos(fromX),
	Math.sin(fromX) * Math.sin(round),
	Math.sin(fromX) * Math.cos(round),
];

describe("the game ball", () => {
	// A game ball's eight panels: the channel round its middle, the one at
	// right angles to it, and the curved ones - nearest the middle of the
	// print over it, out near the one at right angles where they cross the
	// middle one, never meeting it.
	test("its channels lie where a game ball's do", () => {
		assert.isBelow(channelDistance(...at(1.1, 0)), 0.01);
		assert.isBelow(channelDistance(0, 0.6, 0.8), 1e-9);
		assert.isBelow(channelDistance(...at(48 * DEG, 90 * DEG)), 0.01);
		assert.isBelow(channelDistance(...at(77 * DEG, 10 * DEG)), 0.01);
		// The middle of the panel the word is on, and of the one between the
		// curved channel and the one at right angles.
		assert.isAbove(channelDistance(...at(24 * DEG, 90 * DEG)), 0.3);
		assert.isAbove(channelDistance(...at(69 * DEG, 90 * DEG)), 0.3);
		// Out near the crossing, the curved channel still clear of the one at
		// right angles.
		assert.isAbove(channelDistance(...at(84 * DEG, 10 * DEG)), 0.08);
	});

	// Whole turns of spin bring it back the way it was in his hands.
	test("a whole turn of spin brings it back round", () => {
		const frame: Parameters<typeof ballTurn>[0] = [
			[1, 0, 0],
			[0, 0.4, 0.92],
			[0, 0.92, -0.4],
		];
		for (const turns of [1, 2, -3]) {
			const t = ballTurn(frame, Math.PI * 2 * turns);
			for (let c = 0; c < 3; c++) {
				for (let k = 0; k < 3; k++) {
					assert.closeTo(t[c]![k]!, HELD[c]![k]!, 1e-9);
				}
			}
		}
		// And half a turn does not.
		const half = ballTurn(frame, Math.PI);
		assert.isBelow(
			half[0][0] * HELD[0][0] +
				half[0][1] * HELD[0][1] +
				half[0][2] * HELD[0][2],
			0.9,
		);
	});

	// A pass comes into the catcher's hands turned whole times over, and a
	// dribble on the move turns over once a bounce - so the ball held is the
	// ball let go, its print where it was, never snapped round as hands take it.
	test("caught or dribbled up into his hand, it has turned whole turns", () => {
		let passes = 0;
		let dribbles = 0;
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 140);
			for (let i = 0; i < tl.ball.length; i++) {
				const seg = tl.ball[i]!;
				if (seg.kind === "fly" && "pid" in seg.to && seg.t1 - seg.t0 > 100) {
					const b = evalBall(tl, seg.t1 - 0.01, bodyFor);
					const turns = (b.roll ?? 0) / (Math.PI * 2);
					assert.closeTo(
						turns,
						Math.round(turns),
						0.01,
						`${seed} pass at ${seg.t0}`,
					);
					assert.notEqual(Math.round(turns), 0);
					passes += 1;
				}
				if (seg.kind === "hold" && seg.style === "dribble") {
					const end = tl.ball[i + 1]?.t0 ?? seg.t0;
					for (let t = seg.t0 + 20; t < end; t += 70) {
						const b = evalBall(tl, t, bodyFor);
						if (b.roll !== undefined) {
							dribbles += 1;
							assert.isAtMost(Math.abs(b.roll), Math.PI * 2 + 1e-9);
						}
					}
				}
			}
		}
		assert.isAbove(passes, 20);
		assert.isAbove(dribbles, 20);
	}, 300_000);
});
