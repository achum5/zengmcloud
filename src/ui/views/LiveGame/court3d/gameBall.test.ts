import { assert, describe, test } from "vitest";
import {
	ballFaceAt,
	bodyPoint,
	evalBall,
	evalPlayer,
	fastAt,
	poseOf,
	withBody,
} from "./evaluate.ts";
import { ballTurn, channelDistance, facingCamera, HELD } from "./gameBall.ts";
import { bodyOf, skeleton } from "./poses.ts";
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

	// Its print turned round with the man who has it as he turns, and kept
	// as it was as it leaves his hands, through the air, off the floor and
	// into the next man's: never all at once some new way round.
	test("it faces as it did as it changes hands, and turns with the man who has it", () => {
		const { tl } = compile("a", 140);
		const gap = (a: number, b: number) =>
			Math.abs(Math.atan2(Math.sin(a - b), Math.cos(a - b)));
		let changes = 0;
		let holds = 0;
		for (let i = 1; i < tl.ball.length; i++) {
			const seg = tl.ball[i]!;
			const before = ballFaceAt(tl, seg.t0 - 0.01);
			const after = ballFaceAt(tl, seg.t0 + 0.01);
			assert.isBelow(gap(before, after), 0.02, `at ${seg.t0}`);
			changes += 1;
			const end = tl.ball[i + 1]?.t0;
			if (seg.kind === "hold" && end !== undefined && end - seg.t0 > 300) {
				const t = (seg.t0 + end) / 2;
				const yaw0 = evalPlayer(tl, seg.pid, seg.t0 + 1).yaw;
				const yaw1 = evalPlayer(tl, seg.pid, t).yaw;
				assert.isBelow(
					gap(ballFaceAt(tl, t) - ballFaceAt(tl, seg.t0 + 1), yaw1 - yaw0),
					1e-6,
				);
				holds += 1;
			}
		}
		assert.isAbove(changes, 300);
		assert.isAbove(holds, 100);
		// Facing the camera, it is as HELD has it.
		const frame: Parameters<typeof ballTurn>[0] = [
			[0.6, 0.8, 0],
			[0, 0, 1],
			[0.8, -0.6, 0],
		];
		const t = ballTurn(frame, 0, 0, facingCamera({ x: 0.6, y: 0.8 }));
		for (let c = 0; c < 3; c++) {
			for (let k = 0; k < 3; k++) {
				assert.closeTo(t[c]![k]!, HELD[c]![k]!, 1e-9);
			}
		}
	}, 300_000);

	// Thrown past a man in the way - between the passer and his man, out of
	// bounds past him, dropping out of the net past a man under it - it goes
	// by his ear or past his shoulder, never through his head or his body.
	// (Before: 8 in a game.)
	test("in the air, it goes by a man in the way, not through him", () => {
		const { tl } = compile("a", 140);
		let flights = 0;
		const through: string[] = [];
		for (const seg of tl.ball) {
			if (
				seg.kind !== "fly" ||
				fastAt(tl, seg.t0) > 1 ||
				fastAt(tl, seg.t1) > 1
			) {
				continue;
			}
			flights += 1;
			const ends = new Set<number>();
			for (const e of [seg.from, seg.to]) {
				if ("pid" in e) {
					ends.add(e.pid);
				}
			}
			let hit = false;
			for (let t = seg.t0 + 60; t < seg.t1 - 60 && !hit; t += 16) {
				const ball = evalBall(tl, t, bodyFor);
				for (const tr of tl.tracks.values()) {
					const st = evalPlayer(tl, tr.pid, t);
					if (
						ends.has(tr.pid) ||
						!st.shown ||
						Math.hypot(st.x - ball.x, st.y - ball.y) > 3
					) {
						continue;
					}
					const me = withBody(st, body);
					const sk = skeleton(body, poseOf(me));
					const head = bodyPoint(me, sk.head);
					const hip = bodyPoint(me, sk.pelvis);
					const chest = bodyPoint(me, sk.chest);
					const up = {
						x: chest.x - hip.x,
						y: chest.y - hip.y,
						z: chest.z - hip.z,
					};
					const k = Math.max(
						0,
						Math.min(
							1,
							((ball.x - hip.x) * up.x +
								(ball.y - hip.y) * up.y +
								(ball.z - hip.z) * up.z) /
								(up.x ** 2 + up.y ** 2 + up.z ** 2),
						),
					);
					const spine = Math.hypot(
						ball.x - hip.x - up.x * k,
						ball.y - hip.y - up.y * k,
						ball.z - hip.z - up.z * k,
					);
					if (
						Math.hypot(ball.x - head.x, ball.y - head.y, ball.z - head.z) <
							0.75 ||
						spine < 0.7
					) {
						hit = true;
						through.push(`${Math.round(t)} through ${tr.pid}`);
						break;
					}
				}
			}
		}
		assert.isAbove(flights, 300);
		assert.isAtMost(through.length, 1, through.join("; "));
	}, 300_000);
});
