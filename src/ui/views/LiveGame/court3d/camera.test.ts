import { assert, describe, test } from "vitest";
import { courtFit, makeCamera, MAIN_RIG, project } from "./camera.ts";
import { COURT_H, COURT_W } from "./geometry.ts";
import { bodyOf } from "./poses.ts";
import { aimFor, boardAt, momentAt } from "./scene.ts";
import { compile } from "./testGame.ts";

// The games staged for these tests, staged once.
const games = new Map<string, ReturnType<typeof compile>>();
const game = (seed: string) => {
	let g = games.get(seed);
	if (!g) {
		g = compile(seed, 150);
		games.set(seed, g);
	}
	return g;
};

describe("3D camera", () => {
	// However tight the play wants it, both sidelines stay in the picture:
	// the near one above the bottom edge, the far one's players below the top.
	test("the whole floor stays in the picture, sideline to sideline", () => {
		for (const [w, h] of [
			[1280, 720],
			[640, 480],
			[1920, 1080],
		] as const) {
			const fit = courtFit(w / h);
			for (const width of [fit.min, fit.min + 6, fit.min + 20]) {
				for (const x of [20, COURT_W / 2, COURT_W - 20]) {
					const cam = makeCamera({ x, width, y: fit.y(width) }, w, h, MAIN_RIG);
					for (const across of [x - width / 3, x, x + width / 3]) {
						const near = project(cam, { x: across, y: COURT_H, z: 0 });
						const far = project(cam, { x: across, y: 0, z: 7 });
						assert.isBelow(near.y, h, `${w}x${h} ${width}`);
						assert.isAbove(far.y, 0, `${w}x${h} ${width}`);
					}
				}
			}
			// And not zoomed out further than it has to be.
			const tight = makeCamera(
				{ x: COURT_W / 2, width: fit.min, y: fit.y(fit.min) },
				w,
				h,
				MAIN_RIG,
			);
			const near = project(tight, { x: COURT_W / 2, y: COURT_H, z: 0 });
			assert.isAbove(near.y, h * 0.9);
		}
	});

	// At the final buzzer the ball is thrown up and left lying under a
	// basket; the players go to center court to shake hands. The camera goes
	// with them - not parked on the ball, the handshakes at the edge of the
	// picture.
	test("after the final buzzer, the players - not the ball left lying", () => {
		const body = bodyOf();
		const [w, h] = [1280, 720];
		const fit = courtFit(w / h);
		for (const seed of ["a", "b"]) {
			const { tl, players } = game(seed);
			const over = tl.beats.find((b) => b.type === "gameOver")!;
			for (let t = over.actionStart + 4000; t < tl.end; t += 1000) {
				const m = momentAt(tl, t, players, () => body);
				const aim = aimFor(m, false, tl, fit);
				const cam = makeCamera(aim, w, h, MAIN_RIG);
				let sum = 0;
				let n = 0;
				for (const st of m.players) {
					const p = project(cam, { x: st.x, y: st.y, z: 0 });
					assert.isAbove(p.x, w * 0.05, `${seed} @${Math.round(t)}`);
					assert.isBelow(p.x, w * 0.95, `${seed} @${Math.round(t)}`);
					sum += p.x;
					n += 1;
				}
				assert.isBelow(
					Math.abs(sum / n - w / 2),
					w * 0.15,
					`${seed} @${Math.round(t)}`,
				);
			}
		}
	}, 60_000);
});

describe("3D boards", () => {
	// DEFENSE goes up while the road team has the ball in play - not over a
	// timeout or a break between periods, and not once the game is over.
	test("no DEFENSE over a break or after the final buzzer", () => {
		for (const seed of ["a", "b"]) {
			const { tl } = game(seed);
			let looked = 0;
			for (const b of tl.beats) {
				if (!["gameOver", "endOfPeriod", "timeout"].includes(b.type)) {
					continue;
				}
				for (let t = b.actionStart + 3300; t < b.end; t += 500) {
					const screen = boardAt(tl, t);
					assert.notInclude(
						["defense", "defense2"],
						screen,
						`${seed} ${b.type} @${Math.round(t)}`,
					);
					if (b.type === "gameOver") {
						assert.strictEqual(screen, "name");
					}
					looked += 1;
				}
			}
			assert.isAbove(looked, 20);
		}
	}, 60_000);
});
