import { assert, describe, test } from "vitest";
import {
	courtFit,
	makeCamera,
	MAIN_RIG,
	operate,
	project,
	type Operator,
} from "./camera.ts";
import { COURT_H, COURT_W } from "./geometry.ts";
import { bodyOf } from "./poses.ts";
import { aimFor, arenaAim, boardAt, momentAt } from "./scene.ts";
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

	// A free throw is shot from in close - the shooter at the line, the men
	// along the lane and the rim filling the picture, all of them in it -
	// not from as far out as the whole floor takes. Zooming in on it, the
	// aim goes on smoothly from the whole floor's.
	test("a free throw: in close on the shooter, the lane and the rim", () => {
		const body = bodyOf();
		for (const [w, h] of [
			[1280, 720],
			[800, 600],
		] as const) {
			const fit = courtFit(w / h);
			assert.isBelow(
				Math.abs(fit.y(fit.min - 0.3) - fit.y(fit.min)),
				0.3,
				`${w}x${h}`,
			);
			let shots = 0;
			for (const seed of ["a", "b"]) {
				const { tl, players, events } = game(seed);
				for (const b of tl.beats) {
					if (b.type !== "ft" && b.type !== "missFt") {
						continue;
					}
					const shooter = (events[b.i] as { pid?: number }).pid;
					// Set at the line, just before he lets it go.
					const t = b.actionStart - 1500;
					const m = momentAt(tl, t, players, () => body);
					const aim = aimFor(m, false, tl, fit);
					assert.isBelow(aim.width, fit.min * 0.8);
					const cam = makeCamera(aim, w, h, MAIN_RIG);
					const at = `${seed} ${w}x${h} @${Math.round(t)}`;
					const rimX = m.ball.x < COURT_W / 2 ? 5.25 : COURT_W - 5.25;
					const inside = (p: { x: number; y: number }, what: string) => {
						assert.isAbove(p.x, 0, `${what} ${at}`);
						assert.isBelow(p.x, w, `${what} ${at}`);
						assert.isAbove(p.y, 0, `${what} ${at}`);
						assert.isBelow(p.y, h, `${what} ${at}`);
					};
					inside(project(cam, { x: rimX, y: 25, z: 10.4 }), "the rim");
					for (const st of m.players) {
						const lane =
							st.pid === shooter ||
							(Math.abs(st.x - rimX) < 15 && Math.abs(st.y - 25) < 9);
						if (lane) {
							inside(project(cam, { ...st, z: 0 }), `${st.pid}'s feet`);
							inside(project(cam, { ...st, z: 7 }), `${st.pid}'s head`);
						}
					}
					shots += 1;
				}
			}
			assert.isAbove(shots, 6);
		}
	}, 60_000);
});

describe("3D camera operator", () => {
	// The look round the building pans slowly across it (see arenaAim): that
	// pan is seen - not the camera parked where it cut in, the aim moving on
	// without it.
	test("the look round the building pans with it", () => {
		const { tl } = game("a");
		const shot = tl.shots[0]!;
		const op: Operator = {
			camX: 0,
			camW: 0,
			camY: 0,
			camZ: 0,
			camVX: 0,
			camVW: 0,
			camVY: 0,
			camVZ: 0,
		};
		let lag = 0;
		for (let t = shot.t0; t < shot.t1; t += 1000 / 60) {
			const view = arenaAim(tl, t, false)!;
			operate(op, view.shot, undefined, 1 / 60, t === shot.t0);
			lag = Math.max(lag, Math.abs(op.camX - view.shot.x));
		}
		const from = arenaAim(tl, shot.t0, false)!.shot.x;
		const to = arenaAim(tl, shot.t1 - 1, false)!.shot.x;
		assert.isAbove(Math.abs(to - from), 8);
		assert.isBelow(lag, 2);
	});

	// Following the play, never tighter than the whole floor - but for a
	// shot that asks to be (a free throw) - however fast it zooms in;
	// never past the stands at either end; aimed across the floor as the
	// whole floor needs.
	test("following the play, the whole floor in the picture", () => {
		const fit = courtFit(16 / 9);
		const op: Operator = {
			camX: 47,
			camW: 120,
			camY: 0,
			camZ: 0,
			camVX: 0,
			camVW: -400,
			camVY: 0,
			camVZ: 0,
		};
		for (const [x, width] of [
			[-40, 60],
			[47, 30],
			[140, 80],
		] as const) {
			for (let i = 0; i < 240; i++) {
				operate(op, { x, width, y: 99 }, fit, 1 / 60, false);
				assert.isAtLeast(op.camW, Math.min(fit.min, width) - 1e-9);
				assert.isAtLeast(op.camX, op.camW / 2 - 12 - 1e-9);
				assert.isAtMost(op.camX, COURT_W + 12 - op.camW / 2 + 1e-9);
				assert.closeTo(op.camY, fit.y(op.camW), 1e-9);
			}
			assert.closeTo(op.camW, Math.max(width, Math.min(fit.min, width)), 0.5);
		}
	});
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
