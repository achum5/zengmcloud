import { assert, describe, test } from "vitest";
import {
	courtFit,
	dodgeFor,
	makeCamera,
	MAIN_RIG,
	operate,
	project,
	type Operator,
} from "./camera.ts";
import { benchX, COURT_H, COURT_W, HUDDLE_Y, type Side } from "./geometry.ts";
import { evalBall } from "./evaluate.ts";
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

	// The hands all shaken, the last look is round the building - the whole
	// of it from up high, the board over center court in it - and the
	// picture ends on that, not on the two lines stood still at center
	// court.
	test("the game over, the last look is round the building", () => {
		const [w, h] = [1280, 720];
		for (const seed of ["a", "b"]) {
			const { tl } = game(seed);
			const over = tl.beats.find((b) => b.type === "gameOver")!;
			const last = tl.shots.at(-1)!;
			const at = `${seed} ${Math.round(last.t0)}`;
			assert.isAbove(last.t0, over.actionStart + 4000, at);
			assert.isAtLeast(tl.end - last.t0, 3000, at);
			for (const t of [last.t0 + 1, (last.t0 + tl.end) / 2, tl.end]) {
				const view = arenaAim(tl, t, false)!;
				assert.isDefined(view, `${at} @${Math.round(t)}`);
				const cam = makeCamera(view.shot, w, h, view.rig);
				// The board, and both baskets.
				const board = project(cam, { x: COURT_W / 2, y: 36, z: 34 });
				assert.isAbove(board.y, h * 0.1, at);
				for (const x of [5.25, COURT_W - 5.25]) {
					const rim = project(cam, { x, y: COURT_H / 2, z: 10 });
					assert.isAbove(rim.x, 0, at);
					assert.isBelow(rim.x, w, at);
				}
			}
			// (Cut to from the game's own picture on the handshakes.)
			assert.isUndefined(arenaAim(tl, last.t0 - 1, false), at);
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

	// On a break, the camera shows where the play is going: the floor ahead
	// of the ball and the rim it is going to - it looks a way on from the
	// ball, not back toward the players trailing the play.
	test("pushed up the floor, the picture leads the ball", () => {
		const body = bodyOf();
		const fit = courtFit(16 / 9);
		let pushes = 0;
		let ahead = 0;
		let lead = 0;
		for (const seed of ["a", "b"]) {
			const { tl, players } = game(seed);
			const fast = (t: number) => tl.fast.some(([a, b]) => t >= a && t < b);
			for (let t = 5000; t < tl.end - 1000; t += 250) {
				if (fast(t) || fast(t + 800)) {
					continue;
				}
				const m = momentAt(tl, t, players, () => body);
				if (m.ball.holder === undefined) {
					continue;
				}
				const later = evalBall(tl, t + 800, () => body);
				const d = later.x - m.ball.x;
				// Up the floor at 15 ft a second or more, away from either end.
				if (Math.abs(d) < 12 || m.ball.x < 30 || m.ball.x > COURT_W - 30) {
					continue;
				}
				const aim = aimFor(m, false, tl, fit);
				pushes += 1;
				lead += (aim.x - m.ball.x) * Math.sign(d);
				if ((aim.x - m.ball.x) * Math.sign(d) > 1) {
					ahead += 1;
				}
			}
		}
		assert.isAtLeast(pushes, 20);
		const msg = `${ahead} of ${pushes}, ${(lead / pushes).toFixed(1)} ft`;
		assert.isAbove(ahead / pushes, 0.9, msg);
		assert.isAbove(lead / pushes, 6, msg);
	}, 120_000);
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

describe("3D graphics over the picture", () => {
	// The score bug along the foot of the picture: the man with the ball
	// gone down behind it, the camera pans till he is out from behind it -
	// and leaves him be up the floor.
	test("the man with the ball kept out from behind the score bug", () => {
		const [w, h] = [1280, 720];
		const fit = courtFit(w / h);
		const block = { x0: 0.26, x1: 0.74, y0: 0.88 };
		const camAt = (x: number) =>
			makeCamera({ x, width: fit.min, y: fit.y(fit.min) }, w, h, MAIN_RIG);
		const behind = (x: number, at: { x: number; y: number }) => {
			const p = project(camAt(x), { ...at, z: 3 });
			return p.y / h > block.y0 && p.x / w > block.x0 && p.x / w < block.x1;
		};
		let moved = 0;
		for (const camX of [40, 47, 54]) {
			for (const at of [
				{ x: camX - 4, y: 49 },
				{ x: camX, y: 50.5 },
				{ x: camX + 5, y: 49.5 },
			]) {
				const [d] = dodgeFor(camAt(camX), at, block, 0);
				if (!behind(camX, at)) {
					continue;
				}
				moved += 1;
				assert.isFalse(behind(camX + d, at), `${camX} ${at.x},${at.y}`);
				// And the basket at his end still in the picture.
				const rim = project(camAt(camX + d), {
					x: at.x < COURT_W / 2 ? 5.25 : COURT_W - 5.25,
					y: COURT_H / 2,
					z: 10,
				});
				assert.isAbove(rim.x, 0);
				assert.isBelow(rim.x, w);
			}
			// Up the floor, left be.
			assert.strictEqual(
				dodgeFor(camAt(camX), { x: camX, y: 20 }, block, 0)[0],
				0,
			);
		}
		assert.isAtLeast(moved, 3);
	});
});

describe("3D looks round the building", () => {
	// Over a timeout, the picture goes in close on a huddle: everybody in it
	// in the picture, head to toe.
	test("over a timeout, in on a huddle", () => {
		const body = bodyOf();
		const [w, h] = [1280, 720];
		let huddles = 0;
		for (const seed of ["a", "b"]) {
			const { tl, players } = game(seed);
			for (const s of tl.shots) {
				const t = (s.t0 + s.t1) / 2;
				const view = arenaAim(tl, t, false)!;
				if (view.rig === MAIN_RIG || s.kind !== "wide") {
					continue;
				}
				const m = momentAt(tl, t, players, () => body);
				const cam = makeCamera(view.shot, w, h, view.rig);
				const at = `${seed} @${Math.round(t)}`;
				// The huddle it is on: the one nearest the middle of the picture.
				const team: Side =
					Math.abs(benchX(0) - view.shot.x) < Math.abs(benchX(1) - view.shot.x)
						? 0
						: 1;
				const huddle = m.players.filter(
					(st) =>
						st.team === team &&
						Math.hypot(st.x - benchX(team), st.y - HUDDLE_Y) < 4,
				);
				assert.isAtLeast(huddle.length, 3, at);
				for (const st of huddle) {
					const feet = project(cam, { ...st, z: 0 });
					const head = project(cam, { ...st, z: 6.8 });
					for (const p of [feet, head]) {
						assert.isAbove(p.x, 0, at);
						assert.isBelow(p.x, w, at);
						assert.isAbove(p.y, 0, at);
						assert.isBelow(p.y, h, at);
					}
				}
				huddles += 1;
			}
		}
		assert.isAbove(huddles, 2);
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
