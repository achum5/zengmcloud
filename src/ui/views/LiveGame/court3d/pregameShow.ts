import { MAIN_RIG, type Rig, type Shot } from "./camera.ts";
import type { CourtPlayer } from "./director.ts";
import { COURT_H, COURT_W, type Pt3 } from "./geometry.ts";
import { stageWarmup, WARMUP_MS, type Warmup } from "./pregame.ts";

// THE PREGAME SHOW, while the game itself is still being staged (a phone
// takes the better part of a minute over a whole game - see stageCourt.ts):
// the matchup board up (see ShowCards.tsx), the way the broadcasts put the
// two teams up before a tip, over looks round the building - the two teams
// out warming up under it, the lights up. Once the game is staged and the
// board has been up long enough to read, the game, from its tip. (It can be
// skipped from when the game is staged.)

export type ShowPlan = {
	// Both teams warming up, under the board.
	warm: Warmup;
	// Which of the looks round the building goes first.
	lookFrom: number;
};

// A little seeded random, the same on every device for the same game.
const rngOf = (seed: number) => {
	let a = (seed * 2654435761) >>> 0 || 1;
	return () => {
		a = (a + 0x6d2b79f5) >>> 0;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

// THE CAMERAS: looks round the building, each held a while, drifting - from
// up where the game is shown from, the whole floor small under the stands;
// along the far side, up at the stands and the ribbon; down the floor at
// the stands behind a basket.
const LOOKS = 3;
const LOOK_MS = 7000;

export const planShow = (players: CourtPlayer[], seed: number): ShowPlan => ({
	warm: stageWarmup(players, seed),
	lookFrom: Math.floor(rngOf(seed)() * LOOKS),
});

// The warm-ups at the show's t: round again from the top if the game takes
// longer than they were worked out for.
export const warmAt = (t: number): number => t % WARMUP_MS;

// The board up at least this long (ms), the game staged or not.
export const BOARD_MS = 10_000;

// When the show is over (its own time): never, till the game is staged -
// `readyAt`, the show's time when it was.
export const showEndsAt = (readyAt: number | undefined): number =>
	readyAt === undefined ? Infinity : Math.max(BOARD_MS, readyAt);

const ease = (u: number) => {
	const c = Math.min(1, Math.max(0, u));
	return c * c * (3 - 2 * c);
};
const lerp = (a: number, b: number, u: number) => a + (b - a) * u;
const MID = COURT_W / 2;

type Aim = { shot: Shot; rig: Rig };
const from = (at: Pt3, look: Pt3, width: number): Aim => ({
	shot: { x: look.x, y: look.y, z: look.z, width },
	rig: { back: 0, high: 0, slide: 0, upright: false, at },
});

export const showAim = (plan: ShowPlan, t: number, narrow: boolean): Aim => {
	const i = Math.floor(Math.max(0, t) / LOOK_MS);
	const u = ease((Math.max(0, t) % LOOK_MS) / LOOK_MS);
	switch ((i + plan.lookFrom) % LOOKS) {
		case 0:
			return {
				shot: {
					x: lerp(MID - 8, MID + 8, u),
					width: narrow ? 130 : 150,
					y: narrow ? -30 : -20,
				},
				rig: MAIN_RIG,
			};
		case 1:
			return from(
				{ x: lerp(20, 74, u), y: COURT_H + 10, z: 22 },
				{ x: lerp(26, 68, u), y: -14, z: 16 },
				narrow ? 70 : 90,
			);
		default:
			// (From up behind the other basket, over the warm-ups, so nobody
			// gets between it and the stands.)
			return from(
				{ x: COURT_W + 8, y: COURT_H / 2 + lerp(7, -7, u), z: 25 },
				{ x: -20, y: COURT_H / 2, z: 12.5 },
				narrow ? 70 : 84,
			);
	}
};
