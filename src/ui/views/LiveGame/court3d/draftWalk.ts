import type { Authored } from "./authored.ts";
import type { Rig } from "./camera.ts";
import type { CourtTimeline, Track } from "./director.ts";
import { evalPlayer, withBody } from "./evaluate.ts";
import {
	COURT_H,
	COURT_W,
	benchX,
	dist,
	type Pt,
	type Pt3,
} from "./geometry.ts";
import { INTRO_LIGHT, TUNNEL_Y } from "./intro.ts";

// A DRAFT PICK WALKS OUT (see authored.ts, HeadlineScene.tsx): the lights
// down in his new team's building, he comes out from in front of its bench
// under a spotlight - in a suit, its cap on his head - and walks to the
// middle of its floor, the crowd on its feet; there he turns to the
// cameras, waves, and stands as the lights come back up. Nobody else on the
// floor; no ball.

// Where he comes from, and where he stops: on the logo at center court.
const FROM: Pt = { x: benchX(1) - 3, y: TUNNEL_Y + 0.6 };
const MARK: Pt = { x: COURT_W / 2, y: COURT_H / 2 - 1 };
// Walking, not hurrying (feet a second).
const WALK_FTPS = 6.2;
// The lights start down as it opens; he is called, and sets off; once he
// is on his mark he turns to the cameras and waves; the lights come back
// up on him standing there, and it holds (ms).
const CALLED = 500;
const SET_OFF = 700;
const WAVE_AFTER = 350;
const WAVE_MS = 1700;
const DARK_AFTER = 500;
const HOLD = 900;

export const DRAFT_WALK = (() => {
	const arrive = SET_OFF + (dist(FROM, MARK) / WALK_FTPS) * 1000;
	const wave = arrive + WAVE_AFTER;
	const lightsUp = wave + WAVE_MS + DARK_AFTER;
	return {
		arrive,
		wave,
		lightsUp,
		ms: lightsUp + INTRO_LIGHT + HOLD,
	};
})();

// The one camera on him: on its stand off the near sideline, a little along
// from the middle of the floor and not much over his head - looking up the
// floor at him as he comes, the far stands behind him.
const CAMERA: Pt3 = { x: MARK.x + 7, y: COURT_H + 19, z: 9.5 };
const RIG: Rig = { back: 0, high: 0, slide: 0, upright: false, at: CAMERA };
// What he turns to, on his mark.
const CAMERAS: Pt = { x: CAMERA.x, y: CAMERA.y };

// CAMERAS GOING OFF IN THE STANDS (see the picture's flashes) across the
// floor from the one on him: a few as he comes out, all over them once he
// is on his mark - each a moment, at a seat of its own.
const FLASH_MS = 80;
// (The far stands' front row, their rake and how far up they go - see
// arena.ts.)
const STANDS_Y = -12;
const WALL_H = 3.6;
const RAKE = (28 * Math.PI) / 180;
const unit = (n: number): number => {
	let h = Math.imul(n ^ 0x9e3779b9, 0x85ebca6b);
	h ^= h >>> 15;
	h = Math.imul(h, 0x2c1b3c6d);
	h ^= h >>> 12;
	return (h >>> 0) / 4294967296;
};
const flashesAt = (
	t: number,
	from: number,
	busy: number,
	to: number,
): Pt3[] => {
	if (t < from || t >= to) {
		return [];
	}
	const slot = Math.floor(t / FLASH_MS);
	const share = t >= busy ? 0.42 : 0.1;
	const out: Pt3[] = [];
	for (let k = 0; k < 3; k++) {
		if (unit(slot * 3 + k) < share) {
			const up = 2 + unit(slot * 7 + k + 1000) * 40;
			out.push({
				x: -6 + unit(slot * 13 + k + 2000) * (COURT_W + 12),
				y: STANDS_Y - up * Math.cos(RAKE),
				z: WALL_H + up * Math.sin(RAKE) + 2.4,
			});
		}
	}
	return out;
};
// No ball in it: kept where no camera looks.
const NO_BALL = { x: -400, y: -400, z: 0.39 };

export const stageDraftWalk = (
	pid: number,
): Pick<Authored, "show" | "ms" | "moment" | "aim" | "crew" | "flashes"> => {
	const { arrive, wave, lightsUp, ms } = DRAFT_WALK;
	const track: Track = {
		pid,
		team: 1,
		start: { ...FROM },
		moves: [
			{
				t0: SET_OFF,
				t1: arrive,
				from: { ...FROM },
				to: { ...MARK },
				anim: "walk",
			},
		],
		acts: [
			{ t0: 0, t1: SET_OFF, anim: "easy" },
			{ t0: arrive, t1: wave, anim: "easy", look: CAMERAS },
			{ t0: wave, t1: wave + WAVE_MS, anim: "waveBall", look: CAMERAS },
			{ t0: wave + WAVE_MS, t1: ms + 60_000, anim: "easy", look: CAMERAS },
		],
		arms: [],
		faces: [],
		looks: [
			[0, { ...MARK }],
			[arrive, CAMERAS],
		],
		shown: [[0, true]],
	};
	const tl: CourtTimeline = {
		tracks: new Map([[pid, track]]),
		ball: [],
		fx: [],
		beats: [],
		poss: [[0, 1]],
		cuts: [],
		shots: [],
		tension: [],
		fast: [],
		// (The lights, the spotlight on him, the home crowd on its feet - see
		// lights.ts and crowdAt.)
		intro: {
			t0: 0,
			t1: lightsUp + INTRO_LIGHT,
			big: false,
			calls: [{ pid, team: 1, t0: CALLED, t1: lightsUp + INTRO_LIGHT }],
		},
		warmup: true,
		end: ms,
	};
	return {
		show: { tl, parts: [tl] },
		ms,
		moment: (t, bodyFor) => ({
			t,
			players: [withBody(evalPlayer(tl, pid, t), bodyFor(pid))],
			ball: NO_BALL,
		}),
		// On him all the way from the one camera: his whole walk in it as he
		// comes out, in closer once he is on his mark.
		aim: (t, narrow, m) => {
			const st = m.players[0];
			const u = Math.min(1, Math.max(0, (t - SET_OFF) / (arrive - SET_OFF)));
			const ease = u * u * (3 - 2 * u);
			// (Head to foot in it, on a squarer picture and a wider one.)
			const wide = narrow ? 21 : 27;
			const close = narrow ? 13.5 : 18;
			const width = wide + (close - wide) * ease;
			// (A little right of the middle of it: his name is up on the left.)
			const aside = width * (narrow ? 0.1 : 0.05);
			return {
				shot: {
					x: (st ? st.x : MARK.x) - aside,
					y: st ? st.y : MARK.y,
					z: 3.7,
					width,
				},
				rig: RIG,
			};
		},
		// Nobody officiating, coaching or warming anybody up: the scorer's
		// table, the photographers and the building's own people.
		crew: (role) => role === "table" || role === "photo" || role === "staff",
		flashes: (t) => flashesAt(t, CALLED, arrive, lightsUp),
	};
};
