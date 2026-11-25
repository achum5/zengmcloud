import { MAIN_RIG, type Rig, type Shot } from "./camera.ts";
import type { CourtTimeline } from "./director.ts";
import { COURT_H, COURT_W, type Pt3 } from "./geometry.ts";
import {
	INTRO_LIGHT,
	ROW_Y,
	type Intro,
	type IntroCall,
	rowSpot,
} from "./intro.ts";
import type { Moment } from "./scene.ts";

// THE PREGAME SHOW, while the game itself is still being staged (a phone
// takes the better part of a minute over a whole game - see stageCourt.ts).
// It used to sit on the warm-ups; now the building does what it does before
// a tip, the way NBA 2K and the broadcasts show it: the lights go down and
// the building opens in the dark - fire along the floor, beams swept over
// it, or one light on the logo at center court - and the starters are
// called out, the road team's quickly and the home team's each in close as
// he gets to his place in the line, slapping hands down it. All ten out,
// they stand in their lines while the broadcast's cards go up - the
// matchup, the two starting fives, the key matchup - and the lights come up
// for the tip. Once the game is staged it can be skipped; if it is not
// staged by the end, the lines stand on with the cards going round until it
// is.
//
// The lineups are the director's own (see intro.ts and the director's
// callLineups): the opening tip staged on its own with them called first,
// which takes a moment, not the minute the whole game does. The game itself
// is staged without them and starts from the tip.

// The game's play-by-play up to its opening tip: all the show stages.
export const showEvents = (events: any[]): any[] | undefined => {
	const k = events.findIndex((e) => e?.type === "jumpBall");
	return k < 0 ? undefined : events.slice(0, k + 1);
};

// How the building opens, in the dark, before the first name.
export const SHOW_OPENS = ["pyro", "beams", "logo"] as const;
export type ShowOpen = (typeof SHOW_OPENS)[number];

// The broadcast's cards over the lines (see ShowCards.tsx).
export type ShowCardKind = "matchup" | "lineups" | "keyMatchup";

export type ShowPlan = {
	// The opening tip, staged on its own with the lineups called first.
	tl: CourtTimeline;
	intro: Intro;
	big: boolean;
	open: ShowOpen;
	// Which way the close-ups are taken from (each home starter's from his
	// left or his right), and which of the looks at the lines goes first.
	flip: boolean;
	holdFrom: number;
	cards: ShowCardKind[];
	// On the timeline: the lines stood waiting, [h0, h1] - the cards go up
	// from h0, and the lights start back up at h1.
	h0: number;
	h1: number;
	// The first name called.
	first: number;
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

// Settled in their lines this long after the last man is called (the last
// one, the star as often as not, plays to the crowd first - see callOut).
const SETTLED = 1600;

export const planShow = (
	tl: CourtTimeline,
	seed: number,
): ShowPlan | undefined => {
	const intro = tl.intro;
	const first = intro?.calls[0];
	const last = intro?.calls.at(-1);
	if (!intro || !first || !last) {
		return undefined;
	}
	const r = rngOf(seed);
	const open = SHOW_OPENS[Math.floor(r() * SHOW_OPENS.length)]!;
	const flip = r() < 0.5;
	const holdFrom = Math.floor(r() * HOLD_LOOKS);
	// (The key matchup before the two fives, some nights. Left out where
	// there is none to show - see cardAt.)
	const cards: ShowCardKind[] =
		r() < 0.5
			? ["matchup", "keyMatchup", "lineups"]
			: ["matchup", "lineups", "keyMatchup"];
	const h1 = intro.t1 - INTRO_LIGHT;
	return {
		tl,
		intro,
		big: intro.big,
		open,
		flip,
		holdFrom,
		cards,
		h0: Math.min(h1 - 1000, last.t1 + SETTLED),
		h1,
		first: first.t0,
	};
};

// THE SHOW'S CLOCK. Up to the lines stood waiting, the show is the
// timeline as staged. Then, the game not staged yet, the lines stand on -
// the timeline run back and forth over the time they stand there, nobody
// moving much - for as long as it takes. Once it is, the show goes on from
// wherever it had got to, through the rest of the wait and the lights
// coming up.
export type ShowClock = {
	// The show's time when the game was there to go on to, and the
	// timeline's then.
	readyAt?: number;
	from?: number;
};

const waitingAt = (plan: ShowPlan, t: number): number => {
	if (t < plan.h0) {
		return t;
	}
	const span = Math.max(1, plan.h1 - 200 - plan.h0);
	const u = (t - plan.h0) % (2 * span);
	return plan.h0 + (u < span ? u : 2 * span - u);
};

// The timeline's time at the show's t; `ready`, the game staged.
export const showTlAt = (
	plan: ShowPlan,
	clock: ShowClock,
	t: number,
	ready: boolean,
): number => {
	if (ready && clock.readyAt === undefined) {
		clock.readyAt = t;
		clock.from = waitingAt(plan, t);
	}
	if (clock.readyAt === undefined || clock.from === undefined) {
		return waitingAt(plan, t);
	}
	return Math.min(plan.intro.t1, clock.from + Math.max(0, t - clock.readyAt));
};

// When the show is over (its own time): never, till the game is staged.
export const showEndsAt = (plan: ShowPlan, clock: ShowClock): number =>
	clock.readyAt === undefined || clock.from === undefined
		? Infinity
		: clock.readyAt + Math.max(0, plan.intro.t1 - clock.from);

// THE CARDS, by the show's own time (they keep going round while it
// waits): each up for its share of the time the lines stand there, the
// next straight after.
const cardsOf = (plan: ShowPlan, leaders: boolean) =>
	leaders ? plan.cards : plan.cards.filter((c) => c !== "keyMatchup");
export const cardMs = (plan: ShowPlan, leaders: boolean): number =>
	Math.max(3600, (plan.h1 - plan.h0) / cardsOf(plan, leaders).length);
export const cardAt = (
	plan: ShowPlan,
	t: number,
	tlT: number,
	// Each team's leading scorer there to show (the key matchup).
	leaders: boolean,
): { kind: ShowCardKind; k: number } | undefined => {
	const cards = cardsOf(plan, leaders);
	if (t < plan.h0 || tlT >= plan.h1 || cards.length === 0) {
		return undefined;
	}
	const k = Math.floor((t - plan.h0) / cardMs(plan, leaders));
	return { kind: cards[k % cards.length]!, k };
};

// The title, "STARTING LINEUPS", for this long before the first name.
export const TITLE_MS = 3200;

// THE CAMERAS.
const ease = (u: number) => {
	const c = Math.min(1, Math.max(0, u));
	return c * c * (3 - 2 * c);
};
const lerp = (a: number, b: number, u: number) => a + (b - a) * u;
const MID = COURT_W / 2;

type Aim = { shot: Shot; rig: Rig; close?: boolean };
const from = (at: Pt3, look: Pt3, width: number, close?: boolean): Aim => ({
	shot: { x: look.x, y: look.y, z: look.z, width },
	rig: { back: 0, high: 0, slide: 0, upright: false, at },
	...(close ? { close } : {}),
});

// The opening, in the dark: two looks, each its own way for each opening.
const openAim = (plan: ShowPlan, t: number, narrow: boolean): Aim => {
	const half = plan.first * 0.52;
	const a = t < half;
	const u = a ? t / half : (t - half) / (plan.first - half);
	const k = ease(u);
	switch (plan.open) {
		case "pyro":
			return a
				? // Low in the near corner, looking across the floor at the fire
					// going up along the far side.
					from(
						{ x: lerp(10, 16, k), y: COURT_H + 9, z: 6 },
						{ x: lerp(40, 54, k), y: -2, z: 7 },
						narrow ? 62 : 84,
					)
				: // Up over the whole floor, pushing in.
					{
						shot: {
							x: MID,
							width: lerp(narrow ? 150 : 170, narrow ? 112 : 128, k),
							y: narrow ? -30 : -20,
						},
						rig: MAIN_RIG,
					};
		case "beams":
			return a
				? // High in the near corner, looking down across the floor,
					// the beams swept over it - drifting round.
					from(
						{ x: lerp(-6, 6, k), y: COURT_H + 14, z: lerp(24, 18, k) },
						{ x: lerp(32, 44, k), y: 20, z: 0 },
						narrow ? 64 : 84,
					)
				: // The whole floor, the beams swept over it.
					{
						shot: {
							x: lerp(MID - 6, MID + 6, k),
							width: narrow ? 130 : 150,
							y: narrow ? -30 : -20,
						},
						rig: MAIN_RIG,
					};
		default:
			return a
				? // High over the near side - under the board over center
					// court - looking down on the one light on the logo there,
					// pushing in.
					from(
						{ x: MID, y: COURT_H + 6, z: lerp(32, 26, k) },
						{ x: MID, y: COURT_H / 2, z: 0 },
						lerp(narrow ? 44 : 56, narrow ? 28 : 36, k),
					)
				: // Low along the near sideline, the floor going by.
					from(
						{ x: lerp(18, 60, k), y: COURT_H + 7, z: 3.4 },
						{ x: lerp(28, 70, k), y: 12, z: 4 },
						narrow ? 56 : 70,
					);
	}
};

// The man called, in the picture as the broadcasts have him: from the
// main camera's side, in closer than the game, while he runs out; then -
// a home starter - in close in front of the line, once he has got to it,
// slapping hands with the man before him and on into his place, turned
// to the cameras.
const CLOSE_FROM = 0.4;
const callAim = (
	plan: ShowPlan,
	c: IntroCall,
	m: Moment,
	tlT: number,
	narrow: boolean,
): Aim => {
	const st = m.players.find((p) => p.pid === c.pid);
	const k = plan.intro.calls.filter((x) => x.team === c.team).indexOf(c);
	const spot = rowSpot(c.team, Math.max(0, k));
	const x = st?.x ?? spot.x;
	const len = c.t1 - c.t0;
	const there = (st?.y ?? spot.y) >= ROW_Y - 1.5;
	if (c.team === 1 && there && tlT >= c.t0 + len * CLOSE_FROM) {
		// (Each from the other side, and the last - the star - from a little
		// lower, on the side away from the middle of the floor and the
		// officials.)
		const star = k === 4;
		const side = star ? 1 : (k % 2 === 0) === plan.flip ? 1 : -1;
		const u = ease((tlT - c.t0 - len * CLOSE_FROM) / (len * (1 - CLOSE_FROM)));
		return from(
			{
				x: x + side * lerp(3.4, 2.2, u),
				y: ROW_Y + lerp(12.5, 10.5, u),
				z: star ? 4.5 : 5.4,
			},
			{ x, y: ROW_Y, z: star ? 6.1 : 5.9 },
			(narrow ? 7.4 : 9.8) * (star ? 1.12 : 1),
			true,
		);
	}
	return {
		shot: { x, width: narrow ? 34 : 42, y: 4 },
		rig: MAIN_RIG,
	};
};

// All ten out, the cards up: a look at the two lines, then another, one to
// each card - from up where the game is shown from, low in front of them,
// and from round behind the home team's line.
const HOLD_LOOKS = 3;
const holdAim = (
	plan: ShowPlan,
	t: number,
	narrow: boolean,
	leaders: boolean,
): Aim => {
	const ms = cardMs(plan, leaders);
	const i = Math.max(0, Math.floor((t - plan.h0) / ms));
	const u = ease(((t - plan.h0) % ms) / ms);
	switch ((i + plan.holdFrom) % HOLD_LOOKS) {
		case 0:
			return {
				shot: { x: lerp(MID - 3, MID + 3, u), width: narrow ? 66 : 84, y: 12 },
				rig: MAIN_RIG,
			};
		case 1:
			return from(
				{ x: lerp(MID - 6, MID + 6, u), y: ROW_Y + 26, z: 4.6 },
				{ x: MID, y: ROW_Y, z: 5 },
				narrow ? 40 : 54,
			);
		default:
			return from(
				{ x: COURT_W - 6, y: lerp(36, 30, u), z: 7 },
				{ x: MID + 4, y: ROW_Y, z: 4.6 },
				narrow ? 36 : 48,
			);
	}
};

// Where the camera is at the show's t (the timeline at tlT).
export const showAim = (
	plan: ShowPlan,
	t: number,
	tlT: number,
	m: Moment,
	narrow: boolean,
	leaders: boolean,
): Aim => {
	if (tlT >= plan.h1) {
		// The lights back up: the game's own picture, for the tip.
		return {
			shot: { x: MID, width: narrow ? 92 : 112, y: narrow ? -6 : 0 },
			rig: MAIN_RIG,
		};
	}
	if (t >= plan.h0) {
		return holdAim(plan, t, narrow, leaders);
	}
	if (tlT < plan.first) {
		return openAim(plan, tlT, narrow);
	}
	const c =
		plan.intro.calls.find((x) => tlT >= x.t0 && tlT < x.t1) ??
		plan.intro.calls.at(-1)!;
	return callAim(plan, c, m, tlT, narrow);
};

// THE BUILDING'S OWN SHOW (drawn over the dark - see showFx.ts): what is
// going off at the timeline's t.
export type ShowFx = {
	// How dark it is (0..1, the lights' own: see intro.ts's darkAt).
	dark: number;
	// Fire going up off the floor: where, and how far up (0..1).
	jets: { at: Pt3; k: number }[];
	// Beams swept over the floor from up in the roof: where each comes down.
	beams: { from: Pt3; to: Pt3; color: string }[];
	// One light on the logo at center court (0..1).
	logo: number;
	// The crowd's phones held up, lit.
	phones: boolean;
};

// The fire: four jets along the far side, either side of the table, and
// one each corner - up in bursts, the first as the lights go down.
const JETS: Pt3[] = [
	{ x: 20, y: -1.6, z: 0 },
	{ x: 33, y: -1.6, z: 0 },
	{ x: COURT_W - 33, y: -1.6, z: 0 },
	{ x: COURT_W - 20, y: -1.6, z: 0 },
	{ x: -2.5, y: -1.6, z: 0 },
	{ x: COURT_W + 2.5, y: -1.6, z: 0 },
];
// Each burst: when (ms into the opening, or after the last home starter
// gets to his place), and how long it burns.
const BURSTS = [1300, 2500, 3300, 4700, 6200];
const BURN = 900;
const burstK = (t: number, at: number) => {
	const u = (t - at) / BURN;
	return u <= 0 || u >= 1 ? 0 : Math.sin(Math.PI * Math.min(1, u * 1.6)) ** 0.6;
};

export const showFxAt = (
	plan: ShowPlan,
	tlT: number,
	dark: number,
	colors: [string, string],
): ShowFx => {
	const opening = tlT < plan.first;
	const jets: ShowFx["jets"] = [];
	const lastIn = plan.intro.calls.at(-1)!.t1 - 500;
	if (plan.open === "pyro" || plan.big) {
		for (const [i, at] of JETS.entries()) {
			let k = 0;
			if (opening && plan.open === "pyro") {
				for (const b of BURSTS) {
					// (The corners with every other burst.)
					if (i < 4 || b === BURSTS[0] || b === BURSTS[3]) {
						k = Math.max(k, burstK(tlT, b + (i % 2) * 120));
					}
				}
			}
			// And as the last of the home team's five gets to his place.
			k = Math.max(k, burstK(tlT, lastIn + (i % 2) * 140));
			if (k > 0.01) {
				jets.push({ at, k });
			}
		}
	}
	const beams: ShowFx["beams"] = [];
	const sweeping =
		(opening && plan.open === "beams") ||
		(!opening && tlT < plan.h1 && (plan.big || plan.open === "beams"));
	if (sweeping) {
		for (let i = 0; i < 6; i++) {
			const s = tlT / 1000;
			beams.push({
				from: {
					x: MID + (i - 2.5) * 16,
					y: COURT_H / 2 + (i % 2 ? -22 : 22),
					z: 70,
				},
				to: {
					x: MID + 40 * Math.sin(s * 0.7 + i * 1.7),
					y: COURT_H / 2 + 20 * Math.sin(s * 0.9 + i * 2.3),
					z: 0,
				},
				color: colors[i % 2]!,
			});
		}
	}
	const logo =
		plan.open === "logo" && opening
			? Math.min(1, Math.max(0, (tlT - 900) / 700))
			: 0;
	return { dark, jets, beams, logo, phones: dark > 0.3 };
};
