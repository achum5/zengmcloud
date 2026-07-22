import {
	useEffect,
	useId,
	useLayoutEffect,
	useMemo,
	useRef,
	useState,
	type CSSProperties,
	type ReactNode,
} from "react";
import { useLocal } from "../../util/local.ts";
import { usePlayerFace } from "../../util/playerFaces.ts";
import { PlayerPicture } from "../../components/PlayerPicture.tsx";

// A full-court live-game graphic (FBGM-field style, but hardwood): team-colored
// rails with the team names behind each baseline, the home team's logo at
// center court (with the championship trophy behind it during the finals), and
// a SCENE for every play. The full 5-on-5 stands on the floor - the players
// involved in the play appear with their FACE, name, and the exact play-by-play
// line, while their eight teammates read as small team-colored jersey-number
// CHIPS (clean, uncluttered, and cheap to move). The ball animates every
// outcome (swish, rim-out, block, steal, turnover...).
//
// Locations are stylized: the sim only knows coarse shot zones (at rim / low
// post / mid-range / three), so spots are synthesized inside the right zone.
// The away team always attacks the LEFT rim, home the RIGHT (fixed for
// readability, like broadcast graphics - real teams swap at halftime).

// Full court in feet: 94 x 50. Rails/aprons extend the viewBox around it.
const COURT_W = 94;
const COURT_H = 50;
const RAIL_W = 5; // team-colored strip behind each baseline
const APRON = 2.5; // sideline apron top/bottom
const VIEW = `${-RAIL_W} ${-APRON} ${COURT_W + 2 * RAIL_W} ${COURT_H + 2 * APRON}`;

const RIM_INSET = 5.25; // rim center distance from baseline
const THREE_R = 23.75;
const CORNER_DIST = 22; // rim center to corner-three line (across)
const CORNER_LEN = RIM_INSET + Math.sqrt(THREE_R ** 2 - CORNER_DIST ** 2);

export type CourtZone = "atRim" | "lowPost" | "midRange" | "three" | "ft";

export type CourtActor = {
	pid: number;
	name: string;
	// Court coordinates (x along the 94ft length, y across the 50ft width).
	x: number;
	y: number;
	role: "main" | "defender" | "victim" | "in" | "out" | "assist" | "onCourt";
	// Display team (0 = away/left, 1 = home/right). Set for "onCourt" background
	// players so they're colored by their own team rather than the scene's.
	t?: 0 | 1;
};

// Default championship trophy shown at center court during a finals game (Larry
// O'Brien style). Rendered behind the home logo. Overridable per team later.
export const DEFAULT_TROPHY_URL = "https://i.imgur.com/c8cwwka.png";

export type CourtSceneKind =
	| "attempt"
	| "make"
	| "miss"
	| "block"
	// A textless SETUP beat between possessions: the ball is brought up the floor
	// and the five settle into their set (fast break or half-court, per what's
	// coming) BEFORE the play that follows. This is what stops a possession from
	// teleporting end-to-end - the trip up the court gets its own beat.
	| "advance"
	| "tov"
	| "stl"
	| "reb"
	| "foul"
	| "sub"
	| "jump"
	| "other";

export type CourtScene = {
	key: number; // increments per scene, retriggers animations
	kind: CourtSceneKind;
	t: 0 | 1; // display team of the main actor (0 = away/left, 1 = home/right)
	actors: CourtActor[];
	text: ReactNode; // the play-by-play line, shown on the floor
	// A scored basket's running score line (both logos flanking the score),
	// shown centered under the play text.
	score?: ReactNode;
	// Ball flight. For shots: from the attempt spot (ballFrom) to the rim
	// (rimX). For rebounds: from the rim (ballFrom) to the rebounder (ballTo).
	ballFrom?: { x: number; y: number };
	ballTo?: { x: number; y: number };
	rimX?: number;
	// A pass that PRECEDES the play: on an attempt, the ball starts in the
	// passer's hands here and is zipped to the shooter (the "main" actor) just as
	// he arrives at his spot - so the assist reads BEFORE the shot, the way it
	// actually happens. (On a shot-result scene it instead prefixes the flight.)
	passFrom?: { x: number; y: number };
	// The ball-handler's previous spot, so on an attempt the ball comes up the
	// floor WITH him from there instead of beating him to the spot.
	shooterFrom?: { x: number; y: number };
	// How long (ms) the ball-handler takes to glide to his spot, so the ball's
	// arrival (a dribble up, or the catch off a pass) lands exactly when he does.
	arriveMs?: number;
};

const degToRad = (deg: number) => (deg * Math.PI) / 180;
const rand = (lo: number, hi: number) => lo + Math.random() * (hi - lo);

// Lighten (amount > 0) or darken (< 0) a hex color, for plank seam lines.
const shade = (hex: string, amount: number): string => {
	const m = /^#?([0-9a-f]{6})$/i.exec(hex.trim());
	if (!m) {
		return hex;
	}
	const n = Number.parseInt(m[1]!, 16);
	const clamp = (v: number) => Math.max(0, Math.min(255, Math.round(v)));
	const r = clamp(((n >> 16) & 0xff) * (1 + amount));
	const g = clamp(((n >> 8) & 0xff) * (1 + amount));
	const b = clamp((n & 0xff) * (1 + amount));
	return `#${((r << 16) | (g << 8) | b).toString(16).padStart(6, "0")}`;
};

// The rim each display team attacks (away left, home right).
export const rimXFor = (t: 0 | 1): number =>
	t === 0 ? RIM_INSET : COURT_W - RIM_INSET;

// Convert a spot expressed as (depth from the attacked baseline, position
// across the court) into full-court coordinates for team t.
const toCourt = (t: 0 | 1, depth: number, across: number) => ({
	x: t === 0 ? depth : COURT_W - depth,
	y: across,
});

// A fictional-but-plausible spot for a shot in a zone, oriented toward team
// t's rim. Angles fan out from the rim (90° = straight toward half court).
export const synthShotSpot = (
	t: 0 | 1,
	zone: CourtZone,
): { x: number; y: number } => {
	if (zone === "ft") {
		return toCourt(t, 19, 25 + rand(-0.5, 0.5));
	}

	let r: number;
	let theta: number;
	if (zone === "atRim") {
		r = rand(1, 4);
		theta = rand(25, 155);
	} else if (zone === "lowPost") {
		r = rand(4.5, 10);
		theta = rand(30, 150);
	} else if (zone === "midRange") {
		r = rand(11, 20);
		theta = rand(18, 162);
	} else {
		if (Math.random() < 0.3) {
			// Corner three: BEHIND the corner line, which sits at y=3 near the top
			// sideline and y=47 near the bottom. Placed in the y<3 / y>47 strip a
			// short way up from the baseline, so the shooter is genuinely outside
			// the arc instead of standing in front of it.
			const nearSide = Math.random() < 0.5;
			return toCourt(
				t,
				rand(3, 12),
				nearSide ? rand(1.4, 2.7) : rand(47.3, 48.6),
			);
		}
		r = rand(THREE_R + 1, THREE_R + 3.5);
		theta = rand(32, 148);
	}

	// Kept a few feet off every edge so the face centered on this spot (and its
	// name tag) never clips the sideline/baseline.
	const depth = RIM_INSET + r * Math.sin(degToRad(theta));
	const across = 25 + r * Math.cos(degToRad(theta));
	return toCourt(
		t,
		Math.min(43, Math.max(4, depth)),
		Math.min(46, Math.max(4, across)),
	);
};

// A last-second heave: way out from the rim, around or beyond half court.
export const synthHeaveSpot = (t: 0 | 1): { x: number; y: number } =>
	toCourt(t, rand(46, 58), 25 + rand(-14, 14));

// A generic spot for non-shot plays (turnovers, fouls...) in team t's
// frontcourt, away from the paint so faces don't sit on the rim.
export const synthPlaySpot = (t: 0 | 1): { x: number; y: number } =>
	toCourt(t, rand(14, 34), rand(8, 42));

// Rebound spot: right around the rim the shot was at.
export const synthReboundSpot = (rimT: 0 | 1): { x: number; y: number } =>
	toCourt(rimT, rand(3, 9), 25 + rand(-8, 8));

// Subs check in near half court: a single, well-spaced row so the faces +
// name tags don't cram together, set a little in from the sideline (not jammed
// against the very top edge).
export const scorerTableRow = (n: number): { x: number; y: number }[] => {
	const gap = Math.min(11, 62 / Math.max(1, n));
	const startX = COURT_W / 2 - ((n - 1) * gap) / 2;
	return Array.from({ length: n }, (_, i) => ({
		x: startX + i * gap,
		y: 11,
	}));
};

// Deterministic half-court formation slots (depth from the attacked baseline,
// across the width), ordered guard→wing→big. Both clusters live in the
// offense's frontcourt - the end the ball is at - so all ten faces read as one
// 5-on-5 set. Slot assignment is stable (full lineup sorted by position, THEN
// play actors skipped) so a player keeps his slot from scene to scene and just
// glides; the defense man-marks by shadowing its counterpart's slot a step
// closer to the rim.
const OFFENSE_SPOTS: { depth: number; across: number }[] = [
	{ depth: 28, across: 25 }, // point, top of the key
	{ depth: 22, across: 42 }, // wing
	{ depth: 22, across: 8 }, // wing
	{ depth: 10, across: 39 }, // corner / short corner
	{ depth: 7, across: 20 }, // low block
];

// A FAST-BREAK set: the offense is strung out toward the rim in wide lanes -
// a lead attacker at the basket, both wings filling the lanes, a trailer, and a
// deep safety - instead of a settled half-court spread. Ordered guard→big (see
// posRank), so the guard leads the break and the big trails, the way it runs in
// real life. Used for a possession that starts off a steal or a defensive board
// (see buildLineupActors' `transition`), which is what makes a turnover into a
// coast-to-coast break rather than another static set popping onto the floor.
const TRANSITION_OFFENSE_SPOTS: { depth: number; across: number }[] = [
	{ depth: 6, across: 25 }, // lead attacker, at the rim
	{ depth: 13, across: 6 }, // left lane runner
	{ depth: 13, across: 44 }, // right lane runner
	{ depth: 23, across: 31 }, // trailer
	{ depth: 31, across: 19 }, // deep safety / late trailer
];

// The ball's path for a setup beat: brought up from the FAR end (where the
// possession was just won) to the ball-handler's spot in the set the team is
// about to run - the lead attacker on a break, the top of the key in a
// half-court set. Matches OFFENSE_SPOTS[0] / TRANSITION_OFFENSE_SPOTS[0] so the
// ball settles right where a real handler stands.
export const setupBallPath = (
	offenseT: 0 | 1,
	transition: boolean,
): { ballFrom: { x: number; y: number }; ballTo: { x: number; y: number } } => {
	const handler = transition ? TRANSITION_OFFENSE_SPOTS[0]! : OFFENSE_SPOTS[0]!;
	return {
		ballFrom: { x: rimXFor(offenseT === 0 ? 1 : 0), y: 25 },
		ballTo: toCourt(offenseT, handler.depth, handler.across),
	};
};

// Rank a lineup player by court position so the sorted order fills the formation
// slots guard→wing→big, however the sim labels positions.
const posRank = (pos: string | undefined): number => {
	switch (pos) {
		case "PG":
			return 0;
		case "G":
			return 1;
		case "SG":
			return 2;
		case "GF":
			return 3;
		case "SF":
			return 4;
		case "F":
			return 5;
		case "PF":
			return 6;
		case "FC":
			return 7;
		case "C":
			return 8;
		default:
			return 4;
	}
};

export type LineupPlayer = {
	pid: number;
	name: string;
	pos?: string;
	inGame?: boolean;
};

// A small deterministic offset off a player's formation slot, so the five don't
// stand in a rigid geometric lattice. Seeded by (pid) ALONE - NOT the scene -
// so a player keeps the SAME spot every play and simply holds his ground
// between possessions instead of being reshuffled to a new random spot each
// time (which read as everyone teleporting around). He only moves when the ball
// changes ends or when he's the one featured in the play.
const drift = (pid: number, salt: number): number => {
	const h = Math.sin(pid * 668265 + salt * 97) * 43758.5453;
	return (h - Math.floor(h) - 0.5) * 2.8; // ±1.4 ft
};

// The full on-floor 5-on-5 as background actors at their formation spots.
// Returns ALL ten (the caller drops any who are actors in the current play and
// may promote/nudge others - e.g. the assister, or a defender stepping up to
// contest). `teams` is display order [away, home]; `offenseT` is the display
// team with the ball.
export const buildLineupActors = ({
	teams,
	offenseT,
	transition = false,
}: {
	teams: [LineupPlayer[], LineupPlayer[]];
	offenseT: 0 | 1;
	// A fast break: the offense streaks toward the rim (TRANSITION_OFFENSE_SPOTS)
	// and the defense is caught RECOVERING - one man back protecting the rim, the
	// rest trailing the play - instead of a set man-to-man. Turns a steal / defensive
	// board into a real numbers-advantage break instead of another half-court set.
	transition?: boolean;
}): CourtActor[] => {
	const out: CourtActor[] = [];
	const offSpots = transition ? TRANSITION_OFFENSE_SPOTS : OFFENSE_SPOTS;
	for (const t of [0, 1] as const) {
		const isOffense = t === offenseT;
		const onFloor = (teams[t] ?? [])
			.filter((p) => p.inGame)
			.sort((a, b) => posRank(a.pos) - posRank(b.pos))
			.slice(0, offSpots.length);
		for (const [i, p] of onFloor.entries()) {
			const slot = offSpots[i]!;
			let depth: number;
			let across: number;
			if (isOffense) {
				depth = slot.depth;
				across = slot.across;
			} else if (transition) {
				// Recovering defense: the first man sprints back to protect the rim;
				// everyone else trails the break (a higher depth = further from the
				// rim being attacked = behind the streaking offense), pinched middle.
				if (i === 0) {
					depth = 9;
					across = 25 + (slot.across - 25) * 0.3;
				} else {
					depth = Math.min(42, slot.depth + 7);
					across = 25 + (slot.across - 25) * 0.8;
				}
			} else {
				// Half-court man-to-man: shadow the man a step toward the rim, pinched.
				depth = Math.max(4, slot.depth - 4.5);
				across = 25 + (slot.across - 25) * 0.78;
			}
			const { x, y } = toCourt(offenseT, depth, across);
			out.push({
				pid: p.pid,
				name: p.name,
				x: Math.min(90, Math.max(4, x + drift(p.pid, 1))),
				y: Math.min(46, Math.max(4, y + drift(p.pid, 2))),
				role: "onCourt",
				t,
			});
		}
	}
	return out;
};

// Map a play-by-play event type to a shot descriptor, or undefined for
// non-shot events (which the caller handles separately).
export const courtActionFromEventType = (
	type: string,
):
	| { kind: "attempt"; zone: CourtZone }
	| { kind: "result"; zone: CourtZone; made: boolean; blocked?: boolean }
	| undefined => {
	switch (type) {
		case "fgaTipIn":
		case "fgaPutBack":
		case "fgaAtRim":
			return { kind: "attempt", zone: "atRim" };
		case "fgaLowPost":
			return { kind: "attempt", zone: "lowPost" };
		case "fgaMidRange":
			return { kind: "attempt", zone: "midRange" };
		case "fgaTp":
		case "fgaTpFake":
			return { kind: "attempt", zone: "three" };
		case "fgTipIn":
		case "fgTipInAndOne":
		case "fgPutBack":
		case "fgPutBackAndOne":
		case "fgAtRim":
		case "fgAtRimAndOne":
			return { kind: "result", zone: "atRim", made: true };
		case "fgLowPost":
		case "fgLowPostAndOne":
			return { kind: "result", zone: "lowPost", made: true };
		case "fgMidRange":
		case "fgMidRangeAndOne":
			return { kind: "result", zone: "midRange", made: true };
		case "tp":
		case "tpAndOne":
			return { kind: "result", zone: "three", made: true };
		case "missTipIn":
		case "missPutBack":
		case "missAtRim":
			return { kind: "result", zone: "atRim", made: false };
		case "missLowPost":
			return { kind: "result", zone: "lowPost", made: false };
		case "missMidRange":
			return { kind: "result", zone: "midRange", made: false };
		case "missTp":
			return { kind: "result", zone: "three", made: false };
		case "blkAtRim":
		case "blkTipIn":
		case "blkPutBack":
			return { kind: "result", zone: "atRim", made: false, blocked: true };
		case "blkLowPost":
			return { kind: "result", zone: "lowPost", made: false, blocked: true };
		case "blkMidRange":
			return { kind: "result", zone: "midRange", made: false, blocked: true };
		case "blkTp":
			return { kind: "result", zone: "three", made: false, blocked: true };
		case "ft":
			return { kind: "result", zone: "ft", made: true };
		case "missFt":
			return { kind: "result", zone: "ft", made: false };
		default:
			return undefined;
	}
};

export const zoneLabel = (zone: CourtZone): string => {
	switch (zone) {
		case "atRim":
			return "at the rim";
		case "lowPost":
			return "low post";
		case "midRange":
			return "mid-range";
		case "three":
			return "three";
		case "ft":
			return "free throw";
	}
};

export type CourtTeam = {
	tid: number;
	abbrev?: string;
	region?: string;
	name?: string;
	colors?: [string, string, string];
	imgURL?: string;
	court?: import("../../../common/types.ts").CourtStyle;
};

const FLIGHT_MS = 650;
const OUTCOME_MS = 450;
// The assist pass leg (assister → shooter) shown before an assisted make's
// shot flight: quick and flat, like a real kick-out or entry pass.
const PASS_MS = 340;

// Sizes are in container-query units (cqw = % of the court container width), so
// faces and text scale WITH the court on any screen, mobile included - clamped
// so they never get unreadably small on a phone or huge on a wide monitor.
const FACE_W = "clamp(20px, 3.6cqw, 46px)";
const FACE_H = "clamp(30px, 5.4cqw, 68px)";
const NAME_FONT = "clamp(8px, 1.4cqw, 13px)";

// A background teammate reads as a small team-colored jersey CHIP (a numbered
// disc) rather than a full face: clean, unmistakably "the other four guys", and
// cheap to glide (no facesjs SVG per body). Only the play's actors get faces.
const CHIP_SIZE = "clamp(13px, 2.2cqw, 28px)";
const CHIP_FONT = "clamp(8px, 1.3cqw, 14px)";

// Turn a player's real build into on-court body scale, so a 7-footer visibly
// TOWERS over a 6-foot guard and a bruiser reads BROADER than a wiry scorer.
// Height (inches) drives overall size; weight (lbs) adds width-only girth. Both
// fall back to an average build (1) until the measurements load. Referenced off
// a middle-of-the-roster wing (6'6", 215 lb).
const REF_HGT = 78;
const REF_WEIGHT = 215;
const bodyScale = (
	hgt: number | undefined,
	weight: number | undefined,
): { size: number; girth: number } => ({
	size:
		hgt === undefined
			? 1
			: Math.min(1.2, Math.max(0.85, 1 + ((hgt - REF_HGT) / REF_HGT) * 1.35)),
	girth:
		weight === undefined
			? 1
			: Math.min(
					1.2,
					Math.max(0.86, 1 + ((weight - REF_WEIGHT) / REF_WEIGHT) * 0.55),
				),
});

// A soft ground shadow under a body, so it reads as standing ON the floor
// (a light 2.5D cue) rather than floating flat on the hardwood.
const GROUND_SHADOW =
	"radial-gradient(ellipse at center, rgba(0,0,0,0.42), rgba(0,0,0,0) 70%)";

// Face-tag animation keyframes, injected once. The resting transform centers
// the whole tag ON its court point (face directly over the shot dot), baked
// into every frame so the animation doesn't fight the positioning.
const REST = "translate(-50%, -50%)";
// The fouled/stripped player gets ROCKED - a bigger, snappier recoil than a
// polite wobble - and the fouler/stripper takes a hard swiping CHOP. These read
// clearly even at the small on-court face size, which the gentle old versions
// did not.
const FACE_ANIM_CSS = `
@keyframes liveCourtShake {
	0%,100% { transform: ${REST} rotate(0deg) scale(1); }
	12% { transform: ${REST} translateX(6px) rotate(13deg) scale(1.08); }
	28% { transform: ${REST} translateX(-6px) rotate(-12deg) scale(1.06); }
	44% { transform: ${REST} translateX(4px) rotate(9deg) scale(1.04); }
	60% { transform: ${REST} translateX(-3px) rotate(-6deg) scale(1.02); }
	78% { transform: ${REST} translateX(2px) rotate(3deg) scale(1.01); }
}
@keyframes liveCourtSwipe {
	0% { transform: ${REST} rotate(0deg) scale(1); }
	35% { transform: ${REST} translateX(-11px) rotate(-27deg) scale(1.1); }
	55% { transform: ${REST} translateX(5px) rotate(12deg) scale(1.04); }
	75% { transform: ${REST} translateX(-2px) rotate(-5deg) scale(1.01); }
	100% { transform: ${REST} rotate(0deg) scale(1); }
}`;

// Glide duration (seconds) for a body moving `dist` feet, CAPPED so it always
// finishes within the current scene interval `sceneMs`. Without the cap a
// cross-court possession swing (the whole team running to the other end) took
// longer than a scene lasts, so players were still sprinting down as the next
// play - the shot - fired, which read as bodies popping in late (or the ball
// beating them there). Capping to a bit under the interval makes the team land
// before the next play, at any playback speed. Exported so the ball can be
// paced to the exact same arrival.
export const glideSeconds = (
	dist: number,
	sceneMs: number | undefined,
): number => {
	const cap = Math.min(0.9, ((sceneMs ?? 1100) / 1000) * 0.82);
	return Math.min(cap, 0.3 + dist * 0.006);
};

// The compositor-friendly placement style for a body on the floor, shared by
// faces and chips. Moved with a translate3d transform (in measured px) rather
// than left/top - animating left/top forces layout+paint every frame for every
// body, which is what made the court chug on mobile. The transform TRANSITIONS,
// so a change of court point reads as a GLIDE whose duration scales with the
// distance moved (a cross-court end swap RUNS the floor, a small cut barely
// moves), capped to the scene interval; background bodies start on small
// deterministic staggered delays so a team flows down-court instead of shifting
// as one rigid block. Previous position is banked in an effect (post-commit),
// so a double render can't zero out the measured distance. Falls back to
// left/top % for the one paint before the container is measured.
const useGlideStyle = (
	actor: CourtActor,
	size: { w: number; h: number } | undefined,
	background: boolean,
	sceneMs: number | undefined,
): CSSProperties => {
	const fx = (actor.x + RAIL_W) / (COURT_W + 2 * RAIL_W);
	const fy = (actor.y + APRON) / (COURT_H + 2 * APRON);
	const prevPos = useRef<{ x: number; y: number } | undefined>(undefined);
	const prev = prevPos.current;
	const moveDist = prev ? Math.hypot(actor.x - prev.x, actor.y - prev.y) : 0;
	useEffect(() => {
		prevPos.current = { x: actor.x, y: actor.y };
	}, [actor.x, actor.y]);
	const glideDur = glideSeconds(moveDist, sceneMs);
	const glideDelay = background
		? (((actor.pid * 2654435761) >>> 0) % 5) * 0.03
		: 0;
	return size
		? {
				left: 0,
				top: 0,
				transform: `translate3d(${fx * size.w}px, ${fy * size.h}px, 0)`,
				transition: `transform ${glideDur}s ease ${glideDelay}s, opacity 0.3s ease`,
				willChange: "transform",
			}
		: {
				left: `${fx * 100}%`,
				top: `${fy * 100}%`,
			};
};

// One body on the floor, centered ON its court point. Two looks, ONE component
// (and ONE element), always keyed by pid at the call site:
//   - a background teammate reads as a small team-colored jersey CHIP;
//   - a player featured in the current play becomes his FACE, with a name tag.
// Because both looks live in the same component and the outer positioning div is
// identical, a teammate who steps into the play (or drops back out of it) keeps
// the SAME element - so he GLIDES from his formation chip to his action face
// (the shooter visibly runs to his spot) instead of one element vanishing and a
// second popping in elsewhere. That single-element identity is what ended the
// old duplicate-face / teleport glitch, and it also avoids tearing down and
// regenerating the facesjs SVG every play. The one-shot shake/swipe recoil
// retriggers via `animKey`: only the inner animated wrapper remounts on a fresh
// foul/steal, so the face keeps gliding on its persistent outer element.
const BodyOnCourt = ({
	actor,
	season,
	lid,
	color,
	background,
	anim,
	animKey,
	nameAbove,
	size,
	sceneMs,
}: {
	actor: CourtActor;
	season: number | undefined;
	lid: number | undefined;
	color: string;
	// A background 5-on-5 teammate (not part of the current play): a jersey chip.
	background: boolean;
	anim?: "shake" | "swipe";
	// The current scene key, passed only when `anim` is set, so the recoil
	// animation retriggers on a fresh foul/steal without remounting the face on
	// every ordinary play.
	animKey?: number;
	nameAbove?: boolean;
	// Measured px size of the court container (see LiveCourt's ResizeObserver).
	size: { w: number; h: number } | undefined;
	// Current scene interval (ms), so the glide never outlasts a play.
	sceneMs: number | undefined;
}) => {
	const faceData = usePlayerFace(actor.pid, season, lid);
	const glide = useGlideStyle(actor, size, background, sceneMs);
	// Size this body by the player's real build (once his measurements load).
	const { size: sizeScale, girth } = bodyScale(faceData?.hgt, faceData?.weight);

	if (background) {
		const chip = `calc(${CHIP_SIZE} * ${sizeScale})`;
		return (
			<div
				className="position-absolute"
				style={{ ...glide, pointerEvents: "none", zIndex: 2 }}
			>
				<div
					style={{
						position: "relative",
						width: chip,
						height: chip,
						transform: REST,
					}}
				>
					{/* A ground shadow under the chip so it stands ON the floor. */}
					<div
						style={{
							position: "absolute",
							left: "50%",
							bottom: "-14%",
							transform: "translateX(-50%)",
							width: "92%",
							height: "32%",
							background: GROUND_SHADOW,
							pointerEvents: "none",
						}}
					/>
					<div
						style={{
							width: "100%",
							height: "100%",
							borderRadius: "50%",
							background: color,
							border: "1px solid rgba(255,255,255,0.7)",
							boxShadow: "0 2px 3px rgba(0,0,0,0.4)",
							color: "#fff",
							display: "flex",
							alignItems: "center",
							justifyContent: "center",
							fontSize: `calc(${CHIP_FONT} * ${sizeScale})`,
							fontWeight: 700,
							lineHeight: 1,
							textShadow: "0 1px 1px rgba(0,0,0,0.45)",
						}}
					>
						{faceData?.jerseyNumber ?? ""}
					</div>
				</div>
			</div>
		);
	}

	const animation =
		anim === "shake"
			? "liveCourtShake 0.62s ease"
			: anim === "swipe"
				? "liveCourtSwipe 0.6s ease"
				: undefined;

	const nameTag = (
		<div
			style={{
				position: "absolute",
				left: "50%",
				transform: "translateX(-50%)",
				[nameAbove ? "bottom" : "top"]: "100%",
				background: color,
				color: "#fff",
				borderRadius: 3,
				fontSize: NAME_FONT,
				fontWeight: 600,
				lineHeight: 1.3,
				padding: "0 4px",
				whiteSpace: "nowrap",
				textShadow: "0 1px 1px rgba(0,0,0,0.5)",
				boxShadow: "0 1px 2px rgba(0,0,0,0.4)",
			}}
		>
			{actor.role === "in" ? "▲ " : actor.role === "out" ? "▼ " : ""}
			{actor.name}
		</div>
	);

	// A real player PHOTO is drawn as a clean circular avatar (team-colored ring,
	// head cropped to fill) instead of a raw rectangular headshot floating on the
	// hardwood - the rectangles were the worst of the clutter. A generated
	// facesjs face already reads as a tidy head-and-jersey portrait, so it keeps
	// that shape. Both are sized by the player's real build.
	const hasPhoto = !!faceData?.imgURL;
	const diameter = `calc(${FACE_H} * ${sizeScale})`;

	// The OUTER div only places the point on the court (compositor-friendly
	// translate3d, transitioned for the glide between plays). The INNER div
	// carries the centering REST transform and the one-shot shake/swipe
	// animations (whose keyframes bake REST in), so animating never fights the
	// positioning. That inner div is keyed by the scene so its CSS animation
	// replays each foul/steal; the outer div persists and keeps gliding.
	return (
		<div
			className="position-absolute"
			style={{
				...glide,
				pointerEvents: "none",
				zIndex: actor.role === "main" ? 5 : 4,
			}}
		>
			<div
				key={anim ? `anim-${animKey}` : "static"}
				style={
					hasPhoto
						? {
								position: "relative",
								width: diameter,
								height: diameter,
								transform: REST,
								animation,
							}
						: {
								position: "relative",
								// Sized by the player's real build: height grows the whole
								// body, weight adds a little width-only girth.
								height: `calc(${FACE_H} * ${sizeScale})`,
								width: `calc(${FACE_W} * ${sizeScale} * ${girth})`,
								transform: REST,
								animation,
								filter: "drop-shadow(0 1px 2px rgba(0,0,0,0.5))",
							}
				}
			>
				{/* Ground shadow at the body's feet, so he stands ON the floor. */}
				<div
					style={{
						position: "absolute",
						left: "50%",
						bottom: "-7%",
						transform: "translateX(-50%)",
						width: "78%",
						height: "15%",
						background: GROUND_SHADOW,
						pointerEvents: "none",
					}}
				/>
				{hasPhoto ? (
					<div
						style={{
							position: "absolute",
							inset: 0,
							borderRadius: "50%",
							overflow: "hidden",
							border: `2px solid ${color}`,
							background: "#20242b",
							boxShadow: "0 2px 4px rgba(0,0,0,0.5)",
						}}
					>
						<img
							alt=""
							src={faceData?.imgURL}
							style={{
								width: "100%",
								height: "100%",
								objectFit: "cover",
								objectPosition: "center 12%",
							}}
						/>
					</div>
				) : faceData?.face ? (
					<PlayerPicture
						face={faceData.face}
						colors={faceData.colors}
						jersey={faceData.jersey}
					/>
				) : null}
				{nameTag}
			</div>
		</div>
	);
};

const teamColor = (team: CourtTeam | undefined, i: number, fallback: string) =>
	team?.colors?.[i] ?? fallback;

const LiveCourt = ({
	scene,
	teams,
	finals,
	season,
	sceneMs,
}: {
	scene: CourtScene | undefined;
	// Display order: [away (left rim), home (right rim)].
	teams: [CourtTeam | undefined, CourtTeam | undefined];
	finals: boolean;
	season: number | undefined;
	// How long each play stays on screen (playback speed), so a glide never
	// outlasts the play it belongs to.
	sceneMs: number | undefined;
}) => {
	const { lid } = useLocal(["lid"]);

	const ballRef = useRef<SVGCircleElement | null>(null);
	const ringRef = useRef<SVGCircleElement | null>(null);
	const burstRef = useRef<SVGGElement | null>(null);
	const rafRef = useRef<number | undefined>(undefined);

	// Measured px size of the court container, so faces can be positioned with a
	// compositor transform (translate3d) instead of layout-triggering left/top.
	// useLayoutEffect measures before first paint; the ResizeObserver keeps it
	// current through resizes/rotations.
	const containerRef = useRef<HTMLDivElement | null>(null);
	const [size, setSize] = useState<{ w: number; h: number } | undefined>(
		undefined,
	);
	useLayoutEffect(() => {
		const el = containerRef.current;
		if (!el) {
			return;
		}
		const measure = () => {
			const w = el.clientWidth;
			const h = el.clientHeight;
			if (w > 0 && h > 0) {
				setSize((prev) =>
					prev && prev.w === w && prev.h === h ? prev : { w, h },
				);
			}
		};
		measure();
		const observer = new ResizeObserver(measure);
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, []);

	const away = teams[0];
	const home = teams[1];
	const awayColor = teamColor(away, 0, "#fd7e14");
	const homeColor = teamColor(home, 0, "#0d6efd");
	const sceneColor = scene?.t === 0 ? awayColor : homeColor;

	// Ball + effect animation, driven imperatively on the SVG nodes (no React
	// re-render per frame).
	useEffect(() => {
		if (!scene) {
			return;
		}
		const ball = ballRef.current;
		const ring = ringRef.current;
		const burst = burstRef.current;
		if (!ball) {
			return;
		}
		if (rafRef.current !== undefined) {
			cancelAnimationFrame(rafRef.current);
		}
		// Clear any lingering impact burst from a previous scene.
		if (burst) {
			burst.style.opacity = "0";
		}

		const hideBall = () => {
			ball.style.opacity = "0";
			if (ring) {
				ring.style.opacity = "0";
			}
		};

		// The ball resting LIVE with its handler: a soft dribble pulse (top-down
		// view, so the bounce reads as the ball rising toward the camera). Bounded
		// so a paused game isn't running an animation loop forever - after a few
		// seconds the ball just sits visible in his hands.
		const restDribble = (x: number, y: number) => {
			ball.style.opacity = "1";
			ball.setAttribute("cx", String(x));
			ball.setAttribute("cy", String(y));
			const restStart = performance.now();
			const loop = (now: number) => {
				const t = (now - restStart) / 1000;
				if (t > 6) {
					ball.setAttribute("r", "0.85");
					return;
				}
				ball.setAttribute(
					"r",
					String(0.76 + 0.16 * Math.abs(Math.sin(t * 5.2))),
				);
				rafRef.current = requestAnimationFrame(loop);
			};
			rafRef.current = requestAnimationFrame(loop);
		};

		const main = scene.actors.find((a) => a.role === "main");
		const isShot =
			scene.kind === "make" || scene.kind === "miss" || scene.kind === "block";

		// A setup beat: bring the ball up the floor from the previous end to the
		// handler's spot in the new set, then let it rest live in his hands. The
		// five glide into formation on their own (the body glide), so by the time
		// the actual play fires next beat, everyone is already in position - the
		// possession develops instead of teleporting.
		if (scene.kind === "advance" && scene.ballFrom && scene.ballTo) {
			if (ring) {
				ring.style.opacity = "0";
			}
			const from = scene.ballFrom;
			const to = scene.ballTo;
			ball.style.opacity = "1";
			ball.setAttribute("cx", String(from.x));
			ball.setAttribute("cy", String(from.y));
			// Take most of the beat to bring it up, then settle - paced to the scene
			// so it fills the beat at any speed.
			const bringMs = Math.max(280, (sceneMs ?? 1100) * 0.68);
			const start = performance.now();
			const step = (now: number) => {
				const p = Math.min(1, (now - start) / bringMs);
				ball.setAttribute("cx", String(from.x + (to.x - from.x) * p));
				ball.setAttribute("cy", String(from.y + (to.y - from.y) * p));
				// Dribble hops on the way up.
				ball.setAttribute(
					"r",
					String(0.76 + 0.22 * Math.abs(Math.sin(p * Math.PI * 4))),
				);
				if (p < 1) {
					rafRef.current = requestAnimationFrame(step);
				} else {
					restDribble(to.x, to.y);
				}
			};
			rafRef.current = requestAnimationFrame(step);
			return () => {
				if (rafRef.current !== undefined) {
					cancelAnimationFrame(rafRef.current);
				}
			};
		}

		// Rebound / opening tip: the ball travels from one spot to another (off
		// the rim to the rebounder, or tapped from center back behind the winner).
		if (
			(scene.kind === "reb" || scene.kind === "jump") &&
			scene.ballFrom &&
			scene.ballTo
		) {
			if (ring) {
				ring.style.opacity = "0";
			}
			const from = scene.ballFrom;
			const to = scene.ballTo;
			const isReb = scene.kind === "reb";
			ball.style.opacity = "1";
			const start = performance.now();
			const step = (now: number) => {
				const p = Math.min(1, (now - start) / 520);
				ball.setAttribute("cx", String(from.x + (to.x - from.x) * p));
				ball.setAttribute("cy", String(from.y + (to.y - from.y) * p));
				// A little hop off the rim, then settle into the rebounder's hands.
				ball.setAttribute("r", String(0.9 + 0.45 * Math.sin(Math.PI * p)));
				// A rebound STAYS in the rebounder's hands (live ball); a tip fades.
				if (!isReb) {
					ball.style.opacity = p < 0.82 ? "1" : String(1 - (p - 0.82) / 0.18);
				}
				if (p < 1) {
					rafRef.current = requestAnimationFrame(step);
				} else if (isReb) {
					restDribble(to.x, to.y);
				} else {
					hideBall();
				}
			};
			rafRef.current = requestAnimationFrame(step);
			return () => {
				if (rafRef.current !== undefined) {
					cancelAnimationFrame(rafRef.current);
				}
			};
		}

		// A shot being lined up. The ball reaches the shooter exactly as HE does
		// (arriveMs = his glide), so it never beats him to the spot, then stays
		// live in his hands until the result scene takes over the flight.
		if (scene.kind === "attempt" && main) {
			if (ring) {
				ring.style.opacity = "0";
			}
			const to = { x: main.x, y: main.y };
			const arriveMs =
				scene.arriveMs && scene.arriveMs > 0 ? scene.arriveMs : 430;
			ball.style.opacity = "1";
			const start = performance.now();
			const cleanupAttempt = () => {
				if (rafRef.current !== undefined) {
					cancelAnimationFrame(rafRef.current);
				}
			};

			if (scene.passFrom) {
				// A pass sets up the shot: the passer holds it, then zips it to the
				// shooter just as he arrives, so the catch and his arrival line up.
				const from = scene.passFrom;
				const passMs = Math.min(PASS_MS, arriveMs);
				const holdMs = Math.max(0, arriveMs - passMs);
				ball.setAttribute("cx", String(from.x));
				ball.setAttribute("cy", String(from.y));
				const step = (now: number) => {
					const e = now - start;
					if (e < holdMs) {
						ball.setAttribute(
							"r",
							String(0.78 + 0.07 * Math.abs(Math.sin(e / 130))),
						);
						rafRef.current = requestAnimationFrame(step);
						return;
					}
					const p = Math.min(1, (e - holdMs) / passMs);
					ball.setAttribute("cx", String(from.x + (to.x - from.x) * p));
					ball.setAttribute("cy", String(from.y + (to.y - from.y) * p));
					// Flat and quick - a pass, not a lob.
					ball.setAttribute("r", String(0.72 + 0.16 * Math.sin(Math.PI * p)));
					if (p < 1) {
						rafRef.current = requestAnimationFrame(step);
					} else {
						restDribble(to.x, to.y);
					}
				};
				rafRef.current = requestAnimationFrame(step);
				return cleanupAttempt;
			}

			// No pass: the ball comes up the floor WITH the handler from his last
			// spot (falling back to the backcourt if we don't know it), arriving as
			// he does.
			const dir = scene.t === 0 ? 1 : -1;
			const from = scene.shooterFrom ?? {
				x: Math.min(90, Math.max(4, to.x + dir * 13)),
				y: to.y + (25 - to.y) * 0.4,
			};
			const step = (now: number) => {
				const p = Math.min(1, (now - start) / arriveMs);
				ball.setAttribute("cx", String(from.x + (to.x - from.x) * p));
				ball.setAttribute("cy", String(from.y + (to.y - from.y) * p));
				// Dribble hops on the way up.
				ball.setAttribute(
					"r",
					String(0.76 + 0.26 * Math.abs(Math.sin(p * Math.PI * 3))),
				);
				if (p < 1) {
					rafRef.current = requestAnimationFrame(step);
				} else {
					restDribble(to.x, to.y);
				}
			};
			rafRef.current = requestAnimationFrame(step);
			return cleanupAttempt;
		}

		// Non-shot scenes get punchy, readable feedback (the small pulse-only
		// version read as almost nothing).
		if (!isShot) {
			hideBall();
			const victim = scene.actors.find((a) => a.role === "victim");
			const cleanup = () => {
				if (rafRef.current !== undefined) {
					cancelAnimationFrame(rafRef.current);
				}
			};

			// A steal: the ball is knocked LOOSE from the victim and darts to the
			// stealer, red sparks bursting at the poke, a pulse on the stealer. (The
			// victim's face also recoils via CSS.)
			if (scene.kind === "stl") {
				const from =
					scene.ballFrom ??
					(victim ? { x: victim.x, y: victim.y } : { x: COURT_W / 2, y: 25 });
				const to = scene.ballTo ?? (main ? { x: main.x, y: main.y } : from);
				const pokeDir = to.x >= from.x ? 1 : -1;
				if (ring) {
					ring.setAttribute("stroke", "#dc3545");
					ring.setAttribute("cx", String(main?.x ?? from.x));
					ring.setAttribute("cy", String(main?.y ?? from.y));
				}
				if (burst) {
					burst.style.color = "#f04444";
				}
				ball.style.opacity = "1";
				const start = performance.now();
				const step = (now: number) => {
					const p = Math.min(1, (now - start) / 640);
					let bx: number;
					let by: number;
					if (p < 0.32) {
						// Knocked loose: the ball jitters around the victim.
						const q = p / 0.32;
						bx = from.x + pokeDir * Math.sin(q * Math.PI * 3) * 1.6;
						by = from.y + Math.cos(q * Math.PI * 2.5) * 1.2;
					} else {
						// Then darts into the stealer's hands.
						const q = (p - 0.32) / 0.68;
						bx = from.x + (to.x - from.x) * q;
						by = from.y + (to.y - from.y) * q - Math.sin(Math.PI * q) * 1.2;
					}
					ball.setAttribute("cx", String(bx));
					ball.setAttribute("cy", String(by));
					ball.setAttribute("r", "0.9");
					// The ball stays LIVE in the stealer's hands (restDribble below).
					ball.style.opacity = "1";
					if (burst) {
						const bp = Math.min(1, p / 0.42);
						burst.setAttribute(
							"transform",
							`translate(${from.x} ${from.y}) scale(${0.5 + 2.3 * bp})`,
						);
						burst.style.opacity = String(0.85 * (1 - bp));
					}
					if (ring) {
						ring.setAttribute("r", String(1.5 + 4 * p));
						ring.style.opacity = String(0.75 * (1 - p));
					}
					if (p < 1) {
						rafRef.current = requestAnimationFrame(step);
					} else {
						if (ring) {
							ring.style.opacity = "0";
						}
						if (burst) {
							burst.style.opacity = "0";
						}
						// The stolen ball stays live in the stealer's hands.
						restDribble(to.x, to.y);
					}
				};
				rafRef.current = requestAnimationFrame(step);
				return cleanup;
			}

			// A foul: an impact burst + pulse at the point of contact between the
			// fouler and the man he hit. (Their faces chop / recoil via CSS.)
			if (scene.kind === "foul") {
				const bx =
					victim && main ? (victim.x + main.x) / 2 : (main?.x ?? COURT_W / 2);
				const by = victim && main ? (victim.y + main.y) / 2 : (main?.y ?? 25);
				if (ring) {
					ring.setAttribute("stroke", "#f59e0b");
					ring.setAttribute("cx", String(bx));
					ring.setAttribute("cy", String(by));
				}
				if (burst) {
					burst.style.color = "#fbbf24";
				}
				const start = performance.now();
				const step = (now: number) => {
					const p = Math.min(1, (now - start) / 520);
					if (burst) {
						burst.setAttribute(
							"transform",
							`translate(${bx} ${by}) scale(${0.4 + 2.9 * p})`,
						);
						burst.style.opacity = String(0.95 * (1 - p));
					}
					if (ring) {
						ring.setAttribute("r", String(1.5 + 5 * p));
						ring.style.opacity = String(0.85 * (1 - p));
					}
					if (p < 1) {
						rafRef.current = requestAnimationFrame(step);
					} else {
						if (ring) {
							ring.style.opacity = "0";
						}
						if (burst) {
							burst.style.opacity = "0";
						}
					}
				};
				rafRef.current = requestAnimationFrame(step);
				return cleanup;
			}

			// A turnover with no steal credited: a quick red pulse at the culprit.
			if (ring && main && scene.kind === "tov") {
				ring.setAttribute("cx", String(main.x));
				ring.setAttribute("cy", String(main.y));
				ring.setAttribute("stroke", "#dc3545");
				const start = performance.now();
				const pulse = (now: number) => {
					const p = Math.min(1, (now - start) / 600);
					ring.setAttribute("r", String(1.5 + 4.5 * p));
					ring.style.opacity = String(0.9 * (1 - p));
					if (p < 1) {
						rafRef.current = requestAnimationFrame(pulse);
					}
				};
				rafRef.current = requestAnimationFrame(pulse);
			}
			return cleanup;
		}

		const from = scene.ballFrom ?? { x: COURT_W / 2, y: COURT_H / 2 };
		const rimX = scene.rimX ?? rimXFor(scene.t);
		const to = { x: rimX, y: COURT_H / 2 };
		const made = scene.kind === "make";
		const blocked = scene.kind === "block";
		const bounce = { x: to.x + rand(-4, 4), y: to.y + rand(-6, 6) };
		// A block sends the ball sharply BACKWARD - away from the rim, past the
		// shooter - not just to the side.
		const backward = from.x < to.x ? -1 : 1;
		const swat = {
			x: Math.min(90, Math.max(4, from.x + backward * rand(10, 18))),
			y: Math.min(46, Math.max(4, from.y + rand(-6, 6))),
		};

		if (ring) {
			ring.setAttribute("cx", String(to.x));
			ring.setAttribute("cy", String(to.y));
			ring.setAttribute("stroke", sceneColor);
			ring.style.opacity = "0";
		}
		ball.style.opacity = "1";

		// An assisted make first shows the PASS: a quick, flat zip from the
		// assister to the shooter, then the normal shot flight takes over.
		const passFrom = scene.passFrom;
		const passMs = passFrom && !blocked ? PASS_MS : 0;

		const start = performance.now();
		const step = (now: number) => {
			const rawElapsed = now - start;

			if (passMs > 0 && passFrom && rawElapsed <= passMs) {
				const p = rawElapsed / passMs;
				ball.setAttribute("cx", String(passFrom.x + (from.x - passFrom.x) * p));
				ball.setAttribute("cy", String(passFrom.y + (from.y - passFrom.y) * p));
				// Flat and slightly small - a pass, not a shot arc.
				ball.setAttribute("r", String(0.72 + 0.18 * Math.sin(Math.PI * p)));
				rafRef.current = requestAnimationFrame(step);
				return;
			}

			const elapsed = rawElapsed - passMs;

			if (blocked) {
				const p = Math.min(1, elapsed / FLIGHT_MS);
				if (p < 0.35) {
					const q = p / 0.35;
					ball.setAttribute("cx", String(from.x + (to.x - from.x) * q * 0.3));
					ball.setAttribute("cy", String(from.y + (to.y - from.y) * q * 0.3));
					ball.setAttribute("r", String(0.9 + 0.5 * q));
				} else {
					const q = (p - 0.35) / 0.65;
					const bx = from.x + (to.x - from.x) * 0.105;
					const by = from.y + (to.y - from.y) * 0.105;
					ball.setAttribute("cx", String(bx + (swat.x - bx) * q));
					ball.setAttribute("cy", String(by + (swat.y - by) * q));
					ball.setAttribute("r", String(1.4 - 0.6 * q));
					ball.style.opacity = String(1 - 0.7 * q);
				}
				if (p < 1) {
					rafRef.current = requestAnimationFrame(step);
				} else {
					hideBall();
				}
				return;
			}

			if (elapsed <= FLIGHT_MS) {
				// Straight-line travel; the ball swells mid-flight to fake the arc.
				const p = elapsed / FLIGHT_MS;
				ball.setAttribute("cx", String(from.x + (to.x - from.x) * p));
				ball.setAttribute("cy", String(from.y + (to.y - from.y) * p));
				ball.setAttribute("r", String(0.9 + 0.9 * Math.sin(Math.PI * p)));
				rafRef.current = requestAnimationFrame(step);
				return;
			}

			const p = Math.min(1, (elapsed - FLIGHT_MS) / OUTCOME_MS);
			if (made) {
				// Swish: drop into the rim, ring pulse.
				ball.setAttribute("cx", String(to.x));
				ball.setAttribute("cy", String(to.y));
				ball.setAttribute("r", String(Math.max(0.05, 0.9 * (1 - p))));
				ball.style.opacity = String(1 - p * 0.6);
				if (ring) {
					ring.setAttribute("r", String(1 + 3.5 * p));
					ring.style.opacity = String(0.9 * (1 - p));
				}
			} else {
				// Rim out: carom and fade.
				ball.setAttribute("cx", String(to.x + (bounce.x - to.x) * p));
				ball.setAttribute("cy", String(to.y + (bounce.y - to.y) * p));
				ball.setAttribute("r", String(0.9 - 0.4 * p));
				ball.style.opacity = String(1 - 0.8 * p);
			}
			if (p < 1) {
				rafRef.current = requestAnimationFrame(step);
			} else {
				hideBall();
			}
		};
		rafRef.current = requestAnimationFrame(step);

		return () => {
			if (rafRef.current !== undefined) {
				cancelAnimationFrame(rafRef.current);
			}
		};
	}, [scene?.key]);

	// Court styling comes from the home team's custom court (Manage Teams →
	// Court), falling back to a neutral hardwood + the team's colors.
	const court = home?.court;
	const lineColor = court?.lines || "#f8f5f0";
	const woodFill = court?.floor || "#c9a165";
	const woodLine = shade(woodFill, -0.14);
	const floorPattern = court?.floorPattern ?? "hardwood";
	const paintColor = court?.paint || undefined; // undefined = no painted key
	const trophyURL = court?.trophyURL || DEFAULT_TROPHY_URL;
	const centerLogoURL = court?.logoURL || home?.imgURL;
	const secondaryLogoURL = court?.secondaryLogoURL || undefined;
	const sidelineImageURL = court?.sidelineImageURL || undefined;
	const baselineImageURL = court?.baselineImageURL || undefined;
	const cornerLogoURL = court?.cornerLogoURL || undefined;
	const benchImageURL = court?.benchImageURL || undefined;
	const centerText = court?.centerText || undefined;
	const benchText = court?.benchText || undefined;

	// Namespaced ids for the floor's SVG defs (grain filter + clip paths), so
	// two courts on one page (e.g. the editor preview) don't collide.
	const floorUid = useId().replace(/[^a-zA-Z0-9_-]/g, "");
	const grainId = `${floorUid}-grain`;
	const clipId = `${floorUid}-clip`;

	// An oversized field of long maple planks running along the court length,
	// with procedural grain. Drawn directly for "hardwood" and, rotated + clipped
	// to the court, for "diagonal" / "chevron".
	const plankField = (tag: string): ReactNode => {
		const PH = 2.3; // plank height (across the court)
		const x0 = -60;
		const x1 = COURT_W + 60;
		const w = x1 - x0;
		const y0 = -60;
		const y1 = COURT_H + 60;
		const bands: ReactNode[] = [];
		let idx = 0;
		for (let y = y0; y < y1; y += PH, idx++) {
			bands.push(
				<rect
					key={`${tag}p${idx}`}
					x={x0}
					y={y}
					width={w}
					height={PH}
					fill={shade(woodFill, idx % 2 === 0 ? 0.045 : -0.045)}
				/>,
			);
			bands.push(
				<line
					key={`${tag}s${idx}`}
					x1={x0}
					y1={y}
					x2={x1}
					y2={y}
					stroke={woodLine}
					strokeWidth={0.09}
					opacity={0.55}
				/>,
			);
		}
		return (
			<>
				<rect x={x0} y={y0} width={w} height={y1 - y0} fill={woodFill} />
				{bands}
				<rect
					x={x0}
					y={y0}
					width={w}
					height={y1 - y0}
					fill="#000"
					filter={`url(#${grainId})`}
					opacity={0.11}
				/>
			</>
		);
	};

	// A basketweave parquet: a checkerboard of blocks whose wood grain alternates
	// direction block to block, like the classic Boston Garden floor.
	const parquetField = (): ReactNode => {
		const B = 5.6; // block size
		const strips = 5;
		const sw = B / strips;
		const nx = Math.ceil(COURT_W / B) + 1;
		const ny = Math.ceil(COURT_H / B) + 1;
		const out: ReactNode[] = [];
		let bi = 0;
		for (let ix = 0; ix < nx; ix++) {
			for (let iy = 0; iy < ny; iy++) {
				const x = ix * B;
				const y = iy * B;
				const horiz = (ix + iy) % 2 === 0;
				out.push(
					<rect
						key={`pb${bi}`}
						x={x}
						y={y}
						width={B}
						height={B}
						fill={shade(woodFill, horiz ? 0.05 : -0.05)}
					/>,
				);
				for (let s = 1; s < strips; s++) {
					out.push(
						horiz ? (
							<line
								key={`pl${bi}-${s}`}
								x1={x}
								y1={y + s * sw}
								x2={x + B}
								y2={y + s * sw}
								stroke={woodLine}
								strokeWidth={0.06}
								opacity={0.5}
							/>
						) : (
							<line
								key={`pl${bi}-${s}`}
								x1={x + s * sw}
								y1={y}
								x2={x + s * sw}
								y2={y + B}
								stroke={woodLine}
								strokeWidth={0.06}
								opacity={0.5}
							/>
						),
					);
				}
				out.push(
					<rect
						key={`pbd${bi}`}
						x={x}
						y={y}
						width={B}
						height={B}
						fill="none"
						stroke={woodLine}
						strokeWidth={0.1}
						opacity={0.6}
					/>,
				);
				bi++;
			}
		}
		return <>{out}</>;
	};

	// The floor detail (grain/planks) for the chosen pattern, always clipped to
	// the court rectangle so it never paints over the rails/aprons.
	const cx = COURT_W / 2;
	const floorDetail = (): ReactNode => {
		if (floorPattern === "solid") {
			return null;
		}
		if (floorPattern === "parquet") {
			return <g clipPath={`url(#${clipId})`}>{parquetField()}</g>;
		}
		if (floorPattern === "diagonal") {
			return (
				<g clipPath={`url(#${clipId})`}>
					<g transform={`rotate(38 ${cx} 25)`}>{plankField("dg")}</g>
				</g>
			);
		}
		if (floorPattern === "chevron") {
			// Two mirrored diagonal fields meeting at the center line -> a V weave.
			return (
				<>
					<g clipPath={`url(#${clipId}-l)`}>
						<g transform={`rotate(34 ${cx} 25)`}>{plankField("cl")}</g>
					</g>
					<g clipPath={`url(#${clipId}-r)`}>
						<g transform={`rotate(-34 ${cx} 25)`}>{plankField("cr")}</g>
					</g>
				</>
			);
		}
		return <g clipPath={`url(#${clipId})`}>{plankField("hw")}</g>;
	};

	// The painted key (NBA-style colored lane), drawn under the lines.
	const paintFor = (mirror: boolean) => {
		if (!paintColor) {
			return null;
		}
		const tx = mirror ? `translate(${COURT_W} 0) scale(-1 1)` : undefined;
		return (
			<rect
				transform={tx}
				x={0}
				y={17}
				width={19}
				height={16}
				fill={paintColor}
			/>
		);
	};

	// Half-court markings for one side, mirrored for the other.
	const halfMarkings = (mirror: boolean) => {
		const tx = mirror ? `translate(${COURT_W} 0) scale(-1 1)` : undefined;
		return (
			<g
				transform={tx}
				fill="none"
				stroke={lineColor}
				strokeWidth={0.25}
				opacity={0.9}
			>
				{/* Paint + free-throw circle */}
				<rect x={0} y={17} width={19} height={16} />
				<circle cx={19} cy={25} r={6} />
				{/* Backboard + rim */}
				<line x1={4} y1={22} x2={4} y2={28} strokeWidth={0.4} />
				<circle cx={RIM_INSET} cy={25} r={0.95} />
				{/* Restricted area */}
				<path d={`M ${RIM_INSET} 21 A 4 4 0 0 1 ${RIM_INSET} 29`} />
				{/* Three-point line: corners + arc */}
				<line x1={0} y1={3} x2={CORNER_LEN} y2={3} />
				<line x1={0} y1={47} x2={CORNER_LEN} y2={47} />
				<path
					d={`M ${CORNER_LEN} 3 A ${THREE_R} ${THREE_R} 0 0 1 ${CORNER_LEN} 47`}
				/>
			</g>
		);
	};

	// The court belongs to the HOME team - both baselines and rails carry the
	// home branding, just like a real arena. Rail color/text come from the
	// custom court (apron) or the team colors.
	const homeRail = court?.apron || homeColor;
	const homeText = court?.apronText || teamColor(home, 1, "#ffffff");
	const centerTextColor = court?.centerTextColor || homeRail;
	const benchTextColor = court?.benchTextColor || homeText;
	const railLabel = (home?.name || home?.region || home?.abbrev || "")
		.toUpperCase()
		.slice(0, 14);
	const railText = (left: boolean) => {
		if (!railLabel) {
			return null;
		}
		const x = left ? -RAIL_W / 2 : COURT_W + RAIL_W / 2;
		return (
			<text
				x={0}
				y={0}
				transform={`translate(${x} 25) rotate(${left ? -90 : 90})`}
				textAnchor="middle"
				dominantBaseline="central"
				fontSize={3.2}
				fontWeight={700}
				letterSpacing={0.5}
				fill={homeText}
			>
				{railLabel}
			</text>
		);
	};

	const actorAnim = (actor: CourtActor): "shake" | "swipe" | undefined => {
		if (!scene) {
			return undefined;
		}
		if (scene.kind === "foul") {
			return actor.role === "main"
				? "swipe"
				: actor.role === "victim"
					? "shake"
					: undefined;
		}
		if (scene.kind === "stl" && actor.role === "victim") {
			return "shake";
		}
		return undefined;
	};

	const opposingColor = scene?.t === 0 ? homeColor : awayColor;

	// Give each actor's name tag a placement that avoids colliding with a nearby
	// player's tag: when two actors are within a few feet across the court, put
	// the main actor's tag below and the other's above.
	const nameAboveFor = (actor: CourtActor): boolean => {
		if (!scene) {
			return false;
		}
		// Only the play's actors have name tags, so only they can collide.
		const near = scene.actors.some(
			(o) => o !== actor && o.role !== "onCourt" && Math.abs(o.x - actor.x) < 9,
		);
		if (near) {
			return actor.role !== "main";
		}
		// Otherwise flip tags near the bottom edge so they stay on the court.
		return actor.y > COURT_H - 9;
	};

	// The play text sits BESIDE the action but must never cover a face. Anchor it
	// just past the edge of the whole actor cluster, on whichever side faces
	// center court, vertically centered on the cluster. Background 5-on-5 players
	// are excluded - the text hugs the ACTIVE play, not the whole floor.
	const actorsForText = (scene?.actors ?? []).filter(
		(a) => a.role !== "onCourt",
	);
	const clusterMinX = actorsForText.length
		? Math.min(...actorsForText.map((a) => a.x))
		: COURT_W / 2;
	const clusterMaxX = actorsForText.length
		? Math.max(...actorsForText.map((a) => a.x))
		: COURT_W / 2;
	const clusterMidX = (clusterMinX + clusterMaxX) / 2;
	const clusterMidY = actorsForText.length
		? actorsForText.reduce((s, a) => s + a.y, 0) / actorsForText.length
		: COURT_H - 6;
	const bubbleGoesRight = clusterMidX < COURT_W / 2;
	// Start past the last face on that side (+ a face-width gap) so nobody's
	// covered.
	const bubbleEdgeX = bubbleGoesRight ? clusterMaxX + 6 : clusterMinX - 6;
	const bubbleEdgePct = ((bubbleEdgeX + RAIL_W) / (COURT_W + 2 * RAIL_W)) * 100;
	const textTopPct = ((clusterMidY + APRON) / (COURT_H + 2 * APRON)) * 100;

	// The court BACKGROUND - floor grain, painted key, lines, rails, and center
	// branding - depends only on the home team's court style, not on the current
	// play. The plank/parquet/chevron floors are hundreds (parquet: ~1000+) of
	// SVG nodes; rebuilding and reconciling them on EVERY play is what made the
	// live court lag, worst on mobile and on the fancy patterns. Memoizing it so
	// it's built once (and only rebuilt when the court style actually changes)
	// leaves only the ball, bodies, and text to update per play.
	const courtBackground = useMemo(
		() => (
			<>
				<defs>
					{/* Wood grain: fine dark streaks running along the plank length. */}
					<filter id={grainId} x="-5%" y="-5%" width="110%" height="110%">
						<feTurbulence
							type="fractalNoise"
							baseFrequency="0.02 0.75"
							numOctaves={4}
							seed={11}
							stitchTiles="stitch"
							result="n"
						/>
						<feColorMatrix
							in="n"
							type="matrix"
							values="0 0 0 0 0  0 0 0 0 0  0 0 0 0 0  0.7 0 0 0 -0.28"
						/>
					</filter>
					<clipPath id={clipId}>
						<rect x={0} y={0} width={COURT_W} height={COURT_H} />
					</clipPath>
					<clipPath id={`${clipId}-l`}>
						<rect x={0} y={0} width={cx} height={COURT_H} />
					</clipPath>
					<clipPath id={`${clipId}-r`}>
						<rect x={cx} y={0} width={COURT_W - cx} height={COURT_H} />
					</clipPath>
				</defs>
				{/* Colored apron frame (home team) around the whole floor - baselines
				    AND sidelines */}
				<rect
					x={-RAIL_W}
					y={-APRON}
					width={COURT_W + 2 * RAIL_W}
					height={COURT_H + 2 * APRON}
					rx={1}
					fill={homeRail}
				/>
				<rect x={0} y={0} width={COURT_W} height={COURT_H} fill={woodFill} />
				{/* Floor grain/planks for the chosen wood pattern */}
				{floorDetail()}
				{/* Sponsor images stretched lengthwise along each sideline apron */}
				{sidelineImageURL ? (
					<>
						<image
							href={sidelineImageURL}
							x={0}
							y={-APRON}
							width={COURT_W}
							height={APRON}
							preserveAspectRatio="none"
							opacity={0.97}
						/>
						<image
							href={sidelineImageURL}
							x={0}
							y={COURT_H}
							width={COURT_W}
							height={APRON}
							preserveAspectRatio="none"
							opacity={0.97}
						/>
					</>
				) : null}

				{/* Bench-side sponsor banner: a wide image along the BOTTOM sideline
				    apron only (the broadcast bench/scorer's-table side). */}
				{benchImageURL ? (
					<image
						href={benchImageURL}
						x={0}
						y={COURT_H}
						width={COURT_W}
						height={APRON}
						preserveAspectRatio="none"
						opacity={0.97}
					/>
				) : null}

				{/* Baseline branding: a logo/script in each backcourt behind the
				    baseline (e.g. the "THE FINALS" script). Drawn on the floor under
				    the lines so the key/arc stay visible on top. */}
				{baselineImageURL ? (
					<>
						<image
							href={baselineImageURL}
							x={COURT_W * 0.11 - 9}
							y={25 - 6}
							width={18}
							height={12}
							opacity={0.9}
							preserveAspectRatio="xMidYMid meet"
						/>
						<image
							href={baselineImageURL}
							x={COURT_W * 0.89 - 9}
							y={25 - 6}
							width={18}
							height={12}
							opacity={0.9}
							preserveAspectRatio="xMidYMid meet"
						/>
					</>
				) : null}

				{/* Quarter-court logos: one in each of the four quadrant corners. */}
				{cornerLogoURL
					? (
							[
								[COURT_W * 0.25, 10],
								[COURT_W * 0.25, COURT_H - 10],
								[COURT_W * 0.75, 10],
								[COURT_W * 0.75, COURT_H - 10],
							] as const
						).map(([qx, qy], i) => (
							<image
								key={`corner${i}`}
								href={cornerLogoURL}
								x={qx - 4.5}
								y={qy - 4.5}
								width={9}
								height={9}
								opacity={0.8}
								preserveAspectRatio="xMidYMid meet"
							/>
						))
					: null}

				{/* Painted key (drawn under the lines) */}
				{paintFor(false)}
				{paintFor(true)}

				{railText(true)}
				{railText(false)}

				{/* Court lines */}
				<g fill="none" stroke={lineColor} strokeWidth={0.25} opacity={0.9}>
					<rect x={0} y={0} width={COURT_W} height={COURT_H} />
					<line x1={COURT_W / 2} y1={0} x2={COURT_W / 2} y2={COURT_H} />
					<circle cx={COURT_W / 2} cy={25} r={6} />
				</g>
				{halfMarkings(false)}
				{halfMarkings(true)}

				{/* Center-court branding OVER the lines. During a finals game a big
				    championship trophy dominates center court with the home logo in
				    front of its base; otherwise a large home logo. */}
				{finals ? (
					<image
						href={trophyURL}
						x={COURT_W / 2 - 17}
						y={25 - 21}
						width={34}
						height={42}
						opacity={0.97}
						preserveAspectRatio="xMidYMid meet"
					/>
				) : null}
				{centerLogoURL ? (
					<image
						href={centerLogoURL}
						x={COURT_W / 2 - (finals ? 13 : 15)}
						y={25 - (finals ? 13 : 15)}
						width={finals ? 26 : 30}
						height={finals ? 26 : 30}
						opacity={0.97}
						preserveAspectRatio="xMidYMid meet"
					/>
				) : !finals ? (
					<text
						x={COURT_W / 2}
						y={28}
						textAnchor="middle"
						fontSize={10}
						fontWeight={800}
						fill={homeColor}
						opacity={0.9}
					>
						{home?.abbrev ?? ""}
					</text>
				) : null}

				{/* Center-court script text (e.g. "The Finals"), above the center
				    logo near the top sideline so it stays clear of the logo. */}
				{centerText ? (
					<text
						x={COURT_W / 2}
						y={6}
						textAnchor="middle"
						dominantBaseline="central"
						fontSize={3.4}
						fontWeight={800}
						fontStyle="italic"
						letterSpacing={0.4}
						fill={centerTextColor}
						opacity={0.95}
					>
						{centerText.slice(0, 24)}
					</text>
				) : null}

				{/* Bench-side sponsor text (e.g. "celtics.com"), running along the
				    bottom sideline just inside the court. */}
				{benchText ? (
					<text
						x={COURT_W / 2}
						y={COURT_H - 1.9}
						textAnchor="middle"
						dominantBaseline="central"
						fontSize={2.4}
						fontWeight={700}
						letterSpacing={0.8}
						fill={benchTextColor}
						opacity={0.95}
					>
						{benchText.slice(0, 30)}
					</text>
				) : null}

				{/* Secondary logo, shown in each half-court (backcourt branding) */}
				{secondaryLogoURL ? (
					<>
						<image
							href={secondaryLogoURL}
							x={COURT_W * 0.27 - 6.5}
							y={25 - 6.5}
							width={13}
							height={13}
							opacity={0.82}
							preserveAspectRatio="xMidYMid meet"
						/>
						<image
							href={secondaryLogoURL}
							x={COURT_W * 0.73 - 6.5}
							y={25 - 6.5}
							width={13}
							height={13}
							opacity={0.82}
							preserveAspectRatio="xMidYMid meet"
						/>
					</>
				) : null}
			</>
		),
		// Rebuild only when the court styling actually changes - never per play.
		// The floorDetail/paintFor/halfMarkings/railText closures read exactly
		// these values, so they're covered.
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[
			grainId,
			clipId,
			cx,
			homeRail,
			woodFill,
			woodLine,
			lineColor,
			paintColor,
			floorPattern,
			sidelineImageURL,
			secondaryLogoURL,
			baselineImageURL,
			cornerLogoURL,
			benchImageURL,
			centerText,
			centerTextColor,
			benchText,
			benchTextColor,
			centerLogoURL,
			trophyURL,
			railLabel,
			homeText,
			homeColor,
			finals,
			home?.abbrev,
		],
	);

	return (
		<div
			ref={containerRef}
			className="mb-3 position-relative"
			style={{
				userSelect: "none",
				containerType: "inline-size",
				// Own stacking context pinned at the base level, so real dropdown
				// menus (Play menu, fast-forward) always paint ABOVE the court's
				// absolutely-positioned faces/text instead of under them.
				isolation: "isolate",
				zIndex: 0,
			}}
		>
			<style>{FACE_ANIM_CSS}</style>
			{/* The court is TWO stacked same-viewBox SVGs, not one. The ball is
			    animated by mutating attributes every animation frame, and when it
			    lived in the same SVG as the grain-filtered floor, every frame
			    repainted the whole filtered surface - unusably slow on mobile.
			    Splitting the layers means a ball frame repaints only its own tiny
			    overlay. */}
			<svg viewBox={VIEW} style={{ width: "100%", display: "block" }}>
				{/* Static court (floor, lines, branding) - memoized, see above */}
				{courtBackground}
			</svg>

			{/* Pulse ring (swish / turnover / foul) + the ball, on their own
			    compositor layer (willChange) so per-frame mutation stays cheap. */}
			<svg
				viewBox={VIEW}
				style={{
					position: "absolute",
					inset: 0,
					width: "100%",
					height: "100%",
					pointerEvents: "none",
					willChange: "transform",
				}}
			>
				<circle
					ref={ringRef}
					cx={0}
					cy={0}
					r={1}
					fill="none"
					stroke={sceneColor}
					strokeWidth={0.4}
					style={{ opacity: 0, pointerEvents: "none" }}
				/>
				{/* Impact burst: short lines radiating from a point (a steal poke, a
				    foul collision), driven per-frame via its transform attribute so it
				    expands and fades. Lines use currentColor so the effect is recolored
				    per scene by setting the group's `color`. */}
				<g ref={burstRef} style={{ opacity: 0, pointerEvents: "none" }}>
					{Array.from({ length: 8 }, (_, i) => {
						const a = (i / 8) * 2 * Math.PI;
						return (
							<line
								key={i}
								x1={Math.cos(a) * 0.6}
								y1={Math.sin(a) * 0.6}
								x2={Math.cos(a) * 2.4}
								y2={Math.sin(a) * 2.4}
								stroke="currentColor"
								strokeWidth={0.4}
								strokeLinecap="round"
							/>
						);
					})}
				</g>
				<circle
					ref={ballRef}
					cx={0}
					cy={0}
					r={0.9}
					fill="#e8772e"
					stroke="#7a3a12"
					strokeWidth={0.15}
					style={{ opacity: 0, pointerEvents: "none" }}
				/>
			</svg>

			{/* Everyone on the floor, centered on their spot and ALWAYS keyed by
			    PLAYER (never by scene or role). A background teammate renders as a
			    small jersey CHIP; the moment he's featured in a play he becomes a
			    FACE - but because the key is his pid either way, React reuses the
			    same element, so he simply glides to his action spot (and the face's
			    facesjs SVG isn't torn down and regenerated every play, a big mobile
			    cost). The pid key is also what killed the old duplicate-face/teleport
			    glitch: a foul/steal actor keeps his element instead of unmounting
			    under a scene-specific key and popping up elsewhere. The recoil
			    animation retriggers via animKey inside BodyOnCourt. */}
			{scene
				? scene.actors.map((actor) => {
						const background = actor.role === "onCourt";
						// Background teammates are colored by their OWN team; the play's
						// actors by the scene team (or the opposing team for a
						// defender/victim).
						const anim = background ? undefined : actorAnim(actor);
						const color = background
							? actor.t === 0
								? awayColor
								: homeColor
							: actor.role === "defender" || actor.role === "victim"
								? opposingColor
								: sceneColor;
						return (
							<BodyOnCourt
								key={actor.pid}
								actor={actor}
								season={season}
								lid={lid}
								color={color}
								background={background}
								anim={anim}
								animKey={anim ? scene.key : undefined}
								nameAbove={background ? undefined : nameAboveFor(actor)}
								size={size}
								sceneMs={sceneMs}
							/>
						);
					})
				: null}

			{/* The play line, beside the action - placed past the edge of the whole
			    player cluster so it never covers a face. Skipped entirely on a
			    textless setup beat (kind "advance"), which shows movement only. */}
			{scene && scene.text ? (
				<div
					className="position-absolute"
					style={{
						top: `${textTopPct}%`,
						...(bubbleGoesRight
							? { left: `${bubbleEdgePct}%` }
							: { right: `${100 - bubbleEdgePct}%` }),
						transform: "translateY(-50%)",
						maxWidth: "40%",
						background: "rgba(0,0,0,0.82)",
						color: "#fff",
						borderLeft: `3px solid ${sceneColor}`,
						borderRadius: 3,
						padding: "2px 8px",
						fontSize: "clamp(10px, 2cqw, 15px)",
						fontWeight: 500,
						lineHeight: 1.25,
						pointerEvents: "none",
						zIndex: 3,
					}}
				>
					{scene.text}
					{scene.score ? (
						<div style={{ marginTop: 2, textAlign: "center" }}>
							{scene.score}
						</div>
					) : null}
				</div>
			) : null}
		</div>
	);
};

export default LiveCourt;
