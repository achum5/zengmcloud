import type { ReactNode } from "react";
import { courtRandom } from "./courtRng.ts";
import {
	defenseSlots,
	dirFor,
	FIELD_LEN,
	fieldX,
	MID_Y,
	offenseSlots,
	rand,
	runControlPoints,
	snapAcross,
	synthEndPoint,
	synthLooseBall,
	toField,
	UPRIGHT_HALF_W,
} from "./fieldSpots.ts";
import type { BallFlight } from "./fieldAnimation.ts";
import {
	buildFormationActors,
	formationFor,
	type FieldActor,
	type FieldPlayer,
	type FieldScene,
	type FieldSceneKind,
} from "./fieldSpots.ts";

// TURNING A PLAY-BY-PLAY LINE INTO SOMETHING TO WATCH.
//
// Every football event the sim emits names what happened, who it happened to,
// and - crucially, unlike basketball - HOW FAR. That is most of a scene
// already: the line of scrimmage says where the play starts, the yards say
// where it ends, and the type says what the ball did in between (a spiral, a
// handoff, a punt hanging, a ball squirting loose).
//
// What this module decides is the rest: which of the twenty-two are worth a
// face, where the other twenty stand, and the shape of the ball's flight. It is
// deliberately a plain function over an event and a small context, so the live
// game view keeps only the context and the whole mapping can be read - and
// tested - in one place.

export type FieldSceneCtx = {
	// Which hash the ball is on for the play being shown. Re-rolled when a new
	// play starts, so a dropback and the pass that follows are staged from the
	// same spot instead of the offense sliding sideways mid-play.
	ballAcross: number;
	// Identity of the play the context belongs to, so a new one can be detected.
	playCount: number;
	// Where the drive's earlier plays ended, in field coordinates.
	driveMarks: number[];
};

export const newFieldSceneCtx = (): FieldSceneCtx => ({
	ballAcross: 0,
	playCount: -1,
	driveMarks: [],
});

// The pieces of the live game's sport state this module reads. Declared
// structurally rather than importing the football SportState so this file has
// no dependency on a sport-specific UI module.
type SportStateLike = {
	t: 0 | 1;
	scrimmage: number;
	toGo: number;
	awaitingKickoff: boolean;
	awaitingAfterTouchdown: boolean;
	plays: {
		down: number;
		toGo: number;
		scrimmage: number;
		yards: number;
		countsTowardsNumPlays?: boolean;
		countsTowardsYards?: boolean;
		t: 0 | 1;
	}[];
};

type AnyEvent = {
	type: string;
	t?: 0 | 1;
	names?: string[];
	yds?: number;
	td?: boolean;
	made?: boolean;
	lost?: boolean;
	safety?: boolean;
	touchback?: boolean;
	success?: boolean;
};

// A one-line summary of what the ball did, which is all a scene really needs on
// top of the formation. Each event type maps to exactly one of these.
type Beat = {
	kind: FieldSceneKind;
	// Yards the ball travels downfield from the line of scrimmage. Negative is
	// backwards, which is what a sack or a loss is.
	yards: number;
	flight: BallFlight;
	// Whose players are shown as the offense. A return flips this: the team
	// carrying the ball is the one running it back.
	offenseT: 0 | 1;
	// Names to feature, in order: the man with the ball, then whoever did
	// something to him.
	mainName?: string;
	defenderName?: string;
	// The play starts from the punter's, kicker's or quarterback's spot rather
	// than from the line itself.
	launchDepth?: number;
	// The featured man IS the one the ball leaves from, so he stays at the
	// launch spot instead of being dragged downfield after his own kick.
	mainAtLaunch?: boolean;
	// A second face: whoever threw or kicked it, shown where it left his hands.
	launcherName?: string;
	// The ball ends up loose rather than in someone's hands.
	loose?: boolean;
	// Carried rather than thrown or kicked: the runner and the ball travel the
	// same weaving path.
	carried?: boolean;
	scored?: boolean;
	// A place kick that did not go through. The court makes a make and a miss
	// impossible to confuse; a field goal has to do the same, so a miss visibly
	// sails outside the upright rather than ending up in the same place.
	missed?: boolean;
};

const beatFor = (
	event: AnyEvent,
	sportState: SportStateLike,
	// The event's team in BOX SCORE display order. The football processor swaps
	// the raw team index so the home team sits at the bottom of the box score,
	// and sportState.t is already swapped - so the caller does the swap once and
	// nothing in here ever touches event.t.
	displayT: 0 | 1 | undefined,
): Beat | undefined => {
	const t = displayT ?? sportState.t;
	const names = event.names ?? [];
	const yds = event.yds ?? 0;
	const scored = event.td === true;

	switch (event.type) {
		case "dropback":
			// The set before the throw: the quarterback backs up, nothing else has
			// happened yet.
			return {
				kind: "set",
				yards: -5.5,
				flight: "snap",
				offenseT: t,
				mainName: names[0],
			};
		case "handoff":
			return {
				kind: "run",
				yards: -4,
				flight: "pitch",
				offenseT: t,
				mainName: names.at(-1),
			};
		case "run":
		case "kneel":
			return {
				kind: "run",
				yards: yds,
				flight: "carry",
				offenseT: t,
				mainName: names[0],
				carried: true,
				scored,
			};
		case "passComplete":
			return {
				kind: "pass",
				yards: yds,
				flight: "pass",
				offenseT: t,
				mainName: names[1] ?? names[0],
				launcherName: names[0],
				launchDepth: 5.5,
				scored,
			};
		case "passIncomplete":
			return {
				kind: "incomplete",
				yards: Math.max(2, yds),
				flight: "pass",
				offenseT: t,
				mainName: names[1] ?? names[0],
				launcherName: names[0],
				launchDepth: 5.5,
				loose: true,
			};
		case "sack":
			return {
				kind: "sack",
				yards: yds,
				flight: "carry",
				offenseT: t,
				mainName: names[0],
				defenderName: names[1],
				carried: true,
			};
		case "fumble":
			return {
				kind: "fumble",
				yards: 0,
				flight: "loose",
				offenseT: t,
				mainName: names[0],
				defenderName: names[1],
				loose: true,
			};
		case "fumbleRecovery":
			return {
				kind: "fumble",
				yards: yds,
				flight: "carry",
				offenseT: t,
				mainName: names[0],
				carried: true,
				scored,
			};
		case "interception":
			return {
				kind: "interception",
				yards: yds,
				flight: "pass",
				// event.t is the team that INTERCEPTED it, but the throw, the line of
				// scrimmage and the twenty-two all belong to the offense that threw
				// it - so the field stays their way round and the interceptor is
				// featured as the defender he is.
				offenseT: t === 0 ? 1 : 0,
				defenderName: names[0],
				launchDepth: 5.5,
			};
		case "interceptionReturn":
		case "kickoffReturn":
		case "puntReturn":
			return {
				kind: "return",
				yards: yds,
				flight: "carry",
				offenseT: t,
				mainName: names[0],
				carried: true,
				scored,
			};
		case "punt":
			return {
				kind: "punt",
				yards: yds,
				flight: "punt",
				offenseT: t,
				mainName: names[0],
				launchDepth: 14,
				mainAtLaunch: true,
			};
		case "kickoff":
		case "onsideKick":
			return {
				kind: "kickoff",
				// A kickoff's `yds` is not a distance: it is the yard line the ball
				// was kicked TO, counted from the RECEIVING team's goal line. Kicked
				// straight through as a distance it made every kickoff a twelve-yard
				// dribbler. Convert it into how far the ball actually flew from where
				// it was teed up.
				yards:
					event.type === "onsideKick"
						? 12
						: 100 - yds - (sportState.plays.at(-1)?.scrimmage ?? 35),
				flight: "kick",
				offenseT: t,
				mainName: names[0],
				launchDepth: 7,
				mainAtLaunch: true,
			};
		case "onsideKickRecovery":
			return {
				kind: "kickoff",
				yards: 0,
				flight: "carry",
				offenseT: t,
				mainName: names[0],
				carried: true,
			};
		case "fieldGoalAttempt":
		case "fieldGoal":
		case "extraPointAttempt":
		case "extraPoint":
		case "shootoutShot":
			return {
				kind: "kick",
				// A place kick is snapped back before it is struck, and it travels to
				// the posts rather than to a yard line.
				yards: Math.max(10, yds - 17),
				flight: "kick",
				offenseT: t,
				mainName: names[0],
				launchDepth: 8,
				mainAtLaunch: true,
				scored: event.made === true,
				missed: event.made === false,
			};
		case "penalty":
			return {
				kind: "penalty",
				yards: 0,
				flight: "loose",
				// Same as an injury: a flag on the defense is not a change of
				// possession, so the field keeps facing the way it was.
				offenseT: sportState.t,
				mainName: names[0],
				loose: true,
			};
		case "injury":
			return {
				kind: "injury",
				yards: 0,
				flight: "carry",
				// An injury is often to a defender, and letting the hurt man's team
				// stand in for the offense turned the field around for one beat.
				// Whoever has the ball still has it.
				offenseT: sportState.t,
				mainName: names[0],
			};
		case "timeout":
		case "twoMinuteWarning":
		case "quarter":
		case "overtime":
		case "gameOver":
		case "turnoverOnDowns":
		case "twoPointConversionFailed":
			return {
				kind: "dead",
				yards: 0,
				flight: "carry",
				offenseT: sportState.t,
			};
		default:
			return undefined;
	}
};

// Football positions the box score reports, collapsed to the groups the
// formations are written in - so a "G" fills a lineman's slot and a "DE" fills
// a defensive lineman's.
const rosterFor = (
	players: { pid: number; name: string; pos?: string }[],
): FieldPlayer[] =>
	players.map((p) => ({ pid: p.pid, name: p.name, pos: p.pos }));

export const buildFieldScene = ({
	event,
	displayT,
	text,
	score,
	sportState,
	players,
	resolvePid,
	ctx,
}: {
	event: AnyEvent;
	displayT: 0 | 1 | undefined;
	text: ReactNode;
	score: ReactNode | undefined;
	sportState: SportStateLike;
	// Both teams' box score players, in display order [away, home].
	players: [
		{ pid: number; name: string; pos?: string }[],
		{ pid: number; name: string; pos?: string }[],
	];
	resolvePid: (t: 0 | 1, name: string | undefined) => number | undefined;
	ctx: FieldSceneCtx;
}): Omit<FieldScene, "key"> | undefined => {
	const beat = beatFor(event, sportState, displayT);
	if (!beat) {
		return undefined;
	}

	const play = sportState.plays.at(-1);

	// A new play re-rolls where the ball is spotted, and a new drive clears the
	// marks the last one left behind.
	if (sportState.plays.length !== ctx.playCount) {
		ctx.playCount = sportState.plays.length;
		ctx.ballAcross = snapAcross();
		if (sportState.plays.length <= 1) {
			ctx.driveMarks = [];
		}
	}

	const offenseT = beat.offenseT;
	const defenseT: 0 | 1 = offenseT === 0 ? 1 : 0;
	// Which way the play RUNS: the team with the ball attacks its own way.
	const dir = dirFor(offenseT);

	// WHICH TEAM'S YARD LINES THE NUMBERS ARE COUNTED FROM, which is not always
	// the team with the ball. Every scrimmage the sim reports is measured from
	// the goal line of whoever snapped it, and a RETURN keeps those numbers: an
	// interception at the passer's 61 becomes a return sub-play still counted
	// from the passer's end (with the return's yards going NEGATIVE). Read in
	// the returner's frame instead, a kickoff return lands on the wrong goal
	// line and the whole coverage unit stacks up in the corner - which is
	// exactly what it used to do. So the frame is sportState.t, which holds the
	// snapping team until the next clock event, and only the DIRECTION of travel
	// comes from whoever is actually carrying it.
	const frameDir = dirFor(sportState.t);

	const scrimmage = clampScrimmage(play?.scrimmage ?? sportState.scrimmage);
	const losX = fieldX(scrimmage, frameDir);

	// A first down line is only meaningful during a normal down - never on a
	// kickoff, an extra point, or when the line to gain is the goal line.
	const toGo = play?.toGo ?? sportState.toGo;
	const firstDownX =
		beat.kind === "kickoff" ||
		beat.kind === "kick" ||
		beat.kind === "return" ||
		sportState.awaitingKickoff ||
		toGo <= 0 ||
		scrimmage + toGo >= 100
			? undefined
			: fieldX(scrimmage + toGo, frameDir);

	const formation = formationFor(beat.kind);
	const across = ctx.ballAcross;

	// The featured players, and where the play puts them.
	const mainPid = resolvePid(offenseT, beat.mainName);
	// A defender named on the play belongs to the other team - except on a
	// fumble, where the sim names the man who forced it, and on an
	// interception return, where everyone named is on the returning side.
	const defenderPid = resolvePid(defenseT, beat.defenderName);

	const start = beat.launchDepth
		? toField(losX, dir, beat.launchDepth, across)
		: toField(losX, dir, beat.kind === "return" ? 0 : 1, across);
	const end = beat.loose
		? synthLooseBall(
				synthEndPoint(losX, dir, beat.yards, across, 6),
				dir,
				beat.kind === "fumble" ? 5 : 2.5,
			)
		: synthEndPoint(
				losX,
				dir,
				beat.yards,
				across,
				// A long play has room to have drifted a long way across the field; a
				// plunge up the middle does not.
				Math.min(14, 2 + Math.abs(beat.yards) * 0.32),
			);

	const actors: FieldActor[] = [];
	const featured = new Set<number>();

	if (mainPid !== undefined) {
		featured.add(mainPid);
		// A thrown ball's target is where it lands and a carried ball's man ends
		// where the run ended - but a kicker stays where he kicked from. Dragging
		// a punter forty yards downfield behind his own punt was the first thing
		// that looked wrong.
		const at = beat.mainAtLaunch ? start : end;
		actors.push({
			pid: mainPid,
			name: beat.mainName!,
			x: at.x,
			y: at.y,
			role: "main",
			t: offenseT,
		});
	}
	if (defenderPid !== undefined && defenderPid !== mainPid) {
		featured.add(defenderPid);
		// The man who made the play arrives a stride away from where it ended -
		// unless he IS the play (an interception), in which case he is at the ball.
		const solo = mainPid === undefined;
		actors.push({
			pid: defenderPid,
			name: beat.defenderName!,
			x: solo ? end.x : end.x - dir * 1.4,
			y: solo ? end.y : end.y + 1.6,
			role: "defender",
			t: defenseT,
		});
	}
	// Whoever threw or kicked it, at the spot it left his hands.
	const launcherPid = beat.mainAtLaunch
		? undefined
		: resolvePid(offenseT, beat.launcherName);
	if (launcherPid !== undefined && !featured.has(launcherPid)) {
		featured.add(launcherPid);
		actors.push({
			pid: launcherPid,
			name: beat.launcherName!,
			x: start.x,
			y: start.y,
			role: "passer",
			t: offenseT,
		});
	}

	// Everybody else fills the formation around them.
	const offSlots = offenseSlots(formation);
	const defSlots = defenseSlots(formation);
	actors.push(
		...buildFormationActors({
			players: rosterFor(players[offenseT]),
			slots: offSlots,
			losX,
			dir,
			ballAcross: across,
			t: offenseT,
			skipPids: featured,
		}),
		...buildFormationActors({
			players: rosterFor(players[defenseT]),
			slots: defSlots,
			losX,
			dir,
			ballAcross: across,
			t: defenseT,
			skipPids: featured,
		}),
	);

	// A place kick travels to the posts, not to a yard line - and a miss goes
	// past one of them, so nobody has to read the text to know.
	const ballTo =
		beat.kind === "kick"
			? {
					x: dir === 1 ? FIELD_LEN - 1 : 1,
					y:
						MID_Y +
						(beat.missed
							? (courtRandom() < 0.5 ? -1 : 1) * (UPRIGHT_HALF_W + rand(1.5, 5))
							: rand(-UPRIGHT_HALF_W * 0.55, UPRIGHT_HALF_W * 0.55)),
				}
			: end;

	const scene: Omit<FieldScene, "key"> = {
		kind: beat.kind,
		t: offenseT,
		dir,
		losX,
		firstDownX,
		actors: dedupe(actors),
		text,
		score,
		down:
			play &&
			!sportState.awaitingKickoff &&
			// An extra point has no down and distance, and the one carried over
			// from the touchdown ("1st & goal") is just wrong.
			!sportState.awaitingAfterTouchdown &&
			beat.kind !== "kickoff"
				? downAndDistance(play.down, play.toGo, play.scrimmage)
				: undefined,
		ball: {
			flight: beat.flight,
			from: start,
			to: ballTo,
			curve: beat.carried ? runControlPoints(start, ballTo) : undefined,
		},
		impact: beat.scored
			? { kind: "score", at: ballTo }
			: beat.kind === "run" ||
					beat.kind === "pass" ||
					beat.kind === "sack" ||
					beat.kind === "return"
				? { kind: "tackle", at: ballTo }
				: undefined,
		driveMarks: [...ctx.driveMarks],
		drive: driveSummary(sportState),
	};

	// Bank where this play ended, so the drive's shape shows on the field.
	if (play && beat.kind !== "set" && beat.kind !== "dead") {
		ctx.driveMarks = [...ctx.driveMarks.slice(-9), ballTo.x];
	}

	return scene;
};

// "4 plays, 31 yards" - the one thing the old drive chart said that the field
// itself cannot, so it keeps its place under the play text.
const driveSummary = (sportState: SportStateLike): string | undefined => {
	let yards = 0;
	let numPlays = 0;
	for (const play of sportState.plays) {
		if (play.countsTowardsYards) {
			yards += play.yards;
		}
		if (play.countsTowardsNumPlays) {
			numPlays += 1;
		}
	}
	if (numPlays === 0) {
		return undefined;
	}
	return `Drive: ${numPlays} ${numPlays === 1 ? "play" : "plays"}, ${yards} ${
		Math.abs(yards) === 1 ? "yard" : "yards"
	}`;
};

const clampScrimmage = (scrimmage: number): number =>
	Math.min(99.5, Math.max(0.5, scrimmage));

// "2nd & 7", or "2nd & goal" when the line to gain is the goal line - which is
// how it is said, and how every scoreboard shows it.
export const downAndDistance = (
	down: number,
	toGo: number,
	scrimmage: number,
): string | undefined => {
	if (!down) {
		return undefined;
	}
	const ordinal = ["1st", "2nd", "3rd", "4th"][down - 1] ?? `${down}th`;
	return `${ordinal} & ${scrimmage + toGo >= 100 ? "goal" : toGo}`;
};

// The field keys every body by pid, so one player appearing twice in a scene
// collides as a React key and strands a face on the grass. Keep the first
// occurrence: the more meaningful role is pushed first.
const dedupe = (actors: FieldActor[]): FieldActor[] => {
	const seen = new Set<number>();
	return actors.filter((a) => {
		if (seen.has(a.pid)) {
			return false;
		}
		seen.add(a.pid);
		return true;
	});
};
