import type { ReactNode } from "react";
import { courtRandom } from "./courtRng.ts";
import {
	clampX,
	clampY,
	type Dir,
	dirFor,
	FIELD_LEN,
	fieldX,
	MID_Y,
	rand,
	runControlPoints,
	snapAcross,
	synthEndPoint,
	synthLooseBall,
	toField,
	UPRIGHT_HALF_W,
	type FieldPoint,
} from "./fieldSpots.ts";
import type { BallFlight } from "./fieldAnimation.ts";
import {
	assignHuddle,
	assignScramble,
	assignSpecialTeams,
	carrierPath,
	type SpecialTeamsKind,
} from "./specialTeams.ts";
import {
	chooseDefenseFront,
	chooseOffenseFormation,
	defenseSlots,
	offenseSlots,
	specialTeamsDefense,
	specialTeamsOffense,
	type DefenseFront,
	type OffenseFormation,
} from "./formations.ts";
import {
	assignCoverage,
	chooseCoverage,
	type Coverage,
	defenseLabel,
} from "./coverages.ts";
import {
	assignRoutes,
	engageLine,
	assignRunBlocking,
	assignRunPursuit,
	callPass,
	callRun,
	type PassConcept,
	type RunScheme,
	runPath,
} from "./playbook.ts";
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
	// The play being run, kept for as long as the play lasts so the dropback and
	// the throw are the same call.
	call?: { name: string; concept?: PassConcept; scheme?: RunScheme };
	callIsRun?: boolean;
	// What the defense is playing, held for the play the same way.
	coverage?: Coverage;
	// Whether this play has a man in motion, decided once so he does not start
	// motioning again halfway through it.
	motion?: boolean;
	// The set the offense is in and the front the defense answered with, held
	// for the play so it does not change between the snap and the throw.
	formation?: OffenseFormation;
	front?: DefenseFront;
	// The name of whoever took the last snap, so a quarterback keeper can be
	// told from a handoff without the sim saying so.
	quarterback?: string;
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
	// A quarterback taking a knee, which is a scheme of its own and must not be
	// mistaken for a run that happened to lose a yard.
	kneel?: boolean;
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
				kneel: event.type === "kneel",
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

// The plays that happen from scrimmage, which are the ones with a call behind
// them. A punt, a kickoff and a return have their own alignments and no
// concept to run.
const SCRIMMAGE_KINDS = new Set<FieldSceneKind>([
	"set",
	"run",
	"pass",
	"incomplete",
	"sack",
	"interception",
	"fumble",
]);

// THE CALL IS MADE ONCE PER PLAY, not once per event. A dropback and the throw
// that follows it are the same play, so they get the same concept - otherwise
// the five receivers change what they are running halfway through it.
//
// And the call is made from what a coach would know when he made it: the down
// and the distance. Not from how far the ball ended up travelling, which is
// decided by the defence, and which is why the man who actually caught it runs
// his route only as far as the catch (see trimRouteTo).
const playCallFor = ({
	beat,
	down,
	toGo,
	ctx,
}: {
	beat: Beat;
	down: number;
	toGo: number;
	ctx: FieldSceneCtx;
}): { name: string; concept?: PassConcept; scheme?: RunScheme } => {
	const running = beat.kind === "run" || beat.kind === "fumble";
	if (ctx.call && ctx.callIsRun === running) {
		return ctx.call;
	}
	const call = running
		? (() => {
				const scheme = callRun({
					yards: beat.yards,
					down,
					toGo,
					byQuarterback: beat.mainName === ctx.quarterback,
					kneel: beat.kneel === true,
				});
				return { name: scheme.name, scheme };
			})()
		: (() => {
				// Only a resolved throw knows how far the ball went; a dropback's
				// "yards" is how far the quarterback retreated.
				const thrown =
					beat.kind === "pass" ||
					beat.kind === "incomplete" ||
					beat.kind === "interception";
				const concept = callPass({
					airYards: thrown ? beat.yards : undefined,
					toGo,
					sacked: beat.kind === "sack",
				});
				return { name: concept.name, concept };
			})();
	ctx.call = call;
	ctx.callIsRun = running;
	return call;
};

// A route the ball came down on before it ran out. Keeps the shape the receiver
// was actually running - the stem, the break - and finishes it at the catch, so
// a dig caught at eleven yards is a dig, not a dig plus another twenty yards of
// nobody throwing it.
const trimRouteTo = (
	path: FieldPoint[],
	end: FieldPoint,
	losX: number,
	dir: Dir,
): FieldPoint[] => {
	const target = (end.x - losX) * dir;
	const kept = path.filter(
		(p, i) => i === 0 || (p.x - losX) * dir < target - 0.5,
	);
	return [...kept, end];
};

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
		// A new play is a new call. Clearing it here is what makes the call last
		// exactly one play and no longer.
		ctx.call = undefined;
		ctx.callIsRun = undefined;
		ctx.formation = undefined;
		ctx.front = undefined;
		ctx.coverage = undefined;
		ctx.motion = courtRandom() < 0.3;
		if (sportState.plays.length <= 1) {
			ctx.driveMarks = [];
		}
	}
	// Who took the snap. A handoff names the quarterback first, a dropback names
	// only him - either way it is the one piece the run schemes need in order to
	// tell a keeper from a give.
	if (event.type === "dropback" || event.type === "handoff") {
		ctx.quarterback = event.names?.[0];
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

	const unitKind = formationFor(beat.kind);
	const across = ctx.ballAcross;

	// The featured players, and where the play puts them.
	const mainPid = resolvePid(offenseT, beat.mainName);
	// A defender named on the play belongs to the other team - except on a
	// fumble, where the sim names the man who forced it, and on an
	// interception return, where everyone named is on the returning side.
	const defenderPid = resolvePid(defenseT, beat.defenderName);
	const launcherPid = beat.mainAtLaunch
		? undefined
		: resolvePid(offenseT, beat.launcherName);

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

	// EVERY MAN GETS A SLOT FIRST, featured or not. That is what lets the play
	// be staged rather than merely placed: the receiver the sim named has to be
	// the man running the route his slot was given, and he can only be that if
	// he is IN the formation instead of bolted on beside it.
	//
	// A play from scrimmage gets a FORMATION and a FRONT, chosen from the
	// situation and then held for the whole play so a dropback and the throw
	// after it are the same snap. Special teams are their own units and have no
	// situation to read.
	const stOffense = specialTeamsOffense(unitKind);
	const stDefense = specialTeamsDefense(unitKind);
	const down = play?.down ?? 1;
	if (!stOffense && !ctx.formation) {
		ctx.formation = chooseOffenseFormation({
			running: beat.kind === "run" || beat.kind === "fumble",
			down,
			toGo,
			scrimmage,
		});
		ctx.front = chooseDefenseFront({
			offense: ctx.formation,
			down,
			toGo,
			scrimmage,
		});
	}
	const offFormation: OffenseFormation | undefined = stOffense
		? undefined
		: ctx.formation;
	const offSlots = stOffense ?? offFormation?.slots ?? offenseSlots(unitKind);
	const defSlots = stDefense ?? ctx.front?.slots ?? defenseSlots(unitKind);
	const noSkip = new Set<number>();
	const namedOffense = new Set(
		[mainPid, launcherPid].filter((p): p is number => p !== undefined),
	);
	const namedDefense = new Set(
		[defenderPid].filter((p): p is number => p !== undefined),
	);
	let offense = buildFormationActors({
		players: rosterFor(players[offenseT]),
		slots: offSlots,
		losX,
		dir,
		ballAcross: across,
		t: offenseT,
		skipPids: noSkip,
		preferPids: namedOffense,
	});
	let defense = buildFormationActors({
		players: rosterFor(players[defenseT]),
		slots: defSlots,
		losX,
		dir,
		ballAcross: across,
		t: defenseT,
		skipPids: noSkip,
		preferPids: namedDefense,
	});

	// THE CALL. A play from scrimmage gets a concept or a scheme and everybody
	// gets a job; the special teams keep the alignments they already had.
	const geom = { losX, dir, ballAcross: across };
	let playName: string | undefined;
	let defenseName: string | undefined;
	if (SCRIMMAGE_KINDS.has(beat.kind)) {
		const call = playCallFor({ beat, down, toGo, ctx });
		playName = call.name;
		if (call.concept) {
			const protectDepth = beat.launchDepth ?? 5.5;
			// The defense declares first, because the receivers have to be able to
			// read it - which is the entire craft of playing the position.
			if (!ctx.coverage) {
				ctx.coverage = chooseCoverage({
					down,
					toGo,
					scrimmage,
					sacked: beat.kind === "sack",
				});
			}
			offense = assignRoutes({
				actors: offense,
				slots: offSlots,
				concept: call.concept,
				geom,
				protectDepth,
				empty: offFormation?.empty,
				motion: ctx.motion,
				shell: ctx.coverage.shell,
			});
			defense = assignCoverage({
				defenders: defense,
				defSlots,
				receivers: offense,
				coverage: ctx.coverage,
				target: start,
				geom,
				reachTarget: beat.kind === "sack",
				// Before the throw there is no ball to break on; after it, the
				// nearest zone defender closes.
				ballTo:
					beat.kind === "pass" ||
					beat.kind === "incomplete" ||
					beat.kind === "interception"
						? end
						: undefined,
			});
			defenseName = defenseLabel(
				ctx.front?.name ?? "Base",
				ctx.coverage.name,
			);
		} else if (call.scheme?.dropback) {
			// A SCRAMBLE started as a pass and stopped being one. The line is
			// protecting, the receivers are running routes, and the defense is in
			// its coverage - the quarterback is simply the man who ended up with
			// the ball, which is the whole difference between a scramble and a
			// designed quarterback run.
			if (!ctx.coverage) {
				ctx.coverage = chooseCoverage({
					down,
					toGo,
					scrimmage,
					sacked: false,
				});
			}
			const concept = callPass({ airYards: undefined, toGo, sacked: false });
			offense = assignRoutes({
				actors: offense,
				slots: offSlots,
				concept,
				geom,
				protectDepth: 5.5,
				empty: offFormation?.empty,
				motion: ctx.motion,
				shell: ctx.coverage.shell,
			});
			defense = assignCoverage({
				defenders: defense,
				defSlots,
				receivers: offense,
				coverage: ctx.coverage,
				target: toField(losX, dir, 5.5, across),
				geom,
				reachTarget: false,
				ballTo: undefined,
			});
			defenseName = defenseLabel(
				ctx.front?.name ?? "Base",
				ctx.coverage.name,
			);
		} else if (call.scheme) {
			defenseName = ctx.front?.name;
			offense = assignRunBlocking({
				actors: offense,
				slots: offSlots,
				scheme: call.scheme,
				geom,
			});
			defense = assignRunPursuit({ defenders: defense, ballEnd: end, geom });
		}
		// The man the play happened to runs the play, not his route: a carrier
		// follows the scheme to where he was actually brought down, and a target
		// runs his route only as far as the catch.
		if (mainPid !== undefined) {
			offense = offense.map((a) => {
				if (a.pid !== mainPid) {
					return a;
				}
				if (call.scheme) {
					const path = runPath({
						start: { x: a.x, y: a.y },
						end,
						scheme: call.scheme,
						losX,
						dir,
					});
					return { ...a, x: end.x, y: end.y, path, delay: call.scheme.hold };
				}
				const route = a.path;
				const path =
					route && route.length > 1
						? trimRouteTo(route, end, losX, dir)
						: [{ x: a.x, y: a.y }, end];
				return { ...a, x: end.x, y: end.y, path };
			});
		}
	}

	// SPECIAL TEAMS get their own staging: coverage lanes, gunners, a wedge, a
	// kick rush. They have no concept and no coverage, but they are the plays
	// with the most movement in football and they were the ones standing still.
	if (stOffense && stDefense) {
		const stKind: SpecialTeamsKind | undefined =
			beat.kind === "punt"
				? "punt"
				: beat.kind === "kick"
					? "kick"
					: beat.kind === "kickoff"
						? "kickoff"
						: beat.kind === "return"
							? "return"
							: undefined;
		if (stKind) {
			const carrier =
				stKind === "return" ? carrierPath(start, end) : undefined;
			const staged = assignSpecialTeams({
				kind: stKind,
				kicking: offense,
				kickingSlots: offSlots,
				receiving: defense,
				receivingSlots: defSlots,
				geom,
				launch: start,
				landing: end,
				carrier,
			});
			offense = staged.kicking;
			defense = staged.receiving;
			// The returner runs the same weave his ball does.
			if (carrier && mainPid !== undefined) {
				offense = offense.map((a) =>
					a.pid === mainPid
						? { ...a, x: end.x, y: end.y, path: carrier }
						: a,
				);
			}
			defenseName = stKind === "kick" ? "Kick Block" : undefined;
		}
	}

	// THE LINE MEETS THE RUSH. Both sides were sent to spots worked out without
	// reference to each other, so blockers and rushers slid straight through one
	// another. Pairing them up and bringing each pair together is what makes the
	// middle of the field look like football rather than like two teams playing
	// on separate fields. The man who actually made the sack is left out of it:
	// he is the one who got through.
	if (SCRIMMAGE_KINDS.has(beat.kind) && beat.kind !== "set") {
		const blockers = offense.filter(
			(a) => a.job === "block" || a.job === "pull",
		);
		const rushers = defense.filter(
			(a) => a.job === "rush" && a.pid !== defenderPid,
		);
		if (blockers.length > 0 && rushers.length > 0) {
			const engaged = engageLine({
				blockers,
				rushers,
				// A sack means the pocket lost, so the rush wins more ground.
				push: beat.kind === "sack" ? 0.68 : 0.42,
			});
			const byPid = new Map(
				[...engaged.blockers, ...engaged.rushers].map((a) => [a.pid, a]),
			);
			offense = offense.map((a) => byPid.get(a.pid) ?? a);
			defense = defense.map((a) => byPid.get(a.pid) ?? a);
		}
	}

	// PLAY IS STOPPED and the two teams go to their own huddles rather than
	// standing where the whistle caught them.
	if (beat.kind === "dead") {
		offense = assignHuddle({ actors: offense, geom, depth: 9, across: -13 });
		defense = assignHuddle({ actors: defense, geom, depth: -11, across: 13 });
	}

	// A FUMBLE cancels every assignment on the field at once: the men near it
	// stop doing whatever they were doing and go after it.
	if (beat.kind === "fumble" && beat.loose) {
		offense = assignScramble({ actors: offense, ball: end, count: 4 });
		defense = assignScramble({ actors: defense, ball: end, count: 4 });
	}

	// The path the man with the ball is running, so the ball can travel it with
	// him rather than flying a curve of its own beside him.
	const carriedPath = offense.find((a) => a.pid === mainPid)?.path;

	const actors: FieldActor[] = [...offense, ...defense];
	const featured = new Set<number>();

	// THE MEN THE SIM NAMED. Each is already somewhere in the twenty-two, so he
	// is PROMOTED - given his face, his name tag and the spot the play left him
	// - rather than added a second time.
	const promote = (
		pid: number | undefined,
		name: string | undefined,
		role: FieldActor["role"],
		at: FieldPoint,
		t: 0 | 1,
		// PIN him there whatever job the unit gave him. A kicker who does not win
		// the kicker's slot in his own unit picks up a coverage lane instead and
		// sprints forty yards downfield behind his own kick - so the man the play
		// says kicked it is placed where he kicked it, and the slot he happened to
		// fill does not get a vote.
		pin = false,
	) => {
		if (pid === undefined || name === undefined || featured.has(pid)) {
			return;
		}
		featured.add(pid);
		const i = actors.findIndex((a) => a.pid === pid);
		if (i === -1) {
			// Not one of the twenty-two on the field for this play - a returner, a
			// kicker, a man the sim used off the depth chart. He still gets shown.
			actors.push({ pid, name, x: at.x, y: at.y, role, t });
			return;
		}
		const existing = actors[i]!;
		actors[i] = {
			...existing,
			role,
			// A man who was given a path keeps it: the path is how he GOT here, and
			// it already ends where the play left him. A pinned man keeps neither.
			...(pin
				? { x: at.x, y: at.y, path: undefined, delay: undefined }
				: existing.path && existing.path.length > 1
					? {}
					: { x: at.x, y: at.y }),
		};
	};

	// A thrown ball's target is where it lands and a carried ball's man ends
	// where the run ended - but a kicker stays where he kicked from. Dragging a
	// punter forty yards downfield behind his own punt was the first thing that
	// looked wrong.
	promote(
		mainPid,
		beat.mainName,
		"main",
		beat.mainAtLaunch ? start : end,
		offenseT,
		beat.mainAtLaunch,
	);
	// The man who made the play arrives a stride away from where it ended -
	// unless he IS the play (an interception), in which case he is at the ball.
	const solo = mainPid === undefined;
	promote(
		defenderPid,
		beat.defenderName,
		"defender",
		solo ? end : { x: end.x - dir * 1.4, y: clampY(end.y + 1.6) },
		defenseT,
	);
	// THE MAN WHO MADE THE PLAY runs to it. He was being placed a stride from
	// where the play ended with no way of having got there, which reads as a
	// defender teleporting onto the tackle.
	if (defenderPid !== undefined && TACKLE_KINDS.has(beat.kind)) {
		const at = mainPid === undefined
			? end
			: { x: end.x - dir * 1.4, y: clampY(end.y + 1.6) };
		const i = actors.findIndex((a) => a.pid === defenderPid);
		if (i >= 0) {
			const from = actors[i]!.path?.[0] ?? { x: actors[i]!.x, y: actors[i]!.y };
			actors[i] = {
				...actors[i]!,
				x: at.x,
				y: at.y,
				path: [
					from,
					{
						x: from.x + (at.x - from.x) * 0.55,
						y: clampY(from.y + (at.y - from.y) * 0.65),
					},
					at,
				],
			};
		}
	}

	// Whoever threw or kicked it, at the spot it left his hands.
	promote(
		launcherPid,
		beat.launcherName,
		"passer",
		start,
		offenseT,
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
			// If the man carrying it was given a path, the ball travels it too.
			path: beat.carried ? carriedPath : undefined,
		},
		impact: beat.scored
			? { kind: "score", at: ballTo }
			: beat.kind === "run" ||
					beat.kind === "pass" ||
					beat.kind === "sack" ||
					beat.kind === "return"
				? { kind: "tackle", at: ballTo }
				: undefined,
		// A flag lands near where it happened, a few yards off the ball.
		flag:
			beat.kind === "penalty"
				? {
						x: clampX(losX + dir * rand(-4, 7)),
						y: clampY(across + rand(-9, 9)),
					}
				: undefined,
		driveMarks: [...ctx.driveMarks],
		drive: driveSummary(sportState),
		// The formation is known for every scrimmage scene, but a flag or an
		// injury has no CALL - and "Trips · undefined" is worse than showing
		// nothing at all.
		playName:
			playName === undefined
				? offFormation?.name
				: offFormation
					? `${offFormation.name} · ${playName}`
					: playName,
		defenseName,
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

// The plays that end with somebody being brought down, and so with a man who
// had to get there to do it.
const TACKLE_KINDS = new Set<FieldSceneKind>([
	"run",
	"pass",
	"sack",
	"return",
	"fumble",
]);

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
