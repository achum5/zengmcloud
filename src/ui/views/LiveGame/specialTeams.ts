import { courtRandom } from "./courtRng.ts";
import {
	bezierAt,
	clampY,
	dirAcross,
	type Dir,
	type FieldActor,
	type FieldPoint,
	runControlPoints,
	type Slot,
	toField,
} from "./fieldSpots.ts";

// SPECIAL TEAMS, WHICH ARE ALSO PLAYS.
//
// A punt was twenty-two men standing still while a ball went over their heads,
// and a kickoff was the same thing on a longer field. But these are the plays
// with the most movement in football: eleven men sprinting the length of the
// field in lanes, two gunners released a second before anybody else, a wedge
// forming in front of a returner, an edge rusher looping at a field goal.
//
// Every unit here gets the same treatment as a play from scrimmage - a job
// each, walked frame by frame - because that is what they are.

type Geom = { losX: number; dir: Dir; ballAcross: number };

export type SpecialTeamsKind = "punt" | "kick" | "kickoff" | "return";

// THE PUNT TEAM. The two gunners are gone at the snap - that is what a gunner
// is - and the rest of the line holds up the rush for a beat before releasing
// into its lanes. The punter steps into it and stays where he kicked from.
const PUNT_GUNNERS = new Set([7, 8]);
const PUNT_PUNTER = 10;

const puntCoverage = ({
	actors,
	slots,
	geom,
	landing,
}: {
	actors: FieldActor[];
	slots: Slot[];
	geom: Geom;
	landing: FieldPoint;
}): FieldActor[] => {
	const mirror = dirAcross(geom.dir);
	return actors.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined) {
			return actor;
		}
		const slot = slots[i];
		if (!slot) {
			return actor;
		}
		const from = { x: actor.x, y: actor.y };

		if (i === PUNT_PUNTER) {
			// Two steps into the kick, and that is his whole afternoon.
			const step = toField(
				geom.losX,
				geom.dir,
				slot.depth - 2.2,
				geom.ballAcross + slot.across * mirror,
			);
			return { ...actor, x: step.x, y: step.y, path: [from, step] };
		}

		if (PUNT_GUNNERS.has(i)) {
			// Straight down the sideline at the returner, and released at once.
			const at = {
				x: landing.x - geom.dir * 2,
				y: clampY(landing.y + (slot.across > 0 ? 5 : -5)),
			};
			return {
				...actor,
				x: at.x,
				y: at.y,
				path: [
					from,
					{ x: (from.x + at.x) / 2, y: clampY(from.y + (at.y - from.y) * 0.25) },
					at,
				],
			};
		}

		// The rest hold, then release into a lane. Every man covers a different
		// strip of the field, which is exactly what punt coverage is.
		const lane = slot.across * 0.7 + (i % 2 === 0 ? 2.5 : -2.5);
		const at = {
			x: landing.x - geom.dir * (9 + (i % 4) * 3),
			y: clampY(landing.y + lane),
		};
		return {
			...actor,
			x: at.x,
			y: at.y,
			path: [from, { x: geom.losX, y: clampY(from.y + lane * 0.3) }, at],
			delay: 0.2 + (i % 3) * 0.04,
		};
	});
};

// THE PUNT RETURN TEAM. Some of them rush it, the rest peel back and build
// something in front of the man catching it.
const puntReturnSetup = ({
	actors,
	slots,
	geom,
	launch,
	landing,
}: {
	actors: FieldActor[];
	slots: Slot[];
	geom: Geom;
	launch: FieldPoint;
	landing: FieldPoint;
}): FieldActor[] =>
	actors.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined) {
			return actor;
		}
		const slot = slots[i];
		if (!slot) {
			return actor;
		}
		const from = { x: actor.x, y: actor.y };

		// The deepest man is the returner: he drifts under the ball.
		if (slot.depth <= -40) {
			return {
				...actor,
				x: landing.x,
				y: landing.y,
				path: [from, landing],
			};
		}

		// The front four go after it - and mostly do not get there.
		if (slot.pos === "DL") {
			const dx = launch.x - from.x;
			const dy = launch.y - from.y;
			const f = 0.55 + courtRandom() * 0.2;
			const at = { x: from.x + dx * f, y: clampY(from.y + dy * f) };
			return { ...actor, x: at.x, y: at.y, path: [from, at] };
		}

		// Everybody else turns and sets up in front of the catch.
		const at = {
			x: landing.x + geom.dir * (6 + (i % 3) * 4),
			y: clampY(landing.y + (i % 2 === 0 ? 6 : -6) + (i % 3) * 2),
		};
		return {
			...actor,
			x: at.x,
			y: at.y,
			path: [from, at],
			delay: 0.08,
		};
	});

// A KICKOFF. Ten men leave at once and arrive in a WAVE - staggered, because
// they do not all run the same distance - against a return team that is
// backpedalling and building a wedge.
const kickoffCoverage = ({
	actors,
	slots,
	geom,
	landing,
}: {
	actors: FieldActor[];
	slots: Slot[];
	geom: Geom;
	landing: FieldPoint;
}): FieldActor[] => {
	const mirror = dirAcross(geom.dir);
	return actors.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined) {
			return actor;
		}
		const slot = slots[i];
		if (!slot) {
			return actor;
		}
		const from = { x: actor.x, y: actor.y };

		// The kicker takes his steps and watches it like everybody else.
		if (slot.pos === "K") {
			const step = toField(
				geom.losX,
				geom.dir,
				slot.depth - 5,
				geom.ballAcross + slot.across * mirror,
			);
			return { ...actor, x: step.x, y: step.y, path: [from, step] };
		}

		// Down the field in his lane, keeping his width - a cover team that
		// converges is a cover team that gets run through.
		const at = {
			x: landing.x - geom.dir * (7 + (i % 5) * 4),
			y: clampY(landing.y + slot.across * 0.55),
		};
		return {
			...actor,
			x: at.x,
			y: at.y,
			path: [
				from,
				{
					x: from.x + (at.x - from.x) * 0.5,
					y: clampY(from.y + (at.y - from.y) * 0.45),
				},
				at,
			],
			delay: 0.03 + (i % 5) * 0.03,
		};
	});
};

const kickoffReturnSetup = ({
	actors,
	slots,
	geom,
	landing,
}: {
	actors: FieldActor[];
	slots: Slot[];
	geom: Geom;
	landing: FieldPoint;
}): FieldActor[] =>
	actors.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined) {
			return actor;
		}
		const slot = slots[i];
		if (!slot) {
			return actor;
		}
		const from = { x: actor.x, y: actor.y };

		// The deep man catches it.
		if (slot.depth <= -50) {
			return { ...actor, x: landing.x, y: landing.y, path: [from, landing] };
		}

		// Everybody else turns, runs back, and forms up in front of him.
		const at = {
			x: landing.x + geom.dir * (10 + (i % 4) * 5),
			y: clampY(landing.y + slot.across * 0.45),
		};
		return {
			...actor,
			x: at.x,
			y: at.y,
			path: [from, at],
			delay: 0.04,
		};
	});

// A PLACE KICK. The line does not move much and is not supposed to; the whole
// play is the two men on the edge trying to get round it, and the eleven yards
// behind the ball where the holder and the kicker do their jobs.
const kickProtection = ({
	actors,
	slots,
	geom,
}: {
	actors: FieldActor[];
	slots: Slot[];
	geom: Geom;
}): FieldActor[] => {
	const mirror = dirAcross(geom.dir);
	return actors.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined) {
			return actor;
		}
		const slot = slots[i];
		if (!slot) {
			return actor;
		}
		const from = { x: actor.x, y: actor.y };
		// The kicker steps in and swings through it.
		if (slot.pos === "K") {
			const step = toField(
				geom.losX,
				geom.dir,
				slot.depth - 2.4,
				geom.ballAcross + (slot.across + 2.2) * mirror,
			);
			return { ...actor, x: step.x, y: step.y, path: [from, step] };
		}
		if (slot.pos === "RB") {
			// The holder: down, and still.
			return actor;
		}
		const set = toField(
			geom.losX,
			geom.dir,
			slot.depth + 0.8,
			geom.ballAcross + slot.across * 1.05 * mirror,
		);
		return { ...actor, x: set.x, y: set.y, path: [from, set], delay: 0.02 };
	});
};

const kickRush = ({
	actors,
	slots,
	geom,
	launch,
}: {
	actors: FieldActor[];
	slots: Slot[];
	geom: Geom;
	launch: FieldPoint;
}): FieldActor[] =>
	actors.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined) {
			return actor;
		}
		const slot = slots[i];
		if (!slot) {
			return actor;
		}
		const from = { x: actor.x, y: actor.y };
		const edge = Math.abs(slot.across) > 9;
		if (!edge && slot.pos !== "DL") {
			// The back end of a block unit does very little, which is honest.
			const drift = {
				x: from.x + geom.dir * 1.5,
				y: clampY(from.y + (slot.across > 0 ? -1.5 : 1.5)),
			};
			return { ...actor, x: drift.x, y: drift.y, path: [from, drift] };
		}
		// Round the corner and up at the ball, getting a hand near it if he can.
		const f = edge ? 0.78 : 0.4;
		const at = {
			x: from.x + (launch.x - from.x) * f,
			y: clampY(from.y + (launch.y - from.y) * f),
		};
		return {
			...actor,
			x: at.x,
			y: at.y,
			path: [
				from,
				{
					x: from.x + (at.x - from.x) * 0.45,
					y: clampY(from.y + (at.y - from.y) * 0.2 + (edge ? -2.5 : 0) * (slot.across > 0 ? -1 : 1)),
				},
				at,
			],
		};
	});

// COVERING AND BLOCKING A RETURN THAT IS ALREADY UNDERWAY. The chasers converge
// on the man with the ball; his blockers get in front of him. Both sides work
// off the path he is actually running, which is what makes a return read as a
// convoy rather than as a man alone on an empty field.
const returnConvoy = ({
	blockers,
	chasers,
	carrier,
	geom,
}: {
	blockers: FieldActor[];
	chasers: FieldActor[];
	carrier: FieldPoint[];
	geom: Geom;
}): { blockers: FieldActor[]; chasers: FieldActor[] } => {
	const ahead = carrier.at(-1) ?? { x: 0, y: 0 };
	return {
		blockers: blockers.map((actor, n) => {
			const from = { x: actor.x, y: actor.y };
			// A few yards in front of him and to one side, so he has somewhere to
			// run.
			const at = {
				x: ahead.x + geom.dir * (3 + (n % 4) * 3.5),
				y: clampY(ahead.y + ((n % 2 === 0 ? 1 : -1) * (3 + (n % 3) * 3))),
			};
			return { ...actor, x: at.x, y: at.y, path: [from, at], delay: 0.03 };
		}),
		chasers: chasers.map((actor, n) => {
			const from = { x: actor.x, y: actor.y };
			const closes = 0.62 + (n % 4) * 0.1;
			const at = {
				x: from.x + (ahead.x - from.x) * closes,
				y: clampY(from.y + (ahead.y - from.y) * closes + ((n % 3) - 1) * 2),
			};
			return {
				...actor,
				x: at.x,
				y: at.y,
				path: [
					from,
					{
						x: from.x + (at.x - from.x) * 0.45,
						y: clampY(from.y + (at.y - from.y) * 0.5),
					},
					at,
				],
				delay: 0.04 + (n % 3) * 0.05,
			};
		}),
	};
};

// A carrier's weaving path, sampled into waypoints so his body travels the same
// curve the ball does.
export const carrierPath = (
	from: FieldPoint,
	to: FieldPoint,
	steps = 6,
): FieldPoint[] => {
	const [c1, c2] = runControlPoints(from, to);
	return Array.from({ length: steps + 1 }, (_, i) =>
		bezierAt(from, c1, c2, to, i / steps),
	);
};

// The one entry point the scene builder uses: hand it the two units and what
// the ball did, get back both sides with a job each.
export const assignSpecialTeams = ({
	kind,
	kicking,
	kickingSlots,
	receiving,
	receivingSlots,
	geom,
	launch,
	landing,
	carrier,
}: {
	kind: SpecialTeamsKind;
	// The unit that has the ball: the punt team, the kick team, the kickoff
	// team, or - on a return - the team running it back.
	kicking: FieldActor[];
	kickingSlots: Slot[];
	receiving: FieldActor[];
	receivingSlots: Slot[];
	geom: Geom;
	// Where the ball left from and where it came down.
	launch: FieldPoint;
	landing: FieldPoint;
	// The path the returner is running, when there is one.
	carrier?: FieldPoint[];
}): { kicking: FieldActor[]; receiving: FieldActor[] } => {
	switch (kind) {
		case "punt":
			return {
				kicking: puntCoverage({
					actors: kicking,
					slots: kickingSlots,
					geom,
					landing,
				}),
				receiving: puntReturnSetup({
					actors: receiving,
					slots: receivingSlots,
					geom,
					launch,
					landing,
				}),
			};
		case "kickoff":
			return {
				kicking: kickoffCoverage({
					actors: kicking,
					slots: kickingSlots,
					geom,
					landing,
				}),
				receiving: kickoffReturnSetup({
					actors: receiving,
					slots: receivingSlots,
					geom,
					landing,
				}),
			};
		case "kick":
			return {
				kicking: kickProtection({
					actors: kicking,
					slots: kickingSlots,
					geom,
				}),
				receiving: kickRush({
					actors: receiving,
					slots: receivingSlots,
					geom,
					launch,
				}),
			};
		case "return": {
			const convoy = returnConvoy({
				blockers: kicking,
				chasers: receiving,
				carrier: carrier ?? [launch, landing],
				geom,
			});
			return { kicking: convoy.blockers, receiving: convoy.chasers };
		}
	}
};

// A LOOSE BALL ON THE GROUND.
//
// A fumble was a ball squirting away from twenty-two men who carried on with
// whatever they had been doing. It is the one moment in football where every
// assignment on the field is cancelled at once and everybody near it does the
// same thing.
export const assignScramble = ({
	actors,
	ball,
	// How many of the nearest men actually get there. Everybody converging turns
	// a fumble into a magnet.
	count = 6,
}: {
	actors: FieldActor[];
	ball: FieldPoint;
	count?: number;
}): FieldActor[] => {
	const near = actors
		.map((a, i) => ({ i, d: Math.hypot(a.x - ball.x, a.y - ball.y) }))
		.sort((a, b) => a.d - b.d)
		.slice(0, count);
	const chosen = new Map(near.map(({ i, d }) => [i, d]));
	return actors.map((actor, i) => {
		const d = chosen.get(i);
		if (d === undefined) {
			return actor;
		}
		const from = actor.path?.[0] ?? { x: actor.x, y: actor.y };
		// The nearest man gets on it; the rest pile in around him.
		const ring = 0.4 + (i % 5) * 0.5;
		const angle = (i % 8) * (Math.PI / 4);
		const at = {
			x: ball.x + Math.cos(angle) * ring,
			y: clampY(ball.y + Math.sin(angle) * ring),
		};
		return {
			...actor,
			x: at.x,
			y: at.y,
			path: [
				from,
				{
					x: from.x + (at.x - from.x) * 0.5,
					y: clampY(from.y + (at.y - from.y) * 0.55),
				},
				at,
			],
			delay: 0.08 + Math.min(0.25, d * 0.012),
		};
	});
};

// PLAY IS STOPPED. A timeout, the end of a quarter, the final whistle - and
// twenty-two men left standing in the formation they happened to be in, which
// is the one thing on a football field that never happens. They break into
// their own huddles, which is both what really occurs and a clear visual full
// stop between one play and the next.
export const assignHuddle = ({
	actors,
	geom,
	// How far behind the ball the huddle forms, and which side of the field.
	depth,
	across,
}: {
	actors: FieldActor[];
	geom: Geom;
	depth: number;
	across: number;
}): FieldActor[] => {
	const centre = toField(
		geom.losX,
		geom.dir,
		depth,
		geom.ballAcross + across * dirAcross(geom.dir),
	);
	const n = Math.max(1, actors.length);
	return actors.map((actor, i) => {
		const angle = (i / n) * Math.PI * 2;
		const at = {
			x: centre.x + Math.cos(angle) * 3.2,
			y: clampY(centre.y + Math.sin(angle) * 2.6),
		};
		return {
			...actor,
			x: at.x,
			y: at.y,
			path: [{ x: actor.x, y: actor.y }, at],
			delay: 0.02 + (i % 4) * 0.03,
		};
	});
};
