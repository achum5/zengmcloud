import { courtRandom } from "./courtRng.ts";
import {
	clampY,
	dirAcross,
	type Dir,
	type FieldActor,
	type FieldPoint,
	type Slot,
	toField,
} from "./fieldSpots.ts";
import { SLOT_RB, SLOT_TE, SLOT_WR_L, SLOT_WR_R, SLOT_WR_SLOT } from "./playbook.ts";

// WHAT THE DEFENSE IS PLAYING.
//
// The field used to give every defender the same job forever: four men rushed,
// the corners and linebackers trailed whoever lined up in front of them, and
// the safeties backed up. That is one coverage, played on every snap of every
// game, and it is why the defense looked like scenery.
//
// A coverage is really a division of the field. Somebody has each deep piece of
// it and somebody has each underneath piece, or else everybody has a MAN and
// there is nobody left over. The difference between Cover 2 and Cover 3 is one
// safety, and it changes where nine other people stand. All of it is public
// football - drawn in every coaching clinic there has ever been - and it is
// written here from the game.
//
// Zone landmarks are given as (depth downfield from the line, across from the
// ball) so they mirror exactly the way formations and routes do.

export type CoverageJob =
	| { kind: "rush"; loop: boolean }
	// Mirror the quarterback, shallow, waiting for him to run.
	| { kind: "spy" }
	| { kind: "man"; slot: number }
	| { kind: "zone"; depth: number; across: number; label: string };

// What the receivers SEE, which is the only thing about a coverage an offense
// can react to: how many men are over the top, or whether there is anybody back
// there at all.
export type CoverageShell = "man" | "blitz" | "singleHigh" | "twoHigh";

export type Coverage = {
	name: string;
	shell: CoverageShell;
	// How many men come after the quarterback. Anything past the front four is
	// a blitz and takes a body out of the coverage behind it.
	rushers: number;
	// True if every eligible man has somebody on him and there is no help.
	man: boolean;
	build: (groups: Groups, offSlots: Slot[]) => Map<number, CoverageJob>;
};

// Defenders sorted into the groups a coverage talks about, each list ordered
// across the field so "the corner to the left" is always the same man.
export type Groups = {
	dl: number[];
	lb: number[];
	nb: number[];
	cb: number[];
	s: number[];
};

export const groupDefenders = (defSlots: Slot[]): Groups => {
	const groups: Groups = { dl: [], lb: [], nb: [], cb: [], s: [] };
	for (const [i, slot] of defSlots.entries()) {
		const list =
			slot.pos === "DL"
				? groups.dl
				: slot.pos === "LB"
					? groups.lb
					: slot.pos === "NB"
						? groups.nb
						: slot.pos === "CB"
							? groups.cb
							: groups.s;
		list.push(i);
	}
	for (const list of Object.values(groups)) {
		list.sort((a, b) => defSlots[a]!.across - defSlots[b]!.across);
	}
	return groups;
};

// The men who have to be covered when a coverage is man-to-man, in the order a
// defense would take them: outside first, then the slot, then the tight end,
// then the back.
const MAN_ORDER = [SLOT_WR_L, SLOT_WR_R, SLOT_WR_SLOT, SLOT_TE, SLOT_RB];

// Hand out man assignments in the order the defense would: corners on the men
// split outside, the nickel on the slot, linebackers on the tight end and the
// back. Whoever is left over is free to do something else.
const manUp = (groups: Groups): { jobs: Map<number, CoverageJob>; spare: number[] } => {
	const jobs = new Map<number, CoverageJob>();
	const cover = [
		...groups.cb,
		...groups.nb,
		...groups.lb,
		...groups.s,
	];
	const spare: number[] = [];
	const targets = [...MAN_ORDER];
	// Corners take the outside receivers, in the order they line up.
	const cbs = [...groups.cb];
	if (cbs.length >= 2) {
		jobs.set(cbs[0]!, { kind: "man", slot: SLOT_WR_L });
		jobs.set(cbs.at(-1)!, { kind: "man", slot: SLOT_WR_R });
		targets.splice(targets.indexOf(SLOT_WR_L), 1);
		targets.splice(targets.indexOf(SLOT_WR_R), 1);
	}
	for (const i of cover) {
		if (jobs.has(i)) {
			continue;
		}
		const slot = targets.shift();
		if (slot === undefined) {
			spare.push(i);
		} else {
			jobs.set(i, { kind: "man", slot });
		}
	}
	return { jobs, spare };
};

const rushAll = (indices: number[], defSlots?: Slot[]): Map<number, CoverageJob> => {
	const jobs = new Map<number, CoverageJob>();
	for (const i of indices) {
		// An edge rusher loops around the outside; an interior man goes straight
		// through. Without the distinction a four-man rush converges as one blob.
		const wide = Math.abs(defSlots?.[i]?.across ?? 0) > 4;
		jobs.set(i, { kind: "rush", loop: wide });
	}
	return jobs;
};

// COVER 1: everybody has a man and one safety has the whole field behind them.
// The extra defender either spies the quarterback or comes.
const COVER_1: Coverage = {
	name: "Cover 1",
	shell: "man",
	rushers: 4,
	man: true,
	build: (groups) => {
		const free = groups.s[0];
		const rest: Groups = { ...groups, s: groups.s.slice(1) };
		const { jobs, spare } = manUp(rest);
		if (free !== undefined) {
			jobs.set(free, {
				kind: "zone",
				depth: 15,
				across: 0,
				label: "deep middle",
			});
		}
		for (const i of spare) {
			jobs.set(i, { kind: "spy" });
		}
		for (const [i, job] of rushAll(groups.dl)) {
			jobs.set(i, job);
		}
		return jobs;
	},
};

// COVER 0: man across the board and nobody behind it, because everybody else is
// coming. The most dangerous thing a defense can do in both directions.
const COVER_0: Coverage = {
	name: "Cover 0 Blitz",
	shell: "blitz",
	rushers: 6,
	man: true,
	build: (groups) => {
		const { jobs, spare } = manUp(groups);
		// Whoever has nobody to cover is a rusher, and so is a safety left over.
		for (const i of spare) {
			jobs.set(i, { kind: "rush", loop: false });
		}
		for (const [i, job] of rushAll(groups.dl)) {
			jobs.set(i, job);
		}
		// One more, off the edge: this is the point of Cover 0.
		const extra = groups.lb.at(-1);
		if (extra !== undefined) {
			jobs.set(extra, { kind: "rush", loop: true });
		}
		return jobs;
	},
};

// COVER 2: two safeties split the deep field in half, the corners sink under
// them into the flats, and the linebackers have the middle.
const COVER_2: Coverage = {
	name: "Cover 2",
	shell: "twoHigh",
	rushers: 4,
	man: false,
	build: (groups) => {
		const jobs = rushAll(groups.dl);
		const [sLeft, sRight] = [groups.s[0], groups.s.at(-1)];
		if (sLeft !== undefined) {
			jobs.set(sLeft, { kind: "zone", depth: 16, across: -13, label: "deep half" });
		}
		if (sRight !== undefined && sRight !== sLeft) {
			jobs.set(sRight, { kind: "zone", depth: 16, across: 13, label: "deep half" });
		}
		const cbs = groups.cb;
		if (cbs[0] !== undefined) {
			jobs.set(cbs[0], { kind: "zone", depth: 6, across: -19, label: "flat" });
		}
		if (cbs.at(-1) !== undefined && cbs.at(-1) !== cbs[0]) {
			jobs.set(cbs.at(-1)!, { kind: "zone", depth: 6, across: 19, label: "flat" });
		}
		const under = [...groups.lb, ...groups.nb];
		const spots = [-8, 0, 8, -15, 15];
		for (const [n, i] of under.entries()) {
			jobs.set(i, {
				kind: "zone",
				depth: 10,
				across: spots[n] ?? 0,
				label: "hook",
			});
		}
		return jobs;
	},
};

// TAMPA 2: the same two-deep shell, except the middle linebacker runs up the
// pipe to take away the seam that Cover 2 leaves open.
const TAMPA_2: Coverage = {
	name: "Tampa 2",
	shell: "twoHigh",
	rushers: 4,
	man: false,
	build: (groups, offSlots) => {
		const jobs = COVER_2.build(groups, offSlots);
		const middle = groups.lb[Math.floor(groups.lb.length / 2)];
		if (middle !== undefined) {
			jobs.set(middle, {
				kind: "zone",
				depth: 19,
				across: 0,
				label: "deep middle",
			});
		}
		return jobs;
	},
};

// COVER 3: three deep, four under. One safety comes down into the box, which is
// why it is the coverage a defense plays when it wants to stop the run too.
const COVER_3: Coverage = {
	name: "Cover 3",
	shell: "singleHigh",
	rushers: 4,
	man: false,
	build: (groups) => {
		const jobs = rushAll(groups.dl);
		const cbs = groups.cb;
		if (cbs[0] !== undefined) {
			jobs.set(cbs[0], {
				kind: "zone",
				depth: 17,
				across: -17,
				label: "deep third",
			});
		}
		if (cbs.at(-1) !== undefined && cbs.at(-1) !== cbs[0]) {
			jobs.set(cbs.at(-1)!, {
				kind: "zone",
				depth: 17,
				across: 17,
				label: "deep third",
			});
		}
		const [deep, rolled] = [groups.s[0], groups.s.at(-1)];
		if (deep !== undefined) {
			jobs.set(deep, {
				kind: "zone",
				depth: 18,
				across: 0,
				label: "deep middle",
			});
		}
		if (rolled !== undefined && rolled !== deep) {
			jobs.set(rolled, {
				kind: "zone",
				depth: 7,
				across: 14,
				label: "curl/flat",
			});
		}
		const under = [...groups.lb, ...groups.nb];
		const spots = [-14, -5, 5, 13, 0];
		for (const [n, i] of under.entries()) {
			jobs.set(i, {
				kind: "zone",
				depth: 9,
				across: spots[n] ?? 0,
				label: "hook",
			});
		}
		return jobs;
	},
};

// QUARTERS: four defenders split the deep field into four, which is how a
// defense takes away everything down the field and dares you to run.
const COVER_4: Coverage = {
	name: "Quarters",
	shell: "twoHigh",
	rushers: 4,
	man: false,
	build: (groups) => {
		const jobs = rushAll(groups.dl);
		const cbs = groups.cb;
		if (cbs[0] !== undefined) {
			jobs.set(cbs[0], {
				kind: "zone",
				depth: 15,
				across: -19,
				label: "deep quarter",
			});
		}
		if (cbs.at(-1) !== undefined && cbs.at(-1) !== cbs[0]) {
			jobs.set(cbs.at(-1)!, {
				kind: "zone",
				depth: 15,
				across: 19,
				label: "deep quarter",
			});
		}
		const ss = groups.s;
		if (ss[0] !== undefined) {
			jobs.set(ss[0], {
				kind: "zone",
				depth: 15,
				across: -6,
				label: "deep quarter",
			});
		}
		if (ss.at(-1) !== undefined && ss.at(-1) !== ss[0]) {
			jobs.set(ss.at(-1)!, {
				kind: "zone",
				depth: 15,
				across: 6,
				label: "deep quarter",
			});
		}
		const under = [...groups.lb, ...groups.nb];
		const spots = [-11, 0, 11, -18, 18];
		for (const [n, i] of under.entries()) {
			jobs.set(i, {
				kind: "zone",
				depth: 7,
				across: spots[n] ?? 0,
				label: "hook",
			});
		}
		return jobs;
	},
};

// FIRE ZONE: five come, and the three deep and three under behind them play a
// zone anyway. Pressure without giving up the whole field, which is why every
// defense in football has some version of it.
const FIRE_ZONE: Coverage = {
	name: "Fire Zone",
	shell: "singleHigh",
	rushers: 5,
	man: false,
	build: (groups, offSlots) => {
		const jobs = COVER_3.build(groups, offSlots);
		// A linebacker comes; the man beside him widens to cover the space he
		// left, which is exactly the trade a fire zone makes.
		const blitzer = groups.lb.at(-1) ?? groups.nb.at(-1);
		if (blitzer !== undefined) {
			jobs.set(blitzer, { kind: "rush", loop: true });
		}
		return jobs;
	},
};

export const COVERAGES = {
	cover0: COVER_0,
	cover1: COVER_1,
	cover2: COVER_2,
	tampa2: TAMPA_2,
	cover3: COVER_3,
	cover4: COVER_4,
	fireZone: FIRE_ZONE,
} satisfies Record<string, Coverage>;

// WHAT THE DEFENSE CALLED. Down, distance and where the ball is decide most of
// it: a defense on its own goal line plays man because there is no field left
// behind it, and a defense on third and fifteen plays something with three or
// four men deep. A sack is evidence of pressure, so a play that ended in one is
// more likely to have been a blitz - which is honest, since it is the only
// thing about the defense the sim actually tells us.
export const chooseCoverage = ({
	down,
	toGo,
	scrimmage,
	sacked,
}: {
	down: number;
	toGo: number;
	// From the OFFENSE's own goal line, so 96 is the defense's four-yard line.
	scrimmage: number;
	sacked: boolean;
}): Coverage => {
	const r = courtRandom();
	if (sacked) {
		return r < 0.42 ? FIRE_ZONE : r < 0.68 ? COVER_0 : COVER_1;
	}
	// Backed up against their own goal line there is no deep to defend.
	if (scrimmage >= 95) {
		return r < 0.55 ? COVER_1 : COVER_0;
	}
	if (toGo <= 2) {
		return r < 0.45 ? COVER_1 : r < 0.75 ? COVER_3 : COVER_0;
	}
	if (down >= 3 && toGo >= 10) {
		return r < 0.3 ? COVER_4 : r < 0.55 ? TAMPA_2 : r < 0.8 ? COVER_3 : FIRE_ZONE;
	}
	if (down >= 3) {
		return r < 0.3 ? COVER_1 : r < 0.6 ? COVER_3 : r < 0.85 ? COVER_2 : FIRE_ZONE;
	}
	return r < 0.32 ? COVER_3 : r < 0.56 ? COVER_1 : r < 0.78 ? COVER_2 : COVER_4;
};

type Geom = { losX: number; dir: Dir; ballAcross: number };

// THE SHOW BEFORE THE SNAP.
//
// A defense that lines up in what it is about to play has told the quarterback
// everything. Real ones show the OPPOSITE and rotate as the ball moves: a
// two-deep look that spins to a single-high, or one safety down in the box who
// bails to a half at the snap.
//
// Only the safeties do it - they are the men whose position declares the
// coverage - and it costs nothing but the first waypoint of their path.
const disguiseSpot = (
	shell: CoverageShell,
	n: number,
	geom: Geom,
): FieldPoint => {
	const mirror = dirAcross(geom.dir);
	// Two men deep is disguised as one deep and one down; one deep is disguised
	// as two.
	const [depth, across] =
		shell === "twoHigh"
			? n === 0
				? [13, 0]
				: [7, -12]
			: n === 0
				? [12, -11]
				: [12, 11];
	return toField(geom.losX, geom.dir, -depth, geom.ballAcross + across * mirror);
};


// PUTTING THE COVERAGE ON THE GRASS.
//
// A rusher goes at the quarterback. A man defender goes where his receiver
// goes. A zone defender drops to his landmark and then - and this is the part
// that makes a zone look like a zone rather than seven men standing on spots -
// BREAKS ON THE BALL once it is thrown, if he is the nearest one to it.
export const assignCoverage = ({
	defenders,
	defSlots,
	receivers,
	coverage,
	target,
	geom,
	reachTarget,
	ballTo,
}: {
	defenders: FieldActor[];
	defSlots: Slot[];
	// The offense, already carrying their routes.
	receivers: FieldActor[];
	coverage: Coverage;
	// Where the quarterback set up.
	target: FieldPoint;
	geom: Geom;
	// True on a sack: the rush gets home instead of stopping short.
	reachTarget: boolean;
	// Where the ball ended up, once that is known. Undefined before the throw.
	ballTo: FieldPoint | undefined;
}): FieldActor[] => {
	const groups = groupDefenders(defSlots);
	const jobs = coverage.build(groups, []);

	// Which zone defender is closest to where the ball is going. Only one of
	// them breaks on it, because a zone where everybody converges is a man
	// coverage drawn badly.
	let closest: number | undefined;
	if (ballTo) {
		let best = Infinity;
		for (const actor of defenders) {
			const i = actor.slotIndex;
			if (i === undefined) {
				continue;
			}
			const job = jobs.get(i);
			if (job?.kind !== "zone") {
				continue;
			}
			const spot = toField(
				geom.losX,
				geom.dir,
				-job.depth,
				geom.ballAcross + job.across * dirAcross(geom.dir),
			);
			const d = Math.hypot(spot.x - ballTo.x, spot.y - ballTo.y);
			if (d < best) {
				best = d;
				closest = i;
			}
		}
	}

	// Which safety is which, so the two of them can show one thing and rotate to
	// another.
	const safetyOrder = new Map(groups.s.map((slot, n) => [slot, n]));

	return defenders.map((actor) => {
		const i = actor.slotIndex;
		if (i === undefined) {
			return actor;
		}
		const job = jobs.get(i);
		const lined = { x: actor.x, y: actor.y };
		// A safety begins the play where the defense wanted the quarterback to
		// think he was.
		const show = safetyOrder.has(i)
			? disguiseSpot(coverage.shell, safetyOrder.get(i)!, geom)
			: undefined;
		const start = show ?? lined;
		if (!job) {
			return actor;
		}

		if (job.kind === "rush") {
			const stop = reachTarget ? 0.4 : 2.2 + courtRandom() * 1.6;
			const dx = target.x - start.x;
			const dy = target.y - start.y;
			const dist = Math.max(0.1, Math.hypot(dx, dy));
			const f = Math.max(0, (dist - stop) / dist);
			const arc = job.loop ? 3 : 0.6;
			const end = { x: start.x + dx * f, y: clampY(start.y + dy * f) };
			return {
				...actor,
				x: end.x,
				y: end.y,
				job: "rush",
				path: [
					start,
					{
						x: start.x + dx * f * 0.5,
						y: clampY(start.y + dy * f * 0.5 + arc * (dy >= 0 ? -1 : 1)),
					},
					end,
				],
			};
		}

		if (job.kind === "spy") {
			// Mirror him, a few yards off, going nowhere until he does.
			const end = {
				x: start.x + (target.x - start.x) * 0.45,
				y: clampY(start.y + (target.y - start.y) * 0.5),
			};
			return {
				...actor,
				x: end.x,
				y: end.y,
				job: "spy",
				path: [start, end],
				delay: 0.15,
			};
		}

		if (job.kind === "man") {
			const man = receivers.find((r) => r.slotIndex === job.slot);
			if (!man?.path || man.path.length < 2) {
				return actor;
			}
			const inside = geom.ballAcross > man.y ? 1.4 : -1.4;
			const trail = man.path.slice(1).map((p) => ({
				x: p.x - geom.dir * 1.6,
				y: clampY(p.y + inside),
			}));
			const end = trail.at(-1)!;
			return {
				...actor,
				x: end.x,
				y: end.y,
				job: "man",
				path: [start, ...trail],
				delay: 0.04,
			};
		}

		// A zone: get to the landmark, then break on the ball if it is yours.
		const spot = toField(
			geom.losX,
			geom.dir,
			-job.depth,
			geom.ballAcross + job.across * dirAcross(geom.dir),
		);
		const path: FieldPoint[] = [
			start,
			{
				x: start.x + (spot.x - start.x) * 0.55,
				y: clampY(start.y + (spot.y - start.y) * 0.7),
			},
			spot,
		];
		if (ballTo && i === closest) {
			path.push({
				x: ballTo.x - geom.dir * 1.2,
				y: clampY(ballTo.y + (ballTo.y > spot.y ? -1.4 : 1.4)),
			});
		}
		const end = path.at(-1)!;
		return { ...actor, x: end.x, y: end.y, job: "zone", path };
	});
};

// A name for the corner of the screen: the front and the coverage, the way a
// broadcast would put it.
export const defenseLabel = (front: string, coverage: string): string =>
	front === coverage ? front : `${front} · ${coverage}`;
