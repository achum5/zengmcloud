// HOW MANY WAYS ONE KIND OF PLAY PLAYS OUT ON THE 3D FIELD - football's
// version of tools/court3d-tape/variety.test.ts.
//
// Every time the line comes up in the given games, the parts the 3D field
// put it together from - for a run: the formation, the exchange, where he
// hit the line and how he got there, what he did to get by men, how it
// ended and how many brought him down - and how many different
// combinations that comes to, overall and for each player.
//
//   VARIETY_GAMES=/tmp/games/real_7.json,... VARIETY_LINE=run \
//   VARIETY_OUT=/tmp/variety_run.txt \
//     npx vitest run --project football tools/field3d-tape/variety.football.test.ts
//
// Lines: run (a carry from scrimmage), catch (a completed pass), incomplete
// (a pass not caught), punt, kickoff, flag, sack, kick (a field goal or
// a try), pick (an interception), back (the ball tossed down at the end of
// a play, and given back to an official).
// Games: files from realGames.football.test.ts. VARIETY_BY=throw,catch adds
// how those parts go together, just them.

import { readFileSync, writeFileSync } from "node:fs";
import process from "node:process";
import { test } from "vitest";
import {
	compileField,
	type FieldTimeline,
	type Leg,
	type Part,
	type Play,
} from "../../src/ui/views/LiveGame/field3d/director.ts";
import {
	officialId,
	officialOf,
} from "../../src/ui/views/LiveGame/field3d/crew.ts";
import { momentAt } from "../../src/ui/views/LiveGame/field3d/evaluate.ts";
import { moveKind as moveKindOf } from "../../src/ui/views/LiveGame/field3d/poses.ts";
import {
	END_ZONE,
	FIELD_L,
	FIELD_W,
} from "../../src/ui/views/LiveGame/field3d/geometry.ts";

const games = (process.env.VARIETY_GAMES ?? "").split(",").filter(Boolean);
const line = process.env.VARIETY_LINE ?? "run";

type Pt = { x: number; y: number };
const dist = (a: Pt, b: Pt) => Math.hypot(a.x - b.x, a.y - b.y);
const EVADES = new Set(["juke", "spin", "hurdle", "stiffArm"]);
// (Bringing him down, or in on it: a defender's acts at the end.)
const IN_ON_IT = new Set([
	"dive",
	"pileIn",
	"pileDive",
	"pileTop",
	"shove",
	"drive",
	"tackled",
]);

const posOf = (tl: FieldTimeline, pid: number, t: number): Pt | undefined =>
	momentAt(tl, t).players.find((st) => st.id === pid);

// Who had the ball from t on: each holder, and how it got to him.
const holders = (p: Play) => {
	const out: { pid: number; t0: number; by: "hand" | "air" }[] = [];
	let air = false;
	for (const s of p.ball) {
		if (s.kind === "fly") {
			air = true;
		} else if (s.kind === "held") {
			if (out.at(-1)?.pid !== s.pid) {
				out.push({ pid: s.pid, t0: s.t0, by: air ? "air" : "hand" });
			}
			air = false;
		}
	}
	return out;
};

// How far across from the ball he is, feet to the offense's right (as
// formations.ts has it).
const acrossOf = (p: Play, at: Pt) => (at.y - p.ballY) * p.dir;

const bucket = (v: number, edges: number[], names: string[]) => {
	const k = edges.findIndex((e) => v < e);
	return names[k < 0 ? names.length - 1 : k]!;
};

// The carrier's way to the end of it and how it ended.
const endParts = (
	tl: FieldTimeline,
	p: Play,
	carrier: Part,
	from: number,
): Record<string, string> => {
	const legs = carrier.legs.filter(
		(l) => l.t1 > from && l.t0 <= p.whistle + 1500,
	);
	const evades = [
		...new Set([
			...legs.filter((l) => EVADES.has(l.move)).map((l) => l.move),
			...legs.filter((l) => l.arms === "stiffArm").map(() => "stiffArm"),
		]),
	];
	const at = posOf(tl, carrier.pid, p.whistle) ?? { x: p.losX, y: p.ballY };
	const goal = p.dir > 0 ? FIELD_L - END_ZONE : END_ZONE;
	const has = (m: string) => legs.some((l) => l.move === m && l.t0 >= from);
	const swung = legs.some((l: Leg) => l.move === "tackled" && l.turn);
	const end =
		(at.x - goal) * p.dir > 0
			? "into the end zone"
			: at.y < 0 || at.y > FIELD_W
				? "out of bounds"
				: has("qbSlide") || has("slid")
					? "slid"
					: has("tackledBack")
						? "driven back"
						: swung
							? "swung down"
							: has("tackled")
								? "tackled"
								: has("dive")
									? "dove for it"
									: has("drive")
										? "stood up"
										: `other (${legs.findLast((l) => l.t0 <= p.whistle)?.move ?? "-"})`;
	// Men of the other side in on it at the end: their acts there, near him.
	let tacklers = 0;
	for (const q of p.parts) {
		if (q.team === carrier.team) {
			continue;
		}
		const near = q.legs.some(
			(l) =>
				IN_ON_IT.has(l.move) &&
				l.t1 > p.whistle - 1500 &&
				l.t0 < p.whistle + 600 &&
				dist(l.to, at) < 6,
		);
		tacklers += near ? 1 : 0;
	}
	const gain = ((at.x - p.losX) * p.dir) / 3;
	return {
		evades: evades.length ? evades.sort().join("+") : "none",
		end,
		tacklers: tacklers >= 3 ? "3+" : String(tacklers),
		gain: bucket(
			gain,
			[0, 4, 10, 20],
			["loss", "1-3 yds", "4-9 yds", "10-19 yds", "20+ yds"],
		),
	};
};

const runParts = (tl: FieldTimeline, p: Play) => {
	const hs = holders(p).filter((h) => h.t0 >= p.snap - 50);
	const qb = p.parts.find((q) => q.role === "QB");
	const last = hs.at(-1);
	if (!last) {
		return undefined;
	}
	const carrier = p.parts.find((q) => q.pid === last.pid);
	if (!carrier) {
		return undefined;
	}
	const qbSet = qb?.legs.find((l) => l.t0 <= p.snap && l.t1 > p.snap - 1)?.move;
	const toQb = hs.findIndex((h) => h.pid === qb?.pid);
	const exchange =
		carrier === qb
			? "QB keeper"
			: toQb < 0
				? "direct snap"
				: last.by === "air"
					? // (On the option: him down the line with it first.)
						qb &&
						dist(
							posOf(tl, qb.pid, p.snap) ?? { x: 0, y: 0 },
							posOf(tl, qb.pid, last.t0 - 300) ?? { x: 0, y: 0 },
						) > 6
						? // (And the end man into him after it.)
							qb.legs.some(
								(l) =>
									(l.move === "tackled" || l.move === "tackledBack") &&
									l.t0 > last.t0 - 300 &&
									l.t0 < last.t0 + 1500,
							)
							? "option pitch, QB hit"
							: "option pitch"
						: "pitch"
					: "handoff";
	// Where he hit the line: where he first got across it, and the way he
	// first went after he had it.
	let crossed: Pt | undefined;
	let first = 0;
	for (let t = last.t0; t < p.whistle; t += 50) {
		const a = posOf(tl, carrier.pid, t);
		if (!a) {
			continue;
		}
		if (t - last.t0 >= 250 && first === 0) {
			const a0 = posOf(tl, carrier.pid, last.t0)!;
			first = Math.sign(acrossOf(p, a) - acrossOf(p, a0));
		}
		if ((a.x - p.losX) * p.dir > 0) {
			crossed = a;
			break;
		}
	}
	const across = crossed ? acrossOf(p, crossed) : undefined;
	const hole =
		across === undefined
			? "behind the line"
			: `${bucket(Math.abs(across), [6, 14, 28], ["inside", "off tackle", "outside", "wide"])} ${across > 0 ? "right" : "left"}`;
	const cut =
		across === undefined || first === 0
			? "-"
			: Math.sign(across) !== first && Math.abs(across) > 3
				? "cutback"
				: "on his way";
	return {
		pid: carrier.pid,
		parts: {
			formation: `${p.formation ?? "-"} ${qbSet ?? ""}`.trim(),
			exchange,
			hole,
			cut,
			...endParts(tl, p, carrier, last.t0),
		},
	};
};

const catchParts = (tl: FieldTimeline, p: Play) => {
	const qb = p.parts.find((q) => q.role === "QB");
	const throwSeg = p.ball.find(
		(s) =>
			s.kind === "fly" &&
			s.t0 > p.snap + 300 &&
			qb !== undefined &&
			dist(s.from, posOf(tl, qb.pid, s.t0) ?? s.from) < 4,
	);
	if (!throwSeg || throwSeg.kind !== "fly") {
		return undefined;
	}
	const got = holders(p).find((h) => h.t0 >= throwSeg.t1 - 50);
	const rec = got && p.parts.find((q) => q.pid === got.pid);
	if (!rec || rec.team !== qb!.team) {
		return undefined;
	}
	const at = posOf(tl, rec.pid, got!.t0) ?? throwSeg.to;
	const air = ((at.x - p.losX) * p.dir) / 3;
	const across = acrossOf(p, at);
	const before = posOf(tl, rec.pid, got!.t0 - 200) ?? at;
	const pace = dist(before, at) / 0.2;
	const how =
		rec.legs.find(
			(l) =>
				(l.move === "leapCatch" ||
					l.move === "diveCatch" ||
					l.move === "toeTap") &&
				l.t1 > got!.t0 - 300 &&
				l.t0 < got!.t0 + 300,
		)?.move ?? "catch";
	let near = Infinity;
	for (const q of p.parts) {
		if (q.team !== rec.team) {
			const d = posOf(tl, q.pid, got!.t0);
			if (d) {
				near = Math.min(near, dist(d, at));
			}
		}
	}
	// (A hand in at it as it got there, from a man on him.)
	const handIn = p.parts.some(
		(q) =>
			q.team !== rec.team &&
			q.legs.some(
				(l) =>
					(l.arms === "playBall" || l.arms === "playBallL") &&
					l.t0 < got!.t0 + 300 &&
					l.t1 > got!.t0 - 300,
			),
	);
	// (Up for it with him: off his feet at it as it got there.)
	const upWith = p.parts.some(
		(q) =>
			q.team !== rec.team &&
			q.legs.some((l) => l.move === "leap" && l.t0 < got!.t0 && l.t1 > got!.t0),
	);
	const drop = qb ? ((p.losX - throwSeg.from.x) * p.dir) / 3 : 0;
	return {
		pid: rec.pid,
		parts: {
			formation: p.formation ?? "-",
			drop: bucket(drop, [3, 5.5, 8], ["quick", "3-5 yds", "6-8 yds", "deep"]),
			throw: `${bucket(air, [0, 5, 12, 25], ["behind the line", "short", "middle", "deep", "bomb"])} ${bucket(across, [-12, 12], ["left", "middle", "right"])}`,
			arc: bucket(
				throwSeg.peak,
				[4, 9, 16],
				["line drive", "on a line", "touch", "lob"],
			),
			catch: `${how}${pace > 10 ? " in stride" : pace > 4 ? " on the move" : " sat down"}`,
			defense:
				bucket(near, [3, 6, 12], ["contested", "tight", "a step", "open"]) +
				(upWith ? ", up with him" : handIn ? ", a hand in" : ""),
			...endParts(tl, p, rec, got!.t0),
		},
	};
};

// A pass not caught: where it went, how it got away (knocked away, off his
// hands, dove for and missed, overthrown...), how near him it was, and what
// came after.
const incParts = (tl: FieldTimeline, p: Play) => {
	const qb = p.parts.find((q) => q.role === "QB");
	const throwSeg = p.ball.find(
		(s) =>
			s.kind === "fly" &&
			s.t0 > p.snap + 300 &&
			qb !== undefined &&
			dist(s.from, posOf(tl, qb.pid, s.t0) ?? s.from) < 4,
	);
	if (!throwSeg || throwSeg.kind !== "fly" || !qb) {
		return undefined;
	}
	const t = throwSeg.t1;
	const to = throwSeg.to;
	// (Knocked down at the line: a man of theirs up off both feet under
	// where it got to.)
	const batter = p.parts.find(
		(q) =>
			q.team !== qb.team &&
			q.legs.some((l) => l.move === "leap" && l.t0 < t && l.t1 > t) &&
			dist(posOf(tl, q.pid, t) ?? { x: 1e9, y: 0 }, to) < 2.5,
	);
	if (batter) {
		const waved = batter.legs.some((l) => l.move === "sigWave" && l.t0 > t);
		return {
			pid: batter.pid,
			parts: {
				formation: p.formation ?? "-",
				throw: `${bucket(((to.x - p.losX) * p.dir) / 3, [-6, -3, 0], ["deep in the backfield", "in the backfield", "at the line", "past the line"])} ${bucket(acrossOf(p, to), [-12, 12], ["left", "middle", "right"])}`,
				arc: "-",
				how: `batted at the line by the ${batter.role}`,
				height: bucket(
					to.z,
					[8, 9],
					["arms up", "reaching", "up off his feet"],
				),
				missedBy: "-",
				him: batter.legs.some((l) => l.move === "shuffle" && l.t1 <= t)
					? "stepped into the lane"
					: "where he was",
				defense: "-",
				after: waved ? "waved off" : "-",
			},
		};
	}
	let rec: Part | undefined;
	let off = Infinity;
	for (const q of p.parts) {
		const a = q.team === qb.team && q !== qb ? posOf(tl, q.pid, t) : undefined;
		if (a && dist(a, to) < off) {
			off = dist(a, to);
			rec = q;
		}
	}
	if (!rec) {
		return undefined;
	}
	const at = posOf(tl, rec.pid, t)!;
	const air = ((to.x - p.losX) * p.dir) / 3;
	const across = acrossOf(p, to);
	const did = (q: Part, moves: string[], t0: number, t1: number) =>
		q.legs.some(
			(l) =>
				(moves.includes(l.move) || moves.includes(l.arms ?? "")) &&
				l.t1 > t0 &&
				l.t0 < t1,
		);
	const other = p.parts.filter((q) => q.team !== qb.team);
	let near = Infinity;
	for (const q of other) {
		const d = posOf(tl, q.pid, t);
		if (d) {
			near = Math.min(near, dist(d, at));
		}
	}
	const how = other.some((q) => did(q, ["swat"], t - 400, t + 300))
		? "knocked away"
		: did(rec, ["dive"], t - 400, t + 900)
			? "dove for it"
			: to.z < 0.5 && off > 8
				? "thrown away"
				: to.z < 0.5
					? "short, in the turf"
					: did(rec, ["reachUp"], t - 500, t + 300)
						? "over his hands"
						: did(rec, ["catch"], t - 500, t + 300)
							? "off his hands"
							: "-";
	const after = did(rec, ["handsOnHelmet"], t, t + 4000)
		? "hands on helmet"
		: did(rec, ["protest"], t, t + 4000)
			? "palms up"
			: other.some((q) => did(q, ["sigWave"], t, t + 4000))
				? "waved off"
				: "-";
	const before = posOf(tl, rec.pid, t - 200) ?? at;
	const pace = dist(before, at) / 0.2;
	return {
		pid: rec.pid,
		parts: {
			formation: p.formation ?? "-",
			throw: `${bucket(air, [0, 5, 12, 25], ["behind the line", "short", "middle", "deep", "bomb"])} ${bucket(across, [-12, 12], ["left", "middle", "right"])}`,
			arc: bucket(
				throwSeg.peak,
				[4, 9, 16],
				["line drive", "on a line", "touch", "lob"],
			),
			how,
			height: bucket(
				to.z,
				[2, 7],
				["at his feet", "at his hands", "over his head"],
			),
			missedBy: bucket(
				off,
				[2, 5, 10],
				["at him", "a step off", "well off", "nowhere near"],
			),
			him: pace > 10 ? "in stride" : pace > 4 ? "on the move" : "sat down",
			defense: bucket(
				near,
				[3, 6, 12],
				["contested", "tight", "a step", "open"],
			),
			after,
		},
	};
};

// A return run one way across the field and back the other: out 15 feet
// or more from where he had it, and on from there 30 feet or more the other
// way by the whistle.
const reversed = (tl: FieldTimeline, p: Play, ret: Part, from: number) => {
	const y0 = posOf(tl, ret.pid, from)?.y;
	const y1 = posOf(tl, ret.pid, p.whistle)?.y;
	if (y0 === undefined || y1 === undefined) {
		return false;
	}
	for (let t = from; t < p.whistle; t += 100) {
		const y = posOf(tl, ret.pid, t)?.y;
		if (y !== undefined) {
			for (const s of [1, -1]) {
				if ((y - y0) * s >= 15 && (y - y1) * s >= 30) {
					return true;
				}
			}
		}
	}
	return false;
};

// A punt: how far and how high it went and where it came down, how it
// was fielded (caught - and how he was under it -, a fair catch, let
// bounce and downed, into the end zone), and the return.
const puntParts = (tl: FieldTimeline, p: Play) => {
	const kick = p.ball
		.filter((s) => s.kind === "fly" && s.t0 > p.snap)
		.sort(
			(a, b) =>
				(b.kind === "fly" ? b.peak : 0) - (a.kind === "fly" ? a.peak : 0),
		)[0];
	if (!kick || kick.kind !== "fly" || kick.peak < 15) {
		return undefined;
	}
	const kicking = p.offense;
	const pr = holders(p).find((h) => h.t0 >= kick.t1 - 50);
	const ret = pr && p.parts.find((q) => q.pid === pr.pid);
	const yds = ((kick.to.x - p.losX) * p.dir) / 3;
	const hang = (kick.t1 - kick.t0) / 1000;
	const edge = Math.min(kick.to.y, FIELD_W - kick.to.y);
	const did = (q: Part, moves: string[], t0: number, t1: number) =>
		q.legs.some(
			(l) =>
				(moves.includes(l.move) || moves.includes(l.arms ?? "")) &&
				l.t1 > t0 &&
				l.t0 < t1,
		);
	const end = p.ball.at(-1);
	const endX = end?.kind === "spot" ? end.at.x : kick.to.x;
	const goal = p.dir > 0 ? FIELD_L - END_ZONE : END_ZONE;
	let fielded = "-";
	let him = "-";
	let rest: Record<string, string> = {
		evades: "-",
		end: "-",
		tacklers: "-",
		gain: "-",
	};
	if (ret && ret.team !== kicking) {
		const at = posOf(tl, ret.pid, pr!.t0) ?? kick.to;
		// (Off the first touch of it: juggled, the held comes later.)
		const touch = kick.t1;
		const at0 = posOf(tl, ret.pid, touch) ?? at;
		const before = posOf(tl, ret.pid, touch - 150) ?? at0;
		const pace = dist(before, at0) / 0.15;
		const back = ret.legs.some(
			(l) => l.move === "back" && l.t0 < touch && l.t1 > touch - 200,
		);
		him = back
			? "going back"
			: pace > 12
				? "on the run, coming up"
				: pace > 7
					? "on the move"
					: did(ret, ["basket"], touch - 1500, touch + 300)
						? "set under it, in a basket"
						: "set under it";
		const juggled = p.ball.some(
			(s) => s.kind === "fly" && s.t0 >= kick.t1 - 5 && s.t1 <= pr!.t0 + 5,
		);
		fielded = did(ret, ["fairCatch"], kick.t1 - 3000, kick.t1)
			? "fair catch"
			: juggled
				? "caught, juggled"
				: "caught";
		if (fielded !== "fair catch") {
			rest = endParts(tl, p, ret, pr!.t0);
			if (reversed(tl, p, ret, pr!.t0)) {
				rest.evades =
					rest.evades === "none"
						? "field reversed"
						: `field reversed+${rest.evades}`;
			}
			// (The return's own yards, from where he had it.)
			const end = posOf(tl, ret.pid, p.whistle) ?? at;
			rest.gain = bucket(
				((at.x - end.x) * p.dir) / 3,
				[1, 5, 10, 20],
				["none", "1-4 yds", "5-9 yds", "10-19 yds", "20+ yds"],
			);
		}
	} else if (
		p.parts.some((q) => did(q, ["spotBall"], kick.t1, p.whistle + 500))
	) {
		fielded = "let bounce, downed";
	} else if ((endX - goal) * p.dir > 0) {
		fielded = "into the end zone";
	}
	return {
		pid: p.parts.find((q) => did(q, ["punt"], p.snap, kick.t1))?.pid ?? 0,
		parts: {
			kick: bucket(
				yds,
				[35, 45, 55],
				["under 35 yds", "35-44 yds", "45-54 yds", "55+ yds"],
			),
			hang: bucket(hang, [3.8, 4.4], ["low", "4 s", "high"]),
			where: `${bucket(edge, [20, 50], ["by the sideline", "outside the hashes", "middle"])}${kick.spiral ? "" : ", end over end"}`,
			fielded,
			him,
			...rest,
		},
	};
};

// A kickoff: where it came down (out the back, in the end zone, at the
// goal line, short of it) and across, how high, how it was fielded (out
// the back, caught and knelt, on the hop, caught) and how he was under
// it, and the return.
const kickParts = (tl: FieldTimeline, p: Play) => {
	const kick = p.ball
		.filter((s) => s.kind === "fly" && s.t0 >= p.snap - 50)
		.sort(
			(a, b) =>
				(b.kind === "fly" ? b.peak : 0) - (a.kind === "fly" ? a.peak : 0),
		)[0];
	if (!kick || kick.kind !== "fly") {
		return undefined;
	}
	const kicking = p.offense;
	const kr0 = holders(p).find((h) => h.t0 >= kick.t1 - 50);
	const ret = kr0 && p.parts.find((q) => q.pid === kr0.pid);
	const goal = p.dir > 0 ? FIELD_L - END_ZONE : END_ZONE;
	// (Yards out from the goal line it came down; in the end zone, less
	// than nothing.)
	const out = ((goal - kick.to.x) * p.dir) / 3;
	const hang = (kick.t1 - kick.t0) / 1000;
	const edge = Math.min(kick.to.y, FIELD_W - kick.to.y);
	const did = (q: Part, moves: string[], t0: number, t1: number) =>
		q.legs.some(
			(l) =>
				(moves.includes(l.move) || moves.includes(l.arms ?? "")) &&
				l.t1 > t0 &&
				l.t0 < t1,
		);
	let fielded = "let go, touchback";
	let him = "-";
	let rest: Record<string, string> = {
		evades: "-",
		end: "-",
		tacklers: "-",
		gain: "-",
	};
	if (ret && ret.team !== kicking) {
		const at = posOf(tl, ret.pid, kr0!.t0) ?? kick.to;
		const touch = kick.t1;
		const at0 = posOf(tl, ret.pid, touch) ?? at;
		const before = posOf(tl, ret.pid, touch - 150) ?? at0;
		const pace = dist(before, at0) / 0.15;
		const back = ret.legs.some(
			(l) => l.move === "back" && l.t0 < touch && l.t1 > touch - 200,
		);
		him = back
			? "going back"
			: pace > 12
				? "on the run, coming up"
				: pace > 7
					? "on the move"
					: did(ret, ["basket"], touch - 1500, touch + 300)
						? "set under it, in a basket"
						: "set under it";
		const flies = p.ball.filter(
			(s) => s.kind === "fly" && s.t0 >= kick.t1 - 5 && s.t1 <= kr0!.t0 + 5,
		);
		const knelt = did(ret, ["kneelDown"], kr0!.t0, p.whistle + 2500);
		fielded = knelt
			? "caught, knelt"
			: flies.some((s) => s.kind === "fly" && s.from.z < 0.5)
				? "on the hop"
				: flies.length > 0
					? "caught, juggled"
					: "caught";
		if (!knelt) {
			rest = endParts(tl, p, ret, kr0!.t0);
			if (reversed(tl, p, ret, kr0!.t0)) {
				rest.evades =
					rest.evades === "none"
						? "field reversed"
						: `field reversed+${rest.evades}`;
			}
			const end = posOf(tl, ret.pid, p.whistle) ?? at;
			rest.gain = bucket(
				((at.x - end.x) * p.dir) / 3,
				[10, 20, 30, 45],
				["under 10 yds", "10-19 yds", "20-29 yds", "30-44 yds", "45+ yds"],
			);
		}
	}
	return {
		pid: ret && ret.team !== kicking ? ret.pid : 0,
		parts: {
			kick: bucket(
				out,
				[-5, 0, 5, 15],
				[
					"deep in the end zone",
					"in the end zone",
					"at the goal line",
					"inside the 15",
					"short",
				],
			),
			hang: bucket(hang, [3.4, 4], ["low", "3.5 s", "high"]),
			where: bucket(
				edge,
				[20, 50],
				["by the sideline", "outside the hashes", "middle"],
			),
			fielded,
			him,
			...rest,
		},
	};
};

// A flag: when it came out (before the snap, in the play, after the
// whistle), who threw it and how, whether he went and told the referee,
// whether the man flagged argued it, and the referee's signal.
const flagParts = (tl: FieldTimeline, p: Play) => {
	const f = p.flags[0];
	if (!f) {
		return undefined;
	}
	const when =
		f.t0 < p.snap - 50
			? "before the snap"
			: f.t0 <= p.whistle + 100
				? "in the play"
				: "after the whistle";
	const there = (id: number, t: number) => posOf(tl, id, t);
	const thrower = p.crew
		.map((c) => ({ c, at: there(c.pid, f.t0) }))
		.filter((x) => x.at)
		.sort((a, b) => dist(a.at!, f.from) - dist(b.at!, f.from))[0]?.c;
	const by = thrower ? (officialOf(thrower.pid) ?? "?") : "?";
	const far = dist(f.from, f.to);
	const at0 = thrower && there(thrower.pid, f.t0 - 150);
	const at1 = thrower && there(thrower.pid, f.t0);
	const pace = at0 && at1 ? dist(at0, at1) / 0.15 : 0;
	const call = p.calls?.[0]?.t;
	const ref = officialId("R");
	let told = "-";
	if (thrower && thrower.pid !== ref && call !== undefined) {
		told = "no";
		for (let t = p.whistle; t <= call; t += 100) {
			const a = there(thrower.pid, t);
			const b = there(ref, t);
			if (a && b && dist(a, b) < 8) {
				told = "told the referee";
				break;
			}
		}
	}
	const sig =
		call === undefined
			? "-"
			: [
					...new Set(
						(p.crew.find((c) => c.pid === ref)?.legs ?? []).flatMap((l) =>
							l.t1 > call - 300 && l.t0 < call + 4000
								? [l.move, l.arms ?? ""].filter((m) => m.startsWith("sig"))
								: [],
						),
					),
				].join("+") || "none";
	const argued = p.parts.some((q) =>
		q.legs.some(
			(l) =>
				l.arms === "protest" && l.t1 > p.whistle && l.t0 < p.whistle + 4000,
		),
	);
	return {
		pid: 0,
		parts: {
			when,
			by: `${by}${p.flags.some((x) => x.second) ? ", two flags" : ""}`,
			throw: `${bucket(far, [6, 20], ["dropped at his feet", "tossed", "thrown at the spot"])}, ${pace > 6 ? "on the run" : pace > 2 ? "on the move" : "stood"}`,
			told,
			argued: argued ? "argued" : "no",
			signal: sig,
		},
	};
};

// A sack: how long he had, what the quarterback did with it (stood in
// the pocket, stepped up, drifted, slid, got out of it), where the man who
// got there came at him from, how he was brought down, and the
// celebration.
const PARTY = new Set([
	"flex",
	"fistPump",
	"leap",
	"griddy",
	"pointSky",
	"kneelPoint",
	"chestBump",
	"highFive",
	"taunt",
]);
const sackParts = (tl: FieldTimeline, p: Play) => {
	const qb = p.parts.find((q) => q.role === "QB" && q.team === p.offense);
	if (!qb) {
		return undefined;
	}
	const w = p.whistle;
	const a = posOf(tl, qb.pid, w - 1200);
	const b = posOf(tl, qb.pid, w - 100);
	if (!a || !b) {
		return undefined;
	}
	const moved = dist(a, b);
	const fwd = ((b.x - a.x) * p.dir) / (moved || 1);
	const qbDid =
		moved < 3
			? "stood in the pocket"
			: moved > 12
				? "out of the pocket"
				: fwd > 0.5
					? "stepped up"
					: fwd < -0.5
						? "drifted back"
						: "slid aside";
	// (The man of theirs nearest him as it ends: from which side of the
	// way he faced.)
	const faced = momentAt(tl, w - 300).players.find((st) => st.id === qb.pid);
	const near = p.parts
		.filter((q) => q.team !== p.offense)
		.map((q) => ({ q, at: posOf(tl, q.pid, w - 300) }))
		.filter((x) => x.at)
		.sort((x, y) => dist(x.at!, b) - dist(y.at!, b))[0];
	let from = "-";
	if (near && faced) {
		const ang = Math.atan2(near.at!.y - b.y, near.at!.x - b.x) - faced.yaw;
		const c = Math.cos(ang);
		from = c > 0.5 ? "head on" : c < -0.3 ? "from behind" : "from the side";
	}
	const party =
		[
			...new Set(
				p.parts.flatMap((q) =>
					q.team === p.offense
						? []
						: q.legs
								.filter((l) => PARTY.has(l.move) && l.t0 >= w - 200)
								.map((l) => l.move),
				),
			),
		]
			.sort()
			.join("+") || "none";
	const rest = endParts(tl, p, qb, w - 1500);
	// (Another of them slipped first - see action.ts slipFirst - and how.)
	const slipped = p.parts.some(
		(q) =>
			q.team !== p.offense &&
			q.legs.some((l) => l.move === "dive" && l.t0 > p.snap && l.t1 < w - 300),
	);
	const slip = !slipped
		? "none"
		: qb.legs.some((l) => l.move === "spin" && l.t0 > p.snap && l.t0 < w)
			? "spun out"
			: qb.legs.some((l) => l.move === "duck" && l.t0 > p.snap && l.t0 < w)
				? "ducked under"
				: "stepped aside";
	return {
		pid: near?.q.pid ?? 0,
		parts: {
			slip,
			time: bucket(
				(w - p.snap) / 1000,
				[2.8, 3.4],
				["under 2.8 s", "2.8-3.4 s", "over 3.4 s"],
			),
			qb: qbDid,
			from,
			by: near ? near.q.role : "-",
			evades: rest.evades!,
			end: rest.end!,
			tacklers: rest.tacklers!,
			party,
		},
	};
};

// A field goal or a try: how far, how it went through or by the posts and
// how high over the bar, how it bent, and the kicker and the holder after
// it.
const tryParts = (tl: FieldTimeline, p: Play, made: boolean) => {
	const kick = p.ball.filter((b) => b.kind === "fly" && b.t0 >= p.snap)[1];
	if (!kick || kick.kind !== "fly") {
		return undefined;
	}
	const endX = p.dir > 0 ? FIELD_L : 0;
	let cross: { y: number; z: number } | undefined;
	for (let t = kick.t0; t <= kick.t1; t += 5) {
		const b = momentAt(tl, t).ball?.at;
		if (b && (b.x - endX) * p.dir >= 0) {
			cross = b;
			break;
		}
	}
	// (Across to the kicker's left, feet from the middle.)
	const off = cross ? (cross.y - FIELD_W / 2) * p.dir : 0;
	// (Back off the post: on toward the field from where it hit.)
	const doink = p.ball.some(
		(b) =>
			b.kind === "fly" && b.t0 === kick.t1 && (b.to.x - b.from.x) * p.dir < -1,
	);
	const how = !cross
		? "short"
		: doink
			? "off the upright"
			: made
				? Math.abs(off) < 3
					? "down the middle"
					: "inside the upright"
				: off > 0
					? "wide left"
					: "wide right";
	const bend = kick.bend ?? 0;
	const after = (role: string) => {
		const q = p.parts.find((x) => x.role === role && x.team === p.offense);
		const acts = q
			? [
					...new Set(
						q.legs
							.filter(
								(l) =>
									l.t0 >= kick.t1 - 300 &&
									l.t0 < p.whistle + 3000 &&
									(moveKindOf(l.move) === "act" ||
										l.arms ||
										l.move === "onHips" ||
										l.move === "easyHips" ||
										l.move === "takeKnee"),
							)
							.map((l) =>
								l.move === "easyHips" ? "onHips" : (l.arms ?? l.move),
							),
					),
				]
			: [];
		return acts.sort().join("+") || "none";
	};
	return {
		pid: p.parts.find((x) => x.role === "K" && x.team === p.offense)?.pid ?? 0,
		parts: {
			kick:
				p.kind === "extraPoint"
					? "try"
					: bucket(
							Math.abs(kick.from.x - endX) / 3,
							[35, 45, 52],
							["under 35 yds", "35-44 yds", "45-51 yds", "52+ yds"],
						),
			how,
			over: !cross
				? "-"
				: bucket(
						cross.z - 10,
						[8, 16],
						["just over", "well over", "high over"],
					),
			bend:
				Math.abs(bend) < 1.5
					? "straight"
					: bend > 0
						? "bending left"
						: "bending right",
			kicker: after("K"),
			holder: after("H"),
		},
	};
};

// An interception: how deep the throw, how he picked it off (on the run,
// set, up for it, diving, off a tip), who, the return, the man it was meant
// for and the quarterback after it, and the celebration.
const pickParts = (tl: FieldTimeline, p: Play) => {
	const thrown = p.ball.find(
		(b) => b.kind === "fly" && b.spiral === true && b.t0 >= p.snap,
	);
	if (!thrown || thrown.kind !== "fly") {
		return undefined;
	}
	const got = holders(p).find(
		(h) =>
			h.t0 >= thrown.t1 - 50 &&
			p.parts.find((q) => q.pid === h.pid)?.team !== p.offense,
	);
	const picker = got && p.parts.find((q) => q.pid === got.pid);
	if (!picker) {
		return undefined;
	}
	const near = (q: Part, moves: string[], t0: number, t1: number) =>
		q.legs.some(
			(l) =>
				(moves.includes(l.move) || moves.includes(l.arms ?? "")) &&
				l.t1 > t0 &&
				l.t0 < t1,
		);
	const flies = p.ball.filter(
		(b) => b.kind === "fly" && b.t0 >= thrown.t1 - 5 && b.t1 <= got!.t0 + 5,
	);
	const pace = () => {
		const a0 = posOf(tl, picker.pid, got!.t0 - 150);
		const a1 = posOf(tl, picker.pid, got!.t0);
		return a0 && a1 ? dist(a0, a1) / 0.15 : 0;
	};
	const how =
		flies.length > 0
			? "off a tip"
			: near(picker, ["diveCatch"], got!.t0 - 600, got!.t0 + 300)
				? "diving"
				: near(picker, ["leapCatch"], got!.t0 - 600, got!.t0 + 300)
					? "up for it"
					: pace() > 7
						? "on the run"
						: "set";
	const air = ((thrown.to.x - p.losX) * p.dir) / 3;
	const ends = endParts(tl, p, picker, got!.t0);
	const at = posOf(tl, picker.pid, got!.t0) ?? thrown.to;
	const end = posOf(tl, picker.pid, p.whistle) ?? at;
	const qb = p.parts.find((q) => q.role === "QB" && q.team === p.offense);
	const qbDid = !qb
		? "-"
		: near(qb, ["handsOnHelmet", "onHips"], p.whistle - 500, p.whistle + 4000)
			? "hands on helmet or hips"
			: posOf(tl, qb.pid, p.whistle) &&
				  dist(posOf(tl, qb.pid, p.whistle)!, end) < 12
				? "in on the tackle"
				: "after him";
	const party =
		[
			...new Set(
				p.parts.flatMap((q) =>
					q.team === p.offense
						? []
						: q.legs
								.filter((l) => PARTY.has(l.move) && l.t0 >= p.whistle - 200)
								.map((l) => l.move),
				),
			),
		]
			.sort()
			.join("+") || "none";
	return {
		pid: picker.pid,
		parts: {
			throw: bucket(air, [10, 20, 35], ["short", "middle", "deep", "bomb"]),
			how,
			by: picker.role,
			evades: ends.evades!,
			end: ends.end!,
			gain: bucket(
				((end.x - at.x) * -p.dir) / 3,
				[1, 10, 25],
				["none", "1-9 yds", "10-24 yds", "25+ yds"],
			),
			qb: qbDid,
			party,
		},
	};
};

// A timeout (or the two-minute warning): who called it, how that side
// gathered (rows round the head coach, the front row on a knee, the
// quarterback over to him on his own), the other side (a ring, on a knee,
// at its own sideline), the drinks, and the picture after.
const breakParts = (p: Play, warning: boolean) => {
	const to = p.timeout;
	const boss = p.coaches?.[0];
	if (!to || !boss) {
		return undefined;
	}
	const t = to.cut + 1500;
	const moveAt = (q: Part) =>
		q.legs.find((l) => l.t0 <= t && t < l.t1)?.move ?? "-";
	const atOf = (q: Part) => {
		const l = q.legs.find((x) => x.t0 <= t && t < x.t1);
		return l ? l.from : undefined;
	};
	const mine = p.parts.filter((q) => q.pid >= 0 && q.team === boss.team);
	const theirs = p.parts.filter((q) => q.pid >= 0 && q.team !== boss.team);
	const talker =
		mine.find((q) => q.role === "QB") ?? mine.find((q) => q.role === "MIKE");
	const tAt = talker && atOf(talker);
	const rest = mine
		.filter((q) => q !== talker)
		.map(atOf)
		.filter(Boolean) as Pt[];
	const mid = rest.length
		? {
				x: rest.reduce((a, b) => a + b.x, 0) / rest.length,
				y: rest.reduce((a, b) => a + b.y, 0) / rest.length,
			}
		: undefined;
	const gather = mine.some((q) => moveAt(q) === "takeKnee")
		? "front row on a knee"
		: tAt && mid && dist(tAt, mid) > 10
			? "quarterback over to the coach"
			: "rows round the coach";
	const them = (p.coaches ?? []).some(
		(c) =>
			c.team !== boss.team &&
			c.pid < 0 &&
			c.legs.some((l) => l.move === "coachTalk"),
	)
		? "at their sideline"
		: theirs.some((q) => moveAt(q) === "takeKnee")
			? "ring, on a knee"
			: "ring";
	const drinks = p.parts.filter((q) =>
		q.legs.some((l) => l.move === "drink" && l.t0 >= to.cut),
	).length;
	return {
		pid: 0,
		parts: {
			what: warning
				? "two-minute warning"
				: mine.some((q) => q.role === "QB")
					? "offense called it"
					: "defense called it",
			gather,
			them,
			drinks: bucket(drinks, [1, 3, 6], ["none", "1-2", "3-5", "6+"]),
			shots: `${to.call ? "the coach's T, " : ""}huddle${to.cheer ? ", cheerleaders" : to.crowd ? ", crowd" : ""}`,
		},
	};
};

// A man hurt: how he lies, who waved the trainers out, how many went down
// on a knee round him, how he went off, and how many clapped him off.
const hurtParts = (p: Play) => {
	const h = p.hurt;
	const him = h && p.parts.find((q) => q.pid === h.pid);
	if (!h || !him) {
		return undefined;
	}
	const after = (q: Part, moves: string[]) =>
		q.legs.some((l) => l.t0 >= h.from && moves.includes(l.move));
	const lies = him.legs.some((l) => l.move === "hurtProne")
		? "on his face"
		: "on his back";
	const off = him.legs.some((l) => l.t0 >= h.from && l.arms === "overShoulders")
		? "helped off"
		: "on his own";
	const others = p.parts.filter((q) => q.pid >= 0 && q !== him);
	const knees = others.filter((q) => after(q, ["takeKnee"])).length;
	const waved = others.some((q) => after(q, ["waveOver"]));
	const claps = others.filter((q) => after(q, ["clap"]));
	return {
		pid: h.pid,
		parts: {
			lies,
			waved: waved ? "waved out" : "no",
			knees: bucket(knees, [1, 4, 8], ["none", "1-3", "4-7", "8+"]),
			off,
			claps: bucket(claps.length, [1, 3, 6], ["none", "1-2", "3-5", "6+"]),
			both:
				claps.some((q) => q.team === him.team) &&
				claps.some((q) => q.team !== him.team)
					? "both sides"
					: claps.length > 0
						? "one side"
						: "-",
		},
	};
};

// A knee: what the lines did over it, how the quarterback went down and
// what he did after, and where the ball went.
const kneelParts = (p: Play) => {
	const qb = p.parts.find((q) => q.role === "QB");
	if (!qb) {
		return undefined;
	}
	const live = (q: Part) =>
		q.legs.filter((l) => l.t1 > p.snap && l.t0 < p.whistle).map((l) => l.move);
	const ol = p.parts.filter((q) =>
		["LT", "LG", "C", "RG", "RT"].includes(q.role),
	);
	const lines = ol.some((q) => live(q).includes("engage"))
		? "came together"
		: "stood up";
	const after = qb.legs
		.filter((l) => l.t1 > p.whistle && l.t0 < p.whistle + 4000)
		.map((l) => l.move + (l.arms ? `+${l.arms}` : ""));
	const ball = p.ball.filter((b) => b.t0 > p.whistle);
	const to = ball.find((b) => b.kind === "held" && b.pid !== qb.pid);
	return {
		pid: qb.pid,
		parts: {
			lines,
			qb: [...new Set(after)].slice(0, 4).join(" > ") || "-",
			ball: to
				? to.kind === "held" && to.pid < 0
					? "to an official"
					: "to a man"
				: ball.length > 0
					? "set down"
					: "kept",
		},
	};
};

// The ball tossed down at the end of a play (see crew.ts ballBack): flipped
// to an official, picked up off the turf by one, or left lying there - and
// how he had it.
const backParts = (p: Play) => {
	const toss = p.ball.find((b) => b.kind === "fly" && b.down);
	if (toss?.kind !== "fly") {
		return undefined;
	}
	const last = p.ball.at(-1);
	const his = last?.kind === "held" && last.pid < 0 ? last : undefined;
	const legs = his ? p.crew.find((c) => c.pid === his.pid)?.legs : undefined;
	const held = new Set(
		(legs ?? [])
			.filter((l) => his && l.t0 >= his.t0 && l.ball)
			.map((l) => l.ball),
	);
	return {
		pid: his?.pid ?? 0,
		parts: {
			how: !his
				? "left lying"
				: his.t0 === toss.t1
					? "flipped to him"
					: "picked up",
			by: his ? (officialOf(his.pid) ?? "?") : "-",
			held: [...held].join("/") || "-",
		},
	};
};

test.skipIf(games.length === 0)(
	"football variety of one line",
	() => {
		const all: {
			pid: string;
			parts: Record<string, string>;
			where: string;
		}[] = [];
		for (const g of games) {
			const raw = JSON.parse(readFileSync(g, "utf8"));
			const n = Number(/(\d+)\.json$/.exec(g)?.[1] ?? 1);
			const tl = compileField({
				events: raw.events,
				players: raw.roster,
				gid: n,
			});
			const tag = g
				.split("/")
				.at(-1)!
				.replace(/\.json$/, "");
			for (const p of tl.plays) {
				if (p.whistle <= p.snap) {
					continue;
				}
				const evs = raw.events
					.slice(p.i, tl.plays[tl.plays.indexOf(p) + 1]?.i ?? raw.events.length)
					.map((e: { type: string }) => e.type);
				const got =
					line === "run" && p.kind === "run" && evs.includes("run")
						? runParts(tl, p)
						: line === "catch" && evs.includes("passComplete")
							? catchParts(tl, p)
							: line === "incomplete" && evs.includes("passIncomplete")
								? incParts(tl, p)
								: line === "punt" && evs.includes("punt")
									? puntParts(tl, p)
									: line === "kickoff" && evs.includes("kickoff")
										? kickParts(tl, p)
										: line === "flag" && p.flags.length > 0
											? flagParts(tl, p)
											: line === "sack" && evs.includes("sack")
												? sackParts(tl, p)
												: line === "kick" &&
													  (p.kind === "fieldGoal" || p.kind === "extraPoint")
													? tryParts(
															tl,
															p,
															raw.events
																.slice(
																	p.i,
																	tl.plays[tl.plays.indexOf(p) + 1]?.i ??
																		raw.events.length,
																)
																.some(
																	(e: { type: string; made?: boolean }) =>
																		(e.type === "fieldGoal" ||
																			e.type === "extraPoint") &&
																		e.made === true,
																),
														)
													: line === "pick" && evs.includes("interception")
														? pickParts(tl, p)
														: line === "kneel" && p.kind === "kneel"
															? kneelParts(p)
															: line === "back"
																? backParts(p)
																: line === "hurt" && p.hurt
																	? hurtParts(p)
																	: line === "break" && p.timeout
																		? breakParts(
																				p,
																				evs.includes("twoMinuteWarning"),
																			)
																		: undefined;
				if (got) {
					all.push({
						pid: `${tag}:${got.pid}`,
						parts: got.parts,
						where: `${tag} play ${tl.plays.indexOf(p)}`,
					});
				}
			}
		}
		const out: string[] = [
			`${line}: ${all.length} seen, ${games.length} games`,
		];
		const keys = Object.keys(all[0]?.parts ?? {});
		for (const key of keys) {
			const c = new Map<string, number>();
			for (const x of all) {
				c.set(x.parts[key]!, (c.get(x.parts[key]!) ?? 0) + 1);
			}
			out.push(`\n${key}: ${c.size} ways`);
			for (const [v, k] of [...c].sort((a, b) => b[1] - a[1])) {
				out.push(`  ${String(k).padStart(4)}  ${v}`);
			}
		}
		const sig = (x: (typeof all)[number]) =>
			keys.map((k) => x.parts[k]).join(" | ");
		const sigs = new Map<string, number>();
		for (const x of all) {
			sigs.set(sig(x), (sigs.get(sig(x)) ?? 0) + 1);
		}
		const by0 = (process.env.VARIETY_BY ?? "").split(",").filter(Boolean);
		if (by0.length > 0) {
			const c = new Map<string, number>();
			for (const x of all) {
				const v = by0.map((k) => x.parts[k] ?? "?").join(" | ");
				c.set(v, (c.get(v) ?? 0) + 1);
			}
			out.push(`\n${by0.join(" + ")}: ${c.size} ways`);
			for (const [v, k] of [...c].sort((a, b) => b[1] - a[1])) {
				out.push(`  ${String(k).padStart(4)}  ${v}`);
			}
		}
		// (VARIETY_WHERE="how=diving|off a tip": the plays they were, to
		// film.)
		const [wk, wv] = (process.env.VARIETY_WHERE ?? "").split("=");
		if (wk && wv) {
			out.push(`\nwhere ${wk} is ${wv}:`);
			for (const x of all.filter((x) => wv.split("|").includes(x.parts[wk]!))) {
				out.push(`  ${x.parts[wk]}: ${x.where}`);
			}
		}
		out.push(`\nwhole combinations: ${sigs.size} different of ${all.length}`);
		for (const [v, k] of [...sigs].sort((a, b) => b[1] - a[1]).slice(0, 12)) {
			out.push(`  ${String(k).padStart(4)}  ${v}`);
		}
		// For each player: the same way of doing it as one of his before
		// (all but the formation and the gain, which the sim decides).
		const mine = keys.filter((k) => k !== "formation" && k !== "gain");
		const by = new Map<string, string[]>();
		for (const x of all) {
			const list = by.get(x.pid) ?? [];
			list.push(mine.map((k) => x.parts[k]).join(" | "));
			by.set(x.pid, list);
		}
		let repeats = 0;
		let pairs = 0;
		for (const list of by.values()) {
			const seen = new Set<string>();
			for (const s of list) {
				pairs += seen.size > 0 ? 1 : 0;
				if (seen.has(s)) {
					repeats += 1;
				}
				seen.add(s);
			}
		}
		out.push(
			`\nsame player, the same ${mine.join(" + ")} as one of his before: ${repeats} of ${pairs}`,
		);
		writeFileSync(
			process.env.VARIETY_OUT ?? `variety_${line}.txt`,
			out.join("\n") + "\n",
		);
	},
	1_800_000,
);
