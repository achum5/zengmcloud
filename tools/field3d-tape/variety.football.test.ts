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
// (a pass not caught), punt, kickoff, flag.
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
					? "pitch"
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
				(l.move === "leapCatch" || l.move === "diveCatch") &&
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
				(handIn ? ", a hand in" : ""),
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
			by,
			throw: `${bucket(far, [6, 20], ["dropped at his feet", "tossed", "thrown at the spot"])}, ${pace > 6 ? "on the run" : pace > 2 ? "on the move" : "stood"}`,
			told,
			argued: argued ? "argued" : "no",
			signal: sig,
		},
	};
};

test.skipIf(games.length === 0)(
	"football variety of one line",
	() => {
		const all: { pid: string; parts: Record<string, string> }[] = [];
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
											: undefined;
				if (got) {
					all.push({ pid: `${tag}:${got.pid}`, parts: got.parts });
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
