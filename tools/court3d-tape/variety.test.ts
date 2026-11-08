// HOW MANY WAYS ONE PLAY-BY-PLAY LINE PLAYS OUT (see PLAYS.md).
//
// Every time the line comes up in the given games, the parts the 3D court
// put it together from - for a jumper: how he got the ball, where from, his
// form, the defense, how it ended, the rebound - and how many different
// combinations that comes to, overall and for each player.
//
//   VARIETY_GAMES=/tmp/games/real_11.json,... VARIETY_LINE=missTp \
//   VARIETY_OUT=/tmp/variety_missTp.txt \
//     npx vitest run --project basketball tools/court3d-tape/variety.test.ts

import { readFileSync, writeFileSync } from "node:fs";
import process from "node:process";
import { test } from "vitest";
import {
	compileCourt,
	type BallSeg,
	type CourtTimeline,
} from "../../src/ui/views/LiveGame/court3d/director.ts";
import {
	evalPlayer,
	fastAt,
} from "../../src/ui/views/LiveGame/court3d/evaluate.ts";
import {
	RIM_R,
	RIM_Z,
	rimX,
} from "../../src/ui/views/LiveGame/court3d/geometry.ts";
import {
	playShot,
	type ShotPlay,
} from "../../src/ui/views/LiveGame/court3d/physics.ts";

// Where a miss first met the rim along the line of the shot, or the glass,
// or nothing.
const firstTouch = (rim: Pt, from: Pt, p: ShotPlay): string => {
	const first = p.touches[0];
	if (!first) {
		return "airball";
	}
	if (!first.rim) {
		return "off the glass";
	}
	const hx = rim.x - from.x;
	const hy = rim.y - from.y;
	const far = Math.hypot(hx, hy) || 1;
	const along = ((first.at.x - rim.x) * hx + (first.at.y - rim.y) * hy) / far;
	const across = Math.abs(
		((first.at.x - rim.x) * -hy + (first.at.y - rim.y) * hx) / far,
	);
	return across > Math.abs(along)
		? "off the side"
		: along < 0
			? "front rim"
			: "back rim";
};

const games = (process.env.VARIETY_GAMES ?? "").split(",").filter(Boolean);
const line = process.env.VARIETY_LINE ?? "missTp";

type Pt = { x: number; y: number };
const dist = (a: Pt, b: Pt) => Math.hypot(a.x - b.x, a.y - b.y);

// The parts of one jumper.
const jumperParts = (tl: CourtTimeline, shot: BallSeg & { kind: "fly" }) => {
	const k = tl.ball.indexOf(shot);
	const pid = (shot.from as { pid: number }).pid;
	const tr = tl.tracks.get(pid)!;
	const team = tr.team;
	const rim = { x: rimX(team), y: 25 };
	const release = shot.t0;
	const P = evalPlayer(tl, pid, release);

	// How he got it: his time with it, and the pass before.
	let j = k - 1;
	let dribbled = 0;
	while (j >= 0) {
		const h = tl.ball[j]!;
		if (h.kind !== "hold" || h.pid !== pid) {
			break;
		}
		if (h.style !== "hold") {
			dribbled += (tl.ball[j + 1]?.t0 ?? release) - h.t0;
		}
		j -= 1;
	}
	const had = release - (tl.ball[j + 1]?.t0 ?? release);
	// (A bounce pass is two flights - to the floor, and up off it to him:
	// the man who threw it is at the start of the first.)
	const last = tl.ball[j];
	const first = tl.ball[j - 1];
	const bounced =
		last?.kind === "fly" &&
		!("pid" in last.from) &&
		"pid" in last.to &&
		first?.kind === "fly" &&
		"pid" in first.from &&
		!("pid" in first.to);
	const pass = bounced ? first : last;
	const passer =
		pass?.kind === "fly" && "pid" in pass.from && (bounced || "pid" in pass.to)
			? pass.from.pid
			: undefined;
	const acts = tr.acts.filter(
		(a) => a.t1 > release - had - 200 && a.t0 <= release + 50,
	);
	const moves = new Set(acts.map((a) => a.anim));
	const tags: string[] = [];
	const dist2 = dist(P, rim);
	let setup: string;
	if (dist2 > 35) {
		setup = "heave";
	} else if (dribbled >= 600 || passer === undefined) {
		setup = moves.has("stepBack")
			? "step-back"
			: moves.has("sideStep") || moves.has("sidestep")
				? "side-step"
				: [...moves].some((m) => m.startsWith("dribbleHesi"))
					? "hesitation pull-up"
					: [...moves].some((m) => m.startsWith("cross"))
						? "crossover pull-up"
						: "pull-up";
	} else {
		const F = evalPlayer(tl, passer, pass!.t0);
		const L = dist(F, P);
		setup =
			L < 5
				? "handoff"
				: dist(F, rim) < 12
					? "kick-out"
					: L > 30
						? "skip pass"
						: "swing";
		if (had > 1300) {
			setup += ", held";
		}
	}
	if (moves.has("shotFake") || moves.has("pumpFake")) {
		tags.push("fake");
	}
	// Off a screen: a man of his set one by him on the way to it.
	for (const o of tl.tracks.values()) {
		if (o.team !== team || o.pid === pid || o.pid < 0) {
			continue;
		}
		const s = o.acts.find(
			(a) =>
				a.anim === "screen" && a.t1 > release - had - 2500 && a.t0 < release,
		);
		if (s && dist(evalPlayer(tl, o.pid, s.t0), evalPlayer(tl, pid, s.t0)) < 7) {
			tags.push("screen");
			break;
		}
	}
	// In transition: the ball changed hands only a few seconds before.
	const pi = tl.poss.findLastIndex(([at]) => at <= release);
	if (pi >= 0 && release - tl.poss[pi]![0] < 7000) {
		tags.push("transition");
	}

	// Where from.
	const side = P.y < 25 ? "L" : "R";
	const deep = dist2 - 23.75;
	const spot =
		Math.abs(P.y - 25) > 20 && Math.abs(P.x - rim.x) < 14
			? `corner ${side}`
			: Math.abs(P.y - 25) > 9
				? `wing ${side}`
				: "top";
	const depth = deep < 1.2 ? "on the line" : deep < 3.5 ? "a step off" : "deep";

	// His form, as it is drawn.
	const form = evalPlayer(tl, pid, release).anim;

	// His footwork into it, as it is drawn: how he was when it came to him
	// (or he picked his dribble up), which way his last steps took him, and
	// how far round he came to it.
	const got = tl.ball[j + 1]?.t0 ?? release;
	const pace = (t: number) => {
		const a = evalPlayer(tl, pid, t - 50);
		const b = evalPlayer(tl, pid, t + 50);
		return Math.hypot(b.x - a.x, b.y - a.y) * 10;
	};
	// (Coming into the catch: a beat before it, not as he plants.)
	const came = pace(got - 150);
	const arrived = came < 1.5 ? "set" : came < 7 ? "on the move" : "running";
	// (From a beat before the catch to his feet set under him for it.)
	const A0 = evalPlayer(tl, pid, got - 300);
	const A1 = evalPlayer(tl, pid, Math.min(release - 100, got + 250));
	const step = dist(A0, A1);
	let into = "feet set";
	if (step > 0.6) {
		const ux = (rim.x - P.x) / (dist2 || 1);
		const uy = (rim.y - P.y) / (dist2 || 1);
		const along = ((A1.x - A0.x) * ux + (A1.y - A0.y) * uy) / step;
		into =
			along > 0.6
				? "stepping in"
				: along < -0.6
					? "drifting back"
					: "sliding across";
	}
	const yawAt = (t: number) => evalPlayer(tl, pid, t).yaw;
	const da = yawAt(release) - yawAt(got);
	const turned =
		(Math.abs(Math.atan2(Math.sin(da), Math.cos(da))) * 180) / Math.PI;
	const turn =
		turned < 20 ? "squared" : turned < 60 ? "turning" : "turning right round";
	// His way to the spot, off a pass: how far he went in the seconds
	// before it came - out from the rim, in at it, along the arc, down to
	// the corner, or nowhere - and how long he had been there.
	let way = "off the dribble";
	if (dribbled < 600 && passer !== undefined) {
		let L = 0;
		let still = got;
		let prevP = evalPlayer(tl, pid, got - 2500);
		for (let t = got - 2400; t <= got; t += 100) {
			const Q = evalPlayer(tl, pid, t);
			const step = dist(prevP, Q);
			L += step;
			if (step * 10 >= 1.5) {
				still = t;
			}
			prevP = Q;
		}
		const A = evalPlayer(tl, pid, got - 2500);
		const N = { x: P.x - A.x, y: P.y - A.y };
		const n = Math.hypot(N.x, N.y);
		const ux = (rim.x - P.x) / (dist2 || 1);
		const uy = (rim.y - P.y) / (dist2 || 1);
		const along = n > 0 ? (N.x * ux + N.y * uy) / n : 0;
		const corner = Math.abs(P.y - 25) > 20 && Math.abs(P.x - rim.x) < 14;
		const kind =
			L < 1.5
				? "stood there"
				: along < -0.5
					? "lifting out"
					: along > 0.5
						? "coming in"
						: corner
							? "drifting to the corner"
							: "along the arc";
		const there = got - still;
		way =
			L < 1.5
				? kind
				: `${kind}, ${there < 150 ? "on the move" : there < 700 ? "just there" : "set a while"}`;
	}
	// (Off a pass, that is: off the dribble, his feet are his move's.)
	const footwork =
		dribbled >= 600 || passer === undefined
			? "off the dribble"
			: `${arrived}, ${into}, ${turn}`;

	// The pass, as it is drawn: the kind he threw it with, how far, and
	// whether it led a man on the move or went to a man stood there.
	let passKind = "none";
	if (passer !== undefined && pass?.kind === "fly") {
		const ptr = tl.tracks.get(passer);
		const how =
			ptr?.acts.find(
				(a) =>
					a.anim.startsWith("pass") &&
					a.t0 <= pass.t0 + 50 &&
					a.t1 >= pass.t0 - 300,
			)?.anim ?? "?";
		const at = last?.kind === "fly" ? last.t1 : pass.t1;
		const F = evalPlayer(tl, passer, pass.t0);
		const L = dist(F, evalPlayer(tl, pid, at));
		const far = L < 12 ? "short" : L < 25 ? "mid" : "long";
		const led = pace(at - 150) > 4 ? "led" : "to him";
		passKind = `${how}, ${far}, ${led}`;
	}

	// The defense.
	let near = Infinity;
	let contested = false;
	let late = false;
	for (const o of tl.tracks.values()) {
		if (o.team === team || o.pid < 0) {
			continue;
		}
		const D = evalPlayer(tl, o.pid, release);
		if (!D.shown) {
			continue;
		}
		near = Math.min(near, dist(D, P));
		const c = o.acts.find(
			(a) =>
				(a.anim === "contest" || a.anim === "block") &&
				a.t0 < release + 200 &&
				a.t1 > release - 300 &&
				dist(evalPlayer(tl, o.pid, a.t0), P) < 7,
		);
		if (c) {
			contested = true;
			late ||= c.t0 > release - 80;
		}
	}
	// His man, closing out: the one up at it, or the nearest of them - how
	// far he came from as it was caught, how he came in, what hand he got
	// up; and after it (the next second and a bit), what he did.
	let closeout = "nobody";
	let afterIt = "-";
	let arrives = "-";
	{
		let man: number | undefined;
		let hand:
			| { anim: string; t0: number; jump?: [number, number, number] }
			| undefined;
		let best = Infinity;
		for (const o of tl.tracks.values()) {
			if (o.team === team || o.pid < 0) {
				continue;
			}
			const D = evalPlayer(tl, o.pid, release);
			if (!D.shown) {
				continue;
			}
			const c = o.acts.find(
				(a) =>
					(a.anim === "contest" || a.anim === "block") &&
					a.t0 < release + 400 &&
					a.t1 > release - 300,
			);
			const score = dist(D, P) - (c ? 6 : 0);
			if (score < best) {
				best = score;
				man = o.pid;
				hand = c;
			}
		}
		if (man !== undefined && dist(evalPlayer(tl, man, release), P) < 12) {
			const D0 = evalPlayer(tl, man, got - 200);
			const D1 = evalPlayer(tl, man, release);
			const from = dist(D0, P);
			const covered = from - dist(D1, P);
			const approach =
				from < 6 || covered < 3
					? "on him"
					: from < 14
						? "a few steps"
						: "flat out from the help";
			const mtr = tl.tracks.get(man)!;
			const chop = mtr.acts.some(
				(a) => a.anim === "closeout" && a.t1 > got && a.t0 < release + 100,
			);
			const moves = mtr.moves.filter(
				(m) => m.t1 > got - 200 && m.t0 < release + 1500,
			);
			const how =
				approach === "on him"
					? ""
					: chop || moves.some((m) => m.anim === "closeout")
						? ", chopping in"
						: ", running in";
			const peak = hand?.jump?.[2] ?? 0;
			const up = !hand
				? "no hand"
				: hand.anim === "block"
					? "a block try"
					: peak < 0.7
						? "a hand up"
						: peak < 1.5
							? "a hop at it"
							: "a leap at it";
			const lateHand = hand && hand.t0 > release - 80 ? " late" : "";
			closeout = `${approach}${how}, ${up}${lateHand}`;
			// When he got there: the end of his last move in at him.
			const inAt = moves
				.filter((m) => dist(m.to, P) < dist(m.from, P) - 0.5)
				.reduce((t, m) => Math.max(t, m.t1), -Infinity);
			const lag = inAt - release;
			arrives =
				inAt === -Infinity
					? "-"
					: lag < 0
						? "before it goes"
						: lag < 300
							? "as it goes"
							: lag < 700
								? "after, by 0.3-0.7 s"
								: "after, by 0.7 s+";
			// After it.
			const E = evalPlayer(tl, man, release + 1300);
			const S = evalPlayer(tl, pid, release + 1300);
			const boxed = mtr.acts.some(
				(a) =>
					(a.anim === "boxOut" || a.anim === "fight") &&
					a.t1 > release &&
					a.t0 < release + 1500,
			);
			const passed =
				dist(E, rim) > dist(S, rim) + 1 && dist(D1, rim) < dist(P, rim);
			afterIt = boxed
				? dist(E, S) < 5
					? "boxes the shooter out"
					: "boxes out another"
				: passed
					? "runs on by"
					: dist(E, rim) < dist(D1, rim) - 4
						? "to the glass"
						: dist(E, D1) < 1.5
							? "stays, watching"
							: "drifts";
		}
	}
	const defense =
		near > 8
			? "wide open"
			: !contested
				? "open"
				: near < 3
					? late
						? "late hand in his face"
						: "hand in his face"
					: late
						? "late closeout"
						: "contested";

	// The shooter after it, till it comes off the rim and a moment on: held
	// his follow-through, back on defense, in after it - and anything he
	// did with it (a reaction).
	let shooterAfter = "?";
	{
		const S0 = evalPlayer(tl, pid, release + 300);
		const S1 = evalPlayer(tl, pid, release + 1800);
		const moved = dist(S0, S1);
		const towardRim = dist(S1, rim) < dist(S0, rim) - 2;
		const did = tr.acts
			.filter((a) => a.t0 > release + 200 && a.t0 < release + 2500)
			.map((a) => a.anim);
		const kind =
			moved < 1.5
				? "stays where he landed"
				: towardRim
					? "follows his shot"
					: "gets back";
		shooterAfter =
			did.length > 0 ? `${kind}, ${[...new Set(did)].join("+")}` : kind;
	}
	// Everyone else, from it going up to it coming off the rim: on his
	// side, how many went in after it and how many got back; on theirs,
	// how many boxed somebody out or went to the glass - and, of all of
	// them, how many just stood there.
	let everyone = "?";
	{
		const offAt = tl.ball[k + 1]?.t0 ?? release + 1100;
		// (Boxing out, till it is off the rim.)
		const offSeg = tl.ball[k + 1];
		const offEnd =
			offSeg && "t1" in offSeg ? (offSeg.t1 as number) : offAt + 600;
		let crash = 0;
		let back = 0;
		let boxing = 0;
		let glass = 0;
		let still = 0;
		const stillWho: string[] = [];
		for (const o of tl.tracks.values()) {
			if (o.pid < 0 || o.pid === pid) {
				continue;
			}
			const A = evalPlayer(tl, o.pid, release);
			const B = evalPlayer(tl, o.pid, offAt);
			if (!A.shown || !B.shown) {
				continue;
			}
			const went = dist(A, B);
			const inward = dist(A, rim) - dist(B, rim);
			if (o.team === team) {
				if (inward > 2) {
					crash += 1;
				} else if (inward < -2) {
					back += 1;
				} else if (went < 1) {
					still += 1;
					stillWho.push(
						`off ${dist(A, rim) < 12 ? "in close" : dist(A, rim) < 20 ? "mid" : "out"}`,
					);
				}
			} else {
				const boxed = o.acts.some(
					(a) =>
						(a.anim === "boxOut" || a.anim === "fight") &&
						a.t1 > release &&
						a.t0 < offEnd,
				);
				if (boxed) {
					boxing += 1;
				} else if (inward > 2) {
					glass += 1;
				} else if (went < 1) {
					still += 1;
					const busy = o.acts.find((a) => a.t1 > release && a.t0 < offAt)?.anim;
					stillWho.push(
						`def ${dist(A, rim) < 12 ? "in close" : dist(A, rim) < 20 ? "mid" : "out"}${busy ? " " + busy : ""}`,
					);
				}
			}
		}
		everyone = `${crash} in, ${back} back; ${boxing} boxing out, ${glass} to the glass; ${still} still`;
		(globalThis as { __still?: string[] }).__still = [
			...((globalThis as { __still?: string[] }).__still ?? []),
			...stillWho,
		];
	}
	// How it ended: what it hit first, and how it came off. (Its flight ends
	// above the rim; the bounce off it is the path that comes after.)
	const after = tl.ball[k + 1];
	let ending = "airball";
	if (after?.kind === "path") {
		// (Played again from where the path takes over, for where it touched.)
		const pts = after.pts;
		const play = playShot(
			team,
			{ x: pts[0]!, y: pts[1]!, z: pts[2]! },
			after.v0,
		);
		ending = firstTouch(rim, P, play);
		const rimTouches = play.touches.filter((h) => h.rim).length;
		let inside = false;
		for (let i = 0; i + 2 < pts.length; i += 3) {
			const r = Math.hypot(pts[i]! - rim.x, pts[i + 1]! - rim.y);
			if (r < RIM_R - 0.3 && Math.abs(pts[i + 2]! - RIM_Z) < 0.3) {
				inside = true;
			}
		}
		if (inside) {
			ending += ", in and out";
		} else if (rimTouches >= 3) {
			ending += ", rattles round";
		}
	}
	let rebound = "?";
	for (let q = k + 1; q < Math.min(tl.ball.length, k + 6); q++) {
		const s = tl.ball[q]!;
		if (s.kind === "hold") {
			const R = evalPlayer(tl, s.pid, s.t0);
			const by = tl.tracks.get(s.pid)?.team === team ? "offense" : "defense";
			const d = dist(R, rim);
			rebound = `${by} ${d < 8 ? "short" : d < 15 ? "mid" : "long"}`;
			if (
				tl.ball
					.slice(k + 1, q)
					.some((z) => z.kind === "bounce" || z.kind === "rest")
			) {
				rebound += " off the floor";
			}
			break;
		}
	}
	return {
		pid,
		parts: {
			setup: [setup, ...tags].join(" + "),
			spot: `${spot}, ${depth}`,
			form: form ?? "?",
			footwork,
			way,
			pass: passKind,
			defense,
			closeout,
			arrives,
			"after it": afterIt,
			"shooter after": shooterAfter,
			everyone,
			ending,
			rebound,
		},
	};
};

// The parts of one rebound: how he came up with it, where, in what traffic,
// and what he did with it.
const reboundParts = (
	tl: CourtTimeline,
	pid: number,
	from: number,
	until: number,
	pos: string,
) => {
	const k = tl.ball.findIndex(
		(s) => s.kind === "hold" && s.pid === pid && s.t0 >= from && s.t0 <= until,
	);
	if (k < 1) {
		return undefined;
	}
	const h = tl.ball[k]!;
	const t = h.t0;
	const tr = tl.tracks.get(pid)!;
	const team = tr.team;
	// The rim the shot was at: the other side's, off a defensive board.
	const shotTeam = tl.tracks.get(pid)!.team === 0 ? 1 : 0;
	const prev = tl.ball[k - 1]!;
	const R = evalPlayer(tl, pid, t);
	const act = tr.acts.find((a) => a.t0 <= t + 30 && a.t1 >= t - 30);
	const before = tl.ball.slice(Math.max(0, k - 4), k).map((s) => s.kind);
	let how =
		prev.kind === "fly" && !("pid" in prev.from)
			? R.z > 0.6
				? "up high for it"
				: "taken at chest height"
			: "picked up";
	if (before.includes("bounce") || before.includes("rest")) {
		how = R.z > 0.4 ? "off the bounce, in the air" : "off the floor";
	}
	const tap = tl.ball[k - 2];
	if (
		tap?.kind === "fly" &&
		!("pid" in tap.from) &&
		!("pid" in tap.to) &&
		prev.kind === "fly" &&
		!("pid" in prev.from) &&
		Math.abs(tap.t1 - tap.t0 - 240) < 3
	) {
		how = "tipped up to himself";
	}
	if (act?.anim === "rebound") {
		how += ", one hand";
	} else if (act?.anim === "board") {
		how += ", two hands";
	}
	// Where: how far out, and which side.
	const rimAt = { x: rimX(shotTeam as 0 | 1), y: 25 };
	const d = dist(R, rimAt);
	const where = `${d < 7 ? "under it" : d < 13 ? "in the lane" : d < 20 ? "out a way" : "long"}, ${Math.abs(R.y - 25) < 6 ? "middle" : R.y < 25 ? "left" : "right"}`;
	// And how far out, by what it came off: a shot at the rim, a jumper,
	// a three.
	const shot = tl.ball
		.slice(0, k)
		.findLast(
			(z) =>
				z.kind === "fly" &&
				"pid" in z.from &&
				z.from.pid >= 0 &&
				!("pid" in z.to) &&
				tl.tracks.get(z.from.pid)?.team === shotTeam,
		);
	const S =
		shot?.kind === "fly" && "pid" in shot.from
			? evalPlayer(tl, shot.from.pid, shot.t0)
			: undefined;
	const sd = S ? dist(S, rimAt) : undefined;
	const off = `${sd === undefined ? "?" : sd < 8 ? "off one at the rim" : sd < 22 ? "off a jumper" : "off a three"}, ${d < 7 ? "under it" : d < 13 ? "in the lane" : d < 20 ? "out a way" : "long"}`;
	// Traffic, and box-outs.
	let near = 0;
	let boxed = false;
	for (const o of tl.tracks.values()) {
		if (o.pid < 0 || o.pid === pid) {
			continue;
		}
		const q = evalPlayer(tl, o.pid, t);
		if (q.shown && dist(q, R) < 4) {
			near += 1;
		}
		if (
			o.team === team &&
			o.acts.some((a) => a.anim === "boxOut" && a.t1 > t - 1500 && a.t0 < t)
		) {
			boxed = true;
		}
	}
	const self = tr.acts.some(
		(a) =>
			(a.anim === "boxOut" || a.anim === "fight") &&
			a.t1 > t - 1500 &&
			a.t0 < t,
	);
	const traffic = `${near === 0 ? "alone" : near === 1 ? "one man near" : "in a crowd"}${self ? ", boxed his man out" : boxed ? ", mates boxed out" : ""}`;
	// What he did with it.
	let j = k;
	while (j + 1 < tl.ball.length) {
		const z = tl.ball[j + 1]!;
		if (z.kind !== "hold" || z.pid !== pid) {
			break;
		}
		j += 1;
	}
	const kept = tl.ball.slice(k, j + 1);
	const dribbled = kept.some((z) => z.kind === "hold" && z.style !== "hold");
	const next = tl.ball[j + 1];
	const heldFor = (next?.t0 ?? t) - t;
	let after: string;
	if (next?.kind === "fly" && "pid" in next.to && "pid" in next.from) {
		const to = tl.tracks.get(next.to.pid);
		const L = dist(
			evalPlayer(tl, pid, next.t0),
			evalPlayer(tl, next.to.pid, next.t1),
		);
		after =
			to?.team === team
				? `${dribbled ? "dribbled, then " : ""}${L > 30 ? "long outlet" : L > 12 ? "outlet" : "handed off"}${heldFor < 900 ? " at once" : ""}`
				: "lost it";
	} else if (dribbled) {
		after = "brought it up himself";
	} else {
		after = "held it";
	}
	const pace = fastAt(tl, t + 2500) > 1.2 ? "walked up" : "pushed";
	return {
		pid,
		parts: {
			who: pos || "?",
			how,
			where,
			off,
			traffic,
			after: `${after}, ${pace}`,
		},
	};
};

test.skipIf(games.length === 0)(
	"variety of one line",
	() => {
		const all: { pid: string; parts: Record<string, string> }[] = [];
		let fastOnes = 0;
		for (const g of games) {
			const { events, players } = JSON.parse(readFileSync(g, "utf8"));
			const tl = compileCourt({ events, players, gid: 1 });
			const tag = g
				.split("/")
				.at(-1)!
				.replace(/\.json$/, "");
			const posOf = new Map<number, string>(
				players.map((p: { pid: number; pos?: string }) => [p.pid, p.pos ?? ""]),
			);
			for (const b of tl.beats) {
				if (b.type !== line) {
					continue;
				}
				if (line === "drb" || line === "orb") {
					const rpid = events[b.i]?.pid as number | undefined;
					if (rpid === undefined) {
						continue;
					}
					const got = reboundParts(
						tl,
						rpid,
						b.preStart - 3000,
						b.end + 1500,
						posOf.get(rpid) ?? "",
					);
					if (!got) {
						continue;
					}
					if (fastAt(tl, b.actionStart) > 1.2) {
						fastOnes += 1;
						continue;
					}
					all.push({ pid: `${tag}:${got.pid}`, parts: got.parts });
					continue;
				}
				const shot = tl.ball.findLast(
					(s) =>
						s.kind === "fly" &&
						"pid" in s.from &&
						s.from.pid >= 0 &&
						!("pid" in s.to) &&
						s.t0 <= b.actionStart + 200,
				) as (BallSeg & { kind: "fly" }) | undefined;
				if (!shot) {
					continue;
				}
				if (fastAt(tl, shot.t0) > 1.2) {
					fastOnes += 1;
					continue;
				}
				const got = jumperParts(tl, shot);
				all.push({ pid: `${tag}:${got.pid}`, parts: got.parts });
			}
		}
		const out: string[] = [
			`${line}: ${all.length} seen at real speed (${fastOnes} run through fast), ${games.length} games`,
		];
		const keys = Object.keys(all[0]?.parts ?? {});
		for (const key of keys) {
			const c = new Map<string, number>();
			for (const x of all) {
				c.set(x.parts[key]!, (c.get(x.parts[key]!) ?? 0) + 1);
			}
			out.push(`\n${key}: ${c.size} ways`);
			for (const [v, n] of [...c].sort((a, b) => b[1] - a[1])) {
				out.push(`  ${String(n).padStart(4)}  ${v}`);
			}
		}
		const sig = (x: (typeof all)[number]) =>
			keys.map((k) => x.parts[k]).join(" | ");
		const sigs = new Map<string, number>();
		for (const x of all) {
			sigs.set(sig(x), (sigs.get(sig(x)) ?? 0) + 1);
		}
		out.push(`\nwhole combinations: ${sigs.size} different of ${all.length}`);
		for (const [v, n] of [...sigs].sort((a, b) => b[1] - a[1]).slice(0, 12)) {
			out.push(`  ${String(n).padStart(4)}  ${v}`);
		}
		// For each player: the same how-he-got-it, where and form again.
		const by = new Map<string, string[]>();
		for (const x of all) {
			const list = by.get(x.pid) ?? [];
			list.push([x.parts.setup, x.parts.spot, x.parts.form].join(" | "));
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
			`\nsame player, same setup + spot + form as one of his before: ${repeats} of ${pairs}`,
		);
		const sw = new Map<string, number>();
		for (const w of (globalThis as { __still?: string[] }).__still ?? []) {
			sw.set(w, (sw.get(w) ?? 0) + 1);
		}
		out.push(
			`\nstill, who: ${[...sw]
				.sort((a, b) => b[1] - a[1])
				.map(([w, n]) => `${n} ${w}`)
				.join("; ")}`,
		);
		writeFileSync(process.env.VARIETY_OUT!, out.join("\n") + "\n");
	},
	1_800_000,
);
