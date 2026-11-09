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
import { greetsFor } from "../../src/ui/views/LiveGame/court3d/scene.ts";
import { crewAt, crewFor } from "../../src/ui/views/LiveGame/court3d/crew.ts";
import {
	RIM_R,
	RIM_Z,
	rimX,
} from "../../src/ui/views/LiveGame/court3d/geometry.ts";
import {
	kindOf,
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
const jumperParts = (
	tl: CourtTimeline,
	shot: BallSeg & { kind: "fly" },
	// It went in: how it went in, and what came after a make, in place of
	// the miss's.
	made = false,
	// A shot in the post: how he got there - the back-down, his moves.
	post = false,
) => {
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
	// Inside the arc: by its angle to the rim - along the baseline, the
	// elbow or wing, straight on - and how far out.
	const twoSpot = (() => {
		const dx = Math.abs(P.x - rim.x);
		const dy = P.y - 25;
		const ang = (Math.atan2(Math.abs(dy), dx) * 180) / Math.PI;
		const far = dist(P, rim);
		const where =
			ang > 62 ? `baseline ${side}` : ang > 25 ? `elbow/wing ${side}` : "top";
		return `${where}, ${far < 14 ? "short" : far < 19 ? "mid" : "long two"}`;
	})();

	// His form, as it is drawn.
	const form = evalPlayer(tl, pid, release).anim;
	// In the post: how many steps backing his man down (his "post" runs),
	// and what he did with it there - a spin, a fake, a jab - before it went
	// up; and whether a man of theirs was on him, rim side, as it did.
	let postPart = "?";
	if (post) {
		const backs = tr.moves.filter(
			(m) => m.anim === "post" && m.t0 > release - 5000 && m.t0 < release,
		).length;
		const did = [
			...new Set(
				tr.acts
					.filter(
						(a) =>
							a.t0 > release - 3000 &&
							a.t0 < release - 100 &&
							a.anim !== "catch" &&
							a.anim !== "hold",
					)
					.map((a) => a.anim as string),
			),
		];
		let on = "nobody on him";
		for (const o of tl.tracks.values()) {
			if (o.team === team || o.pid < 0) {
				continue;
			}
			const q = evalPlayer(tl, o.pid, release);
			const d = dist(q, P);
			if (d < 4) {
				const front = (q.x - P.x) * (rim.x - P.x) + (q.y - P.y) * (rim.y - P.y);
				on = front > 0 ? "his man rim side" : "his man beside or behind";
				break;
			}
		}
		postPart = `${backs === 0 ? "no back-down" : backs < 3 ? "a short back-down" : "a long back-down"}${did.length ? `, ${did.sort().join("+")}` : ""}; ${on}`;
	}

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
		// (A make: on till well after it is through - back down the floor,
		// or not yet.)
		const S0 = evalPlayer(tl, pid, release + 300);
		const S1 = evalPlayer(tl, pid, release + (made ? 3500 : 1800));
		const moved = dist(S0, S1);
		const towardRim = dist(S1, rim) < dist(S0, rim) - 2;
		const did = tr.acts
			.filter(
				(a) => a.t0 > release + 200 && a.t0 < release + (made ? 4000 : 2500),
			)
			.map((a) => a.anim as string)
			.concat(
				// (Or said with an arm on his way: see Gesture.)
				(tr.arms ?? [])
					.filter(
						(g) =>
							g.t0 > release + 200 && g.t0 < release + (made ? 4000 : 2500),
					)
					.map(
						(g) =>
							`arm ${g.kind}${typeof g.at === "object" && g.at.y < 0 ? " at the bench" : ""}`,
					),
			);
		const kind =
			moved < 1.5
				? "stays where he landed"
				: towardRim
					? made
						? "toward the rim"
						: "follows his shot"
					: "gets back";
		shooterAfter =
			did.length > 0 ? `${kind}, ${[...new Set(did)].join("+")}` : kind;
	}
	// Everyone else, from it going up to it coming off the rim: on his
	// side, how many went in after it and how many got back; on theirs,
	// how many boxed somebody out or went to the glass - and, of all of
	// them, how many just stood there.
	// A make: what the other nine do over the few seconds after it - with
	// their arms (a gesture) or more (an act) - his mates and theirs.
	let madeEveryone = "?";
	if (made) {
		const did = (mine: boolean) => {
			const kinds = new Set<string>();
			let n = 0;
			for (const o of tl.tracks.values()) {
				if (o.pid < 0 || o.pid === pid || (o.team === team) !== mine) {
					continue;
				}
				const acts = o.acts
					.filter((a) => a.t0 > release + 300 && a.t0 < release + 3500)
					.map((a) => a.anim as string);
				const arms = (o.arms ?? [])
					.filter((g) => g.t0 > release + 300 && g.t0 < release + 3500)
					.map((g) => `arm ${g.kind}`);
				if (acts.length + arms.length > 0) {
					n += 1;
					[...acts, ...arms].forEach((x) => kinds.add(x));
				}
			}
			return `${n}${kinds.size ? ` (${[...kinds].sort().join("+")})` : ""}`;
		};
		madeEveryone = `mates ${did(true)}; theirs ${did(false)}`;
	}
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
		if (made) {
			ending =
				play.touches.length === 0
					? "swish"
					: `${ending}${rimTouches >= 3 ? ", rattles in" : ", in"}`;
		} else if (inside) {
			ending += ", in and out";
		} else if (rimTouches >= 3) {
			ending += ", rattles round";
		}
	} else if (made) {
		ending = "swish";
	}
	// A make: who of theirs takes it out (by position, and where from), and
	// how long till it is in again.
	let takenOut = "?";
	if (made) {
		const got = tl.ball
			.slice(k + 1, k + 8)
			.find((z) => z.kind === "hold" && tl.tracks.get(z.pid)?.team !== team) as
			| (BallSeg & { kind: "hold" })
			| undefined;
		const thrown = got && tl.ball[tl.ball.indexOf(got) + 1];
		if (got) {
			const at = evalPlayer(tl, got.pid, got.t0);
			const under = Math.abs(at.x - rim.x) > 4 ? "off the floor" : "under it";
			takenOut = `${under}${thrown ? `, in ${Math.round((thrown.t0 - release) / 500) / 2}s` : ""}`;
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
			spot:
				deep < -1 && Math.abs(P.y - 25) < 21.5 ? twoSpot : `${spot}, ${depth}`,
			form: form ?? "?",
			footwork,
			way,
			pass: passKind,
			defense,
			closeout,
			arrives,
			"after it": afterIt,
			"shooter after": shooterAfter,
			everyone: made ? madeEveryone : everyone,
			...(post ? { post: postPart } : {}),
			ending,
			...(made ? { "taken out": takenOut } : { rebound }),
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
	offensive = false,
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
	// The rim the shot was at: the other side's, off a defensive board -
	// his own, off an offensive one.
	const shotTeam = offensive ? team : team === 0 ? 1 : 0;
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
		how += act.oneHand ? ", ripped down one-handed" : ", two hands";
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
	// A man of theirs up for it with him, or a hand at it.
	const rival = [...tl.tracks.values()].find(
		(o) =>
			o.pid >= 0 &&
			o.team !== team &&
			o.acts.some(
				(a) =>
					(a.anim === "rebound" || a.anim === "reach") &&
					!a.quiet &&
					a.t0 < t + 200 &&
					a.t1 > t - 400,
			),
	);
	const traffic = `${near === 0 ? "alone" : near === 1 ? "one man near" : "in a crowd"}${self ? ", boxed his man out" : boxed ? ", mates boxed out" : ""}${
		rival
			? `, one of theirs up too (${
					rival.acts.find(
						(a) =>
							(a.anim === "rebound" || a.anim === "reach") &&
							!a.quiet &&
							a.t0 < t + 200 &&
							a.t1 > t - 400,
					)!.anim
				})`
			: ""
	}`;
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
	if (
		offensive &&
		next?.kind === "fly" &&
		"pid" in next.from &&
		!("pid" in next.to)
	) {
		// Back up with it: how quick, and from how far.
		const U = evalPlayer(tl, pid, next.t0);
		after = `back up${dribbled ? " off a dribble" : ""} ${heldFor < 700 ? "at once" : heldFor < 1500 ? "after a gather" : "after a while"}, ${dist(U, rimAt) < 6 ? "under it" : "from out"}`;
	} else if (
		offensive &&
		next?.kind === "fly" &&
		"pid" in next.to &&
		"pid" in next.from
	) {
		const to = tl.tracks.get(next.to.pid);
		const C = evalPlayer(tl, next.to.pid, next.t1);
		after =
			to?.team === team
				? `${dribbled ? "dribbled, then " : ""}${dist(C, rimAt) > 22 ? "kicked out to the arc" : "passed out"}${heldFor < 900 ? " at once" : ""}`
				: "lost it";
	} else if (next?.kind === "fly" && "pid" in next.to && "pid" in next.from) {
		const to = tl.tracks.get(next.to.pid);
		const L = dist(
			evalPlayer(tl, pid, next.t0),
			evalPlayer(tl, next.to.pid, next.t1),
		);
		// (Thrown how: the pass he threw it with.)
		const threw = tr.acts.find(
			(a) =>
				a.anim.startsWith("pass") &&
				a.t0 <= next.t0 + 50 &&
				a.t1 >= next.t0 - 300,
		);
		const how2 = threw?.outlet
			? "overhead"
			: threw?.anim.replace(/^pass/, "").toLowerCase() || "chest";
		// (Turned away first - a pivot off a man up on him: his body round
		// more than a right angle off the way it goes, before it goes.)
		const goes = Math.atan2(
			evalPlayer(tl, next.to.pid, next.t1).y - evalPlayer(tl, pid, next.t0).y,
			evalPlayer(tl, next.to.pid, next.t1).x - evalPlayer(tl, pid, next.t0).x,
		);
		let pivoted = false;
		for (let u = t + 200; u < next.t0 - 300; u += 100) {
			let off = evalPlayer(tl, pid, u).yaw - goes;
			off -= Math.round(off / (2 * Math.PI)) * 2 * Math.PI;
			pivoted ||= Math.abs(off) > Math.PI / 2;
		}
		after =
			to?.team === team
				? `${dribbled ? "dribbled, then " : ""}${pivoted ? "turned away, then " : ""}${L > 30 ? "long outlet" : L > 12 ? "outlet" : "handed off"}${L > 12 ? ` (${how2})` : ""}${heldFor < 900 ? " at once" : ""}`
				: "lost it";
	} else if (dribbled) {
		after = "brought it up himself";
	} else {
		after = "held it";
	}
	const pace = fastAt(tl, t + 2500) > 1.2 ? "walked up" : "pushed";
	// The pile round him a moment after he has it: how many men within 5 ft
	// of him, and how many of those stood still in it - the scrum broken up
	// or not.
	const R2 = evalPlayer(tl, pid, t + 900);
	let piled = 0;
	let stood = 0;
	for (const o of tl.tracks.values()) {
		if (o.pid < 0 || o.pid === pid) {
			continue;
		}
		const O = evalPlayer(tl, o.pid, t + 900);
		if (!O.shown || dist(O, R2) > 5) {
			continue;
		}
		piled += 1;
		const O0 = evalPlayer(tl, o.pid, t + 600);
		if (dist(O0, O) < 0.5) {
			stood += 1;
		}
	}
	return {
		pid,
		parts: {
			who: pos || "?",
			how,
			where,
			off,
			traffic,
			after: `${after}, ${pace}`,
			pile: `${piled} near, ${stood} stood`,
		},
	};
};

// A substitution: each man coming on - what stopped play, how long he had
// been down at the table, how he got out there, the man going off (to his
// chair, how quick, greeted there), whether they slapped hands or said who
// to take, and how long it all held the game up.
const subParts = (
	tl: CourtTimeline,
	events: {
		type: string;
		pid?: number;
		pids?: number[];
		pidsOff?: number[];
	}[],
	b: CourtTimeline["beats"][number],
	roster: { pid: number; team: 0 | 1 }[],
): { pid: number; parts: Record<string, string> }[] => {
	const e = events[b.i]!;
	const on = e.pids ?? [];
	const offs = e.pidsOff ?? [];
	const k = tl.beats.indexOf(b);
	let prev = "?";
	for (let q = k - 1; q >= 0; q--) {
		const x = tl.beats[q]!;
		if (x.type !== "sub") {
			prev = x.type;
			break;
		}
	}
	let next = "?";
	for (let q = k + 1; q < tl.beats.length; q++) {
		const x = tl.beats[q]!;
		if (x.type !== "sub") {
			next = x.type;
			break;
		}
	}
	const greets = greetsFor(tl, roster);
	const team = { region: "", name: "", colors: ["#000", "#fff", "#888"] } as {
		region: string;
		name: string;
		colors: [string, string, string];
	};
	const crew = crewFor(1, team, team);
	const out: { pid: number; parts: Record<string, string> }[] = [];
	on.forEach((pid, j) => {
		const tr = tl.tracks.get(pid);
		const off = offs[j];
		const otr = off === undefined ? undefined : tl.tracks.get(off);
		if (!tr) {
			return;
		}
		const ci = (tl.checkIns ?? []).find(
			(c) => c.pid === pid && Math.abs(c.t1 - b.actionStart) < 400,
		);
		const table = ci
			? `at the table ${Math.round((ci.t1 - ci.kneel) / 1000)}s`
			: "not at the table";
		const m = tr.moves.find((x) => x.t0 >= b.actionStart - 50);
		const run = m
			? `${Math.round(dist(m.from, m.to))} ft in ${Math.round((m.t1 - m.t0) / 500) / 2}s`
			: "-";
		let leave = "-";
		let greeted = "-";
		let slap = "no slap";
		let told = "nobody pointed out";
		if (otr && off !== undefined) {
			// (To his chair: from where he was to where he sat down.)
			const sd = (tl.sitDowns ?? []).find(
				(x) =>
					x.pid === off && x.t > b.actionStart && x.t < b.actionStart + 15000,
			);
			const O = evalPlayer(tl, off, b.actionStart);
			const S = sd ? evalPlayer(tl, off, sd.t - 20) : undefined;
			const ft = S ? dist(O, S) : 0;
			const sec = sd ? (sd.t - b.actionStart) / 1000 : 0;
			leave = sd
				? `${Math.round(ft)} ft at ${ft / sec > 9 ? "a run" : ft / sec > 5 ? "a jog" : "a walk"}`
				: "-";
			const g = greets.find(
				(x) => x.pid === off && sd !== undefined && Math.abs(x.t - sd.t) < 200,
			);
			greeted = g ? `greeted by ${g.greeters.length}` : "not greeted";
			const arms = (otr.arms ?? []).filter(
				(a) => a.t0 >= b.actionStart - 500 && a.t0 < b.actionStart + 6000,
			);
			if (arms.some((a) => a.kind === "slap" && a.at === pid)) {
				slap = "slap";
			}
			if (
				arms.some(
					(a) =>
						a.kind === "point" &&
						typeof a.at === "number" &&
						tl.tracks.get(a.at)?.team !== otr.team,
				)
			) {
				// (Their side to defend, as play goes on - or not: the side of
				// the next man to shoot it or lose it has it, not the side of
				// the next to take it off them, or to foul.)
				let attack: number | undefined;
				for (const x of events.slice(b.i + 1)) {
					const xt =
						typeof x.pid === "number" ? tl.tracks.get(x.pid)?.team : undefined;
					if (xt === undefined) {
						if (
							/^(period|overtime|endOfPeriod|gameOver|jumpBall)$/.test(x.type)
						) {
							break;
						}
						continue;
					}
					if (/^(fga|ft$|missFt$|tov$)/.test(x.type)) {
						attack = xt;
						break;
					}
					if (/^(stl$|pf)/.test(x.type)) {
						attack = 1 - xt;
						break;
					}
				}
				const def = attack !== otr.team;
				told = `pointed out his man${def ? "" : " - on offense!"}`;
				if (
					(tr.arms ?? []).some(
						(a) =>
							a.kind === "point" &&
							a.t0 >= b.actionStart &&
							a.t0 < b.actionStart + 6000,
					)
				) {
					told += ", got him";
				}
			}
		}
		const held = b.end - b.actionStart;
		const waved =
			ci !== undefined &&
			crewAt(tl, ci.t1 + 150, crew).states.some((x) => x.anim === "beckon");
		out.push({
			pid,
			parts: {
				after: prev,
				before: next,
				table,
				on: run,
				off: leave,
				bench: greeted,
				hands: slap,
				who: told,
				official: waved ? "waved on" : "not waved on",
				held: `${Math.round(held / 500) / 2}s`,
				many: `${on.length} on`,
			},
		});
	});
	return out;
};

// A free throw: which of his trip it is, his routine (the dribbles, the
// flip, the breath), how long he takes, how it went, what the men on the
// lane do with it in the air, the fives after - and, the last one missed,
// who gets it.
const ftParts = (
	tl: CourtTimeline,
	events: { type: string; pid?: number }[],
	b: CourtTimeline["beats"][number],
): { pid: number; parts: Record<string, string> } | undefined => {
	const pid = events[b.i]!.pid;
	if (typeof pid !== "number") {
		return undefined;
	}
	const tr = tl.tracks.get(pid)!;
	const team = tr.team;
	const rim = { x: rimX(team), y: 25 };
	// (His trip: the run of his free throws this one is in.)
	const isHis = (x: CourtTimeline["beats"][number]) =>
		(x.type === "ft" || x.type === "missFt") && events[x.i]?.pid === pid;
	const k = tl.beats.indexOf(b);
	let a = k;
	while (
		a > 0 &&
		(isHis(tl.beats[a - 1]!) || /^(sub|timeout)$/.test(tl.beats[a - 1]!.type))
	) {
		a -= 1;
	}
	let z = k;
	while (
		z + 1 < tl.beats.length &&
		(isHis(tl.beats[z + 1]!) || /^(sub|timeout)$/.test(tl.beats[z + 1]!.type))
	) {
		z += 1;
	}
	const trip = tl.beats.slice(a, z + 1).filter(isHis);
	const nth = trip.indexOf(b) + 1;
	const last = nth === trip.length;
	// Off his hand: the shot, and the catch from the official before it.
	const shot = tl.ball.find(
		(s) =>
			s.t0 >= b.preStart &&
			s.kind === "fly" &&
			"pid" in s.from &&
			s.from.pid === pid &&
			!("pid" in s.to) &&
			dist(s.to as Pt, rim) < 3,
	) as (BallSeg & { kind: "fly" }) | undefined;
	const set = tr.acts.find(
		(x) => x.anim === "setShot" && x.t0 >= b.preStart - 50,
	);
	if (!set) {
		return undefined;
	}
	const release = shot?.t0 ?? set.t0 + 400;
	// (The official's bounce pass in to him - not his own flip coming back
	// down to him.)
	const caughtSeg = tl.ball.find(
		(s) =>
			s.kind === "fly" &&
			s.t0 >= b.preStart &&
			s.t1 <= set.t0 &&
			"pid" in s.to &&
			s.to.pid === pid &&
			!("pid" in s.from),
	) as (BallSeg & { kind: "fly" }) | undefined;
	const caught = caughtSeg?.t1 ?? b.actionStart;
	// His routine: what the ball did between the catch and the shot.
	const steps: string[] = [];
	for (const s of tl.ball) {
		if (s.t0 < caught + 20 || s.t0 >= set.t0) {
			continue;
		}
		if (s.kind === "hold" && s.pid === pid && s.style !== "hold") {
			// (How many bounces, at what beat.)
			const end = tl.ball.find((x) => x.t0 > s.t0 && x !== s)?.t0 ?? set.t0;
			const rate = s.rate ?? 2.1;
			const n = Math.round(((end - s.t0) * rate) / 1000);
			steps.push(
				`${n} bounce${n === 1 ? "" : "s"}${rate < 1.95 ? " slow" : rate > 2.35 ? " quick" : ""}`,
			);
		} else if (s.kind === "fly" && "pid" in s.from && s.from.pid === pid) {
			steps.push("flip");
		}
	}
	if (
		tr.acts.some((x) => x.anim === "triple" && x.t0 >= caught && x.t0 < set.t0)
	) {
		steps.unshift("sits");
	}
	const routine = steps.length > 0 ? steps.join("+") : "straight up";
	const took = `${Math.round((set.t0 - caught) / 500) / 2}s`;
	// How it went.
	const path = tl.ball.find(
		(s) => s.kind === "path" && s.t0 >= release - 5 && s.t0 < release + 2500,
	);
	let ending = b.type === "ft" ? "in" : "out";
	if (path?.kind === "path") {
		const p0 = { x: path.pts[0]!, y: path.pts[1]!, z: path.pts[2]! };
		const side = Math.abs(p0.x - rimX(0)) < Math.abs(p0.x - rimX(1)) ? 0 : 1;
		ending = kindOf(playShot(side, p0, path.v0));
	}
	// The men on the lane, the ball in the air.
	const lane = new Map<string, number>();
	for (const o of tl.tracks.values()) {
		if (o.pid < 0 || o.pid === pid) {
			continue;
		}
		const O = evalPlayer(tl, o.pid, release + 300);
		if (!O.shown || dist(O, rim) > 14) {
			continue;
		}
		const what = O.anim === "run" || O.anim === "walk" ? "on the move" : O.anim;
		lane.set(what, (lane.get(what) ?? 0) + 1);
	}
	const laneText = [...lane]
		.sort((x, y) => y[1] - x[1])
		.map(([w, n]) => `${n} ${w}`)
		.join(", ");
	const fives = tr.acts.filter(
		(x) => x.anim === "lowFive" && x.t0 > release && x.t0 < release + 4000,
	).length;
	// The last one missed: who gets it.
	let after = "-";
	if (last && b.type === "missFt") {
		const got = tl.ball.find(
			(s) => s.kind === "hold" && s.t0 > release && s.pid >= 0,
		) as (BallSeg & { kind: "hold" }) | undefined;
		after = got
			? tl.tracks.get(got.pid)?.team === team
				? "offense"
				: "defense"
			: "?";
	}
	return {
		pid,
		parts: {
			which: `${nth} of ${trip.length}`,
			routine,
			took,
			stroke: `${Math.round((set.t1 - set.t0) / 100) / 10}s`,
			ending,
			lane: laneText || "nobody",
			fives: `${fives} fives`,
			after,
		},
	};
};

// A foul away from a shot: how it came about (the call), on the ball or
// off it, where on the floor, what the man fouled and the man who fouled
// him do then, what the officials signal, what the others do with the
// whistle gone, the ball after it, and how long till play goes on.
const foulParts = (
	tl: CourtTimeline,
	events: { type: string; pid?: number }[],
	b: CourtTimeline["beats"][number],
): { pid: number; parts: Record<string, string> } | undefined => {
	const fouler = events[b.i]!.pid;
	if (typeof fouler !== "number") {
		return undefined;
	}
	const ftr = tl.tracks.get(fouler)!;
	const whistle = tl.fx.find(
		(f) =>
			f.kind === "whistle" &&
			f.call === "foul" &&
			Math.abs(f.t - b.actionStart) < 50,
	);
	if (!whistle) {
		return undefined;
	}
	const hit = whistle.t;
	const team = ftr.team === 0 ? 1 : 0;
	const rim = { x: rimX(team), y: 25 };
	// Who he fouled: the man of theirs nearest where it was called.
	let victim: number | undefined;
	let best = Infinity;
	for (const o of tl.tracks.values()) {
		if (o.pid < 0 || o.team !== team) {
			continue;
		}
		const O = evalPlayer(tl, o.pid, hit);
		const d = whistle.at ? dist(O, whistle.at) : Infinity;
		if (O.shown && d < best) {
			best = d;
			victim = o.pid;
		}
	}
	const holderSeg = tl.ball.findLast((s) => s.t0 <= hit);
	const onBall =
		holderSeg?.kind === "hold" && holderSeg.pid === victim
			? "on the ball"
			: "off the ball";
	const V = victim === undefined ? undefined : evalPlayer(tl, victim, hit);
	const where = V
		? dist(V, rim) < 10
			? "at the rim"
			: dist(V, rim) < 22
				? "in the half court"
				: (V.x - 47) * (rim.x - 47) > 0
					? "out top"
					: "in the backcourt"
		: "?";
	const did = (pid: number | undefined) => {
		if (pid === undefined) {
			return "?";
		}
		const tr = tl.tracks.get(pid)!;
		const acts = tr.acts
			.filter((a) => a.t0 >= hit - 300 && a.t0 < hit + 2500)
			.map((a) => a.anim as string);
		const arms = (tr.arms ?? [])
			.filter((g) => g.t0 >= hit - 300 && g.t0 < hit + 2500)
			.map((g) => `arm ${g.kind}`);
		const all = [...new Set([...acts, ...arms])];
		return all.length > 0 ? all.join("+") : "nothing";
	};
	// The others, a moment after the whistle: still going, or stopped.
	let going = 0;
	let stopped = 0;
	for (const o of tl.tracks.values()) {
		if (o.pid < 0 || o.pid === fouler || o.pid === victim) {
			continue;
		}
		const A = evalPlayer(tl, o.pid, hit + 1200);
		const B = evalPlayer(tl, o.pid, hit + 1500);
		if (!A.shown) {
			continue;
		}
		if (dist(A, B) / 0.3 > 4) {
			going += 1;
		} else {
			stopped += 1;
		}
	}
	// The ball after it: where it goes.
	const after = tl.ball.filter((s) => s.t0 > hit && s.t0 < hit + 4000);
	const firstAway = after.find(
		(s) => s.kind === "fly" || s.kind === "bounce" || s.kind === "rest",
	);
	const ball =
		firstAway === undefined
			? "kept"
			: firstAway.kind === "fly"
				? "pid" in firstAway.to
					? firstAway.to.pid < 0
						? "to an official"
						: "to a player"
					: "tossed down"
				: "dropped";
	const k = tl.beats.indexOf(b);
	const next = tl.beats.slice(k + 1).find((x) => x.type !== "sub");
	// Reported to the table by an official: within a few seconds of it.
	const team0 = { region: "", name: "", colors: ["#000", "#fff", "#888"] } as {
		region: string;
		name: string;
		colors: [string, string, string];
	};
	const crew = crewFor(1, team0, team0);
	let report = "not reported";
	for (let t = hit + 2000; t < hit + 9000; t += 250) {
		if (crewAt(tl, t, crew).states.some((x) => x.anim === "report")) {
			report = `reported in ${Math.round((t - hit) / 1000)}s`;
			break;
		}
	}
	return {
		pid: fouler,
		parts: {
			call: whistle.how ?? "?",
			ball: onBall,
			where,
			fouled: did(victim),
			fouler: did(fouler),
			others: `${going} going, ${stopped} stopped`,
			official: report,
			after: ball,
			next: next
				? `${next.type} in ${Math.round((next.actionStart - hit) / 1000)}s`
				: "-",
		},
	};
};

// A shooting foul: the shot he was fouled on, the contact, the shooter
// and the man who fouled him after it, the officials' signals, and the
// time till the first free throw.
const shootingFoulParts = (
	tl: CourtTimeline,
	events: { type: string; pid?: number; pidShooting?: number }[],
	b: CourtTimeline["beats"][number],
): { pid: number; parts: Record<string, string> } | undefined => {
	const fouler = events[b.i]!.pid;
	if (typeof fouler !== "number") {
		return undefined;
	}
	const k = tl.beats.indexOf(b);
	const fga = tl.beats.slice(0, k).findLast((x) => /^fga/.test(x.type));
	const shooter = fga ? events[fga.i]?.pid : undefined;
	if (typeof shooter !== "number") {
		return undefined;
	}
	const shot = tl.ball.findLast(
		(z) =>
			z.kind === "fly" &&
			"pid" in z.from &&
			z.from.pid === shooter &&
			!("pid" in z.to) &&
			z.t0 <= b.actionStart + 50,
	);
	if (shot?.kind !== "fly") {
		return undefined;
	}
	const release = shot.t0;
	const acts = (pid: number, from: number, to: number) => {
		const tr = tl.tracks.get(pid)!;
		const all = [
			...tr.acts
				.filter((a) => a.t0 >= from && a.t0 < to)
				.map((a) => a.anim as string),
			...(tr.arms ?? [])
				.filter((g) => g.t0 >= from && g.t0 < to)
				.map((g) => `arm ${g.kind}`),
		];
		return all.length > 0 ? [...new Set(all)].join("+") : "nothing";
	};
	const team0 = { region: "", name: "", colors: ["#000", "#fff", "#888"] } as {
		region: string;
		name: string;
		colors: [string, string, string];
	};
	const crew = crewFor(1, team0, team0);
	// The calling official's signals, in order.
	const seen: string[] = [];
	for (let t = b.actionStart; t < b.actionStart + 6000; t += 100) {
		for (const r of crewAt(tl, t, crew).states) {
			if (
				r.pid < 0 &&
				r.pid >= -3 &&
				/^(signal|report|threeUp|beckon)/.test(r.anim) &&
				seen.at(-1) !== r.anim
			) {
				seen.push(r.anim);
			}
		}
	}
	const firstFt = tl.beats
		.slice(k + 1)
		.find((x) => x.type === "ft" || x.type === "missFt");
	const S = evalPlayer(tl, shooter, release);
	const team = tl.tracks.get(shooter)!.team;
	const rim = { x: rimX(team), y: 25 };
	return {
		pid: shooter,
		parts: {
			shot: `${fga!.type.replace(/^fga/, "")}, ${Math.round(dist(S, rim))} ft`,
			contact: acts(fouler, release - 600, release + 300),
			shooter: acts(shooter, release + 100, release + 3000),
			fouler: acts(fouler, release + 300, release + 3000),
			signals: seen.join(" > ") || "none",
			line: firstFt
				? `to the line in ${Math.round((firstFt.actionStart - b.actionStart) / 1000)}s`
				: "-",
		},
	};
};

// A turnover: how it was lost (a pass picked off, the ball poked away, a
// whistle for a travel or a charge, thrown out of bounds), where, the man
// who lost it after (back after the man who took it, a reaction), the man
// who took it (off up the floor with it, or held), the officials' signals,
// and what came of it next.
const tovParts = (
	tl: CourtTimeline,
	events: { type: string; pid?: number; pidTov?: number }[],
	b: CourtTimeline["beats"][number],
): { pid: number; parts: Record<string, string> } | undefined => {
	const e = events[b.i]!;
	const stl = e.type === "stl";
	const victim = stl ? e.pidTov : e.pid;
	if (typeof victim !== "number") {
		return undefined;
	}
	const vtr = tl.tracks.get(victim)!;
	const team = vtr.team;
	const rim = { x: rimX(team), y: 25 };
	const T = b.actionStart;
	const whistle = tl.fx.find(
		(f) =>
			f.kind === "whistle" && f.call !== undefined && Math.abs(f.t - T) < 1500,
	);
	let how: string;
	if (stl) {
		const taker = e.pid as number;
		const got = tl.ball.find(
			(z) => z.kind === "hold" && z.pid === taker && z.t0 >= T - 1500,
		);
		const k = got ? tl.ball.indexOf(got) : -1;
		const before = k > 0 ? tl.ball[k - 1] : undefined;
		how =
			before?.kind === "fly" &&
			"pid" in before.from &&
			before.from.pid === victim
				? "pass picked off"
				: before?.kind === "fly" || before?.kind === "bounce"
					? "poked loose, picked up"
					: "stripped";
	} else {
		how = whistle?.call ?? "no whistle";
	}
	const V = evalPlayer(tl, victim, T);
	const where =
		dist(V, rim) < 10
			? "at the rim"
			: dist(V, rim) < 24
				? "in the half court"
				: (V.x - 47) * (rim.x - 47) > 0
					? "out top"
					: "in the backcourt";
	const acts = (pid: number, from: number, to: number) => {
		const tr = tl.tracks.get(pid)!;
		const all = [
			...tr.acts
				.filter((a) => a.t0 >= from && a.t0 < to)
				.map((a) => a.anim as string),
			...(tr.arms ?? [])
				.filter((g) => g.t0 >= from && g.t0 < to)
				.map((g) => `arm ${g.kind}`),
		];
		return all.length > 0 ? [...new Set(all)].join("+") : "nothing";
	};
	// After it: the man who lost it - back the other way, or stood.
	const V1 = evalPlayer(tl, victim, T + 1500);
	const back = (V1.x - V.x) * (rim.x - 47) < -3 ? "got back" : "stayed";
	let taker = "-";
	if (stl) {
		const S0 = evalPlayer(tl, e.pid as number, T + 300);
		const S1 = evalPlayer(tl, e.pid as number, T + 1800);
		taker =
			(S1.x - S0.x) * (47 - rim.x) > 6
				? "off up the floor"
				: dist(S0, S1) > 3
					? "on the move"
					: "held";
	}
	const team0 = { region: "", name: "", colors: ["#000", "#fff", "#888"] } as {
		region: string;
		name: string;
		colors: [string, string, string];
	};
	const crew = crewFor(1, team0, team0);
	const seen: string[] = [];
	for (let t = T; t < T + 4000; t += 100) {
		for (const r of crewAt(tl, t, crew).states) {
			if (
				r.pid < 0 &&
				r.pid >= -3 &&
				/^(signal|report|travel)/.test(r.anim) &&
				seen.at(-1) !== r.anim
			) {
				seen.push(r.anim);
			}
		}
	}
	const k = tl.beats.indexOf(b);
	const next = tl.beats.slice(k + 1).find((x) => x.type !== "sub");
	return {
		pid: victim,
		parts: {
			how,
			where,
			lost: `${acts(victim, T - 200, T + 2500)}, ${back}`,
			taker,
			signals: seen.join(" > ") || "none",
			next: next
				? `${next.type} in ${Math.round((next.actionStart - T) / 1000)}s`
				: "-",
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
			const roster = players.map((p: { pid: number; tid: number }) => ({
				pid: p.pid,
				team: tl.tracks.get(p.pid)?.team ?? 0,
			}));
			for (const b of tl.beats) {
				if (b.type !== line) {
					continue;
				}
				if (line === "tov" || line === "stl") {
					if (fastAt(tl, b.actionStart) > 1.2) {
						fastOnes += 1;
						continue;
					}
					const got = tovParts(tl, events, b);
					if (got) {
						all.push({ pid: `${tag}:${got.pid}`, parts: got.parts });
					}
					continue;
				}
				if (line === "pfFG" || line === "pfTP") {
					if (fastAt(tl, b.actionStart) > 1.2) {
						fastOnes += 1;
						continue;
					}
					const got = shootingFoulParts(tl, events, b);
					if (got) {
						all.push({ pid: `${tag}:${got.pid}`, parts: got.parts });
					}
					continue;
				}
				if (line === "pfNonShooting" || line === "pfBonus") {
					if (fastAt(tl, b.actionStart) > 1.2) {
						fastOnes += 1;
						continue;
					}
					const got = foulParts(tl, events, b);
					if (got) {
						all.push({ pid: `${tag}:${got.pid}`, parts: got.parts });
					}
					continue;
				}
				if (line === "ft" || line === "missFt") {
					if (fastAt(tl, b.actionStart) > 1.2) {
						fastOnes += 1;
						continue;
					}
					const got = ftParts(tl, events, b);
					if (got) {
						all.push({ pid: `${tag}:${got.pid}`, parts: got.parts });
					}
					continue;
				}
				if (line === "sub") {
					if (fastAt(tl, b.actionStart) > 1.2) {
						fastOnes += 1;
						continue;
					}
					for (const got of subParts(tl, events, b, roster)) {
						all.push({ pid: `${tag}:${got.pid}`, parts: got.parts });
					}
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
						line === "orb",
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
				const got = jumperParts(
					tl,
					shot,
					/^(fg|tp)/.test(line),
					/LowPost/.test(line),
				);
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
