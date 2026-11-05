import { assert, beforeAll, describe, test } from "vitest";
import {
	compileField,
	type BallSeg,
	type FieldTimeline,
	type Part,
	type Play,
} from "./director.ts";
import { crowdAt } from "./crowd.ts";
import { momentAt } from "./evaluate.ts";
import {
	CROSSBAR_Z,
	dist,
	FIELD_L,
	MID_Y,
	POST_W,
	YD,
	type Pt,
} from "./geometry.ts";
import { simGames, type SimGame } from "./testGames.ts";

let games: SimGame[] = [];
let staged: FieldTimeline[] = [];

beforeAll(async () => {
	games = [
		...(await simGames([41, 42])),
		// (One with a lot of onside kicks, and a lot of them got back.)
		...(await simGames([93, 94], {
			onsideFactor: 900,
			onsideRecoveryFactor: 4,
		})),
		// (And a lot of fumbles.)
		...(await simGames([45, 46], { fumbleFactor: 60 })),
	];
	staged = games.map((g, k) =>
		compileField({ events: g.events, players: g.roster, gid: k }),
	);
}, 120_000);

// A game of its own, flags all over it - staged once, for the tests that
// want one.
let fouledGame: Promise<{ game: SimGame; tl: FieldTimeline }> | undefined;
const fouled = () =>
	(fouledGame ??= simGames([47], { foulRateFactor: 30 }).then(([game]) => ({
		game: game!,
		tl: compileField({ events: game!.events, players: game!.roster, gid: 0 }),
	})));

const ACTED = new Set([
	"pass",
	"run",
	"kneel",
	"kickoff",
	"punt",
	"fieldGoal",
	"extraPoint",
	"onside",
]);

// Each play with the events it is made of.
const eachPlay = (
	f: (play: Play, seg: SimGame["events"], tl: FieldTimeline) => void,
) => {
	staged.forEach((tl, k) => {
		const events = games[k]!.events;
		tl.plays.forEach((play, j) => {
			const end = tl.plays[j + 1]?.i ?? events.length;
			f(play, events.slice(play.i, end), tl);
		});
	});
};

const posAt = (part: Part, t: number): Pt => {
	const l = part.legs.findLast((x) => x.t0 <= t) ?? part.legs[0]!;
	const u =
		l.t1 > l.t0 ? Math.min(1, Math.max(0, (t - l.t0) / (l.t1 - l.t0))) : 1;
	return {
		x: l.from.x + (l.to.x - l.from.x) * u,
		y: l.from.y + (l.to.y - l.from.y) * u,
	};
};

describe("the plays acted out", () => {
	test("every man goes from one place to the next, at a man's speed", () => {
		eachPlay((play, _, tl) => {
			for (const part of play.parts) {
				part.legs.forEach((l, k) => {
					const prev = part.legs[k - 1];
					if (prev) {
						assert.closeTo(l.t0, prev.t1, 1e-6, `${part.role} at ${l.t0}`);
						// (Somewhere else only as the picture cuts away: the coin
						// toss done, set for the kick.)
						if (!tl.cuts.includes(l.t0)) {
							assert.isBelow(dist(l.from, prev.to), 1e-6, `${part.role} jumps`);
						}
					}
					const secs = (l.t1 - l.t0) / 1000;
					if (secs > 0.05) {
						assert.isAtMost(
							dist(l.from, l.to) / secs,
							34,
							`${part.role} in ${l.move} on play ${play.i}`,
						);
					}
				});
			}
		});
	}, 120_000);

	test("the ball always somewhere: thrown to where a man catches it, handed from hand to hand", () => {
		eachPlay((play) => {
			if (!ACTED.has(play.kind)) {
				return;
			}
			const segs = play.ball;
			const part = (pid: number) => play.parts.find((p) => p.pid === pid)!;
			let at: Pt | undefined;
			for (const [k, s] of segs.entries()) {
				if (k > 0) {
					assert.isAtLeast(s.t0, segs[k - 1]!.t0, `play ${play.i}`);
				}
				if (s.kind === "held") {
					assert.isDefined(
						play.parts.find((p) => p.pid === s.pid),
						`play ${play.i}`,
					);
					const p = posAt(part(s.pid), s.t0);
					if (at && s.t0 > play.snap) {
						// Caught where it came down; taken from the hands that
						// had it.
						assert.isBelow(dist(p, at), 7, `${play.kind} on play ${play.i}`);
					}
					const next = segs[k + 1];
					at = posAt(part(s.pid), next ? next.t0 : s.t0);
				} else if (s.kind === "fly") {
					if (at && s.t0 > play.snap) {
						assert.isBelow(
							dist(s.from, at),
							7,
							`thrown from on play ${play.i}`,
						);
					}
					at = s.to;
				} else {
					at = s.at;
				}
			}
		});
	}, 20_000);

	test("the carrier brought down where the sim says, when the line says", () => {
		let checked = 0;
		eachPlay((play, seg, tl) => {
			if (!ACTED.has(play.kind)) {
				return;
			}
			// (A fumble, a penalty: elsewhere.)
			if (
				seg.some(
					(e) =>
						e.type === "fumble" || e.type === "penalty" || e.type === "flag",
				)
			) {
				return;
			}
			const e = seg.find(
				(x) =>
					(x.type === "run" ||
						x.type === "passComplete" ||
						x.type === "sack") &&
					!x.td &&
					!x.safety,
			);
			if (!e || typeof e.yds !== "number") {
				return;
			}
			const beat = tl.beats.find((b) => b.i === play.i + seg.indexOf(e));
			assert.isDefined(beat);
			const holder = [...play.ball]
				.filter(
					(b): b is Extract<BallSeg, { kind: "held" }> => b.kind === "held",
				)
				.findLast((b) => b.t0 <= beat!.actionStart);
			assert.isDefined(holder, `play ${play.i}`);
			const m = momentAt(tl, beat!.actionStart);
			const man = m.players.find((p) => p.id === holder!.pid)!;
			const d = (man.x - play.losX) * play.dir;
			const want = Math.max(e.yds * YD, e.type === "sack" ? -40 : -Infinity);
			if (e.type === "sack") {
				assert.closeTo(d, Math.min(-2, want), 3.5, `sack on play ${play.i}`);
			} else {
				// (A little more for a man still going down forward with it.)
				assert.closeTo(d, want, 4, `${e.type} for ${e.yds} on play ${play.i}`);
			}
			checked += 1;
		});
		assert.isAbove(checked, 50);
		// (The first to have every play's men laid out.)
	}, 120_000);

	test("the man who gets home on the quarterback off his block with a swim or a rip, the arm on the blocker's side", () => {
		let sacks = 0;
		let moves = 0;
		eachPlay((play, seg) => {
			if (play.kind !== "pass" || !seg.some((e) => e.type === "sack")) {
				return;
			}
			sacks += 1;
			for (const part of play.parts) {
				// (Or a spin off him instead: see below.)
				if (part.legs.some((l) => l.move === "spin")) {
					moves += 1;
					continue;
				}
				const ls = part.legs.filter((l) =>
					/^(swim|rip)[LR]$/.test(l.arms ?? ""),
				);
				if (ls.length === 0) {
					continue;
				}
				moves += 1;
				assert.notStrictEqual(part.team, play.offense, `play ${play.i}`);
				const t0 = ls[0]!.t0;
				const t1 = ls.at(-1)!.t1;
				assert.isAtLeast(t1 - t0, 370, `play ${play.i}`);
				// (Off his block in the middle of it, onto the quarterback.)
				const off = part.legs.findLast(
					(l) => l.move === "engage" && l.t1 <= t1,
				);
				assert.isDefined(off, `play ${play.i}`);
				assert.isAbove(off!.t1, t0, `play ${play.i}`);
				// (The arm on the side of him the nearest of the other side is
				// as he goes by.)
				const at = posAt(part, off!.t1);
				const ahead = posAt(part, off!.t1 + 150);
				const way = Math.atan2(ahead.y - at.y, ahead.x - at.x);
				const foe = play.parts
					.filter((p) => p.team === play.offense && p.role !== "QB")
					.map((p) => posAt(p, off!.t1))
					.sort((a, b) => dist(a, at) - dist(b, at))[0]!;
				const left = Math.sin(Math.atan2(foe.y - at.y, foe.x - at.x) - way) > 0;
				assert.strictEqual(ls[0]!.arms!.endsWith("L"), left, `play ${play.i}`);
			}
		});
		// (Not a man come free with nobody on him.)
		assert.isAbove(sacks, 3);
		assert.isAbove(moves, sacks / 3);
	}, 120_000);

	test("a pass rusher now and then spins off his block instead: round once, his feet going on, straight off the block and onto a run", () => {
		let spins = 0;
		eachPlay((play) => {
			if (play.kind !== "pass") {
				return;
			}
			for (const part of play.parts) {
				part.legs.forEach((l, k) => {
					const before = part.legs[k - 1];
					if (l.move !== "spin" || before?.move !== "engage") {
						return;
					}
					spins += 1;
					assert.notStrictEqual(part.team, play.offense, `play ${play.i}`);
					assert.strictEqual(before.t1, l.t0, `play ${play.i}`);
					// (In one piece or cut in two: round once, all told.)
					let n = k;
					let turn = 0;
					while (part.legs[n]?.move === "spin") {
						turn += part.legs[n]!.turn ?? 0;
						n += 1;
					}
					const last = part.legs[n - 1]!;
					assert.closeTo(Math.abs(turn), Math.PI * 2, 1e-9, `play ${play.i}`);
					assert.isAtLeast(dist(l.from, last.to), 2, `play ${play.i}`);
					const after = part.legs[n];
					assert.isDefined(after, `play ${play.i}`);
					assert.strictEqual(after!.t0, last.t1, `play ${play.i}`);
					assert.notStrictEqual(after!.move, "stand", `play ${play.i}`);
				});
			}
		});
		assert.isAtLeast(spins, 2);
	}, 120_000);

	test("a twist on the line now and then: an end a step up the field and round under the man inside him, who slants out ahead of him - never in each other's way", () => {
		let twists = 0;
		eachPlay((play) => {
			if (play.kind !== "pass") {
				return;
			}
			const center = play.parts.find((p) => p.role === "C");
			if (!center) {
				return;
			}
			const mid = posAt(center, play.snap).y;
			const off = (part: Part, t: number) => Math.abs(posAt(part, t).y - mid);
			const line = play.parts.filter(
				(p) => p.team !== play.offense && ["DE", "DT", "NT"].includes(p.role),
			);
			for (const end of line) {
				if (end.role !== "DE") {
					continue;
				}
				// (Stepping across, before he is on anybody.)
				const k = end.legs.findIndex(
					(l) =>
						l.t0 > play.snap &&
						l.t0 < play.snap + 1500 &&
						(l.move === "slide" || l.move === "shuffle"),
				);
				const hit = end.legs.find((l) => l.move === "engage");
				if (k < 0 || !hit || hit.t0 < end.legs[k]!.t1) {
					continue;
				}
				const side = Math.sign(posAt(end, play.snap).y - mid);
				const first = line.find(
					(p) =>
						p !== end &&
						Math.sign(posAt(p, play.snap).y - mid) === side &&
						off(p, play.snap) < off(end, play.snap) &&
						off(p, hit.t0) > off(p, play.snap) + 2,
				);
				if (!first) {
					continue;
				}
				twists += 1;
				// (Up the field first: off the ball, not across it.)
				const jab = end.legs[k]!.from;
				assert.isAtLeast(
					Math.abs(jab.x - posAt(end, play.snap).x),
					1.5,
					`play ${play.i}`,
				);
				// (In on a man inside where he started.)
				assert.isBelow(
					off(end, hit.t0),
					off(end, play.snap) - 2,
					`play ${play.i}`,
				);
				for (let t = play.snap; t <= hit.t0; t += 50) {
					assert.isAtLeast(
						dist(posAt(end, t), posAt(first, t)),
						2.5,
						`play ${play.i} at ${t - play.snap}`,
					);
				}
			}
		});
		assert.isAtLeast(twists, 2);
	}, 120_000);

	test("a pancake now and then on a run: a lineman driven off his feet onto his back off his block, down a while and up again, with nobody going over him", () => {
		let pancakes = 0;
		eachPlay((play, seg) => {
			if (play.kind !== "run") {
				return;
			}
			for (const part of play.parts) {
				if (
					part.team === play.offense ||
					!["DE", "DT", "NT"].includes(part.role)
				) {
					continue;
				}
				const k = part.legs.findIndex(
					(l, j) =>
						l.move === "tackledBack" && part.legs[j - 1]?.move === "engage",
				);
				if (k < 0) {
					continue;
				}
				pancakes += 1;
				const fall = part.legs[k]!;
				const down = part.legs[k + 1];
				assert.strictEqual(down?.move, "supine", `play ${play.i}`);
				// (Or, the sim has him hurt on it, down through it: see
				// hurt.ts.)
				const hurt = seg.some(
					(e) => e.type === "injury" && e.injuredPID === part.pid,
				);
				assert.strictEqual(
					part.legs[k + 2]?.move,
					hurt ? "hurt" : "getUpBack",
					`play ${play.i}`,
				);
				// (Nobody else on his feet over him while he is down.)
				for (let t = fall.t1; t <= down!.t1; t += 50) {
					for (const other of play.parts) {
						if (other === part || other.pid < 0) {
							continue;
						}
						assert.isAtLeast(
							dist(posAt(other, t), down!.to),
							1.2,
							`play ${play.i} at ${t - play.snap}`,
						);
					}
				}
			}
		});
		assert.isAtLeast(pancakes, 2);
	}, 120_000);

	test("the quarterback hit now and then as he lets it go: the ball gone first, a man of the other side's shove, down, and up again, a hand up now and then (or, hurt, not)", () => {
		let passes = 0;
		let hits = 0;
		let helped = 0;
		eachPlay((play, seg) => {
			if (
				play.kind !== "pass" ||
				!seg.some(
					(e) => e.type === "passComplete" || e.type === "passIncomplete",
				)
			) {
				return;
			}
			const thrown = play.ball.find(
				(s) => s.kind === "fly" && s.t0 > play.snap,
			);
			const qb = play.parts.find(
				(p) => p.role === "QB" && p.team === play.offense,
			);
			if (!thrown || !qb) {
				return;
			}
			passes += 1;
			const k = qb.legs.findIndex(
				(l) =>
					(l.move === "tackledBack" || l.move === "tackled") &&
					l.t0 >= thrown.t0 &&
					l.t0 < thrown.t0 + 500,
			);
			if (k < 0) {
				return;
			}
			hits += 1;
			const t = qb.legs[k]!.t0;
			assert.isAbove(t, thrown.t0 + 100, `play ${play.i}`);
			const by = play.parts.filter(
				(p) =>
					p.team !== play.offense &&
					p.legs.some((l) => l.arms === "shove" && l.t0 <= t && l.t1 >= t) &&
					dist(posAt(p, t), posAt(qb, t)) < 3,
			);
			assert.lengthOf(by, 1, `play ${play.i}`);
			assert.include(
				["supine", "prone"],
				qb.legs[k + 1]?.move,
				`play ${play.i}`,
			);
			// (Hurt on it, as the sim says: down there till they see to him.)
			const hurt = seg.some(
				(e) => e.type === "injury" && e.injuredPID === qb.pid,
			);
			assert.include(
				hurt ? ["hurt", "hurtProne"] : ["getUpBack", "getUp"],
				qb.legs[k + 2]?.move,
				`play ${play.i}`,
			);
			// (A hand up, now and then - from one of his own, or the man who
			// hit him: there beside him as he gets up.)
			const up = qb.legs[k + 2]!;
			for (const p of play.parts) {
				const l = p.legs.find(
					(x) => x.move === "helpUp" && x.t0 <= up.t0 && x.t1 >= up.t0,
				);
				// (Not a hand up to somebody else then: the man who caught it,
				// down the field.)
				if (!l || hurt || dist(posAt(p, up.t0), posAt(qb, up.t0)) > 10) {
					continue;
				}
				helped += 1;
				assert.isTrue(p.team === play.offense || p === by[0], `play ${play.i}`);
				assert.isBelow(
					dist(posAt(p, up.t0), posAt(qb, up.t0)),
					4,
					`play ${play.i}`,
				);
			}
		});
		assert.isAbove(passes, 40);
		assert.isAbove(hits, 0);
		assert.isAbove(helped, 0);
		assert.isBelow(hits, passes / 5);
	}, 20_000);

	test("a man flagged for taunting at it after the whistle: at a man of the other side, pointing down at him or flexing, the flag coming in on it", async () => {
		const { game, tl } = await fouled();
		let flagged = 0;
		let acted = 0;
		tl.plays.forEach((play, j) => {
			const seg = game.events.slice(
				play.i,
				tl.plays[j + 1]?.i ?? game.events.length,
			);
			const k = seg.findIndex(
				(e) => e.type === "penalty" && e.penaltyName === "Taunting",
			);
			if (k < 0) {
				return;
			}
			const pid = seg
				.slice(k)
				.find((e) => e.type === "stat" && e.s === "pen")?.pid;
			const him = play.parts.find((p) => p.pid === pid);
			if (!him) {
				return;
			}
			flagged += 1;
			const l = him.legs.find(
				(x) =>
					(x.move === "taunt" || x.move === "flex") && x.t0 >= play.whistle,
			);
			if (!l) {
				return;
			}
			acted += 1;
			assert.isAtMost(l.t0, play.whistle + 2300, `play ${play.i}`);
			// (Him right there, at it.)
			const mid = (l.t0 + l.t1) / 2;
			assert.isTrue(
				play.parts.some(
					(p) => p.team !== him.team && dist(posAt(p, mid), l.to) < 4,
				),
				`play ${play.i}`,
			);
			assert.isTrue(
				play.flags.some((f) => f.t0 >= l.t0 - 500 && f.t0 <= l.t1),
				`play ${play.i}`,
			);
		});
		assert.isAbove(acted, 0);
		assert.isAtLeast(acted, flagged / 2);
	}, 60_000);

	test("unsportsmanlike before the snap: the man flagged up in the face of a man of the other side, a shove, the flag on it", async () => {
		const { game, tl } = await fouled();
		let flagged = 0;
		let acted = 0;
		tl.plays.forEach((play, j) => {
			const seg = game.events.slice(
				play.i,
				tl.plays[j + 1]?.i ?? game.events.length,
			);
			const k = seg.findIndex(
				(e) =>
					e.type === "penalty" && e.penaltyName === "Unsportsmanlike conduct",
			);
			if (k < 0 || seg.some((e) => e.type === "penalty" && e !== seg[k])) {
				return;
			}
			const pid = seg
				.slice(k)
				.find((e) => e.type === "stat" && e.s === "pen")?.pid;
			const him = play.parts.find((p) => p.pid === pid);
			if (!him) {
				return;
			}
			flagged += 1;
			const l = him.legs.find((x) => x.arms === "shove");
			if (!l) {
				return;
			}
			acted += 1;
			const flag = play.flags[0];
			assert.isDefined(flag, `play ${play.i}`);
			assert.isBelow(l.t0, flag!.t0, `play ${play.i}`);
			assert.isAbove(l.t1, flag!.t0 - 800, `play ${play.i}`);
			const t = (l.t0 + l.t1) / 2;
			assert.isTrue(
				play.parts.some(
					(p) => p.team !== him.team && dist(posAt(p, t), posAt(him, t)) < 3.4,
				),
				`play ${play.i}`,
			);
		});
		assert.isAbove(acted, 0);
		assert.isAtLeast(acted, flagged / 3);
	}, 60_000);

	test("offsides, the play run: the man flagged up out of his stance and a step across a moment before the snap, on into the play from there", async () => {
		const { game, tl } = await fouled();
		let flagged = 0;
		let acted = 0;
		tl.plays.forEach((play, j) => {
			const seg = game.events.slice(
				play.i,
				tl.plays[j + 1]?.i ?? game.events.length,
			);
			const k = seg.findIndex(
				(e) => e.type === "penalty" && e.penaltyName === "Offsides",
			);
			if (k < 0) {
				return;
			}
			const pid = seg
				.slice(k)
				.find((e) => e.type === "stat" && e.s === "pen")?.pid;
			const him = play.parts.find((p) => p.pid === pid);
			if (!him || him.team === play.offense) {
				return;
			}
			flagged += 1;
			const l = him.legs.find(
				(x) => x.move === "drive" && x.t0 < play.snap && x.t1 > play.snap,
			);
			if (!l) {
				return;
			}
			acted += 1;
			assert.isAtLeast(l.t0, play.snap - 320, `play ${play.i}`);
			assert.isAtMost(l.t0, play.snap - 160, `play ${play.i}`);
			// (A step on toward the ball - and on from there, not back.)
			const off = (t: number) => (posAt(him, t).x - play.losX) * play.dir;
			assert.approximately(off(l.t0) - off(l.t1), 1, 0.05, `play ${play.i}`);
			assert.isAtMost(off(l.t1 + 100), off(l.t1) + 0.05, `play ${play.i}`);
			assert.isTrue(
				play.flags.some((f) => f.t0 > l.t0),
				`play ${play.i}`,
			);
		});
		assert.isAbove(acted, 0);
		assert.isAtLeast(acted, flagged / 3);
	}, 60_000);

	test("a man hurt with no part in it the sim names - out on the run away from the ball, or in a block: pulled up lame, or down in it, before the whistle, and there", () => {
		let lame = 0;
		eachPlay((play, seg) => {
			for (const e of seg) {
				if (e.type !== "injury" || typeof e.injuredPID !== "number") {
					continue;
				}
				const him = play.parts.find((p) => p.pid === e.injuredPID);
				const k =
					him?.legs.findIndex(
						(l) =>
							(l.move === "tackled" || l.move === "tackledBack") &&
							l.t0 < play.whistle,
					) ?? -1;
				if (!him || k < 0 || him.role === "QB") {
					continue;
				}
				const named = seg.some((x) => x.type === "stat" && x.pid === him.pid);
				const held = play.ball.some(
					(b) => b.kind === "held" && b.pid === him.pid,
				);
				const fell = him.legs[k]!;
				// (Brought down in the play itself - not this.)
				if (named || held) {
					continue;
				}
				lame += 1;
				// (Pulled up first, slowing over his last steps on his feet - or
				// in a block.)
				const before = him.legs[k - 1]!;
				const pace =
					Math.hypot(before.to.x - before.from.x, before.to.y - before.from.y) /
					((before.t1 - before.t0) / 1000);
				assert.isBelow(pace, 8, `play ${play.i}`);
				// (And on the turf, there, till they see to him.)
				for (const l of him.legs.slice(k + 1)) {
					if (l.t0 > play.whistle + 300) {
						break;
					}
					assert.include(
						["prone", "supine", "hurt", "hurtProne"],
						l.move,
						`play ${play.i}`,
					);
					assert.isBelow(
						Math.hypot(l.to.x - fell.to.x, l.to.y - fell.to.y),
						0.01,
						`play ${play.i}`,
					);
				}
			}
		});
		assert.isAbove(lame, 0);
	}, 20_000);

	test("a touchdown when he gets in with it, not before", () => {
		const bad: string[] = [];
		let checked = 0;
		eachPlay((play, seg, tl) => {
			if (!ACTED.has(play.kind) || seg.some((e) => e.type === "flag")) {
				return;
			}
			const k = seg.findIndex(
				(x) =>
					x.td === true &&
					(x.type === "run" ||
						x.type === "passComplete" ||
						x.type === "interceptionReturn" ||
						x.type === "kickoffReturn" ||
						x.type === "puntReturn"),
			);
			if (k < 0) {
				return;
			}
			const beat = tl.beats.find((b) => b.i === play.i + k)!;
			const holder = play.ball.findLast(
				(b): b is Extract<BallSeg, { kind: "held" }> =>
					b.kind === "held" && b.t0 <= beat.actionStart + 1,
			);
			const m = momentAt(tl, beat.actionStart + 1);
			const man = m.players.find((p) => p.id === holder?.pid);
			// (In past the goal line: the end zone of whichever way he went.)
			if (!man || (man.x > 30 + 1 && man.x < FIELD_L - 30 - 1)) {
				bad.push(`not in on play ${play.i}`);
			}
			checked += 1;
		});
		assert.deepEqual(bad.slice(0, 5), []);
		assert.isAbove(checked, 10);
	}, 20_000);
	test("after a score, his teammates round the scorer - and the home crowd on its feet", () => {
		let checked = 0;
		let home = 0;
		let alone = 0;
		eachPlay((play, seg, tl) => {
			if (!ACTED.has(play.kind) || seg.some((e) => e.type === "flag")) {
				return;
			}
			const k = seg.findIndex(
				(x) =>
					x.td === true &&
					["run", "passComplete", "interceptionReturn"].includes(x.type),
			);
			if (k < 0) {
				return;
			}
			const beat = tl.beats.find((b) => b.i === play.i + k)!;
			const holder = play.ball.findLast(
				(b): b is Extract<BallSeg, { kind: "held" }> =>
					b.kind === "held" && b.t0 <= beat.actionStart + 1,
			)!;
			const scorer = play.parts.find((p) => p.pid === holder.pid)!;
			// Those of them anywhere near him when he got in, round him by the
			// end of it.
			const end = play.t1 - 1;
			const at = posAt(scorer, end);
			const mates = play.parts.filter(
				(p) => p !== scorer && p.team === scorer.team,
			);
			const by = mates.filter(
				(p) =>
					dist(posAt(p, beat.actionStart), posAt(scorer, beat.actionStart)) <
					45,
			);
			const round = mates.filter((p) => dist(posAt(p, end), at) < 8);
			assert.isAtLeast(
				round.length,
				Math.min(2, by.length),
				`round him on play ${play.i}`,
			);
			// (Away on his own, the first of them with him all the same - he
			// out to meet them - before the picture moves on.)
			if (by.length === 0) {
				assert.isAtLeast(round.length, 1, `met on play ${play.i}`);
				alone += 1;
			}
			// (The home side: 1.)
			const up = crowdAt(tl, beat.actionStart + 1500, 0).up;
			if (scorer.team === 1) {
				assert.isAbove(up, 0.9, `home crowd on play ${play.i}`);
				home += 1;
			} else {
				assert.isBelow(up, 0.5, `road crowd on play ${play.i}`);
			}
			checked += 1;
		});
		assert.isAbove(checked, 10);
		assert.isAbove(home, 3);
		assert.isAbove(alone, 2);
	}, 20_000);
	test("a pick taken back from where the sim says, to where it says", () => {
		let checked = 0;
		eachPlay((play, seg, tl) => {
			const pick = seg.find((e) => e.type === "interception");
			const ret = seg.find((e) => e.type === "interceptionReturn");
			if (
				!pick ||
				!ret ||
				ret.td ||
				ret.touchback ||
				typeof pick.yds !== "number" ||
				typeof ret.yds !== "number" ||
				seg.some(
					(e) =>
						e.type === "fumble" || e.type === "penalty" || e.type === "flag",
				)
			) {
				return;
			}
			const beat = tl.beats.find((b) => b.i === play.i + seg.indexOf(ret));
			assert.isDefined(beat);
			const m = momentAt(tl, beat!.actionStart);
			assert.strictEqual(m.carrier, 1 - play.offense, `play ${play.i}`);
			assert.isDefined(m.focus);
			assert.closeTo(
				(m.focus!.x - play.losX) * play.dir,
				(pick.yds - ret.yds) * YD,
				4,
				`picked at ${pick.yds}, back ${ret.yds} on play ${play.i}`,
			);
			checked += 1;
		});
		assert.isAbove(checked, 3);
	}, 20_000);

	test("a pass to the man the sim says, caught by him", () => {
		let caught = 0;
		eachPlay((play, seg) => {
			if (play.kind !== "pass" || !seg.some((e) => e.type === "passComplete")) {
				return;
			}
			const target = seg.find((e) => e.type === "stat" && e.s === "rec")?.pid;
			const fly = play.ball.findIndex((b) => b.kind === "fly" && b.spiral);
			assert.isAtLeast(fly, 0, `play ${play.i}`);
			const next = play.ball[fly + 1];
			assert.strictEqual(next?.kind, "held");
			const pid = next?.kind === "held" ? next.pid : undefined;
			// (A two-point try keeps no stats: him by name, on the field.)
			assert.isDefined(
				play.parts.find((p) => p.pid === pid && p.team === play.offense),
			);
			if (target !== undefined) {
				assert.strictEqual(pid, target);
			}
			caught += 1;
		});
		assert.isAbove(caught, 10);
	}, 20_000);

	test("a kick returned to where the sim says, the returner brought down there", () => {
		let checked = 0;
		eachPlay((play, seg, tl) => {
			if (play.kind !== "kickoff" && play.kind !== "punt") {
				return;
			}
			if (seg.some((e) => e.type === "penalty" || e.type === "flag")) {
				return;
			}
			const kick = seg.find((e) => e.type === play.kind);
			const ret = seg.find(
				(e) => e.type === "kickoffReturn" || e.type === "puntReturn",
			);
			if (!kick || !ret || ret.td || typeof ret.yds !== "number") {
				return;
			}
			const kickYds = typeof kick.yds === "number" ? kick.yds : 0;
			// (The kicking team's way: a kickoff to so many yards out from the
			// goal line, a punt so far from the line.)
			const goalD = (play.dir === 1 ? FIELD_L - 30 : 30) - play.losX;
			const want =
				play.kind === "kickoff"
					? goalD * play.dir - (kickYds + ret.yds) * YD
					: (kickYds - ret.yds) * YD;
			const beat = tl.beats.find((b) => b.i === play.i + seg.indexOf(ret));
			assert.isDefined(beat);
			const holder = [...play.ball]
				.filter(
					(b): b is Extract<BallSeg, { kind: "held" }> => b.kind === "held",
				)
				.findLast((b) => b.t0 <= beat!.actionStart);
			assert.isDefined(holder, `play ${play.i}`);
			const returner = play.parts.find((p) => p.pid === holder!.pid)!;
			assert.notStrictEqual(returner.team, play.offense, `play ${play.i}`);
			const m = momentAt(tl, beat!.actionStart);
			const man = m.players.find((p) => p.id === holder!.pid)!;
			assert.closeTo(
				(man.x - play.losX) * play.dir,
				want,
				3.5,
				`${play.kind} returned ${ret.yds} on play ${play.i}`,
			);
			checked += 1;
		});
		assert.isAbove(checked, 10);
	}, 20_000);

	test("a field goal through the uprights when it is good, and not when it is not", () => {
		let checked = 0;
		eachPlay((play, seg, tl) => {
			if (play.kind !== "fieldGoal" && play.kind !== "extraPoint") {
				return;
			}
			const e = seg.find((x) => x.type === play.kind);
			if (!e) {
				return;
			}
			const beat = tl.beats.find((b) => b.i === play.i + seg.indexOf(e));
			const m = momentAt(tl, beat!.actionStart);
			assert.isDefined(m.ball, `play ${play.i}`);
			const b = m.ball!.at;
			const endLine = play.dir === 1 ? FIELD_L : 0;
			const through = Math.abs(b.y - MID_Y) < POST_W / 2 && b.z > CROSSBAR_Z;
			if (e.made) {
				assert.closeTo(b.x, endLine, 1, `play ${play.i}`);
				assert.isTrue(through, `good on play ${play.i}`);
			} else {
				assert.isFalse(through, `no good on play ${play.i}`);
			}
			checked += 1;
		});
		assert.isAbove(checked, 4);
	}, 20_000);
	test("an onside kick got back where the sim says, by the side it says", () => {
		let checked = 0;
		let got = 0;
		eachPlay((play, seg, tl) => {
			if (play.kind !== "onside") {
				return;
			}
			const e = seg.find((x) => x.type === "onsideKickRecovery");
			if (!e || e.td || seg.some((x) => x.type === "penalty")) {
				return;
			}
			const beat = tl.beats.find((b) => b.i === play.i + seg.indexOf(e));
			assert.isDefined(beat);
			const m = momentAt(tl, beat!.actionStart);
			const want = e.success ? play.offense : 1 - play.offense;
			assert.strictEqual(m.carrier, want, `play ${play.i}`);
			// Down where the next snap is from (as near as the man on it: the
			// ball out at arm's length in front of him, whichever side of it
			// he comes from - and it out the ten yards at least, see
			// action.ts onside).
			const next = tl.plays[tl.plays.indexOf(play) + 1];
			if (next && next.quarter === play.quarter && m.focus) {
				const d = (m.focus.x - play.losX) * play.dir;
				const at = (next.losX - play.losX) * play.dir;
				if (at >= 10 * YD && at <= 25 * YD) {
					assert.closeTo(d, at, 5, `play ${play.i}`);
				}
				assert.isAtLeast(d, 9 * YD, `play ${play.i}`);
			}
			checked += 1;
			got += e.success ? 1 : 0;
		});
		assert.isAbove(checked, 3);
		assert.isAbove(got, 0);
	}, 20_000);
	test("a fumble loose on the turf, come up with by the man the sim says, down where it says", () => {
		let checked = 0;
		eachPlay((play, seg, tl) => {
			const k = seg.findLastIndex((x) => x.type === "fumbleRecovery");
			if (
				k < 0 ||
				!ACTED.has(play.kind) ||
				seg.some((x) => x.type === "penalty" || x.type === "flag")
			) {
				return;
			}
			const e = seg[k]!;
			const fum = seg.findLastIndex((x, j) => j < k && x.type === "fumble");
			const out = tl.beats.find((b) => b.i === play.i + fum);
			const beat = tl.beats.find((b) => b.i === play.i + k);
			assert.isDefined(out, `play ${play.i}`);
			assert.isDefined(beat, `play ${play.i}`);
			// Out of his hands: on the turf, or in the air off it.
			const loose = momentAt(tl, out!.actionStart + 200);
			assert.isUndefined(loose.carrier, `loose on play ${play.i}`);
			// Had by the side the sim says - the man it says, if it says.
			const m = momentAt(tl, beat!.actionStart);
			const side = 1 - (e.t === 1 ? 1 : 0);
			assert.strictEqual(m.carrier, side, `whose on play ${play.i}`);
			const rec = seg.findLast(
				(x, j) => j > fum && j < k && x.type === "stat" && x.s === "defFmbRec",
			)?.pid;
			const holder = play.ball.findLast(
				(b): b is Extract<BallSeg, { kind: "held" }> =>
					b.kind === "held" && b.t0 <= beat!.actionStart,
			);
			if (rec !== undefined) {
				assert.strictEqual(holder?.pid, rec, `who on play ${play.i}`);
			}
			// And down where the next snap is from: the ball - where it lay
			// if he fell on it (he is down a few feet back from it), or him
			// with it.
			const next = tl.plays[tl.plays.indexOf(play) + 1];
			const lay = holder
				? play.ball.findLast((b) => b.t0 < holder.t0)
				: undefined;
			const on = play.parts
				.find((p) => p.pid === holder?.pid)
				?.legs.findLast((l) => l.t0 <= beat!.actionStart);
			const at =
				lay?.kind === "spot" && on?.move === "prone" ? lay.at : m.focus;
			if (
				next &&
				next.quarter === play.quarter &&
				!e.td &&
				!e.safety &&
				!e.touchback &&
				at &&
				(next.kind === "pass" || next.kind === "run")
			) {
				assert.closeTo(at.x, next.losX, 3.5, `where on play ${play.i}`);
			}
			checked += 1;
		});
		assert.isAbove(checked, 10);
	}, 20_000);
	test("a flag thrown for every one the sim has, and the referee's call of each", () => {
		const bad: string[] = [];
		let flags = 0;
		let early = 0;
		let seconds = 0;
		eachPlay((play, seg) => {
			const thrown = seg.filter((x) => x.type === "flag").length;
			// (Not counting a second man's flag on one: see crew.ts.)
			const firsts = play.flags.filter((f) => !f.second).length;
			if (firsts !== thrown) {
				bad.push(`${firsts} of ${thrown} flags on play ${play.i}`);
			}
			seconds += play.flags.length - firsts;
			for (const f of play.flags) {
				if (f.t0 < play.t0 || f.t1 > play.t1) {
					bad.push(`a flag out of its play on play ${play.i}`);
				}
				if (
					f.to.x < -4 ||
					f.to.x > FIELD_L + 4 ||
					f.to.y < -4 ||
					f.to.y > 164
				) {
					bad.push(`a flag off the field on play ${play.i}`);
				}
			}
			flags += thrown;
			// The referee, after the play: each foul's signal, then which way
			// it goes (or waved off, declined).
			const calls = seg.filter((x) => x.type === "penalty").length;
			const ref = play.crew.find((p) => p.pid === -1)!;
			const sides = ref.legs.filter(
				(l) =>
					(l.arms === "sigSide" || l.arms === "sigWave") &&
					l.armsSpan?.[0] === 0,
			).length;
			if (sides !== calls) {
				bad.push(`${sides} of ${calls} calls on play ${play.i}`);
			}
			// A foul before the snap: no snap.
			if (
				thrown > 0 &&
				!seg.some((x) =>
					[
						"dropback",
						"handoff",
						"run",
						"kneel",
						"punt",
						"kickoff",
						"fieldGoal",
						"extraPoint",
					].includes(x.type),
				)
			) {
				early += 1;
				if (
					play.ball.some(
						(b) => b.kind === "fly" || (b.kind === "held" && b.t0 > play.snap),
					)
				) {
					bad.push(`snapped with a foul before it on play ${play.i}`);
				}
			}
		});
		assert.deepEqual(bad.slice(0, 5), []);
		assert.isAbove(flags, 20);
		assert.isAbove(early, 3);
		assert.isAbove(seconds, 0);
	}, 20_000);
	test("the officials on every play: all seven, at a man's speed, on the field or just off it", () => {
		const bad: string[] = [];
		eachPlay((play, _, tl) => {
			if (play.crew.length !== 7) {
				bad.push(`${play.crew.length} officials on play ${play.i}`);
			}
			for (const part of play.crew) {
				part.legs.forEach((l, k) => {
					const prev = part.legs[k - 1];
					if (
						prev &&
						(Math.abs(l.t0 - prev.t1) > 1e-6 ||
							(dist(l.from, prev.to) > 1e-6 && !tl.cuts.includes(l.t0)))
					) {
						bad.push(`official ${part.pid} jumps on play ${play.i}`);
					}
					const secs = (l.t1 - l.t0) / 1000;
					if (secs > 0.05 && dist(l.from, l.to) / secs > 23) {
						bad.push(`official ${part.pid} too fast on play ${play.i}`);
					}
					// (Under the posts, just past the end line.)
					const p = l.to;
					if (p.x < -4 || p.x > FIELD_L + 4 || p.y < -4 || p.y > 164) {
						bad.push(`official ${part.pid} off the field on play ${play.i}`);
					}
				});
				if (part.legs.at(-1)!.t1 < play.t1 - 1) {
					bad.push(`official ${part.pid} gone early on play ${play.i}`);
				}
			}
		});
		assert.deepEqual(bad.slice(0, 5), []);
	}, 20_000);
});
