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
		...(await simGames([21, 22])),
		// (One with a lot of onside kicks, and a lot of them got back.)
		...(await simGames([23, 24], {
			onsideFactor: 900,
			onsideRecoveryFactor: 4,
		})),
		// (And a lot of fumbles.)
		...(await simGames([31, 32], { fumbleFactor: 60 })),
	];
	staged = games.map((g, k) =>
		compileField({ events: g.events, players: g.roster, gid: k }),
	);
}, 120_000);

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
	}, 30_000);

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
	});

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
				assert.closeTo(
					d,
					want,
					3.5,
					`${e.type} for ${e.yds} on play ${play.i}`,
				);
			}
			checked += 1;
		});
		assert.isAbove(checked, 50);
		// (The first to have every play's men laid out.)
	}, 30_000);

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
	});
	test("after a score, his teammates round the scorer - and the home crowd on its feet", () => {
		let checked = 0;
		let home = 0;
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
	});
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
	});

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
	});

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
	});

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
	});
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
			// Down where the next snap is from.
			const next = tl.plays[tl.plays.indexOf(play) + 1];
			if (next && next.quarter === play.quarter && m.focus) {
				const d = (m.focus.x - play.losX) * play.dir;
				const at = (next.losX - play.losX) * play.dir;
				if (at >= 10 * YD && at <= 25 * YD) {
					assert.closeTo(d, at, 3.5, `play ${play.i}`);
				}
				assert.isAtLeast(d, 9 * YD, `play ${play.i}`);
			}
			checked += 1;
			got += e.success ? 1 : 0;
		});
		assert.isAbove(checked, 3);
		assert.isAbove(got, 0);
	});
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
	});
	test("a flag thrown for every one the sim has, and the referee's call of each", () => {
		const bad: string[] = [];
		let flags = 0;
		let early = 0;
		eachPlay((play, seg) => {
			const thrown = seg.filter((x) => x.type === "flag").length;
			if (play.flags.length !== thrown) {
				bad.push(`${play.flags.length} of ${thrown} flags on play ${play.i}`);
			}
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
	});
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
	});
});
