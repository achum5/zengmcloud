import { assert, beforeAll, describe, test } from "vitest";
import {
	compileField,
	type BallSeg,
	type FieldTimeline,
	type Part,
	type Play,
} from "./director.ts";
import { momentAt } from "./evaluate.ts";
import { dist, YD, type Pt } from "./geometry.ts";
import { simGames, type SimGame } from "./testGames.ts";

let games: SimGame[] = [];
let staged: FieldTimeline[] = [];

beforeAll(async () => {
	games = await simGames([21, 22]);
	staged = games.map((g, k) =>
		compileField({ events: g.events, players: g.roster, gid: k }),
	);
}, 120_000);

const ACTED = new Set(["pass", "run", "kneel"]);

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
		eachPlay((play) => {
			for (const part of play.parts) {
				part.legs.forEach((l, k) => {
					const prev = part.legs[k - 1];
					if (prev) {
						assert.closeTo(l.t0, prev.t1, 1e-6, `${part.role} at ${l.t0}`);
						assert.isBelow(dist(l.from, prev.to), 1e-6, `${part.role} jumps`);
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
	});

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
});
