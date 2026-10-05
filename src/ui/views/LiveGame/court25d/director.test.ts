import { assert, describe, test } from "vitest";
import { makeCourtRng } from "../courtRng.ts";
import {
	compileCourt,
	isLineItem,
	snapForCursor,
	targetForCursor,
	type RawEvent,
	type CourtPlayer,
	type CourtTimeline,
} from "./director.ts";
import { evalBall, evalPlayer } from "./evaluate.ts";
import { bodyOf } from "./poses.ts";
import { finishOf } from "../../../util/liveGameWording.basketball.ts";

// A small stand-in for GameSim.basketball's play-by-play: the same event
// shapes, in the same orders the sim emits them (an attempt, then a make, a
// miss and a rebound, a block, or a shooting foul and its free throws; a
// putback with no attempt line; turnovers, steals, fouls, subs, timeouts and
// period breaks). Raw team 0 is home, as in the sim.
const fakeGame = (seed: string, possessions: number) => {
	const rng = makeCourtRng(seed);
	const pick = <T>(arr: T[]): T => arr[Math.floor(rng() * arr.length)]!;
	const POS = ["PG", "SG", "SF", "PF", "C", "G", "F", "C"];
	const roster: [number[], number[]] = [
		[1, 2, 3, 4, 5, 6, 7, 8],
		[11, 12, 13, 14, 15, 16, 17, 18],
	];
	const players: CourtPlayer[] = [];
	for (const raw of [0, 1] as const) {
		roster[raw].forEach((pid, j) => {
			players.push({ pid, team: raw === 0 ? 1 : 0, pos: POS[j] });
		});
	}
	const onCourt: [number[], number[]] = [
		roster[0].slice(0, 5),
		roster[1].slice(0, 5),
	];
	const events: RawEvent[] = [{ type: "init", boxScore: {} }];
	for (const raw of [0, 1] as const) {
		for (const pid of onCourt[raw]) {
			events.push({ type: "stat", t: raw, pid, s: "gs", amt: 1 });
		}
	}
	let clock = 720;
	let period = 1;
	const tick = (lo: number, hi: number) => {
		clock = Math.max(0.5, clock - (lo + rng() * (hi - lo)));
		return Math.round(clock * 10) / 10;
	};
	events.push({
		type: "jumpBall",
		t: 0,
		pid: onCourt[0][4],
		pid2: onCourt[1][4],
		clock,
	});
	let o: 0 | 1 = 0;
	let putbackFor: number | undefined;

	const rebound = (shooterTeam: 0 | 1) => {
		const r = rng();
		if (r < 0.08) {
			events.push({
				type: "outOfBounds",
				t: rng() < 0.5 ? shooterTeam : 1 - shooterTeam,
				on: "offense",
				clock: tick(0.2, 1),
			});
			o = (1 - shooterTeam) as 0 | 1;
			return;
		}
		if (r < 0.75) {
			const d = (1 - shooterTeam) as 0 | 1;
			events.push({
				type: "drb",
				t: d,
				pid: pick(onCourt[d]),
				clock: tick(0.2, 1),
			});
			o = d;
		} else {
			const pid = pick(onCourt[shooterTeam]);
			events.push({ type: "orb", t: shooterTeam, pid, clock: tick(0.2, 1) });
			o = shooterTeam;
			if (rng() < 0.5) {
				putbackFor = pid;
			}
		}
	};
	const freeThrows = (team: 0 | 1, pid: number, n: number) => {
		for (let k = 0; k < n; k++) {
			const made = rng() < 0.75;
			events.push({ type: made ? "ft" : "missFt", t: team, pid, clock });
			if (made) {
				events.push({ type: "stat", t: team, pid, s: "pts", amt: 1 });
			}
			if (k === n - 1) {
				if (made) {
					o = (1 - team) as 0 | 1;
				} else {
					rebound(team);
				}
			}
		}
	};

	for (let n = 0; n < possessions; n++) {
		const d = (1 - o) as 0 | 1;
		if (n > 0 && n % 22 === 0) {
			events.push({ type: "endOfPeriod", t: o, reason: "noShot", clock: 0 });
			period += 1;
			clock = 720;
			events.push({ type: "period", period, clock });
			const off = onCourt[0].slice(0, 2);
			const on = roster[0].filter((p) => !onCourt[0].includes(p)).slice(0, 2);
			onCourt[0] = [...onCourt[0].filter((p) => !off.includes(p)), ...on];
			events.push({ type: "sub", t: 0, pids: on, pidsOff: off, clock });
		}
		if (rng() < 0.05) {
			events.push({
				type: "timeout",
				t: o,
				numLeft: 3,
				advancesBall: false,
				clock: tick(0, 0),
			});
		}
		if (rng() < 0.06) {
			const team = (rng() < 0.5 ? 0 : 1) as 0 | 1;
			const off = [pick(onCourt[team])];
			const on = [pick(roster[team].filter((p) => !onCourt[team].includes(p)))];
			onCourt[team] = [...onCourt[team].filter((p) => !off.includes(p)), ...on];
			events.push({ type: "sub", t: team, pids: on, pidsOff: off, clock });
		}

		if (putbackFor !== undefined && onCourt[o].includes(putbackFor)) {
			const pid = putbackFor;
			putbackFor = undefined;
			if (rng() < 0.55) {
				events.push(
					{
						type: "fgPutBack",
						t: o,
						pid,
						clock: tick(0.3, 1.2),
						period,
					},
					{ type: "stat", t: o, pid, s: "pts", amt: 2 },
				);
				o = d;
			} else {
				events.push({ type: "missPutBack", t: o, pid, clock: tick(0.3, 1.2) });
				rebound(o);
			}
			continue;
		}
		putbackFor = undefined;

		const r = rng();
		const shooter = pick(onCourt[o]);
		if (r < 0.7) {
			const zone = pick(["AtRim", "LowPost", "MidRange", "Tp"]);
			events.push({
				type: zone === "Tp" ? "fgaTp" : `fga${zone}`,
				t: o,
				pid: shooter,
				clock: tick(4, 18),
				desperation: false,
			});
			const res = rng();
			if (res < 0.45) {
				const ast =
					rng() < 0.6
						? pick(onCourt[o].filter((p) => p !== shooter))
						: undefined;
				const andOne = rng() < 0.05;
				const base = zone === "Tp" ? "tp" : `fg${zone}`;
				events.push(
					{
						type: andOne ? `${base}AndOne` : base,
						t: o,
						pid: shooter,
						pidAst: ast,
						pidDefense: zone === "AtRim" ? pick(onCourt[d]) : undefined,
						pidFoul: andOne ? pick(onCourt[d]) : undefined,
						clock: tick(0.2, 1.5),
						period,
					},
					{
						type: "stat",
						t: o,
						pid: shooter,
						s: "pts",
						amt: zone === "Tp" ? 3 : 2,
					},
				);
				if (andOne) {
					freeThrows(o, shooter, 1);
				} else {
					o = d;
				}
			} else if (res < 0.88) {
				events.push({
					type: zone === "Tp" ? "missTp" : `miss${zone}`,
					t: o,
					pid: shooter,
					clock: tick(0.2, 1.5),
				});
				rebound(o);
			} else if (res < 0.95) {
				events.push({
					type: zone === "Tp" ? "blkTp" : `blk${zone}`,
					t: d,
					pid: pick(onCourt[d]),
					clock: tick(0.1, 0.4),
				});
				rebound(o);
			} else {
				events.push({
					type: zone === "Tp" ? "pfTP" : "pfFG",
					t: d,
					pid: pick(onCourt[d]),
					pidShooting: shooter,
					clock,
				});
				freeThrows(o, shooter, zone === "Tp" ? 3 : 2);
			}
		} else if (r < 0.8) {
			events.push({
				type: "tov",
				t: o,
				pid: shooter,
				outOfBounds: rng() < 0.3,
				clock: tick(3, 12),
			});
			o = d;
		} else if (r < 0.88) {
			events.push({
				type: "stl",
				t: d,
				pid: pick(onCourt[d]),
				pidTov: shooter,
				outOfBounds: rng() < 0.1,
				clock: tick(3, 12),
			});
			o = d;
		} else if (r < 0.95) {
			events.push({
				type: "pfNonShooting",
				t: d,
				pid: pick(onCourt[d]),
				clock: tick(2, 10),
			});
		} else {
			events.push({
				type: "pfBonus",
				t: d,
				pid: pick(onCourt[d]),
				pidShooting: shooter,
				clock: tick(2, 10),
			});
			freeThrows(o, shooter, 2);
		}
	}
	events.push(
		{ type: "endOfPeriod", t: o, reason: "noShot", clock: 0 },
		{ type: "gameOver" },
	);
	return { events, players };
};

const gidOf = (seed: string) =>
	[...seed].reduce((h, c) => (h * 31 + c.charCodeAt(0)) % 100_000, 7);

const compile = (seed: string, possessions = 120) => {
	const { events, players } = fakeGame(seed, possessions);
	const tl = compileCourt({ events, players, gid: gidOf(seed) });
	return { events, players, tl };
};

const body = bodyOf();
const bodyFor = () => body;

// Whether the picture cut between two moments (anyone may be anywhere after).
const cutBetween = (tl: CourtTimeline, a: number, b: number) =>
	tl.cuts.some((c) => c > a && c <= b);

const sampleTimes = (tl: CourtTimeline, step: number) => {
	const out: number[] = [];
	for (let t = 0; t <= tl.end; t += step) {
		out.push(t);
	}
	return out;
};

describe("2.5D director", () => {
	test("every play-by-play line gets one beat, in order, tiling the timeline", () => {
		for (const seed of ["a", "b", "c"]) {
			const { events, tl } = compile(seed);
			const lines = events
				.map((e, i) => ({ e, i }))
				.filter(({ e }) => isLineItem(e))
				.map(({ i }) => i);
			assert.deepStrictEqual(
				tl.beats.map((b) => b.i),
				lines,
			);
			let prevEnd = 0;
			for (const b of tl.beats) {
				assert.strictEqual(
					b.preStart,
					prevEnd,
					`beat ${b.i} ${b.type} starts where the last ended`,
				);
				assert.isAtLeast(b.actionStart, b.preStart);
				assert.isAbove(b.end, b.actionStart);
				prevEnd = b.end;
			}
			assert.strictEqual(tl.end, prevEnd);
		}
	});

	test("the same game stages the same way every time (every device agrees)", () => {
		const a = compile("same").tl;
		const b = compile("same").tl;
		assert.strictEqual(JSON.stringify(a.beats), JSON.stringify(b.beats));
		assert.strictEqual(JSON.stringify(a.ball), JSON.stringify(b.ball));
		assert.strictEqual(
			JSON.stringify([...a.tracks.values()]),
			JSON.stringify([...b.tracks.values()]),
		);
	});

	// A saved replay is the play-by-play read back out of the database and
	// staged again, on whatever device, after whatever else was watched.
	test("a saved replay stages exactly like the game did, every viewing", () => {
		const live = compile("replay");
		compile("another game in between");
		// (IndexedDB stores a structured clone.)
		const saved = structuredClone(live.events);
		const again = compileCourt({
			events: saved,
			players: live.players,
			gid: gidOf("replay"),
		});
		const dump = (tl: CourtTimeline) =>
			JSON.stringify({ ...tl, tracks: [...tl.tracks.entries()] });
		assert.strictEqual(dump(again), dump(live.tl));
		for (const t of sampleTimes(live.tl, 250)) {
			for (const p of live.players) {
				assert.deepStrictEqual(
					evalPlayer(again, p.pid, t),
					evalPlayer(live.tl, p.pid, t),
				);
			}
			assert.deepStrictEqual(
				evalBall(again, t, bodyFor),
				evalBall(live.tl, t, bodyFor),
			);
		}
	});

	// Shots are thrown, not floated: under gravity a three from the arc
	// climbs to about fifteen feet, the way real ones do.
	test("a three arcs like a real one", () => {
		const { tl } = compile("arcs", 200);
		let threes = 0;
		for (const seg of tl.ball) {
			if (seg.kind !== "fly" || !("pid" in seg.from) || "pid" in seg.to) {
				continue;
			}
			const a = evalBall(tl, seg.t0, bodyFor);
			const b = evalBall(tl, seg.t1, bodyFor);
			const across = Math.hypot(b.x - a.x, b.y - a.y);
			if (across < 22 || b.z < 9.5) {
				continue;
			}
			threes += 1;
			let apex = 0;
			for (let t = seg.t0; t <= seg.t1; t += 10) {
				apex = Math.max(apex, evalBall(tl, t, bodyFor).z);
			}
			assert.isAbove(apex, 13, "a three gets up");
			assert.isBelow(apex, 19, "a three is not a moonball");
		}
		assert.isAbove(threes, 0);
	});

	test("in the air, the ball falls at gravity's pace", () => {
		const { tl } = compile("gravity", 120);
		for (const seg of tl.ball) {
			if (seg.kind !== "fly" || seg.t1 - seg.t0 < 300) {
				continue;
			}
			// The second difference of height over equal steps is -g * dt^2.
			const dt = 0.04;
			const m = (seg.t0 + seg.t1) / 2;
			const z = (t: number) => evalBall(tl, t, bodyFor).z;
			const accel =
				(z(m + dt * 1000) - 2 * z(m) + z(m - dt * 1000)) / (dt * dt);
			assert.closeTo(accel, -32.2, 1.5);
		}
	});

	test("a player's moves never overlap and stay near the floor", () => {
		const { tl } = compile("moves", 160);
		for (const tr of tl.tracks.values()) {
			for (let k = 0; k < tr.moves.length; k++) {
				const m = tr.moves[k]!;
				assert.isAbove(m.t1, m.t0);
				if (k > 0) {
					assert.isAtLeast(
						m.t0,
						tr.moves[k - 1]!.t1 - 1,
						`pid ${tr.pid} move ${k}`,
					);
				}
				// On the floor, or on his way to or from the bench.
				for (const p of [m.from, m.to]) {
					assert.isTrue(
						p.x > -4 && p.x < 98 && p.y > -8.5 && p.y < 54,
						`pid ${tr.pid} off the floor: ${p.x},${p.y}`,
					);
				}
			}
		}
	});

	test("bodies glide - nobody on the floor teleports between frames", () => {
		const { tl } = compile("glide", 140);
		const pids = [...tl.tracks.keys()];
		let prev = new Map<number, ReturnType<typeof evalPlayer>>();
		let last = -Infinity;
		for (const t of sampleTimes(tl, 20)) {
			const cur = new Map<number, ReturnType<typeof evalPlayer>>();
			const cut = cutBetween(tl, last, t);
			last = t;
			for (const pid of pids) {
				const st = evalPlayer(tl, pid, t);
				cur.set(pid, st);
				const p = prev.get(pid);
				if (p && p.shown && st.shown && !cut) {
					const d = Math.hypot(st.x - p.x, st.y - p.y);
					assert.isBelow(d, 2, `pid ${pid} jumped ${d.toFixed(2)}ft at t=${t}`);
				}
			}
			prev = cur;
		}
	}, 60_000);

	test("the ball travels - it never jumps across the floor between frames", () => {
		const { tl } = compile("ball", 140);
		let prev: { x: number; y: number; z: number } | undefined;
		let jumps = 0;
		let last = -Infinity;
		for (const t of sampleTimes(tl, 20)) {
			const b = evalBall(tl, t, bodyFor);
			assert.isTrue(
				Number.isFinite(b.x) && Number.isFinite(b.y) && Number.isFinite(b.z),
			);
			const cut = cutBetween(tl, last, t);
			last = t;
			if (prev && !cut) {
				const d = Math.hypot(b.x - prev.x, b.y - prev.y, b.z - prev.z);
				if (d > 6) {
					jumps += 1;
				}
			}
			prev = b;
		}
		assert.strictEqual(jumps, 0);
	}, 60_000);

	test("the picture cuts only between plays, to five a side", () => {
		const { events, tl } = compile("cuts", 160);
		assert.isAbove(tl.cuts.length, 40);
		const pids = [...tl.tracks.keys()];
		for (const c of tl.cuts) {
			// Never in the middle of a line's action.
			for (const b of tl.beats) {
				assert.isFalse(
					c > b.actionStart && c < b.end && /^(fg|miss|blk|tp)/.test(b.type),
					`cut at ${c} inside ${b.type}`,
				);
			}
			const on = [0, 0];
			for (const pid of pids) {
				const st = evalPlayer(tl, pid, c + 1);
				if (st.shown) {
					on[st.team]! += 1;
				}
			}
			assert.deepStrictEqual(on, [5, 5], `at ${c}`);
		}
		// Cut times are in order and the game still tiles.
		assert.deepStrictEqual(
			tl.cuts,
			[...tl.cuts].sort((a, b) => a - b),
		);
		assert.isAbove(events.length, 0);
	});

	test("whoever holds the ball is on the floor", () => {
		const { tl } = compile("holder", 140);
		for (const t of sampleTimes(tl, 50)) {
			const b = evalBall(tl, t, bodyFor);
			if (b.holder !== undefined) {
				assert.isTrue(
					evalPlayer(tl, b.holder, t).shown,
					`holder ${b.holder} hidden at ${t}`,
				);
			}
		}
	});

	test("a make swishes and a miss rattles, right when its line shows", () => {
		const { tl } = compile("fx", 140);
		for (const b of tl.beats) {
			const near = (kind: string) =>
				tl.fx.some(
					(f) =>
						f.kind === kind &&
						f.t >= b.actionStart - 10 &&
						f.t <= b.actionStart + 200,
				);
			if (/^(fg|tp)/.test(b.type) && !b.type.startsWith("fga")) {
				assert.isTrue(near("swish"), `${b.type} at ${b.actionStart}`);
			}
			if (b.type.startsWith("miss") && b.type !== "missFt") {
				assert.isTrue(near("clank"), `${b.type} at ${b.actionStart}`);
			}
		}
	});

	test("the playback target only moves forward as lines are shown", () => {
		const { events, tl } = compile("cursor", 60);
		let prev = -1;
		for (let c = 0; c <= events.length; c++) {
			const t = targetForCursor(tl, c);
			assert.isAtLeast(t, prev);
			assert.isAtMost(snapForCursor(tl, c), t);
			prev = t;
		}
		assert.strictEqual(targetForCursor(tl, events.length), tl.end);
	});
	test("a finish at the rim looks the way the play-by-play words it", () => {
		for (const gender of ["male", "female"] as const) {
			const { events, players } = fakeGame(`words-${gender}`, 220);
			const gid = 4242;
			const tl = compileCourt({ events, players, gid, gender });
			const beatOf = new Map(tl.beats.map((b) => [b.i, b]));
			let checked = 0;
			let dunks = 0;
			events.forEach((e, i) => {
				if (e.type !== "fgAtRim" && e.type !== "fgAtRimAndOne") {
					return;
				}
				// Staged from its attempt line on.
				let a = i - 1;
				while (a >= 0 && events[a]!.type !== "fgaAtRim") {
					a -= 1;
				}
				const from = beatOf.get(a)!.preStart;
				const to = beatOf.get(i)!.end;
				const acts = tl.tracks
					.get(e.pid)!
					.acts.filter((x) => x.t0 >= from && x.t0 <= to);
				const dunked = acts.some((x) =>
					["dunk", "dunk1", "tomahawk"].includes(x.anim),
				);
				const finish = finishOf(e, gid, gender);
				assert.strictEqual(
					dunked,
					finish === "dunk" || finish === "poster",
					`line ${i}`,
				);
				assert.strictEqual(
					acts.some((x) => x.anim === "layup"),
					finish === "layup",
					`line ${i}`,
				);
				checked += 1;
				dunks += dunked ? 1 : 0;
			});
			assert.isAbove(checked, 5);
			if (gender === "female") {
				assert.isBelow(dunks, 2);
			} else {
				assert.isAbove(dunks, 0);
			}
		}
	});
});
