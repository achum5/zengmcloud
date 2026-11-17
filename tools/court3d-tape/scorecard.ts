// THE SCORECARD (REALISM.md, step 0): the numbers every change to the 3D
// game is judged by - how bodies move, whether a screen reads as a screen,
// tempo, and contact - worked out off a staged game, the way it is drawn.
// Only what is shown at real speed with the ball in play counts.
//
// Run on real games with scorecard.test.ts (SCORECARD_GAMES).

import type {
	CourtTimeline,
	ScreenNote,
} from "../../src/ui/views/LiveGame/court3d/director.ts";
import {
	bodyPoint,
	evalPlayer,
	fastAt,
	joltAt,
	poseOf,
	withBody,
} from "../../src/ui/views/LiveGame/court3d/evaluate.ts";
import { COURT_H, rimX } from "../../src/ui/views/LiveGame/court3d/geometry.ts";
import { bodyOf, skeleton } from "../../src/ui/views/LiveGame/court3d/poses.ts";

const SHOT_PLAYS = /^(fg|miss|tp|ast|tov|stl|pf|blk|orb|drb)/;
const CONTACT_ANIMS: Record<string, string> = {
	screen: "screen",
	screenChest: "screen",
	screenWide: "screen",
	postUp: "post",
	postUpSeal: "post",
	postUpTarget: "post",
	post: "post",
	boxOut: "boxOut",
	boxOutHigh: "boxOut",
	boxOutWide: "boxOut",
	fight: "boxOut",
	bump: "reaction",
	stumble: "reaction",
	jolt: "reaction",
	takeCharge: "reaction",
	fall: "reaction",
};
const REACTIONS = new Set(["bump", "stumble", "jolt", "fall", "takeCharge"]);

export type Scorecard = {
	game: string;
	liveMin: number;
	movement: {
		// Inches a planted foot slides in a 50 ms frame, on average, and the
		// share of planted frames it goes over an inch and a half.
		slideIn: number;
		slideShare: number;
		// Trunk pitch forward (degrees), by what he is doing: getting going
		// hard, running steady, pulling up hard.
		pitchStart: number;
		pitchSteady: number;
		pitchStop: number;
		// Banked into a hard turn at speed (degrees, into the turn).
		bankTurn: number;
		// Head or a foot moving faster against his hips than a body can,
		// frame to frame (a minute of live play).
		jumpsPerMin: number;
		jumpsBy: Record<string, number>;
		slideBy: Record<string, number>;
	};
	tempo: {
		mph: number;
		standing: number;
		standingBy: Record<string, number>;
		over15: number;
		// Men setting off within 0.1 s of another man.
		lockstep: number;
		setOffs: number;
		// Seconds of live play shown a possession.
		possS: number;
	};
	screens: {
		staged: number;
		ball: number;
		handoffs: number;
		coverage: Record<string, number>;
		// Of the ball screens: the screener within a step (3 ft) of the man
		// guarding the ball as it is set; the ball used off it (within 4 ft of
		// the screener) inside 1.5 s of it being set; and a position-only
		// detector finding it.
		onHisMan: number;
		used: number;
		useS: number;
		found: number;
		// Ball screens a position-only detector finds, a possession.
		detectedPerPoss: number;
		// Of the ball screens with coverage named: the share that looks it.
		looks: Record<string, number>;
	};
	contact: {
		// Seconds a possession two men of opposite teams are within 2 ft
		// middle to middle, by what they are doing.
		perPoss: Record<string, number>;
		// Closest middle to middle any two come (median of the contacts'
		// least gaps), and the share of contacts that go under a foot.
		gapMedian: number;
		through: number;
		// Contacts (gap under 1.7 ft, one of them moving 6 ft/s or more) with
		// a reaction drawn on either man within half a second.
		reacted: number;
		hard: number;
	};
};

const top = (by: Record<string, number>) =>
	Object.fromEntries(
		Object.entries(by)
			.sort((a, b) => b[1] - a[1])
			.slice(0, 8),
	);
const r1 = (x: number) => Math.round(x * 10) / 10;
const r2 = (x: number) => Math.round(x * 100) / 100;
const median = (xs: number[]) => {
	if (xs.length === 0) {
		return Number.NaN;
	}
	const s = [...xs].sort((a, b) => a - b);
	return s[Math.floor(s.length / 2)]!;
};

type Frame = {
	t: number;
	live: boolean;
	raw: boolean;
	holder?: number;
	ps: Map<
		number,
		{
			x: number;
			y: number;
			v: number;
			team: string;
			anim: string;
			shown: boolean;
			jolt: number;
		}
	>;
};

// When the ball is in whose hands (undefined in the air or on the floor).
const holderOf = (tl: CourtTimeline) => {
	return (t: number): number | undefined => {
		let lo = 0;
		let hi = tl.ball.length - 1;
		let k = -1;
		while (lo <= hi) {
			const mid = (lo + hi) >> 1;
			if (tl.ball[mid]!.t0 <= t) {
				k = mid;
				lo = mid + 1;
			} else {
				hi = mid - 1;
			}
		}
		const s = tl.ball[k];
		return s?.kind === "hold" ? s.pid : undefined;
	};
};

export const scorecard = (
	tl: CourtTimeline,
	game: string,
	opts: { poseMin?: number } = {},
): Scorecard => {
	const body = bodyOf();
	const STEP = 100;
	const tracks = [...tl.tracks.values()].filter((t) => t.pid >= 0);
	const holderAt = holderOf(tl);
	const cutNear = (a: number, b: number) =>
		tl.cuts.some((c) => c > a - 1 && c <= b + 1);
	let beatK = 0;
	const frames: Frame[] = [];
	// Every 100 ms, the game over.
	for (let t = 0; t < tl.end; t += STEP) {
		while (beatK + 1 < tl.beats.length && tl.beats[beatK + 1]!.preStart <= t) {
			beatK++;
		}
		const beat = tl.beats[beatK];
		const real = fastAt(tl, t) <= 1.01;
		const live =
			real &&
			!!beat &&
			beat.preStart <= t &&
			SHOT_PLAYS.test(beat.type) &&
			!/ft$/i.test(beat.type) &&
			!cutNear(t - STEP, t);
		const ps: Frame["ps"] = new Map();
		const prev = frames.at(-1);
		for (const tr of tracks) {
			const q = evalPlayer(tl, tr.pid, t);
			const p0 = prev?.ps.get(tr.pid);
			const v =
				p0 && p0.shown && q.shown
					? (Math.hypot(q.x - p0.x, q.y - p0.y) * 1000) / STEP
					: 0;
			ps.set(tr.pid, {
				x: q.x,
				y: q.y,
				v,
				team: tr.team,
				anim: q.anim,
				shown: q.shown,
				jolt: q.shown ? Math.hypot(...(joltAt(tl, tr, t, q.yaw) ?? [0, 0])) : 0,
			});
		}
		// (The first live frame has no speed yet: counted from the next.)
		frames.push({
			t,
			live: live && !!prev?.raw,
			raw: live,
			holder: holderAt(t),
			ps,
		});
	}
	// Who is on the floor: shown, live.
	const onFloor = (f: Frame) =>
		[...f.ps.entries()].filter(([, p]) => p.shown && p.x > -1 && p.x < 95);

	// TEMPO.
	let n = 0;
	let sumV = 0;
	let still = 0;
	let fast = 0;
	// (Stood still, by who he is: the man with the ball, his teammates,
	// the defense.)
	const stillBy = { ball: [0, 0], off: [0, 0], def: [0, 0] };
	let liveFrames = 0;
	for (const f of frames) {
		if (!f.live) {
			continue;
		}
		liveFrames++;
		const side = f.holder !== undefined ? f.ps.get(f.holder)?.team : undefined;
		for (const [pid, p] of onFloor(f)) {
			n++;
			sumV += Math.min(p.v, 35);
			const role =
				side === undefined
					? undefined
					: pid === f.holder
						? stillBy.ball
						: p.team === side
							? stillBy.off
							: stillBy.def;
			if (role) {
				role[1]++;
			}
			if (p.v < 1.5) {
				still++;
				if (role) {
					role[0]++;
				}
			}
			if (p.v > 15) {
				fast++;
			}
		}
	}
	// Setting off: still (under 1.5 ft/s) to going (over 6) within 0.3 s.
	const setOffs: { t: number; pid: number }[] = [];
	for (let i = 3; i < frames.length; i++) {
		const f = frames[i]!;
		if (!f.live || !frames[i - 3]!.live) {
			continue;
		}
		for (const [pid, p] of f.ps) {
			const a = frames[i - 3]!.ps.get(pid);
			const b = frames[i - 1]!.ps.get(pid);
			if (p.shown && a && b && a.v < 1.5 && b.v < 6 && p.v >= 6) {
				setOffs.push({ t: f.t, pid });
			}
		}
	}
	let lock = 0;
	for (const s of setOffs) {
		if (
			setOffs.some(
				(o) => o.pid !== s.pid && Math.abs(o.t - s.t) <= 100 && o !== s,
			)
		) {
			lock++;
		}
	}
	// Possessions: changes of who has it, with live play in them.
	const possLive: number[] = [];
	for (let i = 0; i < tl.poss.length; i++) {
		const t0 = tl.poss[i]![0];
		const t1 = tl.poss[i + 1]?.[0] ?? tl.end;
		let k = 0;
		for (const f of frames) {
			if (f.t >= t0 && f.t < t1 && f.live) {
				k++;
			}
		}
		if (k > 10) {
			possLive.push((k * STEP) / 1000);
		}
	}
	const possN = Math.max(1, possLive.length);

	// SCREENS.
	const screens = tl.screens ?? [];
	const frameAt = (t: number) =>
		frames[Math.max(0, Math.min(frames.length - 1, Math.round(t / STEP)))]!;
	const nearestFoe = (f: Frame, pid: number) => {
		const me = f.ps.get(pid);
		if (!me) {
			return undefined;
		}
		let best: number | undefined;
		let bd = Infinity;
		for (const [q, p] of f.ps) {
			if (p.team !== me.team && p.shown) {
				const d = Math.hypot(p.x - me.x, p.y - me.y);
				if (d < bd) {
					bd = d;
					best = q;
				}
			}
		}
		return best === undefined ? undefined : { pid: best, d: bd };
	};
	const dPid = (f: Frame, a: number, b: number) => {
		const p = f.ps.get(a);
		const q = f.ps.get(b);
		return p && q ? Math.hypot(p.x - q.x, p.y - q.y) : Infinity;
	};
	const ballScreens = screens.filter((s) => s.ball && !s.handoff);
	const coverage: Record<string, number> = {};
	const looksN: Record<string, number> = {};
	const looksOf: Record<string, number> = {};
	let onHis = 0;
	let used = 0;
	const useS: number[] = [];
	for (const s of screens) {
		coverage[s.coverage] = (coverage[s.coverage] ?? 0) + 1;
	}
	const usedAt = new Map<ScreenNote, number>();
	for (const s of ballScreens) {
		const f = frameAt(s.set);
		const scr = s.screeners[0]!;
		const guard = nearestFoe(frameAt(s.set - 400), s.user);
		if (guard && dPid(f, scr, guard.pid) <= 3) {
			onHis++;
		}
		// Used: the handler by the screener, and on 5 ft from where he was
		// as it was set, within 2.5 s.
		const H0 = f.ps.get(s.user);
		let by = false;
		for (let t = s.set - 300; H0 && t <= s.set + 2500; t += STEP) {
			const g = frameAt(t);
			if (g.holder !== s.user && t > s.set) {
				break;
			}
			by ||= dPid(g, s.user, scr) <= 4.5;
			const H = g.ps.get(s.user)!;
			if (by && Math.hypot(H.x - H0.x, H.y - H0.y) >= 5) {
				used++;
				useS.push(Math.max(0, t - s.set) / 1000);
				usedAt.set(s, t);
				break;
			}
		}
	}
	// Coverage: how the screener's man plays it, as the ball comes off.
	for (const s of ballScreens) {
		const tu = usedAt.get(s);
		if (tu === undefined) {
			continue;
		}
		const scr = s.screeners[0]!;
		const f0 = frameAt(s.set - 400);
		const big = nearestFoe(f0, scr)?.pid;
		const guard = nearestFoe(f0, s.user)?.pid;
		if (big === undefined || guard === undefined || big === guard) {
			continue;
		}
		const f = frameAt(tu + 500);
		const rim = { x: rimX(s.team), y: COURT_H / 2 };
		const B = f.ps.get(big)!;
		const toRim = (p: { x: number; y: number }) =>
			Math.hypot(p.x - rim.x, p.y - rim.y);
		const later = frameAt(tu + 1500);
		let looks = false;
		if (s.coverage === "drop") {
			looks = toRim(B) <= toRim(s.at) - 5 && toRim(B) <= 19;
		} else if (s.coverage === "hedge" || s.coverage === "blitz") {
			looks = dPid(f, big, s.user) <= 5 && toRim(B) >= toRim(s.at) - 3;
		} else if (s.coverage === "switch") {
			looks =
				dPid(later, big, s.user) < dPid(later, guard, s.user) &&
				dPid(later, guard, scr) < dPid(later, big, scr);
		} else {
			looks = dPid(later, guard, s.user) <= 6;
		}
		looksOf[s.coverage] = (looksOf[s.coverage] ?? 0) + 1;
		if (looks) {
			looksN[s.coverage] = (looksN[s.coverage] ?? 0) + 1;
		}
	}
	const looks: Record<string, number> = {};
	for (const k of Object.keys(looksOf)) {
		looks[k] = r2((looksN[k] ?? 0) / looksOf[k]!);
	}
	// A position-only detector: a man of the team with the ball, near still,
	// within 3.5 ft of the man guarding the ball handler, the handler within
	// 4 ft of him inside the next 1.5 s, out past the paint.
	const detected: number[] = [];
	for (let i = 0; i < frames.length; i++) {
		const f = frames[i]!;
		if (!f.live || f.holder === undefined) {
			continue;
		}
		const H = f.ps.get(f.holder);
		if (!H) {
			continue;
		}
		const guard = nearestFoe(f, f.holder);
		if (!guard || guard.d > 6) {
			continue;
		}
		for (const [pid, p] of f.ps) {
			if (pid === f.holder || p.team !== H.team || !p.shown || p.v > 2.5) {
				continue;
			}
			if (dPid(f, pid, guard.pid) > 3.5) {
				continue;
			}
			const rim = { x: rimX(p.team as any), y: COURT_H / 2 };
			if (Math.hypot(p.x - rim.x, p.y - rim.y) < 12) {
				continue;
			}
			let hit = false;
			for (let k = i; k < Math.min(frames.length, i + 15); k++) {
				if (
					frames[k]!.holder === f.holder &&
					dPid(frames[k]!, f.holder, pid) <= 4
				) {
					hit = true;
					break;
				}
			}
			if (hit && (detected.length === 0 || f.t - detected.at(-1)! > 3000)) {
				detected.push(f.t);
			}
		}
	}
	let found = 0;
	for (const s of ballScreens) {
		if (detected.some((t) => Math.abs(t - s.set) <= 1500)) {
			found++;
		}
	}

	// CONTACT.
	const contactS: Record<string, number> = {};
	const contacts: { gap: number; hard: boolean; reacted: boolean }[] = [];
	const open = new Map<
		string,
		{ gap: number; hard: boolean; t: number; a: number; b: number }
	>();
	const screenAround = (pid: number, t: number) =>
		screens.some(
			(s) => s.screeners.includes(pid) && t >= s.set - 800 && t <= s.set + 2000,
		);
	const reactionNear = (pid: number, t: number) => {
		for (let k = -5; k <= 5; k++) {
			const p = frameAt(t + k * STEP).ps.get(pid);
			if (p && (REACTIONS.has(p.anim) || p.jolt > 2)) {
				return true;
			}
		}
		return false;
	};
	const close = (key: string) => {
		const c = open.get(key)!;
		open.delete(key);
		contacts.push({
			gap: c.gap,
			hard: c.hard,
			reacted: c.hard && (reactionNear(c.a, c.t) || reactionNear(c.b, c.t)),
		});
	};
	for (const f of frames) {
		const seen = new Set<string>();
		if (f.live) {
			const fl = onFloor(f);
			for (let i = 0; i < fl.length; i++) {
				for (let j = i + 1; j < fl.length; j++) {
					const [a, p] = fl[i]!;
					const [b, q] = fl[j]!;
					if (p.team === q.team) {
						continue;
					}
					const gap = Math.hypot(p.x - q.x, p.y - q.y);
					if (gap >= 2) {
						continue;
					}
					const kind =
						CONTACT_ANIMS[p.anim] ??
						CONTACT_ANIMS[q.anim] ??
						(screenAround(a, f.t) || screenAround(b, f.t)
							? "screen"
							: f.holder === a || f.holder === b
								? (f.holder === a ? p.v : q.v) > 6
									? "drive"
									: "onBall"
								: "other");
					contactS[kind] = (contactS[kind] ?? 0) + STEP / 1000;
					const key = `${Math.min(a, b)}-${Math.max(a, b)}`;
					seen.add(key);
					const hard = gap < 1.7 && Math.max(p.v, q.v) >= 6;
					const c = open.get(key);
					if (c) {
						c.gap = Math.min(c.gap, gap);
						if (hard && !c.hard) {
							c.hard = true;
							c.t = f.t;
						}
					} else {
						open.set(key, { gap, hard, t: f.t, a, b });
					}
				}
			}
		}
		for (const key of open.keys()) {
			if (!seen.has(key)) {
				close(key);
			}
		}
	}
	const perPoss: Record<string, number> = {};
	for (const [k, v] of Object.entries(contactS)) {
		perPoss[k] = r2(v / possN);
	}
	const hard = contacts.filter((c) => c.hard);

	// MOVEMENT: the bodies, every 50 ms over the first so many minutes of
	// live play.
	const PSTEP = 50;
	const poseMs = (opts.poseMin ?? 6) * 60_000;
	let posed = 0;
	let planted = 0;
	let slideSum = 0;
	let slideBig = 0;
	const pitch: { start: number[]; steady: number[]; stop: number[] } = {
		start: [],
		steady: [],
		stop: [],
	};
	const bank: number[] = [];
	let jumps = 0;
	const jumpsBy: Record<string, number> = {};
	const slideBy: Record<string, number> = {};
	type P = {
		x: number;
		y: number;
		yaw: number;
		feet: { x: number; y: number; z: number }[];
		head: { x: number; y: number; z: number };
		hip: { x: number; y: number; z: number };
		v?: number;
		vx?: number;
		vy?: number;
		rel?: { x: number; y: number; z: number }[];
	};
	const last = new Map<number, P>();
	for (const f of frames) {
		if (posed >= poseMs) {
			break;
		}
		if (!f.live) {
			last.clear();
			continue;
		}
		for (let t = f.t; t < f.t + STEP; t += PSTEP) {
			posed += PSTEP / tracks.length;
			for (const tr of tracks) {
				const st = evalPlayer(tl, tr.pid, t);
				if (!st.shown || st.x < -1 || st.x > 95) {
					last.delete(tr.pid);
					continue;
				}
				const me = withBody(st, body);
				const sk = skeleton(body, poseOf(me));
				const feet = [sk.legR.end, sk.legL.end].map((v) => bodyPoint(me, v));
				const head = bodyPoint(me, sk.head);
				const hip = bodyPoint(me, sk.pelvis);
				const chest = bodyPoint(me, sk.chest);
				const q = last.get(tr.pid);
				const p: P = { x: st.x, y: st.y, yaw: st.yaw, feet, head, hip };
				if (q) {
					const vx = ((p.x - q.x) * 1000) / PSTEP;
					const vy = ((p.y - q.y) * 1000) / PSTEP;
					p.v = Math.hypot(vx, vy);
					p.vx = vx;
					p.vy = vy;
					// Feet: one down on the floor both frames.
					for (let k = 0; k < 2; k++) {
						const a = q.feet[k]!;
						const b = feet[k]!;
						if (a.z < 0.2 && b.z < 0.2 && st.z < 0.05) {
							planted++;
							const s = Math.hypot(b.x - a.x, b.y - a.y) * 12;
							slideSum += s;
							if (s > 1.5) {
								slideBig++;
								slideBy[st.anim] = (slideBy[st.anim] ?? 0) + 1;
							}
						}
					}
					// Head or a foot against the hips jerked: its way and speed
					// against them changed more from one frame to the next than a
					// body can (a pop, not just quick).
					const rel = (
						a: P["head"],
						b: P["head"],
						qa: P["head"],
						qb: P["head"],
					) => ({
						x: a.x - b.x - (qa.x - qb.x),
						y: a.y - b.y - (qa.y - qb.y),
						z: a.z - b.z - (qa.z - qb.z),
					});
					const now = [
						rel(head, hip, q.head, q.hip),
						rel(feet[0]!, hip, q.feet[0]!, q.hip),
						rel(feet[1]!, hip, q.feet[1]!, q.hip),
					];
					p.rel = now;
					if (q.rel) {
						const jerk = now.map((v, k) =>
							Math.hypot(
								v.x - q.rel![k]!.x,
								v.y - q.rel![k]!.y,
								v.z - q.rel![k]!.z,
							),
						);
						if (jerk[0]! > 0.3 || jerk[1]! > 0.9 || jerk[2]! > 0.9) {
							jumps++;
							jumpsBy[st.anim] = (jumpsBy[st.anim] ?? 0) + 1;
						}
					}
					// Lean: trunk pitch forward along the way he faces, against
					// how he is speeding up along it.
					if (q.v !== undefined && q.vx !== undefined) {
						const fx = Math.cos(st.yaw);
						const fy = Math.sin(st.yaw);
						const along = vx * fx + vy * fy;
						const alongQ = q.vx * fx + q.vy! * fy;
						const acc = ((along - alongQ) * 1000) / PSTEP;
						const tx = chest.x - hip.x;
						const ty = chest.y - hip.y;
						const tz = chest.z - hip.z;
						const fwd = tx * fx + ty * fy;
						const side = -tx * fy + ty * fx;
						const pitchDeg = (Math.atan2(fwd, tz) * 180) / Math.PI;
						const sideDeg = (Math.atan2(side, tz) * 180) / Math.PI;
						if (along > 4 && acc > 10 && along < 14) {
							pitch.start.push(pitchDeg);
						} else if (along > 10 && Math.abs(acc) < 3) {
							pitch.steady.push(pitchDeg);
						} else if (along > 3 && acc < -10) {
							pitch.stop.push(pitchDeg);
						}
						// Turning: how fast his way swings, times his speed (the
						// pull to the inside of the turn); positive to his left.
						if (p.v > 8 && q.v > 8) {
							const sw =
								((Math.atan2(vy, vx) - Math.atan2(q.vy!, q.vx) + 3 * Math.PI) %
									(2 * Math.PI)) -
								Math.PI;
							const cen = ((sw * 1000) / PSTEP) * p.v;
							if (Math.abs(cen) > 15) {
								bank.push(Math.sign(cen) * sideDeg);
							}
						}
					}
				}
				last.set(tr.pid, p);
			}
		}
	}
	const mean = (xs: number[]) =>
		xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : Number.NaN;
	const liveMin = (liveFrames * STEP) / 60_000;
	return {
		game,
		liveMin: r1(liveMin),
		movement: {
			slideIn: r2(slideSum / Math.max(1, planted)),
			slideShare: r2(slideBig / Math.max(1, planted)),
			pitchStart: r1(mean(pitch.start)),
			pitchSteady: r1(mean(pitch.steady)),
			pitchStop: r1(mean(pitch.stop)),
			bankTurn: r1(mean(bank)),
			jumpsPerMin: r1(jumps / Math.max(0.1, posed / 60_000)),
			jumpsBy: top(jumpsBy),
			slideBy: top(slideBy),
		},
		tempo: {
			mph: r2((sumV / Math.max(1, n)) * 0.6818),
			standing: r2(still / Math.max(1, n)),
			standingBy: Object.fromEntries(
				Object.entries(stillBy).map(([k, [a, b]]) => [
					k,
					r2(a / Math.max(1, b)),
				]),
			),
			over15: r2(fast / Math.max(1, n)),
			lockstep: r2(lock / Math.max(1, setOffs.length)),
			setOffs: setOffs.length,
			possS: r1(mean(possLive)),
		},
		screens: {
			staged: screens.length,
			ball: ballScreens.length,
			handoffs: screens.filter((s) => s.handoff).length,
			coverage,
			onHisMan: r2(onHis / Math.max(1, ballScreens.length)),
			used: r2(used / Math.max(1, ballScreens.length)),
			useS: r2(median(useS)),
			found: r2(found / Math.max(1, ballScreens.length)),
			detectedPerPoss: r2(detected.length / possN),
			looks,
		},
		contact: {
			perPoss,
			gapMedian: r2(median(contacts.map((c) => c.gap))),
			through: r2(
				contacts.filter((c) => c.gap < 1).length / Math.max(1, contacts.length),
			),
			reacted: r2(
				hard.filter((c) => c.reacted).length / Math.max(1, hard.length),
			),
			hard: hard.length,
		},
	};
};
