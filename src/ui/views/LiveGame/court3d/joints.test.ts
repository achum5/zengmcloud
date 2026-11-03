import { assert, test } from "vitest";
import {
	evalPlayer,
	gripped,
	holdOf,
	onRim,
	poseOf,
	shownAt,
	withBody,
} from "./evaluate.ts";
import { bodyOf, skeleton, type Limb, type V3 } from "./poses.ts";
import { compile } from "./testGame.ts";

// HIS JOINTS GO WHERE A MAN'S GO. Through whole games, every man on the
// floor, sampled: no foot down wrung round from where its leg points past
// what a hip and a knee turn; no bone of an arm or a leg drawn longer or
// shorter than it is - but for a cartoon's reach up over that big head,
// both bones of the arm the same way; no arm swung back behind him past
// what a shoulder does, no elbow folded past what an elbow does, no wrist
// bent past what a wrist does.

const body = bodyOf();
const len = (a: V3, b: V3) => Math.hypot(a.f - b.f, a.s - b.s, a.u - b.u);

const FOOT_MOST = 50;
const SHOULDER_BACK = -75;
const ELBOW_MOST = 155;
const WRIST_MOST = 90;
// (A bone's length as drawn, to its own: within this much - and an arm up
// over his head, its two bones stretched together, by no more than this.)
const BONE = 0.015;
const REACH_MOST = 1.17;

test("his joints go where a man's go", () => {
	const bad: Record<string, string[]> = {};
	const note = (what: string, ex: string) => {
		const list = (bad[what] ??= []);
		if (list.length < 3) {
			list.push(ex);
		}
	};
	let samples = 0;
	for (const seed of ["joints-a", "joints-b"]) {
		const { tl, players } = compile(seed, 45);
		for (let t = 0; t < tl.end; t += 137) {
			if (tl.fast.some(([a, b]) => t >= a && t < b)) {
				continue;
			}
			for (const p of players) {
				if (!shownAt(tl, p.pid, t)) {
					continue;
				}
				const st0 = evalPlayer(tl, p.pid, t);
				if (!st0.shown) {
					continue;
				}
				samples++;
				const st = withBody(st0, body);
				const q = poseOf(st);
				const at = `${seed} @${t} #${p.pid} ${st.anim}`;
				for (const [name, v] of [
					["foot", q.footN],
					["foot", q.footF],
				] as const) {
					if (Math.abs(v) > FOOT_MOST) {
						note(`${name} turned ${Math.round(v)}`, at);
					}
				}
				for (const v of [q.shN, q.shF]) {
					if (v < SHOULDER_BACK) {
						note("shoulder swung back past a shoulder's", `${at} ${v}`);
					}
				}
				for (const v of [q.elN, q.elF]) {
					if (v > ELBOW_MOST) {
						note("elbow folded past an elbow's", `${at} ${v}`);
					}
				}
				for (const v of [q.wrN, q.wrF]) {
					if (Math.abs(v) > WRIST_MOST) {
						note("wrist bent past a wrist's", `${at} ${v}`);
					}
				}
				const held =
					st.holding || st.grip !== undefined ? holdOf(st, body, q) : undefined;
				const sk = held
					? gripped(held.sk, st, body, q)
					: onRim(skeleton(body, q), st, body);
				const bones = (l: Limb, a: number, b: number, arm: boolean) => {
					const r1 = len(l.root, l.mid) / a;
					const r2 = len(l.mid, l.end) / b;
					if (arm && Math.abs(r1 - r2) <= BONE * 2 && r1 >= 1 - BONE) {
						if (r1 > REACH_MOST) {
							note("arm stretched past its reach", `${at} ${r1}`);
						}
						return;
					}
					for (const r of [r1, r2]) {
						if (Math.abs(r - 1) > BONE) {
							note(
								`${arm ? "arm" : "leg"} bone ${r > 1 ? "stretched" : "shrunk"}`,
								`${at} ${r.toFixed(3)}`,
							);
						}
					}
				};
				bones(sk.armR, body.upper, body.fore, true);
				bones(sk.armL, body.upper, body.fore, true);
				bones(sk.legR, body.thigh, body.shin, false);
				bones(sk.legL, body.thigh, body.shin, false);
			}
		}
	}
	assert.isAbove(samples, 5000);
	assert.deepStrictEqual(bad, {});
}, 600_000);
