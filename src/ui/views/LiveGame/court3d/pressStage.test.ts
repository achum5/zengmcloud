import { assert, describe, test } from "vitest";
import { bodyOf } from "./poses.ts";
import {
	AT_PODIUM,
	besideOf,
	EXEC_PID,
	lineupOf,
	planOf,
	PODIUM,
	RISER,
	stagePress,
	takeOf,
	type PressMade,
	type PressTake,
} from "./pressStage.ts";

const made = (eid: number, n: number): PressMade => ({
	eid,
	season: 2026,
	team: { tid: eid % 30, colors: ["#1d428a", "#ffc72c", "#000000"] },
	players: Array.from({ length: n }, (_, i) => ({
		pid: 1000 + eid * 10 + i,
		hgt: 74 + ((eid + i) % 12),
	})),
});
const EIDS = Array.from({ length: 160 }, (_, i) => i + 1);
const onRiser = (p: { x: number; y: number }) =>
	p.x >= RISER.x0 && p.x <= RISER.x1 && p.y >= RISER.y0 && p.y <= RISER.y1;

describe("the press conference's take", () => {
	test("is the same each time for the same move and team", () => {
		for (const eid of EIDS.slice(0, 20)) {
			assert.deepEqual(takeOf(eid, 3), takeOf(eid, 3));
		}
	});

	test("is not the same from one move to the next", () => {
		const takes = EIDS.map((eid) => takeOf(eid, eid % 30));
		const seen = (f: (t: PressTake) => unknown) =>
			new Set(takes.map((t) => JSON.stringify(f(t)))).size;
		assert.equal(
			seen((t) => t.order),
			2,
		);
		assert.equal(
			seen((t) => t.talkAt),
			2,
		);
		assert.equal(
			seen((t) => t.balls),
			2,
		);
		assert.equal(
			seen((t) => t.hold),
			2,
		);
		assert.equal(
			seen((t) => t.edition),
			2,
		);
		assert.equal(
			seen((t) => t.backdrop),
			4,
		);
		assert.isAtLeast(
			seen((t) => [...t.left, ...t.right]),
			4,
		);
		assert.isAtLeast(
			seen((t) => t.shots),
			8,
		);
	});
});

describe("its timing", () => {
	test("the pictures and the talk one after the other, the jersey up in the pictures", () => {
		for (const eid of EIDS) {
			const take = takeOf(eid, eid % 30);
			const P = planOf(take);
			const [a, b] =
				P.photo.t0 < P.talk.t0 ? [P.photo, P.talk] : [P.talk, P.photo];
			assert.equal(a.t0, 0);
			assert.equal(a.t1, b.t0);
			assert.equal(P.ms, b.t1);
			assert.isAbove(P.held, P.up);
			assert.isBelow(P.held, P.photo.t1 - 3000, `eid ${eid}`);
			assert.isAtLeast(P.gmTalk[0], P.talk.t0);
			assert.isBelow(P.gmTalk[1], P.himTalk[0]);
			assert.isAtMost(P.himTalk[1], P.talk.t1);
			assert.isAtLeast(P.himTalk[1] - P.himTalk[0], 2500, `eid ${eid}`);
		}
	});
});

describe("where they stand", () => {
	test("in a row on the riser for the pictures, not on top of each other", () => {
		for (const eid of EIDS) {
			const take = takeOf(eid, eid % 30);
			for (let n = 1; n <= 4; n++) {
				const row = lineupOf(
					take,
					Array.from({ length: n }, (_, i) => i + 1),
				);
				assert.equal(row.length, n + take.left.length + take.right.length);
				for (const r of row) {
					assert.isTrue(onRiser(r), `eid ${eid}: ${r.x}`);
				}
				for (let i = 1; i < row.length; i++) {
					assert.isAtLeast(row[i]!.x - row[i - 1]!.x, 2.6);
				}
				// (The best of them in the middle of the players.)
				const players = row.filter((r) => !r.exec);
				const mid = players[Math.floor((players.length - 1) / 2)]!;
				assert.equal(mid.pid, 1, `n ${n}`);
			}
		}
	});

	test("either side of the podium, clear of it and of each other", () => {
		for (const eid of EIDS) {
			const take = takeOf(eid, eid % 30);
			const b = besideOf(take, [1, 2, 3, 4]);
			const spots = [
				AT_PODIUM,
				...b.players.map((p) => p.at),
				...b.rest.map((p) => p.at),
			];
			for (const s of spots) {
				assert.isTrue(onRiser(s));
			}
			for (let i = 0; i < spots.length; i++) {
				for (let j = i + 1; j < spots.length; j++) {
					const d = Math.hypot(
						spots[i]!.x - spots[j]!.x,
						spots[i]!.y - spots[j]!.y,
					);
					assert.isAtLeast(d, 2.8);
				}
			}
			// (Stood behind it, not in it.)
			assert.isBelow(AT_PODIUM.y, PODIUM.y - PODIUM.d / 2 - 0.4);
			for (const s of spots.slice(1)) {
				assert.isAtLeast(Math.abs(s.x - PODIUM.x), PODIUM.w / 2 + 1.2);
			}
			assert.equal(b.gm, EXEC_PID.gm);
		}
	});
});

describe("the scene", () => {
	const at = (eid: number, n = 2) => {
		const m = made(eid, n);
		const scene = stagePress(m);
		const moment = (t: number) => scene.moment(t, () => bodyOf(78, 215));
		return { m, scene, moment };
	};

	test("jerseys held up only through the pictures; the table or the podium only through the talk", () => {
		for (const eid of EIDS.slice(0, 60)) {
			const { m, scene, moment } = at(eid);
			const P = scene.plan;
			for (let t = 0; t < P.ms; t += 400) {
				moment(t);
				const inPhoto = t >= P.photo.t0 && t < P.photo.t1;
				assert.equal(
					scene.seen.jerseys(t).length,
					inPhoto ? m.players.length : 0,
				);
				assert.equal(
					scene.seen.table(t),
					!inPhoto && scene.take.talkAt === "table",
				);
				assert.equal(
					scene.seen.podium(t),
					!inPhoto && scene.take.talkAt === "podium",
				);
				if (!inPhoto) {
					assert.equal(scene.seen.balls(t).length, 0);
				}
			}
		}
	});

	test("his jersey held out in front of him by its shoulders, its straps about a foot apart", () => {
		for (const eid of EIDS.slice(0, 40)) {
			const { scene, moment } = at(eid, 1);
			const P = scene.plan;
			const t = Math.max(P.held + 500, P.photo.t0 + 200);
			const m = moment(t);
			const me = m.players[0]!;
			const [j] = scene.seen.jerseys(t);
			assert.isDefined(j);
			const { r, l } = j!.hands;
			const apart = Math.hypot(r.x - l.x, r.y - l.y);
			assert.isAbove(apart, 0.6, `eid ${eid}`);
			assert.isBelow(apart, 1.7, `eid ${eid}`);
			assert.isBelow(Math.abs(r.z - l.z), 0.3);
			// (Toward the cameras, and up at his chest or higher.)
			assert.isAbove((r.y + l.y) / 2, me.y + 0.5);
			assert.isAbove((r.z + l.z) / 2, me.z + 3.4);
		}
	});

	test("nobody eased from sat at the table to stood for the pictures across the cut", () => {
		for (const eid of EIDS.slice(0, 60)) {
			const { scene, moment } = at(eid, 2);
			const P = scene.plan;
			const cut = Math.max(P.photo.t0, P.talk.t0);
			const sat = (anim: string) => anim.startsWith("sit");
			for (const t of [cut, cut + 100, cut + 200]) {
				for (const p of moment(t).players) {
					if (p.from) {
						assert.equal(sat(p.from.anim), sat(p.anim), `eid ${eid} at ${t}`);
					}
				}
			}
		}
	});

	test("at the podium: the general manager first, then him", () => {
		const eids = EIDS.filter(
			(eid) => takeOf(eid, eid % 30).talkAt === "podium",
		);
		assert.isAtLeast(eids.length, 20);
		for (const eid of eids) {
			const { m, scene, moment } = at(eid, 2);
			const P = scene.plan;
			const star = m.players[0]!.pid;
			const gmAt = (t: number) => {
				const i = scene.cast.findIndex((c) => c.pid === EXEC_PID.gm);
				return scene.castAt(t)[i]!;
			};
			const early = P.talk.t0 + 300;
			const late = P.talk.t1 - 300;
			moment(early);
			assert.equal(gmAt(early).anim, "podiumTalk");
			assert.closeTo(gmAt(early).x, AT_PODIUM.x, 1e-9);
			const meEarly = moment(early).players.find((p) => p.pid === star)!;
			assert.notEqual(meEarly.anim, "podiumTalk");
			const meLate = moment(late).players.find((p) => p.pid === star)!;
			assert.equal(meLate.anim, "podiumTalk");
			assert.closeTo(meLate.x, AT_PODIUM.x, 1e-9);
			assert.notEqual(gmAt(late).anim, "podiumTalk");
		}
	});

	test("everybody in the room, on their feet on the riser or the floor", () => {
		for (const eid of EIDS.slice(0, 40)) {
			const { scene, moment } = at(eid, 3);
			for (let t = 0; t < scene.ms; t += 700) {
				for (const p of moment(t).players) {
					assert.isTrue(onRiser(p), `eid ${eid} pid ${p.pid} at ${t}`);
					assert.equal(p.z, RISER.h);
				}
				for (const c of scene.castAt(t)) {
					// (Off stage: the photographers not wanted at the table.)
					if (c.x > RISER.x1 + 30) {
						continue;
					}
					assert.isAtMost(Math.abs(c.x - 47), 30);
					assert.isAtLeast(c.y, 8);
				}
			}
		}
	});
});
