import { assert, beforeAll, test } from "vitest";
import { aimFor } from "./aim.ts";
import { cheerleadersAt, cheerleadersFor } from "./cheerleaders.ts";
import { cheerAt } from "./crowd.ts";
import { compileField, type FieldTimeline } from "./director.ts";
import { momentAt, playAt, type FieldMoment } from "./evaluate.ts";
import { LIMIT_LINE, SIDE_ROOM } from "./geometry.ts";
import { photographersFor } from "./media.ts";
import { simGames } from "./testGames.ts";

let tls: FieldTimeline[] = [];

// (What they go by: when it is - not where anybody is.)
const when = (tl: FieldTimeline, t: number): FieldMoment => ({
	t,
	play: playAt(tl, t),
	players: [],
});

beforeAll(async () => {
	const games = await simGames([23, 24]);
	tls = games.map(({ events, roster }, k) =>
		compileField({ events, players: roster, gid: 23 + k }),
	);
});

test("the cheerleaders in their places by the wall across the field, clear of each other and of the photographers", () => {
	for (let gid = 0; gid < 10; gid++) {
		const squad = cheerleadersFor(gid);
		assert.isAtLeast(squad.length, 12);
		const snaps = photographersFor(gid);
		for (const [k, c] of squad.entries()) {
			assert.isBelow(c.at.y, -LIMIT_LINE - 6, `${c.id}`);
			assert.isAbove(c.at.y, -SIDE_ROOM + 3, `${c.id}`);
			for (const o of squad.slice(k + 1)) {
				assert.isAtLeast(Math.hypot(o.at.x - c.at.x, o.at.y - c.at.y), 4);
			}
			for (const s of snaps) {
				assert.isAtLeast(Math.hypot(s.at.x - c.at.x, s.at.y - c.at.y), 4);
			}
		}
	}
});

test("the team runs out of the tunnel between them - they turned in to it, the poms going", () => {
	for (const tl of tls) {
		const play = tl.plays[0]!;
		const ro = play.runOut!;
		assert.isDefined(ro);
		const squad = cheerleadersFor(1);
		const lane = squad.filter((c) => c.group === 0);
		let closest = Infinity;
		for (let t = ro.out; t < ro.cut; t += 50) {
			const m = momentAt(tl, t);
			for (const st of m.players) {
				for (const c of lane) {
					closest = Math.min(closest, Math.hypot(st.x - c.at.x, st.y - c.at.y));
				}
			}
			for (const st of cheerleadersAt(lane, tl, m)) {
				assert.strictEqual(st.move, "poms");
				assert.isAbove(Math.cos(st.yaw - Math.PI / 2), 0.5);
			}
		}
		assert.isAbove(closest, 3);
	}
});

test("watching while the ball is live; the routine at a timeout; up and jumping for a big play of the home team's, never the visitors'", () => {
	const squad = cheerleadersFor(2);
	let danced = 0;
	let jumped = 0;
	for (const tl of tls) {
		for (const play of tl.plays) {
			const at = (t: number) => cheerleadersAt(squad, tl, when(tl, t));
			const mid = (play.snap + play.whistle) / 2;
			const cheer = cheerAt(tl, mid);
			if (!cheer || cheer.since > 4000) {
				for (const st of at(mid)) {
					assert.strictEqual(st.move, "onHips", `${play.i}`);
				}
			}
			const timeout = play.timeout;
			if (timeout) {
				const t = (timeout.cut + play.t1) / 2 + 600;
				if (!cheerAt(tl, t)) {
					for (const st of at(t)) {
						assert.strictEqual(st.move, "dance", `${play.i}`);
					}
					danced += 1;
				}
			}
			const c = play.cheer;
			if (c?.big) {
				const leaping = at(c.t + 700).filter((st) => st.move === "leap");
				if (c.side === 1) {
					assert.isAbove(leaping.length, squad.length / 2, `${play.i}`);
					jumped += 1;
				} else {
					assert.strictEqual(leaping.length, 0, `${play.i}`);
				}
			}
		}
	}
	assert.isAbove(danced, 0);
	assert.isAbove(jumped, 0);
});

test("the picture on them now and then for the end of a timeout, and from them to the next play", () => {
	let shots = 0;
	for (const tl of tls) {
		for (const play of tl.plays) {
			const shot = play.timeout?.cheer;
			if (!shot) {
				continue;
			}
			shots += 1;
			assert.include(tl.cuts, shot.cut);
			assert.include(tl.cuts, play.t1);
			const aim = aimFor(momentAt(tl, shot.cut + 100));
			assert.strictEqual(aim.x, shot.x);
			assert.strictEqual(aim.y, shot.y);
			// (Mid-routine.)
			const squad = cheerleadersFor(0);
			for (const st of cheerleadersAt(squad, tl, when(tl, shot.cut))) {
				assert.strictEqual(st.move, "dance");
			}
		}
	}
	assert.isAbove(shots, 0);
});
