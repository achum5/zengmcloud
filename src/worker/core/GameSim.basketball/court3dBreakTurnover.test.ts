import { assert, beforeAll, describe, test } from "vitest";
import {
	compileCourt,
	type CourtTimeline,
} from "../../../ui/views/LiveGame/court3d/director.ts";
import { evalPlayer } from "../../../ui/views/LiveGame/court3d/evaluate.ts";
import { COURT_H, rimX } from "../../../ui/views/LiveGame/court3d/geometry.ts";
import { simRealGame } from "./realGameForTests.ts";

let tl: CourtTimeline;
beforeAll(async () => {
	const { events, players } = await simRealGame(11);
	tl = compileCourt({ events, players, gid: 1 });
}, 120_000);

describe("3D court, a real game", () => {
	// A break the sim ends in somebody else's turnover: the man pushing it
	// used to take it all the way in to the rim, kick it back out to him for
	// no reason, and he threw it straight to the other side.
	test("nobody dribbles it in to the rim and kicks it out to a man who throws it to the other side", () => {
		const bad: string[] = [];
		let lost = 0;
		for (let k = 2; k < tl.ball.length; k++) {
			const s = tl.ball[k]!;
			if (s.kind !== "fly" || !("pid" in s.from) || !("pid" in s.to)) {
				continue;
			}
			const A = tl.tracks.get(s.from.pid);
			const B = tl.tracks.get(s.to.pid);
			if (!A || !B || A.team === B.team) {
				continue;
			}
			lost += 1;
			// How it got to him: the pass before, and the dribble before that.
			let j = k - 1;
			while (j >= 0 && tl.ball[j]!.kind === "hold") {
				j -= 1;
			}
			const kick = tl.ball[j];
			if (
				!kick ||
				kick.kind !== "fly" ||
				!("pid" in kick.from) ||
				!("pid" in kick.to) ||
				kick.to.pid !== s.from.pid
			) {
				continue;
			}
			const driver = kick.from.pid;
			let dribbled = 0;
			for (let q = j - 1; q >= 0; q--) {
				const z = tl.ball[q]!;
				if (z.kind !== "hold" || z.pid !== driver) {
					break;
				}
				if (z.style !== "hold") {
					dribbled += (tl.ball[q + 1]?.t0 ?? z.t0) - z.t0;
				}
			}
			const P = evalPlayer(tl, driver, kick.t0);
			const team = tl.tracks.get(driver)!.team;
			const d = Math.hypot(P.x - rimX(team), P.y - COURT_H / 2);
			if (d < 5 && dribbled > 1500) {
				bad.push(
					`${driver} at ${d.toFixed(1)} ft to ${s.from.pid}, at ${Math.round(kick.t0)}`,
				);
			}
		}
		assert.isAbove(lost, 5);
		assert.deepEqual(bad, []);
	}, 900_000);
});
