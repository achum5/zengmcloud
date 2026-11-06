// The shots of a staged game, and where each was let go from - read off the
// court's own timeline (see gamecast/shots.ts), so a shot chart puts every
// shot exactly where the 3D court shows it taken.

import type { CourtTimeline } from "./director.ts";
import { evalPlayer } from "./evaluate.ts";
import { COURT_H, rimX } from "./geometry.ts";
import type { ShotSpot } from "../gamecast/shots.ts";

export const shotSpots = (tl: CourtTimeline): ShotSpot[] => {
	const out: ShotSpot[] = [];
	const beats = tl.beats;
	let k = 0;
	// (One a line: the shot, not a pass on the way to it.)
	let done = -1;
	for (const s of tl.ball) {
		if (
			s.kind !== "fly" ||
			!("pid" in s.from) ||
			"pid" in s.to ||
			s.from.pid < 0
		) {
			continue;
		}
		while (k + 1 < beats.length && beats[k + 1]!.preStart <= s.t0) {
			k += 1;
		}
		const beat = beats[k];
		if (
			!beat ||
			!beat.type.startsWith("fga") ||
			s.t0 >= beat.end ||
			done === k
		) {
			continue;
		}
		// (Its result is the next line - made, missed, blocked or fouled.)
		const result = beats[k + 1];
		const pid = s.from.pid;
		const tr = tl.tracks.get(pid);
		if (!result || !tr) {
			continue;
		}
		// At his own rim - the shot, not a pass to somewhere on the floor or
		// the other way.
		const to = s.to as { x: number; y: number };
		if (
			Math.abs(to.x - rimX(tr.team)) > 4 ||
			Math.abs(to.y - COURT_H / 2) > 4
		) {
			continue;
		}
		done = k;
		const P = evalPlayer(tl, pid, s.t0);
		out.push({
			cursor: result.i + 1,
			pid,
			side: tr.team === 1 ? 1 : 0,
			x: P.x,
			y: P.y,
			made: /^(fg|tp)/.test(result.type),
			three: beat.type === "fgaTp" || beat.type === "fgaTpFake",
		});
	}
	return out;
};
