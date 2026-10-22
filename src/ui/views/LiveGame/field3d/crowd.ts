import type { FieldTimeline } from "./director.ts";
import { playAt } from "./evaluate.ts";

// THE CROWD ON ITS FEET: up for a big play of the home team's, arms going -
// the road team's fans, a few of them - and sitting back down (the 3D
// court's, for football: see court3d/scene.ts crowdUp).

// Up over this long (ms), held, and back down; held longer for a score, a
// takeaway.
const RISE = 300;
const HOLD = 2200;
const HOLD_BIG = 3800;
const FALL = 1400;
// How much of the stadium stands for the road team.
const ROAD = 0.3;

export const crowdAt = (
	tl: FieldTimeline,
	t: number,
	now: number,
): { up: number; wave: boolean } => {
	const play = playAt(tl, t);
	const k = play ? tl.plays.indexOf(play) : -1;
	let up = 0;
	// (Its own play's, or still up from the one before.)
	for (const p of [tl.plays[k], tl.plays[k - 1]]) {
		const c = p?.cheer;
		if (!c || c.t > t) {
			continue;
		}
		const e = t - c.t;
		const hold = c.big ? HOLD_BIG : HOLD;
		const level =
			e < RISE ? e / RISE : e < hold ? 1 : Math.max(0, 1 - (e - hold) / FALL);
		const weight = (c.side === 1 ? 1 : ROAD) * (c.big ? 1 : 0.75);
		up = Math.max(up, level * weight);
	}
	return { up, wave: up > 0.3 && Math.sin(now / 130) > 0 };
};
