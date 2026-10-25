import type { FieldTimeline } from "./director.ts";
import { playAt } from "./evaluate.ts";
import type { Side } from "./geometry.ts";

// THE CROWD ON ITS FEET: up for a big play of the home team's (and for the
// team out of its tunnel before the game), arms going -
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

// The big play the crowd is up for at t, if any - its own play's, or still
// up from the one before: for which side, how far up (0 to 1), and how long
// since it came.
export const cheerAt = (
	tl: FieldTimeline,
	t: number,
): { side: Side; big: boolean; level: number; since: number } | undefined => {
	const play = playAt(tl, t);
	const k = play ? tl.plays.indexOf(play) : -1;
	let out: ReturnType<typeof cheerAt>;
	// (The home team out of its tunnel too: see tunnel.ts.)
	for (const c of [
		tl.plays[k]?.cheer,
		tl.plays[k]?.runOut?.cheer,
		tl.plays[k - 1]?.cheer,
	]) {
		if (!c || c.t > t) {
			continue;
		}
		const e = t - c.t;
		const hold = c.hold ?? (c.big ? HOLD_BIG : HOLD);
		const level =
			e < RISE ? e / RISE : e < hold ? 1 : Math.max(0, 1 - (e - hold) / FALL);
		if (level > 0 && (!out || level > out.level)) {
			out = { side: c.side, big: c.big, level, since: e };
		}
	}
	return out;
};

export const crowdAt = (
	tl: FieldTimeline,
	t: number,
	now: number,
): { up: number; wave: boolean } => {
	const c = cheerAt(tl, t);
	const up = c ? c.level * (c.side === 1 ? 1 : ROAD) * (c.big ? 1 : 0.75) : 0;
	return { up, wave: up > 0.3 && Math.sin(now / 130) > 0 };
};
