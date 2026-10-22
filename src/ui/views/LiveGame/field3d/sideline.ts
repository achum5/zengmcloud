import { makeCourtRng } from "../courtRng.ts";
import { cheerAt } from "./crowd.ts";
import type { FieldTimeline, Play } from "./director.ts";
import type { FieldMoment } from "./evaluate.ts";
import {
	BENCH_OFF,
	COACH_LINE,
	FIELD_W,
	MID_X,
	TEAM_AREA,
	type Pt,
	type Side,
} from "./geometry.ts";
import type { RosterMan } from "./personnel.ts";
import { liftOf, type Move } from "./poses.ts";
import type { FieldPlayerState } from "./sprite.ts";

// THE SIDELINES: each team on its own, the home team across the field and
// the visitors on the near side under the press box - between the 25-yard
// lines (the NFL's team area), behind the coaches' yellow line twelve feet
// off the field, the men not in for the snap standing two deep, the
// defense's together and the offense's, a few back on the benches thirty
// feet off; the kickers down at the end of it by their net; the coaches up
// in front, over toward midfield. Each keeps his place all game, and watches
// the ball. A score, a takeaway, a big play, and the side that made it is up
// and jumping.

// The benches: along the middle of the team area, a few long ones end to
// end.
export const BENCHES = [
	[MID_X - 52, MID_X - 28],
	[MID_X - 24, MID_X],
	[MID_X + 4, MID_X + 28],
] as const;
// Which sideline a team has (0 the far one).
export const sidelineOf = (team: Side): 0 | 1 => (team === 1 ? 0 : 1);
// The kicking net: near the end of the team area - the left end across the
// field, the right on the near side.
export const netX = (line: 0 | 1) =>
	line === 0 ? TEAM_AREA[0] + 9 : TEAM_AREA[1] - 9;
const yOf = (line: 0 | 1, off: number) => (line === 0 ? -off : FIELD_W + off);

export type SidelineMan = {
	id: number;
	team: Side;
	at: Pt;
	move: Move;
	// Sat on a bench; a coach, never in the game.
	seated?: boolean;
	coach?: boolean;
	// How far he will turn from facing the field to watch the ball.
	turn: number;
	// How he takes a big play of his side's: jumps, an arm up, both.
	cheer: Move;
	delay: number;
};

const OFFENSE = new Set(["QB", "RB", "FB", "WR", "TE", "OL"]);
const KICKERS = new Set(["K", "P"]);
const STANDING: Move[] = [
	"stand",
	"stand",
	"stand",
	"onHips",
	"armsFolded",
	"hands",
];
const CHEERS: Move[] = ["leap", "leap", "ballUp", "flex"];
// Two deep, this far apart (feet), and how far each may be off his spot.
const COL = 3.8;
const ROW = 3.1;
const FRONT = COACH_LINE + 1.6;
const JITTER = 0.9;
const SEATED = 0.16;
const SEAT = 2.6;
// The coaches: a head coach and his assistants, ids of their own.
const COACHES = 4;
export const coachId = (team: Side, i: number) => -100 - team * 10 - i;

export const sidelineFor = (
	roster: RosterMan[],
	gid: number,
): SidelineMan[] => {
	const out: SidelineMan[] = [];
	for (const team of [0, 1] as Side[]) {
		const line = sidelineOf(team);
		// (Along the sideline from the camera's left, the defense, then the
		// offense; the other way round across the field.)
		const lean = line === 0 ? 1 : -1;
		const men = roster
			.filter((p) => p.team === team)
			.sort((a, b) => a.pid - b.pid);
		const rng = makeCourtRng(`sideline|${gid}|${team}`);
		const seats: Pt[] = [];
		for (const [x0, x1] of BENCHES) {
			for (let x = x0 + SEAT / 2; x < x1; x += SEAT) {
				seats.push({ x, y: yOf(line, BENCH_OFF + 0.7) });
			}
		}
		const groups: [RosterMan[], number][] = [
			[men.filter((p) => !OFFENSE.has(p.pos) && !KICKERS.has(p.pos)), -1],
			[men.filter((p) => OFFENSE.has(p.pos)), 1],
		];
		const watch = (r: () => number) => 0.5 + r() * 0.7;
		const common = (r: () => number) => ({
			turn: watch(r),
			cheer: CHEERS[Math.floor(r() * CHEERS.length)]!,
			delay: r() * 500,
		});
		for (const [group, way] of groups) {
			const standing: RosterMan[] = [];
			for (const p of group) {
				const r = makeCourtRng(`sideline|${gid}|${p.pid}`);
				const seat =
					r() < SEATED
						? seats.splice(Math.floor(r() * seats.length), 1)[0]
						: undefined;
				if (seat) {
					out.push({
						id: p.pid,
						team,
						at: seat,
						move: "sit",
						seated: true,
						...common(r),
					});
				} else {
					standing.push(p);
				}
			}
			const cols = Math.ceil(standing.length / 2);
			const cx = MID_X + way * lean * (cols * COL * 0.5 + 6);
			standing.forEach((p, k) => {
				const r = makeCourtRng(`sideline|${gid}|${p.pid}|spot`);
				const col = Math.floor(k / 2);
				const x = cx + (col - (cols - 1) / 2) * COL + (r() - 0.5) * 2 * JITTER;
				const off = FRONT + (k % 2) * ROW + (r() - 0.5) * 2 * JITTER;
				out.push({
					id: p.pid,
					team,
					at: { x, y: yOf(line, off) },
					move: STANDING[Math.floor(r() * STANDING.length)]!,
					...common(r),
				});
			});
		}
		// The kickers by their net.
		men
			.filter((p) => KICKERS.has(p.pos))
			.forEach((p, k) => {
				const r = makeCourtRng(`sideline|${gid}|${p.pid}|net`);
				out.push({
					id: p.pid,
					team,
					at: {
						x: netX(line) + (line === 0 ? 1 : -1) * (5 + k * 3.5),
						y: yOf(line, FRONT + 2 + r() * 2),
					},
					move: "stand",
					...common(r),
				});
			});
		// The coaches, out in front of their players.
		for (let i = 0; i < COACHES; i++) {
			const x =
				i === 0
					? MID_X + (rng() - 0.5) * 30
					: MID_X + lean * (i === 1 ? -1 : 1) * (18 + rng() * 22);
			out.push({
				id: coachId(team, i),
				team,
				at: { x, y: yOf(line, COACH_LINE - 1.8 - rng() * 1.6) },
				move: i === 0 ? "armsFolded" : rng() < 0.5 ? "stand" : "onHips",
				coach: true,
				turn: 0.9,
				cheer: "ballUp",
				delay: rng() * 300,
			});
		}
	}
	return out;
};

// Who is on the field for a play: everyone else is on his sideline.
const inFor = new WeakMap<Play, Set<number>>();
const onField = (play: Play) => {
	let s = inFor.get(play);
	if (!s) {
		s = new Set(play.parts.map((p) => p.pid));
		inFor.set(play, s);
	}
	return s;
};

// How long one jump takes (ms), and how far into a cheer the side's men are
// up for it.
const LEAP_MS = 900;
const UP_LEVEL = 0.55;

export const sidelineAt = (
	men: SidelineMan[],
	tl: FieldTimeline,
	m: FieldMoment,
): FieldPlayerState[] => {
	const play = m.play;
	if (!play) {
		return [];
	}
	const out: FieldPlayerState[] = [];
	const on = onField(play);
	const cheer = cheerAt(tl, m.t);
	const ball = m.focus;
	for (const s of men) {
		if (on.has(s.id)) {
			continue;
		}
		const line = s.at.y < 0 ? 0 : 1;
		const facing = line === 0 ? Math.PI / 2 : -Math.PI / 2;
		let yaw = facing;
		if (ball && !s.seated) {
			let d = Math.atan2(ball.y - s.at.y, ball.x - s.at.x) - facing;
			d = Math.atan2(Math.sin(d), Math.cos(d));
			yaw = facing + Math.max(-s.turn, Math.min(s.turn, d));
		}
		let move = s.move;
		let phase = 0;
		let arms: FieldPlayerState["arms"];
		if (cheer && cheer.side === s.team && cheer.level > UP_LEVEL) {
			const e = cheer.since - s.delay;
			if (e > 0) {
				if (s.seated) {
					arms = { move: "ballUp", phase: 0 };
				} else {
					move = s.cheer;
					phase =
						s.cheer === "leap" || s.cheer === "flex"
							? (e / (s.cheer === "leap" ? LEAP_MS : 2 * LEAP_MS)) % 1
							: 0;
				}
			}
		}
		out.push({
			id: s.id,
			team: s.team,
			x: s.at.x,
			y: s.at.y,
			z: liftOf(move, phase),
			yaw,
			move,
			phase,
			...(arms ? { arms } : {}),
		});
	}
	return out;
};
