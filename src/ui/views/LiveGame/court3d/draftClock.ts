import type { Race } from "../../../../common/types.ts";
import type { Authored } from "./authored.ts";
import type { CrewMember } from "./crew.ts";
import type { CourtTimeline } from "./director.ts";
import { withBody, type PlayerState } from "./evaluate.ts";
import {
	castState,
	familyOf,
	groundAt,
	kinOf,
	mix3,
	NO_BALL,
	OPENING,
	OPENING_LOOK,
	OPENING_WIDTH,
	placesOf,
	podiumCam,
	shotOf,
	takeOf,
	type Beat,
	type Cam,
	type DraftPlaces,
	type DraftRoom,
	type DraftTake,
} from "./draftStage.ts";

// THE ROOM BETWEEN PICKS (see draftStage.ts for the room, ClockStage.tsx for
// the page): a team on the clock, its colors on the LED wall, the podium
// empty - and the cameras going round the room, up to the wall, and in on
// the best still on the board, sat at their families' tables. The same
// tables, the same families, as when each one's name is read (see takeOf).
// Round and round till the pick is in.

export const CLOCK_LOOP = 32_000;
// How long it runs: as long as anybody will be on the clock.
const CLOCK_MS = 6 * 60 * 60 * 1000;

export type ClockShot =
	| { kind: "room"; side: -1 | 0 | 1 }
	| { kind: "wall" }
	| { kind: "podium" }
	| { kind: "prospect"; i: number };

// What the cameras are on at t (and from when to when, in the loop), with
// so many prospects to go to.
export const clockShotAt = (
	t: number,
	prospects: number,
): { shot: ClockShot; t0: number; t1: number } => {
	const loop = Math.floor(t / CLOCK_LOOP);
	const u = t - loop * CLOCK_LOOP;
	const side = loop % 2 === 0 ? 1 : -1;
	if (u < 7000) {
		return { shot: { kind: "room", side: 0 }, t0: 0, t1: 7000 };
	}
	if (u < 12_500) {
		return { shot: { kind: "wall" }, t0: 7000, t1: 12_500 };
	}
	if (u < 19_000) {
		return {
			shot: prospects > 0 ? { kind: "prospect", i: 0 } : { kind: "podium" },
			t0: 12_500,
			t1: 19_000,
		};
	}
	if (u < 25_500) {
		return {
			shot: prospects > 1 ? { kind: "prospect", i: 1 } : { kind: "podium" },
			t0: 19_000,
			t1: 25_500,
		};
	}
	return { shot: { kind: "room", side }, t0: 25_500, t1: CLOCK_LOOP };
};

const smooth = (u: number) => {
	const v = Math.min(1, Math.max(0, u));
	return v * v * (3 - 2 * v);
};

export type ClockProspect = { pid: number; race?: Race };
type Seated = ClockProspect & { take: DraftTake; g: DraftPlaces; k: number };

const sitting = (pid: number, at: DraftPlaces["seat"], t: number) =>
	castState(
		pid,
		[{ t0: 0, t1: Infinity, at, yaw: at.yaw, anim: "chairSit" } as Beat],
		t,
		groundAt,
	);

export const stageOnTheClock = (
	prospects: ClockProspect[],
): Pick<
	Authored,
	"show" | "ms" | "moment" | "aim" | "crew" | "cast" | "castAt"
> & {
	room: DraftRoom;
	seated: number[];
} => {
	// Each at his own table - two of them at most, and not two at one.
	const seated: Seated[] = [];
	for (const p of prospects) {
		const take = takeOf(p.pid);
		const g = placesOf(take);
		if (
			seated.some((s) => s.g.table.x === g.table.x && s.g.table.y === g.table.y)
		) {
			continue;
		}
		seated.push({ ...p, take, g, k: seated.length });
		if (seated.length === 2) {
			break;
		}
	}
	// His family round him (see kinOf): their own pids, a family apart.
	const kinPid = (k: number, j: number) => -210 - 10 * k - j;
	const cast: CrewMember[] = seated.flatMap((s) => [
		kinOf(s.take.hugger, s.pid, s.race, kinPid(s.k, 0)),
		...(s.take.other === "none"
			? []
			: [kinOf(s.take.other, s.pid, s.race, kinPid(s.k, 1))]),
	]);

	const cameraAt = (t: number, narrow: boolean): Cam => {
		const k = narrow ? 0.82 : 1;
		const { shot, t0, t1 } = clockShotAt(t, seated.length);
		const u = smooth(((t % CLOCK_LOOP) - t0) / (t1 - t0));
		if (shot.kind === "room") {
			// From high in the stands at the back: down the middle, or a
			// corner of it.
			const [from, to] = OPENING[shot.side];
			return {
				at: mix3(from, to, u),
				look: OPENING_LOOK,
				width: (OPENING_WIDTH + 4 - 8 * u) * k,
			};
		}
		if (shot.kind === "wall") {
			// Up at the wall: the team's logo the size of a house.
			return {
				at: mix3({ x: 45, y: 30, z: 9 }, { x: 49, y: 25, z: 10 }, u),
				look: { x: 47, y: -10, z: 13.5 },
				width: (52 - 5 * u) * k,
			};
		}
		if (shot.kind === "podium") {
			return podiumCam(1, u, k);
		}
		// In on him at his table, across it - as when his name is read.
		const { g } = seated[shot.i]!;
		return {
			at: { x: g.tableCam.x, y: g.tableCam.y, z: 5.8 },
			look: { x: g.tableLook.x, y: g.tableLook.y, z: 3.2 },
			width: (10.5 - 1.2 * u) * k,
		};
	};

	const tl: CourtTimeline = {
		tracks: new Map(),
		ball: [],
		fx: [],
		beats: [],
		poss: [[0, 1]],
		cuts: [],
		shots: [],
		tension: [],
		fast: [],
		warmup: true,
		end: CLOCK_MS,
	};

	return {
		show: { tl, parts: [tl] },
		ms: CLOCK_MS,
		moment: (t, bodyFor) => ({
			t,
			players: seated.map((s) =>
				withBody(sitting(s.pid, s.g.seat, t), bodyFor(s.pid)),
			),
			ball: NO_BALL,
		}),
		aim: (t, narrow) => shotOf(cameraAt(t, narrow)),
		crew: () => false,
		cast,
		castAt: (t) =>
			seated.flatMap((s): PlayerState[] => [
				sitting(kinPid(s.k, 0), s.g.kinSeat, t),
				...(s.take.other === "none"
					? []
					: [sitting(kinPid(s.k, 1), s.g.otherSeat, t)]),
			]),
		room: {
			families: seated.map((s) => familyOf(s.g)),
			upAt: () => false,
			clapAt: () => false,
			jetFrom: Infinity,
		},
		seated: seated.map((s) => s.pid),
	};
};
