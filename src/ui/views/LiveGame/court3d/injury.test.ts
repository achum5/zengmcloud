import { assert, describe, test } from "vitest";
import { crewAt, crewFor } from "./crew.ts";
import { compileCourt, type CourtPlayer, type RawEvent } from "./director.ts";
import { evalPlayer, poseOf, withBody } from "./evaluate.ts";
import { bodyOf, skeleton } from "./poses.ts";

const body = bodyOf();

// A trip, then a man hurt - with what the box score says he hurt (only
// known once the game is over: a game watched live has none), at `clock`.
const hurtWith = (
	injury: { type: string; games: number } | undefined,
	clock = 698,
	hurt = 3,
) => {
	const players: CourtPlayer[] = [];
	const POS = ["PG", "SG", "SF", "PF", "C"];
	const events: RawEvent[] = [{ type: "init", boxScore: {} }];
	for (const raw of [0, 1] as const) {
		for (let j = 0; j < 5; j++) {
			const pid = raw * 10 + j + 1;
			players.push({
				pid,
				team: raw === 0 ? 1 : 0,
				pos: POS[j],
				...(pid === hurt && injury ? { injury } : {}),
			});
			events.push({ type: "stat", t: raw, pid, s: "gs", amt: 1 });
		}
	}
	events.push(
		{ type: "jumpBall", t: 0, pid: 5, pid2: 15, clock: 720 },
		{ type: "fgaMidRange", t: 0, pid: 2, clock: 704, desperation: false },
		{ type: "missMidRange", t: 0, pid: 2, clock: 703 },
		{ type: "drb", t: 1, pid: 15, clock: 702.5 },
		{ type: "injury", t: 0, pid: hurt, clock },
		{ type: "sub", t: 0, pids: [6], pidsOff: [hurt], clock },
		{ type: "gameOver" },
	);
	players.push({ pid: 6, team: 1, pos: "G" });
	return compileCourt({ events, players, gid: 1 });
};

describe("3D injuries", () => {
	// How he goes down is the same on every device, watched live or again
	// after: picked by who he is and when, not by what he hurt - which the
	// game only knows once it is over.
	test("he goes down the same way whatever the box score later says he hurt", () => {
		const acts = (tl: ReturnType<typeof hurtWith>) =>
			JSON.stringify(tl.tracks.get(3)!.acts);
		const none = acts(hurtWith(undefined));
		assert.strictEqual(acts(hurtWith({ type: "Torn ACL", games: 60 })), none);
		assert.strictEqual(acts(hurtWith({ type: "Back Spasms", games: 2 })), none);
	}, 60_000);

	test("down on the floor or on his feet, his teammates over and the trainer out to him if he is down", () => {
		const DOWN = new Set(["hurtKnee", "hurtAnkle"]);
		const UP = new Set(["hurt", "hurtHead", "hurtHand", "hurtArm"]);
		const seen = { down: 0, up: 0 };
		const team = (region: string) => ({
			region,
			name: region,
			colors: ["#8c1d40", "#f2c14e", "#ffffff"] as [string, string, string],
		});
		const crew = crewFor(1, team("Road"), team("Home"));
		const trainers = new Set(
			crew.filter((m) => m.role === "trainer").map((m) => m.pid),
		);
		for (const [clock, hurt] of [
			[698, 3],
			[698, 1],
			[698, 4],
			[697, 2],
			[696, 5],
			[695, 3],
			[694, 1],
			[693, 4],
		] as const) {
			const tl = hurtWith(undefined, clock, hurt);
			const a = tl.tracks
				.get(hurt)!
				.acts.find((x) => x.anim.startsWith("hurt"));
			assert.isTrue(
				!!a && (DOWN.has(a.anim) || UP.has(a.anim)),
				`${clock}: ${a?.anim}`,
			);
			const down = DOWN.has(a!.anim);
			seen[down ? "down" : "up"] += 1;
			// Down on the floor - sat on it, not floating over it - or on his
			// feet.
			const st = withBody(evalPlayer(tl, hurt, (a!.t0 + a!.t1) / 2), body);
			const sk = skeleton(body, poseOf(st));
			assert.strictEqual(sk.pelvis.u < 1.2, down, `${clock}: ${sk.pelvis.u}`);
			// Teammates come over.
			const P = evalPlayer(tl, hurt, a!.t1 - 100);
			const near = [1, 2, 3, 4, 5, 6].filter((q) => {
				if (q === hurt) {
					return false;
				}
				const Q = evalPlayer(tl, q, a!.t1 - 100);
				return Math.hypot(Q.x - P.x, Q.y - P.y) < 4.5;
			});
			assert.isAtLeast(near.length, 1, `${clock}`);
			// Down, his team's trainer comes out to him and gets down beside
			// him.
			const by = crewAt(tl, a!.t1 - 300, crew).states.filter(
				(c) => trainers.has(c.pid) && Math.hypot(c.x - P.x, c.y - P.y) < 2.5,
			);
			assert.strictEqual(by.length, down ? 1 : 0, `${clock}`);
			if (down) {
				assert.strictEqual(by[0]!.anim, "coachKneel", `${clock}`);
			}
		}
		assert.isAbove(seen.down, 0, JSON.stringify(seen));
		assert.isAbove(seen.up, 0, JSON.stringify(seen));
	}, 120_000);
});
