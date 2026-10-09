import { assert, describe, test } from "vitest";
import { compileCourt, type CourtPlayer, type RawEvent } from "./director.ts";
import { evalPlayer, poseOf, withBody } from "./evaluate.ts";
import { bodyOf, skeleton } from "./poses.ts";

const body = bodyOf();

// A trip, then a man hurt with what the box score says he hurt.
const hurtWith = (type: string, games: number) => {
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
				...(pid === 3 ? { injury: { type, games } } : {}),
			});
			events.push({ type: "stat", t: raw, pid, s: "gs", amt: 1 });
		}
	}
	events.push(
		{ type: "jumpBall", t: 0, pid: 5, pid2: 15, clock: 720 },
		{ type: "fgaMidRange", t: 0, pid: 2, clock: 704, desperation: false },
		{ type: "missMidRange", t: 0, pid: 2, clock: 703 },
		{ type: "drb", t: 1, pid: 15, clock: 702.5 },
		{ type: "injury", t: 0, pid: 3, clock: 698 },
		{ type: "sub", t: 0, pids: [6], pidsOff: [3], clock: 698 },
		{ type: "gameOver" },
	);
	players.push({ pid: 6, team: 1, pos: "G" });
	return compileCourt({ events, players, gid: 1 });
};

describe("3D injuries", () => {
	test("he goes down the way what he hurt has him", () => {
		const cases: [string, number, string, boolean][] = [
			["Torn ACL", 60, "hurtKnee", true],
			["Sprained Ankle", 4, "hurtAnkle", true],
			["Sore Ankle", 1, "hurt", false],
			["Concussion", 8, "hurtHead", false],
			["Fractured Finger", 10, "hurtHand", false],
			["Dislocated Shoulder", 20, "hurtArm", false],
			["Back Spasms", 2, "hurt", false],
		];
		for (const [type, games, anim, down] of cases) {
			const tl = hurtWith(type, games);
			const a = tl.tracks.get(3)!.acts.find((x) => x.anim.startsWith("hurt"));
			assert.strictEqual(a?.anim, anim, type);
			// Down on the floor - sat on it, not floating over it - or on his
			// feet.
			const st = withBody(evalPlayer(tl, 3, (a!.t0 + a!.t1) / 2), body);
			const sk = skeleton(body, poseOf(st));
			assert.strictEqual(sk.pelvis.u < 1.2, down, `${type}: ${sk.pelvis.u}`);
			// Teammates come over.
			const P = evalPlayer(tl, 3, a!.t1 - 100);
			const near = [1, 2, 4, 5].filter((q) => {
				const Q = evalPlayer(tl, q, a!.t1 - 100);
				return Math.hypot(Q.x - P.x, Q.y - P.y) < 4.5;
			});
			assert.isAtLeast(near.length, 1, type);
		}
	}, 60_000);
});
