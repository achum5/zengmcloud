import { assert, describe, test } from "vitest";
import { crewAt, crewFor, type CrewMember } from "./crew.ts";
import { cameraCuts } from "./evaluate.ts";
import { benchX, COURT_H, COURT_W } from "./geometry.ts";
import { compile, gidOf } from "./testGame.ts";

const AWAY = {
	region: "Golden State",
	name: "Warriors",
	colors: ["#1d428a", "#ffc72c", "#ffffff"] as [string, string, string],
};
const HOME = {
	region: "Chicago",
	name: "Bulls",
	colors: ["#ce1141", "#000000", "#ffffff"] as [string, string, string],
};

const SIGNALS = new Set(["signalUp", "signalSide", "threeUp", "travel"]);
const isRef = (pid: number) => pid >= -3 && pid <= -1;

// A game and everyone working it.
const working = (seed: string) => {
	const { events, tl } = compile(seed);
	return { events, tl, crew: crewFor(gidOf(seed), AWAY, HOME) };
};

describe("2.5D crew", () => {
	test("the same people work a game every time it is shown", () => {
		const a = crewFor(77, AWAY, HOME);
		assert.deepEqual(crewFor(77, AWAY, HOME), a);
		const count = (crew: CrewMember[], role: CrewMember["role"]) =>
			crew.filter((m) => m.role === role).length;
		assert.strictEqual(count(a, "ref"), 3);
		assert.strictEqual(count(a, "coach"), 2);
		// No caps, Santa hats or headbands on officials and coaches.
		for (const seed of [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) {
			for (const m of crewFor(seed, AWAY, HOME)) {
				assert.strictEqual(m.face.accessories.id, "none", m.role);
			}
		}
		assert.isAbove(count(a, "photo"), 3);
		// A team's coach is its own, whoever and wherever it plays.
		const b = crewFor(912, HOME, AWAY);
		const coach = (crew: CrewMember[], team: 0 | 1) =>
			crew.find((m) => m.role === "coach" && m.team === team)!;
		assert.deepEqual(coach(b, 1).face, coach(a, 0).face);
		assert.deepEqual(coach(b, 0).face, coach(a, 1).face);
		const { tl, crew } = working("crew");
		for (const t of [500, 40_000, 120_000]) {
			assert.deepEqual(crewAt(tl, t, crew), crewAt(tl, t, crew));
		}
	});

	// Officials run with the play, so in the picture they never pop from
	// one place to another - except where the picture cuts.
	test("the officials keep to the floor and run - never jump, but at a cut", () => {
		for (const seed of ["a", "b"]) {
			const { tl, crew } = working(seed);
			const cuts = cameraCuts(tl);
			let prev: Map<number, { x: number; y: number }> | undefined;
			let prevT = 0;
			for (let t = 0; t <= tl.end; t += 50) {
				const refs = crewAt(tl, t, crew).states.filter((s) => isRef(s.pid));
				assert.strictEqual(refs.length, 3);
				for (const r of refs) {
					assert.isTrue(
						r.x >= -3 && r.x <= COURT_W + 3 && r.y >= -1 && r.y <= COURT_H + 2,
						`${seed} ${t} ${r.pid} at ${r.x}, ${r.y}`,
					);
				}
				if (prev && !cuts.some((c) => c > prevT && c <= t)) {
					for (const r of refs) {
						const p = prev.get(r.pid)!;
						// Thirty feet a second is a sprint.
						assert.isBelow(
							Math.hypot(r.x - p.x, r.y - p.y),
							1.5,
							`${seed} ${t} ${r.pid}`,
						);
					}
				}
				prev = new Map(refs.map((r) => [r.pid, { x: r.x, y: r.y }]));
				prevT = t;
			}
		}
	}, 60_000);

	test("every whistle is signaled, and a made three gets both arms up", () => {
		for (const seed of ["a", "b", "c"]) {
			const { tl, crew } = working(seed);
			let calls = 0;
			let threes = 0;
			tl.fx.forEach((f, i) => {
				// Another whistle right behind it takes over.
				const next = tl.fx
					.slice(i + 1)
					.find((g) => g.kind === "whistle" || g.kind === "roar");
				if (next && next.t < f.t + 700) {
					return;
				}
				const refs = crewAt(tl, f.t + 350, crew).states.filter((s) =>
					isRef(s.pid),
				);
				if (f.kind === "whistle" && f.call) {
					assert.isTrue(
						refs.some((r) => SIGNALS.has(r.anim)),
						`${seed}: ${f.call} at ${f.t}`,
					);
					calls += 1;
				} else if (f.kind === "roar" && f.what === "three") {
					assert.isTrue(
						refs.some((r) => r.anim === "threeUp"),
						`${seed}: three at ${f.t}`,
					);
					threes += 1;
				}
			});
			assert.isAbove(calls, 10);
			assert.isAbove(threes, 0);
		}
	});

	test("at the line, the official under the basket bounces the shooter the ball", () => {
		const { tl, crew } = working("a");
		let checked = 0;
		for (const g of tl.ball) {
			if (
				g.kind !== "fly" ||
				"pid" in g.from ||
				"pid" in g.to ||
				g.from.z < 3 ||
				g.to.z > 1
			) {
				continue;
			}
			const from = g.from;
			const refs = crewAt(tl, g.t0 - 60, crew).states.filter((s) =>
				isRef(s.pid),
			);
			const near = Math.min(
				...refs.map((r) => Math.hypot(r.x - from.x, r.y - from.y)),
			);
			assert.isBelow(near, 1.6, `at ${g.t0}`);
			// Under the basket: nearer the end line than the line.
			assert.isTrue(from.x < 9 || from.x > COURT_W - 9, `at ${g.t0}`);
			checked += 1;
		}
		assert.isAbove(checked, 4);
	});

	test("the opening tip is thrown up by an official at center court", () => {
		const { tl, crew } = working("a");
		const toss = tl.fx.find((f) => f.kind === "toss")!;
		const refs = crewAt(tl, toss.t - 200, crew).states.filter((s) =>
			isRef(s.pid),
		);
		const tosser = refs.find((r) => r.anim === "toss");
		assert.isDefined(tosser);
		assert.isBelow(
			Math.hypot(tosser!.x - COURT_W / 2, tosser!.y - COURT_H / 2),
			2.5,
		);
	});

	test("coaches keep to their boxes, photographers to the baselines", () => {
		const { tl, crew } = working("b");
		for (let t = 0; t <= tl.end; t += 400) {
			for (const s of crewAt(tl, t, crew).states) {
				if (s.pid === -11 || s.pid === -12) {
					const team = s.pid === -11 ? 0 : 1;
					assert.isBelow(Math.abs(s.x - benchX(team)), 7, `${t}`);
					assert.isTrue(s.y > -4 && s.y < 0.5, `${t}`);
				} else if (s.pid <= -21) {
					assert.isTrue(s.x < -2 || s.x > COURT_W + 2, `${t}`);
					// Clear of the basket's stanchion and the lane under it.
					assert.isTrue(s.y < 17 || s.y > 33, `${t}`);
				}
			}
		}
	});
});
