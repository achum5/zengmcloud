import { describe, expect, test } from "vitest";
import {
	createAdvancedPlan,
	playerHand,
	sampleAdvancedPlan,
} from "./advancedCourtMotion.ts";
import type { CourtScene } from "./LiveCourt.tsx";

const scene = (overrides: Partial<CourtScene> = {}): CourtScene => ({
	key: 1,
	seed: "game-10|event-20",
	kind: "attempt",
	t: 0,
	zone: "three",
	text: null,
	actors: [
		{ pid: 1, name: "First Shooter", x: 28, y: 7, role: "main" },
		{ pid: 2, name: "Second Passer", x: 27, y: 26, t: 0, role: "assist" },
		{ pid: 3, name: "Third Defender", x: 25, y: 9, t: 1, role: "onCourt" },
	],
	...overrides,
});

describe("advanced court choreography", () => {
	test("phone-sized players keep their court spot and the ball in their hand", () => {
		const input = scene();
		input.actors = [input.actors[0]!];
		const plan = createAdvancedPlan(input);
		const desktop = sampleAdvancedPlan(plan, 1);
		const phone = sampleAdvancedPlan(plan, 1, 1.5);
		expect(phone.players[0]).toMatchObject({
			x: desktop.players[0]!.x,
			y: desktop.players[0]!.y,
		});
		expect(phone.ball.x).toBeCloseTo(playerHand(phone.players[0]!).x);
		expect(phone.ball.owner).toBe(desktop.ball.owner);
	});

	test("continues from the displayed frame instead of snapping to the previous target", () => {
		const first = createAdvancedPlan(scene({ shooterFrom: { x: 68, y: 20 } }));
		const midway = sampleAdvancedPlan(first, 0.3);
		const next = createAdvancedPlan(
			scene({ key: 2, kind: "make", ballFrom: { x: 28, y: 7 }, rimX: 5.25 }),
			midway,
		);
		const start = sampleAdvancedPlan(next, 0);
		for (const p of midway.players) {
			expect(start.players.find((a) => a.pid === p.pid)).toMatchObject({
				x: p.x,
				y: p.y,
			});
		}
	});

	test("is deterministic and does not mutate the event or the previous frame", () => {
		const input = scene({ shooterFrom: { x: 68, y: 20 } });
		const before = structuredClone(input);
		const previous = sampleAdvancedPlan(createAdvancedPlan(scene()), 0.5);
		const previousCopy = structuredClone(previous);
		const a = sampleAdvancedPlan(createAdvancedPlan(input, previous), 0.45);
		const b = sampleAdvancedPlan(createAdvancedPlan(input, previous), 0.45);
		expect(a).toEqual(b);
		expect(input).toEqual(before);
		expect(previous).toEqual(previousCopy);
	});

	test("keeps the credited shooter on his spot while background players yield", () => {
		const input = scene();
		input.actors[2] = { ...input.actors[2]!, x: 28, y: 7 };
		const end = sampleAdvancedPlan(createAdvancedPlan(input), 1);
		const shooter = end.players.find((p) => p.pid === 1)!;
		const defender = end.players.find((p) => p.pid === 3)!;
		expect(shooter).toMatchObject({ x: 28, y: 7 });
		expect(
			Math.hypot(shooter.x - defender.x, shooter.y - defender.y),
		).toBeGreaterThanOrEqual(2.99);
	});

	test("passes from the actual assister to the actual shooter before the result", () => {
		const plan = createAdvancedPlan(scene());
		expect(sampleAdvancedPlan(plan, 0).ball.owner).toBe(2);
		expect(sampleAdvancedPlan(plan, 0.4).ball.owner).toBeUndefined();
		expect(sampleAdvancedPlan(plan, 1).ball.owner).toBe(1);
	});

	test("dribbling follows the moving handler, not his future location", () => {
		const plan = createAdvancedPlan(
			scene({
				kind: "advance",
				ballTo: { x: 28, y: 7 },
				shooterFrom: { x: 75, y: 20 },
			}),
		);
		const frame = sampleAdvancedPlan(plan, 0.25);
		const owner = frame.players.find((a) => a.pid === frame.ball.owner)!;
		expect(owner.pid).toBe(1);
		expect(frame.ball.x).toBeCloseTo(playerHand(owner).x);
		expect(frame.ball.y).toBeCloseTo(playerHand(owner).y);
		expect(owner.x).toBeGreaterThan(28);
	});

	test.each([0, 1] as const)(
		"makes go through team %i's rim and misses finish away from it",
		(t) => {
			const rimX = t === 0 ? 5.25 : 88.75;
			const base = scene({ t, rimX, ballFrom: { x: t === 0 ? 28 : 66, y: 7 } });
			const made = sampleAdvancedPlan(
				createAdvancedPlan({ ...base, kind: "make" }),
				1,
			);
			const missed = sampleAdvancedPlan(
				createAdvancedPlan({ ...base, kind: "miss" }),
				1,
			);
			expect(made.ball).toMatchObject({ x: rimX, y: 25, visible: true });
			expect(made.ball.z).toBeCloseTo(0.3);
			expect(missed.ball.x).not.toBe(rimX);
			expect(made.impact?.made).toBe(true);
			expect(missed.impact?.made).toBe(false);
		},
	);

	test("rebounds start where the missed ball finished and end with the credited rebounder", () => {
		const miss = sampleAdvancedPlan(
			createAdvancedPlan(scene({ kind: "miss", rimX: 5.25 })),
			1,
		);
		const rebound = createAdvancedPlan(
			scene({
				kind: "reb",
				actors: [
					{ pid: 3, name: "Third Defender", x: 8, y: 28, role: "main", t: 1 },
				],
				t: 1,
			}),
			miss,
		);
		expect(sampleAdvancedPlan(rebound, 0).ball).toMatchObject({
			x: miss.ball.x,
			y: miss.ball.y,
			z: miss.ball.z,
		});
		expect(sampleAdvancedPlan(rebound, 1).ball.owner).toBe(3);
	});

	test("a block includes the named defender and never produces a made-basket reaction", () => {
		const input = scene({ kind: "block", rimX: 5.25 });
		input.actors[2]!.role = "defender";
		const frame = sampleAdvancedPlan(createAdvancedPlan(input), 0.8);
		expect(frame.players.find((p) => p.pid === 3)?.t).toBe(1);
		expect(frame.impact?.made).toBe(false);
	});

	test("a steal changes ball ownership from the victim to the credited stealer", () => {
		const plan = createAdvancedPlan(
			scene({
				kind: "stl",
				actors: [
					{ pid: 9, name: "Ball Stealer", x: 20, y: 25, role: "main" },
					{ pid: 8, name: "Ball Loser", x: 24, y: 25, role: "victim" },
				],
			}),
		);
		expect(sampleAdvancedPlan(plan, 0).ball.owner).toBe(8);
		expect(sampleAdvancedPlan(plan, 1).ball.owner).toBe(9);
		expect(
			sampleAdvancedPlan(plan, 1).players.find((p) => p.pid === 8)?.t,
		).toBe(1);
	});

	test("free throws keep defenders grounded", () => {
		const plan = createAdvancedPlan(scene({ kind: "make", zone: "ft" }));
		expect(
			sampleAdvancedPlan(plan, 0.4).players.find((p) => p.pid === 3)?.jump,
		).toBe(0);
	});

	test("stoppages stop the ball and completed beats stop all running", () => {
		const frame = sampleAdvancedPlan(
			createAdvancedPlan(scene({ kind: "dead" })),
			1,
		);
		expect(frame.ball.visible).toBe(false);
		expect(
			frame.players.every(
				(p) => p.moving === 0 && p.stride === 0 && p.jump === 0,
			),
		).toBe(true);
	});

	test("a new lineup removes departed players and deduplicates event participants", () => {
		const previous = sampleAdvancedPlan(createAdvancedPlan(scene()), 1);
		const input = scene({
			actors: [
				scene().actors[0]!,
				scene().actors[0]!,
				{ pid: 4, name: "New Player", x: 47, y: 44, role: "in" },
			],
		});
		const frame = sampleAdvancedPlan(createAdvancedPlan(input, previous), 1);
		expect(frame.players.map((p) => p.pid)).toEqual([1, 4]);
	});

	test.each([
		"attempt",
		"make",
		"miss",
		"block",
		"advance",
		"swing",
		"reb",
		"stl",
		"tov",
		"oob",
		"foul",
		"jump",
		"dead",
		"sub",
		"other",
	] as const)("%s supports empty or older replay data", (kind) => {
		const plan = createAdvancedPlan(scene({ kind, actors: [] }));
		for (const p of [0, 0.25, 0.8, 1, Number.NaN]) {
			const frame = sampleAdvancedPlan(plan, p);
			expect(
				[frame.ball.x, frame.ball.y, frame.ball.z].every(Number.isFinite),
			).toBe(true);
		}
	});
});
