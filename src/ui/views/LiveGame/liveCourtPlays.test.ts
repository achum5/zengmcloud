import { assert, describe, test } from "vitest";
import {
	buildNonShotPlay,
	buildShotPlay,
	playRoles,
	placePlay,
	samplePath,
	sideOf,
	synthCanonicalSpot,
	type Binding,
	type BuiltPlay,
	type NonShotCat,
	type ShotCat,
} from "./liveCourtPlays.ts";

const SHOT_CATS: ShotCat[] = ["rim", "post", "mid", "three", "ft"];
const NON_SHOT_CATS: NonShotCat[] = [
	"steal",
	"tov",
	"orb",
	"drb",
	"foul",
	"jump",
];

const finite = (n: number) => Number.isFinite(n);
// Court is 94 x 50; plays (with jitter) should stay comfortably on/near it.
const inBounds = (x: number, y: number) =>
	x >= -8 && x <= 102 && y >= -6 && y <= 56;

const assertWellFormed = (built: BuiltPlay, label: string) => {
	assert.ok(built.tracks.length > 0, `${label} has no tracks`);
	const scorers = built.tracks.filter((t) => t.role === "scorer");
	for (const tr of built.tracks) {
		assert.ok(tr.nodes.length >= 1, `${label}/${tr.role} has no nodes`);
		for (let i = 1; i < tr.nodes.length; i++) {
			assert.ok(
				tr.nodes[i]!.t >= tr.nodes[i - 1]!.t,
				`${label}/${tr.role} node times not monotonic`,
			);
			assert.ok(
				finite(tr.nodes[i]!.x) && finite(tr.nodes[i]!.y),
				`${label}/${tr.role} non-finite node`,
			);
		}
	}
	return scorers;
};

describe("shot play generation", () => {
	test("every shot category builds a well-formed play, with and without a passer", () => {
		for (const cat of SHOT_CATS) {
			for (const passer of [false, true]) {
				// Builders are randomized; exercise many draws.
				for (let i = 0; i < 40; i++) {
					const { built, spot } = buildShotPlay(cat, passer);
					const label = `${cat}/${passer}`;
					const scorers = assertWellFormed(built, label);
					assert.strictEqual(scorers.length, 1, `${label} not exactly one scorer`);
					assert.ok(
						built.ball.some((b) => b.kind === "shot"),
						`${label} has no shot segment`,
					);
					assert.ok(
						finite(spot.x) && finite(spot.y) && inBounds(spot.x, spot.y),
						`${label} spot off court: ${spot.x},${spot.y}`,
					);
					// When a passer is requested, the play should actually show a pass
					// (so an assist is depicted) - except free throws, which never pass.
					if (passer && cat !== "ft") {
						assert.ok(
							built.tracks.some((t) => t.role === "passer") &&
								built.ball.some((b) => b.kind === "pass"),
							`${label} requested a passer but shows no pass`,
						);
					}
					// A self-created shot must not fabricate a pass.
					if (!passer) {
						assert.ok(
							!built.ball.some((b) => b.kind === "pass"),
							`${label} is self-created but shows a pass`,
						);
					}
				}
			}
		}
	});
});

describe("non-shot play generation", () => {
	test("every non-shot category builds a well-formed play", () => {
		for (const cat of NON_SHOT_CATS) {
			for (let i = 0; i < 10; i++) {
				const built = buildNonShotPlay(cat);
				assertWellFormed(built, cat);
			}
		}
	});
});

describe("synthCanonicalSpot", () => {
	test("spots are finite and on-court in the right general area", () => {
		for (const cat of SHOT_CATS) {
			for (let i = 0; i < 200; i++) {
				const s = synthCanonicalSpot(cat);
				assert.ok(
					finite(s.x) && finite(s.y) && inBounds(s.x, s.y),
					`${cat} spot off court: ${s.x},${s.y}`,
				);
			}
		}
	});

	test("three-point spots are spread across the whole arc and both corners", () => {
		const ys: number[] = [];
		const xs: number[] = [];
		for (let i = 0; i < 800; i++) {
			const s = synthCanonicalSpot("three");
			ys.push(s.y);
			xs.push(s.x);
		}
		// Both corners appear (near the sidelines).
		assert.ok(
			ys.some((y) => y < 4),
			"no top-corner threes",
		);
		assert.ok(
			ys.some((y) => y > 46),
			"no bottom-corner threes",
		);
		// The top of the key / straightaway appears (small x, near mid y).
		assert.ok(
			xs.some((x, i) => x < 68 && Math.abs(ys[i]! - 25) < 8),
			"no top-of-key threes",
		);
		// Wings on both sides appear.
		assert.ok(
			ys.some((y) => y > 12 && y < 22),
			"no top-wing threes",
		);
		assert.ok(
			ys.some((y) => y > 28 && y < 38),
			"no bottom-wing threes",
		);
	});

	test("rim spots cluster near the rim; three spots sit beyond the arc", () => {
		for (let i = 0; i < 100; i++) {
			const rim = synthCanonicalSpot("rim");
			const dRim = Math.hypot(rim.x - 88.75, rim.y - 25);
			assert.ok(dRim <= 6, `rim spot too far: ${dRim}`);
			const three = synthCanonicalSpot("three");
			const dThree = Math.hypot(three.x - 88.75, three.y - 25);
			assert.ok(dThree >= 20, `three spot too close: ${dThree}`);
		}
	});
});

describe("placePlay + samplePath", () => {
	const binding = (): Binding => {
		const b: Binding = new Map();
		for (const role of [
			"scorer",
			"passer",
			"screen",
			"def1",
			"def2",
			"victim",
			"stealer",
			"fouler",
			"jumper2",
		] as const) {
			b.set(role, { pid: role.length + 100, name: role });
		}
		return b;
	};

	test("places every generated play on the court with finite, on-court paths", () => {
		const plays: { built: BuiltPlay; spot?: { x: number; y: number } }[] = [];
		for (const cat of SHOT_CATS) {
			for (const passer of [false, true]) {
				plays.push(buildShotPlay(cat, passer));
			}
		}
		for (const cat of NON_SHOT_CATS) {
			plays.push({ built: buildNonShotPlay(cat) });
		}

		for (const { built, spot } of plays) {
			for (const attackT of [0, 1] as const) {
				for (const flipY of [false, true]) {
					const inst = placePlay(built, binding(), {
						key: 1,
						attackT,
						flipY,
						jitterX: 1,
						jitterY: -1,
						made: true,
						blocked: false,
						shotSpot: spot,
					});
					assert.ok(inst.players.length >= 1, "placed with no players");
					for (const pl of inst.players) {
						for (let g = 0; g <= 1; g += 0.1) {
							const p = samplePath(pl.nodes, g);
							assert.ok(
								finite(p.x) && finite(p.y) && inBounds(p.x, p.y),
								`sample off court at ${g}: ${p.x},${p.y}`,
							);
						}
					}
				}
			}
		}
	});

	test("the shot releases from the provided spot, aimed at the attacked rim", () => {
		const { built, spot } = buildShotPlay("three", false);
		const inst = placePlay(built, binding(), {
			key: 1,
			attackT: 1,
			flipY: false,
			jitterX: 0,
			jitterY: 0,
			made: false,
			blocked: false,
			shotSpot: spot,
		});
		const shot = inst.ball.find((b) => b.kind === "shot");
		assert.ok(shot && shot.kind === "shot");
		// Home attacks the right rim (~88.75).
		assert.ok(shot.rimX > 80, `rimX ${shot.rimX}`);
		// The release spot is exactly the (placed) synthesized spot: home attacking
		// right isn't mirrored, so x/y match with no jitter.
		assert.ok(
			Math.abs(shot.spot.x - spot.x) < 0.001 &&
				Math.abs(shot.spot.y - spot.y) < 0.001,
			`shot spot ${shot.spot.x},${shot.spot.y} != ${spot.x},${spot.y}`,
		);
	});

	test("only bound roles survive; a pass to an unbound passer is dropped", () => {
		const { built } = buildShotPlay("three", true);
		const partial: Binding = new Map();
		partial.set("scorer", { pid: 1, name: "s" });
		const inst = placePlay(built, partial, {
			key: 1,
			attackT: 1,
			flipY: false,
			jitterX: 0,
			jitterY: 0,
			made: true,
			blocked: false,
		});
		assert.deepStrictEqual(
			inst.players.map((p) => p.pid),
			[1],
		);
		assert.ok(!inst.ball.some((b) => b.kind === "pass"));
	});
});

describe("helpers", () => {
	test("playRoles lists each track's role", () => {
		const { built } = buildShotPlay("ft", false);
		assert.ok(playRoles(built).includes("scorer"));
	});

	test("sideOf reflects the half of the court a spot is in", () => {
		assert.strictEqual(sideOf({ x: 80, y: 40 }), 1);
		assert.strictEqual(sideOf({ x: 80, y: 10 }), -1);
	});
});
