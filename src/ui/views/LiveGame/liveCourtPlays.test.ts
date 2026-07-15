import { assert, describe, test } from "vitest";
import {
	PLAY_TEMPLATES,
	type Binding,
	type PlayCat,
	pickTemplate,
	placePlay,
	samplePath,
	templateRoles,
} from "./liveCourtPlays.ts";

const CATS: PlayCat[] = [
	"rim",
	"post",
	"mid",
	"three",
	"ft",
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

describe("play template library", () => {
	test("every category has at least one template", () => {
		for (const cat of CATS) {
			const matches = PLAY_TEMPLATES.filter((t) => t.cat === cat);
			assert.ok(matches.length > 0, `no template for category ${cat}`);
		}
	});

	test("every template's paths and ball script are well-formed", () => {
		for (const tpl of PLAY_TEMPLATES) {
			assert.ok(tpl.tracks.length > 0, `${tpl.id} has no tracks`);
			// Exactly one scorer-ish primary role we can bind.
			for (const tr of tpl.tracks) {
				assert.ok(tr.nodes.length >= 1, `${tpl.id}/${tr.role} has no nodes`);
				// Times run monotonically from ~0 to ~1.
				for (let i = 1; i < tr.nodes.length; i++) {
					assert.ok(
						tr.nodes[i]!.t >= tr.nodes[i - 1]!.t,
						`${tpl.id}/${tr.role} node times not monotonic`,
					);
				}
			}
			if (tpl.hasShot) {
				assert.ok(
					tpl.ball.some((b) => b.kind === "shot"),
					`${tpl.id} hasShot but no shot segment`,
				);
			}
		}
	});
});

describe("pickTemplate", () => {
	test("returns a matching template, honoring the passer requirement", () => {
		for (const cat of CATS) {
			for (const hasPasser of [false, true]) {
				for (let r = 0; r < 1; r += 0.13) {
					const tpl = pickTemplate(cat, hasPasser, r);
					assert.ok(tpl, `no template picked for ${cat}`);
					assert.strictEqual(tpl!.cat, cat);
					if (!hasPasser) {
						assert.ok(
							!tpl!.needsPasser,
							`${tpl!.id} needs a passer but none available`,
						);
					}
				}
			}
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

	test("places every template on the court with finite, on-court paths", () => {
		for (const tpl of PLAY_TEMPLATES) {
			for (const attackT of [0, 1] as const) {
				for (const flipY of [false, true]) {
					const inst = placePlay(tpl, binding(), {
						key: 1,
						attackT,
						flipY,
						jitterX: 1.2,
						jitterY: -1.2,
						made: true,
						blocked: false,
					});
					assert.ok(
						inst.players.length >= 1,
						`${tpl.id} placed with no players`,
					);
					for (const pl of inst.players) {
						for (const nd of pl.nodes) {
							assert.ok(
								finite(nd.x) && finite(nd.y),
								`${tpl.id}/${pl.pid} non-finite node`,
							);
							assert.ok(
								inBounds(nd.x, nd.y),
								`${tpl.id}/${pl.pid} node off court: ${nd.x},${nd.y}`,
							);
						}
						// Sample the path densely; every sample must be finite/on-court.
						for (let g = 0; g <= 1; g += 0.1) {
							const p = samplePath(pl.nodes, g);
							assert.ok(
								finite(p.x) && finite(p.y) && inBounds(p.x, p.y),
								`${tpl.id}/${pl.pid} sample off court at ${g}: ${p.x},${p.y}`,
							);
						}
					}
				}
			}
		}
	});

	test("a shot template yields a shot segment aimed at the attacked rim", () => {
		const tpl = PLAY_TEMPLATES.find((t) => t.id === "rim-iso-wing")!;
		const inst = placePlay(tpl, binding(), {
			key: 1,
			attackT: 1,
			flipY: false,
			jitterX: 0,
			jitterY: 0,
			made: false,
			blocked: false,
		});
		const shot = inst.ball.find((b) => b.kind === "shot");
		assert.ok(shot && shot.kind === "shot");
		// Home attacks the right rim (~88.75).
		assert.ok(shot.rimX > 80, `rimX ${shot.rimX}`);
	});

	test("only bound roles survive; unbound optional roles are dropped", () => {
		const tpl = PLAY_TEMPLATES.find((t) => t.id === "rim-cut-backdoor")!;
		const partial: Binding = new Map();
		partial.set("scorer", { pid: 1, name: "s" });
		// no passer / defenders bound
		const inst = placePlay(tpl, partial, {
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
		// The pass segment referencing the missing passer is dropped.
		assert.ok(!inst.ball.some((b) => b.kind === "pass"));
	});
});

describe("templateRoles", () => {
	test("lists each track's role", () => {
		const tpl = PLAY_TEMPLATES.find((t) => t.id === "ft")!;
		const roles = templateRoles(tpl);
		assert.ok(roles.includes("scorer"));
	});
});
