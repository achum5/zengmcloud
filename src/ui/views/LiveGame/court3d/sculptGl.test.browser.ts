import { beforeAll, describe, expect, test } from "vitest";
import { makeCamera, MAIN_RIG } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import type { Look } from "./figure.ts";
import { kitArtOf } from "./kitArt.ts";
import { bodyOf } from "./poses.ts";
import { glCard } from "./sculptGl.ts";
import { glAgainstPage, glAgrees } from "./sprite.ts";

// THE GRAPHICS CARD'S MAN IS THE PAGE'S (see sculptGl.ts): whatever he
// wears and whatever he is doing, sculpted on the card he is the man
// sculpt.ts makes a pixel at a time - the same shape, the same colors.

const body = bodyOf();
const cam = (() => {
	const cw = 300;
	const ch = 480;
	const k = ch / (body.H * 1.3);
	return makeCamera(
		{ x: 47, width: cw / k, y: 25, z: body.H * 0.5 },
		cw,
		ch,
		MAIN_RIG,
	);
})();
const kit = {
	jersey: "#1d428a",
	trim: "#ffc72c",
	number: "#ffffff",
	numberEdge: "#000000",
	shorts: "#1d428a",
	stripe: "#ffc72c",
	sock: "#ffffff",
	shoe: "#f4f4f4",
	sole: "#22252c",
};
const base: Look = {
	kit,
	skin: "#8d5524",
	hair: "#1f1612",
	jerseyNumber: "",
	name: "",
	lastName: "",
	wordmark: "",
};
const man = (over: Partial<PlayerState>): PlayerState =>
	({
		pid: 0,
		team: 0,
		shown: true,
		x: 47,
		y: 25,
		z: 0,
		yaw: Math.PI / 2,
		anim: "ready",
		phase: 0.3,
		moving: false,
		holding: false,
		...over,
	}) as PlayerState;

// How far apart the two are over these men: the share of his pixels there
// one way and not the other, and the colors of the rest (a channel, 0-255).
const apart = (look: Look, men: Partial<PlayerState>[]) => {
	const all = { drawn: 0, off: 0, tone: 0, toned: 0 };
	for (const over of men) {
		const one = glAgainstPage(cam, man(over), body, look);
		expect(one).toBeDefined();
		all.drawn += one!.drawn;
		all.off += one!.off;
		all.tone += one!.tone;
		all.toned += one!.toned;
		expect(one!.lost).toBe(0);
	}
	expect(all.drawn).toBeGreaterThan(2000);
	return { shape: all.off / all.drawn, tone: all.tone / all.toned };
};

const TURNS = [Math.PI / 2, 0, -2.2, 2.6];
const POSES: Partial<PlayerState>[] = [
	...TURNS.map((yaw) => ({ yaw })),
	...TURNS.map((yaw) => ({ yaw, anim: "run", phase: 0.2 }) as const),
	...TURNS.map((yaw) => ({ yaw, anim: "contest", phase: 0.6 }) as const),
	...TURNS.map((yaw) => ({ yaw, anim: "sit", phase: 0.4 }) as const),
];

// (Software drawing, where the tests run: slow.)
describe("a man sculpted on the graphics card", { timeout: 60_000 }, () => {
	let card = false;
	beforeAll(() => {
		// (Whatever draws for the browser the tests run in.)
		localStorage.setItem("court3dGl", "1");
		card = glCard() !== undefined;
	});

	test("is the page's, checked as a visit checks it", ({ skip }) => {
		skip(!card, "no WebGL2 here");
		const said = glAgrees();
		expect(said.shape).toBeLessThan(0.005);
		expect(said.tone).toBeLessThan(1);
		expect(said.ok).toBe(true);
	});

	test("is the page's in his uniform, whatever he is doing", ({ skip }) => {
		skip(!card, "no WebGL2 here");
		const d = apart(base, POSES);
		expect(d.shape).toBeLessThan(0.005);
		expect(d.tone).toBeLessThan(1);
	});

	test("is the page's with the ball in his hands", ({ skip }) => {
		skip(!card, "no WebGL2 here");
		const d = apart(
			base,
			TURNS.map((yaw) => ({ yaw, holding: true, anim: "triple" }) as const),
		);
		expect(d.shape).toBeLessThan(0.005);
		// (The ball's grain is a speck a cell: a cell's edge falls a hair
		// differently in the card's sums.)
		expect(d.tone).toBeLessThan(2.5);
	});

	test("is the page's in his gear, numbered and named", ({ skip }) => {
		skip(!card, "no WebGL2 here");
		const d = apart(
			{
				...base,
				jerseyNumber: "23",
				lastName: "Player",
				wordmark: "City",
				gear: {
					sleeve: { arms: "R", color: "#111111", elbow: false },
					tights: { legs: "L", color: "#eeeeee" },
					wrist: { arms: "RL", color: "#cc2222" },
					knee: { legs: "R", color: "#222222" },
					shoe: "#cc2222",
					sole: "#ffffff",
				} as Look["gear"],
			},
			POSES,
		);
		expect(d.shape).toBeLessThan(0.005);
		// (His lettering is shrunk to his size by the card, not the page: the
		// edges of it a shade apart.)
		expect(d.tone).toBeLessThan(2.5);
	});

	test("is the page's in a uniform drawn from a picture", ({ skip }) => {
		skip(!card, "no WebGL2 here");
		const cv = document.createElement("canvas");
		cv.width = 512;
		cv.height = 608;
		const g = cv.getContext("2d")!;
		for (let y = 0; y < 608; y += 16) {
			for (let x = 0; x < 512; x += 16) {
				g.fillStyle = `hsl(${(x * 0.7 + y * 0.4) % 360} 70% ${40 + ((x / 16 + y / 16) % 2) * 20}%)`;
				g.fillRect(x, y, 16, 16);
			}
		}
		g.clearRect(120, 60, 60, 60);
		g.clearRect(300, 330, 80, 50);
		const art = kitArtOf(cv, "test");
		expect(art).toBeDefined();
		const d = apart({ ...base, kitArt: art!, jerseyNumber: "7" }, POSES);
		expect(d.shape).toBeLessThan(0.005);
		expect(d.tone).toBeLessThan(2.5);
	});

	test("is the page's in a referee's shirt and a coach's suit", ({ skip }) => {
		skip(!card, "no WebGL2 here");
		for (const outfit of [
			{ sleeves: "short", stripes: "#111111" },
			{ sleeves: "short", sleeveColor: "#111111", sides: "#111111" },
			{ sleeves: "long", shirt: "#ffffff", tie: "#aa2222" },
			{ sleeves: "long" },
			{ sleeves: "long", plain: true },
			{ sleeves: "long", camera: true },
			{ towel: true },
		] as const) {
			const d = apart({ ...base, outfit }, [POSES[0]!, POSES[5]!, POSES[10]!]);
			expect(d.shape).toBeLessThan(0.005);
			expect(d.tone).toBeLessThan(1.5);
		}
	});
});
