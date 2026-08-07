import { assert, describe, test } from "vitest";
import {
	buildCardBackPrompt,
	buildCardFrontPrompt,
	type CardSubject,
} from "./tradingCardPrompt.ts";
import {
	CARD_ERAS,
	CARD_SETS,
	cardErasById,
	cardSetsById,
	cardTitle,
} from "./tradingCards.ts";

const subject = (overrides: Partial<CardSubject> = {}): CardSubject => ({
	name: "Ray Harris",
	pos: "SG",
	jerseyNumber: "8",
	heightIn: 78,
	weightLbs: 205,
	age: 24,
	bornYear: 2002,
	bornLoc: "Akron, OH",
	college: "Ohio State",
	draft: { year: 2024, round: 1, pick: 5, teamName: "Boston Celtics" },
	teamName: "Boston Celtics",
	season: 2026,
	awards: ["Rookie of the Year (2025)"],
	stats: [
		{
			season: 2025,
			abbrev: "BOS",
			gp: 80,
			gs: 60,
			min: 30.2,
			pts: 18.4,
			trb: 4.1,
			ast: 3.2,
			stl: 1.1,
			blk: 0.3,
			fgp: 46.2,
			tpp: 37.5,
			ftp: 84.1,
		},
		{
			season: 2026,
			abbrev: "BOS",
			gp: 82,
			gs: 82,
			min: 34.8,
			pts: 24.1,
			trb: 4.8,
			ast: 4.0,
			stl: 1.3,
			blk: 0.4,
			fgp: 48.0,
			tpp: 39.1,
			ftp: 86.6,
		},
	],
	career: {
		gp: 162,
		gs: 142,
		min: 32.5,
		pts: 21.3,
		trb: 4.5,
		ast: 3.6,
		stl: 1.2,
		blk: 0.4,
		fgp: 47.1,
		tpp: 38.3,
		ftp: 85.4,
	},
	...overrides,
});

// The whole feature turns on this: the set is a LOOK and the season is what the
// card depicts. A 1985-86 Star design showing a 2026 season is the point, not a
// bug, and the prompt has to say so or the model quietly "corrects" the year.
describe("the set is a look, the season is the subject", () => {
	test("the front prints the depicted season, not the set's year", () => {
		const prompt = buildCardFrontPrompt("1985-86-star", "base", subject());
		assert.ok(prompt.includes("1985-86 Star Company"));
		assert.ok(prompt.includes("**2026**"));
		assert.ok(
			prompt.includes(
				"A 1985-86 Star Company design showing 2026 is exactly what is wanted",
			),
		);
	});

	test("the back says the same thing", () => {
		const prompt = buildCardBackPrompt("1985-86-star", "base", subject());
		assert.ok(prompt.includes("**2026** is the season this card depicts"));
	});

	test("the title carries the depicted season, not the set year", () => {
		assert.strictEqual(
			cardTitle("1996-97-topps-chrome", "refractor", 2026),
			"1996-97 Topps Chrome · Refractor · 2026",
		);
	});

	test("a base card does not repeat itself in the title", () => {
		assert.strictEqual(
			cardTitle("1996-97-topps-chrome", "base", 2026),
			"1996-97 Topps Chrome · 2026",
		);
	});
});

describe("the front prompt", () => {
	test("carries the set's own design fields", () => {
		const prompt = buildCardFrontPrompt("1990-91-skybox", "base", subject());
		// The SkyBox computer-generated background is the set's whole identity.
		assert.ok(prompt.includes("computer-generated abstract background"));
		assert.ok(prompt.includes("gold border"));
	});

	test("carries the era's design language above the set", () => {
		const prompt = buildCardFrontPrompt("1990-91-skybox", "base", subject());
		assert.ok(prompt.includes("The Junk-Wax Boom"));
		assert.ok(prompt.includes("digital gradients, neon geometrics"));
	});

	test("describes the specific variant, not just the base card", () => {
		const prompt = buildCardFrontPrompt(
			"1997-98-metal-universe",
			"pmg-green",
			subject(),
		);
		assert.ok(prompt.includes("Precious Metal Gems (Green)"));
		assert.ok(prompt.includes("green gemstone-like faceted foil"));
	});

	test("says nothing extra for a plain base card", () => {
		const prompt = buildCardFrontPrompt("1990-91-skybox", "base", subject());
		assert.ok(!prompt.includes("## This particular card"));
	});

	test("asks for a courtside photo rendered as a faces.js cartoon", () => {
		const prompt = buildCardFrontPrompt("prizm", "silver", subject());
		assert.ok(prompt.includes("sideline or the baseline"));
		assert.ok(prompt.includes("flat faces.js cartoon style"));
	});

	// A posed, camera-aware player is the default an image model drifts to, and
	// it is the one thing that stops a card reading like a real card.
	test("demands a candid in-game action shot, not a portrait", () => {
		const prompt = buildCardFrontPrompt("prizm", "silver", subject());
		assert.ok(prompt.includes("A CANDID shot, not a portrait"));
		assert.ok(prompt.includes("PLAYING BASKETBALL"));
		assert.ok(prompt.includes("does not know the camera is there"));
		assert.ok(
			!prompt.includes("EXCEPTION for this particular set"),
			"an ordinary set gets no posed carve-out",
		);
	});

	// The handful of vintage sets whose whole look is a staged gym portrait
	// would otherwise get two contradictory instructions.
	test("a set that is posed on purpose keeps its posed shot", () => {
		const prompt = buildCardFrontPrompt("1961-62-fleer", "base", subject());
		assert.ok(prompt.includes("A CANDID shot, not a portrait"));
		assert.ok(prompt.includes("EXCEPTION for this particular set"));
	});

	test("pins the uniform to the depicted season", () => {
		const prompt = buildCardFrontPrompt("1985-86-star", "base", subject());
		assert.ok(
			prompt.includes(
				"Boston Celtics uniform as that franchise actually wore it in 2026",
			),
		);
	});

	// The league is fictional everywhere else in the app and has to stay that
	// way here, with the uniform carved out as the single exception.
	test("blocks real-world knowledge except the uniform", () => {
		const prompt = buildCardFrontPrompt("prizm", "silver", subject());
		assert.ok(prompt.includes("THIS IS A FICTIONAL LEAGUE"));
		assert.ok(prompt.includes("THE ONE EXCEPTION is the uniform"));
	});

	test("points at the attached screenshot when there is a face", () => {
		const withFace = subject({ face: { fatness: 0.4 } as any });
		const prompt = buildCardFrontPrompt("prizm", "silver", withFace);
		assert.ok(prompt.includes("A screenshot of this player is attached"));
		assert.ok(prompt.includes("flat vector cartoon style"));
	});

	// The reference is a roster headshot. Told to "match it exactly", the model
	// copied its blank stare and slack open mouth onto a player mid-drive - the
	// likeness was right and the card was still wrong. Identity is fixed;
	// expression and head angle belong to the action.
	test("the headshot fixes the likeness, not the expression", () => {
		const withFace = subject({ face: { fatness: 0.4 } as any });
		const prompt = buildCardFrontPrompt("prizm", "silver", withFace);
		assert.ok(prompt.includes("HEADSHOT"), "says what the reference is");
		assert.ok(
			prompt.includes(
				"Do NOT copy the expression, the mouth, or the head angle",
			),
			"frees the expression",
		);
		assert.ok(
			prompt.includes("skin tone, face shape, hair"),
			"still pins the likeness",
		);
		assert.ok(
			!prompt.includes("match it exactly"),
			"the instruction that caused it is gone",
		);
	});

	// The FaceConfig used to be dumped into the prompt as JSON to disambiguate
	// the screenshot. An image model cannot run the faces.js renderer, so those
	// numbers describe nothing to it - they only push the instructions that do
	// work further down a long prompt.
	test("never dumps the raw face config", () => {
		const withFace = subject({
			face: { fatness: 0.4, eyeLine: 0.12, hairBg: "none" } as any,
		});
		const prompt = buildCardFrontPrompt("prizm", "silver", withFace);
		assert.ok(!prompt.includes("FaceConfig"), "no mention of the config");
		assert.ok(!prompt.includes('"fatness"'), "no serialized fields");
		assert.ok(!prompt.includes("eyeLine"), "no serialized fields");
		assert.ok(!prompt.includes("```json"), "no json block at all");
	});

	test("omits the face section entirely when there is no face", () => {
		const prompt = buildCardFrontPrompt("prizm", "silver", subject());
		assert.ok(!prompt.includes("## The player's face"));
	});
});

describe("the back prompt", () => {
	test("reproduces every season row and the career line", () => {
		const prompt = buildCardBackPrompt("panini-hoops", "base", subject());
		assert.ok(prompt.includes("2025"));
		assert.ok(prompt.includes("2026"));
		assert.ok(prompt.includes("24.1"));
		assert.ok(prompt.includes("18.4"));
		assert.ok(prompt.includes("CAREER"));
	});

	test("a single-season player gets no career line", () => {
		const one = subject({ stats: [subject().stats[0]!], career: undefined });
		const prompt = buildCardBackPrompt("panini-hoops", "base", one);
		assert.ok(!prompt.includes("CAREER"));
	});

	test("a player with no games gets a bio-only back, not an empty grid", () => {
		const prompt = buildCardBackPrompt(
			"panini-hoops",
			"base",
			subject({ stats: [] }),
		);
		assert.ok(prompt.includes("pre-debut card"));
		assert.ok(!prompt.includes("SEASON  TEAM"));
	});

	test("forbids inventing numbers past the depicted season", () => {
		const prompt = buildCardBackPrompt("panini-hoops", "base", subject());
		assert.ok(prompt.includes("no extrapolation past 2026"));
	});

	test("uses the set's own card-back description", () => {
		const prompt = buildCardBackPrompt("1991-92-fleer", "base", subject());
		assert.ok(prompt.includes("hardwood-floor background"));
	});
});

describe("the catalogue", () => {
	test("every set id is unique", () => {
		const ids = CARD_SETS.map((set) => set.id);
		assert.strictEqual(new Set(ids).size, ids.length);
	});

	test("every variant id is unique within its set", () => {
		for (const set of CARD_SETS) {
			const ids = set.variants.map((v) => v.id);
			assert.strictEqual(new Set(ids).size, ids.length, set.id);
		}
	});

	test("every set has at least one card to pick", () => {
		for (const set of CARD_SETS) {
			assert.ok(set.variants.length > 0, set.id);
		}
	});

	// Both prompts are built by looking the set up by id, so a set that isn't in
	// the map is a card that silently generates an empty prompt.
	test("every set is reachable by id", () => {
		for (const set of CARD_SETS) {
			assert.strictEqual(cardSetsById.get(set.id), set);
		}
	});

	test("every set produces a non-empty prompt for every one of its cards", () => {
		for (const set of CARD_SETS) {
			for (const variant of set.variants) {
				assert.ok(
					buildCardFrontPrompt(set.id, variant.id, subject()).length > 500,
					`${set.id}/${variant.id} front`,
				);
				assert.ok(
					buildCardBackPrompt(set.id, variant.id, subject()).length > 500,
					`${set.id}/${variant.id} back`,
				);
			}
		}
	});

	// A 1969 card rendered factory-fresh looks like a modern reprint of itself,
	// so the era's aging is carried into the prompt alongside the design.
	test("every era describes how its cards age, and the front prompt says so", () => {
		for (const era of CARD_ERAS) {
			assert.ok(era.wear.length > 0, era.id);
		}
		const front = buildCardFrontPrompt("1969-70-topps", "base", subject());
		assert.ok(
			front.includes(cardErasById.get("vintage")!.wear),
			"the vintage wear profile reached the prompt",
		);
	});

	// The tall boys are DEFINED by not being 2.5 x 3.5. If the shape doesn't
	// survive into the prompt, the set is just a plain white-bordered card.
	test("a non-standard card shape overrides the default proportions", () => {
		for (const setId of ["1969-70-topps", "1970-71-topps"]) {
			const set = cardSetsById.get(setId)!;
			for (const build of [buildCardFrontPrompt, buildCardBackPrompt]) {
				const prompt = build(setId, "base", subject());
				assert.ok(
					prompt.includes(set.proportions!),
					`${setId} keeps its shape`,
				);
				assert.ok(
					!prompt.includes("2.5 x 3.5 inches"),
					`${setId} does not also claim to be standard-sized`,
				);
			}
		}
		assert.ok(
			buildCardFrontPrompt("2012-13-prizm", "base", subject()).includes(
				"2.5 x 3.5 inches, PORTRAIT",
			),
			"an ordinary set still gets the standard size",
		);
	});

	// THE BUG: a front came back portrait and the back came back landscape, so
	// the two halves of one card were different shapes. The prompts are pasted
	// into the model separately, so the only thing that can hold them together
	// is both of them saying the same size.
	test("the front and the back state the same size, every set", () => {
		for (const set of CARD_SETS) {
			const front = buildCardFrontPrompt(set.id, "base", subject());
			const back = buildCardBackPrompt(set.id, "base", subject());
			const size =
				set.proportions ??
				"2.5 x 3.5 inches, PORTRAIT - taller than it is wide, a 5:7 ratio. Render the image at 1024 x 1434 pixels.";
			assert.ok(front.includes(size), `${set.id} front`);
			assert.ok(back.includes(size), `${set.id} back`);
			for (const [side, prompt] of [
				["front", front],
				["back", back],
			] as const) {
				assert.ok(
					prompt.includes("MUST come out at identical dimensions"),
					`${set.id} ${side} says the two halves have to match`,
				);
			}
		}
	});

	// Eight sets in the catalogue have a back that reads horizontally, which is
	// true of the real cards - but it describes the LAYOUT, not the card, and
	// the model took it as the card. That is what produced the landscape back.
	test("a horizontally-laid-out back is still a portrait card", () => {
		const horizontal = CARD_SETS.filter((set) => /horizontal/i.test(set.back));
		assert.ok(
			horizontal.length >= 5,
			"the horizontal-back sets are still catalogued",
		);
		for (const set of horizontal) {
			const back = buildCardBackPrompt(set.id, "base", subject());
			assert.ok(
				back.includes("Do not output a landscape image"),
				`${set.id} is told not to turn the card`,
			);
			assert.ok(
				back.includes("the layout, not the card"),
				`${set.id} is told what horizontal actually refers to`,
			);
		}
	});

	// An unlicensed set that renders team logos is the one way these can be
	// factually wrong on their face, so the instruction has to be in the prompt.
	test("unlicensed sets carry their no-team-marks rule into every card", () => {
		const unlicensed = CARD_SETS.filter((set) =>
			set.markers?.includes("UNLICENSED"),
		);
		assert.ok(
			unlicensed.length >= 4,
			"the unlicensed sets are still catalogued",
		);
		for (const set of unlicensed) {
			for (const variant of set.variants) {
				assert.ok(
					buildCardFrontPrompt(set.id, variant.id, subject()).includes(
						"UNLICENSED",
					),
					`${set.id}/${variant.id}`,
				);
			}
		}
	});

	test("an unknown set produces nothing rather than a half-built prompt", () => {
		assert.strictEqual(buildCardFrontPrompt("nope", "base", subject()), "");
		assert.strictEqual(buildCardBackPrompt("nope", "base", subject()), "");
		assert.strictEqual(cardTitle("nope", "base", 2026), "2026");
	});
});
