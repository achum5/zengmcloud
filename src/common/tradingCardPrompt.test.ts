import { assert, describe, test } from "vitest";
import {
	buildCardBackPrompt,
	buildCardFrontPrompt,
	type CardSubject,
} from "./tradingCardPrompt.ts";
import { CARD_SETS, cardSetsById, cardTitle } from "./tradingCards.ts";

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

	test("embeds the face config when there is one", () => {
		const withFace = subject({ face: { fatness: 0.4 } as any });
		const prompt = buildCardFrontPrompt("prizm", "silver", withFace);
		assert.ok(prompt.includes("faces.js FaceConfig"));
		assert.ok(prompt.includes('"fatness"'));
	});

	test("omits the face section entirely when there is no face", () => {
		const prompt = buildCardFrontPrompt("prizm", "silver", subject());
		assert.ok(!prompt.includes("faces.js FaceConfig"));
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

	test("an unknown set produces nothing rather than a half-built prompt", () => {
		assert.strictEqual(buildCardFrontPrompt("nope", "base", subject()), "");
		assert.strictEqual(buildCardBackPrompt("nope", "base", subject()), "");
		assert.strictEqual(cardTitle("nope", "base", 2026), "2026");
	});
});
