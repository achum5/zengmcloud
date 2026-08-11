import { assert, describe, test } from "vitest";
import {
	achievementCardId,
	achievementPromptOverride,
	CHAMPION_CARD_PLAYERS,
	deriveDraftAchievementCards,
	deriveSeasonAchievementCards,
} from "./achievementCards.ts";
import {
	buildCardBackPrompt,
	buildCardFrontPrompt,
	type CardSubject,
} from "./tradingCardPrompt.ts";
import { CARD_SETS } from "./tradingCards.ts";

const p = (pid: number, name = `Player ${pid}`) => ({ pid, name });

describe("deriveSeasonAchievementCards", () => {
	const input = {
		season: 2027,
		awards: {
			mvp: p(1, "Alpha One"),
			dpoy: p(2),
			smoy: p(3),
			mip: p(4),
			roy: p(5),
			finalsMvp: p(1, "Alpha One"),
			allLeague: [
				{ players: [p(1), p(6), p(7)] },
				{ players: [p(8)] },
				{ players: [p(9)] },
			],
			allDefensive: [{ players: [p(2)] }, { players: [p(10)] }],
			allRookie: [p(5), p(11)],
		},
		allStars: [p(1), p(6), p(12)],
		champions: [p(1), p(6), p(13)],
	};

	test("one card per achievement, so a stacked season yields several for one player", () => {
		const cards = deriveSeasonAchievementCards(input);
		const alpha = cards.filter((c) => c.pid === 1);
		assert.deepStrictEqual(
			alpha.map((c) => c.kind),
			["mvp", "finalsMvp", "allLeague1", "allStar", "champion"],
		);
	});

	test("ids are deterministic and unique", () => {
		const cards = deriveSeasonAchievementCards(input);
		assert.strictEqual(new Set(cards.map((c) => c.id)).size, cards.length);
		assert.strictEqual(
			cards[0]!.id,
			achievementCardId(2027, "mvp", 1),
			"the same season derives the same ids on every device",
		);
		assert.deepStrictEqual(
			deriveSeasonAchievementCards(input).map((c) => c.id),
			cards.map((c) => c.id),
		);
	});

	test("named teams map to their numbered kinds with readable labels", () => {
		const cards = deriveSeasonAchievementCards(input);
		const second = cards.find((c) => c.pid === 8);
		assert.strictEqual(second?.kind, "allLeague2");
		assert.strictEqual(second?.label, "Second Team All-League");
		const def2 = cards.find((c) => c.pid === 10);
		assert.strictEqual(def2?.kind, "allDefensive2");
	});

	test("champions are capped at the key-player count", () => {
		const cards = deriveSeasonAchievementCards({
			season: 2027,
			champions: Array.from({ length: 12 }, (_, i) => p(100 + i)),
		});
		assert.strictEqual(cards.length, CHAMPION_CARD_PLAYERS);
		assert.ok(cards.every((c) => c.kind === "champion"));
	});

	test("a missing awards row or a null winner derives nothing, not a crash", () => {
		assert.deepStrictEqual(deriveSeasonAchievementCards({ season: 2027 }), []);
		assert.strictEqual(
			deriveSeasonAchievementCards({
				season: 2027,
				awards: { mvp: null, allLeague: [] },
			}).length,
			0,
		);
	});
});

describe("deriveDraftAchievementCards", () => {
	const picks = [
		{ pid: 30, name: "Third Guy", pick: 3 },
		{ pid: 10, name: "First Guy", pick: 1 },
		{ pid: 20, name: "Second Guy", pick: 2 },
		{ pid: 40, name: "Fourth Guy", pick: 4 },
		{ pid: 99, name: "Undrafted Guy", pick: 0 },
	];

	test("takes the top N in pick order with ordinal labels", () => {
		const cards = deriveDraftAchievementCards({
			season: 2027,
			picks,
			numPicks: 3,
		});
		assert.deepStrictEqual(
			cards.map((c) => [c.pid, c.label]),
			[
				[10, "1st Overall Pick"],
				[20, "2nd Overall Pick"],
				[30, "3rd Overall Pick"],
			],
		);
	});

	test("pick 0 (not yet drafted) never qualifies, so pre-draft the list is empty", () => {
		const cards = deriveDraftAchievementCards({
			season: 2027,
			picks: [{ pid: 99, name: "X", pick: 0 }],
			numPicks: 3,
		});
		assert.strictEqual(cards.length, 0);
	});

	test("numPicks 0 disables draft cards entirely", () => {
		assert.strictEqual(
			deriveDraftAchievementCards({ season: 2027, picks, numPicks: 0 }).length,
			0,
		);
	});
});

describe("achievementPromptOverride", () => {
	const subject = { teamName: "Detroit Pistons", college: "Kansas" };

	test("award cards keep the normal photograph and just gain the flag", () => {
		const o = achievementPromptOverride(
			{ kind: "mvp", label: "Most Valuable Player", season: 2027, pid: 1 },
			subject,
		);
		assert.strictEqual(o.photograph, undefined);
		assert.strictEqual(o.uniform, undefined);
		assert.strictEqual(o.achievement, "Most Valuable Player, 2027");
	});

	test("champion cards swap the action for the celebration", () => {
		const o = achievementPromptOverride(
			{ kind: "champion", label: "League Champion", season: 2027, pid: 1 },
			subject,
		);
		assert.ok(o.photograph?.includes("confetti"));
		assert.strictEqual(o.uniform, undefined, "still in his real uniform");
	});

	test("draft night replaces both the scene and the uniform", () => {
		const o = achievementPromptOverride(
			{ kind: "draft", label: "1st Overall Pick", season: 2027, pid: 10 },
			subject,
			"draftNight",
		);
		assert.ok(o.photograph?.includes("draft stage"));
		assert.ok(o.uniform?.includes("suit"));
		assert.ok(o.uniform?.includes("Detroit Pistons"));
	});

	test("the college scene names the school but forbids real college uniforms", () => {
		const o = achievementPromptOverride(
			{ kind: "draft", label: "2nd Overall Pick", season: 2027, pid: 20 },
			subject,
			"college",
		);
		assert.ok(o.photograph?.includes("college"));
		assert.ok(o.uniform?.includes('"Kansas"'));
		assert.ok(o.uniform?.includes("do NOT reproduce any real university"));
	});
});

describe("card prompts with an override", () => {
	const subject: CardSubject = {
		name: "Test Player",
		pos: "PG",
		heightIn: 75,
		weightLbs: 190,
		teamName: "Detroit Pistons",
		season: 2027,
		awards: [],
		stats: [],
	};
	const setId = CARD_SETS[0]!.id;

	test("a photograph override replaces the candid action but keeps the cartoon rule", () => {
		const front = buildCardFrontPrompt(setId, "base", subject, 0, {
			photograph: "UNIQUE-SCENE-MARKER on the draft stage.",
			achievement: "1st Overall Pick, 2027",
		});
		assert.ok(front.includes("UNIQUE-SCENE-MARKER"));
		assert.ok(!front.includes("A CANDID shot"), "the stock scene is replaced");
		assert.ok(
			front.includes("RENDERED in flat faces.js cartoon style"),
			"the render style applies to every scene",
		);
		assert.ok(front.includes(`reading exactly "1st Overall Pick, 2027"`));
	});

	test("a uniform override replaces the jersey section", () => {
		const front = buildCardFrontPrompt(setId, "base", subject, 0, {
			uniform: "UNIQUE-UNIFORM-MARKER: a suit and cap.",
		});
		assert.ok(front.includes("UNIQUE-UNIFORM-MARKER"));
		assert.ok(!front.includes("He is wearing the Detroit Pistons uniform"));
	});

	test("no override means the standard card, byte for byte", () => {
		assert.strictEqual(
			buildCardFrontPrompt(setId, "base", subject, 0),
			buildCardFrontPrompt(setId, "base", subject, 0, undefined),
		);
	});

	test("safe mode strips what image models refuse, and keeps the card", () => {
		const risky = [
			/may coincide with real people/i,
			/real-world memory/i,
			/its real wordmark/i,
			/\bNBA\b/,
		];
		const normal = buildCardFrontPrompt(setId, "base", subject, 0);
		assert.ok(
			risky.some((re) => re.test(normal)),
			"the default prompt is the one that gets refused",
		);

		const safe = buildCardFrontPrompt(setId, "base", subject, 0, {
			safeMode: true,
		});
		for (const re of risky) {
			assert.ok(!re.test(safe), `safe mode still contains ${re}`);
		}
		assert.ok(
			safe.includes("FICTIONAL CHARACTER"),
			"safe mode states the player is fictional",
		);
		assert.ok(
			safe.includes("INVENT the design"),
			"safe mode invents the uniform instead of reproducing one",
		);
		// The card is still the card: same shape, same season, same stat grid.
		assert.ok(safe.includes("2027"));
		assert.ok(safe.includes("Test Player"));
		assert.ok(
			buildCardBackPrompt(setId, "base", subject, { safeMode: true }).includes(
				"FICTIONAL CHARACTER",
			),
		);
	});

	test("safe mode reaches the draft-night scene, which asks for real merch", () => {
		const spec = {
			kind: "draft" as const,
			label: "1st Overall Pick",
			season: 2027,
			pid: 10,
		};
		const unsafe = achievementPromptOverride(
			spec,
			subject,
			"draftNight",
			false,
		);
		assert.ok(/franchise's real 2027 design/.test(unsafe.uniform ?? ""));

		const safe = achievementPromptOverride(spec, subject, "draftNight", true);
		assert.ok(!/real 2027 design/.test(safe.uniform ?? ""));
		assert.ok(/INVENT the cap and jersey/.test(safe.uniform ?? ""));
	});

	test("the back gains the commemoration line", () => {
		const back = buildCardBackPrompt(setId, "base", subject, {
			achievement: "Finals MVP, 2027",
		});
		assert.ok(back.includes("This card commemorates: **Finals MVP, 2027**"));
	});
});
