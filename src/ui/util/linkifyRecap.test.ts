import { assert, beforeAll, describe, test } from "vitest";
import {
	buildRecapLinksForGame,
	linkifyRecap,
	type RecapLink,
} from "./linkifyRecap.ts";
import { local } from "./local.ts";

const P = (name: string, pid: number): RecapLink => ({
	name,
	href: `/l/1/player/${pid}`,
});

describe("linkifyRecap", () => {
	test("links a plain name", () => {
		const out = linkifyRecap("Zion Williamson scored 30.", [
			P("Zion Williamson", 5),
		]);
		assert.strictEqual(out, "[Zion Williamson](/l/1/player/5) scored 30.");
	});

	test("keeps bold on a bolded name (bold link)", () => {
		const out = linkifyRecap("**Zion Williamson** was great.", [
			P("Zion Williamson", 5),
		]);
		assert.strictEqual(out, "**[Zion Williamson](/l/1/player/5)** was great.");
	});

	test("longer name wins; shorter name doesn't double-link inside it", () => {
		const out = linkifyRecap("Trey Murphy III struggled.", [
			P("Trey Murphy III", 7),
			P("Murphy", 9),
		]);
		assert.strictEqual(out, "[Trey Murphy III](/l/1/player/7) struggled.");
	});

	test("does not corrupt plain numbers in the text", () => {
		const out = linkifyRecap("combining for 27 efficient points", [
			P("Nobody Here", 1),
		]);
		assert.strictEqual(out, "combining for 27 efficient points");
	});

	test("links team names too", () => {
		const out = linkifyRecap("the Bayou faded late", [
			{ name: "Bayou", href: "/l/1/roster/NOL_3" },
		]);
		assert.strictEqual(out, "the [Bayou](/l/1/roster/NOL_3) faded late");
	});

	test("only whole-word matches (no substring links)", () => {
		// "Cam" must not link inside "Camden".
		const out = linkifyRecap("Camden and Cam played.", [P("Cam", 4)]);
		assert.strictEqual(out, "Camden and [Cam](/l/1/player/4) played.");
	});
});

describe("buildRecapLinksForGame", () => {
	// leagueUrl reads the current lid from local state.
	beforeAll(() => {
		local.setState({ lid: 1 });
	});

	const names = (links: RecapLink[]) => links.map((l) => l.name);

	const game = {
		season: 2076,
		teams: [
			{
				tid: 0,
				// Past-season branding travels on the game (Daily Schedule sets this).
				branding: { abbrev: "LAL", region: "LA", name: "Lakers" },
				players: [{ pid: 10, name: "Star Guy" }],
			},
			{
				tid: 1,
				players: [{ pid: 20, name: "Role Guy" }],
			},
			// A placeholder team (e.g. an All-Star side) is skipped.
			{ tid: -1, players: [{ pid: 30, name: "Ghost" }] },
		],
	};

	const teamInfo = (tid: number) =>
		tid === 1
			? { abbrev: "BOS", region: "Boston", name: "Celtics" }
			: undefined;

	test("links both teams (branding or resolver) and their players", () => {
		const out = names(buildRecapLinksForGame(game, teamInfo));
		// Team 0 via its own branding, team 1 via the resolver.
		assert.ok(out.includes("LA Lakers"));
		assert.ok(out.includes("Lakers"));
		assert.ok(out.includes("Boston Celtics"));
		assert.ok(out.includes("Celtics"));
		// Players from both real teams.
		assert.ok(out.includes("Star Guy"));
		assert.ok(out.includes("Role Guy"));
	});

	test("skips placeholder (negative-tid) teams and their players", () => {
		const out = names(buildRecapLinksForGame(game, teamInfo));
		assert.ok(!out.includes("Ghost"));
	});

	test("team href points at the season-correct roster; player href at the player", () => {
		const links = buildRecapLinksForGame(game, teamInfo);
		const lakers = links.find((l) => l.name === "LA Lakers");
		const star = links.find((l) => l.name === "Star Guy");
		assert.ok(lakers!.href.includes("roster/LAL_0/2076"), lakers!.href);
		assert.ok(star!.href.includes("player/10"), star!.href);
	});
});
