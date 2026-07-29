import { assert, beforeAll, describe, test } from "vitest";
import {
	buildRecapLinksForGame,
	buildPlayerNoteLinks,
	buildTeamSeasonRecapLinks,
	linkifyRecap,
	linkifySeasonNote,
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

describe("buildTeamSeasonRecapLinks", () => {
	// leagueUrl reads the current lid from local state.
	beforeAll(() => {
		local.setState({ lid: 1 });
	});

	const teamInfoCache = [
		{ abbrev: "LAL", region: "LA", name: "Lakers" }, // tid 0
		{ abbrev: "BOS", region: "Boston", name: "Celtics" }, // tid 1
	];
	const players = [
		{ pid: 10, firstName: "Star", lastName: "Guy" },
		{ pid: 20, firstName: "Role", lastName: "Player" },
	];

	test("links every league team and this season's roster players", () => {
		const links = buildTeamSeasonRecapLinks({
			season: 2026,
			players,
			teamInfoCache,
		});
		const names = links.map((l) => l.name);
		assert.ok(names.includes("LA Lakers"));
		assert.ok(names.includes("Lakers"));
		assert.ok(names.includes("Boston Celtics"));
		assert.ok(names.includes("Star Guy"));
		assert.ok(names.includes("Role Player"));
	});

	test("team href points at the season roster; player href at the player", () => {
		const links = buildTeamSeasonRecapLinks({
			season: 2026,
			players,
			teamInfoCache,
		});
		const celtics = links.find((l) => l.name === "Boston Celtics");
		const star = links.find((l) => l.name === "Star Guy");
		assert.ok(celtics!.href.includes("roster/BOS_1/2026"), celtics!.href);
		assert.ok(star!.href.includes("player/10"), star!.href);
	});

	test("skips empty team slots", () => {
		const withGap = [
			{ abbrev: "LAL", region: "LA", name: "Lakers" },
			undefined,
		];
		const links = buildTeamSeasonRecapLinks({
			season: 2026,
			players: [],
			teamInfoCache: withGap as any,
		});
		assert.ok(links.every((l) => l.name !== "undefined"));
		assert.ok(links.some((l) => l.name === "LA Lakers"));
	});
});

describe("linkifySeasonNote", () => {
	beforeAll(() => {
		local.setState({ lid: 1 });
	});

	const teamInfoCache = [
		{ abbrev: "BOS", region: "Boston", name: "Celtics" },
		{ abbrev: "LAL", region: "Los Angeles", name: "Lakers" },
	];

	const note = [
		"[2003] Still the anchor",
		"He carried the Boston Celtics again.",
		"",
		"[2001]",
		"A quiet first year in Boston.",
	].join("\n");

	// The whole point: a career note spans many years, and a team named in the
	// 2001 section means that team in 2001. Linking the note against one year
	// would send every mention to the wrong page.
	test("each section links to its own season", () => {
		const out = linkifySeasonNote(note, buildPlayerNoteLinks(teamInfoCache));
		assert.ok(out.includes("[Boston Celtics](/l/1/roster/BOS_0/2003)"), out);
		assert.ok(out.includes("[Boston](/l/1/roster/BOS_0/2001)"), out);
	});

	test("the year headers are left alone", () => {
		const out = linkifySeasonNote(note, buildPlayerNoteLinks(teamInfoCache));
		assert.ok(out.includes("[2003] Still the anchor"));
		assert.ok(out.includes("\n[2001]\n"));
	});

	test("text written before any year header still links, without a season", () => {
		const out = linkifySeasonNote(
			"Hand-typed by me about Boston.\n\n[2001]\nRookie year.",
			buildPlayerNoteLinks(teamInfoCache),
		);
		assert.ok(out.includes("[Boston](/l/1/roster/BOS_0)"), out);
	});

	test("a note with no year headers is linked as one piece", () => {
		const out = linkifySeasonNote(
			"Traded to the Los Angeles Lakers.",
			buildPlayerNoteLinks(teamInfoCache),
		);
		assert.ok(out.includes("[Los Angeles Lakers](/l/1/roster/LAL_1)"), out);
	});

	test("an empty note is left as is", () => {
		assert.strictEqual(
			linkifySeasonNote("", buildPlayerNoteLinks(teamInfoCache)),
			"",
		);
	});
});

describe("linking teammates in a player note", () => {
	beforeAll(() => {
		local.setState({ lid: 1 });
	});

	const teamInfoCache = [{ abbrev: "BOS", region: "Boston", name: "Celtics" }];
	const teammates = [
		{ season: 2003, players: [{ pid: 9, name: "Paul Pierce" }] },
		{ season: 2001, players: [{ pid: 12, name: "Antoine Walker" }] },
	];

	test("a teammate named in a season's section links to his page", () => {
		const out = linkifySeasonNote(
			"[2003]\nHe and Paul Pierce carried it.",
			buildPlayerNoteLinks(teamInfoCache, teammates),
		);
		assert.ok(out.includes("[Paul Pierce](/l/1/player/9)"), out);
	});

	// Scoping is the whole point: a name is only linked in the years the two
	// actually played together, so a common name can't be resolved to whichever
	// player happened to be found first.
	test("a name is not linked in a season they didn't share", () => {
		const out = linkifySeasonNote(
			"[2001]\nHe and Paul Pierce carried it.",
			buildPlayerNoteLinks(teamInfoCache, teammates),
		);
		assert.ok(!out.includes("/l/1/player/9"), out);
		assert.ok(out.includes("Paul Pierce"));
	});

	test("teammates and teams are linked in the same pass", () => {
		const out = linkifySeasonNote(
			"[2001]\nAntoine Walker led Boston.",
			buildPlayerNoteLinks(teamInfoCache, teammates),
		);
		assert.ok(out.includes("[Antoine Walker](/l/1/player/12)"), out);
		assert.ok(out.includes("[Boston](/l/1/roster/BOS_0/2001)"), out);
	});

	test("a bolded teammate keeps its bold", () => {
		const out = linkifySeasonNote(
			"[2003]\n**Paul Pierce** did the rest.",
			buildPlayerNoteLinks(teamInfoCache, teammates),
		);
		assert.ok(out.includes("**[Paul Pierce](/l/1/player/9)**"), out);
	});

	test("no teammate data still links teams", () => {
		const out = linkifySeasonNote(
			"[2001]\nHe led Boston.",
			buildPlayerNoteLinks(teamInfoCache),
		);
		assert.ok(out.includes("[Boston](/l/1/roster/BOS_0/2001)"), out);
	});
});

// A retirement writeup is an article about a whole career, not an entry in a
// season log. It was rendering as "[2003] Retirement — A hundred and three
// games Crotty started 41 games for..." - the year, the marker and the headline
// all run into the first sentence, because the stored header and body are
// separated by a single newline and markdown treats that as a soft break.
describe("a retirement writeup reads as an article", () => {
	const note = [
		"[2003] Retirement — A hundred and three games",
		"Crotty started 41 games for a Boston team that lost 76.",
		"",
		"[2001] Crotty played three games for Boston.",
	].join("\n");

	const out = () => linkifySeasonNote(note, () => []);

	test("the headline stands alone, with no year", () => {
		assert.ok(out().startsWith("**A hundred and three games**\n\n"), out());
	});

	test("the year, the marker and the em dash are all gone from it", () => {
		const headline = out().split("\n")[0]!;
		assert.ok(!headline.includes("2003"), headline);
		assert.ok(!headline.includes("Retirement"), headline);
		assert.ok(!headline.includes("—"), headline);
	});

	test("the body is its own paragraph, not a continuation", () => {
		assert.ok(out().includes("**\n\nCrotty started 41 games"), out());
	});

	// Only retirement. A season entry's year is the whole point of it.
	test("season sections keep their year label", () => {
		assert.ok(out().includes("[2001] Crotty played three games"), out());
	});

	test("a retirement writeup with no headline still gets one", () => {
		const bare = linkifySeasonNote(
			"[2003] Retirement\nHe walked away at 35.",
			() => [],
		);
		assert.ok(bare.startsWith("**Retirement**\n\n"), bare);
	});

	// The stored format is untouched, so re-running a batch still finds and
	// replaces the same section rather than appending a duplicate.
	test("only the display changes - links still know the season", () => {
		const linked = linkifySeasonNote(
			"[2003] Retirement — The end\nHe finished in Boston.",
			(season) =>
				season === 2003
					? [{ name: "Boston", href: "/l/1/roster/BOS_1/2003" }]
					: [],
		);
		assert.ok(linked.includes("[Boston](/l/1/roster/BOS_1/2003)"), linked);
	});
});
