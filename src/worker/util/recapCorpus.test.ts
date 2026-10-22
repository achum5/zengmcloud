import { assert, test } from "vitest";
import {
	NUM_TEAMS,
	simRecapLeague,
	withSeededRandom,
} from "../../test/recapLeague.ts";
import { idb } from "../db/index.ts";
import {
	getAutoRecapsForDay,
	recapGamesForDay,
	type RecapGame,
} from "./getDayGamesForRecap.ts";
import { verifyRecap } from "./recapAccuracy.ts";
import {
	buildFeedDay,
	clearSocialFeedCache,
	getFeedSnapshot,
} from "./socialFeed.ts";

// A CORPUS OF REAL RECAPS.
//
// getAutoRecap writes every recap the game shows - one per game, one per day -
// and the only honest way to judge it is to read a lot of its output at once.
// A weakness a hand-built fixture never shows will show up nine times across a
// season. So: build a league, play it with the real engine, store the games the
// way the game stores them, and run the real recap path over every day.
//
//   SPORT=basketball RECAP_DAYS=30 RECAP_SEED=1 \
//     RECAP_LOG=/tmp/recaps.txt npx vitest --run src/worker/util/recapCorpus.test.ts
//
// Writes the corpus to RECAP_LOG and a metrics summary to RECAP_LOG.stats.txt.
// Skipped entirely unless RECAP_LOG is set, so it costs CI nothing.
const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};

const LOG = nodeEnv.RECAP_LOG;
const DAYS = Number(nodeEnv.RECAP_DAYS ?? 30);
// A postseason on the end of the regular season, so the playoff recaps - the
// ones a reader cares most about - are measured too. Off by default so a quick
// corpus run stays quick; RECAP_PLAYOFFS=1 turns it on.
const PLAYOFFS = nodeEnv.RECAP_PLAYOFFS === "1";
const SEED = Number(nodeEnv.RECAP_SEED ?? 1);

test("recap corpus", { timeout: 3_600_000 }, async () => {
	if (!LOG) {
		return;
	}
	// Same dodge decadesSim uses: the typecheck project has no node types.
	const { writeFileSync } = await import(("node" + ":fs") as any);

	// SEED THE WHOLE RUN. common/random.ts calls Math.random directly, so player
	// generation and GameSim are otherwise a different league every time - which
	// makes a before/after on a phrasing change a comparison of two different
	// seasons. Rates over 900 games still move honestly that way, but nothing
	// small does, and no individual recap can be diffed at all.
	await withSeededRandom(SEED, () => runCorpus(writeFileSync));
});

const runCorpus = async (writeFileSync: (p: string, d: string) => void) => {
	const LOG = nodeEnv.RECAP_LOG!;
	const { season, lastDay } = await simRecapLeague({
		days: DAYS,
		seed: SEED,
		playoffs: PLAYOFFS,
	});

	// The real recap path, day by day. Every recap is dumped WITH the RecapGame
	// it was written from, so an accuracy checker can hold each number in the
	// prose against the box score it came out of.
	const out: string[] = [];
	const gameRecaps: string[] = [];
	const dayRecaps: string[] = [];
	const pairs: { gid: number; recap: string; game: RecapGame }[] = [];
	for (let day = 1; day <= lastDay; day++) {
		const { notes, dayRecap } = await getAutoRecapsForDay({ season, day });
		for (const rg of await recapGamesForDay({ season, day })) {
			const note = notes[rg.gid];
			if (note) {
				pairs.push({ gid: rg.gid, recap: note, game: rg });
			}
		}
		out.push(
			`\n${"=".repeat(78)}\nDAY ${day}\n${"=".repeat(78)}\n`,
			`--- DAY RECAP ---\n${dayRecap}\n`,
		);
		dayRecaps.push(dayRecap);
		for (const [id, note] of Object.entries(notes)) {
			gameRecaps.push(note);
			out.push(`--- GAME ${id} ---\n${note}\n`);
		}
	}
	writeFileSync(LOG, out.join("\n"));
	writeFileSync(`${LOG}.jsonl`, pairs.map((x) => JSON.stringify(x)).join("\n"));

	// THE LEAGUE'S NEWS, which this harness never had. It plays games straight
	// through GameSim, so nothing ever writes to the events store - and the
	// feed reads that store for everything that is not a game. Reading a
	// corpus therefore showed a timeline made of results, performances and
	// standings and NOTHING ELSE, which says nothing about whether the other
	// half works: a league with no injuries, trades or signings in its log
	// cannot produce posts about them.
	//
	// RECAP_NEWS=1 writes a representative spread of the log's own event types
	// so that half can be read at all. These are synthetic, in the same shape
	// the league writes (see eventFromLeagueEvent: eid, type, season, text,
	// score, tids, pids), and they are only ever added by this harness.
	if (nodeEnv.RECAP_NEWS === "1") {
		const playing = Array.from({ length: NUM_TEAMS }, (_, i) => i);
		const somePlayers = await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		]);
		const pick = <T>(arr: readonly T[], i: number): T =>
			arr[i % arr.length] as T;
		const NEWS: {
			type: string;
			score: number;
			text: (a: string, b: string, p: string) => string;
			teams: number;
		}[] = [
			{
				type: "injured",
				score: 12,
				teams: 1,
				text: (a, _b, p) =>
					`${p} of the ${a} was injured. (Sprained ankle, out 3 weeks)`,
			},
			{
				type: "trade",
				score: 18,
				teams: 2,
				text: (a, b, p) => `The ${a} traded ${p} to the ${b}.`,
			},
			{
				type: "freeAgent",
				score: 14,
				teams: 1,
				text: (a, _b, p) => `The ${a} signed ${p} for 4 years, $92M.`,
			},
			{
				type: "reSigned",
				score: 11,
				teams: 1,
				text: (a, _b, p) => `The ${a} re-signed ${p} for 3 years, $54M.`,
			},
			{
				type: "release",
				score: 8,
				teams: 1,
				text: (a, _b, p) => `The ${a} released ${p}.`,
			},
			{
				type: "playerFeat",
				score: 16,
				teams: 1,
				text: (a, _b, p) => `${p} of the ${a} had 52 points and 18 rebounds.`,
			},
			{
				type: "award",
				score: 20,
				teams: 1,
				text: (_a, _b, p) => `${p} won Most Valuable Player.`,
			},
			{
				type: "retired",
				score: 15,
				teams: 1,
				text: (a, _b, p) => `${p} of the ${a} retired.`,
			},
			{
				type: "madePlayoffs",
				score: 13,
				teams: 1,
				text: (a) => `The ${a} made the playoffs.`,
			},
		];

		let eid = 1;
		const rows: any[] = [];
		for (let i = 0; i < NEWS.length * 4; i++) {
			const spec = pick(NEWS, i);
			const tidA = pick(playing, i * 3 + 1);
			const tidB = pick(
				playing.filter((t) => t !== tidA),
				i * 5 + 2,
			);
			const teamA = (await idb.cache.teams.get(tidA))!;
			const teamB = (await idb.cache.teams.get(tidB))!;
			const player = pick(somePlayers, i * 7 + 3);
			rows.push({
				eid: eid++,
				type: spec.type,
				season,
				score: spec.score,
				text: spec.text(
					`${teamA.region} ${teamA.name}`,
					`${teamB.region} ${teamB.name}`,
					`${player.firstName} ${player.lastName}`,
				),
				tids: spec.teams === 2 ? [tidA, tidB] : [tidA],
				pids: [player.pid],
			});
		}
		for (const row of rows) {
			await idb.cache.events.add(row);
		}
	}

	// THE FEED, same league, same nights. RECAP_FEED=1 builds every day's
	// timeline through the real feed path and dumps it beside the recaps, so
	// both systems can be read describing the same games - and so the feed can
	// be read at all without clicking through a browser.
	if (nodeEnv.RECAP_FEED === "1") {
		clearSocialFeedCache();
		const snapshot = await getFeedSnapshot(season);
		const feedOut: string[] = [];
		const feedRows: string[] = [];
		for (let i = 0; i < snapshot.days.length; i++) {
			const feedDay = await buildFeedDay({ snapshot, dayIndex: i });
			feedOut.push(
				`\n${"=".repeat(78)}\nFEED DAY ${feedDay.day} (${feedDay.posts.length} posts)\n${"=".repeat(78)}\n`,
			);
			for (const post of feedDay.posts) {
				feedOut.push(
					`@${post.handle} [${post.kind}/${post.archetypeId}]${post.verified ? " *" : ""} ${post.time} | ${post.eventType} | ${post.engagement.likes} likes`,
					post.text,
				);
				if (post.quoted) {
					feedOut.push(
						`    [quoting @${post.quoted.handle}, day ${post.quoted.day}]: ${post.quoted.text}`,
					);
				}
				for (const reply of post.replies) {
					feedOut.push(
						`    > @${reply.handle} [${reply.kind}/${reply.archetypeId}]${reply.quote ? " (quote)" : ""}: ${reply.text}`,
					);
				}
				feedOut.push("");
				feedRows.push(JSON.stringify(post));
			}
		}
		writeFileSync(`${LOG}.feed.txt`, feedOut.join("\n"));
		writeFileSync(`${LOG}.feed.jsonl`, feedRows.join("\n"));

		// What a read of the feed kept turning up, counted, so the next read
		// starts from numbers. A hole in a template ("undefined rebounds too")
		// is fatal; the rest are measurements.
		const texts = feedOut.filter((line) => !line.startsWith("@"));
		const replyTexts = texts
			.filter((line) => line.startsWith("    > "))
			.map((line) => line.replace(/^ {4}> @\S+ \[[^\]]*]( \(quote\))?: /, ""));
		const bare = (t: string) =>
			t
				.toLowerCase()
				.replace(/[#@]\w+/g, "")
				.replace(/[^\d a-z]/g, "")
				.trim();
		const FILLER = new Set([
			"maybe",
			"fair",
			"suppose so",
			"seen it",
			"true",
			"hard to say either way",
			"watching this one closely",
			"say it louder",
			"not sure about that one",
			"cosigned",
			"you have a point",
			"we will find out",
			"okay",
			"good enough for me",
			"noting this one down",
		]);
		const holes = texts.filter((t) => /undefined|NaN|\[object/.test(t));
		const calendar = texts.filter((t) =>
			/\btomorrow\b|\byesterday\b|back-to-back(?! champion)|\bin a month\b|\bapril\b|\boctober\b/i.test(
				t,
			),
		);
		writeFileSync(
			`${LOG}.feed.stats.txt`,
			[
				`posts=${feedRows.length} replies=${replyTexts.length}`,
				`filler replies=${replyTexts.filter((t) => FILLER.has(bare(t))).length}`,
				`calendar claims=${calendar.length}`,
				...calendar.slice(0, 10).map((t) => `    ${t}`),
				`template holes=${holes.length}`,
				...holes.slice(0, 10).map((t) => `    ${t}`),
			].join("\n"),
		);
		assert.deepStrictEqual(holes, [], "a feed line with a hole in it");
	}

	// ACCURACY. Every number in the finished prose, held against the box score
	// it was written from. A corpus run is the only place this can be asked at
	// scale, so it is asked here and it is fatal - a recap that states something
	// the game does not contain is the one defect no amount of good phrasing
	// makes up for.
	const violations: string[] = [];
	for (const { gid, recap, game } of pairs) {
		for (const v of verifyRecap(recap, game)) {
			violations.push(`gid ${gid} [${v.kind}] ${v.detail}\n    ${v.sentence}`);
		}
	}
	writeFileSync(
		`${LOG}.accuracy.txt`,
		violations.length === 0
			? `${pairs.length} recaps checked, no violations`
			: violations.join("\n"),
	);

	// Metrics. Repetition is the failure mode a corpus is FOR: a phrase that
	// reads fine once reads like a template the ninth time on the same page.
	const headline = (r: string) => r.split("\n")[0] ?? "";
	const sentences = (r: string) =>
		r
			.split("\n")
			.slice(1)
			.join(" ")
			.split(/(?<=[!.?])\s+/)
			.map((s) => s.trim())
			.filter(Boolean);

	// A sentence's SHAPE: names and numbers blanked out, so two sentences built
	// from one template collide even though they describe different games.
	// The bold markers come off FIRST. Blanking `**...**` as a unit turned
	// every headline into the single token "N", so the headline metric read
	// "1 distinct shape over 450 games" for a year while the headlines were
	// in fact all different.
	const shape = (s: string) =>
		s
			.replaceAll("**", "")
			.replace(/\b[A-Z][a-z]+\b/g, "N")
			.replace(/\d+(\.\d+)?%?/g, "#")
			.replace(/\s+/g, " ")
			.trim();

	const tally = (items: string[]) => {
		const m = new Map<string, number>();
		for (const i of items) {
			m.set(i, (m.get(i) ?? 0) + 1);
		}
		return [...m.entries()].sort((a, b) => b[1] - a[1]);
	};

	const allSentences = gameRecaps.flatMap(sentences);
	const shapes = tally(allSentences.map(shape));
	const headlineShapes = tally(gameRecaps.map((r) => shape(headline(r))));
	const dupSentences = tally(allSentences).filter(([, c]) => c > 1);

	const stats = [
		`games=${gameRecaps.length} days=${dayRecaps.length}`,
		`game recap: mean chars=${Math.round(gameRecaps.reduce((a, b) => a + b.length, 0) / gameRecaps.length)}`,
		`sentences=${allSentences.length} distinct shapes=${shapes.length} ` +
			`(top shape used ${shapes[0]?.[1]}x = ${((100 * (shapes[0]?.[1] ?? 0)) / allSentences.length).toFixed(1)}%)`,
		`headline shapes: ${headlineShapes.length} distinct over ${gameRecaps.length} games ` +
			`(top ${headlineShapes[0]?.[1]}x)`,
		`verbatim repeated sentences: ${dupSentences.length}`,
		// The two fillers that were crowding out the game: the betting line
		// (35% of recaps at its worst) and a foul-out nobody would have missed.
		`recaps quoting the betting line: ${gameRecaps.filter((r) => /underdog|favorite|favored|the books/.test(r)).length}`,
		`recaps with a foul-out: ${gameRecaps.filter((r) => /fouled out|fouling out|sixth foul|Foul trouble cost/.test(r)).length}`,
		// What the score log makes possible: a close game that says how it ended.
		`recaps saying how it finished: ${gameRecaps.filter((r) => /for good|last tie|tied at|lead changes|changed hands|with two minutes/.test(r)).length}`,
		`recaps naming a run or a blown lead: ${gameRecaps.filter((r) => /-0 run|straight points|as many as|at one stage|At one point/.test(r)).length}`,
		"",
		"TOP 25 SENTENCE SHAPES:",
		...shapes.slice(0, 25).map(([s, c]) => `${String(c).padStart(4)}  ${s}`),
		"",
		"TOP 15 HEADLINE SHAPES:",
		...headlineShapes
			.slice(0, 15)
			.map(([s, c]) => `${String(c).padStart(4)}  ${s}`),
		"",
		"TOP 20 VERBATIM REPEATS:",
		...dupSentences
			.slice(0, 20)
			.map(([s, c]) => `${String(c).padStart(4)}  ${s}`),
	];
	writeFileSync(`${LOG}.stats.txt`, stats.join("\n"));

	assert.ok(gameRecaps.length > 0, "produced recaps");
	assert.strictEqual(
		violations.length,
		0,
		`${violations.length} inaccurate claims across ${pairs.length} recaps:\n${violations
			.slice(0, 25)
			.join("\n")}`,
	);
};
