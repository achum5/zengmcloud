import { assert, beforeAll, test } from "vitest";
import { formatClock } from "../../../../common/formatClock.ts";
import { buildFieldClocks, fieldClockAt, type FieldClocks } from "./clock.ts";
import { compileField, type FieldTimeline } from "./director.ts";
import { buildPlayClocks, playClockAt } from "./playClock.ts";
import { simGames, type SimGame } from "./testGames.ts";

let games: { game: SimGame; tl: FieldTimeline; clocks: FieldClocks }[] = [];

beforeAll(async () => {
	games = (await simGames([18, 19])).map((game, k) => {
		const tl = compileField({
			events: game.events,
			players: game.roster,
			gid: 18 + k,
		});
		return { game, tl, clocks: buildFieldClocks(tl, game.events, 4) };
	});
}, 120_000);

const secondsOf = (text: string): number => {
	const [m, s] = text.split(":");
	return Number(m) * 60 + Number(s);
};

test("at each snap, the sim's clock - and from one to the next, only ever down, never quicker than it has to", () => {
	for (const { game, tl, clocks } of games) {
		for (const play of tl.plays) {
			const e = game.events[play.i];
			if (e?.type !== "clock") {
				continue;
			}
			assert.strictEqual(
				fieldClockAt(clocks, play.snap)?.text,
				formatClock(e.clock),
				`${play.i}`,
			);
			// (Through the play at no more than real speed - give or take the
			// seconds rounded up, and a minute's last held at :59.)
			const from = secondsOf(fieldClockAt(clocks, play.snap)!.text);
			const to = fieldClockAt(clocks, play.whistle - 1)!;
			if (!to.ended) {
				assert.isAtMost(
					from - secondsOf(to.text),
					(play.whistle - play.snap) / 1000 + 2,
					`${play.i}`,
				);
			}
		}
		let last: { q: number; secs: number } | undefined;
		for (let t = 0; t < tl.end; t += 250) {
			const now = fieldClockAt(clocks, t)!;
			const play = tl.plays.find((p) => t >= p.t0 && t < p.t1);
			if (now.ended || !play) {
				last = undefined;
				continue;
			}
			const secs = secondsOf(now.text);
			if (last && last.q === play.quarter) {
				assert.isAtMost(secs, last.secs, `${t}`);
			}
			last = { q: play.quarter, secs };
		}
	}
});

test("between quarters the bug says so - END Q1, HALF, END Q3 - and FINAL once it is over", () => {
	for (const { tl, clocks } of games) {
		const said = new Set<string>();
		for (const [k, play] of tl.plays.entries()) {
			const next = tl.plays[k + 1];
			if (next && next.quarter === play.quarter) {
				continue;
			}
			const now = fieldClockAt(clocks, play.t1 - 1)!;
			assert.isTrue(now.ended, `${play.i}`);
			said.add(now.text);
			// (The time run out first.)
			assert.strictEqual(fieldClockAt(clocks, play.snap)!.ended, false);
		}
		assert.includeMembers([...said], ["END Q1", "HALF", "END Q3", "FINAL"]);
		assert.strictEqual(fieldClockAt(clocks, tl.end + 60_000)!.text, "FINAL");
	}
});

test("the play clock: up before each down from scrimmage, counting down at real speed, gone at the snap", () => {
	for (const { game, tl } of games) {
		const spans = buildPlayClocks(tl, game.events);
		let shown = 0;
		for (const play of tl.plays) {
			const at = playClockAt(spans, play.snap - 1);
			if (play.kind === "kickoff" || play.kind === "onside") {
				assert.isUndefined(at, `${play.i}`);
				continue;
			}
			if (play.kind === "none" || play.snap <= play.t0) {
				continue;
			}
			shown++;
			assert.isDefined(at, `${play.i}`);
			// (Never more than 40 on it, never run out on a play run.)
			const first = playClockAt(spans, play.t0)!;
			assert.isAtMost(first, 40, `${play.i}`);
			assert.isAtLeast(at!, 1, `${play.i}`);
			assert.closeTo(first - at!, (play.snap - 1 - play.t0) / 1000, 1e-6);
			assert.isUndefined(playClockAt(spans, play.snap), `${play.i}`);
			assert.isUndefined(playClockAt(spans, play.whistle), `${play.i}`);
		}
		assert.isAbove(shown, 100);
	}
});

test("a delay of game runs the play clock out", () => {
	const events = [
		{ type: "clock", clock: 2.5, down: 1, scrimmage: 70, t: 0, toGo: 10 },
		{ type: "flag", clock: 2.5 },
		{
			type: "penalty",
			clock: 2.5,
			decision: "accept",
			penaltyName: "Delay of game",
			names: [],
			t: 0,
			yds: 5,
		},
	];
	const play = {
		i: 0,
		kind: "none",
		quarter: 4,
		t0: 0,
		snap: 6000,
		whistle: 6800,
		flags: [{}],
	};
	const spans = buildPlayClocks(
		{ plays: [play] } as unknown as FieldTimeline,
		events,
	);
	assert.strictEqual(playClockAt(spans, 6000), 0);
	assert.strictEqual(playClockAt(spans, 7000), 0);
	assert.isAbove(playClockAt(spans, 5000)!, 0);
	assert.isUndefined(playClockAt(spans, 8000));
});
