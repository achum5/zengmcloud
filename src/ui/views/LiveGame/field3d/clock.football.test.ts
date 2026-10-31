import { assert, beforeAll, test } from "vitest";
import { formatClock } from "../../../../common/formatClock.ts";
import { buildFieldClocks, fieldClockAt, type FieldClocks } from "./clock.ts";
import { compileField, type FieldTimeline } from "./director.ts";
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
