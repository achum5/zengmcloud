import { assert, describe, test } from "vitest";
import { compileCourt, compileCourtPart } from "./director.ts";
import { evalPlayer } from "./evaluate.ts";
import { stagedAs } from "./follow.ts";
import { goesOn, mergeCourt, partsOf, unsorted, type Staged } from "./parts.ts";
import { fakeGame, stepped } from "./testGame.ts";

// THE GAME A PERIOD AT A TIME (see parts.ts): staged a part at a time and
// put together, it is the whole game's - the same lines at the same moments,
// nobody anywhere else at a join than he would be.
describe("3D: a game staged a period at a time", () => {
	const { events, players } = fakeGame("parts", 50);
	const input = { events, players, gid: 3, step: stepped };
	const parts = partsOf(events);

	test("a part a period, each through the line that starts the next", () => {
		assert.strictEqual(parts.length, 3);
		assert.strictEqual(parts[0]!.from, 0);
		assert.strictEqual(parts.at(-1)!.to, events.length - 1);
		for (let k = 1; k < parts.length; k++) {
			assert.strictEqual(parts[k]!.from, parts[k - 1]!.to + 1);
			assert.strictEqual(events[parts[k - 1]!.to]!.type, "period");
		}
		// (A reel of highlights, whole.)
		const reel = events.map((e, i) =>
			i === 0 ? { ...e, clipStart: true } : e,
		);
		assert.strictEqual(partsOf(reel).length, 1);
	});

	test("put together: the whole game's lines, moments and men", () => {
		unsorted.clear();
		const staged: Staged[] = [];
		let m: Staged | undefined;
		for (let k = 0; k < parts.length; k++) {
			const tl = compileCourtPart(input, parts[k]!, k === parts.length - 1);
			tl.staging = 1;
			staged.push(tl);
			const was = m;
			m = m ? mergeCourt(m, tl) : tl;
			if (was) {
				assert.isTrue(goesOn(was, m));
				assert.isAbove(m.beats.length, was.beats.length);
			}
			if (k < parts.length - 1) {
				assert.strictEqual(m.upTo, parts[k]!.to + 1);
			}
		}
		// Every field a director has is one parts.ts knows what to do with
		// at a part's start (kept, or cut back to it - KEPT, CUT): a new one,
		// say which.
		assert.deepEqual([...unsorted], []);
		const whole = compileCourt(input);
		const all = m!;
		assert.isUndefined(all.upTo);
		assert.isUndefined(all.from);
		assert.strictEqual(all.end, whole.end);
		assert.deepEqual(all.beats, whole.beats);
		// (Each part's picture says the game is the same one as the first's.)
		const fp = stagedAs(staged[0]!);
		assert.strictEqual(stagedAs(all), fp);
		// Who checks in when, the subs at the start of a period too (on their
		// way to the table in the break before it).
		const ins = (tl: Staged) =>
			(tl.checkIns ?? []).map((c) => `${c.pid}@${Math.round(c.t0)}`);
		assert.deepEqual(ins(all), ins(whole));
		// In order, and about as much of it as the whole game has.
		for (const tr of all.tracks.values()) {
			for (const list of [tr.moves, tr.acts, tr.arms]) {
				for (let i = 1; i < list.length; i++) {
					assert.isAtLeast(list[i]!.t0, list[i - 1]!.t0);
				}
			}
			for (const list of [tr.faces, tr.looks, tr.shown]) {
				for (let i = 1; i < list.length; i++) {
					assert.isAtLeast(list[i]![0], list[i - 1]![0]);
				}
			}
		}
		for (const key of ["ball", "fx", "poss", "checkIns", "sitDowns"] as const) {
			const a = all[key]?.length ?? 0;
			const b = whole[key]?.length ?? 0;
			assert.isAtLeast(a, b * 0.9, key);
			assert.isAtMost(a, b * 1.1, key);
		}
		// At each join, nobody moves faster than a man can - but where the
		// whole game has him put straight there too (a cut at the restart).
		const fastest = (tl: Staged, pid: number, at: number) => {
			let most = 0;
			let prev = evalPlayer(tl, pid, at - 3000);
			for (let t = at - 2950; t <= at + 3000; t += 50) {
				const p = evalPlayer(tl, pid, t);
				most = Math.max(most, Math.hypot(p.x - prev.x, p.y - prev.y) / 0.05);
				prev = p;
			}
			return most;
		};
		for (const part of staged.slice(1)) {
			const at = part.from!;
			for (const pid of whole.tracks.keys()) {
				if (pid < 0) {
					continue;
				}
				if (fastest(all, pid, at) > 35) {
					assert.isAbove(fastest(whole, pid, at), 35, `${pid} at ${at}`);
				}
			}
		}
	}, 600_000);
});
