import { assert, describe, test } from "vitest";
import { applyPaths, deepEqual, diffPaths } from "./fieldPaths.ts";

const player = () => ({
	pid: 7,
	tid: 3,
	injury: { type: "Healthy", gamesRemaining: 0 },
	stats: [
		{ season: 2026, tid: 3, gp: 10, pts: 150 },
	] as Record<string, number>[],
	transactions: [{ season: 2025, tid: 3, type: "draft" }] as unknown[],
	ratings: [{ season: 2026, ovr: 60, skills: ["3", "B"] }],
});

describe("fieldPaths", () => {
	test("an unchanged record has no paths", () => {
		assert.deepEqual(diffPaths(player(), player()), []);
	});

	test("a trade: the team, a new stats row and a transaction", () => {
		const base = player();
		const traded = player();
		traded.tid = 9;
		traded.stats.push({ season: 2026, tid: 9, gp: 0, pts: 0 });
		traded.transactions.push({ season: 2026, tid: 9, type: "trade" });
		assert.deepEqual(diffPaths(base, traded), [
			["tid"],
			["stats", 1],
			["transactions", 1],
		]);
	});

	// THE RACE THIS IS FOR. A league mate's trade, captured before a sim and
	// published after it, laid over the sim's player: the sim's games and
	// injury stay, the trade moves him.
	test("a trade laid over a sim keeps the sim's stats and injury", () => {
		const base = player();
		const traded = player();
		traded.tid = 9;
		traded.stats.push({ season: 2026, tid: 9, gp: 0, pts: 0 });
		const simmed = player();
		simmed.stats[0]!.gp = 13;
		simmed.stats[0]!.pts = 201;
		simmed.injury = { type: "Sprained Ankle", gamesRemaining: 4 };

		const paths = diffPaths(base, traded)!;
		const merged = applyPaths(simmed, traded, paths);
		assert.strictEqual(merged.tid, 9);
		assert.strictEqual(merged.stats[0]!.gp, 13);
		assert.strictEqual(merged.stats[0]!.pts, 201);
		assert.deepEqual(merged.stats[1], { season: 2026, tid: 9, gp: 0, pts: 0 });
		assert.strictEqual(merged.injury.type, "Sprained Ankle");
		// Neither input changed.
		assert.strictEqual(simmed.tid, 3);
		assert.strictEqual(traded.stats[0]!.gp, 10);
	});

	test("applied to a record that equals the base, it is the edit itself", () => {
		const base = player();
		const edited = player();
		edited.tid = 9;
		delete (edited as any).injury;
		edited.ratings[0]!.skills = ["3"];
		const paths = diffPaths(base, edited)!;
		assert.isTrue(deepEqual(applyPaths(base, edited, paths), edited));
	});

	test("a shrunk array travels whole", () => {
		const base = player();
		const edited = player();
		edited.transactions = [];
		assert.deepEqual(diffPaths(base, edited), [["transactions"]]);
	});

	test("a path that doesn't line up takes the whole top-level field", () => {
		const base = player();
		const edited = player();
		edited.stats.push({ season: 2026, tid: 9, gp: 0, pts: 0 });
		const paths = diffPaths(base, edited)!;
		const other = player();
		other.stats = [];
		const merged = applyPaths(other, edited, paths);
		assert.deepEqual(merged.stats, edited.stats);
	});

	test("too much changed, or a different kind of value, travels whole", () => {
		assert.isUndefined(diffPaths({ a: 1 }, [1]));
		const base = { xs: Array.from({ length: 300 }, () => 0) };
		const edited = { xs: Array.from({ length: 300 }, () => 1) };
		assert.isUndefined(diffPaths(base, edited));
	});
});
