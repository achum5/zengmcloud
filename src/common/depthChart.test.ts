import { assert, test } from "vitest";
import { depthChart } from "./depthChart.ts";

const names = (groups: ({ name: string } | undefined)[][]) =>
	groups.map((group) => group.map((p) => p?.name ?? "-"));

test("each group of five goes point guard to center", () => {
	const players = [
		{ name: "C", posValue: 4.1 },
		{ name: "SG", posValue: 1.2 },
		{ name: "PF", posValue: 3 },
		{ name: "PG", posValue: -0.3 },
		{ name: "SF", posValue: 2.2 },
		{ name: "c", posValue: 3.6 },
		{ name: "g", posValue: 0.5 },
	];
	assert.deepEqual(names(depthChart(players, 5)), [
		["PG", "SG", "SF", "PF", "C"],
		["g", "-", "-", "-", "c"],
	]);
});

test("a short group fills the spots its players fit", () => {
	const players = [1, 2, 3, 4, 5].map((i) => ({ name: `s${i}`, posValue: 2 }));
	players.push(
		{ name: "f", posValue: 2.9 },
		{ name: "f2", posValue: 3.1 },
		{ name: "g", posValue: 0.2 },
	);
	assert.deepEqual(names(depthChart(players, 5)), [
		["s1", "s2", "s3", "s4", "s5"],
		["g", "-", "f", "f2", "-"],
	]);
});

test("third string at most, and other numbers on the court", () => {
	const players = Array.from({ length: 17 }, (_, i) => ({
		name: `p${i}`,
		posValue: i % 4,
	}));
	const groups = depthChart(players, 5);
	assert.strictEqual(groups.length, 3);
	assert.deepEqual(
		groups.map((group) => group.length),
		[5, 5, 5],
	);
	assert.deepEqual(names(depthChart(players.slice(0, 3), 3)), [
		["p0", "p1", "p2"],
	]);
});
