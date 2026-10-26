import { expect, test } from "vitest";
import { getScoreBreakdown, splitTerms } from "./scoreBreakdown.ts";

test("splitTerms splits only top-level binary + and -", () => {
	expect(
		splitTerms("-pts + 2*-ast - min(a-b, 1) - 1e-5 * x").map((term) => [
			term.sign,
			term.text,
		]),
	).toEqual([
		[1, "-pts"],
		[1, "2*-ast"],
		[-1, "min(a-b, 1)"],
		[-1, "1e-5 * x"],
	]);
});

test("getScoreBreakdown terms add up to the score", () => {
	const formula =
		"ewa / 22 + vorp / 32 + min(ws/max(teamWs,ws),0.8) / 10 + seasonFraction * winp";
	const breakdown = getScoreBreakdown(
		formula,
		{ ewa: 11, vorp: 4, ws: 10, teamWs: 40, seasonFraction: 0.5, winp: 0.6 },
		false,
	)!;
	expect(breakdown.values.map(([name]) => name)).toEqual([
		"ewa",
		"vorp",
		"ws",
		"teamWs",
		"seasonFraction",
		"winp",
	]);
	expect(breakdown.terms!.map((term) => term.value)).toEqual([
		0.5, 0.125, 0.025, 0.3,
	]);
});
