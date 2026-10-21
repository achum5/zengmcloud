import { assert, test } from "vitest";
import { fitLogistic } from "./logistic.ts";

test("recovers a logistic curve from its own probabilities", () => {
	const rows = Array.from({ length: 30 }, (_, i) => {
		const x = -8 + (16 * i) / 29;
		return { x: [x], p: 1 / (1 + Math.exp(-(-4 + 0.45 * x))) };
	});
	const [a, b] = fitLogistic(rows, [0, 0.3]);
	assert.closeTo(a!, -4, 0.05);
	assert.closeTo(b!, 0.45, 0.01);
});

test("stays finite when one team takes every title", () => {
	// Perfectly separable: plain Newton runs off to infinity here.
	const rows = Array.from({ length: 30 }, (_, i) => ({
		x: [-8 + (16 * i) / 29],
		p: i === 29 ? 1 : 0,
	}));
	const [a, b] = fitLogistic(rows, [0, 0.3]);
	assert.isTrue(Number.isFinite(a!) && Number.isFinite(b!));
	assert.isBelow(Math.abs(b!), 20);
	// And still says the best team is the likeliest champion.
	assert.isAbove(b!, 0);
});

test("fits two strengths at once", () => {
	const rows = [];
	for (let i = 0; i < 10; i++) {
		for (let j = 0; j < 10; j++) {
			const x = [-5 + i, -5 + j];
			rows.push({
				x,
				p: 1 / (1 + Math.exp(-(-3 + 0.1 * x[0]! + 0.4 * x[1]!))),
			});
		}
	}
	const [a, regular, playoff] = fitLogistic(rows, [0, 0.15, 0.15]);
	assert.closeTo(a!, -3, 0.05);
	assert.closeTo(regular!, 0.1, 0.01);
	assert.closeTo(playoff!, 0.4, 0.01);
});
