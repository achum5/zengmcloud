import { assert, describe, test } from "vitest";
import {
	sameAcos,
	sameAsin,
	sameAtan,
	sameAtan2,
	sameCos,
	sameEverywhere,
	sameExp,
	sameHypot,
	sameLog,
	sameLog2,
	samePow,
	sameSin,
	sameTan,
} from "./sameMath.ts";

// The same game on every device (see sameMath): its sums are its own - to
// the bit the same in any browser - and to the bit node's own (V8's, as
// node builds it): the tests' games are staged as they always were.
const same = (got: number, want: number, what: string) => {
	assert.isTrue(Object.is(got, want), `${what}: ${got} vs ${want}`);
};

describe("the same sums everywhere", () => {
	test("to the bit node's own", () => {
		let seed = 12345;
		const rand = () => {
			seed = (Math.imul(seed, 1103515245) + 12345) >>> 0;
			return seed / 2 ** 32;
		};
		// (Small, middling, a long way round - a stride's phase over a whole
		// game - and tiny.)
		const any = () => {
			const r = rand();
			const k =
				r < 0.3 ? 2 : r < 0.6 ? 20 : r < 0.8 ? 2000 : r < 0.9 ? 1e7 : 1e-6;
			return (rand() - 0.5) * k;
		};
		for (let i = 0; i < 50_000; i++) {
			const a = any();
			const b = any();
			const u = rand() * 2 - 1;
			const p = Math.abs(any());
			const e = (rand() - 0.5) * 60;
			same(sameSin(a), Math.sin(a), `sin ${a}`);
			same(sameCos(a), Math.cos(a), `cos ${a}`);
			same(sameTan(a), Math.tan(a), `tan ${a}`);
			same(sameAtan(b), Math.atan(b), `atan ${b}`);
			same(sameAtan2(a, b), Math.atan2(a, b), `atan2 ${a} ${b}`);
			same(sameAsin(u), Math.asin(u), `asin ${u}`);
			same(sameAcos(u), Math.acos(u), `acos ${u}`);
			same(sameExp(e), Math.exp(e), `exp ${e}`);
			same(sameLog(p), Math.log(p), `log ${p}`);
			same(sameLog2(p), Math.log2(p), `log2 ${p}`);
			const y = i % 2 ? Math.floor(rand() * 9) - 4 : (rand() - 0.5) * 6;
			same(samePow(a, y), Math.pow(a, y), `pow ${a} ${y}`);
			same(samePow(p, y), Math.pow(p, y), `pow ${p} ${y}`);
			same(sameHypot(a, b), Math.hypot(a, b), `hypot ${a} ${b}`);
			same(sameHypot(a, b, u), Math.hypot(a, b, u), `hypot ${a} ${b} ${u}`);
		}
		// The edges.
		const edges = [0, -0, 1, -1, 0.5, -0.5, 2, Math.PI, -Math.PI, Math.PI / 2];
		edges.push(1e-300, 5e-324, Infinity, -Infinity, Number.NaN, 1e8, 1e300);
		for (const x of edges) {
			same(sameSin(x), Math.sin(x), `sin ${x}`);
			same(sameCos(x), Math.cos(x), `cos ${x}`);
			same(sameTan(x), Math.tan(x), `tan ${x}`);
			same(sameAtan(x), Math.atan(x), `atan ${x}`);
			same(sameAsin(x), Math.asin(x), `asin ${x}`);
			same(sameAcos(x), Math.acos(x), `acos ${x}`);
			same(sameExp(x), Math.exp(x), `exp ${x}`);
			same(sameLog(x), Math.log(x), `log ${x}`);
			same(sameLog2(x), Math.log2(x), `log2 ${x}`);
			same(sameHypot(x), Math.hypot(x), `hypot ${x}`);
			for (const y of edges) {
				same(sameAtan2(x, y), Math.atan2(x, y), `atan2 ${x} ${y}`);
				same(samePow(x, y), Math.pow(x, y), `pow ${x} ${y}`);
				same(sameHypot(x, y), Math.hypot(x, y), `hypot ${x} ${y}`);
			}
		}
		same(sameHypot(), Math.hypot(), "hypot");
	});

	test("the browser's own back once a game is staged - even one that throws", () => {
		const sin = Math.sin;
		const hypot = Math.hypot;
		let inside: typeof Math.sin | undefined;
		sameEverywhere(() => {
			inside = Math.sin;
			// (Within: one inside another changes nothing.)
			sameEverywhere(() => assert.strictEqual(Math.sin, sameSin));
			assert.strictEqual(Math.sin, sameSin);
		});
		assert.strictEqual(inside, sameSin);
		assert.strictEqual(Math.sin, sin);
		assert.throws(() =>
			sameEverywhere(() => {
				throw new Error("no");
			}),
		);
		assert.strictEqual(Math.sin, sin);
		assert.strictEqual(Math.hypot, hypot);
	});
});
