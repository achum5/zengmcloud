import { assert, describe, test } from "vitest";
import { BOWL_MATERIALS, parseBowl, type Bowl } from "./bowlData.ts";
import {
	fanLooks,
	type FanPool,
	seatCrowd,
	shareAt,
	takenAt,
} from "./bowlFans.ts";

const HOME = {
	abbrev: "CLE",
	colors: ["#860038", "#fdbb30", "#041e42"] as [string, string, string],
};
const AWAY = {
	abbrev: "SAC",
	colors: ["#5a2d81", "#63727a", "#000000"] as [string, string, string],
};

// The bowl as bowl.py exports it.
const importNode = (name: string) => import(/* @vite-ignore */ name);
let made: Bowl | undefined;
const bowl = async (): Promise<Bowl> => {
	if (!made) {
		const fs = await importNode("node:fs");
		const buf: Uint8Array = fs.readFileSync("public/img/arena/bowl.bin");
		made = parseBowl(
			buf.buffer.slice(
				buf.byteOffset,
				buf.byteOffset + buf.byteLength,
			) as ArrayBuffer,
		);
	}
	return made;
};
const pool = (n: number): FanPool =>
	({ atlas: undefined as never, n, cols: 32, rows: 1 }) as FanPool;

describe("the bowl built in Blender", () => {
	test("is all there: its shell, see-through glass, seats round the far side and both ends", async () => {
		const b = await bowl();
		assert.isAbove(b.points, 10_000);
		assert.isAbove(b.opaque, 10_000);
		assert.isAbove(b.clear, 0);
		assert.strictEqual((b.opaque + b.clear) % 3, 0);
		for (const i of b.index) {
			assert.isBelow(i, b.points);
		}
		for (let p = 0; p < b.points; p++) {
			assert.isBelow(b.lightMat[p * 4 + 3]!, BOWL_MATERIALS.length);
		}
		assert.strictEqual(b.anchors.x0, -13);
		assert.strictEqual(b.anchors.y1, 62);
		assert.isAbove(b.anchors.ribbon1Top, b.anchors.ribbon1Bottom);
		assert.isAbove(b.seats, 5000);
		// (None on the near side: no camera looks at it.)
		for (let j = 0; j < b.seats; j++) {
			assert.isAtMost(b.seat[j * 4 + 1]! * b.unit, 62.5);
		}
	});
});

describe("the crowd in its seats", () => {
	test("the same people every time, a good third of them in the home colors", () => {
		const a = fanLooks(HOME, AWAY, "fans-a", undefined, false);
		assert.deepEqual(
			fanLooks(HOME, AWAY, "fans-a", undefined, false).map((l) => l.color),
			a.map((l) => l.color),
		);
		const home = a.filter((l) => l.homeFan).length / a.length;
		assert.isAbove(home, 0.25);
		assert.isBelow(home, 0.5);
	});

	test("a playoff color-out: everybody in the home colors, a section in each", async () => {
		const looks = fanLooks(HOME, AWAY, "fans-b", undefined, true);
		assert.isTrue(looks.every((l) => l.homeFan));
		const b = await bowl();
		const c = seatCrowd(b, looks, pool(looks.length), "fans-b", true);
		const half = looks.length / 2;
		// (Along the far side, sections 26 ft wide from the corner.)
		const colorOf = new Map<number, Set<number>>();
		for (let j = 0; j < b.seats; j++) {
			if ((c.seat[j * 4 + 3]! & 3) !== 0) {
				continue;
			}
			const section = Math.floor((b.seat[j * 4]! * b.unit + 13) / 26);
			const set = colorOf.get(section) ?? new Set();
			set.add(c.seat[j * 4]! < half ? 0 : 1);
			colorOf.set(section, set);
		}
		for (const [section, set] of colorOf) {
			assert.strictEqual(set.size, 1, `section ${section}`);
		}
	});

	test("behind each basket its own stands; most home fans get up, under half the rest", async () => {
		const looks = fanLooks(HOME, AWAY, "fans-c", undefined, false);
		const b = await bowl();
		const c = seatCrowd(b, looks, pool(looks.length), "fans-c", false);
		const ends = [0, 0, 0];
		const up = { home: [0, 0], rest: [0, 0] };
		for (let j = 0; j < b.seats; j++) {
			const stands = c.seat[j * 4 + 3]! & 3;
			ends[stands]! += 1;
			const x = b.seat[j * 4]! * b.unit;
			if (stands === 1) {
				assert.isBelow(x, 0);
			} else if (stands === 2) {
				assert.isAbove(x, 94);
			}
			const look = looks[c.seat[j * 4]!]!;
			const tally = look.homeFan ? up.home : up.rest;
			tally[0]! += 1;
			tally[1]! += c.seat[j * 4 + 2]! < 65535 ? 1 : 0;
		}
		assert.isTrue(
			ends.every((n) => n > 500),
			`${ends}`,
		);
		assert.isAbove(up.home[1]! / up.home[0]!, 0.8);
		assert.isBelow(up.rest[1]! / up.rest[0]!, 0.55);
	});

	test("as many seats taken as people came - fewer as they leave", async () => {
		const looks = fanLooks(HOME, AWAY, "fans-d", undefined, false);
		const b = await bowl();
		const c = seatCrowd(b, looks, pool(looks.length), "fans-d", false, {
			att: 9000,
			capacity: 18000,
		});
		assert.closeTo(c.fill, 0.5, 1e-9);
		const sat = (share: number) => {
			const taken = takenAt(c, share);
			let n = 0;
			for (let j = 0; j < b.seats; j++) {
				n += c.seat[j * 4 + 1]! <= taken ? 1 : 0;
			}
			return n / b.seats;
		};
		assert.closeTo(sat(shareAt(c, 1)), 0.5, 0.01);
		assert.closeTo(sat(shareAt(c, 0.55)), 0.25, 0.01);
		assert.strictEqual(sat(0), 0);
		assert.strictEqual(sat(1), 1);
		// The best seats - low, near half court - taken first.
		const taken = takenAt(c, 0.2);
		let low = 0;
		let lowSat = 0;
		for (let j = 0; j < b.seats; j++) {
			if (b.seat[j * 4 + 2]! * b.unit < 15) {
				low++;
				lowSat += c.seat[j * 4 + 1]! <= taken ? 1 : 0;
			}
		}
		assert.isAbove(lowSat / low, 0.4);
	});
});
