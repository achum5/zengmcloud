import { assert, describe, test } from "vitest";
import { bugShown, writeBug, type BugNow } from "./bugWriter.ts";
import type { BugRefs } from "./ScoreBug.tsx";
import { SHOT_LOW_INK } from "./scoreBug.ts";

// An element as the writer sees one: its text, its style, its data-.
const el = (dataset: Record<string, string> = {}) =>
	({ textContent: "", style: {}, dataset }) as unknown as HTMLSpanElement & {
		style: Record<string, string>;
	};

const refsOf = (els: {
	clock?: ReturnType<typeof el>;
	shot?: ReturnType<typeof el>;
	fouls?: [ReturnType<typeof el>, ReturnType<typeof el>];
	bonus?: [ReturnType<typeof el>, ReturnType<typeof el>];
}): BugRefs => ({
	clock: { current: els.clock ?? null },
	shot: { current: els.shot ?? null },
	fouls: [
		{ current: els.fouls?.[0] ?? null },
		{ current: els.fouls?.[1] ?? null },
	],
	ball: [{ current: null }, { current: null }],
	bonus: [
		{ current: els.bonus?.[0] ?? null },
		{ current: els.bonus?.[1] ?? null },
	],
});

const now = (o: Partial<BugNow>): BugNow => ({
	clock: "7:28",
	shot: "14",
	low: false,
	ball: 0,
	...o,
});

describe("writeBug, for a league's own bug", () => {
	test("the clocks the broadcasts' way: :09.2 and :24, tenths as they are", () => {
		const clock = el({ colon: "" });
		const shot = el({ colon: "shot" });
		const refs = refsOf({ clock, shot });
		const shown = bugShown();
		writeBug(refs, shown, now({ clock: "9.2", shot: "24" }));
		assert.strictEqual(clock.textContent, ":09.2");
		assert.strictEqual(shot.textContent, ":24");
		writeBug(refs, shown, now({ clock: "11:58", shot: "4.1", low: true }));
		assert.strictEqual(clock.textContent, "11:58");
		assert.strictEqual(shot.textContent, "4.1");
		// (And a plain one, as it comes.)
		const plain = el();
		writeBug(refsOf({ clock: plain }), bugShown(), now({ clock: "9.2" }));
		assert.strictEqual(plain.textContent, "9.2");
	});

	test("the shot clock's own look in its last seconds, and back", () => {
		const shot = el({
			low: "#fff",
			lowBackground: "#d3202f",
			background: "#111",
		});
		const refs = refsOf({ shot });
		const shown = bugShown();
		writeBug(refs, shown, now({ shot: "4.1", low: true }));
		assert.strictEqual(shot.style.color, "#fff");
		assert.strictEqual(shot.style.background, "#d3202f");
		writeBug(refs, shown, now({ shot: "24", low: false }));
		assert.strictEqual(shot.style.background, "#111");
		// (A plain one goes red.)
		const plain = el();
		writeBug(
			refsOf({ shot: plain }),
			bugShown(),
			now({ shot: "3.0", low: true }),
		);
		assert.strictEqual(plain.style.color, SHOT_LOW_INK);
	});

	test("a bug's own last seconds: from 10 on its say", () => {
		const shot = el({
			lowBackground: "#a3291f",
			background: "transparent",
			lowAt: "10",
		});
		const refs = refsOf({ shot });
		const shown = bugShown();
		writeBug(refs, shown, now({ shot: "7", low: false }));
		assert.strictEqual(shot.style.background, "#a3291f");
		writeBug(refs, shown, now({ shot: "10", low: false }));
		assert.strictEqual(shot.style.background, "transparent");
	});

	test("fouls its way - FOUL LIMIT once the other side is in the bonus - and the bonus marks", () => {
		const fouls: [ReturnType<typeof el>, ReturnType<typeof el>] = [
			el({ fmt: "FOULS: {n}", limit: "FOUL LIMIT" }),
			el({ fmt: "FOULS: {n}", limit: "FOUL LIMIT" }),
		];
		const bonus: [ReturnType<typeof el>, ReturnType<typeof el>] = [el(), el()];
		const refs = refsOf({ fouls, bonus });
		const shown = bugShown();
		writeBug(
			refs,
			shown,
			now({
				fouls: ["FOULS 5", "BONUS"],
				counts: [5, 2],
				bonus: [false, true],
			}),
		);
		assert.strictEqual(fouls[0].textContent, "FOUL LIMIT");
		assert.strictEqual(fouls[1].textContent, "FOULS: 2");
		assert.strictEqual(bonus[0].style.visibility, undefined);
		assert.strictEqual(bonus[1].style.visibility, "visible");
		writeBug(
			refs,
			shown,
			now({
				fouls: ["FOULS 0", "FOULS 0"],
				counts: [0, 0],
				bonus: [false, false],
			}),
		);
		assert.strictEqual(fouls[0].textContent, "FOULS: 0");
		assert.strictEqual(bonus[1].style.visibility, "hidden");
	});

	test("fouls moved over for the bonus mark, and back", () => {
		const fouls: [ReturnType<typeof el>, ReturnType<typeof el>] = [
			el({ fmt: "FOULS: {n}", bonusShift: "-11cqw" }),
			el({ fmt: "FOULS: {n}" }),
		];
		const bonus: [ReturnType<typeof el>, ReturnType<typeof el>] = [el(), el()];
		const refs = refsOf({ fouls, bonus });
		const shown = bugShown();
		const write = (b: [boolean, boolean]) =>
			writeBug(refs, shown, now({ fouls: ["", ""], counts: [2, 2], bonus: b }));
		write([true, true]);
		assert.strictEqual(fouls[0].style.transform, "translateX(-11cqw)");
		assert.strictEqual(fouls[1].style.transform, undefined);
		write([false, true]);
		assert.strictEqual(fouls[0].style.transform, "");
	});

	test("the default bug's fouls as they come", () => {
		const fouls: [ReturnType<typeof el>, ReturnType<typeof el>] = [el(), el()];
		writeBug(
			refsOf({ fouls }),
			bugShown(),
			now({
				fouls: ["FOULS 3", "BONUS"],
				counts: [3, 5],
				bonus: [true, false],
			}),
		);
		assert.strictEqual(fouls[0].textContent, "FOULS 3");
		assert.strictEqual(fouls[1].textContent, "BONUS");
	});

	test("pieces shown only before or after the bonus, either side's or one's", () => {
		const piece = (when: string) => el({ when });
		const pieces = {
			noBonus: piece("noBonus"),
			bonus: piece("bonus"),
			awayBonus: piece("awayBonus"),
			homeBonus: piece("homeBonus"),
		};
		const bug = {
			querySelectorAll: () => Object.values(pieces),
		} as unknown as HTMLDivElement;
		const refs = { ...refsOf({}), bug: { current: bug } };
		const shown = bugShown();
		const seen = () =>
			Object.entries(pieces)
				.filter(([, p]) => p.style.visibility !== "hidden")
				.map(([k]) => k);
		writeBug(refs, shown, now({ bonus: [false, false] }));
		assert.deepStrictEqual(seen(), ["noBonus"]);
		writeBug(refs, shown, now({ bonus: [false, true] }));
		assert.deepStrictEqual(seen(), ["bonus", "homeBonus"]);
		writeBug(refs, shown, now({ bonus: [true, true] }));
		assert.deepStrictEqual(seen(), ["bonus", "awayBonus", "homeBonus"]);
		// A new period: back to before.
		writeBug(refs, shown, now({}));
		assert.deepStrictEqual(seen(), ["noBonus"]);
	});
});
