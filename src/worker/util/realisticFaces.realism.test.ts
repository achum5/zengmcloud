import { generate } from "facesjs";
import { assert, describe, test } from "vitest";
import { mulberry32 } from "../../common/sportsbookOdds.ts";
import {
	ageFace,
	applyRealisticFace,
	baldingOnsetAge,
	bandForAge,
	CLEANS_UP_FROM_AGE,
	CLEANS_UP_SHARE,
	CLEANS_UP_SPREAD,
	cleansUpAtAge,
	EARLY_FACIAL_HAIR_SHARE,
	facialHairStart,
	LATE_FACIAL_HAIR_FROM_AGE,
	CUTS_HAIR_FROM_AGE,
	CUTS_HAIR_SPREAD,
	cutsHair,
	cutsHairFromAge,
	facialHairCeiling,
	facialHairForAge,
	growFacialHair as growFacialHairForTest,
	fatnessForAthlete,
	FULL_BEARD_SHARE,
	growsFacialHair,
	hairAllowedForRace,
	HAIR_TEXTURES,
	HAIR_VOLUMINOUS,
	LIGHT_ONLY_SHARE,
	SHORT_CUTS_COILED,
	SHORT_CUTS_STRAIGHT,
	shortCutsFor,
	tierOf,
	TIER_ORDER_FOR_TESTS,
	WRINKLE_DELAY_MAX,
	wrinkleDelay,
} from "./realisticFaces.ts";

// HELD TO THE REAL LEAGUE. The numbers these bounds come from are the
// hand-picked faces of the league's active real players, each matched to his
// headshot - see the table on FACE_AGE_BANDS. Bounds are wide on purpose: the
// point is to catch a rate that doubled or went to zero, not to freeze one.

const RACES = ["black", "black", "white", "brown", "asian"] as const;

// Careers drafted at 20 and aged one season at a time, sampled at each age.
const replayLeague = (n: number, ages: readonly number[]) => {
	const at = new Map<number, { none: number; heavy: number }>(
		ages.map((age) => [age, { none: 0, heavy: 0 }]),
	);
	const realRandom = Math.random;
	try {
		for (let i = 0; i < n; i++) {
			const pid = i * 7 + 3;
			const race = RACES[i % RACES.length]!;
			// generate() rolls on the global, so seed it too - the same n men
			// every run, and a test that cannot flake on an unlucky draw.
			Math.random = mulberry32(pid);
			const face = generate(
				{ jersey: { id: "jersey" } },
				{ gender: "male", race },
			);
			const rand = mulberry32(pid * 31 + 1);
			applyRealisticFace(face, { age: 20, race, pid, rand });
			for (let age = 21; age <= Math.max(...ages); age++) {
				ageFace(face, age, pid, rand);
				const tally = at.get(age);
				if (tally) {
					if (face.facialHair.id === "none") {
						tally.none += 1;
					} else if (tierOf(face.facialHair.id) === "heavy") {
						tally.heavy += 1;
					}
				}
			}
		}
	} finally {
		Math.random = realRandom;
	}
	return (age: number) => {
		const tally = at.get(age)!;
		return { none: tally.none / n, heavy: tally.heavy / n };
	};
};

describe("facial hair, against the real league", () => {
	const league = replayLeague(2000, [22, 26, 30, 36]);

	// The old bands left two thirds of 24-year-olds clean-shaven and put the
	// first full beard in the league at 28. The real floor is half bearded by
	// 22 and four in five by 26.
	test("it arrives in the early twenties", () => {
		const at22 = league(22);
		const at26 = league(26);
		assert.isAbove(at22.none, 0.35, `${at22.none} clean-shaven at 22`);
		assert.isBelow(at22.none, 0.55, `${at22.none} clean-shaven at 22`);
		assert.isBelow(at26.none, 0.3, `${at26.none} clean-shaven at 26`);
	});

	test("a few already wear a full beard at 22; most come in the mid-twenties", () => {
		// Not every draft class is baby-faced - real 19-to-23-year-olds include
		// one in ten - but it is still a minority look that young.
		const at22 = league(22).heavy;
		assert.isAbove(at22, 0.03, `${at22} bearded at 22`);
		assert.isBelow(at22, 0.16, `${at22} bearded at 22`);
		const at26 = league(26).heavy;
		assert.isAbove(at26, 0.15, `${at26} bearded at 26`);
		assert.isBelow(at26, 0.4, `${at26} bearded at 26`);
	});

	// And it settles. Thickening used to run to the top tier for everyone, so
	// beards were past half of every 34-year-old's face and the goatee had all
	// but gone from the veterans.
	test("a veteran floor is mostly bearded, not all bearded", () => {
		for (const age of [30, 36]) {
			const { none, heavy } = league(age);
			assert.isAbove(none, 0.08, `${none} clean-shaven at ${age}`);
			assert.isBelow(none, 0.25, `${none} clean-shaven at ${age}`);
			assert.isAbove(heavy, 0.25, `${heavy} bearded at ${age}`);
			assert.isBelow(heavy, 0.5, `${heavy} bearded at ${age}`);
		}
	});

	// THE CONTRACT. A face built at an age has to look like one that aged
	// there, or a new league starts out looking different from the same league
	// played forward. Generation replays the same seasons now, so the two
	// should be the same population up to sampling.
	test("a face built at an age wears what one aged there would", () => {
		for (const age of [24, 30, 36]) {
			const aged = replayLeague(2000, [age])(age);
			let none = 0;
			let heavy = 0;
			const rand = mulberry32(age * 101);
			for (let i = 0; i < 2000; i++) {
				const id = facialHairForAge(age, rand, i * 7 + 3);
				if (id === "none") {
					none += 1;
				} else if (tierOf(id) === "heavy") {
					heavy += 1;
				}
			}
			assert.closeTo(none / 2000, aged.none, 0.05, `clean-shaven at ${age}`);
			assert.closeTo(heavy / 2000, aged.heavy, 0.05, `bearded at ${age}`);
		}
	});
});

describe("how far a man takes it", () => {
	const order = (tier: string | undefined) =>
		TIER_ORDER_FOR_TESTS.indexOf(tier as any);

	test("most grow a full beard, some stop at a goatee, and it is fixed", () => {
		const counts = { light: 0, medium: 0, heavy: 0 } as Record<string, number>;
		for (let pid = 0; pid < 4000; pid++) {
			const ceiling = facialHairCeiling(pid);
			assert.strictEqual(facialHairCeiling(pid), ceiling);
			counts[ceiling]! += 1;
		}
		assert.closeTo(counts.heavy! / 4000, FULL_BEARD_SHARE, 0.03);
		assert.closeTo(counts.light! / 4000, LIGHT_ONLY_SHARE, 0.03);
		assert.isAbove(counts.medium!, 0);
		// An unknown player is not held back.
		assert.strictEqual(facialHairCeiling(undefined), "heavy");
	});

	test("no season of aging takes him past it", () => {
		for (let pid = 0; pid < 300; pid++) {
			// The men who clean up end below it by design - see the next block.
			if (!growsFacialHair(pid) || cleansUpAtAge(pid) !== undefined) {
				continue;
			}
			const face: any = generate(
				{ jersey: { id: "jersey" } },
				{ gender: "male", race: "white" },
			);
			face.facialHair.id = "none";
			for (let age = 20; age <= 40; age++) {
				// The most generous roll there is, every season.
				ageFace(face, age, pid, () => 0);
			}
			const reached = tierOf(face.facialHair.id);
			assert.strictEqual(
				order(reached),
				order(facialHairCeiling(pid)),
				`pid ${pid} ended at ${face.facialHair.id}, ceiling ${facialHairCeiling(pid)}`,
			);
		}
	});

	test("a style already past it is kept, never trimmed back", () => {
		const pid = (() => {
			for (let p = 0; p < 500; p++) {
				if (
					facialHairCeiling(p) === "light" &&
					cleansUpAtAge(p) === undefined
				) {
					return p;
				}
			}
			throw new Error("no light-ceiling pid found");
		})();
		const face: any = generate(
			{ jersey: { id: "jersey" } },
			{ gender: "male", race: "black" },
		);
		face.facialHair.id = "beard2";
		for (let age = 25; age <= 38; age++) {
			ageFace(face, age, pid, () => 0);
		}
		assert.strictEqual(face.facialHair.id, "beard2");
	});

	// A man clean-shaven at 31 has mostly decided. A flat rate kept sprouting
	// goatees on settled veterans every few seasons - in a league of
	// hand-picked faces, a change to someone's real photo each time.
	test("a clean-shaven veteran mostly stays that way", () => {
		assert.isBelow(bandForAge(31).growPerYear, bandForAge(23).growPerYear);
		let grew = 0;
		let growers = 0;
		const rand = mulberry32(5);
		for (let pid = 0; pid < 2000; pid++) {
			if (!growsFacialHair(pid)) {
				continue;
			}
			growers += 1;
			const face: any = generate(
				{ jersey: { id: "jersey" } },
				{ gender: "male", race: "black" },
			);
			face.facialHair.id = "none";
			for (let age = 32; age <= 36; age++) {
				ageFace(face, age, pid, rand);
			}
			if (face.facialHair.id !== "none") {
				grew += 1;
			}
		}
		assert.isBelow(grew / growers, 0.3, `${grew}/${growers} in five seasons`);
	});
});

describe("a cut keeps its texture", () => {
	const straight = new Set<string>(HAIR_TEXTURES.straight);

	test("straight hair is cut to something straight hair can be", () => {
		for (const id of SHORT_CUTS_STRAIGHT) {
			assert.isTrue(hairAllowedForRace(id, "white"), id);
			assert.isTrue(hairAllowedForRace(id, "asian"), id);
			assert.notInclude(HAIR_VOLUMINOUS, id, id);
		}
		// The coiled crop and the buzz, which hand-picking real faces kept
		// having to take back off white players.
		assert.notInclude(SHORT_CUTS_STRAIGHT, "short3");
		assert.notInclude(SHORT_CUTS_STRAIGHT, "short");
	});

	test("coiled hair is cut to something coiled hair can be", () => {
		for (const id of SHORT_CUTS_COILED) {
			assert.isTrue(hairAllowedForRace(id, "black"), id);
			assert.notInclude(HAIR_VOLUMINOUS, id, id);
		}
	});

	test("every voluminous style cuts by its own texture", () => {
		for (const id of HAIR_VOLUMINOUS) {
			assert.strictEqual(
				shortCutsFor(id),
				straight.has(id) ? SHORT_CUTS_STRAIGHT : SHORT_CUTS_COILED,
				id,
			);
		}
	});

	test("and the cut a career makes is one of them", () => {
		const cutter = (() => {
			for (let pid = 0; pid < 500; pid++) {
				if (cutsHair(pid)) {
					return pid;
				}
			}
			throw new Error("no cutter found");
		})();
		for (const id of ["messy", "shaggy1", "hair", "afro", "dreads"]) {
			const face: any = generate(
				{ jersey: { id: "jersey" } },
				{ gender: "male", race: straight.has(id) ? "white" : "black" },
			);
			face.hair.id = id;
			ageFace(face, cutsHairFromAge(cutter), cutter, () => 0.001);
			assert.include(
				straight.has(id) ? SHORT_CUTS_STRAIGHT : SHORT_CUTS_COILED,
				face.hair.id,
				`${id} cut to ${face.hair.id}`,
			);
		}
	});
});

// Two traits used to read the same hash, which ties them together whatever
// either one's own comment says.
describe("traits that are meant to be independent are", () => {
	test("the league's cuts span the whole spread, not its bottom half", () => {
		const ages = new Set<number>();
		for (let pid = 0; pid < 4000; pid++) {
			if (cutsHair(pid)) {
				ages.add(cutsHairFromAge(pid));
			}
		}
		for (let a = 0; a <= CUTS_HAIR_SPREAD; a++) {
			assert.isTrue(ages.has(CUTS_HAIR_FROM_AGE + a), `nobody cuts at ${a}`);
		}
	});

	test("an early balder is not always an early wrinkler", () => {
		const delays = new Set<number>();
		for (let pid = 0; pid < 4000; pid++) {
			if (baldingOnsetAge(pid) === baldingOnsetAge(undefined)) {
				delays.add(wrinkleDelay(pid));
			}
		}
		assert.strictEqual(delays.size, WRINKLE_DELAY_MAX + 1);
	});
});

describe("athletes", () => {
	// Rendered, a face reads as an athlete's up to about 0.3 and has visibly
	// rounded by 0.5. Hand-picking the real league put almost everyone between
	// 0.15 and 0.3.
	test("are drawn lean, with a heavy tail", () => {
		const rand = mulberry32(9);
		let sum = 0;
		let heavy = 0;
		let max = 0;
		const n = 20000;
		for (let i = 0; i < n; i++) {
			const f = fatnessForAthlete(rand);
			sum += f;
			max = Math.max(max, f);
			if (f > 0.5) {
				heavy += 1;
			}
		}
		assert.isAbove(sum / n, 0.18);
		assert.isBelow(sum / n, 0.28);
		assert.isAbove(heavy / n, 0.08);
		assert.isBelow(heavy / n, 0.2);
		assert.isAtMost(max, 0.7);
	});
});

// EVERY MAN ON HIS OWN SCHEDULE - see facialHairStart and cleansUpAtAge. The
// report behind it: aging read as the same thing for everyone, a baby-faced
// rookie filling into a beard.
describe("not everyone wears it the same way", () => {
	// What each man wore every season from `from` to `to`.
	const careers = (n: number, from: number, to: number) => {
		const out: { pid: number; ids: Map<number, string> }[] = [];
		for (let i = 0; i < n; i++) {
			const pid = i * 7 + 3;
			const rand = mulberry32(pid * 13 + 5);
			let id = facialHairForAge(from, rand, pid);
			const ids = new Map<number, string>([[from, id]]);
			for (let age = from + 1; age <= to; age++) {
				id = growFacialHairForTest(id, age, pid, rand);
				ids.set(age, id);
			}
			out.push({ pid, ids });
		}
		return out;
	};

	test("the traits are fixed per player and come in roughly their shares", () => {
		let early = 0;
		let cleans = 0;
		let growers = 0;
		const N = 4000;
		for (let pid = 0; pid < N; pid++) {
			assert.strictEqual(facialHairStart(pid), facialHairStart(pid));
			assert.strictEqual(cleansUpAtAge(pid), cleansUpAtAge(pid));
			if (facialHairStart(pid) === "early") {
				early += 1;
			}
			if (growsFacialHair(pid)) {
				growers += 1;
				const at = cleansUpAtAge(pid);
				if (at !== undefined) {
					cleans += 1;
					assert.isAtLeast(at, CLEANS_UP_FROM_AGE);
					assert.isAtMost(at, CLEANS_UP_FROM_AGE + CLEANS_UP_SPREAD);
				}
			}
		}
		assert.closeTo(early / N, EARLY_FACIAL_HAIR_SHARE, 0.03);
		assert.closeTo(cleans / growers, CLEANS_UP_SHARE, 0.03);
		// A man who never grows any has nothing to clean up.
		for (let pid = 0; pid < N; pid++) {
			if (!growsFacialHair(pid)) {
				assert.isUndefined(cleansUpAtAge(pid));
			}
		}
	});

	test("an early starter is already wearing it at the draft", () => {
		const rand = mulberry32(31);
		for (let pid = 0; pid < 3000; pid++) {
			if (facialHairStart(pid) === "early" && growsFacialHair(pid)) {
				assert.notStrictEqual(facialHairForAge(19, rand, pid), "none");
			}
		}
	});

	test("a late starter is clean until his mid-twenties", () => {
		for (const { pid, ids } of careers(3000, 19, 30)) {
			if (facialHairStart(pid) === "late") {
				for (let age = 19; age < LATE_FACIAL_HAIR_FROM_AGE; age++) {
					assert.strictEqual(ids.get(age), "none", `pid ${pid} at ${age}`);
				}
			}
		}
	});

	test("a man who cleans up does it once, at his own age, and keeps it", () => {
		let seen = 0;
		for (const { pid, ids } of careers(3000, 19, 40)) {
			const at = cleansUpAtAge(pid);
			if (at === undefined) {
				continue;
			}
			const before = ids.get(at - 1)!;
			const after = ids.get(at)!;
			if (before !== "none") {
				seen += 1;
				// Shaved clean or trimmed back to something light.
				assert.isTrue(
					after === "none" || tierOf(after) === "light",
					`pid ${pid}: ${before} -> ${after}`,
				);
				if (tierOf(before) === "light") {
					assert.strictEqual(after, "none");
				}
			}
			// What he chose is what he wears for the rest of his career.
			for (let age = at; age <= 40; age++) {
				assert.strictEqual(ids.get(age), after, `pid ${pid} at ${age}`);
			}
		}
		assert.isAbove(seen, 0);
	});

	test("at thirty, a league has every look in it", () => {
		const league30 = replayLeague(2000, [30])(30);
		assert.isAbove(league30.none, 0.12, "clean-shaven");
		assert.isAbove(league30.heavy, 0.3, "full beards");
		assert.isAbove(
			1 - league30.none - league30.heavy,
			0.25,
			"goatees and the like",
		);
	});
});
