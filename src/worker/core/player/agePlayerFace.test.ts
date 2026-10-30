import { assert, describe, test, vi } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { generateFace } from "../../util/face.ts";
import { agePlayerFace, fixBrownHairOnBlackPlayer } from "./agePlayerFace.ts";
import {
	appearanceForSeason,
	latestAppearanceSeason,
} from "../../../common/playerAppearance.ts";
import { PLAYER } from "../../../common/constants.ts";
import { mulberry32 } from "../../../common/sportsbookOdds.ts";
import { inferRaceFromFace } from "../../util/realisticFaces.ts";

const DRAFT = 2020;

const makePlayer = (pid: number, race: "black" | "white" = "black") => {
	const spy = vi.spyOn(Math, "random").mockImplementation(mulberry32(pid));
	try {
		return {
			pid,
			tid: 3,
			born: { year: DRAFT - 20, loc: "" },
			draft: { year: DRAFT },
			face: generateFace({ race, age: 20, pid }),
			imgURL: "",
			ratings: [{ season: DRAFT }],
			relatives: [],
		} as any;
	} finally {
		spy.mockRestore();
	}
};

const setUp = () => {
	resetG();
	g.setWithoutSavingToDB("realisticFaces", true);
	g.setWithoutSavingToDB("faceAging", true);
	g.setWithoutSavingToDB("faceAgingPlayers", "all");
};

// Twenty preseasons, the way newPhasePreseason runs them: a ratings row for
// the new season, then the face. Returns the face he wore each season.
const playCareer = (p: any, lastSeason: number) => {
	const worn = new Map<number, unknown>([[DRAFT, structuredClone(p.face)]]);
	const spy = vi
		.spyOn(Math, "random")
		.mockImplementation(mulberry32(p.pid + 9));
	try {
		for (let season = DRAFT + 1; season <= lastSeason; season++) {
			p.ratings.push({ season });
			agePlayerFace(p, season);
			worn.set(season, structuredClone(p.face));
		}
	} finally {
		spy.mockRestore();
	}
	return worn;
};

describe("one face per season", () => {
	test("every season of a career shows the face he wore that season", () => {
		setUp();
		for (let pid = 1; pid <= 40; pid++) {
			const p = makePlayer(pid);
			const worn = playCareer(p, DRAFT + 19);
			const latest = latestAppearanceSeason(p.ratings);
			for (const [season, face] of worn) {
				assert.deepStrictEqual(
					appearanceForSeason(p, season, latest).face,
					face,
					`pid ${pid}, ${season}`,
				);
			}
		}
	});

	test("the history stays small - changes, not copies", () => {
		setUp();
		let bytes = 0;
		let seasons = 0;
		for (let pid = 1; pid <= 40; pid++) {
			const p = makePlayer(pid);
			playCareer(p, DRAFT + 19);
			const [base, ...rest] = p.appearances;
			assert.isDefined(base.face);
			for (const entry of rest) {
				assert.isUndefined(entry.face, "after the first, only changes");
				bytes += JSON.stringify(entry).length;
				seasons += 1;
			}
		}
		assert.isBelow(bytes / seasons, 200, "bytes per season");
	});

	test("a retired player is never aged again", () => {
		setUp();
		const p = makePlayer(7);
		playCareer(p, DRAFT + 12);
		p.tid = PLAYER.RETIRED;
		const face = structuredClone(p.face);
		const appearances = structuredClone(p.appearances);
		for (let season = DRAFT + 13; season <= DRAFT + 30; season++) {
			assert.isFalse(agePlayerFace(p, season));
		}
		assert.deepStrictEqual(p.face, face);
		assert.deepStrictEqual(p.appearances, appearances);
		// And his last season is still the face he retired with.
		assert.deepStrictEqual(
			appearanceForSeason(p, DRAFT + 12, latestAppearanceSeason(p.ratings))
				.face,
			face,
		);
	});

	test("with aging off, nothing is aged or recorded", () => {
		setUp();
		g.setWithoutSavingToDB("faceAging", false);
		const p = makePlayer(8);
		const face = structuredClone(p.face);
		playCareer(p, DRAFT + 10);
		assert.deepStrictEqual(p.face, face);
		assert.isUndefined(p.appearances);
	});

	test("an edit made during a season is that season's face", () => {
		// The editor writes p.face; the next preseason has to bank exactly that
		// as the season that just ended.
		setUp();
		const p = makePlayer(9);
		playCareer(p, DRAFT + 3);
		p.face.hair.id = "dreads";
		const edited = structuredClone(p.face);
		p.ratings.push({ season: DRAFT + 4 });
		agePlayerFace(p, DRAFT + 4);
		assert.deepStrictEqual(
			appearanceForSeason(p, DRAFT + 3, latestAppearanceSeason(p.ratings)).face,
			edited,
		);
	});
});

describe("fixBrownHairOnBlackPlayer", () => {
	test("the old brown goes black, in every season of the history", () => {
		setUp();
		const p = makePlayer(11);
		playCareer(p, DRAFT + 4);
		assert.strictEqual(inferRaceFromFace(p.face), "black");
		p.face.hair.color = "#3f2d22";
		p.appearances[0].face.hair.color = "#3b2a20";
		assert.isTrue(fixBrownHairOnBlackPlayer(p));
		assert.notStrictEqual(p.face.hair.color, "#3f2d22");
		assert.notStrictEqual(p.appearances[0].face.hair.color, "#3b2a20");
		// Once fixed, there is nothing left to fix.
		assert.isFalse(fixBrownHairOnBlackPlayer(p));
	});

	test("near-black hair, a dyed head and a real player are left alone", () => {
		setUp();
		const p = makePlayer(12);
		for (const color of ["#272421", "#1a1613", "#e9c67b", "#b55239"]) {
			p.face.hair.color = color;
			assert.isFalse(fixBrownHairOnBlackPlayer(p), color);
			assert.strictEqual(p.face.hair.color, color);
		}
		p.face.hair.color = "#3f2d22";
		p.srID = "realpl01";
		assert.isFalse(fixBrownHairOnBlackPlayer(p));
	});

	test("a white player's brown hair is his own", () => {
		setUp();
		const p = makePlayer(13, "white");
		p.face.hair.color = "#5a3825";
		assert.isFalse(fixBrownHairOnBlackPlayer(p));
	});
});
