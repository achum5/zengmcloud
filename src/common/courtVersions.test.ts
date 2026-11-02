import { assert, describe, test } from "vitest";
import {
	pairedAdjust,
	parseVersions,
	versionOfGame,
	versionPictures,
	withVersionPictures,
} from "./courtVersions.ts";
import type { CourtStyle } from "./types.ts";

const court: CourtStyle = {
	versions: {
		playoffs: {
			pictures: [
				{ image: "https://example.com/playoffs.png", pair: "diagonal" },
			],
		},
		homeOpener: { pictures: [{ image: "pic:abc" }] },
	},
};

describe("court versions", () => {
	test("a game is played on the finals', the playoffs' or the home opener's version", () => {
		assert.strictEqual(
			versionOfGame({ playoffs: true, finals: true }),
			"finals",
		);
		assert.strictEqual(versionOfGame({ playoffs: true }), "playoffs");
		assert.strictEqual(versionOfGame({ homeOpener: true }), "homeOpener");
		assert.strictEqual(versionOfGame({}), undefined);
	});

	test("the finals, with no pictures of their own, have the playoffs'", () => {
		assert.deepStrictEqual(
			versionPictures(court, "finals"),
			court.versions!.playoffs!.pictures,
		);
		const own: CourtStyle = {
			versions: { ...court.versions, finals: { pictures: [] } },
		};
		assert.deepStrictEqual(versionPictures(own, "finals"), []);
		assert.deepStrictEqual(versionPictures(court, undefined), []);
		assert.deepStrictEqual(versionPictures(undefined, "playoffs"), []);
	});

	test("the other one of a pair: mirrored across center court, or round it the same way up", () => {
		const a = { dx: -38, dy: 15, rotate: 10, scale: 0.5 };
		assert.deepStrictEqual(pairedAdjust(a, true), {
			dx: 38,
			dy: 15,
			rotate: -10,
			scale: 0.5,
		});
		assert.deepStrictEqual(pairedAdjust(a, "diagonal"), {
			dx: 38,
			dy: -15,
			rotate: 10,
			scale: 0.5,
		});
		assert.deepStrictEqual(pairedAdjust(undefined, "diagonal"), {
			dx: -0,
			dy: -0,
		});
	});

	test("uploaded pictures filled in, one no longer there left out", () => {
		assert.deepStrictEqual(withVersionPictures(court.versions!, {}), {
			playoffs: court.versions!.playoffs,
			homeOpener: { pictures: [] },
		});
		assert.deepStrictEqual(
			withVersionPictures(court.versions!, { abc: "data:image/png;base64,AA" })
				.homeOpener,
			{ pictures: [{ image: "data:image/png;base64,AA" }] },
		);
	});

	test("a court file's versions are checked", () => {
		assert.deepStrictEqual(parseVersions(court.versions), court.versions);
		assert.deepStrictEqual(parseVersions({ finals: {} }), { finals: {} });
		assert.deepStrictEqual(
			parseVersions({
				playoffs: { pictures: [{ image: "https://x.png", pair: false }] },
			}),
			{ playoffs: { pictures: [{ image: "https://x.png" }] } },
		);
		for (const bad of [
			[],
			{ allStar: {} },
			{ playoffs: [] },
			{ playoffs: { logos: [] } },
			{ playoffs: { pictures: {} } },
			{ playoffs: { pictures: [{}] } },
			{ playoffs: { pictures: [{ image: "" }] } },
			{ playoffs: { pictures: [{ image: "https://x.png", pair: "both" }] } },
			{
				playoffs: {
					pictures: [{ image: "https://x.png", adjust: { dx: "far" } }],
				},
			},
			{ playoffs: { pictures: [{ image: "https://x.png", where: 1 }] } },
		]) {
			assert.strictEqual(
				typeof parseVersions(bad),
				"string",
				JSON.stringify(bad),
			);
		}
	});
});
