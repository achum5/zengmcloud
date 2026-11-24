import { assert, describe, test } from "vitest";
import { realTeamLooks } from "./realTeamLooks.ts";
import { realTeamInfoSchema } from "./types.ts";

// A team's real look, season by season, from Real Team Info: checked when
// the file is loaded, and each part as last listed - or, a new season's,
// only what is listed for it.

const PNG = `data:image/png;base64,${"A".repeat(40)}=`;
const JPEG = `data:image/jpeg;base64,${"B".repeat(40)}`;
const BUG = {
	width: 600,
	height: 60,
	pieces: [{ show: "clock", x: 0, y: 0, w: 60, h: 50 }],
};

const parse = (raw: unknown) => realTeamInfoSchema.safeParse(raw);

describe("Real Team Info looks", () => {
	test("jerseys, a court and a score bug are read in, by season", () => {
		const result = parse({
			BOS: {
				seasons: {
					1947: {
						jerseySkins: { home: PNG, away: "https://example.com/a.png" },
						court: { floorImageURL: JPEG, floorLines: "picture" },
					},
					1996: { scoreBug: BUG },
				},
			},
		});
		assert.ok(result.success, JSON.stringify(result.error?.issues));
		const looks = realTeamLooks(result.data, "BOS", 2000);
		assert.deepStrictEqual(looks?.jerseySkins, {
			home: PNG,
			away: "https://example.com/a.png",
		});
		assert.strictEqual(looks?.court?.floorImageURL, JPEG);
		assert.strictEqual(looks?.scoreBug?.width, 600);
	});

	test("a picture must be a link or written in", () => {
		for (const bad of [
			{ jerseySkins: { home: "pic:abc" } },
			{ jerseySkins: { home: JPEG } }, // a jersey's is a PNG
			{ court: { floorImageURL: "pic:abc" } },
			{ court: { logoURL: "/img/logo.png" } },
			{ court: { floor: 7 } },
			{ court: { madeUp: "#fff" } },
			{ scoreBug: { ...BUG, image: "pic:abc" } },
			{ scoreBug: { pieces: [] } },
		]) {
			assert.notOk(
				parse({ BOS: { seasons: { 2000: bad } } }).success,
				JSON.stringify(bad),
			);
		}
	});

	test("each part as last listed; a new season, only its own", () => {
		const result = parse({
			BOS: {
				court: { floor: "#111111" },
				seasons: {
					1950: { jerseySkins: { home: PNG } },
					1970: { court: { floor: "#222222" } },
					1990: { name: "Celtics" },
				},
			},
		});
		assert.ok(result.success);
		const info = result.data;

		assert.deepStrictEqual(realTeamLooks(info, "BOS", 1940), {
			court: { floor: "#111111" },
		});
		assert.deepStrictEqual(realTeamLooks(info, "BOS", 1980), {
			court: { floor: "#222222" },
			jerseySkins: { home: PNG },
		});

		assert.deepStrictEqual(
			realTeamLooks(info, "BOS", 1970, { exactSeason: true }),
			{ court: { floor: "#222222" } },
		);
		assert.strictEqual(
			realTeamLooks(info, "BOS", 1971, { exactSeason: true }),
			undefined,
		);
		// A season listed for something else, not a look.
		assert.strictEqual(
			realTeamLooks(info, "BOS", 1990, { exactSeason: true }),
			undefined,
		);
		assert.strictEqual(realTeamLooks(info, "NYK", 1980), undefined);
		assert.strictEqual(realTeamLooks(info, undefined, 1980), undefined);
	});
});
