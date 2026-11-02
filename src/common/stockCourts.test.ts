import { assert, describe, test } from "vitest";
import teamInfos from "./teamInfos.ts";
import {
	apart,
	contrast,
	courtOf,
	STOCK_COURT_TEAMS,
	stockCourt,
	stockCourtKey,
} from "./stockCourts.ts";

// The floor as the court draws it, feet (see LiveCourt): 94 by 50, the rims
// 5.25 ft in, the three-point arc 23.75 ft round them; the center logo's box
// 30 ft square at center court, the secondary logo's 13 ft square at 27% and
// 73% of the way along (the one in the far half moved the other way), the
// center-court text 3.4 ft tall, 6 ft in from the far sideline. And where a
// league's own pictures go when added (see EditTeamCourt's NEW_PICTURE): 11
// ft square, 38 ft from center court and 15 across, one each way round.
const W = 94;
const H = 50;
const RIM = 5.25;
const ARC = 23.75;
type Box = { x0: number; y0: number; x1: number; y1: number; what: string };
const square = (cx: number, cy: number, side: number, what: string): Box => ({
	x0: cx - side / 2,
	y0: cy - side / 2,
	x1: cx + side / 2,
	y1: cy + side / 2,
	what,
});
const overlap = (a: Box, b: Box) =>
	a.x0 < b.x1 && b.x0 < a.x1 && a.y0 < b.y1 && b.y0 < a.y1;
// The nearest a box comes to a point.
const nearest = (b: Box, x: number, y: number) =>
	Math.hypot(Math.max(b.x0 - x, 0, x - b.x1), Math.max(b.y0 - y, 0, y - b.y1));
const LEAGUE = [
	square(W / 2 - 38, H / 2 + 15, 11, "league picture"),
	square(W / 2 + 38, H / 2 - 15, 11, "league picture"),
];

const team = (abbrev: string) => {
	const info = teamInfos[abbrev]!;
	return {
		region: info.region,
		name: info.name,
		colors: info.colors,
		imgURL: `/img/logos-primary/${abbrev}.svg`,
		imgURLSmall: `/img/logos-secondary/${abbrev}.svg`,
	};
};

describe("stock courts", () => {
	test("every team the game comes with has one", () => {
		for (const abbrev of Object.keys(teamInfos)) {
			assert.isDefined(stockCourt(team(abbrev)), abbrev);
		}
		assert.strictEqual(STOCK_COURT_TEAMS.length, Object.keys(teamInfos).length);
	});

	test("a team with a logo of its own has none, and a court of its own is its own", () => {
		assert.isUndefined(stockCourt({ imgURL: "https://example.com/logo.png" }));
		assert.isUndefined(stockCourt({ imgURL: "/img/logos-primary/XYZ.svg" }));
		assert.isUndefined(stockCourt({}));
		assert.strictEqual(stockCourtKey(team("ABQ")), "ABQ");
		const own = { floor: "#123456" };
		assert.strictEqual(courtOf({ ...team("ABQ"), court: own }), own);
		assert.deepStrictEqual(courtOf(team("ABQ")), stockCourt(team("ABQ")));
	});

	test("it follows the team: its colors, its name", () => {
		const t = team("ABQ");
		const recolored = stockCourt({
			...t,
			colors: ["#00ff00", "#0000ff", "#ff0000"],
			region: "Santa Fe",
		})!;
		assert.strictEqual(recolored.paint, "#0000ff");
		assert.strictEqual(recolored.railText, "SANTA FE");
		// (No secondary logo, no wing logos.)
		const bare = stockCourt({ ...t, imgURLSmall: undefined })!;
		assert.isUndefined(bare.secondaryLogoURL);
		assert.isUndefined(bare.adjust?.secondary);
	});

	test("its lettering can be read, its lines seen, its paint told from the wood", () => {
		for (const abbrev of STOCK_COURT_TEAMS) {
			const c = stockCourt(team(abbrev))!;
			assert.isAtLeast(
				contrast(c.apronText!, c.apron!),
				3,
				`${abbrev} rail text`,
			);
			assert.isAtLeast(contrast(c.lines!, c.floor!), 1.5, `${abbrev} lines`);
			if (c.paint) {
				assert.isAtLeast(
					apart(c.paint, c.insideFloor ?? c.floor!),
					0.12,
					`${abbrev} paint`,
				);
			}
			if (c.centerText) {
				assert.isAtLeast(
					contrast(c.centerTextColor!, c.floor!),
					3,
					`${abbrev} center text`,
				);
			}
		}
	});

	test("nothing on it overlaps anything else, the lines, or where a league's pictures go", () => {
		for (const abbrev of STOCK_COURT_TEAMS) {
			const c = stockCourt(team(abbrev))!;
			const boxes: Box[] = [
				square(W / 2, H / 2, 30 * (c.adjust?.logo?.scale ?? 1), "center logo"),
			];
			const wing = c.adjust?.secondary;
			if (c.secondaryLogoURL && wing) {
				const side = 13 * (wing.scale ?? 1);
				boxes.push(
					square(
						W * 0.27 + (wing.dx ?? 0),
						H / 2 + (wing.dy ?? 0),
						side,
						"wing",
					),
					square(
						W * 0.73 - (wing.dx ?? 0),
						H / 2 + (wing.dy ?? 0),
						side,
						"wing",
					),
				);
			}
			if (c.centerText) {
				const n = c.centerText.length;
				const wide = Math.min(40, n * 3.4 * 0.66 + (n - 1) * 0.4);
				boxes.push({
					x0: W / 2 - wide / 2,
					x1: W / 2 + wide / 2,
					y0: 6 - 1.7,
					y1: 6 + 1.7,
					what: "center text",
				});
			}
			for (const [i, a] of boxes.entries()) {
				for (const b of [...boxes.slice(i + 1), ...LEAGUE]) {
					assert.isFalse(overlap(a, b), `${abbrev}: ${a.what} on ${b.what}`);
				}
			}
			for (const b of boxes.filter((b) => b.what === "wing")) {
				// Clear of the three-point arcs, the sidelines, the half-court line
				// and the center circle.
				for (const rim of [RIM, W - RIM]) {
					assert.isAbove(
						nearest(b, rim, H / 2),
						ARC + 1,
						`${abbrev}: wing on the arc`,
					);
				}
				assert.isAbove(b.y0, 1.5, `${abbrev}: wing on the sideline`);
				assert.isAbove(b.y0 > H / 2 ? H - b.y1 : b.y0, 1.5);
				assert.isFalse(
					b.x0 < W / 2 && b.x1 > W / 2,
					`${abbrev}: wing on half court`,
				);
				assert.isAbove(
					nearest(b, W / 2, H / 2),
					6 + 1,
					`${abbrev}: wing on the circle`,
				);
			}
			// The center logo inside the circle it fills, where it has one.
			if (c.centerCircle) {
				assert.isAtMost(30 * (c.adjust?.logo?.scale ?? 1), 12, abbrev);
			}
		}
	});
});
