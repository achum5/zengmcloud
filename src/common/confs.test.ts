import { assert, describe, test } from "vitest";
import {
	confByCid,
	confsDivsTeamsProblem,
	normalizeConfImgURL,
} from "./confs.ts";

const confs = [
	{ cid: 0, name: "Eastern Conference", abbrev: "East", imgURL: "e.png" },
	{ cid: 1, name: "Western Conference" },
];

describe("confByCid", () => {
	test("a team's conference is whatever its cid points at", () => {
		assert.deepStrictEqual(confByCid(confs, 0), {
			cid: 0,
			name: "Eastern Conference",
			abbrev: "East",
			imgURL: "e.png",
		});
		// No logo set is still a conference, just one with nothing to draw.
		assert.deepStrictEqual(confByCid(confs, 1), {
			cid: 1,
			name: "Western Conference",
			abbrev: undefined,
			imgURL: undefined,
		});
	});

	test("a cid nothing points at is not a conference", () => {
		assert.strictEqual(confByCid(confs, 7), undefined);
		assert.strictEqual(confByCid(confs, undefined), undefined);
		assert.strictEqual(confByCid([], 0), undefined);
	});
});

describe("normalizeConfImgURL", () => {
	test("a cleared field removes the logo rather than storing an empty string", () => {
		assert.strictEqual(normalizeConfImgURL(""), undefined);
		assert.strictEqual(normalizeConfImgURL("   "), undefined);
		assert.strictEqual(normalizeConfImgURL(undefined), undefined);
	});

	test("a real URL is kept, trimmed", () => {
		assert.strictEqual(
			normalizeConfImgURL("  /img/east.png "),
			"/img/east.png",
		);
	});
});

describe("confsDivsTeamsProblem", () => {
	const confs = [
		{ cid: 0, name: "East" },
		{ cid: 1, name: "West" },
	];
	const divs = [
		{ cid: 0, did: 0, name: "Atlantic" },
		{ cid: 1, did: 1, name: "Pacific" },
	];
	const teams = [
		{ tid: 0, cid: 0, did: 0, abbrev: "BOS" },
		{ tid: 1, cid: 1, did: 1, abbrev: "LAL" },
	];

	test("a consistent league has no problem", () => {
		assert.strictEqual(confsDivsTeamsProblem(confs, divs, teams), undefined);
	});

	test("a team whose cid disagrees with its division is refused by name", () => {
		const bad = [...teams, { tid: 2, cid: 0, did: 1, abbrev: "SAC" }];
		assert.match(confsDivsTeamsProblem(confs, divs, bad) ?? "", /SAC.*another/);
	});

	test("a team in a division that does not exist is refused", () => {
		const bad = [...teams, { tid: 2, cid: 0, did: 9, abbrev: "SAC" }];
		assert.match(
			confsDivsTeamsProblem(confs, divs, bad) ?? "",
			/SAC.*does not exist/,
		);
	});

	test("a division pointing at a missing conference is refused", () => {
		const bad = [...divs, { cid: 5, did: 2, name: "Central" }];
		assert.match(confsDivsTeamsProblem(confs, bad, teams) ?? "", /Central/);
	});

	test("duplicate ids are refused", () => {
		assert.match(
			confsDivsTeamsProblem([...confs, { cid: 0, name: "Dup" }], divs, teams) ??
				"",
			/share the id 0/,
		);
		assert.match(
			confsDivsTeamsProblem(
				confs,
				[...divs, { cid: 0, did: 0, name: "Dup" }],
				teams,
			) ?? "",
			/share the id 0/,
		);
	});

	test("a league needs at least one conference and one division", () => {
		assert.ok(confsDivsTeamsProblem([], divs, teams));
		assert.ok(confsDivsTeamsProblem(confs, [], teams));
	});
});
