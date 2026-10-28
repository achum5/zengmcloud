import { assert, beforeEach, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import type {
	FrontOfficeAsset,
	FrontOfficeSide,
} from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import { dealNotes } from "./dealNotes.ts";
import type { LeagueOutlook } from "./outlook.ts";

beforeEach(() => {
	resetG();
	g.setWithoutSavingToDB("salaryCap", 100_000);
});

// Team 0 a contender, team 1 rebuilding.
const outlook = {
	teams: new Map([
		[0, { strength: [6], title: [0.2] }],
		[1, { strength: [-6], title: [0] }],
	]),
} as unknown as LeagueOutlook;

const player = (age: number, amount: number): FrontOfficeAsset => ({
	kind: "player",
	pid: age,
	name: "A Player",
	age,
	ovr: 60,
	amount,
	exp: 2030,
	ovrBySeason: [],
	hereBySeason: [],
	reSign: 0.5,
	effect: [],
});

const pick: FrontOfficeAsset = {
	kind: "pick",
	dpid: 1,
	season: 2028,
	round: 1,
	originalTid: 0,
	place: 20,
	spread: 5,
	ovrBySeason: [],
	hereBySeason: [],
	effect: [],
};

const side = (
	tid: number,
	value: number[],
	gets: FrontOfficeAsset[],
	gives: FrontOfficeAsset[],
	money?: number[],
): FrontOfficeSide =>
	({ tid, value, gets, gives, money }) as unknown as FrontOfficeSide;

test("a contender buying a veteran with picks, a rebuilding team selling", () => {
	const [buyer, seller] = dealNotes(outlook, {
		seasons: [],
		sides: [
			side(0, [0.02, 0.01, -0.002, -0.002], [player(30, 10_000)], [pick]),
			side(1, [-0.001, 0, 0.003, 0.004], [pick], [player(30, 10_000)]),
		],
	});
	assert.strictEqual(buyer.why, "A contender adding for a title run");
	assert.strictEqual(buyer.wants, "Players who help now");
	assert.strictEqual(seller.why, "Rebuilding, for later");
	assert.strictEqual(seller.wants, "Young players and picks");
});

test("shedding salary for nothing", () => {
	const [shedder] = dealNotes(outlook, {
		seasons: [],
		sides: [
			side(
				1,
				[0.004, 0.004, 0, 0],
				[],
				[player(31, 30_000)],
				[0.004, 0.004, 0, 0],
			),
			side(0, [0.001, 0, 0, 0], [player(31, 30_000)], []),
		],
	});
	assert.strictEqual(shedder.why, "Saves money");
	assert.strictEqual(shedder.wants, "Cap relief");
});
