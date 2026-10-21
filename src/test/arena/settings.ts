// The owner's multiplayer league, as a starting point for the arena: the
// gameplay settings exported from it, minus anything that only affects one
// device, the UI or the room. Applied to a random-players league (owner's call:
// no real players in these tests).
//
// Two deliberate differences from the owner's league, both the owner's call:
// nobody is under the secondary hard cap (in the owner's league it binds the human
// teams), and every team is run by the CPU.
export const ARENA_SETTINGS = {
	allStarType: "draft",
	brotherRate: 0.05,
	budget: false,
	difficultySigning: 2.5,
	difficultyTrade: 3.5,
	draftAges: [18, 22],
	draftType: "cola",
	easterEggPlayers: false,
	elamASG: true,
	equalizeRegions: true,
	fakeAges: true,
	luxuryPayroll: 132650,
	maxContract: 36400,
	maxRosterSize: 18,
	minContract: 900,
	minPayroll: 72750,
	numGamesConf: null,
	numGamesDiv: null,
	numSeasonsFutureDraftPicks: 7,
	realDraftRatings: "rookie",
	rookieContractLengths: [4, 3],
	salaryCap: 109150,
	sonRate: 0.05,
	stealFactor: 1.1,
	threePointTendencyFactor: 0.946,
} as const;
