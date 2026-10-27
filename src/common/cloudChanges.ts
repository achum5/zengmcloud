// WHAT'S CHANGED in this game, newest first - shown once to everybody, the
// first time they load it after (see checkCloudChanges). Every update adds
// one: a number one past the last, the day, and what changed in a sentence
// or two, the way the league will want to hear it.
export type CloudChange = { n: number; date: string; text: string };

export const CLOUD_CHANGES: CloudChange[] = [
	{
		n: 7,
		date: "2026-10-27",
		text: "Realistic Front Offices: CPU teams now judge much better which players will re-sign with them - a star on an expiring deal is worth more to them in a trade, a fringe veteran less.",
	},
	{
		n: 6,
		date: "2026-10-27",
		text: "The latest from Basketball GM: more options for how real draft prospects are rated (and the best prospects in career/peak modes can now be better than a usual #1 pick), real player overrides can be loaded from a URL in Global Settings, and the game copes better with browser translation.",
	},
	{
		n: 5,
		date: "2026-10-27",
		text: "Realistic Front Offices: CPU teams got smarter. They haggle before agreeing to a trade, won't make a rival contender better (yours included), and treat payroll over the cap - and the luxury tax - as real money, so no more max deals to role players. Full rosters still sign a clearly better free agent during the season, and draft-week pick swaps for nothing are gone.",
	},
	{
		n: 4,
		date: "2026-10-27",
		text: "3D game: players no longer move in lockstep - after a rebound, a shot or a free throw each one reacts on his own beat. Also fixed blocks made from way across the floor.",
	},
	{
		n: 3,
		date: "2026-10-27",
		text: "3D game: more of each player's ratings show. Good rebounders crash the glass and box out, poor ones watch. Tired players with low endurance get their breath at the line and get back slower. Poor passers sometimes hit cutters late. Rim protectors go up by their leaping. Smart players keep working off the ball. Good handlers dribble low, poor ones high.",
	},
	{
		n: 2,
		date: "2026-10-27",
		text: "3D game: every shooter has his own form - his own release (high, quick or the usual) and his own way of landing - and poor shooters push the ball instead of shooting it pure.",
	},
	{
		n: 1,
		date: "2026-10-27",
		text: "3D game: shooters get their own shots off their own way - step-backs, side-steps, hesitation pull-ups, crossovers, fadeaways and elbow turnarounds, by their handle, shooting and go-to moves. A hard closeout can get shot-faked and driven past.",
	},
];

// The changes somebody who has seen up to `last` has not (all of them if
// he has never seen any), newest first.
export const unseenChanges = (last: number | undefined): CloudChange[] =>
	CLOUD_CHANGES.filter((c) => last === undefined || c.n > last);
