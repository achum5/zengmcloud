// WHAT'S CHANGED in this game, newest first - shown once to everybody, the
// first time they load it after (see checkCloudChanges). Every update adds
// one: a number one past the last, the day, and what changed in a sentence
// or two, the way the league will want to hear it.
export type CloudChange = { n: number; date: string; text: string };

export const CLOUD_CHANGES: CloudChange[] = [
	{
		n: 23,
		date: "2026-10-28",
		text: 'Realistic Front Offices: pending deals on Trade Proposals now show each player\'s age and ovr/pot, why each team is making the deal ("A contender adding for a title run", "Rebuilding, for later", "Saves money"...) and what it wants most in a bid (players who help now, young players and picks, or cap relief).',
	},
	{
		n: 22,
		date: "2026-10-28",
		text: "Realistic Front Offices: when two CPU teams agree a deal that's open for bids, a \"Pending deal\" button now shows in the header (it takes you to Trade Proposals) and everyone's phone gets a notification. The front office breakdown on a trade's summary page now shows without God Mode. New leagues start with Budget off.",
	},
	{
		n: 21,
		date: "2026-10-28",
		text: 'Realistic Front Offices: the trading block now shows what the market would pay - put a player up and teams bid what he\'s worth around the league (contenders will offer firsts for a good young starter), and the position and skill tags work too, with each bid including a player you asked for. Salaries are left out, so make the money work before you offer. The "!" button in the header is gone.',
	},
	{
		n: 20,
		date: "2026-10-28",
		text: "3D game: lighter on phones - it now draws a steady 30 frames a second instead of running your phone flat out, stops drawing when you scroll away from the court, and does less work per frame, so it should run smoother and cooler.",
	},
	{
		n: 19,
		date: "2026-10-28",
		text: "Watching a league-mate's live game: if their broadcast stops before the game is over on your screen, your game keeps playing on from where you are (with your own controls) instead of jumping to the final. The app also checks for updates whenever you come back to it - if you see \"Update Available\", tap it so you have all of today's live-game fixes.",
	},
	{
		n: 18,
		date: "2026-10-28",
		text: "Live games: the score bar and the ticker at the bottom no longer show the final of a game that's still playing out live - yours, or a league-mate's you're watching (or that's still being broadcast). It shows up once the game has played out.",
	},
	{
		n: 17,
		date: "2026-10-28",
		text: 'Player Stats: with a team picked, the Regular Season / Playoffs / Combined menu now also lists each playoff series that team played (like "R1 vs WAS") - pick one to see the stats from just that series, advanced stats included.',
	},
	{
		n: 16,
		date: "2026-10-28",
		text: "Watching a league-mate's live game: switching to another app (or the person simming switching apps) no longer ends it for you - no more jumping straight to the final with the score showing up top. When you come back it picks up where they are.",
	},
	{
		n: 15,
		date: "2026-10-28",
		text: "Realistic Front Offices: \"What would make this work?\" now names the CPU team's price - the cheapest deal it would take for the player you asked for - instead of almost always saying it doesn't see a deal. Whether the price is worth it is up to you.",
	},
	{
		n: 14,
		date: "2026-10-28",
		text: "Fixed a playoff game that was already played sometimes showing up again under Upcoming Games (with Watch/Sim buttons) after catching up on a league-mate's sims.",
	},
	{
		n: 13,
		date: "2026-10-28",
		text: "Box scores: Lineups can now switch between 5-man, 4-man, 3-man and 2-man groups, and there's a new On/Off section - pick any player on either team and see how his team (or the other team) did with him on the floor vs. off it: +/-, ratings, eFG%, rebounding, assist, block, steal and turnover rates. The shooting, rebounding and other rate stats show for games played from now on.",
	},
	{
		n: 12,
		date: "2026-10-28",
		text: "3D game: a shooter with a hand right in his face sometimes leans away from it as he rises - an off-balance jumper instead of the same straight-up shot every time.",
	},
	{
		n: 11,
		date: "2026-10-28",
		text: 'No more "database error ... the operation was aborted - maybe your browser isn\'t letting the game store any more data" popup when coming back to the app on your phone. Nothing was ever wrong; the warning now only shows when storage really is nearly full.',
	},
	{
		n: 10,
		date: "2026-10-28",
		text: "Realistic Front Offices: CPU teams now treat all payroll above the league minimum as real money, not just payroll over the cap - so bad teams with cap room stop handing out long max deals to mediocre young players. They also have a more accurate idea of what cap room buys in free agency.",
	},
	{
		n: 9,
		date: "2026-10-28",
		text: "3D game: poor passers stare down where the ball is going before they throw it; great passers see it late and now and then throw it without looking.",
	},
	{
		n: 8,
		date: "2026-10-28",
		text: "The game flow chart (score and win %) now always uses the two teams' colors, and shows during live games too, under the box score, filling in as the game goes. The faces in the Lineups section are plain now - no colored boxes around them.",
	},
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
