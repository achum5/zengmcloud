// WHAT'S CHANGED in this game, newest first - shown once to everybody, the
// first time they load it after (see checkCloudChanges). Every update adds
// one: a number one past the last, the day, and what changed in a sentence
// or two, the way the league will want to hear it.
export type CloudChange = { n: number; date: string; text: string };

export const CLOUD_CHANGES: CloudChange[] = [
	{
		n: 51,
		date: "2026-10-30",
		text: "Realistic Front Offices has its own difficulty setting in League Settings (Game Modes > Front Office Difficulty): Easy, Realistic (the default), Hard or Insane. Realistic puts you on an even footing with CPU teams; higher makes them want a surer gain before they trade with you, lower makes them easier to deal with. The regular difficulty settings still do nothing in this mode.",
	},
	{
		n: 50,
		date: "2026-10-30",
		text: "Realistic Front Offices: Trade Proposals works like regular BBGM again - same page, same refresh button - but each CPU team offers what it would really give for one of your players or picks, so every offer there is one it will accept. The page can take several seconds the first time; after that it's quick until the offers change.",
	},
	{
		n: 49,
		date: "2026-10-30",
		text: 'Recaps in a women\'s league now say she and her instead of he and his, and "six players in double figures" instead of "six men", in game and day recaps alike.',
	},
	{
		n: 48,
		date: "2026-10-30",
		text: "Recaps no longer call it an upset when a team with a clearly better record beats a worse one, even if the betting line had it the other way. The All-Star Game recap gets more headlines and puts the dunk contest and three-point shootout in their own paragraph.",
	},
	{
		n: 47,
		date: "2026-10-30",
		text: 'Recaps: when a team was behind with two minutes left and the winning basket is already in the first sentence, the finish now says how the last two minutes went ("The Spurs were up 120-116 with two minutes left. The Nuggets outscored them 13-6 from there.") instead of skipping straight to the final score.',
	},
	{
		n: 46,
		date: "2026-10-30",
		text: "Playoff recaps always say which game of which round it was in the first sentence (\"...113-111 in Game 1 of the Finals\"), and a title recap says when it's the franchise's first championship (or a repeat) right up top instead of in the last line.",
	},
	{
		n: 45,
		date: "2026-10-30",
		text: "Realistic Front Offices: pending deals and bidding are gone - trades work like regular BBGM again, and the header bell and the Pending Deals setting are removed. CPU teams still won't take less from you than another team would give them: if a team turns you down because someone else would pay more, it now just says it has a better offer elsewhere, without saying who.",
	},
	{
		n: 44,
		date: "2026-10-30",
		text: "Day recaps: a season high or a milestone for a player the recap hasn't mentioned yet now says whose player he is (\"the Thunder's Marcus Vaughn set a season high with 32 points\").",
	},
	{
		n: 43,
		date: "2026-10-30",
		text: 'Recaps: in a blowout, the star\'s line can say how little he played ("28 points in just 26 minutes"), and the day recap\'s standings list says "climbed" once instead of for every team.',
	},
	{
		n: 42,
		date: "2026-10-30",
		text: 'Recaps now and then tag a conference leader in the first line, the way the wire does ("The Raptors held off the East-leading Knicks 114-109"), and the day recap\'s list of comebacks no longer says "to beat" every time.',
	},
	{
		n: 41,
		date: "2026-10-30",
		text: "Recaps no longer skip the part of a game that made it close: a 20-point lead that shrank to 3 in the fourth, a team that came back late before losing, or a lead the other side built after halftime in a game won in overtime are all told now, in order. The day recap opens on the game its headline is about (with the streak it ended, when that's why it's the headline), and a few more awkward bits are fixed, like \"at the buzzer... but time had run out\".",
	},
	{
		n: 40,
		date: "2026-10-30",
		text: 'Recaps: when both teams come in with the same record, the first line now shows it for both of them instead of only one, and a few clumsy sentences that used the same verb twice ("They led the rest of the way and led by as many as 27", "had 31 points... had a triple-double") are gone.',
	},
	{
		n: 39,
		date: "2026-10-30",
		text: 'Recaps read more like a reporter wrote them: after a player\'s first mention he goes by his last name ("Ingram\'s layup tied it"), and now and then a team goes by its city ("for Indiana"). Game recaps open in more ways - a streak first, a player first, or where a road win happened ("beat the Celtics 102-91 in Boston") - and the halftime score and the biggest lead share a sentence more often. Day recaps sometimes move around the league with a city ("In Charlotte, ..."), and "Game 2 in Golden State" is now "Game 2 at the Warriors\' place".',
	},
	{
		n: 38,
		date: "2026-10-30",
		text: "3D game: the building looks a lot more like an NBA arena. Every fan has a real face and a body with arms and hands, the stick-figure people along the court are gone, and the courtside seats behind the baskets and in the corners are full of real-looking fans in padded chairs - on their phones, holding drinks, clapping when their team scores and up waving when the place erupts. The officials wear the NBA's gray, the benches have high-backed chairs with the staff in a row behind them, a cooler and towels, the stanchions show the home team's name, and there's a sideline reporter, security, TV cameras in the corners, and moppers who wipe the floor during breaks. After the game there's no more handshake line: players find a few of the other team around midcourt for daps and hugs before heading off, the head coaches meet at the scorer's table, and the officials leave the floor.",
	},
	{
		n: 37,
		date: "2026-10-30",
		text: "3D game: a shot blocked by a man trailing the play is now a chase-down - he runs the shooter down and gets a hand on it from behind, instead of turning up in front of him out of nowhere. A travel now shows the player carrying the ball a stride or two too many before the whistle, and the arena has tunnels into the lower bowl. Fixed: a defender's arms reaching into the man he's guarding, a missed layup passing through the net, a defender sliding out of bounds along the baseline, a second crossover snapping the ball up off the floor, and a player's body snapping forward as he takes off.",
	},
	{
		n: 36,
		date: "2026-10-30",
		text: "Realistic faces: Black players have black hair now - about a third were getting brown hair and beards, and made-up players who already have it get fixed at the next preseason. Fewer redheads and blonds, no more blush or cheek scribbles, and draft prospects look their draft-day age. Facial hair is different for every player: some already have a goatee or even a beard as rookies, some start late, some stay clean-shaven their whole career, and some shave it off or trim it back partway through. With Face Aging, every season's face is kept (click a player's picture to see them all), the latest one always matches his picture, and a retired player's face never changes again.",
	},
	{
		n: 35,
		date: "2026-10-30",
		text: "3D game: you can start to tell who the stars are. A great shooter now and then pulls up from a few steps behind the line, and sometimes turns and heads back up the floor before his three is even through; a great ball handler can cross a slow defender up so badly his feet go out from under him (an average one hardly ever does). A ball handler with his man right up on him turns his shoulder into him to protect the ball, and two players face to face no longer lean into each other until their heads overlap.",
	},
	{
		n: 34,
		date: "2026-10-30",
		text: "New Draft setting, Realistic Draft Prospects (needs God Mode): random draft classes come out with skills spread out and paired like real NBA players' - more true non-shooters, 3-point shooters who aren't automatically good from mid-range, ball handlers who aren't all passers. It only changes classes generated after it's turned on, and replaces Specialized Draft Prospects if both are on.",
	},
	{
		n: 33,
		date: "2026-10-29",
		text: "3D game: a quarter that runs out without a shot now ends the way it does in the NBA - with a few seconds left, the man with the ball races up the floor with it as the horn goes instead of standing there with it, and more often than not he gets a shot or a heave off a split second after the horn. The officials run onto the floor waving it off, and it doesn't count even if it goes in.",
	},
	{
		n: 32,
		date: "2026-10-29",
		text: "3D game: fouls look like fouls - a reach-in is a chop down at the ball that jerks it in the ball handler's hands, a help man a step late knocks the driver off balance, and a cutter held up gets grabbed and looks for the call. The official then signals what it was - a chop on his wrist for a hit, a hand round it for holding, hands on his hips for blocking, a fist punched out for a charge - and points the way it goes.",
	},
	{
		n: 31,
		date: "2026-10-29",
		text: "3D game: dribbling looks more like real basketball - walking it up with nobody near him, a player stands tall and bounces it easy at his waist; with his man up on him he gets down in his stance and pounds it quicker; and every player has his own stance, rhythm and dribble height. Turnovers outside a set play now look like the real thing too - a pass picked off in the lane, the ball stripped off the dribble, a pass thrown away past the man it was meant for, a charge, a travel - instead of the ball just flying to the defender.",
	},
	{
		n: 30,
		date: "2026-10-29",
		text: "3D game: the ball no longer jumps into a player's hands - picked up off the dribble or off the floor, it comes up into his hands as they close on it, and passes always come out of the passer's hands instead of straight up out of a bounce of his dribble.",
	},
	{
		n: 29,
		date: "2026-10-29",
		text: "3D game: end-of-quarter heaves now happen in the time that was actually left - the inbound goes straight to the shooter, he races up the floor and lets it fly as time runs out, with a real heave thrown from over his head on the run instead of a 70-foot jump shot, and his man chasing him instead of standing in his face. The game clock also keeps running at real speed while a shot is in the air or the ball bounces out of bounds, instead of crawling, and the horn goes with the heave in the air.",
	},
	{
		n: 28,
		date: "2026-10-29",
		text: "3D game: players on offense no longer hang around in the lane - a big who ducks in to seal his man pops right back out, and anybody left standing in there steps out to the short corner or dunker spot before three seconds are up. The 3D court also does about half the work it did each frame, so it should run smoother and cooler on phones.",
	},
	{
		n: 27,
		date: "2026-10-28",
		text: "Realistic Front Offices: pending deals now stay open for two full sims after they open or get a new bid (a sim being one press of Play, however many days), so a bid made right before someone sims can't win before everyone else gets a sim to answer it. Every pending deal is decided by the last day before the trade deadline. Your own big trades with CPU teams (one of their 5 best players, or a 1st) now become pending deals too: the CPU team agrees, other teams get two sims to top it, and if nobody does it goes through.",
	},
	{
		n: 26,
		date: "2026-10-28",
		text: "Realistic Front Offices: when a pending deal is about to go to a better bid, the team it was agreed with now gets to answer with the most it would really pay for the same players. So a bid wins only if it beats what every CPU team would actually give - no more stealing a player just because nobody else got a chance to match.",
	},
	{
		n: 25,
		date: "2026-10-28",
		text: 'Realistic Front Offices: the "Pending deal" button is now a bell in the header with a red count of pending deals and trade proposals made to your team. A contender\'s pending deal now says how much it raises its title chance, and a player thrown in just for salary no longer counts as "help now" in what a team wants.',
	},
	{
		n: 24,
		date: "2026-10-28",
		text: 'Realistic Front Offices: a bid on a pending deal can now also ask for more of that team\'s players to make the salaries work - before, adding one (say, to match salary with a team over the cap) got the offer turned down as "part of a pending deal". "What would make this work?" works on bids too.',
	},
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
