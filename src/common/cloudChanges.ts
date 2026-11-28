// WHAT'S CHANGED in this game, newest first - shown once to everybody, the
// first time they load it after (see checkCloudChanges). Every update adds
// one: a number one past the last, the day, and what changed in a sentence
// or two, the way the league will want to hear it. (`quiet`: taken back out
// - its number kept, so the next one is still one past it, and never shown.)
export type CloudChange = {
	n: number;
	date: string;
	text: string;
	quiet?: boolean;
};

export const CLOUD_CHANGES: CloudChange[] = [
	{
		n: 683,
		date: "2026-11-28",
		text: "3D game: no more freezes of most of a second when Key Plays cuts to the next highlight or into a replay. The players' steps were being worked out all the way from the tip at every cut; they now start just before the moment shown.",
	},
	{
		n: 682,
		date: "2026-11-28",
		text: "3D game: uses less of your phone's memory. An old copy of the crowd and the ad boards was being kept all game, and the GPU drawing (in progress) no longer paints crowd pictures it never shows. The GPU drawing also sends its pictures to the graphics card before each frame is drawn instead of partway through.",
	},
	{
		n: 681,
		date: "2026-11-28",
		text: "3D game, GPU drawing (in progress): less is sent to the graphics card each frame, and the page now tries two ways of sending it on your device and keeps the faster one.",
	},
	{
		n: 680,
		date: "2026-11-28",
		text: "3D game, GPU drawing (in progress): faster again on phones. Each player takes less work to get ready, heads are redrawn a few at a time, and the baskets, ball, name tag and heads go to the graphics card together once a frame instead of one by one.",
	},
	{
		n: 679,
		date: "2026-11-28",
		text: "3D game, GPU drawing (in progress): faster on phones with benches and refs on the court. Heads are only redrawn when they change, and far fewer steps go to the graphics card each frame. The frame rate meter now shows the time each part of the picture takes.",
	},
	{
		n: 678,
		date: "2026-11-27",
		text: "3D game, GPU drawing (Global Settings > 3D Drawing > GPU, in progress): the court lines, baskets, ball, scoreboard over center court, scorer's table, courtside seats, benches, refs and photographers are now in, looking the same as in today's drawing. Next: the other 3D scenes.",
	},
	{
		n: 677,
		date: "2026-11-27",
		text: "3D game, GPU drawing (Global Settings > 3D Drawing > GPU, in progress): the players now look the same as in today's drawing, with their own jerseys, numbers, names, faces, hair, shoes and hands, the dark edge around them, and their shadows and reflections on the floor. Court lines, baskets, the scoreboard, benches and refs come next.",
	},
	{
		n: 676,
		date: "2026-11-27",
		text: "CPU teams over the roster limit no longer release a player with years left on a big contract while they could cut a cheaper spare. They first try to trade him away, with a pick attached if that's what it takes, and if nobody takes him they keep him and cut the player they owe least after this season.",
	},
	{
		n: 675,
		date: "2026-11-27",
		text: "Trade rumors: when CPU teams talk trades with each other, the News Feed and the scores ticker now pass it on, from teams calling about a player to offers turned down and deals that came close. Each card's header shows the teams after him and his team, and a player's page lists this season's talk about him below Transactions.",
	},
	{
		n: 674,
		date: "2026-11-27",
		text: 'Shared leagues: when your phone\'s connection drops right as a sim is uploading (switching towers, for example), the app now makes itself a fresh connection and finishes the upload within about half a minute, instead of sitting on "Uploading to the cloud" until you close and reopen it. Sync activity on the Multiplayer Sync page shows real times again instead of "20785d ago".',
	},
	{
		n: 673,
		date: "2026-11-27",
		text: "3D game, New sim: players run like runners now, not like a quick walk. They push off a nearly straight back leg, bend less over the planted foot, and spring up between steps, measured against NBA players running the floor in game film. Ball handlers bringing it up ride higher in their legs too.",
	},
	{
		n: 672,
		date: "2026-11-27",
		text: "3D game: no more freezes of up to a second and a half when Key Plays jumps on to the next play. Each jump now takes a few hundredths of a second, with the same picture as before.",
	},
	{
		n: 671,
		date: "2026-11-27",
		text: "3D game: the score bugs file now has ESPN's looks from its first season (2002-03) on, with ABC's Sundays, playoffs and Finals in ESPN's look from 2006-07, NBC's from 1996-97 to 2001-02, TNT's from 1989-90 to 1998-99, and TNT's playoff series lines. A playoff series line too long for its bug's room is now set smaller to fit. Import the file again to get them.",
	},
	{
		n: 670,
		date: "2026-11-27",
		text: "3D game, a faster way to draw it, to try: Global Settings > 3D Drawing > GPU (in progress) draws the whole game on your device's graphics chip in one picture, in plain kit colors for now, with its frame rate shown over it. Current + frame rate shows today's drawing with the same readout, to compare.",
	},
	{
		n: 669,
		date: "2026-11-27",
		text: "Player Stats: one team in this season now shows only the players on its roster today, like the roster page, so guys traded away or released drop off. The Active roster switch above the table brings them back, and past seasons still show everyone who played for the team that year.",
	},
	{
		n: 668,
		date: "2026-11-27",
		text: "3D game: the score bugs file now also has CBS's late-80s look (1987-88 to 1989-90: Sundays, the playoffs and the Finals) and TBS NBA Thursday's (1989-90 to 1994-95). A team name too long for a bug's room is now set smaller to fit.",
	},
	{
		n: 667,
		date: "2026-11-27",
		text: "Daily Schedule: the trade deadline box is now just a Trade Deadline Day header over that night's games. The Pass deadline button and the extra line are gone; Play > One day passes the deadline as before.",
	},
	{
		n: 666,
		date: "2026-11-27",
		text: "3D game, New sim: drives look like drives. The man with the ball no longer jogs his drive straight at his defender to fill the time before his pass or shot: he sizes his man up where he is, then goes at full speed. And he no longer drifts ahead toward where a drive ends and then crawls, waiting for it to start.",
	},
	{
		n: 665,
		date: "2026-11-27",
		text: "3D game, New sim: the man with the ball no longer walks his dribble straight into the defender in front of him. With a defender in his way he goes by his shoulder at a drive's pace, and a defender being driven at rides the driver on his hip instead of backpedaling square in front of him. Defenders also pick the ball up around half court instead of chasing it back to the far baseline.",
	},
	{
		n: 664,
		date: "2026-11-27",
		text: "Smart CPU: GMs now read the playoffs the way the game plays them. Playoff games are more decisive than regular season ones (starters tire less and lineup fit counts more), so real contenders' title chances go up and long shots' go down. Contenders pay a bit more for upgrades, and teams with little chance keep their picks.",
	},
	{
		n: 663,
		date: "2026-11-27",
		text: "3D game: national TV score bugs can now change with the years. A team looks file can give each network's bug its seasons, so a 2005 game on TNT carries TNT's 2005 bug. The new score bugs file has TNT's looks from 1999-2000 to 2024-25, on its Thursday doubleheaders and in the playoffs, and today's networks from 2025-26 on.",
	},
	{
		n: 662,
		date: "2026-11-27",
		text: 'Smart CPU: the Danger Zone\'s "Run it now" button (Smart CPU moves) now also works during the season, up to the trade deadline. On deadline day, press it after your own moves for one more round of trades between CPU teams.',
	},
	{
		n: 661,
		date: "2026-11-27",
		text: "Smart CPU: CPU teams now make the small trades real teams make most, like a rotation player for a bench player and a second-round pick. In tests they made about twice as many trades with each other by the deadline, spread over twice as many teams.",
	},
	{
		n: 660,
		date: "2026-11-27",
		text: "3D game, New sim: players working their spot off the ball no longer pace back and forth. When the next spot they'd pick is back the way they came, they keep going or pull up and stand a couple of seconds, the way real players spot up.",
	},
	{
		n: 659,
		date: "2026-11-27",
		text: "3D game, New sim: when the defense traps a pick-and-roll, both defenders now jump the ball handler as he comes off the screen and stay on him for a beat. Before, they drifted over late and the trap was hard to see.",
	},
	{
		n: 658,
		date: "2026-11-27",
		text: "3D game, New sim: how the defense plays a pick-and-roll now comes from the players in it. A slow center drops back, players of a size switch, and star ball handlers get trapped now and then. Before, the coverage gave away who was about to take the shot.",
	},
	{
		n: 657,
		date: "2026-11-27",
		text: "Smart CPU: GMs now count how teams have actually played this season, not just their ratings. A team beating the margin its roster and schedule say it should gets credit for the part of that which isn't luck (about a sixth of it by the trade deadline), so its title chances and trade plans move a little with how it plays.",
	},
	{
		n: 656,
		date: "2026-11-27",
		text: "3D game, New sim: players off the ball no longer stop dead on every spot and step straight back to where they came from. They keep walking from one spot into the next, and players running the play's routes carry on into their next run instead of stopping for half a second in between. Far fewer players stand around in a set.",
	},
	{
		n: 655,
		date: "2026-11-27",
		text: "3D game: games start much sooner and no longer crash phones. The game is now worked out a quarter at a time, so it starts as soon as the first quarter is ready, the rest comes in while you watch, and it takes about half the memory it did.",
	},
	{
		n: 654,
		date: "2026-11-27",
		text: "3D game, New sim: rebounders no longer come down and stand still holding the ball. They land still moving, turn up the floor and throw the outlet on the way. Jump shooters drift through the air the way they came into the shot and land a step on, instead of going straight up and down.",
	},
	{
		n: 653,
		date: "2026-11-27",
		text: "3D game: a floor picture can now take in more of the floor around the court, so lettering behind the baselines and along the sidelines shows in full instead of getting cut off. A floor picture made 118 x 66 ft (12 ft past each baseline and 8 past each sideline, 2360 x 1320 at 20 px a foot) is laid down that wide; pictures made on the usual template work as before.",
	},
	{
		n: 652,
		date: "2026-11-27",
		text: "3D game, New sim: players no longer jerk sideways when they get too close to a teammate or an opponent. They see each other coming and bend their run around smoothly, staying on one side instead of swerving one way and then cutting back the other.",
	},
	{
		n: 651,
		date: "2026-11-27",
		text: "Daily Schedule: trade deadline day now reads as one day. Instead of a bare Trade Deadline box and no games, it shows a Trade Deadline Day header on top of that night's games, and picking that date in the week strip shows the same thing. Once you pass the deadline, the games are ready to watch or sim.",
	},
	{
		n: 650,
		date: "2026-11-26",
		text: "3D game, New sim: nobody stands around watching a shot anymore. Players getting back keep jogging back while the ball is in the air, the shooter heads back or a big follows his shot in, and defenders with nobody to box out drift in toward the rebound. Players also no longer freeze for two seconds on every bounce pass.",
	},
	{
		n: 649,
		date: "2026-11-26",
		text: "3D game, New sim: defenders sliding with their man no longer hold both arms straight out like airplane wings. They keep their hands low and active, one down at the ball and the other up in the passing lane, the way real defenders do.",
	},
	{
		n: 648,
		date: "2026-11-26",
		text: "3D game, New sim: the player with the ball stands still less. After catching a pass he eases to a stop over most of a second instead of pulling up in a stride, and on the dribble he keeps working his defender himself: crossing the floor, attacking off the catch and flowing from one move into the next, rather than being walked through the play in short steps with stops in between.",
	},
	{
		n: 647,
		date: "2026-11-25",
		text: "3D game, New sim: players no longer flinch into a defensive crouch. Someone nudged a step aside, slowing for a moment near the ball, or backing off at a walk now stays upright instead of dropping into a slide with arms flung out and popping straight back up, and getting into or out of a stance takes a third of a second instead of a blink.",
	},
	{
		n: 646,
		date: "2026-11-25",
		text: "3D jerseys: the number on the back can sit higher with the name under it, the way Memphis wears it, and names can arch as much as on the real jerseys. Manage Teams, 3D jerseys, Lettering: the back number height, name height and name arch sliders go further.",
	},
	{
		n: 645,
		date: "2026-11-25",
		text: "3D game, New sim: players speed up and slow down more smoothly. A step or two to a new spot eases in and out the way the player actually moved, instead of a burst and a hard stop, and off-ball defenders shift with the ball instead of darting from spot to spot. About a third fewer jerky moments in a game.",
	},
	{
		n: 644,
		date: "2026-11-25",
		text: "NBA Calendar: from next season, Rivals Week has real rivalry games. The league's biggest rivalries (recent playoff matchups, division rivals, nearby cities, good teams), and in shared leagues your teams against each other when they fit, move into that week and get its national TV games. Only dates change: every team keeps the same games, opponents and home games, and no team gets an extra back-to-back.",
	},
	{
		n: 643,
		date: "2026-11-25",
		text: "3D game, Key plays: fixed games where the whole 4th quarter (sometimes the whole second half) played out every possession even when it wasn't close, using up your key plays time so the first three quarters flew by. The time you pick is now also spread evenly over all four quarters, instead of about half of it going to the 4th. A close finish still plays in full on top of that time.",
	},
	{
		n: 642,
		date: "2026-11-25",
		text: "3D jerseys: the chest number can now sit off to one side, like the real Mavericks, Kings and Blazers jerseys, and go much smaller, like the Warriors number inside their logo. Manage Teams, 3D jerseys, Lettering has a new Number left/right slider, and that window now fits on a phone.",
	},
	{
		n: 641,
		date: "2026-11-25",
		text: "Shared leagues with auto play on: the Daily Schedule's game cards now show when auto play sims that day, on your own clock, on a tab at the top of each card like a tip-off time (for example 1:15 – 1:30 PM). Tap any day still to come to see its time on a phone.",
		quiet: true,
	},
	{
		n: 640,
		date: "2026-11-25",
		text: "3D game, New sim: the ball moves around the floor before the play is run, out to a wing, around the arc and back, instead of sitting with the point guard at the top. Players catch it and move it on quickly or take a dribble or two first, and a player who catches it carries on a step instead of stopping dead.",
	},
	{
		n: 639,
		date: "2026-11-25",
		text: "NBA Calendar, Play Through Injuries: the betting line, the sportsbook and team strength now judge your hurt players by the days they'll have left on game day, the same way the game decides who plays. A player at 3 days today whose next game is two days away counts as 2 days, so a 2-day setting already includes him. Also, if you never moved the slider with the calendar on, it now always shows the same number of days. Before, it could move a day on its own.",
	},
	{
		n: 638,
		date: "2026-11-25",
		text: "Shared leagues with auto play on: put your mouse over a day still to come on any calendar (the scores bar, the team calendars, the Daily Schedule's days and its month calendar, the date at the top) to see when auto play sims it, on your own clock, like 1:15 – 1:30 PM. On a phone, press and hold the day.",
	},
	{
		n: 637,
		date: "2026-11-25",
		text: 'NBA Calendar: injuries now show how many games a player will miss next to his days out, like "9 days (5 games)", counted from his own team\'s schedule. It\'s in the injury tooltip, the Injuries page, his injury history, the game page and the injury news. "12+ games" means more may come (the playoffs, or a series going long). Also fixed: when the game skipped a date with no games, like Christmas Eve, injury days only went down by one. Now every day that passed counts.',
	},
	{
		n: 636,
		date: "2026-11-25",
		text: "3D game, New sim: legs move more like a real body. Toes point out instead of in, so knees stop caving inward, hips turn with the feet, and players step around as they turn instead of twisting on planted feet until their legs cross. A foot no longer gets left behind on its toes, and the stepping leg no longer kicks out to the side.",
	},
	{
		n: 635,
		date: "2026-11-25",
		text: "3D game: the score bug keeps the same proportions on every size of picture, as if the whole picture were shrunk or blown up. The small picture in the Daily Schedule's Nightly Recap no longer has an oversized score bug, and a full size game shows the bug in proportion too. Phones keep their bigger, easier to read bug.",
	},
	{
		n: 634,
		date: "2026-11-25",
		text: "Recaps: every team named in a recap is now a link to its page, not just the teams that played. That covers the night's Streaks, The race and Worth watching on the Daily Schedule, and the Up next part of a game's recap.",
	},
	{
		n: 633,
		date: "2026-11-25",
		text: "Milestone Watch has a new look. Players close to a round number and seasons on a record pace are now clean tables with the numbers in columns. The all-time lists show each active player between the legends he's chasing, with how many he needs to pass the next one. On a phone, tap PTS, REB, AST and the rest to switch lists.",
	},
	{
		n: 632,
		date: "2026-11-25",
		text: "3D game, Cavaliers' score bug: THE DIFF tab now has room for a two-digit lead like +18, instead of cutting it off.",
	},
	{
		n: 631,
		date: "2026-11-25",
		text: 'Daily Schedule: the "Coming up" list of the next night\'s games is gone. To see them, tap that night in the strip of days at the top.',
	},
	{
		n: 630,
		date: "2026-11-25",
		text: "Daily Schedule: finished games that were on national TV now show the network's logo above Final, like the games still to be played and the game's own page.",
	},
	{
		n: 629,
		date: "2026-11-25",
		text: 'Shared leagues: when every team readies up for a regular-season stop like Dec 15, the day no longer sims right away. The ready-up goes away, the auto play countdown comes back, and the day sims when it hits zero. The trade deadline still goes as soon as everyone is ready. Phones also get a push when a ready-up is needed: "Dec 15. Ready up to advance."',
	},
	{
		n: 628,
		date: "2026-11-25",
		text: "3D game, New sim (Global Settings, 3D Game Sim): players no longer skip and drop into a lunge with every step. Feet land where legs can reach, and hips rise and fall smoothly like a real runner's instead of snapping down when a foot touches the floor.",
	},
	{
		n: 627,
		date: "2026-11-25",
		text: "Daily Schedule: on games still to be played, a team's logo, name and record now take you to that team's page.",
	},
	{
		n: 626,
		date: "2026-11-25",
		text: "3D game: the basket's stanchion no longer glitches as the camera moves. The pad on the arm by the backboard stopped blinking in and out, and the post behind the padded column stopped flickering.",
	},
	{
		n: 625,
		date: "2026-11-25",
		text: "3D game score bugs: BONUS no longer covers a team's abbreviation. On the bug the Magic, Spurs, Wolves, Clippers, Grizzlies, Hornets, Pistons, Pacers and Heat share, a team in the bonus now has its end of the bar slide out to make room for the BONUS tag, like on TV. On the Mavericks' bug the scores sit higher, so BONUS under them no longer runs into the numbers.",
	},
	{
		n: 624,
		date: "2026-11-25",
		text: "Finished games: no more green checks in the Line and O/U columns; the side that hit is just in bold.",
	},
	{
		n: 623,
		date: "2026-11-25",
		text: "3D game, New sim (Global Settings, 3D Game Sim): players no longer flicker between standing, a step and a backpedal for a split second. A man creeping an inch at a time now just stands there (still calling for the ball, pointing or with his hands on his hips), and he only backpedals when he is really turned around.",
	},
	{
		n: 622,
		date: "2026-11-25",
		text: "Standings: games back is never negative anymore. Teams are still ranked by winning percentage, like the NBA, so an 18-5 team sits above a 20-6 team. But the 20-6 team is half a game ahead, so it now gets the dash and the 18-5 team shows 0.5 back. Fixed on the Daily Schedule, the Standings page, the dashboard, team pages and the live game's standings.",
	},
	{
		n: 621,
		date: "2026-11-25",
		text: "Month calendars: on a phone you can swipe left or right to go to the next or previous month, with the month sliding along under your finger. The arrows still work too.",
	},
	{
		n: 620,
		date: "2026-11-25",
		text: "NBA Calendar: your team's month calendar (dashboard, Schedule, team page) now looks like the scores bar. Each game shows vs or @ and the opponent's logo with the score under it, green for a win and red for a loss, with the national TV network at the top. Home games are no longer filled in your team's color, so they can't be mistaken for wins.",
	},
	{
		n: 619,
		date: "2026-11-25",
		text: "3D game, new arena to try: Global Settings > 3D Arena > New (in progress). The whole building is real 3D, built and lit in Blender: rows, aisles and tunnels, two levels of suites, ribbon boards, the upper deck and the roof. The crowd sits in its seats, stands, waves and claps like before, with empty seats when the gate is small. Off by default while it gets tested on phones. Also, cameras down on the floor no longer see a flat wall of color behind the basket.",
	},
	{
		n: 618,
		date: "2026-11-25",
		text: "3D game: a player with his arms up (dunks, layups, rebounds, blocks, contests, high fives) no longer grows huge wings of jersey from his armpits down his sides. His chest stays its own width with the jersey on it, and his armpit opens up under the raised arm, the way a real body looks reaching up.",
	},
	{
		n: 617,
		date: "2026-11-25",
		text: "Scores bar: a team's name now underlines when you hover or tap it. Tapping the team that lost a played game (or either team in an if-necessary playoff game) now opens that team's roster like it should, instead of the game.",
	},
	{
		n: 616,
		date: "2026-11-25",
		text: "Daily Schedule on phones: each game card now has a player from each team under it, on his team's side. Upcoming games show each team's best player with his points, rebounds and assists a game; finished games show each team's top game score of the night with his points, rebounds and assists in it.",
	},
	{
		n: 615,
		date: "2026-11-25",
		text: "3D game, New sim (Global Settings, 3D Game Sim): a defender running up on a man in his way slows down in time instead of stopping dead on him, keeps up with a man running just ahead of him, and steps aside when someone walks into him instead of being shoved over in a blink.",
	},
	{
		n: 614,
		date: "2026-11-25",
		text: "Links underline again when you hover or tap them: player names everywhere, recap sentences, the ticker, team banners, the league feed and the leaders on Daily Schedule game cards. The date in the top bar is a button now: no underline, just a soft shade on hover and a press when you tap it, and it's no longer cut off at the top and bottom. The yesterday/today arrows at the end of the scores bar no longer go blank on iPhones.",
	},
	{
		n: 613,
		date: "2026-11-25",
		text: "Shared leagues: a game being played live no longer gets stuck as Delayed on everyone else's screen when the person playing it loses their connection. If they get back within a couple of minutes the game carries on; if not, it ends for everyone within a few minutes instead of hanging around. The dot by the date also gets itself back to green when uploads stall, instead of staying red until you refresh.",
	},
	{
		n: 612,
		date: "2026-11-25",
		text: "Daily Schedule: upcoming game cards no longer say Today in the middle, just the line and the over/under. Games on national TV still show the network's logo above them.",
	},
	{
		n: 611,
		date: "2026-11-25",
		text: "Finished games: the line and the over/under are now two columns of the score-by-quarter box, each team's side on its row (+2.5 / -2.5, O 212.5 / U 212.5) with a check on the side that hit. On phones they are two short lines under the box, and the box no longer runs into the team names on narrow phones.",
	},
	{
		n: 610,
		date: "2026-11-25",
		text: "Player pages: All-Time Ranks is now a list like a TV graphic. Pick any stat (every career total, per game numbers and the playoffs, each with his rank on it) to see him with the two players just ahead and the one just behind. Old-time players who only have total rebounds, like Wilt Chamberlain, now count on the rebounding lists here and on Milestone Watch.",
	},
	{
		n: 609,
		date: "2026-11-25",
		text: "3D game: the starting lineups are gone. Before the tip, a matchup board now comes up over the arena while both teams warm up: records and standings, points, points allowed, rebounds, assists, 3PT % and last 10 going in, each team's leading scorer, and the line. It stays up at least 10 seconds, then the game starts from the tip.",
	},
	{
		n: 608,
		date: "2026-11-25",
		text: "Daily Schedule: the month calendar under the date now looks like the strip of days: each day's date with how many games, and your team's little logo in the corner on the days it plays.",
	},
	{
		n: 607,
		date: "2026-11-25",
		text: "Daily Schedule: the upcoming game cards no longer show team overalls in a league with No Visible Team Ratings on.",
	},
	{
		n: 606,
		date: "2026-11-25",
		text: "Daily Schedule: in the strip of days across the top, the days your team plays show its little logo in the corner, instead of the games count in your team's colors.",
	},
	{
		n: 605,
		date: "2026-11-25",
		text: "Daily Schedule: games still to be played get the same cards as played ones, in the teams' colors with each team's record where the score goes, the spread and over/under in the middle, and each team's best player beside it. Watch and Sim sit right on the card, only on games you can sim.",
	},
	{
		n: 604,
		date: "2026-11-25",
		text: "On the NBA calendar, a game in the scores bar turns green when your team won it and red when it lost. Games that were on national TV show the network's logo in the top right corner, and the scores line up now that the arrow by the winner's score is gone.",
	},
	{
		n: 603,
		date: "2026-11-25",
		text: "While a league mate's game is still being played live, no other page gives it away: team records, rank and games back on the roster page, the dashboard and power rankings, team and player stats, league leaders, season highs, injuries picked up in it, head-to-head records and the playoff picture all show how things stood going in, until it's over.",
	},
	{
		n: 602,
		date: "2026-11-25",
		text: "Scores: both teams' records now sit the same way on every game card, under the score on both sides or beside it on both, instead of one jumping up next to a two-digit score.",
	},
	{
		n: 601,
		date: "2026-11-25",
		text: "Player pages: the all-time milestones moved to the bottom as All-Time Ranks, shown for everyone who has played, retired players too: where they stand on the career lists for points, rebounds, assists, steals, blocks and 3-pointers. Tap one to open Player Stats' career totals sorted by that stat, the same as Full list on Milestone Watch.",
	},
	{
		n: 600,
		date: "2026-11-25",
		text: "3D game: with two minutes left in the 4th, a team or network score bug's timeouts drop to the two a team can still use and count down from there as they're called. In overtime it shows each team's two.",
	},
	{
		n: 599,
		date: "2026-11-25",
		text: "On the NBA calendar, the scores bar's games look like a TV score strip: the date and the W or L on top, then both teams on their own rows with logo and score, the winner marked. A game being played live shows the quarter and clock up top, and the next game shows the favorite's spread.",
	},
	{
		n: 598,
		date: "2026-11-25",
		text: "In leagues on the NBA calendar, trades, signings and releases now say the day they happened: on Trade Summary, Transactions, the News Feed, player pages, Free Agents and the best and worst trades. Moves made before this update, and anything in the offseason, still show the season.",
	},
	{
		n: 597,
		date: "2026-11-25",
		text: "3D game, New sim (Global Settings, 3D Game Sim): a defender going for a foul, a steal or a block sets off in time, turning back sooner if he was headed the other way, instead of being yanked onto the spot at the last moment. A loose ball is grabbed on the move instead of with a dead stop.",
	},
	{
		n: 596,
		date: "2026-11-25",
		text: "A finished game's recap opens with its headline and first paragraph on desktop too, like on phones. Read more shows the rest.",
	},
	{
		n: 595,
		date: "2026-11-25",
		text: "A finished game's score banner is laid out more evenly: FINAL sits just over the scores, and the score by quarter with the line and over/under sits just under them. In a window narrower than a full desktop, each team's leaders show under the banner, and the home side is no longer cut off on tablets.",
	},
	{
		n: 594,
		date: "2026-11-25",
		text: "3D game, New sim (Global Settings, 3D Game Sim): threes are left wide open about as often as real ones are as the shot goes up, about one in three, down from about four in ten. More of them now have a defender a step late, closing out.",
	},
	{
		n: 593,
		date: "2026-11-25",
		text: "3D game: the bowl looks like a real NBA arena's. Two rings of suites, lit warm inside, wrap the building above the lower bowl with the ribbon board between them, the score now runs along the middle of the ribbon, and the tunnels into the lower bowl show the lit concourse behind them instead of black holes.",
	},
	{
		n: 592,
		date: "2026-11-25",
		text: "3D game: the baskets look like the NBA's now. The stanchion is padded in the home team's color, with a column up the front leaning back to the boom and a lit sign along the top of it, and the shot clock on the backboard is the league's new round one, with the game clock in amber over the shot clock in red.",
	},
	{
		n: 591,
		date: "2026-11-25",
		text: "3D game: the photographers behind each baseline work from their own boxes, lined on the floor either side of the basket, like at a real NBA game.",
	},
	{
		n: 590,
		date: "2026-11-25",
		text: "3D game, New sim (Global Settings, 3D Game Sim): closeouts look like real ones. The defender is still coming at the shooter as the shot goes up, at a pace drawn from how real defenders close out, and carries on by him after it instead of stopping on his spot and hopping straight up. A defender already on his way flies on through it, and one who was set on his spot starts from a step further off.",
	},
	{
		n: 589,
		date: "2026-11-25",
		text: "3D game: the far sideline is laid out like a real NBA arena. The scorer's table is longer, with courtside seats between it and each bench, and the benches sit further toward the baselines, with black padded chairs and the drinks coolers in the row behind. Coaches pace and huddle in front of their own bench, inside the coaching box.",
	},
	{
		n: 588,
		date: "2026-11-25",
		text: "Season Summary has a Season in review button for every finished season. Tap through the year like a story: the champions, the Finals MVP and MVP, the other awards, the league leaders, your own season and the biggest careers that ended.",
	},
	{
		n: 587,
		date: "2026-11-25",
		text: "Trade Summary now shows a trade tree when something from the deal was traded again: what each team got, what it later turned those players and picks into, and so on, down to where each one ended up.",
	},
	{
		n: 586,
		date: "2026-11-25",
		text: "Smart CPU GMs judge young players more carefully, in the draft and in trades and signings. Lottery picks go more often to the prospects most likely to pan out. The player the draft board ranks first can still fall a long way when his potential is mostly luck.",
	},
	{
		n: 585,
		date: "2026-11-25",
		text: "3D game, New sim (Global Settings, 3D Game Sim): rebounders go up for the ball on the move, still coming as it gets to their hands, instead of getting to the spot and waiting for it to drop to them. How fast each one is going is drawn from how real rebounders move. Also fixed a one-frame hitch when the ball changed hands.",
	},
	{
		n: 584,
		date: "2026-11-25",
		text: "3D game: the scorer's table looks like it does on TV now. Officials and media sit in two rows behind it, the back row up at its own counter in the home team's color, with monitors, laptops, papers and water bottles on the table and a black curtain behind them.",
	},
	{
		n: 583,
		date: "2026-11-25",
		text: "3D game: the NBC and Peacock score bug is about a third bigger on phones, so the records, the clock and the network line are easier to read. It was the smallest score bug on a phone. Desktop is unchanged.",
	},
	{
		n: 582,
		date: "2026-11-25",
		text: "A finished game's score banner now shows the line and the over/under under the score by quarter, with how each went: who covered, and over or under. On phones the game's recap opens with its headline and first paragraph, and Read more shows the rest.",
	},
	{
		n: 581,
		date: "2026-11-25",
		text: "Draft History's Redraft column now shows how far second-round picks rose or fell too, counted from where they went overall.",
	},
	{
		n: 580,
		date: "2026-11-24",
		text: "Fixed the 3D game crashing on iPhones during the starting lineups. Players shown up close are drawn a little coarser now, so they no longer use up the phone's memory.",
	},
	{
		n: 579,
		date: "2026-11-24",
		text: "Draft History's Redraft column now goes by more than career win shares. A player's best five seasons count twice, so stars outrank long careers off the bench, and anyone still playing also gets credit for the seasons he's likely to have left, from his rating and age.",
	},
	{
		n: 578,
		date: "2026-11-24",
		text: "Smart CPU: CPU teams now look for trades with each other twice a week, going into every Sunday and Thursday, and every night for the 10 days before the trade deadline. It used to be about every 10 days. Simming up to the deadline takes a little longer.",
	},
	{
		n: 577,
		date: "2026-11-24",
		text: "3D game, New sim (Global Settings, 3D Game Sim): defenders close out like real ones. The man contesting a jumper is still coming at the shooter as the shot goes up, carries on in the air and comes down going, past him or on to the boards, instead of getting there early and standing still. How open each shot is hasn't changed. Pull-up jumpers off the dribble are taken in stride too, instead of from a dead stop.",
	},
	{
		n: 576,
		date: "2026-11-24",
		text: "Draft History no longer has the Redraft card. The Redraft column stays, and players with no redraft pick now stay at the bottom whichever way you sort it.",
	},
	{
		n: 575,
		date: "2026-11-24",
		text: "Watching a game in 3D, a pregame show plays while the game loads. The lights go down and the building opens with fire, colored lights or a spotlight on the logo, then the starters are called out, each home starter up close as he slaps hands down the line. Cards show the matchup, the starting lineups and the key matchup before the lights come up for the tip. Skip it once the game is ready.",
	},
	{
		n: 574,
		date: "2026-11-24",
		text: "Any two teams have a page of their own: who leads the all-time regular season series, every time they met in the playoffs, the record season by season, their totals side by side and their latest games. Tap a team on a Head-to-Head page, or All-time on a playoff series page.",
	},
	{
		n: 573,
		date: "2026-11-24",
		text: "Draft History redrafts every class that has played at least two seasons, by career win shares: the first round done again, with each pick's team, who it would take now and where that player really went, plus a Redraft column in the table.",
	},
	{
		n: 572,
		date: "2026-11-24",
		text: "NBA calendar: the separate Calendar page is gone, since a team's Schedule page has the same month calendar. The menu and team pages no longer list it, and the dashboard's Full Calendar link and any old Calendar links now open Schedule.",
	},
	{
		n: 571,
		date: "2026-11-24",
		text: "New page, Stats > Milestone Watch: the active players climbing the league's all-time lists in points, rebounds, assists, steals, blocks and 3-pointers and how many it takes to pass the next man up, the ones a few games from a round number like 20,000 points, and anyone on pace for one of the best seasons the league has seen. A player's page shows his part of it.",
	},
	{
		n: 570,
		date: "2026-11-24",
		text: "Game and night recaps repeat themselves less. Their most common lines now come in several wordings. The race only brings up a magic number or a cut line on a night that moved it, so a team moving into or out of a playoff spot is never crowded out, and Worth watching says how far ahead a conference leader is.",
	},
	{
		n: 569,
		date: "2026-11-24",
		text: "The game-by-game strip on Schedule pages is now Game Results: green bars up for wins and red bars down for losses, as tall as the margin, with gray blocks for the games still to play. With the NBA Calendar on, every date gets a slot and the games are grouped by month. Hover a bar for the game, tap it for the box score.",
	},
	{
		n: 568,
		date: "2026-11-24",
		text: "The night's recap on the Daily Schedule is built out. Under the headline is a line of the night's other stories, and each game in Around the league says what the result meant: a streak, first place, a clinch, the end of a road trip, who got hurt. New sections follow, each only on a night that has one: By the numbers (like a 20-1 closing run or 12-of-13 shooting), Streaks, The race (clinches, the top of each conference, who moved into or out of the playoff and play-in spots, how close the cut lines are, magic numbers late in the season), Milestones and firsts, Moves (a traded or signed player's first game for his new team, or against his old one) and Worth watching (the next day's best game and why). Game recaps no longer give career totals for players whose earlier seasons came before the league began.",
	},
	{
		n: 567,
		date: "2026-11-24",
		text: "A team's Schedule page opens with its season game by game: a bar for every game, up for a win and down for a loss, as tall as the margin, in the team's color. The longest winning streak is shaded and the playoffs follow a line. Tap a bar for its box score.",
	},
	{
		n: 566,
		date: "2026-11-24",
		text: "Every playoff series has its own page. Tap the games won on the Playoffs bracket, or the series score on a box score: it shows where the series stands, every game with its recap and the games still to come, the series leaders, both teams' stats side by side, their regular season meetings and every other time they met in the playoffs.",
	},
	{
		n: 565,
		date: "2026-11-24",
		text: "3D game, New sim (Global Settings, 3D Game Sim): players catch and throw passes on the move. A man running to a pass no longer pulls up and waits on his spot for it. He catches it in stride and gathers into his shot, his dribble or his next pass, and the man with the ball passes off the dribble instead of stopping first.",
	},
	{
		n: 564,
		date: "2026-11-24",
		text: 'Game recaps know players\' careers and the race now. They call a career high one ("a career-high 34 points"), note a first career triple-double or 30-point game, a rookie\'s debut and where he was drafted, a player\'s first game for a new team or against the team that traded him. They say where a team is on a road trip or homestand ("finished their five-game trip 3-2"), and when a team clinches a playoff spot, the best record, a play-in spot, gets eliminated, or becomes the first team to 50 wins. The Key stat can be offensive rebounds too.',
	},
	{
		n: 563,
		date: "2026-11-24",
		text: 'Game notifications now start the title with the date, like "11/12/25 - Lakers 110 vs. Celtics 86", instead of putting it on its own line under the title.',
	},
	{
		n: 562,
		date: "2026-11-24",
		text: 'Game recaps read like a wire story now. They open with the home city ("DENVER — ") and end with Takeaways (one line on what the game means for each team, like a streak, a season high or where they sit in the standings), the Key moment, the Key stat, and Up next (who each team plays next, with the day of the week when the league uses the NBA calendar). On the Daily Schedule and every box score.',
	},
	{
		n: 561,
		date: "2026-11-24",
		text: "NBA calendar: every date is a day now. A date with no games anywhere, like Thanksgiving, Christmas Eve, the All-Star break or the days between playoff games, no longer gets skipped. One day takes you to it, it's today in the header and on the Daily Schedule, injuries and trade waits count down that day, and teams can make moves. It starts with each league's next season.",
	},
	{
		n: 560,
		date: "2026-11-24",
		text: 'Game recaps tell runs the way broadcasts count them, both numbers and who carried them ("a 31-6 run in the second quarter, 12 of the points from Devin Nash"), say how a close game got away late ("The Knicks were within a point with 4:51 left before the Nuggets closed on a 20-1 run"), and point out when a team went the last five minutes without a field goal. The same goes for the night recap on the Daily Schedule.',
	},
	{
		n: 559,
		date: "2026-11-24",
		text: "3D games: uniforms aren't skin tight anymore. Jerseys hang loose from the chest and shoulders down to the shorts, the way real ones do, instead of clinging to every muscle, and shorts are roomier again.",
	},
	{
		n: 558,
		date: "2026-11-24",
		text: 'Trading Block: while CPU teams think over your offer, the button shows how far along it is, like "Processing 42%", with a bar filling along its bottom edge.',
	},
	{
		n: 557,
		date: "2026-11-24",
		text: "3D games: jerseys fit like real ones. They cover the whole chest out to the shoulders, with proper straps and trim round the armholes, instead of hanging like a loose tank top. Shorts are less baggy, and the short shorts of the 70s and 80s have a clean hem instead of a ragged one.",
	},
	{
		n: 556,
		date: "2026-11-24",
		text: "3D games: players can be drawn on your device's graphics chip, taking work off the game so it runs smoother. Each device checks for itself which way is quicker and uses that. And an arm raised in front of a player's face is now drawn over his head instead of behind it.",
	},
	{
		n: 555,
		date: "2026-11-24",
		text: "Shared leagues: tapping something just as a league mate's change is arriving (a press conference's play button, a signing, a roster move) no longer says \"Still catching up to the cloud\". The press conference plays right away, and anything that changes the league waits a moment for your device to catch up, then goes through.",
	},
	{
		n: 554,
		date: "2026-11-24",
		text: "3D game settings (the gear) are tidier. In a game on the teams' own local broadcasts, you can pick which team's broadcast to watch, and its score bug, right there. The list of play types to put in Key Plays is gone: every kind of big play is always in the reel.",
	},
	{
		n: 553,
		date: "2026-11-24",
		text: "3D games: players' shorts are as long as they were worn in the season being played. Mid-thigh in the 1940s to 60s, short and tight from the 70s through the late 80s, longer through the 90s, baggy down to the knee in the 2000s, and today's length since. Uniform pictures stretch or shrink to fit.",
	},
	{
		n: 552,
		date: "2026-11-24",
		text: "3D games: players' heads sit on their necks instead of looking stuck on the end of a thin stalk, and defenders off the ball keep their hands loose at their sides instead of holding them out in front of them.",
	},
	{
		n: 551,
		date: "2026-11-24",
		text: "3D Key Plays: the reel always shows the last few seconds of the game up to the final horn, even when a team is just dribbling it out, instead of cutting from the last big play straight to the final.",
	},
	{
		n: 550,
		date: "2026-11-24",
		text: "Team schedule: the players' stat lines on upcoming games no longer get cut off when Upcoming and Completed Games sit side by side. The team names make the room instead, and a national TV logo stays at the end of the team names, clear of the players.",
	},
	{
		n: 549,
		date: "2026-11-24",
		text: "When a league mate plays a game live, your header keeps showing the season and the score bar and ticker keep going, instead of \"Live game in progress\". That's only shown to whoever is playing the game or watching it. Leaving a league mate's game you're watching no longer warns that you'll lose the play-by-play; leaving your own live game still does.",
	},
	{
		n: 548,
		date: "2026-11-24",
		text: "3D games: each player's picture takes about a quarter less work to make, pixel for pixel the same, so the background helpers keep up with more of them.",
	},
	{
		n: 547,
		date: "2026-11-24",
		text: "Live games and replays: the buttons over the 3D court (pause, skip, speed, settings) now stay out of the way like the Nightly Recap's, and show when you hover over or tap the court. A new full size button beside them fills the screen with the game.",
	},
	{
		n: 546,
		date: "2026-11-24",
		text: "Score bugs from a team looks file, and the National TV bug, now look the same on a phone as on a big screen. On small screens the scores and other lettering could sit a little high, and thin lines like the ring around a logo came out too thick.",
	},
	{
		n: 545,
		date: "2026-11-24",
		text: "3D games, a step toward a smooth 60 fps: the background helpers that draw the players now hand back each player's picture finished, head and all, so the page itself spends about a tenth of the time it did on them each frame. Players look the same.",
	},
	{
		n: 544,
		date: "2026-11-24",
		text: "3D games: an and-one on a shot from way out at the buzzer now shows the foul. The man who fouled him is right on him as he lets it go, swiping at his arm, instead of nowhere near him.",
	},
	{
		n: 543,
		date: "2026-11-24",
		text: "Shared leagues: when the auto-play scheduler or a league mate sims a day, your screen now keeps up without a reload. The date at the top, the Play menu, today on the scores bar, the ticker's next games, the calendar on a team's page and the Auto Play schedule all move to the new day.",
	},
	{
		n: 542,
		date: "2026-11-24",
		text: 'No more "Watch live?" popup when a league mate starts a live game. It shows up in the header\'s live scores like any other, and you tap it there to watch.',
	},
	{
		n: 541,
		date: "2026-11-24",
		text: "Daily Schedule: the Nightly Recap video now sits at the top of the right-hand column, above Top Performers, so the night's story and the scores move up. On a phone it's still first in the Recap tab.",
	},
	{
		n: 540,
		date: "2026-11-24",
		text: 'NBA calendar: the month calendar no longer labels back-to-backs with "B2B".',
	},
	{
		n: 539,
		date: "2026-11-24",
		text: "The rest of BBGM's latest updates: releasing a player now gives you an Undo button, exhibition games can heal injuries first, Back from Customize Settings keeps what you entered for a new league, the graph pages remember your picks, and God Mode's Danger Zone can randomize every contract. Starting and stopping sims is also more reliable.",
	},
	{
		n: 538,
		date: "2026-11-24",
		text: "Daily Schedule: a national TV game's network logo now sits in the same spot on every upcoming game, at the end of the team names next to Watch/Sim, instead of floating mid-card or running into the buttons.",
	},
	{
		n: 537,
		date: "2026-11-24",
		text: "National TV: a game you watch live is on the same network the Daily Schedule showed for it. Watching your game before the rest of its night was played could put it on national TV by mistake.",
	},
	{
		n: 536,
		date: "2026-11-24",
		text: "Sportsbook: preseason win totals are no longer too far apart. Before the schedule comes out, the best teams' lines ran up to 3 wins too high and the worst teams' up to 3 too low, so a favourite's over hardly ever hit. They now match the lines opening night shows.",
	},
	{
		n: 535,
		date: "2026-11-24",
		text: "Box score banner (refreshed look): the city name now links to the team page like the logo and team name. A game that was on national TV shows the network's logo next to FINAL.",
	},
	{
		n: 534,
		date: "2026-11-24",
		text: "NBA calendar: the scores bar starts on opening night, even when your team was off that night.",
	},
	{
		n: 533,
		date: "2026-11-24",
		text: "3D game: the court floor is drawn at twice the detail, so wood planks, paint and floor pictures look sharper. Real players leagues: Real Team Info (Global Settings) can now also set each team's 3D jerseys, court and score bug season by season, put on automatically each new season.",
	},
	{
		n: 532,
		date: "2026-11-24",
		text: "Team colors show again in shared leagues. Opening a league while it was still reconnecting left them off for the whole visit, so home games on the scores bar were black. The scores bar's days and arrows also line up now, with no stray line under them.",
	},
	{
		n: 531,
		date: "2026-11-24",
		text: "National TV score bug: the scores, team names, clock and records now sit right in the middle of their bars.",
	},
	{
		n: 530,
		date: "2026-11-24",
		text: "Fixes from BBGM: dropdown menus are readable again in dark mode on the newest Chrome, pausing and playing a live game can no longer count its result twice in the standings, the top bar shows your next game right after the trade deadline, and undoing a trade gives players back their playing time settings.",
	},
	{
		n: 529,
		date: "2026-11-24",
		text: "3D game: smoother on most phones and computers. The players' new poses are drawn in the background again instead of holding up each frame, which devices with a graphics card had been doing for every player.",
	},
	{
		n: 528,
		date: "2026-11-24",
		text: "3D Game Sim (Global Settings): players look more natural. Defenders away from the ball stand and walk upright instead of crouching in a stance, and the man guarding the ball sits in a lighter stance. Running and walking strides fit the players' legs now, so no more wide, spidery splits.",
	},
	{
		n: 527,
		date: "2026-11-24",
		text: "NBA calendar: the scores bar at the top no longer gets its bottom row cut off when a scrollbar shows.",
	},
	{
		n: 526,
		date: "2026-11-24",
		text: "NBA calendar: on a team's calendar, every day now opens that day's Daily Schedule, not just the days the team plays.",
	},
	{
		n: 525,
		date: "2026-11-24",
		text: "Player names no longer underline when you hover them, anywhere: tables, Daily Schedule game cards, top performers, the news feed and recaps (a recap sentence gets a faint shade instead). A player's face now opens the player page too, wherever it sits next to the name.",
	},
	{
		n: 524,
		date: "2026-11-24",
		text: "Daily Schedule on a phone: the Scores tab now comes first, before Recap and Standings. Live game: the line, total and season series above the court no longer run over the team names. When they don't fit beside the names, they drop to a line underneath.",
	},
	{
		n: 523,
		date: "2026-11-24",
		text: "NBA calendar: the defending champion now always opens the season at home on opening night. To fit it in, a couple of nearby games can trade nights, and nobody ends up with a tougher stretch than before.",
	},
	{
		n: 522,
		date: "2026-11-24",
		text: "NBA calendar: checked across dozens of generated seasons. Opening night and Christmas matchups no longer add any three-in-threes, cross-country back-to-backs or extra travel to anyone's schedule, and the defending champion now hosts opening night far more often.",
	},
	{
		n: 521,
		date: "2026-11-24",
		text: "Team page (refreshed look), Schedule section: on the NBA calendar it's now the team's month calendar, each game on its date, instead of numbered games.",
	},
	{
		n: 520,
		date: "2026-11-24",
		text: "NBA calendar: the Play Through Injuries sliders now go a day at a time (1 day, 2 days, 3 days...). Performance is prorated by days left, so a player plays exactly as well as he would with the same time left in a league off the calendar.",
	},
	{
		n: 519,
		date: "2026-11-24",
		text: "NBA calendar: national TV now follows the NBA's real TV schedule. Tuesdays and Wednesdays are doubleheaders, Thursdays get games from mid-January, Fridays double up from January, and weekend games start in January and February. Every game on opening night and Christmas is national, MLK Day has four, and Rivals Week (the week after MLK Day) features division rivals. Starting next season, the champion hosts opening night, and the biggest matchups land on opening night and Christmas. The calendar marks all these nights.",
	},
	{
		n: 518,
		date: "2026-11-24",
		text: "NBA calendar month view: the arrows now sit on either side of the month, and the month itself is a menu to jump to any month of the season.",
	},
	{
		n: 517,
		date: "2026-11-24",
		text: "3D game, New sim (Global Settings, 3D Game Sim): the man with the ball no longer stands on his spot. On the dribble he works his defender, going side to side, attacking him a few dribbles and backing out, giving ground when crowded and sizing him up, all within a few steps of where the play has him. Once he picks it up, his feet stay put: no walking with the ball.",
	},
	{
		n: 516,
		date: "2026-11-24",
		text: "NBA calendar scores bar and month calendar: tap a game's team logo or abbreviation to open that team's roster (the rest of the box still opens the game). In the scores bar, off days are now slim columns, and scores no longer get cut off on phones.",
	},
	{
		n: 515,
		date: "2026-11-24",
		text: "Nightly Recap is now just the night's games, one after another (no Player of the Night, top plays or standings after them). A night's recap also waits until the whole day is over: simming one or a few of a day's games no longer brings it up.",
	},
	{
		n: 514,
		date: "2026-11-24",
		text: "Daily Schedule's row of days: each day shows how many games it has, on your team's color when your team plays.",
	},
	{
		n: 513,
		date: "2026-11-24",
		text: "NBA calendar scores bar: hovering or tapping a day keeps its color and no longer underlines everything, it just lights up a little.",
	},
	{
		n: 512,
		date: "2026-11-24",
		text: 'Shared leagues: no more "Reconnecting to the shared league" pop-up. While your device reconnects, the header still shows it.',
	},
	{
		n: 511,
		date: "2026-11-24",
		text: "3D game, New sim (Global Settings, 3D Game Sim): players off the ball no longer stand on their spots waiting for the play. Each works his spot, walking it and jogging to a new one, gets out of the way of a drive or slides to where the kick-out would find him, and after a pass cuts to the rim or relocates instead of standing to watch.",
	},
	{
		n: 510,
		date: "2026-11-24",
		text: "Sportsbook, checked against hundreds of simulated seasons and fixed where it missed: spreads price the margins games really end on (some now come at -120 or +100), opening-night and playoff totals are no longer 6 points off, futures and award odds are sharper (the MVP and rookie-of-the-year leaders were priced far too long), and a bet on a game still being shown live stays open until it ends there.",
	},
	{
		n: 509,
		date: "2026-11-24",
		text: 'Shared leagues: the "Reconnecting to the shared league" warning shows once and fades, instead of stacking up several at a time.',
	},
	{
		n: 508,
		date: "2026-11-24",
		text: "NBA calendar leagues: Pause Sim On Days has one-tap Dec 15, Jan 15 and Trade deadline, and a month and day picker for any other date.",
	},
	{
		n: 507,
		date: "2026-11-24",
		text: "Shared leagues: when your device isn't in charge of simming and you try something only that device can do, it now tells you who is, instead of doing nothing. The Danger Zone's Smart CPU button also offers Sim here, and in the preseason it now runs a last round of free-agent signings before the trades.",
	},
	{
		n: 506,
		date: "2026-11-24",
		text: "NBA calendar leagues: Pause Sim On Days now takes dates, like Dec 15. Each one pauses the sim every season, before the first night with games on or after it.",
	},
	{
		n: 505,
		date: "2026-11-24",
		text: 'Player pages now show, in small print under the contract, when a recently signed player can be traded, like "Tradable Dec 15".',
	},
	{
		n: 504,
		date: "2026-11-24",
		text: "NBA calendar leagues: every re-signed player can't be traded until January 15, this summer's included. Free agents signed in the summer still open up December 15.",
	},
	{
		n: 503,
		date: "2026-11-24",
		text: "NBA calendar: the trade deadline now falls when the NBA's does, on the Thursday about 10 days before the All-Star Game (early February). It starts with next season's schedule. A league with its own Trade Deadline setting keeps it.",
	},
	{
		n: 502,
		date: "2026-11-24",
		text: "Smart CPU GMs: a CPU team now passes on a trade if a clearly better one will open up once recently signed players can be traded. CPU teams also never trade a player they just signed, even with God Mode on.",
	},
	{
		n: 501,
		date: "2026-11-24",
		text: "NBA calendar leagues: a player who signs now can't be traded until the NBA's real dates. That's December 15 for summer signings (it was 14 games, around mid-November), January 15 for every re-signing, and three months after signing for anyone signed in the preseason or during the season. This summer's signings move to those dates when the season starts. Classic leagues keep 14 games.",
	},
	{
		n: 500,
		date: "2026-11-24",
		text: "Smart CPU: the Danger Zone (God Mode) has a button that runs the CPU teams' trade market now, while re-signing players or in the preseason. It's for a league that turned Smart CPU on after that market would have met. Only the person in charge of simming can run it.",
	},
	{
		n: 499,
		date: "2026-11-24",
		text: "3D game, New sim: players' feet stay planted through shots, jumps, screens and jab steps too. A shooter steps into his shot and leaves the floor from where his feet are, comes down on the feet his jump puts down, and a jab step is a real step out and back.",
	},
	{
		n: 498,
		date: "2026-11-23",
		text: "Nightly Recap: smoother into each game. The GAME RECAP card no longer stutters as it comes up; the 3D court is set up behind it once it's still, and the arena is painted once instead of twice.",
	},
	{
		n: 497,
		date: "2026-11-23",
		text: "NBA calendar month view: home games are a plain fill in your team's color (no stripe on top), and the small calendar on the dashboard now shows each game's score.",
	},
	{
		n: 496,
		date: "2026-11-23",
		text: "Daily Schedule's row of days (NBA calendar): swipe it to move a week at a time, or use the arrows on a computer. The night you're looking at is filled in, and tonight has a red dot.",
	},
	{
		n: 495,
		date: "2026-11-23",
		text: "NBA calendar month view, redesigned (Schedule, Calendar, dashboard): every night is a tile, home games filled in your team's colors, road games a plain card, each with the opponent's logo, the result and the score. National TV games still to come show the network, and the months are tabs with your record in each.",
	},
	{
		n: 494,
		date: "2026-11-23",
		text: "NBA calendar scores bar: it now works like the regular one. It opens on its right end with your next game, runs through that game instead of the days after it, and has the same yesterday and today buttons on the right, which stay put while you scroll back.",
	},
	{
		n: 493,
		date: "2026-11-23",
		text: "3D game, New sim (Global Settings, 3D Game Sim): players' feet now plant on the floor and stay put instead of sliding. Pulling up takes braking steps out in front, a hard cut plants the outside foot, and defenders slide with their feet apart instead of skating.",
	},
	{
		n: 492,
		date: "2026-11-23",
		text: 'NBA calendar: injuries count in days. Players show "Out 19 days" (roster, Injuries page, box scores, news, injury history), going down by one every day, off days included, and they still miss as many games as they would without the calendar. Play Through Injuries and the wait before a newly signed player can be traded are in days too.',
	},
	{
		n: 491,
		date: "2026-11-23",
		text: 'Live games: the top of the page now shows the matchup instead of "Live Game Simulation": both teams with their records coming into the game (their seeds in the playoffs), the spread, the over/under from the sportsbook, and the season series or the playoff series. A game on national TV shows the network\'s logo. The Replay label is gone.',
	},
	{
		n: 490,
		date: "2026-11-23",
		text: "Smart CPU GMs, trading with people: on top of the trade difficulty, a CPU team now also turns down any trade it would lose by its own read, wherever it sits in the standings. Before, only contenders and rebuilding teams said no, so a team in the middle could still give a good player away for less. What would make this work, the trading block and trade proposals now come back with deals the team would actually take. On an easy trade difficulty nothing changes.",
	},
	{
		n: 489,
		date: "2026-11-23",
		text: "Press conferences: when a team trades for or signs a player with 50+ potential, it now holds a 3D press conference for him. He stands in front of the team's backdrop in his own clothes holding up his new jersey (name and number on the back), with the GM and coach (sometimes the owner) beside him and photographers flashing, then they talk at a table or a podium. Each one is different and plays the same every time. Watch it with the play button on the trade page, in the news feed, and in player and team transactions.",
	},
	{
		n: 488,
		date: "2026-11-23",
		text: "League hub fixes: on the live site the home team's side of each score card now mirrors the away side like it should, and the phone tabs keep their order. In the playoffs, the series (Game 7, DAL wins 4-3) hangs on a small tag at the top of the card, and Final with the line sits below it. The week strip's days no longer underline when tapped.",
	},
	{
		n: 487,
		date: "2026-11-23",
		text: "Nightly Recap: the newest night's recap now leads the League Headlines on the new dashboard and starts playing on its own once you scroll to it. Tap it to watch it full size (turn your phone sideways for the biggest picture). Every recap now has a full size button too.",
	},
	{
		n: 486,
		date: "2026-11-23",
		text: "Nightly Recap: after the games now come the Player of the Night (his best plays, with his name and line along the bottom; Full and Medium), the night's top plays counted down from 10 (5 on Medium, 3 on Short), each under its own game's score bug, and the standings through that night, each conference split into the playoff spots, the play-in and the rest.",
	},
	{
		n: 485,
		date: "2026-11-23",
		text: "Live games, more safety checks: while the last game of the Finals is being played live, nobody can move on to the draft until its final. A live game left behind by a page that closed without saying so can no longer hold up the playoffs. While you're playing your own game live, the other live scores in the header can't be tapped, since leaving your game would end it for everyone. And the standings on Daily Schedule no longer count a game still being played.",
	},
	{
		n: 484,
		date: "2026-11-23",
		text: "NBA calendar leagues: the date in the header is now a calendar page with the weekday on it (WED 12 NOV), next to the season and phase (2025-26, Regular season). Tap the calendar for the Daily Schedule, or the words for your roster (the bracket in the playoffs). In the scores bar, home games are filled in your team's color instead of black, and a game still to come is centered in its box.",
	},
	{
		n: 483,
		date: "2026-11-23",
		text: "Nightly Recap: smarter picks of each game's plays, for games played from this update on. A close game's recap is built around the plays that swung it most at the end, and a blowout's around the best plays from while it was still a game, never garbage time. Dunks now count over layups, the offense comes first (a block now and then, a steal only when it turned the game), and no one player fills the whole recap. One play a game is the game-winner if there was one, else the shot that swung a tight finish most, else the MVP's best play.",
	},
	{
		n: 482,
		date: "2026-11-23",
		text: "Score bugs: a league with no national networks from a team looks file now gets our own National TV, with its logo on the national games' previews and its own score bug in the 3D game. Team score bugs in the 3D game now show all seven timeouts the way TV does. Loading a team looks file on one team's score bug or court page now tells you to import it in Manage Teams instead.",
	},
	{
		n: 481,
		date: "2026-11-23",
		text: "Daily Schedule is now a league hub, like ESPN's or NBA.com's. A week strip runs across the top. The night's recap leads with the league's biggest stories, under the Nightly Recap video. Each final is a card in both teams' colors, with the records, the pregame line, the recap and each team's two best players. Beside it are top performers, standings, injuries and the next night's games with records and lines. On a phone, it's in Recap, Scores and Standings tabs.",
	},
	{
		n: 480,
		date: "2026-11-23",
		text: "Live games: the League Hub's scores, your team's results and news, the Nightly Recap video and player game logs now also wait for a live game's final. If the screen playing a game goes quiet (phone locked, app closed), the header says Delayed, and after 3 minutes the game is final for everyone and stays final even if they pick it back up.",
	},
	{
		n: 479,
		date: "2026-11-23",
		text: "NBA calendar leagues: the League Feed, profiles and quoted posts now show the date (Tue, Nov 11) instead of a day number, and so do the sim's pause stops: the ready-up says Play Jan 15 and auto play says it stopped after Jan 15.",
	},
	{
		n: 478,
		date: "2026-11-23",
		text: "Smart CPU GMs: a CPU team now signs a new free agent only when it is clearly better off with him, the same rule it already uses for trades. Bad teams no longer hand big multi-year deals to low-rated players (like $19.7M a year to a 49) and keep their cap room open instead. Good free agents still sign.",
	},
	{
		n: 477,
		date: "2026-11-23",
		text: "Live games in the header now look like a real scoreboard: each game shows both teams with their logos and scores, and the quarter and clock (Half, End Q3 or Paused on a break). Tap a game to watch it.",
	},
	{
		n: 476,
		date: "2026-11-23",
		text: "3D game: the board over center court is now a real jumbotron. It has video screens on all four sides, a ring round the top with both teams, the score, the period and the clock, and a lower ring that runs with the ribbon boards. Each arena has its own shape: a cube with cut corners, a big halo over the screens, or a long curved board. Over breaks the screens show the score, the home team's logo, crowd prompts and the sponsors.",
	},
	{
		n: 475,
		date: "2026-11-23",
		text: "Phone notifications now say which day they're about: the first line under the headline is the date (Wed, Nov 12), or Nov 14–20 for a week. Leagues without the NBA calendar show the day number (Day 34).",
	},
	{
		n: 474,
		date: "2026-11-23",
		text: "NBA calendar leagues: the scores bar is now your team's calendar in one row. Every day has a box, off days too, with its weekday and date on top (SAT 11/15). A game day shows the opponent (@ on the road) and a green W or red L with the score, home days are filled black like on the team calendar, and today is outlined in red. Arrows page through it on a computer, and All scores at the end opens the Daily Schedule.",
	},
	{
		n: 473,
		date: "2026-11-23",
		text: "3D game, New sim (Global Settings, 3D Game Sim): after a miss or a turnover, defenders now run back and pick up their man on the way, the way NBA defenses do, instead of all dropping into the paint. On a fast break the engine scored, the play still has them beaten.",
	},
	{
		n: 472,
		date: "2026-11-23",
		text: "Draft night: players now take hearing their name like the real thing. Some smile, nod and button their jacket, some bury their face in their hands or wipe away tears, some throw their arms up, pump a fist and shout, point to the sky or put a hand on their heart, and some just sit there stunned. There are 13 ways to take it in the chair and 16 on their feet, each player always takes it the same way, and the happy ones grin through the hugs. On stage they wave, point, raise a fist, clap or bow with their hands together instead of dropping into a game stance.",
	},
	{
		n: 471,
		date: "2026-11-23",
		text: "3D game, New sim (Global Settings, 3D Game Sim): defenders now guard their own man between plays. On the ball they sit about a step and a half off, tighter on a shooter and looser on a quick driver; off the ball they sag toward the rim and over to the ball's side the way NBA defenders do, more off a man who can't shoot. When a man drives, the nearest defender away from the ball steps up into his way.",
	},
	{
		n: 470,
		date: "2026-11-23",
		text: "Nightly Recap: a new look. Its titles and cards are set in a tall broadcast typeface, the opening title stands on one line over a lit arena floor, and the Game Recap, Final and 2nd Half graphics are flatter and sharper.",
	},
	{
		n: 469,
		date: "2026-11-23",
		text: "National games: the box on the Daily Schedule and the team schedule now shows the network's logo in its own colors next to the teams, instead of the black tab (the scores bar and the dashboard too). National score bugs show the game's real night with the NBA calendar on, and leave the night out with it off.",
	},
	{
		n: 468,
		date: "2026-11-23",
		text: "Nightly Recap: it now opens with a Nightly Recap title, and a 2nd Half graphic (or Overtime) plays between the halves. The controls are on the video itself, like the 3D game's, and the plays can run at 2x (the graphics stay at normal speed). The Final card shows each player's head and shoulders instead of a cut-off face.",
	},
	{
		n: 467,
		date: "2026-11-23",
		text: "Nightly Recap: the Daily Schedule now has a recap video of the night, for games played from this update on. Each game gets a Game Recap card, its biggest plays on the 3D court, and a Final card with the score and three players a side. Pick how long it runs (Full, Medium, Short or One play a game). The shortest still shows each game's biggest play: the game-winner if there was one, otherwise the best play of the game's top player. The last 7 nights can be watched.",
	},
	{
		n: 466,
		date: "2026-11-23",
		text: 'NBA calendar leagues: the playoffs now run on the NBA\'s real schedule. The play-in is Tuesday, Wednesday and Friday, round 1 opens with four series on Saturday and four on Sunday, series play every two or three nights with no back-to-backs, the conference finals alternate nights, and the Finals start on a Thursday. Every game of a series shows on the calendar and in the scores bar once the matchup is set, with "if necessary" on the games that may not be played. One day sims one night, and injuries heal at the same pace as in a classic league.',
	},
	{
		n: 465,
		date: "2026-11-23",
		text: "Live games in shared leagues: several of you can live sim your own games at the same time, and nobody waits on anyone else's. Every game being played live shows at the top of the screen with its score and clock; tap one to watch from where it is. Until the final buzzer on the screen playing it, the game looks in progress everywhere: LIVE with the score on the Daily Schedule, the team schedule and the scores bar, and no final in the box score, standings, playoff bracket or news. In the regular season, once a later day is simmed the result shows for everyone not watching. In the playoffs, the next day waits for every live game to finish.",
	},
	{
		n: 464,
		date: "2026-11-23",
		text: "National broadcasts now show before tipoff: a game on a national network has the network's tag on its box on the Daily Schedule and the team schedule, beside it in the scores bar, and next to the next game on the dashboard. A night's pick can still change as records change, until it's the next night.",
	},
	{
		n: 463,
		date: "2026-11-23",
		text: "NBA calendar leagues: the team calendar has a new look, like a team's own schedule page. Home nights are filled in black, each game has the opponent's logo and name, and played games show a green W or red L with the score (tap a game for its box score). Today is outlined in red.",
	},
	{
		n: 462,
		date: "2026-11-23",
		text: "NBA calendar leagues: the smart CPU GMs' last days of trading before the deadline now cover the same stretch of games, and meet as often, as in a classic league (they had about half as many).",
	},
	{
		n: 461,
		date: "2026-11-23",
		text: "3D game: national broadcasts. A team looks file can now bring national networks' score bugs. Each night's biggest game (the two best teams going in) is on that night's network, and every playoff game is national, with the Finals on its own network. Custom score bugs can also show where a team stands (\"11-3 (2nd IN EAST)\"), swap the records for timeouts or fouls once a side is in the bonus, and show the playoff series.",
	},
	{
		n: 460,
		date: "2026-11-23",
		text: "Box scores: the game's date moved out of the banner into the All games this day bar, which now has a Daily Schedule button to jump to that whole day.",
	},
	{
		n: 459,
		date: "2026-11-23",
		text: "NBA calendar leagues: injuries, trade cooldowns and CPU trades and signings now run at exactly the same pace per game as classic BBGM, so a calendar season is no harder or easier. The month calendar is now on Daily Schedule, your Schedule page and the dashboard, and the scores bar goes night by night: the season so far, today and the next 5 nights, each with its weekday and date.",
	},
	{
		n: 458,
		date: "2026-11-23",
		text: "Draft night: everyone wears proper trousers down to their shoes, with socks to match, instead of shorts. Each prospect comes in his own suit, from dark classics to bold colors, with his own shirt and tie (or none), and it's the same one every time you watch him. His tie is no longer in the colors of the team that drafts him. The commissioner is now bald with thin glasses, in a navy pinstripe suit, pale blue shirt and navy tie. The shot of the commissioner at the podium no longer lags.",
	},
	{
		n: 457,
		date: "2026-11-23",
		text: "Draft night: the family hug at the table is a real hug now. The player and his mom or dad come in close and wrap their arms right around each other, rocking a little, him bending down over his mom (more the taller he is), with back pats in a hug with his dad or brother, then a step back before he lets go. The camera goes in tight over their shoulder on his face, like the broadcast. Photographers' flashes no longer white out the picture.",
	},
	{
		n: 456,
		date: "2026-11-23",
		text: "Smart CPU GM: a team clearing cap room for a free agent now asks for a pick back when the team taking the salary gains plenty from it, and weighs every team with room instead of the first that would take it.",
	},
	{
		n: 455,
		date: "2026-11-23",
		text: "Draft fixes for shared leagues. Once a stretch of picks was running because everyone had readied up, Ready for the next pick and Not ready didn't work until it stopped, so the draft could run on to your pick. Both now work at any time: the pick being made finishes and the draft waits. And Sim one pick makes one pick, even if your team had readied up further.",
	},
	{
		n: 454,
		date: "2026-11-23",
		text: "3D game, New sim (Global Settings, 3D Game Sim): players run with their own stride. Their steps go about as quickly as a real player's of their height at that pace, so going hard they take long strides instead of quick little ones, and a jog lengthens smoothly into a run and a run into a sprint. Each player also gets his own way of running, walking and sitting in his stance, picked from his ratings.",
	},
	{
		n: 453,
		date: "2026-11-23",
		text: "Draft night looks more like the broadcast again. Before each pick is read, the camera is down among the tables looking up at the big board, with the picks coming up and the team on the clock under them, and the lights over the stage glaring into the lens. The arena's ribbon boards glow blue for the draft. After the pictures, the player walks off the back of the stage and the commissioner goes back to the podium, seen from the end of the stage. The live draft room on the Draft page also no longer drops out for a pick when it can't load right away: it keeps trying.",
	},
	{
		n: 452,
		date: "2026-11-23",
		text: "3D game: more of each possession plays at real speed. The ball brought up the floor still runs by fast, but the picture now slows down about five seconds before each shot, turnover or foul (it was about three), so you see the play that leads to it. To watch every possession whole at real speed, turn off Fast-forward to the action in the live game's settings (the button next to Key plays).",
	},
	{
		n: 451,
		date: "2026-11-23",
		text: "Global Settings has a new 3D Game Sim switch, kept on each device, for trying the new 3D game while it is being built. It stays on Current, today's 3D game, unless you change it. New is early: for now it only changes how players get about the floor, each moving the way his own ratings let him.",
	},
	{
		n: 450,
		date: "2026-11-23",
		text: "Leagues with the smart CPU GM sim faster: about 15% less time over a season, and its trade market moments about a fifth quicker. The CPU teams make exactly the same moves as before.",
	},
	{
		n: 449,
		date: "2026-11-23",
		text: "3D game: score bugs can now show a team in the broadcast's own colors (the Heat in black, the Raptors in black, the Knicks in MSG's darker blue), set figures the way a broadcast does (a narrow 1), and slide FOULS aside when BONUS shows. A superscript 4TH now sits level with the top of the 4.",
	},
	{
		n: 448,
		date: "2026-11-23",
		text: "Leagues on the NBA Calendar now show today's date at the top of every page, with the year and the real day of the week (Wed, Nov 12, 2025), and the season named the NBA way (2025-26). Tap it for the new Calendar page: a month at a time for any team, with home games shaded, results, back-to-backs, playoff games, the trade deadline and the All-Star Game, or how many games are on each night across the league.",
	},
	{
		n: 447,
		date: "2026-11-23",
		text: "Draft night looks more like the real broadcast. The commissioner reads each pick square on at a dark lectern, team-color panels either side of him. At his table the player faces the stage, with the room, the photographers and the stands behind him, and hugs each of his family. He walks up to the stage through the smoke, is handed his cap at the foot of the steps and puts it on, and hugs the commissioner, seen from behind the commissioner looking out at the room. The arena is darker, with full stands all the way round and black tablecloths.",
	},
	{
		n: 446,
		date: "2026-11-23",
		text: "3D game: score bugs can now show timeouts running up and down and the home team's lead (THE DIFF +4), the way some local broadcasts do, and a bug made bigger to read on a phone no longer runs off the side of the screen.",
	},
	{
		n: 445,
		date: "2026-11-23",
		text: "NBA Calendar schedules are now built the way the NBA builds them, for leagues that don't play a real NBA season: road trips loop through nearby cities (about as many miles as NBA teams really fly), no back-to-backs across the country, a few two-game series in the same arena, long road trips for teams whose arena is booked, and no games on Election Day or the night of the NCAA final. The All-Star break now runs Friday to Wednesday.",
	},
	{
		n: 444,
		date: "2026-11-23",
		text: "CPU teams in leagues on the NBA Calendar now look for trades on every one of the last five nights before the trade deadline. Before, they skipped about half of those nights.",
	},
	{
		n: 443,
		date: "2026-11-23",
		text: "Every team now has a place on the map, for NBA Calendar travel: about 750 cities worldwide, so most regions are found on their own. A team with a made-up region can be given a Home city in Manage Teams.",
	},
	{
		n: 442,
		date: "2026-11-23",
		text: "3D game: some of the league's high-flyers now finish an open-floor dunk with a 360, like NBA 2K's Athletic 360s - up off two feet, the ball tucked in at his chest as he spins all the way round in the air, then slammed down facing the rim. The rest still windmill it.",
	},
	{
		n: 441,
		date: "2026-11-23",
		text: "The Draft page now has the draft room in 3D at the top. Between picks it shows the team on the clock, with the cameras going round the room and cutting to the best players left, sat at their tables with their families. When a pick is made, it plays: the commissioner reads it, the player hugs his family and walks up to the stage. If a sim makes several picks at once, it shows yours, then a league mate's, then the last one. The X hides it.",
	},
	{
		n: 440,
		date: "2026-11-23",
		text: "Draft night: no two picks play out the same now. Where his family's table is, who's there with him, how he reacts when his name is read, what he does on the stage and where the cameras are all change from player to player. Each player always gets his own, so a replay is what you saw live.",
	},
	{
		n: 439,
		date: "2026-11-23",
		text: "Draft night is on for everyone now, whichever interface you use. Every pick on the Draft page has a play button too, so you can watch a player get picked as soon as it happens.",
	},
	{
		n: 438,
		date: "2026-11-23",
		text: "3D game: games get ready to watch about 10% faster on phones - the stretches the replay fast-forwards through (free throws, substitutions, timeouts, the walk up the floor) are no longer worked out a step at a time.",
	},
	{
		n: 437,
		date: "2026-11-23",
		text: "New league setting, NBA Calendar (off unless you turn it on): every game gets a real date, from late October to the Finals in June, with back-to-backs, road trips and Christmas games. NBA-shaped leagues play a real NBA schedule, other leagues get one built like it. Each sim day becomes one night (about 165 in a season), and injuries count games, not days. Turn it on in Settings, under Schedule; it starts with the next regular season.",
	},
	{
		n: 436,
		quiet: true,
		date: "2026-11-23",
		text: "Seasons now run on a real calendar: every game has a date, from late October to the Finals in June, with back-to-backs, road trips and Christmas games. NBA-shaped leagues play a real NBA schedule, other leagues get one built like it. Each sim day is now one night (about 165 in a season), and injuries count games, not days. Starts with the next regular season; NBA Calendar in Settings turns it off.",
	},
	{
		n: 435,
		date: "2026-11-22",
		text: "3D game: score bugs can now look like the real NBA broadcasts' - a BONUS mark that shows when a team is in the bonus, fouls written their way (FOULS: 4, FOUL LIMIT), 3RD-style quarters, clocks like :24 and :09.2, and the bug placed where the broadcast puts it. Score bugs also stay readable on phones.",
	},
	{
		n: 434,
		date: "2026-11-22",
		text: "Draft picks now happen at the draft, not on the team's home court: the floor is full of green room tables, and the stage has an LED wall, a podium and a tower in the picking team's colors next to the draft board. The commissioner reads the pick, the player gets up from his family's table and hugs his mom, walks up the aisle and onto the stage through the smoke, and shakes the commissioner's hand for the photos.",
	},
	{
		n: 433,
		date: "2026-11-22",
		text: "Draft walk-outs now play like draft night on TV: the pick is read out over the team's colors - round and pick, SELECTION, then his name - and on his way to center court the broadcast bar shows his school, age and size, true facts about him, the next picks in the draft order and the best players still on the board, then the next team on the clock.",
	},
	{
		n: 432,
		date: "2026-11-22",
		text: "3D game: every team can have its own score bug, like its local TV broadcast's (Manage Teams, Score bug next to each team, or in a team looks file). You see your own team's bug when it plays and the home team's otherwise. Score bugs can now also use their own fonts, angled panels and team-colored gradients.",
	},
	{
		n: 431,
		date: "2026-11-22",
		text: "CPU teams value first-round picks a little more in trades, so they give up fewer firsts for older role players and more for real starters.",
	},
	{
		n: 430,
		date: "2026-11-22",
		text: "3D game: joining a league-mate's live game late no longer leaves the 3D stuck loading on a phone that runs short of memory getting it ready - it loads a lighter version of the game instead, or plays it on the 2D court.",
	},
	{
		n: 429,
		date: "2026-11-22",
		text: "3D game: players are back to the build they had before the NBA 2K proportions - the bigger head and sturdier arms and legs - with all the new moves kept.",
	},
	{
		n: 428,
		date: "2026-11-22",
		text: "3D game: shooters hold the guide hand up beside the shooting arm after the release, like NBA 2K's, instead of dropping it to the forehead - and a player walking the ball up under pressure is bent over it like a real dribbler.",
	},
	{
		n: 427,
		date: "2026-11-22",
		text: "3D game: arms reaching up - dunks, blocks, rebounds, jump shots - keep their own length, the shoulders coming up with them the way a real player's do, instead of the arms stretching out long and thin.",
	},
	{
		n: 426,
		date: "2026-11-22",
		text: "3D game: moves fitted to motion capture of real players - spin moves bent low over the ball and whipped round off the plant, drives that lean hard into the first steps, pull-ups that sit down into the legs instead of leaning back, and shot fakes that stay down in the knees. And the ball no longer jumps in a player's hands as he picks up his dribble.",
	},
	{
		n: 425,
		date: "2026-11-22",
		quiet: true,
		text: "3D game: players are built like real NBA players - measured off NBA 2K's player editor: a smaller head, longer legs and longer arms, shoulders up where a man's are. They run and step to fit their new legs, too: no more dropping into a backpedal or a defensive slide in a frame.",
	},
	{
		n: 424,
		date: "2026-11-22",
		text: "3D game: the players are quicker to draw, so games run smoother on phones - and the picture stays sharp instead of going blocky when a phone falls behind.",
	},
	{
		n: 423,
		date: "2026-11-21",
		text: "3D game: everyone's players are now the ones rebuilt in Blender - one smooth body with real muscle on their shoulders, arms and calves. The 3D Players setting is gone.",
	},
	{
		n: 422,
		date: "2026-11-21",
		text: "3D game: defensive slides are lower, like a real defender's, measured from motion capture of a defender sliding side to side - knees bent past a right angle as the feet come together, chest out over the knees, and the knees out over the toes instead of straight-legged from the front.",
	},
	{
		n: 421,
		date: "2026-11-21",
		text: "3D game: the stanchions holding up the baskets are rebuilt in Blender as solid 3D pieces - a padded base and post, and a boom with a brace and cross-bars out to the backboard - instead of flat shapes.",
	},
	{
		n: 420,
		date: "2026-11-21",
		// (Its setting is gone: 423 says what is so now.)
		quiet: true,
		text: "3D game: new players to try - the same players rebuilt as one smooth body in Blender, with real muscle on their shoulders, arms and calves. Turn them on in Settings under 3D Players: Blender (beta). Classic stays the default for now.",
	},
	{
		n: 419,
		date: "2026-11-21",
		text: "CPU trades: a team now knows which of its players it wouldn't pay to re-sign, and takes a fair offer for him before the deadline instead of holding out and watching him leave for nothing. In test leagues good players walking away from losing teams for nothing fell by almost two thirds. The CPU trade market also thinks a little faster, and answers your trade offers faster.",
	},
	{
		n: 418,
		date: "2026-11-21",
		text: "CPU trades rebuilt to work like real front offices. Every team now sizes up its situation each time the trade market meets, scouts every player in the league, and puts up for trade anyone it would trade - starting with players about to leave in free agency, every time until the deadline. Each player goes to every team that could really use him, so a player lots of contenders want fetches what he's worth, and prices come from the whole league, not just the best teams. A first-round pick packaged with players nobody wants is now counted as the first it is. As the deadline nears, contenders take more risk to win now and losing teams move players before they walk. In test leagues more of the league's best players changed teams, buyers were better off for it, and titles were spread across more teams.",
	},
	{
		n: 417,
		date: "2026-11-21",
		text: "3D game: players now run the way real people do, measured from motion capture of real runners and a real speed dribble. The back leg stays straight as it leaves the floor before the heel folds up, arms swing in time with the legs instead of a beat behind, and runners lean into it with their shoulders turning. Speed dribbles are lower and harder, with the free arm pumping.",
	},
	{
		n: 416,
		date: "2026-11-21",
		text: '3D game: the "Realistic (beta)" players are gone, and so is the 3D Players setting - everybody sees the Classic players. New, more realistic players are being built to replace them.',
	},
	{
		n: 415,
		date: "2026-11-21",
		text: "3D game: defenders on the far side of the floor now guard from in front, between their man and the basket, the way they do on the near side, instead of standing off to his side so the camera could see the shooter. That includes the man on the ball, closeouts on jump shots and help stepping up.",
	},
	{
		n: 414,
		date: "2026-11-21",
		text: "3D game: a player driving to the basket no longer turns sideways partway through the drive - he faces where he's going. Drives also bend around the defender instead of running in dead-straight lines.",
	},
	{
		n: 413,
		date: "2026-11-20",
		text: "CPU trades: teams now count a late first-round pick's real chance of turning into a good player, so contenders stop throwing firsts at older bench players. In test leagues that cut firsts paid for role players 27 or older by more than a third, while more of the league's best players changed teams and fewer good players walked away from losing teams for nothing.",
	},
	{
		n: 412,
		date: "2026-11-20",
		text: "The 3D game is ready sooner: getting a game ready to watch in 3D now takes about 15-20% less time on iPhones and about 10% less on Android and computers. The games play out exactly the same as before.",
	},
	{
		n: 411,
		date: "2026-11-20",
		text: "CPU trades: contenders can now land real star trades. A team selling a star counts the players it gets back at what they're worth on the market, not just to itself, and a buyer can put up to six pieces in an offer. In test leagues twice as many of the league's best players changed teams, and half as many firsts were paid for older role players. The CPU trade market takes a little longer to think each time it meets.",
	},
	{
		n: 410,
		date: "2026-11-20",
		text: "Smart CPU: each trade market round runs a little faster. The trades are the same; the CPU just stops redoing work it had already done while trimming offers.",
	},
	{
		n: 409,
		date: "2026-11-20",
		text: "Watching a league mate's live game in 3D: the 3D game starts loading as soon as the \"Watch ... live?\" question pops up, so it's ready sooner after you tap Watch. While it loads, the score, plays and box score keep up with the game instead of sitting at 0-0, and leaving the page and coming back no longer starts the loading over.",
	},
	{
		n: 408,
		date: "2026-11-20",
		text: "CPU trades: a team now sees that its own first-round pick a few years out could become a top pick if it falls apart, and prices it that way, so good teams stop throwing in far-off firsts as if they were sure to land late. Every team's pick is now judged against where the rest of the league is headed.",
	},
	{
		n: 407,
		date: "2026-11-20",
		text: "Smart CPU: the offseason advances faster. The CPU trade market no longer meets as the draft opens or right after it (it hardly ever made a deal there), and teams clearing cap room in free agency work it out about twice as fast. On a long trade market, the phone in charge of simming keeps the league marked busy, so another phone can't start the same step.",
	},
	{
		n: 406,
		date: "2026-11-20",
		text: "League setup and Settings: on a phone the page no longer slides sideways. The Filter settings box sits on its own line above the buttons.",
	},
	{
		n: 405,
		date: "2026-11-20",
		text: "CPU free agency: a team clearing cap room for a free agent now counts a player it sends away at what other teams would pay for him, so it sends its worst contracts, or gets a pick back, instead of giving good players away for nothing.",
	},
	{
		n: 404,
		date: "2026-11-20",
		text: "CPU trades: teams can now put together a real offer for a player on a big contract. They send the salary the trade needs in the contracts they'll miss least, then add the picks or young players the other team wants. Losing teams sell expensive veterans instead of letting them walk for nothing, and contenders can actually trade for them.",
	},
	{
		n: 403,
		date: "2026-11-20",
		text: "The next-game spread in the top bar now updates as soon as a starting five changes (auto sort, dragging the roster, Keep auto sorted, or a league mate's changes), so it always matches the Daily Schedule.",
	},
	{
		n: 402,
		date: "2026-11-20",
		text: "Daily Schedule: whoever is in charge of simming gets a Sim CPU games button at the bottom of the page (where everyone else sees Sim here). It sims every game today that no user team is playing in, so only the user games are left.",
	},
	{
		n: 401,
		date: "2026-11-20",
		text: "CPU drafts: teams draft for themselves. A rebuilding team takes the young players with upside it can build around instead of the older, ready players contenders would want, and when it can't tell two prospects apart it takes the one higher on the board.",
	},
	{
		n: 400,
		date: "2026-11-20",
		text: "CPU trades: teams no longer give up useful players just to collect far-off draft picks at prices nobody really pays, a first sent along with filler contracts now costs what a first is worth, and pointless swaps between teams out of the race are gone - both sides have to come out clearly ahead, trades made to clear cap room included.",
	},
	{
		n: 399,
		date: "2026-11-20",
		text: "CPU teams now judge whether a young player will re-sign by how good the team should be when his contract runs out, not by how bad it is today - so rebuilding teams keep the young players they're building around instead of selling them for spare parts, and making the team better now counts toward keeping a young star happy.",
	},
	{
		n: 398,
		date: "2026-11-20",
		text: "Game pages and live games: the Shots button is gone from the bottom menu. The shot chart is still right under the game flow, so the Game Flow button takes you to both.",
	},
	{
		n: 397,
		date: "2026-11-19",
		text: "Game pages: on a phone the recap's headline and story are centered on the screen.",
	},
	{
		n: 396,
		date: "2026-11-19",
		text: "Game pages: the Recap button in the bottom menu takes you all the way back to the top of the page, score banner and all.",
	},
	{
		n: 395,
		date: "2026-11-19",
		text: 'League list: shared cloud leagues now show your own team and logo instead of "Multi Team Mode". It updates the next time you open the league.',
	},
	{
		n: 394,
		date: "2026-11-19",
		text: "Roster page: Auto sort, Reset playing time, Keep auto sorted and Play Through Injuries are back above the depth chart, small and tucked beside your team's info (under the logo on a phone). Play Through Injuries opens as a little panel over the page, so nothing jumps around.",
	},
	{
		n: 393,
		date: "2026-11-19",
		text: "Player highlights: watching a player's highlights from a game now has just a back arrow (previous highlight), a forward arrow (next highlight) and the speed menu.",
	},
	{
		n: 392,
		date: "2026-11-19",
		text: "Team pages: the Roster button in the bottom menu takes you all the way back to the top of the page.",
	},
	{
		n: 391,
		date: "2026-11-19",
		text: "Box scores: each team's box score now has its own quarter picker next to its menu, so you can look at one team's 3rd quarter while the other shows the whole game. It replaces the quarter picker that sat at the bottom left of the game page.",
	},
	{
		n: 390,
		date: "2026-11-19",
		text: "Live games: fixed a rare glitch where the box score's player stats stopped counting (everyone at 0) while the score kept going.",
	},
	{
		n: 389,
		date: "2026-11-19",
		text: "Box scores list your team first - the box score, the players on the floor, rotations and lineups - in live games and in the game log. The quarter picker at the bottom left of the live game is gone.",
	},
	{
		n: 388,
		date: "2026-11-19",
		text: "Game pages: the recap is centered on the page.",
	},
	{
		n: 387,
		date: "2026-11-19",
		text: "Game pages: no more section headings (Recap, Box Score and so on) - the menu at the bottom shows where you are. The line, season series, standings and recent games now sit at the foot of the page instead of next to the recap.",
	},
	{
		n: 386,
		date: "2026-11-19",
		text: "Live games: ball handlers keep the ball away from their defender. A left/right mix-up had them switching it over toward him; now, with a defender close, the ball is on his far side about two times in three, and three in four on the move.",
	},
	{
		n: 385,
		date: "2026-11-19",
		text: "Game pages: Game Flow now has its own button in the menu at the bottom, to jump straight to it.",
	},
	{
		n: 384,
		date: "2026-11-19",
		text: "Smart CPU: CPU teams trade picks more sensibly. Each team now judges its own title chances correctly (the favorite was overrating every role player), a seller can't demand two firsts when the buyer has other players just as good on offer, and a team only makes a trade when it's clearly better off, not by a hair it can't be sure of. Expect fewer firsts spent on role players and fewer pointless swaps.",
	},
	{
		n: 383,
		date: "2026-11-19",
		text: "Game pages: \"All games this day\" now shows each team's record, the pregame line, and each team's top performer with points/rebounds/assists. The recap no longer repeats the game leaders already in the header, and the header's leaders sit a little lower.",
	},
	{
		n: 382,
		date: "2026-11-19",
		text: "Game pages: the recap now comes first, then the box score.",
	},
	{
		n: 381,
		date: "2026-11-19",
		text: "Live games: no more long necks when players bend down. Bent over for the ball or slapping the floor, their head drops toward the ball and sits on their shoulders, instead of sticking out on a stretched neck.",
	},
	{
		n: 380,
		date: "2026-11-19",
		text: "Live games: dribbling looks more like the real thing. Pushing it up the floor, players run tall on a full stride and push the ball out in front, catching it at the waist; walking it up with nobody near, they stand up instead of crouching; and the free arm swings with the stride, or stays low and ready sizing up. Dribbling left-handed, the right arm now comes up to hold off a defender too.",
	},
	{
		n: 379,
		date: "2026-11-19",
		text: "Live games: players' heads stay on their necks. On crossovers, in-and-outs, spins, leaning jumpers and stumbles, the head no longer comes away from the body.",
	},
	{
		n: 378,
		date: "2026-11-19",
		text: "Smart CPU: a CPU team shopping for an upgrade now takes the best deal on offer to it, not just the first one a seller wants. Teams spend their firsts on better players and give fewer of them away for role players.",
	},
	{
		n: 377,
		date: "2026-11-19",
		text: "Smart CPU: CPU teams value the future more. A title five years away now counts about 77% as much as one this season (it was 59%), so draft picks and young players are worth more to every team, and it takes a bigger upgrade for a contender to give up a first.",
	},
	{
		n: 376,
		date: "2026-11-19",
		text: "Smart CPU: CPU teams judge trades more accurately. They no longer count on old players coming back who they would never re-sign, so they stop dumping veterans for worse players, and they no longer overrate what makes one role player different from another of the same rating, so they make fewer trades that only look good on paper.",
	},
	{
		n: 375,
		date: "2026-11-19",
		text: "Live games: no more heads poking out in front on a stretched neck. Running, sprinting, driving and down in a defensive stance, players keep their head up over their shoulders while their body leans into it.",
	},
	{
		n: 374,
		date: "2026-11-19",
		text: "Live games: players run, jog and walk more like real ones. Running, they stay upright, push off a straight back leg and pump their arms bent at the elbow instead of holding them out in front; walking, their arms hang loose at their sides.",
	},
	{
		n: 373,
		date: "2026-11-19",
		text: "Realistic players for 3D games is now a setting of its own in Global Settings (3D Players: Classic or Realistic), instead of in the court's gear menu. Classic stays the default.",
	},
	{
		n: 372,
		date: "2026-11-19",
		text: "Live games: try the new Realistic (beta) option for 3D Players in Global Settings. Players are drawn as real human bodies in their team's jersey and baggy shorts, with each player's face on a 3D head. It's an early version and can be slower on older phones; the refs and coaches stay as they were.",
	},
	{
		n: 371,
		date: "2026-11-19",
		text: "Smart CPU: CPU teams stop selling their young potential stars for spare parts and far-off draft picks. A team now values the players it trades for by what they do for it, and picks two or more drafts away are no longer priced like sure things.",
	},
	{
		n: 370,
		date: "2026-11-18",
		text: "Live games: defenders guard more like real ones. The man on the ball stays down in his stance as he slides, backs up and steps up with the ball handler, instead of walking or jogging upright beside him, and every defensive slide sits lower and wider with the arms out.",
	},
	{
		n: 369,
		date: "2026-11-18",
		text: "Smart CPU: making the playoffs and going out in the first round no longer counts as a good season for a CPU team. Only winning playoff series does, so a team stuck around 43 wins gains nothing from staying there and has more reason to build toward a real contender.",
	},
	{
		n: 368,
		date: "2026-11-18",
		text: "Smart CPU: CPU teams now sell players who are about to walk in free agency instead of holding out for a price nobody pays and losing them for nothing. A player in the last year of his deal is worth his full trade value only as far as he is likely to re-sign.",
	},
	{
		n: 367,
		date: "2026-11-18",
		text: "Live games: a player who's hot gets hyped. Make three shots in a row and he pumps his fist; keep the streak going and he lets out a roar. An and-one dunk gets a roar too.",
	},
	{
		n: 366,
		date: "2026-11-18",
		text: "Smart CPU: when a CPU team trades for a player, it now offers what the selling team actually values, like young players and picks other teams would pay for, instead of filler. And a team taking on salary so another team can sign a free agent now wants to be paid for its cap room.",
	},
	{
		n: 365,
		date: "2026-11-18",
		text: "Smart CPU: CPU teams no longer hand the league's best teams a big upgrade for scraps. A team selling a player another team wants far more than anyone else now holds out for a fair share of what the deal is worth, so contenders have to pay real value, like young players or good picks, for stars.",
	},
	{
		n: 364,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): in a game's header, each team's name is back where it was, with its points, rebounds and assists leaders tucked beside its record under the name.",
	},
	{
		n: 363,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): a game's header shows each team's points, rebounds and assists leaders in small print under its record (on a phone, just under the header), each linked to the player.",
	},
	{
		n: 362,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): in a live game, the Live button at the bottom scrolls all the way back to the top of the window.",
	},
	{
		n: 361,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): the standings, the playoff bracket, the trade summary and player pages are back to the stock BBGM look. The new look now only changes the game page, the live game and the team page.",
	},
	{
		n: 360,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): a game's header shows the season and day under FINAL (\"2024 Day 45\", to that day's schedule), with the playoff series by it. Each team's arrows are easier to see in its own colors, and the Game Log bar no longer sits over the game.",
	},
	{
		n: 359,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): on a team page, Play Through Injuries sits on the row with Auto sort and Reset playing time and folds open under it, instead of stretching across the whole window.",
	},
	{
		n: 358,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): team and player pages are back to the classic header - the title bar with its dropdowns, the team's record, payroll and links, the player's picture and bio - while keeping the one long team page, the menu at the bottom and the game tiles. In a box score, Traditional / Advanced / Rotations sit next to the Box Score heading.",
	},
	{
		n: 357,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): a team's header has everything the classic team page had - rating (tap for healthy), MOV, age, payroll (tap for any penalty) with its cap status, salary cap, hard cap and profit, with the record and playoffs linked as before. Auto sort, reset playing time, keep auto sorted and Play Through Injuries share a row. The player header's face fills it, with a bigger last name.",
	},
	{
		n: 356,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): a team's page is one long page - its roster, every game of the season as a tile (tap one for its box score; pick any year by the team's name), finances, picks and transactions - with its record and key numbers in its header. A box score is its own page, with each team's previous and next game arrows, the replay by FINAL, all of that day's games, and a table for each team. The player header is laid out like HoopsJunkie's, and the menu at the bottom now follows your scrolling.",
	},
	{
		n: 355,
		date: "2026-11-18",
		text: "Smart CPU: free agency sims about a third faster, and the season a little faster. CPU teams make exactly the same moves as before; they just no longer re-price every coming draft class each time a free agent signs.",
	},
	{
		n: 354,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): the box score has Traditional and Advanced tabs on BBGM's own table, with its three-dot menu to customize columns. On/Off is easier to read, and the menu at the bottom of a game fits on a phone without sliding. Player pages get the same bottom menu (Overview, Stats, Ratings, Career, More) and keep the player's name pinned at the top as you scroll; On The Floor With is gone there.",
	},
	{
		n: 353,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): box scores open as fast as the classic ones. The phone box score and rotations use BBGM's own tables again. A game's score stays pinned at the top as you scroll, and the team header is the same on every team page. Four Factors and the repeated rotation, lineup and on/off charts are gone from the game page. The player page's face is bigger.",
	},
	{
		n: 352,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): tapping a section in the menu at the bottom of a box score or the live game now lands it right under the court or team header, instead of a little short of it.",
	},
	{
		n: 351,
		date: "2026-11-18",
		text: "New look (Look: Refreshed), phone box score: each player has the ratings and stats button by his name instead of his number, and the faces are a little bigger without the grey circle behind them.",
	},
	{
		n: 350,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): the team header on team pages loses the little stripes that sat on its tabs.",
	},
	{
		n: 349,
		date: "2026-11-18",
		text: "New look (Look: Refreshed): a box score's header now shows each team's top scorer, and on the game log the season picker sits next to the team's name, sized like the buttons by it. The team's strip is in its colors again where it had come up gray.",
	},
	{
		n: 348,
		date: "2026-11-18",
		text: "Fixed the new look (Look: Refreshed) on phones: a box score's header lost its team colors and the score slid under the quarter scores, and some pages could be dragged sideways.",
	},
	{
		n: 347,
		date: "2026-11-18",
		text: "A new look you can turn on in Global Settings (Look: Refreshed). Team pages, box scores and player pages get team-colored headers, and standings, playoffs and trades show each team in its colors. A box score becomes one page with a menu at the bottom. Phones get a box score you can pick the columns for, plus rotations, lineups and on/off from each game, also on the live game. Classic stays the default.",
	},
	{
		n: 346,
		date: "2026-11-17",
		text: "Smart CPU: CPU teams' players now re-sign as willingly as players on a human's team. Before, CPU teams' players tested free agency far more often, so CPU teams lost their stars for nothing about twice as often as you would.",
	},
	{
		n: 345,
		date: "2026-11-17",
		text: "Smart CPU: CPU teams now spend to win, not to save money. They use their cap room in free agency (looking every day and weighing who is really still out there), make room by trade for a free agent who is a clear upgrade, keep young players they used to let walk, see when a player's injury runs into next season, and better judge who will re-sign.",
	},
	{
		n: 344,
		date: "2026-11-17",
		text: "3D: on a pick-and-roll the screener sets the screen beside the ball handler's defender, level with him, instead of squeezing in between the two of them.",
	},
	{
		n: 343,
		date: "2026-11-17",
		text: "A playoff game's box score now says the round, which game of the series it was and where the series stands under the score, like: 1st round · Game 3 · BOS leads 2-1. Tap it for the bracket.",
	},
	{
		n: 342,
		date: "2026-11-17",
		text: "3D: when a ball handler sizes up his man and then drives or pulls up, his last move (a hesitation, an in-and-out or a jab) leads straight into it instead of him standing there after it.",
	},
	{
		n: 341,
		date: "2026-11-17",
		text: "3D: on a pick-and-roll the screener's defender is set for it as the screen comes: sitting back near the foul line in a drop, or up level with the screen to hedge or switch. Screens are set before the ball handler comes by, a defender who ends up right on a jump shooter always gets a hand up, and a post player's defender is on his back as he starts backing him down.",
	},
	{
		n: 340,
		date: "2026-11-17",
		text: "Shared leagues: a trade or signing that uploads after a sim no longer wipes that sim's games, stats and injuries for the players involved, and a sim that uploads after your move no longer undoes it. Steps a league mate's phone runs (a draft pick, the next phase) now hold everyone's moves like a sim does, and if your own game ends the regular season or the finals, the next phase starts with the next sim from whoever is in charge of simming.",
	},
	{
		n: 339,
		date: "2026-11-17",
		text: "3D: a ball handler keeps the ball away from the defender in front of him, switching it to the far hand with a crossover, or between the legs when he's sizing up a defender who's tight on him.",
	},
	{
		n: 338,
		date: "2026-11-17",
		text: "Shared leagues: CPU teams now only trade and sign on the device in charge of simming, so your own last draft pick or your own game no longer freezes your phone while the CPU thinks, and league mates' teams no longer show up as CPU trade offers. Saving League Settings now only changes what you changed, Auto Play holds everyone's moves until it's done, and Sim here waits until the current sim finishes.",
	},
	{
		n: 337,
		date: "2026-11-17",
		text: "3D: a defender guarding the ball plays an arm's length off the ball handler and gives ground as he comes, instead of standing in his chest, and a dribble at a defender goes by his hip instead of straight through him.",
	},
	{
		n: 336,
		date: "2026-11-17",
		text: "Shared leagues: while free agency is being simmed, league mates' signings and other moves wait until the sim is uploaded, the same as during the season, so a long free agency day can't overwrite them.",
	},
	{
		n: 335,
		date: "2026-11-17",
		text: "Shared leagues: notifications wait again while you're playing or watching a live game, so they don't spoil a score, and come through as one when it ends. This time a phone closed in the middle of a game still gets them within about 15 minutes, and nothing waits more than 45.",
	},
	{
		n: 334,
		date: "2026-11-17",
		text: "Smart CPU now keeps the game's normal rules: turning it on no longer brings in the realistic free agency and contract rules (the MLE and the rest), it just makes the CPU teams smarter. In League Settings, Difficulty and Smart CPU are one box at the top: one Difficulty sets trading, signing and revenue together, or set any of them on its own (Sync puts them back together).",
	},
	{
		n: 333,
		date: "2026-11-17",
		text: "Shared leagues: a phone that stopped getting notifications now signs itself back up when you open the app. There's also a Re-register button under Phone notifications on the Multiplayer Sync page, if they ever stop coming.",
	},
	{
		n: 332,
		date: "2026-11-17",
		text: "3D: a player stripped of the ball now looks to the official for the call, and one whose pass is picked off puts his hands on his head. Also fixed: the ball snapping into the hands of a player who never went to get it, a defender drifting into a player going up for a layup, and crossovers going the opposite way from where the dribbler moves.",
	},
	{
		n: 331,
		date: "2026-11-17",
		text: "Smart CPU: CPU teams that aren't the favorites now play for a deep playoff run too, not just the title, so when one team is far ahead the rest still trade to get better during the season. CPU teams also rarely pass a player they just traded for straight on to someone else.",
	},
	{
		n: 330,
		date: "2026-11-17",
		text: "3D: players' whole bodies lean with how they move: tipped forward bursting off, sat back pulling up, leaning into a hard cut. Two players who run into each other (a screen, a drive, a closeout) both rock back off the hit, and players fighting for position in a post-up or box-out lean on each other.",
	},
	{
		n: 329,
		date: "2026-11-17",
		text: "Shared leagues: catching up after time away is quicker when the connection stalls partway. The app no longer stops to reconnect in the middle; it fetches the rest another way and reconnects once you're caught up.",
	},
	{
		n: 328,
		date: "2026-11-17",
		text: "Smart CPU now works with Realistic Front Offices off: free agency, re-signing, the trade screens and your trade difficulty stay as they are, but CPU teams trade with each other all season, sign, re-sign and draft by what actually helps them win, and won't take a trade that goes against where they're headed or helps a rival contender more than it helps them.",
	},
	{
		n: 327,
		date: "2026-11-17",
		text: "Shared leagues: phone notifications work again. Holding a device's notifications while it was in a live game (yesterday's change) kept them from going out at all, so it's off for now: you may get a score while you're still watching that game.",
	},
	{
		n: 326,
		date: "2026-11-16",
		text: 'Recaps: the prompts you copy into Claude or ChatGPT now ask for American English ("offense", not "offence"), and they hand the AI every title season, playoff run and award count already worked out, so a recap no longer comes back with "a third title in seven seasons" when it was five in ten.',
	},
	{
		n: 325,
		date: "2026-11-16",
		text: "Shared leagues: phone notifications no longer spoil a game you're in the middle of. While you're playing a live game or watching a league mate's, notifications (like the day's scores) wait and come through as one once the game ends on your screen or you leave it.",
	},
	{
		n: 324,
		date: "2026-11-16",
		text: "Shared leagues: watching a league mate's live game, switching to another tab or app for a bit (during the warm-ups, say) no longer drops you out of watching. Before, coming back could leave your screen playing the game on its own with its own controls while theirs was still playing it.",
	},
	{
		n: 323,
		date: "2026-11-16",
		text: '3D: a play-in game\'s score bug now says it\'s a play-in game and shows both teams\' seeds (like "Play-In · 10 CHA vs 9 NYK") instead of "Game 1".',
	},
	{
		n: 322,
		date: "2026-11-16",
		text: "3D: players move less like robots, especially on phones. Defenders in their stance move smoothly instead of in little ticks, turning in place is a step at a time instead of tapping their feet, shots, passes and catches flow through each motion, and each player bounces to his own rhythm instead of the whole defense in lockstep.",
	},
	{
		n: 321,
		date: "2026-11-15",
		text: "3D: zoomed in, the basket no longer sits at the edge of the picture or off the top of it. In a half-court set the camera keeps the basket, its backboard and the baseline well inside the frame with the ball, and pulls back a little when it can't fit both.",
	},
	{
		n: 320,
		date: "2026-11-15",
		text: "3D: the zoomed-in camera now works like a TV camera operator. It leads the play up the floor instead of locking onto the basket behind it, settles on the basket being attacked once the offense sets up, and holds steady through small moves instead of chasing every step.",
	},
	{
		n: 319,
		date: "2026-11-15",
		text: "Shared leagues: the Auto Play Scheduler now says at the top who's in charge of simming, with a Sim here button to take it over from that page.",
	},
	{
		n: 318,
		date: "2026-11-15",
		text: "The Slam Dunk and Three-Point Contest pages no longer show a second, small face next to each contestant's name under their picture.",
	},
	{
		n: 317,
		date: "2026-11-15",
		text: 'Daily recaps now read like a newspaper\'s nightly roundup: a headline on the night\'s biggest story, a line on the rest, the top game first, then a short paragraph for every other game led by its final score ("Nuggets 116, Pelicans 108:"), with streaks, injuries, upsets and series news worked in. Headlines on daily and game recaps are written the way real wire headlines are, like "Young leads Pistons past 76ers 110-107" or "Rockets beat Bucks 119-112 in OT, take 2-0 series lead".',
	},
	{
		n: 316,
		date: "2026-11-15",
		text: "3D: games no longer get laggy and blurry on phones as they go on. The 3D court was holding on to more and more of the game in memory (twice as much by the fourth quarter); now it keeps only what is on screen, so the fourth quarter plays as smoothly as the first.",
	},
	{
		n: 315,
		date: "2026-11-14",
		text: "Shared leagues: a sim now keeps the league marked as simming from its first day to its last, so changes league mates make meanwhile wait and go in after it instead of colliding with it. With Smart CPU on, a long sim on a slow phone is no longer taken for a crashed one and run again on another device, and the CPU teams' trade and free agency plans carry over a reload or a league mate taking over the simming.",
	},
	{
		n: 314,
		date: "2026-11-14",
		text: "Win Probability on a game's page is much more accurate: it's now measured against tens of thousands of simmed games, so a team down 2 with the ball and 9 seconds left shows about 25% instead of 0%, and early leads count for less. Tap or drag across the chart and the play at that moment shows under the score (for games with play-by-play).",
	},
	{
		n: 313,
		date: "2026-11-14",
		text: "Smart CPU: the trading block's What are you looking for? choices now shape every team's offer. Ask for draft picks, prospects, the best current players, a position, a skill or a salary limit, and each team leads with that (or makes no offer if it has nothing it would give).",
	},
	{
		n: 312,
		date: "2026-11-14",
		text: "League Settings now has one switch for the CPU front offices: Smart CPU, under Game Modes. Realistic Front Offices and Front Office Difficulty are gone from the list - turning Smart CPU on turns on everything it needs. Leagues already using Realistic Front Offices keep running as they are.",
	},
	{
		n: 311,
		date: "2026-11-14",
		text: "New in League Settings: Smart CPU (Game Modes). Every CPU team is then run like a real general manager - each trade, signing, re-signing and draft pick judged by what it does to the team's title chances this season and the years ahead, and a good player sold only for the best offer around. The trading block, trade proposals and What would make this work show what the team would really do. It's off unless your league turns it on, and simming is slower with it.",
	},
	{
		n: 310,
		date: "2026-11-14",
		text: "3D: the game at 1x moves at real NBA tempo. Players' legs turn over as fast as a real player's (no more slow-motion jogs, backpedals and slides), stances and dribbling in place are livelier, the man with the ball sizes up his defender with quick moves instead of a slow sway, and shooters spaced around the arc slide to open spots when the ball is driven.",
	},
	{
		n: 309,
		date: "2026-11-13",
		text: "3D: on a poster dunk, the defender being dunked on now goes up right where he set himself under the rim, instead of sometimes sliding a step or two away before he jumps.",
	},
	{
		n: 308,
		date: "2026-11-13",
		text: "The draft walk-outs, the headlines that play and the League Logo setting are only on the new interface for now (Global Settings > Interface > New). The classic interface is back to how it was.",
	},
	{
		n: 307,
		date: "2026-11-13",
		text: "New interface: draft picks walk out in the league home's headlines. A story about a pick now plays his walk to center court under the spotlight, in his new team's cap, right in the headline - no button to press.",
	},
	{
		n: 306,
		date: "2026-11-13",
		text: "New interface: every pick on Draft History has a play button now. Press it to watch the player walk out to center court under a spotlight, in a suit and his new team's cap, with his name and pick on screen. Players with a photo for a face don't have one yet.",
	},
	{
		n: 305,
		date: "2026-11-13",
		text: "New interface: the league home's headlines now play. The top stories scroll by on their own, and a story about a game with a saved replay (a game winner, a big stat night) shows the moment itself on the 3D court, right in the headline. Games with a replay also get a play button in Latest Scores for their key plays. Replays are saved when the commissioner turns it on: League Settings > Auto-Save Replays > Feats & game winners (or pick teams).",
	},
	{
		n: 304,
		date: "2026-11-13",
		text: "3D: a defender guarding a jump shooter no longer backs away as he goes up. He holds his ground and gets a hand up, so far more jumpers are properly contested.",
	},
	{
		n: 303,
		date: "2026-11-13",
		text: "Realistic Front Offices: CPU teams now price a player by how he's playing today. Before, a player's price could stay stuck at what it was the first time anyone looked at it that season, until the game was reloaded - so two league mates could see different prices for the same player. Teams' plans (all-in, playoff, retool, rebuild) also no longer get redone early when the game reloads, and after the trade deadline teams know how much of the season is left.",
	},
	{
		n: 302,
		date: "2026-11-13",
		text: "New interface (Global Settings > Interface > New): the league home page now shows your playoff series during the playoffs, where to go next in the offseason, a task when your roster is over the limit or under the minimum, and the team leaders, league leaders, team stats, finances and starting lineup from the old dashboard.",
	},
	{
		n: 301,
		date: "2026-11-13",
		text: "A new look is on the way, one page at a time, with the same menus and buttons as now. Turn it on under Global Settings > Interface > New. First up is the league's home page: your messages and team news, the league's top stories, your next game and form, the latest scores, a two-week calendar, your schedule and the standings. Commissioners can set the league's logo under League Settings > UI, and it shows in the top bar.",
	},
	{
		n: 300,
		date: "2026-11-12",
		text: "3D: players' legs hang loose in the air instead of pointing straight down like a pencil. Knees soften and heels come up on jump shots, dunks, rebounds, blocks and contests, and a jump shooter lands with a deeper bend.",
	},
	{
		n: 299,
		date: "2026-11-12",
		text: 'Trading Block: you can now say what kind of player you want back - a PER of at least, a contract that runs through a season, and a salary of at most - and the offers stick to it. The "What are you looking for?" choices also fit on a phone now.',
	},
	{
		n: 298,
		date: "2026-11-12",
		text: "3D: a heave at the buzzer that nobody rebounds now bounces off the rim like a real ball - on the way it came off, each bounce lower, then rolling to a stop.",
	},
	{
		n: 297,
		date: "2026-11-12",
		text: "Realistic Front Offices: draft picks are priced by what picks really turn into, measured over 1,400 drafted careers. Today's earlier change left firsts after the top few and all seconds worth about nothing to CPU teams, so they'd throw several in just to move up a spot. Now a late first or a second is a small but real asset: moving up one spot costs about a second, and a pile of picks has to add up to the pick you want.",
	},
	{
		n: 296,
		date: "2026-11-12",
		text: "3D camera zoom: zoomed in on a half-court play, the camera now keeps the basket in the picture along with the ball, instead of letting it slide off the edge.",
	},
	{
		n: 295,
		date: "2026-11-12",
		text: "3D games: a camera zoom. Tap the gear beside the 3D controls and drag Camera zoom, up to 2.5x. Zoomed in, the players are bigger and the camera follows the play across the floor as well as along it. It is saved on your device, and 1x is the camera as it always was.",
	},
	{
		n: 294,
		date: "2026-11-12",
		text: "Realistic Front Offices: CPU teams without a real shot at the title no longer spend their future to be a little better this year. A playoff team that isn't a true contender won't trade young players and picks for an aging star - a young team waits until it's within striking distance - so the old stars go to the real contenders.",
	},
	{
		n: 293,
		date: "2026-11-12",
		text: "Realistic Front Offices: young players are valued by what the game shows their potential to be, not by a long-shot chance of far more. A raw teenager is no longer worth more than a better young player with higher potential, so CPU teams stop trading good young players for lottery tickets. Draft picks are worth less than before as a result - about a rotation player for a typical first.",
	},
	{
		n: 292,
		date: "2026-11-12",
		text: "3D: every device now plays out exactly the same game - the shot chart, the clips and the 3D match on every phone and computer, for everyone in the league. Games simmed before this update can still be a little off.",
	},
	{
		n: 291,
		date: "2026-11-12",
		text: "Realistic Front Offices: when a CPU team trades players away to clear cap room for a free agent, it no longer overpays. The team taking the salary gets only the picks it actually needs to take it - one contender had thrown in two first-round picks to clear $5.5M - and a team only clears room for a free agent who's worth more to it than what it gives up.",
	},
	{
		n: 290,
		date: "2026-11-12",
		text: "Realistic Front Offices: CPU teams now trade in the offseason too - on draft night, after the draft, during re-signings and in the preseason, not just during the season and free agency.",
	},
	{
		n: 289,
		date: "2026-11-12",
		text: "Realistic Front Offices: a CPU team that falls out of the playoff race now stops buying for this season - more than about 4 games behind the last play-in spot in its conference at the deadline, it sells its veterans instead. Teams also rethink their plans earlier, so one that slumps can still sell before the deadline.",
	},
	{
		n: 288,
		date: "2026-11-12",
		text: 'Realistic Front Offices: "What would make this work" and Trade Proposals ask for less. Once a deal works, they drop anything of yours the other team would still say yes without - one ask was eight players for a prospect when two of them carried the deal.',
	},
	{
		n: 287,
		date: "2026-11-12",
		text: "Realistic Front Offices: CPU teams no longer undervalue their young players on rookie deals. A losing team counted on losing a prospect when his rookie deal ran out - but he becomes a restricted free agent it can match, so it now counts on keeping him and won't trade him away cheaply.",
	},
	{
		n: 286,
		date: "2026-11-12",
		text: "Realistic Front Offices: draft picks in future drafts are priced sensibly now. A second-round pick a few years out was worth almost a third of a first to a rebuilding team, so CPU teams handed over real players for far-off seconds - a second is now worth about what one this year is, which is very little. A first a few years away is worth a bit less than one this year for the wait, instead of more.",
	},
	{
		n: 285,
		date: "2026-11-12",
		text: "Realistic Front Offices: CPU teams now pay market prices to each other too. When a team goes after a player, every other team that wants him gets to bid, and the one that would pay most gets him - paying just enough to beat the next best offer. If nobody else bids, the two teams meet in the middle instead of the seller taking the least it would accept. So a bad team's veterans go to whichever contender pays most, not to whoever asks first - the same price you have to beat when you trade for them.",
	},
	{
		n: 284,
		date: "2026-11-12",
		text: "Realistic Front Offices fix: a bad team no longer gives away young players for next to nothing just to lose more games. A better draft pick now only makes up for the games a player would win it this season, so a tanking team keeps its young core unless someone pays for it.",
	},
	{
		n: 283,
		date: "2026-11-12",
		text: "Realistic Front Offices: the CPU trade market now runs all season. Every team shops, more and more often as the deadline nears, and teams are pickier early in the season - a deal has to be clearly good for them then, and only has to be fair on deadline day. When a team gets an offer for a player, the other interested teams bid too and the best offer wins. Prices know the calendar: a contender pays more for help in a playoff race, an expiring contract is worth less to a team that won't use it this season, a long injury counts for the games it costs, and a bad team counts how much better its own pick gets when it sells. A CPU team never trades you a player for less than any other team would really give for him.",
	},
	{
		n: 282,
		date: "2026-11-12",
		text: "Realistic Front Offices: trades now run on the normal game's trade engine, made smarter. Players are priced by what they'll be worth in each coming season, so aging stars cost more than before and raw high-potential kids less. Every CPU team has a plan - all-in, playoff team, retooling or rebuilding - and pays for the seasons it cares about: a contender won't swap a starter for a teenager who won't help for years, and a rebuilder sells its veterans for picks and young players. No team sells a player for less than another team would pay for him. \"What would make this work\" no longer offers more players than your roster has room for, and the News Feed has a new Front Office section with the moves and rumors around the league. Free agency is unchanged.",
	},
	{
		n: 281,
		date: "2026-11-11",
		text: 'Game recaps are rewritten to read as the story of the game instead of a list of numbers: one opening sentence with the result and what made the game, then how it went - who led, when it turned, how it finished - then who did it for each side and why the other side lost. Sentences repeat far less from one game to the next. Games played from now on also say whose quarter or run it was ("scored 14 of his 31 in the fourth").',
	},
	{
		n: 280,
		date: "2026-11-11",
		text: 'Realistic Front Offices: "What would make this work" no longer asks for players or picks you\'ve excluded from the trade.',
	},
	{
		n: 279,
		date: "2026-11-11",
		text: 'Realistic Front Offices: the trading block, "What would make this work" and Trade Proposals now work just like the normal game\'s - a team keeps adding its own players and picks until the deal is only just worth it to it, its best offer, and "What would make this work" evens a deal up when you\'re overpaying - only the teams judge every deal with the mode\'s far smarter read. Trading block bids are no longer held to three assets: put a star up and nearly every team bids, often five firsts or a package of young players.',
	},
	{
		n: 278,
		date: "2026-11-11",
		text: "Realistic Front Offices: put a star on the trading block and nearly every team now makes an offer. Before, only teams with top draft picks could - a bid had to be worth a third of what the old trade values put on him, and those value a star far beyond any three picks or players, even though almost every team wanted him.",
		quiet: true,
	},
	{
		n: 277,
		date: "2026-11-11",
		text: "Realistic Front Offices: free agency's five stages are now just Phase 1 to Phase 5. And the mode's pages look like the rest of the game again - plain text and tables instead of colored labels: your cap space, the free agency phase, your offers and each phase's signings on Free Agents, contract notes on Team Finances and player pages.",
	},
	{
		n: 276,
		date: "2026-11-11",
		text: "Realistic Front Offices: free agents still unsigned at training camp now take the best offer they have instead of holding out into the season. Once the season starts, teams can rarely offer more than the minimum, so waiting got them nothing - before, some good players turned down every camp offer and ended up signing for the minimum.",
	},
	{
		n: 275,
		date: "2026-11-11",
		text: "3D games on phones: a phone that can draw 60 frames a second with a little room to spare keeps 60 again instead of being held to 30 - only one working flat out drops to 30.",
	},
	{
		n: 274,
		date: "2026-11-11",
		text: "3D games: fixed two more spots where a player seemed to jump - a post defender no longer darts back through the man he's guarding as he goes up, and a defender contesting a shot no longer changes direction in mid-air.",
	},
	{
		n: 273,
		date: "2026-11-11",
		text: "Realistic Front Offices: free agents' asking prices now follow the market. There's no set amount they drop by each stage any more - each stage a player's price moves toward what teams would actually pay him, so it falls as teams use up their cap room. At training camp a team can also offer less than he asks, and he may take it rather than wait into the season. And CPU teams now clear cap room for a free agent when it's worth it to them: they trade players (and a pick if they have to) to a team with room - but only once he takes their offer.",
	},
	{
		n: 272,
		date: "2026-11-11",
		text: "3D games on phones: a phone that has to work flat out to draw 60 frames a second now draws a steady 30 instead, so it doesn't overheat and dim its screen.",
	},
	{
		n: 271,
		date: "2026-11-11",
		text: "The Highlights section is gone from the live game page and the box score page.",
	},
	{
		n: 270,
		date: "2026-11-11",
		text: "3D games on phones: a phone that can't keep up with the full-detail court now draws it a bit coarser instead of stuttering along at under 20 frames a second.",
	},
	{
		n: 269,
		date: "2026-11-11",
		text: "Realistic Front Offices: players really don't like team options now. A team option makes an offer worth about 15% less to a player (it was 5%), so it cuts his chance of taking it a lot - an offer he'd take 70% of the time drops to under 10% with one, unless you pay more. Player options are unchanged.",
	},
	{
		n: 268,
		date: "2026-11-11",
		text: "Realistic Front Offices: the Contract Rules page (Team menu) opens on a new How it works tab that explains every part of the contract rules in plain words with your league's numbers - the cap, luxury tax and hard cap, max and minimum contracts, raises and options, each way to sign a player, re-signing, restricted free agents and offer sheets, each free agency stage, and how a free agent chooses.",
	},
	{
		n: 267,
		date: "2026-11-11",
		text: "Realistic Front Offices: simpler contract rules, closer to regular BBGM. Free agency still runs in stages, but now there's one max contract for everyone, you re-sign your own players in the Re-sign Players phase (once free agency starts they're open to everyone - except restricted players, whose team can still match), and a team over the cap gets one mid-level signing a summer. Gone: cap holds and the Renounce button, the taxpayer mid-level and the apron, insults, and the special contract-length limits. The hard cap, raises and options, and restricted free agency stay. In multiplayer, a team now gets a notification when its restricted free agent signs an offer sheet, and Submit on Re-sign Players, team options and Match wait while a league-mate is simming, like offers do.",
	},
	{
		n: 266,
		date: "2026-11-11",
		text: "Clone league has a new option, Clone without box scores: the copy has everything but the box scores and saved replays, so it's smaller and quicker to make.",
	},
	{
		n: 265,
		date: "2026-11-11",
		text: "Game flow and win probability charts: tap or click a spot and it stays picked - no more holding it down. Tap the right end of the chart to go back to the final score.",
	},
	{
		n: 264,
		date: "2026-11-11",
		text: "3D games: players running in late to catch a pass, box out or contest a shot now slow into it instead of stopping dead the moment it starts, and a defender sliding in to poke at the ball arrives already facing his man.",
	},
	{
		n: 263,
		date: "2026-11-11",
		text: "3D games: another big batch of animations. Ball handlers now have their own versions of the crossover (sold harder), between-the-legs (lower), behind-the-back (wrapped tighter), in-and-out, spin and euro step, and some step back with the ball swung low. Passes finish with a snap. There are floaters with a leg kick, sky hooks, dunks with a knee up, swatted blocks and charges taken with arms crossed, and defenders off the ball spread their arms wide. Players throw their hands up after fouling or slap their own arm to argue a call. New celebrations: a finger to the lips, ice in the veins, too small, raise the roof, a jersey tug and night-night. At dead balls players hike their shorts, wipe the soles of their shoes and walk with their hands on their hips.",
	},
	{
		n: 262,
		date: "2026-11-11",
		text: "3D games: fixed players looking like they jump or teleport. A player's legs could snap into a new stance in a single frame - mostly as he pulled up to a stop or came round a corner - and now his foot comes down and his hips turn back under him smoothly.",
	},
	{
		n: 261,
		date: "2026-11-11",
		text: 'Realistic Front Offices: free agency fixes. A free agent now signs on his decision day if he has an offer at his asking price, and at training camp he takes the best offer at or above his minimum instead of sitting out the season - in testing, a league\'s best free agent went unsigned into the season. Asking prices no longer climb past what a player asked when free agency opened, "to beat" only counts offers from teams that can still sign him, CPU teams now look at young prospects too, and from camp on a player signs for less than his opening ask only on a one-year deal. Contract lengths now follow the rules for everyone: 5 years with Bird rights, 4 with cap room or the mid-level, 2 with the taxpayer mid-level or a minimum deal.',
	},
	{
		n: 260,
		date: "2026-11-10",
		text: "3D games: each shooter now has his own guide hand after a jumper or free throw - kept up a beat, dropped straight to his side, or both hands held up through the landing. Some one-hand dunkers fling the free arm up, players off the ball wave for it or point teammates where to go, some thank the passer with both hands, and at dead balls players now and then stretch or shake their arms loose.",
	},
	{
		n: 259,
		date: "2026-11-10",
		text: "3D games: more ways for players to be themselves. Hop-step layups and a deep-dip free throw stroke; rebounds kept high overhead and two-handed blocks; new celebrations (a fist pump, arms spread wide, both arms up to the sky); a hand on the head or bent over with hands on knees after a blown chance; each player's own way of posting up and of waiting on the lane at the line; and off-ball defenders who deny the pass or slap the floor.",
	},
	{
		n: 258,
		date: "2026-11-10",
		text: "Realistic Front Offices: team options are now yours to decide. A contract you gave a team option shows up on the Re-sign Players page the summer before its last season - Pick up, or Decline and he joins your players to re-sign. Any you leave are picked up when free agency opens.",
	},
	{
		n: 257,
		date: "2026-11-10",
		text: "3D games: lots of new animations. Ball handlers sizing up their man now mix in between-the-legs and behind-the-back combos, rocker steps and shoulder shakes, and cross the ball over into the hand on the side they cut to. You'll also see no-look passes, double-clutch layups, pump dunks and one-legged fadeaways, and each player now has his own way of contesting shots, boxing out, setting screens, reaching for steals and arguing calls.",
	},
	{
		n: 256,
		date: "2026-11-10",
		text: "Realistic Front Offices: re-signing is one shot per player. Set your offer, see his chance of taking it, and press Submit - he signs or heads to free agency right then, with no second try (offers you don't submit are decided when free agency opens). Every re-signing decision now goes in the transaction log on the player's and the team's pages, refusals too, with the offer and the odds.",
	},
	{
		n: 255,
		date: "2026-11-10",
		text: "Realistic Front Offices: a new free agency and NBA-style contract rules, starting with each league's next offseason. Free agency now runs in 5 stages instead of 30 days - Negotiation window, Decision day 1 (the best free agents decide), Decision day 2, Open market, Training camp - and on a decision day everyone sees the offer to beat. Teams over the cap keep their own players with Bird rights and get one mid-level exception a summer; there's an apron and a hard cap; max contracts depend on years in the league; offers can have raises and player or team options; and players coming off rookie deals are restricted (their team can match). Re-signing stays in the Re-sign Players phase, and a team keeps its Bird rights into free agency. The new Contract Rules page (Team menu) has your league's numbers, every team's cap status and every signing.",
	},
	{
		n: 254,
		date: "2026-11-10",
		text: "3D games: defenders off the ball now shift with the ball. When the man with it works his defender, the help side slides over with him instead of standing still.",
	},
	{
		n: 253,
		date: "2026-11-10",
		text: "3D games: players now move like players instead of gliding from spot to spot. The man with the ball works his defender with jabs, crossovers and hesitations, and the defender answers a beat late; players off the ball step to get open and relocate around the three-point line; cuts are planted and stops are crisp. A 3D game takes a few seconds longer to get ready to watch.",
	},
	{
		n: 252,
		date: "2026-11-10",
		text: "Contracts now keep their whole history: a player's Salaries table shows which team paid each season, including a team still paying a player it released, and the first load of each league converts it (a big league can take a moment). In a shared league, everyone should reload to update before the next sim.",
	},
	{
		n: 251,
		date: "2026-11-10",
		text: "3D games: ball handlers move with the ball more - no more pulling up dead between dribbling from one spot to the next, and when a player works his man with dribble moves where he stands, his weight goes with the ball instead of standing planted.",
	},
	{
		n: 250,
		date: "2026-11-10",
		text: "Contract offers with Realistic Front Offices: paying a player more than he asks now raises your odds less than before, and offering less than he asks lowers them much more. Odds at his asking price are the same.",
	},
	{
		n: 249,
		date: "2026-11-10",
		text: "3D games: defenders now step into a player crashing the glass and box him out before the ball is off the rim. And a shooter fouled hard - going up at the rim, or by a closeout landing under him on a jumper - is sometimes knocked to the floor: the man who fouled him steps off, and a teammate comes over to pull him up before he goes to the line.",
	},
	{
		n: 248,
		date: "2026-11-10",
		text: "Realistic Front Offices: faster sims - CPU teams spend about 60% less time looking for trades each day, so a season with the mode on goes noticeably quicker. In test leagues teams did as well as before.",
	},
	{
		n: 247,
		date: "2026-11-09",
		text: "3D games on phones: the picture could drop to a blocky, low-detail size and stay there whenever the game was running at 30 frames a second (as phones often do, always in Low Power Mode). It now stays as sharp as the phone can draw in time.",
	},
	{
		n: 246,
		date: "2026-11-09",
		text: "Realistic Front Offices: CPU teams trading with each other now follow the same draft rule as with you - no giving up an earlier pick for later ones in the same draft unless the rest of the deal pays for it. In test leagues teams playing by it won more titles.",
	},
	{
		n: 245,
		date: "2026-11-09",
		text: "3D games: uniforms hang like cloth - folds fall down the jersey and gather at the waist, and the shorts hang in folds toward the hem - and skin has more to it: shadow in the creases between muscles, darker elbows and knees, and the neck shaded under the jaw. Best seen in replays and close-ups.",
	},
	{
		n: 244,
		date: "2026-11-09",
		text: "3D games: players wear proper basketball shoes - high-tops with a laced, sloped instep and a lower toe, instead of a rounded slipper - their arms and legs read as one limb rather than muscles laid on top, and their wrists are as wide as a wrist is.",
	},
	{
		n: 243,
		date: "2026-11-09",
		text: "Watching a replay no longer uploads anything to the league. (Since the shot chart update, re-watching a game in 3D could send that game's whole play-by-play back up to the cloud.)",
	},
	{
		n: 242,
		date: "2026-11-09",
		text: "3D games: players' legs are shaped like legs - the thigh runs into the knee and the calf swells below it, instead of two rods with a ball between them - and their shorts hang looser.",
	},
	{
		n: 241,
		date: "2026-11-09",
		text: "3D games: the new player bodies were too bulky - shoulders, arms, necks and ankles are now sized to real measurements, so players look like athletes rather than bodybuilders. Also fixed: hands not quite meeting when one player helps another up off the floor.",
	},
	{
		n: 240,
		date: "2026-11-09",
		text: "3D games: a player running through a turn - a cut, a curl, coming round a screen - now leans into it, the harder the turn the further, instead of running it bolt upright.",
	},
	{
		n: 239,
		date: "2026-11-09",
		text: "3D games: players have new bodies - smaller heads, muscle on their arms and legs, and fuller lighting. And each player's build is now his own: his listed height and weight set his size, his strength rating says how much of it is muscle (a strong player is wider across the shoulders with thicker arms and a trim waist; a weak, heavy one carries it round his middle), and good jumpers and runners are leaner in the leg and waist.",
	},
	{
		n: 238,
		date: "2026-11-09",
		text: "3D games: players are now drawn by your device's graphics card when that is faster (the game checks for itself, and keeps the old way where it isn't). They look the same as before - games should just run smoother, with fewer hitches at replays and camera cuts.",
	},
	{
		n: 237,
		date: "2026-11-09",
		text: "3D games: with a second or so left in a quarter after a basket, a free throw or the ball going out, the ball is now inbounded before the horn - often with a heave just too late - instead of the horn going with the ball on the floor. And a team running out the clock at the end of a game it has won dribbles it out with the defense standing off.",
	},
	{
		n: 236,
		date: "2026-11-09",
		text: "3D games: a missed shot is now set up the way that player's made shots are - off the dribble or off a pass - so corner threes no longer miss far more often than threes from above the break; and a player crashing the glass now meets a box-out more often.",
	},
	{
		n: 235,
		date: "2026-11-09",
		text: "Realistic Front Offices: CPU teams count money spent past the cap as a little cheaper than before, so bad teams use their cap room instead of sitting on it. In test leagues they bottomed out about half as often and won more titles. And a CPU team no longer takes your later pick in the same draft for its earlier one (picks 30 and 58 for 15) unless the rest of the deal pays for it.",
	},
	{
		n: 234,
		date: "2026-11-09",
		text: "Shot chart: tapping a shot now plays it from the spot shown on the chart, and a game's chart on its box score is updated each time you watch it in 3D, so the dots match where the shots were taken.",
	},
	{
		n: 233,
		date: "2026-11-09",
		text: "3D games: fewer offensive-foul whistles - moving screens about once a game instead of four or so, and travels as common as charges.",
	},
	{
		n: 232,
		date: "2026-11-09",
		text: "3D games: after missing a shot in close - a layup, a hook, a floater - the shooter now follows it in, fights his man for position or backs out, instead of standing where he landed.",
	},
	{
		n: 231,
		date: "2026-11-09",
		text: "3D games: players out injured now sit on the bench in street clothes (a suit) instead of their uniform, and don't take part in the warm-ups.",
	},
	{
		n: 230,
		date: "2026-11-09",
		text: "3D games: when the ball goes out of bounds, the players nearest it now and then point the way they think it should go.",
	},
	{
		n: 229,
		date: "2026-11-09",
		text: "3D games: a blocked shot now brings the crowd up (the home fans most of all for a home block) and the blocker's bench up off its chairs.",
	},
	{
		n: 228,
		date: "2026-11-09",
		text: "3D games: when the ball is stolen from a player he now and then puts an arm up at the official as if he was fouled, or runs back with his hands on his head.",
	},
	{
		n: 227,
		date: "2026-11-09",
		text: "3D games: after a shooting foul the shooter now and then claps it (he drew it) or has a word with the official, and the man who fouled him sometimes stands with his hands on his hips or on his head.",
	},
	{
		n: 226,
		date: "2026-11-09",
		text: "3D games: after a foul with the ball to be taken out, the official who called it runs over to the scorer's table and reports it before getting back to his spot.",
	},
	{
		n: 225,
		date: "2026-11-09",
		text: "3D games: free throw routines vary much more from player to player - how many bounces and how quick, a backspin flip first or after, a breath before - and each player keeps his own every trip to the line.",
	},
	{
		n: 224,
		date: "2026-11-09",
		text: "3D games: on a substitution an official now waves the man at the scorer's table on, and a player going off on defense points out the man he was guarding for the one taking his place.",
	},
	{
		n: 223,
		date: "2026-11-09",
		text: "3D games: mid-range jumpers now and then come from along the baseline - a corner shooter drifting up for a kick-out, or a ball handler taking it baseline to pull up - not only from the elbows and the top.",
	},
	{
		n: 222,
		date: "2026-11-09",
		text: "3D games: a missed layup or shot in the paint no longer always comes off the front of the rim - now and then it's off the side, or too hard off the glass. And a man backing down in the post has his defender on his back the whole way, giving ground at his pace instead of backing off ahead of him.",
	},
	{
		n: 221,
		date: "2026-11-09",
		text: "3D games: on an and-one jumper, the man who fouls the shooter is right up on him as he lets it go, not a few feet short of him.",
	},
	{
		n: 220,
		date: "2026-11-09",
		text: "3D games: as a defensive rebound comes down, only the nearest man of the other team swipes at it now - the rest still crowd in but keep their hands to themselves - and the man who goes up with the rebounder does so less often, more for a good rebounder.",
	},
	{
		n: 219,
		date: "2026-11-09",
		text: "3D games, defensive rebounds: a good rebounder now and then rips one down with one hand, the other coming in to it on the way down; the man who comes down with a board mostly throws the outlet up over his head (bigs more than guards); and with a man right on him he turns a shoulder into him before he throws it. A pass thrown over the top is caught up high and a bounce pass down low, and after a three the passer now and then points back at the shooter while the man who was guarding him puts his hands on his head.",
	},
	{
		n: 218,
		date: "2026-11-09",
		text: "Multiplayer: when a league-mate is simming the game between your two teams, Watch on it (on the schedule, or anywhere else) now joins their live game in step with them, instead of starting your own copy of it at your own pace. Sim is turned down for that game while it's live.",
	},
	{
		n: 217,
		date: "2026-11-08",
		text: '3D playoff games: the score bug has a strip across its top with the round, which game of the series it is and how the series stands going into it ("1st round · Game 4 · BOS leads 2-1"). The Series section says "wins" once a team has taken the series.',
	},
	{
		n: 216,
		date: "2026-11-08",
		text: "3D highlights: a play opened on its own now starts as the play does - the clock at its time instead of running down from the start of the quarter, and both benches and coaches already in their places instead of walking back from the break.",
	},
	{
		n: 215,
		date: "2026-11-08",
		text: 'Game recaps read more like a writer\'s: "tied at 78 going into the fourth" instead of "78-78", "trailed by a point" instead of "were 1 down", "They" instead of a team\'s name three sentences running, and no "went in front for good in the third" after a comeback that became a rout.',
	},
	{
		n: 214,
		date: "2026-11-08",
		text: 'Player Stats for one playoff series ("R1 vs DET"): its advanced stats - BPM, win shares, PER - are now measured against the whole playoffs, the same as the Playoffs view, so with only the first round played the two match.',
	},
	{
		n: 213,
		date: "2026-11-08",
		text: "3D arena: the scorer's table is now a lit LED board running sponsor ads, as are the boards round the floor and the panels on the basket stanchions - made-up sponsors, or your own (the team court editor has a new Arena ads section: words, colors or a picture). Photographers sit in a row along each baseline, and the bench chairs have towels over them.",
	},
	{
		n: 212,
		date: "2026-11-08",
		text: "Realistic Front Offices: CPU teams treat money as twice as dear as before, so they pay less for middling free agents and run up far less luxury tax without a title to show for it. In tests they won more titles and playoff rounds, though a little less in the first season.",
	},
	{
		n: 211,
		date: "2026-11-08",
		text: "3D games: on a missed shot, defenders in close with a man right on their back turn and box him out as the ball comes off the rim, and he fights them for it.",
	},
	{
		n: 210,
		date: "2026-11-08",
		text: "3D games: when a team fouls on purpose late to stop the clock, the man they go after is the one who goes to the line - the ball gets to him first - not whoever was bringing it up.",
	},
	{
		n: 209,
		date: "2026-11-08",
		text: "A playoff game's page shows the playoff bracket instead of the standings - every round's series and wins as they stood at that game.",
	},
	{
		n: 208,
		date: "2026-11-08",
		text: "Multiplayer: your own game's Watch and Sim buttons no longer grey out while a league-mate's live game is on (\"Live game in progress\" up top). When they are greyed out for a real reason - the scheduled sim is about to run - tapping one tells you why.",
	},
	{
		n: 207,
		date: "2026-11-08",
		text: 'Live game on phones: the "On the Court" heading is gone - the score by quarter and the players on the floor sit straight under the tabs.',
	},
	{
		n: 206,
		date: "2026-11-08",
		text: "3D games: while a jumper is in the air everybody plays it - defenders turn and sink toward the glass, and the men going in for the rebound or getting back set off as they read the shot instead of standing still until it hits the rim. And the shooter no longer stands frozen where he landed: most back off a few steps still holding the follow-through, and a big sometimes follows his shot in.",
	},
	{
		n: 205,
		date: "2026-11-08",
		text: "3D games: a defender closing out late on a shooter pulls up once the shot is away, instead of still chop-stepping in at him a second after it - and one still sprinting at him as it goes up sometimes leaps and flies on past. Also fixed: a defender pushed through a screener, a post-up with nobody guarding it, and coaches snapping between poses.",
	},
	{
		n: 204,
		date: "2026-11-08",
		text: "3D games: a good passer now and then swings the ball on with one hand straight off his catch, instead of always a two-handed chest pass.",
	},
	{
		n: 203,
		date: "2026-11-08",
		text: "3D games: a shooter getting to his spot just as a catch-and-shoot pass comes opens up to the rim on his last steps, instead of swinging right round after he catches it - and coming in across the floor he carries on into the catch rather than stopping to wait for it.",
	},
	{
		n: 202,
		date: "2026-11-07",
		text: "3D games: a man called for traveling on a drive along the baseline takes his extra steps facing where he is going, instead of jogging sideways and backward, and a run bent round another man no longer darts round him in a blink.",
	},
	{
		n: 201,
		date: "2026-11-07",
		text: "3D games: far fewer players slamming to a stop or bursting off from nothing. A defender chasing his man into the corner slows into it instead of stopping dead on the sideline, the long step through to the rim off a post move pushes off like a real step, a hard dribble at a man builds up, and a defender stepping aside for a teammate never steps into a shooter going up at the rim.",
	},
	{
		n: 200,
		date: "2026-11-06",
		text: "Live gamecast on a phone: the players on the court are up top under the tabs, so the plays coming in early in a game no longer push them down out of sight. And their line reads points, rebounds, assists, then field goals.",
	},
	{
		n: 199,
		date: "2026-11-06",
		text: "3D games: a cutter who catches the ball on his way to the rim takes it in stride into his dunk or layup instead of stopping dead to catch it, and a man trailing the play up the floor gets going and pulls up like a runner instead of starting and stopping at full speed.",
	},
	{
		n: 198,
		date: "2026-11-06",
		text: "Live gamecast: the play, pause and speed controls sit up in the top corner of the court, clear of the score bug. And on any shot chart, click a shot to watch it in 3D right there in its popup.",
	},
	{
		n: 197,
		date: "2026-11-06",
		text: "Realistic Front Offices: CPU teams no longer give up picks just to get rid of a cheap player they could simply release. In tests this won as many titles and more playoff rounds, with fewer bottom-of-the-league seasons.",
	},
	{
		n: 196,
		date: "2026-11-06",
		text: 'Game recaps, round two: numbers the way a wire story prints them (nine in words, 11 in figures, "No. 7" seeds in playoff headlines), fewer lines about how many lead changes there were or who was still out hurt, and more ways of saying a player beat his scoring average.',
	},
	{
		n: 195,
		date: "2026-11-06",
		text: "Live gamecast: the five on the court for each team are listed point guard to center, the way the depth chart lines them up.",
	},
	{
		n: 194,
		date: "2026-11-06",
		text: "Live gamecast: the players on the court beside the game no longer show skill badges, so their names show in full.",
	},
	{
		n: 193,
		date: "2026-11-06",
		text: 'Game recaps read more like a sportswriter wrote them: close games go straight to how they were decided instead of always giving the halftime score, and stock lines like "in a losing cause", "more than doubled his average" and bracketed double-doubles turn up far less. Plainer verbs, runs told more than one way, and wire-style lists.',
	},
	{
		n: 192,
		date: "2026-11-06",
		text: "Box scores and the live gamecast: every play in the play-by-play opens with its down arrow and plays right there in 3D, with the score and fouls as they were. Point at a shot on the shot chart to see who took it, from how far, when and the score. And a finished game's highlights show its biggest plays in the order they happened, not just the last few.",
	},
	{
		n: 191,
		date: "2026-11-06",
		text: "Live gamecast: the players on the court beside the game show their skill badges and open the ratings and stats popup, with game score and minutes added.",
	},
	{
		n: 190,
		date: "2026-11-06",
		text: "Highlights on a box score and in the live gamecast: tap the down arrow on one and it plays right there in 3D under its row, in a second or so - no trip over to the replay.",
	},
	{
		n: 189,
		date: "2026-11-06",
		text: "3D games: a player stepping aside on his way - into the way of a pass, off another man, to the side of a shot - eases over like a real step instead of jerking a couple of feet over and back in a blink.",
	},
	{
		n: 188,
		date: "2026-11-06",
		text: "Realistic Front Offices: CPU teams under the minimum payroll no longer treat future salary as free, so they stop paying bench veterans two or three times their asking price for years and keep their room for better players. In tests the teams that did this won more titles.",
	},
	{
		n: 187,
		date: "2026-11-06",
		text: "3D games: a reverse layup carries under the rim to the far side at a believable pace instead of jumping across in the last instant before he leaves the floor. A loose ball on the floor is picked up when the man gets to it, never pulled up into his hands from feet away, and a hand up at a jump shot is always a step to the side, so the shot stays in sight.",
	},
	{
		n: 186,
		date: "2026-11-06",
		text: "3D games: a man backing his defender down in the post has the defender give ground step for step. A heave at the buzzer after the ball goes out of bounds goes up before the horn - the clock no longer starts when the ball is handed to the man taking it out. And a run of small fixes: a spot-up shooter on the wing waits turned to the rim, nobody runs back through a shooter or a man who just got the ball, and a teammate coming to help up a man who took a charge doesn't walk into the man who ran into him.",
	},
	{
		n: 185,
		date: "2026-11-06",
		text: "Realistic Front Offices: CPU teams no longer re-sign or play players they could replace with a minimum free agent, and let an expiring player go when his salary buys more as cap room. Fewer pointless cuts and signings at the end of benches, and in tests the teams that did this won more.",
	},
	{
		n: 184,
		date: "2026-11-06",
		text: "3D games: a missed jumper misses like a real one - off the front of the rim, long off the back, off the side, in and out, now and then off the glass or an air ball - by how good a shooter he is, how far out, and where the rebound goes. The ball off a dunk drops through the net away from the dunker instead of bouncing up through his legs. And a high flier puts on a show at the rim about as often as his ratings say, a great shooter pulls up from way out now and then, instead of either depending on a run of luck.",
	},
	{
		n: 183,
		date: "2026-11-06",
		text: "3D games: a man sent in after a rebound coming off the rim gets there to take it, and a loose ball never flies the last few feet into a man's hands in a blink.",
	},
	{
		n: 182,
		date: "2026-11-06",
		text: "Tabbed layout: in a live game, the Box Score tab shows the box score a quarter at a time too - the whole game, or any quarter so far.",
	},
	{
		n: 181,
		date: "2026-11-06",
		text: "Tabbed layout: a shot chart - every shot right where the 3D court had it taken, made or missed, for both teams or one. On a box score it shows for games watched in 3D from now on (it is kept with the saved replay, a few kilobytes a game).",
	},
	{
		n: 180,
		date: "2026-11-06",
		text: 'Tabbed layout: plays read the way a broadcast lists them - "Devin Booker misses three point jumper", "Wendell Carter Jr. makes layup (Franz Wagner assists)". In a live game, tap a play to see it again on the court. On a box score whose replay is saved: a Play-by-Play tab and the game\'s highlights, where tapping any play opens the replay right at it in 3D, and the box score a quarter at a time.',
	},
	{
		n: 179,
		date: "2026-11-06",
		text: "Tabbed layout: the box score's score line fits on one row on a phone, with the score by quarter in the middle. New Win Probability and Game Flow charts. Game leaders take less room. The line shows each side's spread, chance to win and whether it covered. Season series games link to their box scores, the standings show both teams' conferences as they stood that day, and each team's recent games are listed.",
	},
	{
		n: 178,
		date: "2026-11-06",
		text: "3D games: a few rare glitches fixed. The ball no longer flies the last few feet to a rebounder in a blink, nobody fights for rebounding position against nobody, a defender running past a shooter contests from beside him instead of hiding the shot from the camera, and after the final buzzer the picture goes straight from the interview to the last look round the arena.",
	},
	{
		n: 177,
		date: "2026-11-05",
		text: "With the tabbed layout turned on, a game's box score page is laid out as a game recap too: the score across the top, then Recap, Box Score, Team Stats and Game Flow tabs. The Recap tab has the game leaders and team stats, the story of the game with how it went, and the line, the season series, the standings and the team's games.",
	},
	{
		n: 176,
		date: "2026-11-05",
		text: "The tabbed live game layout has a Highlights section: the game's dunks, threes and blocks so far, each with a Replay button that shows it again on the 3D court before going back to the game.",
	},
	{
		n: 175,
		date: "2026-11-05",
		text: "New, to try: a tabbed layout for live games, like a sports app's gamecast. The court with the score by quarter and who's on the floor beside it, the latest play right under the picture, then Gamecast, Box Score, Play-by-Play, Team Stats and Game Flow tabs. Turn it on with \"Tabbed layout\" at the foot of the live game page, or in Global Settings.",
	},
	{
		n: 174,
		date: "2026-11-05",
		text: "Fixed: in 3D games, a player off the ball who drifted over to keep his spacing could jump back across the floor in an instant for his next run. He now only drifts when he has time to get back.",
	},
	{
		n: 173,
		date: "2026-11-05",
		text: "3D games: a defender sliding with his man no longer spins his whole body round on his planted foot when a pass whips past him - his head goes to the ball and his body follows more slowly.",
	},
	{
		n: 172,
		date: "2026-11-04",
		text: "Fixed: a player whose photo was turned into a face no longer shows broken pictures for his past seasons in his career gallery - every season shows his face. Any player picture that fails to load now falls back to the face too.",
	},
	{
		n: 171,
		date: "2026-11-04",
		text: "3D games: free throws are shown from the normal court camera. The close-up from under the basket is gone.",
	},
	{
		n: 170,
		date: "2026-11-04",
		text: "3D games: a point guard who's about to have a pass stolen no longer stands alone dribbling in his own backcourt for seconds and then throws it the length of the floor to a defender. He brings it up first, and the pass is picked off up the floor.",
	},
	{
		n: 169,
		date: "2026-11-04",
		text: "3D games: defenders closing out on a shooter no longer dart the last few feet faster than anyone can run. They get only as far as they really could, so a late closeout looks late.",
	},
	{
		n: 168,
		date: "2026-11-04",
		text: "3D games: a shooter waiting for a pass he'll shoot right away now stands with his feet already turned toward the rim, instead of facing the passer and spinning round after the catch.",
	},
	{
		n: 167,
		date: "2026-11-04",
		text: "3D games on slower phones: the players on the court are now redrawn before the bench and the crew, so their running and moves look smoother, and each frame does a little less work.",
	},
	{
		n: 166,
		date: "2026-11-04",
		text: "3D games: players move more like people. Pulling up off the dribble, a player turns toward the rim on his last dribbles instead of stopping and spinning round on the spot. A drop step or up-and-under on the block now really fakes or steps round his man instead of dribbling into him. A help defender stepping up no longer zips across sideways, and legs keep running while a player swipes at the ball or goes up for a block.",
	},
	{
		n: 165,
		date: "2026-11-04",
		text: "3D games: on a fast break that ends in a turnover, the ball handler no longer dribbles all the way to the rim and kicks it back out for no reason first.",
	},
	{
		n: 164,
		date: "2026-11-04",
		text: "Fixed: on a phone, opening More team info on a team page no longer makes the whole page wider than the screen - its stats now wrap onto new lines.",
	},
	{
		n: 163,
		date: "2026-11-04",
		text: "Realistic Front Offices: CPU teams with a full roster stop signing and releasing the same end-of-bench players over and over during the season. A team no longer signs a player it would turn around and cut, and signs at most one player a day when it has no open spot.",
	},
	{
		n: 162,
		date: "2026-11-04",
		text: "3D game: no more open lanes passed up. When a player with the ball pulls up, floats it or passes out with nothing between him and the rim, a help defender now steps into the lane in front of him first and contests the shot - then gets back to his own man. Also fixed a player jumping several feet in a frame after picking up a loose ball, and defenders running backwards at full speed on their way back.",
	},
	{
		n: 161,
		date: "2026-11-03",
		text: "Team pages have a More team info dropdown, like Basketball Reference's: points scored and allowed, SRS, pace, offensive, defensive and net rating and expected record, each with where it ranks in the league, plus attendance and how the playoffs went. It also shows the preseason odds - the title price and the win total, and whether the team went over or under - saved from this season on.",
	},
	{
		n: 160,
		date: "2026-11-03",
		text: "Realistic Front Offices: swapping picks in the same draft makes sense again. A CPU team offered the pick just ahead of its own, straight up for its own, takes it as offered - it no longer asks to be paid a first and a second to move up - and two picks in the same draft no longer each count their full doubt.",
	},
	{
		n: 159,
		date: "2026-11-03",
		text: "Multiplayer: if the game ends up running twice on one device (an older copy still alive in the background), only the newest copy syncs now - the other one stops and asks to be reloaded. A ready-up step that has already run can no longer be run again, and the device in charge of simming can apply a repair file from Multiplayer Sync.",
	},
	{
		n: 158,
		date: "2026-11-03",
		text: "Realistic Front Offices: CPU teams now judge how good each team is with only as much doubt as their forecasts have actually earned, so a team with a real shot at the title pushes harder for it, and a team without one is quicker to build for later.",
	},
	{
		n: 157,
		date: "2026-11-03",
		text: "Realistic Front Offices, COLA leagues: CPU teams now project each team's draft-lottery chances along the path it is likely to take - deep playoff runs and lottery wins cost a team its chances, missing the playoffs builds them up - so a contender values its own future firsts less and a team out of contention values its own more.",
	},
	{
		n: 156,
		date: "2026-11-03",
		text: "Realistic Front Offices: CPU teams now expect their players to miss some games to injuries still to come, not just the ones they have now - so a team built around one star no longer counts on him playing every game, and a deep bench counts for more.",
	},
	{
		n: 155,
		date: "2026-11-03",
		text: "3D game: the bench greets a man coming off. As he gets back to his chair, the teammate sat nearest him on either side gets up, steps out in front of the bench and slaps him a high five, then they all sit back down.",
	},
	{
		n: 154,
		date: "2026-11-03",
		text: "3D game: when a player goes down hurt, his team's athletic trainer jogs out from the end of the bench and gets down beside him until he is up.",
	},
	{
		n: 153,
		date: "2026-11-03",
		text: "3D game: the post-game interview. After the handshakes, the winners' top scorer heads to the near sideline, the sideline reporter comes out to him with her microphone, and the broadcast closes in on the two of them for his interview before its last look round the building.",
	},
	{
		n: 152,
		date: "2026-11-03",
		text: "3D game: a close-up at the free throw line. On most trips to the line the broadcast cuts in close on the shooter from under the basket - his face, the ball in his hands, the crowd soft behind him - through his routine before the first free throw, then back out for the shot.",
	},
	{
		n: 151,
		date: "2026-11-03",
		text: "Player recaps: season and retirement pieces are now written like a beat reporter's - a real angle and lede, the moments that defined the year or the career, no stat-sheet recitals. Retirement headlines are about the player himself, not a count of years and rings, and the retirement piece now knows his best single games.",
	},
	{
		n: 150,
		date: "2026-11-03",
		text: "Coarse Ratings: a retired player's full ratings now show on every page - old team rosters, the Hall of Fame, draft history and the rest - not just his own player page. Tables still sort him in with the tens digit his ratings would show.",
	},
	{
		n: 149,
		date: "2026-11-03",
		text: 'Team History: in multi team mode, the "All seasons / With you as GM" toggle and the GM season marks are gone.',
	},
	{
		n: 148,
		date: "2026-11-03",
		text: "Key plays settings: the close-game boxes can be cleared to type a new number - they used to snap straight back to the old one.",
	},
	{
		n: 147,
		date: "2026-11-03",
		text: "3D game: a player who blows a layup or a dunk that the other team rebounds puts his hands on his head on the way back up the floor; and beaten at the buzzer, one of the losers stands there stunned with his hands on his head.",
	},
	{
		n: 146,
		date: "2026-11-03",
		text: "3D game: coaches call timeout like coaches do - the head coach of the team taking it throws his hands up in a T at the official just before the whistle, then heads for the huddle.",
	},
	{
		n: 145,
		date: "2026-11-03",
		text: "3D game: blocks get the slow-motion replay too. When a block sends the ball out of bounds or play stops right after it, the broadcast shows it again in slow motion from courtside, with both the shooter and the man who sent it back in the picture.",
	},
	{
		n: 144,
		date: "2026-11-03",
		text: "3D game: the home crowd claps. After every home basket the fans in their seats give it a hand, and with a game on the line they clap along with DEFENSE on the boards. And the arena lights now reflect off the floor as streaks running toward the camera instead of round white spots.",
	},
	{
		n: 143,
		date: "2026-11-03",
		text: "3D game: joints that bend like a real body's. A planted foot pivots on the floor as a player turns instead of staying put with his ankle wrung round, arms keep their length going to the hips, the knees or the ball (the forearm used to shrink by a third), a defender reaching for a man beside him reaches out to the side instead of swinging his arm up behind his back, and no wrist, elbow or shoulder bends past where a real one stops.",
	},
	{
		n: 142,
		date: "2026-11-02",
		text: "3D game: clean profiles again. A player turning toward the camera no longer has his face squeezed and slid over the side of his head (it read as his face and profile smeared together); he shows his profile, then his face, never one faded in over the other.",
	},
	{
		n: 141,
		date: "2026-11-02",
		text: "3D game: a more lifelike crowd. Fans in the stands are drawn the right height for the camera (they used to come out short and squat), each row's heads in front of the laps behind it the way raked seats look, not lined up in a grid; they wear what people wear - lots of black, gray, white and navy, open jackets, caps, a third of the house in the home colors - and sit in the shadow of the row in front. No cost to how smoothly the game runs.",
	},
	{
		n: 140,
		date: "2026-11-02",
		text: "Every team the game comes with now has its own home floor, laid out like an NBA floor: its wood, its lane painted in its colors with a trim, a colored border with the city and name on the baselines, its logo at center court and its second logo out on the wings. Customize Court opens on it and can change any of it - and courts can now have a two-tone floor, a lane trim, a filled center circle, your own baseline text and a choice of lettering.",
	},
	{
		n: 139,
		date: "2026-11-02",
		text: "3D game: more lifelike players. Hands are sized like real hands - slimmer palms and fingers, with a thumb muscle at the base - the back of the head is shaded round instead of flat, faces turn properly at three-quarter angles, and the biceps bunch up as the elbow bends.",
	},
	{
		n: 138,
		date: "2026-11-02",
		text: "3D game: a scorer celebrates on his way back down the floor - a fist pump, a three up, a flex - and points at the man who set him up as he runs, instead of standing still for it. He only stops to celebrate on a dead ball, like an and-one or a timeout.",
	},
	{
		n: 137,
		date: "2026-11-02",
		text: "Live game: a settings button next to the Key plays picker sets what the key plays show - a close game shown in full (within how many points, how many minutes left, and whether that comes on top of the length you picked) and what makes the reel (dunks, threes, blocks, steals, and-ones, buzzer beaters, lead changes, big swings, your team first). The close game settings moved here from Global Settings.",
	},
	{
		n: 136,
		date: "2026-11-02",
		text: "Finals: center court now looks like a real finals floor - the championship trophy 27 feet tall with the team logo in front of it, a dozen feet across. On an imported floor picture, the picture's own center logo is painted out for the finals (wood carried in over it, the half-court line drawn on through). The trophy now comes with the game instead of loading from imgur, so it always shows.",
	},
	{
		n: 135,
		date: "2026-11-02",
		text: "Courts: each team's court can now have its own Home opener, Playoffs and Finals versions - pictures laid on the floor for those games, like the Playoffs logo in two spots, swapped for the Finals logo in the Finals. Pick the version under the court editor's preview and place them right there; they're saved in the court's file too. League court decals can also pair diagonally now.",
	},
	{
		n: 134,
		date: "2026-11-02",
		text: "Realistic Front Offices: CPU teams may trade first-round picks in back-to-back years again - they just value them more the fewer they have, and contenders within reach of a title spend them freely. They can also flip a player they just traded for when it clearly pays, like a rebuilding team moving a veteran it took on as salary filler.",
	},
	{
		n: 133,
		date: "2026-11-02",
		text: "3D game: smoother bodies. A player's feet no longer jump as he comes down off a stride into his stance, a reach or swipe at the ball eases onto it instead of the hand snapping there, and arms drawn in against a man ease in and out instead of popping - about 90% fewer sudden hand and foot jumps.",
	},
	{
		n: 132,
		date: "2026-11-02",
		text: "Realistic Front Offices: CPU teams deal less freely. They guard their first-round picks (never two drafts in a row without one), don't flip a player they just traded for, and every trade they make in a season raises the bar for the next.",
	},
	{
		n: 131,
		date: "2026-11-02",
		text: "Latest BBGM updates: a Sign all rookies button on the re-signing page, Team History marks the seasons you were the GM (with an All seasons / With you as GM toggle), award settings let you pick which awards to import, signing errors are handled more gracefully, and football overtime fixes.",
	},
	{
		n: 130,
		date: "2026-11-02",
		text: "3D game: playoff crowds do a color-out - every section in one of the home team's colors, the next section in the other, all the way round (green, white, green in Boston; wine, gold, wine in Cleveland), and no visiting fans' colors in the stands.",
	},
	{
		n: 129,
		date: "2026-11-02",
		text: "Finals: an imported court picture now gets the championship trophy at center court too, with the home logo in front of it - the same finals look as every other court - instead of the trophy going on the scorer's table.",
	},
	{
		n: 128,
		date: "2026-11-02",
		text: "Sportsbook: futures are priced for real now - they count injuries (a star out for the year no longer counts in full), and the title favorite is priced to what actually happens instead of as a long shot that wins twice as often. Win totals are -110 a side, not -135. Bets can't be cancelled once placed. A parlay can't stack picks that depend on each other (moneyline and spread on the same game, a team to win the title and its conference). Ties push, divisions go by the real tiebreakers, props agree with the game's total and spread, and the bet slip tells you when a line has moved or a game has started.",
	},
	{
		n: 127,
		date: "2026-11-02",
		text: "Spreads: a finished game now keeps the spread it had before tipoff, instead of switching to a different number once it's over. And the spread shown before a game is now exactly the one the game is played and graded against - it had been reading the wrong starting five, so it was often a point or two off (more in the playoffs).",
	},
	{
		n: 126,
		date: "2026-11-02",
		text: "3D game: in crunch time a star who scores lets it out - his own roar or chest pound, or a flex - where earlier in the game he'd just get back.",
	},
	{
		n: 125,
		date: "2026-11-02",
		text: "3D game: a good shooter who swings or kicks the ball on doesn't stand and watch - he relocates round the arc to the next open spot, ready for it to come back; and a shooter's shot fake sends his man flying by wide of him, not through him.",
	},
	{
		n: 124,
		date: "2026-11-02",
		text: "3D game: a good shooter open for it calls for the ball with his hand up nearly every time; a poor shooter only now and then.",
	},
	{
		n: 123,
		date: "2026-11-02",
		text: "3D game: after a charge, the teammate coming to pull the man up comes round the far side from the man who ran into him - nobody runs through anybody - and the man who ran into him walks straight off rather than stepping round over the downed man's legs.",
	},
	{
		n: 122,
		date: "2026-11-02",
		text: "Realistic Front Offices: CPU teams now count what losing does to their own players - the worse a team gets, the less its players want to re-sign - so fewer teams gut themselves into 5-win seasons.",
	},
	{
		n: 121,
		date: "2026-11-02",
		text: "3D game: crunch time looks like it - in the last minutes of a close game, the star gets the ball and his teammates clear out so he can go one-on-one.",
	},
	{
		n: 120,
		date: "2026-11-02",
		text: "3D game: with the shot clock all but gone, a star making his own shot goes to the tough one - the step-back, the fadeaway, the side-step - rather than a plain pull-up.",
	},
	{
		n: 119,
		date: "2026-11-02",
		text: "3D game: more extra passes - off a pick-and-roll the ball handler kicks it out to the wing and the wing swings it on to the open man in the corner, and a rolling big who catches the pocket pass kicks it out to the corner himself.",
	},
	{
		n: 118,
		date: "2026-11-02",
		text: "3D game: a star coming off a pick-and-roll switched onto a big takes him on - steps back or crosses him over. A point guard who sees the floor now and then points a teammate to the spot he wants him in.",
	},
	{
		n: 117,
		date: "2026-11-02",
		text: "New players' faces match today's NBA: about one in five American-born players is white, as on real rosters, white players no longer come out with skin dark enough to pass for someone else, and skin tones overall are drawn from the real league's. A player born in France now usually looks it, and one born in Czechia, the UK, the U.S. Virgin Islands, the Republic of the Congo or Cabo Verde gets a face from his own country rather than a random one.",
	},
	{
		n: 116,
		date: "2026-11-02",
		text: "3D game: a good ball handler sets up his screen - as the big comes up to screen for him, he takes his man a hard dribble the other way, then comes back off it shoulder to shoulder with the screener - and the better the handler, the more often he waves the screener up. A fine passer, on his dribble, has his head up reading the floor, looking round to his teammates one after another; a lesser one keeps his eyes on what's in front of him.",
	},
	{
		n: 115,
		date: "2026-11-02",
		text: "3D game: charges and and-ones look right - the man who takes the charge is the one the driver runs into, the driver backs straight off him, and the teammate who pulls him up turns to face him before taking his hands; fouled on a jumper by his own man, that man is out on the shooter as it goes up, not still back in the help.",
	},
	{
		n: 114,
		date: "2026-11-02",
		text: "3D game: a team's best creator runs the offense - the star with the handle has the ball in his hands and gets his own shot off the pick-and-roll or one-on-one, setting up his pull-up with a hesitation - while bigs and role players keep it simple and move the ball on, no crossover show where they stand.",
	},
	{
		n: 113,
		date: "2026-11-02",
		text: "Realistic Draft Prospects: fixed lopsided ratings - no more top prospects all at 98 mid-range, free throws are no longer capped around 51 for the whole class, and skills no longer pile dozens of prospects onto one exact number. Only classes generated from now on change.",
	},
	{
		n: 112,
		date: "2026-11-02",
		text: "Realistic Front Offices: free agency always works the realistic way when the mode is on - the separate Realistic Free Agency setting is gone.",
	},
	{
		n: 111,
		date: "2026-11-02",
		text: "Face aging is easier on hair and beards: most players keep their hairstyle their whole career, balding is rare and comes late, big full beards are uncommon, and plenty of players stay clean-shaven. Faces age instead through lines and filling out, slowly, each player at his own pace.",
	},
	{
		n: 110,
		date: "2026-11-02",
		text: "Realistic Front Offices: CPU teams never give away a draft pick for nothing - not to each other, and not to you.",
	},
	{
		n: 109,
		date: "2026-11-01",
		text: "3D game: a heave from way out - farther than any jumper - goes up two-handed like a floater: the ball brought up from his waist and pushed up and out at the rim off both hands, lofted, not thrown.",
	},
	{
		n: 108,
		date: "2026-11-01",
		text: "3D game: scoring off an assist, the scorer turns and points at the man who found him - easy about it, the arm loose and the elbow bent, not stuck out straight - or points back at him on his way down the floor.",
	},
	{
		n: 107,
		date: "2026-11-01",
		text: "3D game: a dribbler's free arm is a forearm across at his man's chest, and it comes in off the man's face when they are right up on each other - no more hand on the defender's nose.",
	},
	{
		n: 106,
		date: "2026-11-01",
		text: "Realistic Front Offices: CPU teams draft the most talented prospect more often instead of reaching for a position they're short at (point guards especially). Position fit now only decides between prospects who are about equally good.",
	},
	{
		n: 105,
		date: "2026-11-01",
		text: "3D game: a hack, a reach-in or a poke at the dribble lands on the ball or the arm it goes for - the defender's hand meets it, not a swipe at the air beside it.",
	},
	{
		n: 104,
		date: "2026-11-01",
		text: "3D game: a real tip-off - the two jumpers crouch set at the circle, the official tosses it and backs out of the way, and the winner goes up with one arm and taps the ball with his hand at the top of his jump while the other man reaches for it. The camera comes in close on the circle for it, and the crowd is on its feet for the opening tip.",
	},
	{
		n: 103,
		date: "2026-11-01",
		text: "3D game: officials signal with their hands as NBA refs do - a fist up for the whistle, a fist for a charge, fists rolled for a travel, three fingers up for a three.",
	},
	{
		n: 102,
		date: "2026-11-01",
		text: "3D game: after a steal or a rebound, players take off up the floor each in their own time, not all on the same count.",
	},
	{
		n: 101,
		date: "2026-11-01",
		text: "3D game: a defender's hand up at the man with the ball stays short of his face - no more palm on his nose - and the dribbler's free arm comes in off a man's face too.",
	},
	{
		n: 100,
		date: "2026-11-01",
		text: "3D game: hanging on the rim after a dunk, his fingers hook over the iron.",
	},
	{
		n: 99,
		date: "2026-11-01",
		text: "3D game: baggy shorts hang down off a raised knee or a seated player's thighs instead of wrapping them like a sleeve.",
	},
	{
		n: 98,
		date: "2026-11-01",
		text: "3D game: hands go up for a rebound spread and cupped for the ball, and a high or low five is open palm to open palm.",
	},
	{
		n: 97,
		date: "2026-11-01",
		text: "3D game: dapping up after the game, or pulling a teammate up off the floor, hands clasp - fingers wrapped round, thumbs up - not two flat palms.",
	},
	{
		n: 96,
		date: "2026-11-01",
		text: "3D game: a defender contesting a shot puts his hand up with his fingers spread wide, palm to the shooter.",
	},
	{
		n: 95,
		date: "2026-11-01",
		text: "3D game: officials blow their whistles - the whistle in his mouth as he makes the call.",
	},
	{
		n: 94,
		date: "2026-11-01",
		text: "3D game: a hand on another player - a hand check, boxing out, a contest up close - lies flat on him, fingers open.",
	},
	{
		n: 93,
		date: "2026-11-01",
		text: "3D game: the ball squashes against the floor as it bounces - dribbled, bounce-passed or loose.",
	},
	{
		n: 92,
		date: "2026-11-01",
		text: "3D game: players yell - mouth open, jaw dropped - slamming a dunk, swatting a block, flexing and celebrating.",
	},
	{
		n: 91,
		date: "2026-11-01",
		text: "3D game: each frame is about 15% cheaper to draw, for smoother play on phones.",
	},
	{
		n: 90,
		date: "2026-11-01",
		text: "3D game: the net wraps round the ball going through it and is pulled down with it before it snaps back up.",
	},
	{
		n: 89,
		date: "2026-11-01",
		text: "3D game: the home team's mascot is gone, t-shirt toss and all.",
	},
	{
		n: 88,
		date: "2026-10-31",
		text: "3D game: coming out of a timeout, players walk round the official holding the ball instead of through him.",
	},
	{
		n: 87,
		date: "2026-10-31",
		text: "3D game: at a timeout the ball goes to an official, who holds it through the break and gives it to the inbounder after. Officials now hold the ball in both hands.",
	},
	{
		n: 86,
		date: "2026-10-31",
		text: "3D game: at timeouts the mascot throws t-shirts up into the crowd.",
	},
	{
		n: 85,
		date: "2026-10-31",
		text: "3D game: the home team has a mascot - a big furry one in the team's colors and jersey. He's behind the baseline through the game, arms up for the home side's big plays, and out on the floor dancing at timeouts.",
	},
	{
		n: 84,
		date: "2026-10-31",
		text: "3D game: a defender being backed down in the post has a forearm in his man's back as he gives ground.",
	},
	{
		n: 83,
		date: "2026-10-31",
		text: "3D game: a low pass goes round a defender's legs instead of through them.",
	},
	{
		n: 82,
		date: "2026-10-31",
		text: "3D game: a ball handler with a defender right up on him holds him off with his free arm, bent and out between the ball and his man.",
	},
	{
		n: 81,
		date: "2026-10-31",
		text: "3D game: in the handshakes after the game, a player no longer jogs straight through the man waiting for him and then turns back to dap him up.",
	},
	{
		n: 80,
		date: "2026-10-31",
		text: "3D game: nobody walks over a player down on the floor after taking a charge - everyone goes round him, and the teammate coming to pull him up comes round to his feet.",
	},
	{
		n: 79,
		date: "2026-10-31",
		text: "3D game: a pass goes round a defender's outstretched hand instead of straight through it.",
	},
	{
		n: 78,
		date: "2026-10-31",
		text: "3D game: players cast soft shadows of their own shape across the floor from the arena lights, not just a dark spot under their feet.",
	},
	{
		n: 77,
		date: "2026-10-31",
		text: "3D game: a defender stepping aside to let a driver go up no longer crosses behind him to the other side, brushing right past him.",
	},
	{
		n: 76,
		date: "2026-10-31",
		text: "3D game: no more hitch in the picture as a pass or a shot leaves a player's hands - smoother play, most of all on phones.",
	},
	{
		n: 75,
		date: "2026-10-31",
		text: "3D game: a loose ball bouncing or rolling around is knocked aside by a leg or a body in its way instead of going through it.",
	},
	{
		n: 74,
		date: "2026-10-31",
		text: "3D game: the ball in a player's hands turns with him and keeps its print as it goes from hand to hand, instead of always showing the camera the same face. Hands right by a loose ball close their fingers on it.",
	},
	{
		n: 73,
		date: "2026-10-31",
		text: "3D game: a defender picking up the ball handler reads the drive coming - he no longer runs out at a man who is about to attack him and gets blown by.",
	},
	{
		n: 72,
		date: "2026-10-31",
		text: "3D game: the ball no longer passes through a player's own legs on his dribble - between the legs, crossovers and plain bounces all go round his thighs and shins.",
	},
	{
		n: 71,
		date: "2026-10-30",
		text: "3D game: players' hands have real fingers now - jointed, curled naturally, with nails up close, no more mitten. On the ball they wrap it: laid over it when holding it, spread over the top of it at every dribble, and a shooter's hand hangs loose off his wrist on the follow-through.",
	},
	{
		n: 70,
		date: "2026-10-30",
		text: "3D game: a defender who ends up right on a jump shooter gets a hand up at the shot instead of standing there in his stance watching it go.",
	},
	{
		n: 69,
		date: "2026-10-30",
		text: "3D game: no more boxing out the air on a rebound, or fighting for position with a player who isn't there.",
	},
	{
		n: 68,
		date: "2026-10-30",
		text: "3D game: an and-one looks like one. The defender who fouled the shooter is right on him, hand across him as he lets it go, instead of swiping at the air a second later from across the floor. And nobody jab-steps or pump-fakes at an empty floor anymore.",
	},
	{
		n: 67,
		date: "2026-10-30",
		text: "3D game: a player backing down in the post now always has his defender on his back. The defender steps around him as the entry pass comes and gives ground with him. If nobody can get there in time (his man still trailing him down the floor, say), he faces up and takes it in on the dribble instead of backing into empty space.",
	},
	{
		n: 66,
		date: "2026-10-30",
		quiet: true,
		text: "Football 3D game: replays go in and out behind the same sweep as basketball's - a band in the colors of the team that made the play, its logo in the middle - instead of a dip to black.",
	},
	{
		n: 65,
		date: "2026-10-30",
		quiet: true,
		text: "Football 3D game: Key plays, like basketball's. Pick a length next to the speed and the game plays as its big moments - touchdowns, takeaways, long gains, kicks that count, lead changes - with the score catching up in between, and every play of a close finish. What counts as close is in Global Settings.",
	},
	{
		n: 64,
		date: "2026-10-30",
		quiet: true,
		text: "Football 3D game: drawn the way the basketball 3D game is - at most 60 frames a second (a 120 Hz screen no longer draws twice as many), a steady 30 on a device that can't keep up, 30 while paused, and nothing at all while it's scrolled off screen. Phones run cooler.",
	},
	{
		n: 63,
		date: "2026-10-30",
		text: "Realistic Front Offices with Realistic Free Agency turned off: CPU teams now look at free agents every day instead of most days sitting it out, so decent players no longer sit unsigned all summer.",
	},
	{
		n: 62,
		date: "2026-10-30",
		text: "3D game: the shot clock on the court matches the game's own shot clock at every play again, instead of drifting a second or two off after some resets.",
	},
	{
		n: 61,
		date: "2026-10-30",
		text: "3D game: phones and tablets now draw at 60 frames a second, like computers. They only drop to 30 if they really can't keep up.",
	},
	{
		n: 60,
		date: "2026-10-30",
		text: "3D game: defenders read a ball handler's dribble up the floor as one continuous move, so they pick him up in stride instead of setting up for him to stop - fewer handlers slipping past their man without trying to.",
	},
	{
		n: 59,
		date: "2026-10-30",
		text: "Player Stats: switching between Regular Season, Playoffs, Combined and a single playoff series no longer scrambles the columns. The Contract Value column stays in every view - always the player's regular-season value against his salary, rather than a few playoff games against a full year's pay - the Shot Locations column headers line up again, the YOE heading is over years in the league instead of over Age, and the career and all-seasons tables keep their own sort and column settings.",
	},
	{
		n: 58,
		date: "2026-10-30",
		text: "3D game: defenders stay with their man instead of drifting off and coming back. A defender no longer heads for a spot his man has already left, so a ball handler who turns the corner doesn't slip past him by accident, he doesn't sprint ahead to where a drive ends and wait there, and he no longer runs right past a ball handler he's stepping up to pick up. A post or cutter's defender no longer turns and runs away from his man after the catch, only to turn back and jump at the shot. When he has to turn and run to stay in front of a drive, he keeps his head and shoulders turned toward the ball.",
	},
	{
		n: 57,
		date: "2026-10-30",
		text: "Highlight reels now always include every shot taken in the last 3 seconds of a quarter or overtime, made or missed.",
	},
	{
		n: 56,
		date: "2026-10-30",
		text: "3D game: timeouts and breaks between quarters now look like an NBA huddle - chairs are set out in front of each bench, the five players on the floor sit down in them facing their coach, who walks over and kneels in front of them, the rest of the team gets up off the bench and stands around behind them, and everyone breaks together as the timeout ends. Players heading to their own huddle walk around the other team's instead of through it. Also fixed: a player who contests a shot and then goes up for the rebound no longer drops to the floor in a single frame between the two jumps.",
	},
	{
		n: 55,
		date: "2026-10-30",
		text: "3D game: when a player takes a charge, the teammate who comes over to help them up now stands at their feet, grabs both of their hands, and actually pulls them up by them - hands gripped the whole way - instead of reaching down somewhere near them.",
	},
	{
		n: 54,
		date: "2026-10-30",
		text: "3D game: dunks look like real dunks - players rise to the rim and come back down at a natural speed instead of popping up and dropping in a quarter of a second, so the takeoff, the slam and the hang on the rim are all easy to see (especially in replays).",
	},
	{
		n: 53,
		date: "2026-10-30",
		text: "3D game: stars go one-on-one more. Players who create their own shot well get the floor cleared for them and isolate up to three times as often as most. At many timeouts the home team's dance team now runs out and performs at center court while the teams huddle, then clears the floor as play resumes. Also cleaned up a few awkward moments: on a charge, the player who ran into the defender backs off instead of standing over them, and the teammate who helps them up comes around rather than walking through people. At the final horn, players caught in a scramble step apart, and players heading to dap each other up no longer walk on top of each other. Fixed: a defender leaving the ball handler to follow a cutter, which could leave the ball handler wide open for seconds.",
	},
	{
		n: 52,
		date: "2026-10-30",
		text: "Realistic Front Offices has a new setting, Realistic Free Agency (on by default). Turn it off to keep the smart CPU teams with regular BBGM free agency and re-signing: you negotiate and sign like normal, players treat every team the same, asking prices follow what CPU teams would really pay, and CPU teams decide who to sign by their own valuation.",
	},
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
	CLOUD_CHANGES.filter((c) => !c.quiet && (last === undefined || c.n > last));
