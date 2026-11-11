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
