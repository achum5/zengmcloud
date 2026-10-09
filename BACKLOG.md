# Backlog

The owner's running wish list for the 3D live game (currently named "2.5D" in
the code) and the presentation around it. Items are worked one at a time, never
rushed, with clarifying questions to the owner before and during each one.

Each item keeps the owner's own words as the source of truth, lightly cleaned up
for typos, with the original meaning untouched. Groups and order below are a
proposal. "Raw list" at the bottom is the exact text as sent.

Status key: `[ ]` not started, `[~]` in progress, `[x]` done.

## Before anything else

- [x] Bring in the upstream zengm update (draft pick trade value accounts for
      lottery and playoff settings, version 2026.10.07.0804) plus every other
      upstream change we haven't merged yet, without breaking anything in the fork.

## A. Bugs and correctness

- [x] Replay score bug is broken: shows 0-0 for all games.
- [x] Crash when another league member starts their own game while I'm watching
      a 3D sim: `TypeError: ... is not iterable at beatFreeThrow` (Hn.handle ->
      beatFreeThrow).
- [x] Shot clock must always match the sim. Seen at 0 while play continues.
- [~] Play-by-play line on the 3D screen (bottom middle) only shows the home team
  color, at least on replays. Show the team logo next to it like the Plays
  section does. Where this text lives needs deciding as part of the broadcast
  work (score bug etc.).
- [x] Bench players visibly stutter all game. Investigate and fix.
- [x] Watching a live game being simmed on another device is very choppy and
      unwatchable. Work out how to send the seed/data so the viewing device plays it
      back smoothly and precisely.
- [x] Ball out of bounds should always be correct for which team it went off
      last.
- [x] Fast breaks and timing must always make sense given the shot clock / time
      into the possession.

## B. Replays

- [x] Individual player replays are a bit broken. Rework them overall so they cut
      correctly to each play, with a very brief, visible cut at every switch so it
      reads as a new clip.

## C. Game play realism (the 3D engine)

- [x] Free throws: shooter's feet still over the line. Make the free throw
      animation look nice, with a few different quick ready-up routines.
- [x] Free throws: players lined up along the key have their feet in the wrong
      places. On the last free throw of a trip they box out and fight for the
      rebound; on earlier ones they stand casually and watch, like real life.
- [x] Free throw high fives: teammates should naturally come out and give the
      shooter a low five, like real life (research what it looks like).
- [x] Default dribbling animation needs a ton of work: currently arm out, ball
      bouncing, very basic. Make it realistic and dynamic with the player's movement.
- [x] Rebounding lead-up: when a shot goes up, players box out and fight for
      position realistically. The jump itself is OK; the lead-up is clunky and
      stagnant.
- [x] Ball going out of bounds looks clunky. Make it physics based if possible.
- [x] Fouls are clunky: a player often just runs over to the ball handler and
      gets fouled. The engine knows the result going into the possession, so set it
      up so it looks natural.
- [x] Steals are ugly ("X stole the ball from Y": the ball goes way up in the air
      and lands with the stealer). Like fouls, set it up from the start of the
      possession: matchups, a drive with a help defender stripping it, etc. Many
      (hundreds?) of possible outcomes, all natural and sleek.
- [~] Entry passes are often ugly and clunky. In general, possessions should look
  like real basketball: defenders genuinely getting beaten by good offense.
- [x] Poor ball handlers who get the defensive rebound should usually hold it and
      look to give it to a ball handler to bring it up. Exceptions: sets, urgency.
- [x] Layups: many natural, real-NBA-looking finishes (finger rolls, normal
      layups, etc.) so it never looks pre-animated.
- [x] Deep shots to end a period are ugly: the player just backs up to half court
      and shoots. Rework.
- [~] Defense overall should look like it's genuinely trying to stop the offense.
  Defenders can get crossed up, confused, etc. when the offense does good things.
- [x] "Attempts low post shot" should usually be a real post move, not an awkward
      drive into a 6-foot jumper (currently close to 100% of the time).
- [x] Injuries need an animation that depends on the kind of injury.

## D. Visual quality and performance

- [~] Still dropping frames fairly often. Optimize so the frame rate stays high
  without lowering quality or making it look more pixelated (which also happens
  fairly often).
- [ ] Player head profiles should match their faces.js face much better.
- [ ] Back of the head should match the faces.js face, including hair.
- [x] Rims look like a bunch of circles. Make the rim and the whole basket look
      better. Basketball should look like a real ball with correct lines.
- [ ] Better-looking feet, and customizable shoes (like jerseys and courts).
- [x] Improve the look of the name tag under players.

## E. Customization system (jerseys, courts, score bug, shoes, banners)

These share one open design question: how users author assets. Options raised:
image uploads, SVG, or a code block that can pull in image URLs and position
them freely. Users will want to make these with AI help. Decide together before
building.

- [ ] Custom courts per team, potentially via image or SVG/code, so users can
      recreate real courts. Revisit whether the current image-based approach (as for
      jerseys) is right.
- [ ] Court decals, like the finals trophy at center court: fully customizable
      for every court. Examples: an opening night image on every court, an uploadable
      playoff decal on all playoff courts. Optionally season by season so each era
      can look right.
- [ ] TV-style score bug: a good generic default used at all times, fully
      customizable so users can recreate ESPN, TNT, etc.
- [ ] Arena banners should match the ones the game already draws on the playoffs
      and team history pages, and be customizable.

## F. Arena and broadcast atmosphere

Reference photos from the owner (2026-10-09: MSG, Crypto.com Arena, Frost
Bank Center, a 2K27 broadcast) - what the floor and building should look like:

- [x] Apron: the floor outside the lines painted in the home color all the
      way round, the team name big along each baseline, sponsor lettering down
      the sidelines.
- [x] Behind each basket: photographers sitting on the apron, a row or two of
      courtside seats, then the stands rising straight up - the crowd wraps the
      whole floor.
- [x] Stanchion: dark padded base with a lit ad panel.
- [ ] Far sideline: bench, the scorer's table with a lit LED front, courtside
      seats, then the stands.
- [x] Center-hung scoreboard over the floor (MSG).

- [~] Bench players wear warm-ups with the team logo on the front and name and
  number on the back. Once a player has been in the game, back on the bench he
  is in his uniform only, like real life.
- [x] Scorer's table that looks real: people sitting with monitors. Players
      about to sub in walk to the table ahead of time (the engine looks a few plays
      ahead for substitutions), taking off the warm-up shirt and dropping it as they
      go.
- [~] Baseline and behind the basket like real life: courtside seats, crowd
  continuing behind the basket. Crowd as real people with faces.js faces and
  outfits supporting their team, or the opponent at least in away games.
- [ ] Dynamic crowds based on the team's "hype" saved in the league file: smaller
      crowds for less hype. Crowd coming back from halftime, home crowd while being
      blown out, and any other "arena alive" ideas.
- [~] End of game: players go around dapping each other up. Huge celebration for
  a game winner, and for winning a close game in general. Confetti when a
  championship is won at the buzzer, only if the home team wins it. Playoff wins
  celebrated an appropriate amount.
- [ ] Very brief pre-game cut scene on load: people with microphones on court
      doing pre-game shows, players warming up at their baskets. Skip button to the
      starting lineups (when those exist), then another to the opening tip. Quick
      even without skipping.
- [ ] Starting lineup introductions to open a game, with a skip button: dark
      lights, spotlights, neon, all the hype.

## G. Naming

- [x] Rename "2.5D" to "3D" everywhere, front end and back end, and call it 3D
      from now on.

## H. Bottom of the list

- [ ] Draft night: drafted players walk across the stage in the team's hat and
      meet the commissioner.
- [ ] Free agent press conference: holding up the jersey with the front office.
      Other small immersive moments like these.
- [ ] Investigate adding some of what hoopsjunkie.io does with games, e.g.
      https://hoopsjunkie.io/games/2026-10-08/bos-vs-cle#box-score
- [ ] Eventually a genuinely smart AI GM mode. The current one is probably far too
      complicated and should likely be restarted. Very low priority.

## Raw list (as sent, 2026-10-09)

```
replay score bug broken. shows 0-0- all games
replays a bit broken for individula players. please just work on this overall and make sure it cuts correctly to each play. have it actaully cut with a very tempotary cut to show that it's a new clip every switch
free throws still have player's feet over the line. make sure to make this look good. also make the free throw animation look nice. have a. few different quick ready up for players as their about to shoot a free throw look nice
also for free thrwos players lining up on the side of the key don't have their feet in the correct places. pleas fix this. also make htem correclty box out and fight for the rebound when it's the last free throw in a sequence. if not last free throw the players should be casually standing there watching the free throws like real life
just the default dribbing animation needs a ton of work. i want it to look realistic dribbling animation. currently its off arm extended out and dribbling had just bouncing ball very basic. i want it to look nice and dynamic with their movement
when a shot geos up work on having players try to box out players and make fighting for a rebound like really ncie and realsitic. currenlty it's ugly and stqagnant. the actual rebound animation currently isn't bad when they jump for it. but the lead up to that is a bit clunky and just not worked on enought at the moment
woek on ball going out of bounds look better. it's very clunky. make it physics based if possible. and it should always be correct based on which team it went off alst
fouling is very clunky as it stands now. often times a player just randomly runs over to the ball handler/foulee whoever that is, and gets fouled. it needs to be nicer set up than this. going into the possession the engine should know the result and have it set up nicely so it looks natural to the user watching
Malachi Richardson stole (1 STL) the ball from Juancho Hernangómez (2 TOV) this is so ugly right now. the ball just randomly just gets sent way high up in the air then lands with the "stealer" player. this needs ot be worked on like the fouling so goin ginto the possession the engine knows what is going to happen so it can go ahead and set up matchups correctly or whatever is going to happen like a drive and help defender stealing teh ball. should be hundreds(?) of potential outcomes for the user to watch when this happens. should all look natural and sleek
in genearl entry passes are still ugly often times. just looks clunky. just lots of stuff like this per possession needs work to look more like irl basketball looks. like defenders are genuinly getting beat and it's a good play by the offense
if a person with bad dribbling gets the defensive round, theu should often times be holding the ball trying to get it to a ball handler to bring it up the court. there are certainly edge cases this doesn't apply and a big man/bad ball handler brings it up due to a set or just like needing to be urgent or something. but in general this needs work atm
work further on making layup animations look like real life from real nba players. like finger rolls or just normal layups and stuff. just need a ton of natural looking animations so it never looks preanimated but always look natural.
for som reason we are still dropping frames a good amount of times. is there some way to optimize this entire thing to make frames always be high iwthout also dropping quality making it look more pixelated? that also happens pretty often
i want to potneitally work on making the courts a way to be coded in either via image or like some sort of svg coding/file? i want a way to basically have the user make their own courts for all teams so they can be the genuoine real courts from real life if they wanted to. i'm not sure the exact structure that we shoudl do this. this is the same as jerseys. i'm not sure if our image way of thinking about this is correct as is. maybe it is? idk i just need your input on this stuff.
simlar to the above topic, i want a good score bug like real tv. then i want there to bea. way for users to fully customize this so they could make like genuine espn, tnt, etc score bugs to be used in game if they wanted to to match real life. but we need a good generic default score bug to be used at all times. again i'm not sure exactly the style we should go about this on the back end. users will want to use AI to generate these for them. please again think about this deeply to figure out the best way to do this. a code block would be best if users could genuinley making highly customizable good looking assets with this (score bug, jerseys, courts). but maybe not. maybe it would need to be image uploads? irdk. maybe code with the abilitiy to upload image urls and fit them in and move thema round however they want
also a court customization like we have foe the finals trophy to be in the middle of the court during the finals. this would be cool to be fully cusotmizable for all courts. like opening night have a universal image to put on the court. playoffs have an uploadable decal to be on all playoff courts. etc. this should also be season to season if the user wants so eras can look exactly how they should look if the user wants this. please ask a ton of clarifying questions on ALL of this and the above becuase i'm sure you will have questions and be confused about much i have said
make bench players have on warm up suits with the team logo on the front and player jersey and number on back. once the player has been in the game, if they go back to the bench they should then only have their uniform on to show they've been in the game similar to real life.
make the scorer's table look more like real life with people sitting there with monitors. also make palyers that are coming into the game in the near future go to the scorer's table. this will have to be done via the engine knowing some plays away and detecting substitions coming on and send them to the scorer's table to be ready to come in. would be a really cool thing. also make them take off their warm up shirt and drop it on ground as they walk to the scorer's table
i want the baseline and behidn the basket to be much more like real life. there are people sitting courtsides od these games. would be cool to have that with the crowd going back behind the basket as well. also a way to improve crows as a whole making htem genuine people with faces.js faces and outfits supporting their team or the oppoennt at minimum during away games. just like rela life some of those people being sprinkled in
potentially having starting lineup animations to start a game? with a skip button for the user to click onto if they want to skip them. with all the hoot and holler of it too. dark lights, spotlights, neon lights etc. would be a huge but cool thing to do
draft night walking across the stage with draft team hat and commissioner would be really cool. please probably put this at the bottom of the list of things to do but would be very cool to tackle one day.. simialr to this could be like free agents having a press conference holding up their jersey with the front office there and stuff. just small stuff like this could make the game way more immersive than it is now. again this needs ot be at the bottom o fthe list but could be very cool one day once we nail down everything else
dynamic crows based on "hype" of the team that is saved in the bbgm file per team at the time. would be cool to have lesser crowds when there is lesser "hype" for the team. also dynamic crowds coming back from halftime, dynamic crowds for the home team getting blown out. any other general arena being alive improvements you can think of. just follow up with many questinos please
we need to work on making the profiles of player heads look better and match their faces.js face better than it does now. currenlty doesn't match very well for many players
make rims look better than they do now. currenlty looks like a bunch of circles put together. also just work on making the entire basket look bett. also make basketball look more like ar eal basketball with correct lines
very brief cut scene when loading in of people with microphones on the court doing pre game shows, players shooting on their goals getting warmed up. with a skip button that takes you to starting lineups once we get that in with another skip button to opening tip. without skip buttons it should be pretty quick regardless
correct fast breaks and stuff based on timing of the shot into the possession. make sure this alway smakes sense.
deep shots to end the period need work atm. currenlty very ugly. currently jsut in the half court offense thent he palyer just backs up to half court and shoots. needs work
make defense as a whole look way more in it like they are genuinly playing defense trying to prevent the offense from scoring. they can get crossed up, confused, etc just like real life via the offense doing good things
the guys on the bench like weidly stutter all game. please investigate this and fix it
when a player hits a free throw i like that a player goes and gives him a high five but it needs to be worked on to look more realsitic. make both players kind of naturally go out and give hte player a low five like real life. research if needed to see what this looks like and fix it
work on players making post moves to score more often for "attempts low post shot" rather than like awkwardly driving in and taking a 6 foot jumper. that could happen occassionaly but it happens 100%(?) of the time right now which needs to be drastically worked on
when a games ends make players go around the court kinda dapping each other up and stuff. if a game winning shot making it so the winning team celebrats really huge. same with just generally winning a close game. just needs to be like real life. make it so confetti falls when a team wins the championship whrn the buzzer sounds. only if the home team while winning it to keep it realistic. playoff wins should be celebrated an approportaite ampount by the winning team
when viewing a game live that is simming on another device it is incredibly choppy and just overall unwatchable. we need to figure out how to correclty communicate the seeding or whatever so it can nicely and precisly be viewed on the "viewing" device that isn't actually the one doing the simming. does this make sense?
also back of head needs to get more accurate to the faces.js face with hair and stuff so this needs to be worked on
injuries need an animtion based on what kind of injury it is please
i would like to improve how feet look and have shoes that are customizable same as jerseys, courts, etc etc
investigate improvin the look of the name tag under players
make sure shot clock is always visually shown correct according to the back end sim. i've seen it at times be at 0 while the play is going on
"i tihnk this happens when another user in the cloud league starts their own game while i'm viewing a 2.5d sim?: Error
(intermediate value)(intermediate value)(intermediate value) is not iterable

TypeError: (intermediate value)(intermediate value)(intermediate value) is not iterable
    at https://bbgmcloud.vercel.app/gen/ui-chunk-Dbda03Bh.js:1:179991
    at Array.map (<anonymous>)
    at Hn.beatFreeThrow (https://bbgmcloud.vercel.app/gen/ui-chunk-Dbda03Bh.js:1:179970)
    at Hn.handle (https://bbgmcloud.vercel.app/gen/ui-chunk-Dbda03Bh.js:1:171615)
    at $n (https://bbgmcloud.vercel.app/gen/ui-chunk-Dbda03Bh.js:1:225691)
    at https://bbgmcloud.vercel.app/gen/ui-chunk-Dbda03Bh.js:1:252838
    at Object.Xs [as useMemo] (https://bbgmcloud.vercel.app/gen/ui-2026.10.07.0880.js:19:58176)
    at e.useMemo (https://bbgmcloud.vercel.app/gen/ui-chunk-CzeY8rCK.js:1:7444)
    at Xi (https://bbgmcloud.vercel.app/gen/ui-chunk-Dbda03Bh.js:1:252819)
    at $o (https://bbgmcloud.vercel.app/gen/ui-2026.10.07.0880.js:19:49253)"
Make banners hanging in the arena match the ones that the game already generates on playoff page, team history page. Also make customizable so the user can change the look if they want
the color of the text of what just happened during the play by play seem to be broken at least on replays as only showing hte home team color. lets have this show up like hte plays section does with the team logo next to it rather than the team color. also ew will need ot figure this out specifically with our broadcast improvements like score bug and stuff. im not exactly sure how or where the play by play on the actual screen should show. (i'm talkin about what currenlty shows in the bottom middle of the actual 2.5d screen)
also let's just change this from being called 2.5d to 3d. change all references both on front end and back end to 3d so we can just refer to it as that from now on
very low on the list of things to do but i want to invesitgate adding some of the stuff that this site does with games: https://hoopsjunkie.io/games/2026-10-08/bos-vs-cle#box-score

eventually investigate making an actual smart ai gm mode. we have one but i think it's probably way too overcomplicated at this point and i think we should probably restart. very very low on teh todo list
```
