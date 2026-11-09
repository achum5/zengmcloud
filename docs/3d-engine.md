# The 3D engine: one system, every sport

The live game's 3D picture is one engine with a sport plugged into it.
Basketball (`src/ui/views/LiveGame/court3d/`) came first and is the engine's
home; football (`field3d/`) is being moved onto it; hockey and baseball come
after, built on it from the start. All four should look and behave like
sister games - the same camera, renderer, playback and controls - and differ
only in what the sport itself is.

Nothing here touches a game sim. Each sport's 3D picture acts out that
sport's play-by-play, line by line, as it already exists.

## Rules while the engine is put together

- **Basketball does not change.** Every step that makes a court3d module
  serve more than one sport keeps basketball's numbers, timings and
  behaviour exactly as they were - the sport-specific parts become
  parameters with basketball's values as the defaults. A step that would
  change basketball's picture is a bug in the step.
- **Generalize in place.** Shared code stays in the court3d file it lives in
  today (made sport-neutral there) rather than moved, so work on basketball
  going on at the same time merges cleanly. Brand-new shared code may get a
  new file. The folder gets an engine name at the end, once it is quiet.
- **Football keeps working.** Each step leaves football playable and its
  suites green; football's own copy of a module is deleted in the same step
  that moves it onto the shared one.
- **Only what basketball has.** A sport's picture has the equivalents of
  basketball's scenes and nothing else: no ceremonies basketball has no
  counterpart for (football's tunnel run-out, coin toss, Gatorade shower,
  injury cart, measurement cutaway and kickers' net were taken out for this).

## The layers

| Layer          | Shared (engine)                                                                                                                                                                                                                                                     | Per sport                                                                                                      |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Page component | the props, staging promise and its failure path, roster check, kit and ad pictures, faces, the frame loop (rAF, visibility, hidden-tab interval), resolution and frame pacing, the sprite cache and farm, dips / flashes / replay wipes, caption, score-bug wrapper | venue paint, logos, what everybody looks like, the moment / aim / camera / draw of one frame, the bug's extras |
| Timeline       | `Beat`, cuts, clips, fast stretches, `end`; reading the page's cursor (`targetForCursor`, `snapForCursor`, `linesBetween`)                                                                                                                                          | the director that stages the game; its payload (tracks and ball, or plays)                                     |
| Playback       | the clock along the timeline, waiting at the cursor and asking for the next line, replays, the key-plays reel, following another device                                                                                                                             | which plays are shown again and how slowly; what is a key play                                                 |
| Staging        | the worker queue that stages a game off the page's thread                                                                                                                                                                                                           | the compile function                                                                                           |
| Camera         | projection, `cameraAt`, the critically damped follow                                                                                                                                                                                                                | rigs, framing, where it looks                                                                                  |
| Renderer       | sculpting a figure from masses along its skeleton, per-pixel nearest mass, joints melting, two-tone light, clothes painted in body coordinates, the sprite cache, stamping, shadows, outline, the sculpting farm                                                    | the uniform and gear (`Dress`), body proportions, held objects                                                 |
| Body           | the skeleton, the `Pose` type, gaits (walk, jog, run, sprint, backpedal, slide, shuffle), the move-table mechanism, blending, foot planting, the motion model                                                                                                       | each sport's own moves                                                                                         |

## Where it stands

Shared now (each in court3d/, football's copy deleted):

| What                                                                                                                                                                                                                                                                                                                                          | Where                               |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| Camera: `cameraAt` with a sport's framing                                                                                                                                                                                                                                                                                                     | camera.ts                           |
| Staging off the page: `makeStager`, `serveStaging`                                                                                                                                                                                                                                                                                            | stager.ts                           |
| Timeline: `Beat`, the cursor helpers, `clockAt`                                                                                                                                                                                                                                                                                               | timeline.ts                         |
| The show's clock: playback, replays, the key-plays reel, following another device                                                                                                                                                                                                                                                             | show.ts (a sport hands it a `Show`) |
| The key-plays fill: `fillReel`, `reelCursors`                                                                                                                                                                                                                                                                                                 | keyPlays.ts                         |
| The page's side: stakes, uniform and ad pictures, staging and the roster check (`useStaged`), faces (`useFaces`), size, the frame loop, the dip, the wipe, the caption's fade                                                                                                                                                                 | broadcast.ts                        |
| What lies over the picture: vignette, dip, flash, wipe, REPLAY, the caption                                                                                                                                                                                                                                                                   | Overlays.tsx                        |
| The picture on the page: its box and face loaders, the canvas, the score bug (`PictureBox`, `PictureCanvas`, `GameBug`)                                                                                                                                                                                                                       | Picture.tsx                         |
| The score bug kept up frame by frame (`writeBug`)                                                                                                                                                                                                                                                                                             | bugWriter.ts                        |
| The canvas sized to the picture (`sizeCanvas`), finer poses (`fineNow`)                                                                                                                                                                                                                                                                       | resolution.ts                       |
| Sprites: shadows (`castShadows`), the plan (`SpritePlan`), reuse and budget (`pickSprite`), the hands' pictures (`takeFarmed`), the frame's new pictures (`prepareOf`)                                                                                                                                                                        | sprite.ts                           |
| The sculpting farm (a sport hands it a `FarmCodec`)                                                                                                                                                                                                                                                                                           | farm.ts                             |
| The sculptor's groundwork and its per-pixel loop (`shade`; a sport hands it its `paint`)                                                                                                                                                                                                                                                      | sculpt.ts                           |
| Building a figure: where a point on him lands (`seerOf`), laying masses (`massesFor`), the torso's frame (`torsoOf`), torso, arm, hand, leg, joins (`torsoMasses`, `armMasses`, `handMasses`, `legMasses`, `joinBody`), the camera's frame (`frameOf`) - a sport hands each its build                                                         | sculpt.ts                           |
| Moving: strides worked out foot by foot (`freshStart`, `stepThrough`), turning on the spot (`pivotFeet`), stepping from one stand into another (`settleStep`), a foot down held where it landed (`footTurn`, `hipsTurn`), legs turned under a man going one way and facing another (`legsTurned`) - held to it by each sport's footwork tests | stepping.ts, evaluate.ts            |
| Blending one move into the next: how long each kind of change takes (`blendMsOf`), its curve (`blendEase`), a pose eased back into what he was (`easedFrom`), as many changes back as `BLEND_DEPTH` - each sport finding its own changes                                                                                                      | evaluate.ts                         |
| Moves: `MoveDef`, `moveAt` (straight or smooth keys), `driven`                                                                                                                                                                                                                                                                                | poses.ts, evaluate.ts               |

Still each sport's own, and next to be brought together:

1. **The page component.** Court3D.tsx and Field3D.tsx now share their
   pieces but are still two components, each with its own frame loop
   wiring them together. The goal is one `Broadcast3D` with a sport
   adapter.
2. **What is left of blending**: basketball's arms and shoulders easing
   on their own time (into a dribble, down from an arm thrown up), and
   stepping a foot at a time deeper in a chain of changes - football
   eases its arms with the rest of him, and steps only between two ways of
   standing. Facing - which way a man looks - stays each sport's, as
   reading a moment does.

## The graphics card

Sculpting a man's new pose is most of what a frame costs. Basketball's is
done on the graphics card where there is one (`sculptGl.ts`): his masses,
built as ever in `sculpt.ts`, are handed to a fragment shader that does
`shade`'s work and basketball's `paint` for all his pixels at once, and his
picture is drawn from its sheet straight into the one kept (`glKeep` in
`sprite.ts`). The page's sculptor stays the word on what he looks like:

- once a visit, one man is made both ways and the pictures compared
  (`glAgrees`); a card whose man is not the page's is not used;
- its first men are timed against the page making some of the same
  (`glTook`); a card no quicker than the page is not used;
- `sculptGl.test.browser.ts` holds the two to each other across uniforms,
  gear, outfits and poses. A change to `shade` or to basketball's `paint`
  is made in both, and that suite says whether it was.

`localStorage.court3dGl` set to `"0"` turns it off on a device, `"1"` on
whatever the card. Football's men are still sculpted on the page: its
`paint` has no shader yet.

What stays each sport's: the director (play-by-play into a timeline),
reading a moment off it, where the camera looks, the venue and its
markings, uniforms, the sport's own moves, its crew, the score bug's
extras.

## A sport, plugged in

What a sport supplies, once the engine is whole:

- **Venue:** the building and the playing surface - painted once a game,
  stood up in the world as flat pictures - and its markings.
- **Dress:** the uniform and gear, body proportions, held objects.
- **Moves:** the sport's own moves on the shared skeleton.
- **Director:** the play-by-play staged into a timeline - where everybody
  is and what each is doing, the ball, the cuts, the beats.
- **Moment, aim, frame:** reading the timeline at an instant, where the
  camera looks, and what is drawn.
- **Crew:** officials and the people around the game, the equivalents of
  basketball's.
- **Bug:** the clocks and the extras on the score bug.
