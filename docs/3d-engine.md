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

| Layer | Shared (engine) | Per sport |
|---|---|---|
| Page component | the props, staging promise and its failure path, roster check, kit and ad pictures, faces, the frame loop (rAF, visibility, hidden-tab interval), resolution and frame pacing, the sprite cache and farm, dips / flashes / replay wipes, caption, score-bug wrapper | venue paint, logos, what everybody looks like, the moment / aim / camera / draw of one frame, the bug's extras |
| Timeline | `Beat`, cuts, clips, fast stretches, `end`; reading the page's cursor (`targetForCursor`, `snapForCursor`, `linesBetween`) | the director that stages the game; its payload (tracks and ball, or plays) |
| Playback | the clock along the timeline, waiting at the cursor and asking for the next line, replays, the key-plays reel, following another device | which plays are shown again and how slowly; what is a key play |
| Staging | the worker queue that stages a game off the page's thread | the compile function |
| Camera | projection, `cameraAt`, the critically damped follow | rigs, framing, where it looks |
| Renderer | sculpting a figure from masses along its skeleton, per-pixel nearest mass, joints melting, two-tone light, clothes painted in body coordinates, the sprite cache, stamping, shadows, outline, the sculpting farm | the uniform and gear (`Dress`), body proportions, held objects |
| Body | the skeleton, the `Pose` type, gaits (walk, jog, run, sprint, backpedal, slide, shuffle), the move-table mechanism, blending, foot planting, the motion model | each sport's own moves |

## Where football forked the engine (and the order it is undone)

Low-traffic modules first; the files basketball is busiest in last.

1. **Camera.** `field3d/camera.ts` copies the vector helpers and the body
   of `makeCamera` because court3d's has the court's size built in. The
   engine exports `cameraAt(pos, shot, w, h, upright, framing)`; football's
   rigs (press box, end zone, Skycam, goal line) stay football's.
2. **Staging worker.** `stageField.ts` is a line-for-line copy of
   `stageCourt.ts`'s queue: one `makeStager({ compile, spawn })`.
3. **Timeline helpers.** `Beat`, `targetForCursor`, `snapForCursor`,
   `linesBetween` are identical in both directors.
4. **Playback.** `field3d/playback.ts` is an adapted copy (wipe and sting,
   `stepTarget`, `alignTo`, the lost-sync constants, the clock state). One
   playback with a sport adapter: `replayCues(tl)`, replay speed, the sting's
   lead. Basketball keeps its 8 ms integration through fast stretches, its
   asked replays, its wipe hold and its fingerprint.
5. **Key plays.** The same skeleton in both (forced picks, weighted
   candidates, the budgeted fill, merging, the cursor mapping, the same
   constants); each sport picks its own candidates and clip windows.
6. **Game clock marks.** The mark lookup and lerp are shared; how each
   sport's clocks run is its own.
7. **Sprites and farm.** `field3d/sprite.ts` copies the dark-frame,
   budget and pick logic and the shadow half of `stamp`; `field3d/farm.ts`
   is a type-substituted copy. One sprite pipeline with a per-sport
   `SpriteSport`, one farm with a per-sport codec.
8. **Page component.** `Court3D.tsx` and `Field3D.tsx` share their props
   and most of their lifecycle line for line. One `Broadcast3D` shell with
   a sport adapter (stage, placeholder, fingerprint, key plays, clock,
   assets, appearance, frame, bug).
9. **Sculptor.** `field3d/sculpt.ts` is a fork of `court3d/sculpt.ts` (the
   per-pixel loop, the vector maths, lettering, untwisting, lighting are
   copies). One sculptor taking a `Figure` (pose, skeleton, hands, held
   object) and a per-sport `Dress` (torso top, head, legs, held objects,
   joins, cut, palette, paint hooks). Basketball's head stays drawn on top
   from its faces.js face; football's helmet is sculpted.
10. **Body.** Football already uses court3d's skeleton and gaits; it
    re-implements key interpolation, settling into a stand, stride
    counting, drive and blending. One body core, each sport with its own
    move table built from the shared gaits plus its own moves (seventeen
    move names mean different things in the two sports, so the tables stay
    separate).

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
