# Photo → faces.js prompt

Paste everything below the line into a **vision-capable chat AI** (Claude, ChatGPT,
Gemini) along with one clear, front-facing photo of the player. It replies with a
JSON object you paste straight into **Tools → Customize Player → Face** in ZenGM.

Not an image _generator_ — Midjourney/DALL-E/Stable Diffusion can't read a photo
and emit structured JSON. It has to be a chat model that accepts image input.

**The photo matters more than anything in the prompt.** Best: a recent,
front-facing headshot (a media-day portrait is ideal), even light, eyes open,
no hat or sunglasses, at least a few hundred pixels across the face. Arena
action shots, side profiles and tiny thumbnails all cost accuracy.

You can attach **two or three photos of the same player** in one message — say
so — and it will cross-check them (colors from the best-lit one, shapes from the
most front-on). For several DIFFERENT players, number the photos and ask for one
JSON object per photo.

The reply is a `json` code block followed by a short `Notes:` block
flagging anything it had to guess at. Use the chat's copy button on the code
block — that is the whole reason it asks for a fence. The notes are there so you
know which slots to double-check, not for pasting; the game strips the fence and
ignores anything around it, so pasting the block as-is is fine.

If ZenGM says **Invalid JSON**, it's almost always curly quotes (`“` `”` instead of
`"`) — some chat apps and phone keyboards swap them in when you select text by
hand. Copying from the code block avoids that; failing that, replace every curly
quote with a straight one and it'll paste fine.

---

You are converting a photograph of a person into a **faces.js** `FaceConfig`
object (faces.js v5, the cartoon-avatar library used by ZenGM / Basketball GM).

Look at the attached photo and pick the option in each slot that best matches the
real person. The goal is that someone who knows this player recognises him from
the avatar at a glance.

**Study the face before you choose anything** (silently — the reply still starts
with the JSON). Go feature by feature: skin tone, hair (color, length, texture,
hairline), facial hair, face shape and width, eyes, brows, nose, mouth, ears,
age marks. Then name to yourself the **two or three things that make THIS face
recognisable** — what a caricaturist would draw first: big ears, a broad nose, a
long narrow face, heavy brows, a cleft chin, a receding hairline, a
distinctive beard shape, a gap-toothed grin, very round cheeks. Likeness comes
from getting those unmistakably right, and from every other slot matching the
photo as closely as the drawings allow, so:

- make each distinctive feature clearly visible in the output — the option
  that shows it, and the size or angle pushed far enough to read at avatar
  size;
- for every other feature, read the descriptions and take the drawing
  closest to what the photo shows. Every option in a slot is a real choice;
  none is a default to fall back on.

**Tell the face apart from the expression.** Headshots are often taken
mid-grin, and a big smile changes several features at once. Read each one as it
would be at rest:

- a laugh squint narrows the eyes: still an ordinary eye, not a sleepy or slit
  one, unless the eyes are narrow when he is not smiling;
- the folds beside the mouth and the creases at the eye corners are the grin,
  not age: a young player still gets `smileLine`/`eyeLine` `none`;
- raised cheeks make the face look rounder: do not raise `fatness` for them.

The smile itself is kept, one step calmer (see **mouth** below).

If you are given **more than one photo of the same person**, use them together
and reply with ONE object: take skin and hair color from the best-lit photo,
face shape, nose and ears from the most front-on one, and treat anything that
shows in only one photo (a beard, a headband) as belonging to that photo, not to
the man, unless he is clearly the same age in both.

**Output the JSON object first, with nothing before it, inside a fenced
markdown code block tagged `json`** — no preamble, no explanation ahead of it.
The fence matters: it is what gives me a one-tap copy button instead of a
hand-selected blob of text, and it stops the chat app from smart-quoting the
`"` characters. Inside the object: no comments, no trailing commas, and every
key listed below present.

Quote every key and string with a plain ASCII double quote (`"`, U+0022). Curly
quotes (`“` `”`) are not valid JSON and the game rejects the whole object.

**Never put a line break inside a string.** Every value here is short - an id, a
hex, an `rgba(...)` - so each one fits on its own line with no wrapping. A
string broken across two lines is a "Bad control character in string literal"
error and the game refuses the whole object. Same for a literal tab inside a
string: use plain spaces only.

**After the code block, add a short `Notes:` block** — up to three one-line bullets,
only for calls you are genuinely unsure about and where knowing would let me fix
it myself (bald vs. buzzed, stubble vs. a shaped goatee, a skin tone you had to
judge through bad lighting). Skip it entirely when nothing is in doubt; don't
narrate choices you're confident in. The game only ever reads the JSON, so
anything after it is free.

## Output shape

Every value below is filler, there to show the SHAPE of each entry - which keys
exist, and whether a slot takes an id, a number, a hex, or a boolean. Not one of
them is a default or a suggestion. Read every slot off the photo; the
only exceptions are `teamColors` and `jersey`, which you copy exactly as shown.
Reply in this exact form, fence and all.

```json
{
  "fatness": 0.42,
  "teamColors": ["#89bfd3", "#7a1319", "#07364f"],
  "hairBg":      { "id": "none" },
  "body":        { "id": "body3", "color": "#74453d", "size": 1 },
  "jersey":      { "id": "jersey" },
  "ear":         { "id": "ear2", "size": 1 },
  "head":        { "id": "head5", "shave": "rgba(0,0,0,0.3)" },
  "eyeLine":     { "id": "line1" },
  "smileLine":   { "id": "line4", "size": 0.82 },
  "miscLine":    { "id": "none" },
  "facialHair":  { "id": "none" },
  "eye":         { "id": "eye8", "angle": 6 },
  "eyebrow":     { "id": "eyebrow11", "angle": 13 },
  "hair":        { "id": "short", "color": "#272421", "flip": true },
  "mouth":       { "id": "mouth7", "flip": false },
  "nose":        { "id": "nose7", "flip": true, "size": 0.9 },
  "glasses":     { "id": "none" },
  "accessories": { "id": "none" }
}
```

## Allowed `id` values

Use these EXACT strings. Anything not on the list renders as a blank slot.

- **head**: head1, head2, head3, head4, head5, head6, head7, head8, head9,
  head10, head11, head12, head13, head14, head15, head16, head17, head18
- **hair**: afro, afro2, bald, blowoutFade, cornrows, crop, crop-fade,
  crop-fade2, curly, curly2, curly3, curlyFade1, curlyFade2, dreads, emo,
  faux-hawk, fauxhawk-fade, hair, high, juice, longHair, messy, messy-short,
  middle-part, parted, shaggy1, shaggy2, short, short2, short3, short-bald,
  short-fade, short-fade-2, shortBangs, spike, spike2, spike3, spike4, tall-fade
- **hairBg** (straight hair drawn hanging BEHIND the head — only for long
  straight or wavy hair; never braids, locs or afros): none, longHair, shaggy
- **facialHair**: none, beard1, beard2, beard3, beard4, beard5, beard6,
  beard-point, chin-strap, chin-strapStache, fullgoatee, fullgoatee2,
  fullgoatee3, fullgoatee4, fullgoatee5, fullgoatee6, goatee1, goatee1-stache,
  goatee2, goatee3, goatee4, goatee4-stache, goatee5, goatee6, goatee7, goatee8,
  goatee9, goatee10, goatee11, goatee12, goatee15, goatee16, goatee17, goatee18,
  goatee19, goatee-thin, goatee-thin-stache, harley1, harley1-sb-1, harley1-sb-2,
  harley2, harley2-sb-1, harley2-sb-2, harly3, harly3-sb-1, harly3-sb-2,
  honest-abe, honest-abe-stache, logan, loganGoatee2, loganGoatee2Stache,
  loganGoatee3, loganGoatee3soul, loganGoatee3soulStache, loganSoul,
  mustache1, mustache1SB1, mustache1SB2, mustache-thin, mutton, muttonGoatee1,
  muttonGoatee1Stache, muttonGoatee2, muttonGoatee2Stache, muttonGoatee5,
  muttonGoatee5Stache, muttonSoul, muttonStache, muttonStacheSoul, neckbeard,
  neckbeard2, neckbeard2SB1, neckbeard2SB2, neckbeardSB1, neckbeardSB2,
  sideburns1, sideburns2, sideburns3, soul, soul-stache, wilt,
  wilt-sideburns-long, wilt-sideburns-short
- **eye**: eye1, eye2, eye3, eye4, eye5, eye6, eye7, eye8, eye9, eye10, eye11,
  eye12, eye13, eye14, eye15, eye16, eye17, eye18, eye19
- **eyebrow**: eyebrow1, eyebrow2, eyebrow3, eyebrow4, eyebrow5, eyebrow6,
  eyebrow7, eyebrow8, eyebrow9, eyebrow10, eyebrow11, eyebrow12, eyebrow13,
  eyebrow14, eyebrow15, eyebrow16, eyebrow17, eyebrow18, eyebrow19, eyebrow20
- **nose**: nose1, nose2, nose3, nose4, nose5, nose6, nose7, nose8, nose9,
  nose10, nose11, nose12, nose13, nose14, honker, pinocchio, small
- **mouth**: mouth, mouth2, mouth3, mouth4, mouth5, mouth6, mouth7, mouth8,
  angry, closed, side, straight, smile, smile2, smile3, smile4, smile-closed
- **ear**: ear1, ear2, ear3
- **body**: body, body2, body3, body4, body5
- **jersey**: jersey, jersey2, jersey3, jersey4, jersey5, baseball, baseball2,
  baseball3, baseball4, football, football2, football3, football4, football5,
  hockey, hockey2, hockey3, hockey4
- **eyeLine** (age marks around the eye — NOT an eyelid crease): none, line1,
  line2, line3, line4, line5, line6
- **smileLine** (nasolabial folds): none, line1, line2, line3, line4
- **miscLine**: none, blush, chin1, chin2, forehead1, forehead2, forehead3,
  forehead4, forehead5, freckles1, freckles2
- **glasses**: none, glasses1-primary, glasses1-secondary, glasses2-black,
  glasses2-primary, glasses2-secondary, facemask
- **accessories**: none, headband, headband-high, hat, hat2, hat3, eye-black,
  santa-hat

Do NOT use any id beginning with `female` unless the subject is a woman; those
exist only in eye, eyebrow, hair, hairBg and head.

## What every option looks like

You cannot see the drawings, so each id is described below one by one. Every
description was written by RENDERING that option and looking at it, not by
reading its name. Several names are actively misleading (`afro` is a smooth
cap, `dreads` is a top-knot, `eyeLine` is not an eyelid crease, `neckbeard`
sits on the jaw, `eye10` is not an ordinary eye), so trust the description,
never the name.

The drawings are simple cartoon line art: a bold black outline, flat colors,
no shading. Each slot starts with a few CALLS to make about the photo (what
the tip of the nose does, where the jaw turns, how the lid sits) and a table
from those calls to the drawing. Make every call from the photo, for every
player, even when a feature looks ordinary: ordinary faces still lean one
way or another on each call, and those leanings are what make two faces
different. Then check the drawing's description below the table. The groupings below only put
similar drawings next to each other; no group or id is preferred.

### head

Make three calls on the lower face, then find the jaw (and set `fatness`
from the face's length and width, see "Face shape and `fatness`"):

1. **CORNER** — where the sides of the face turn in toward the chin: no
   corner (a smooth curve), a soft corner, or a sharp, bony corner.
2. **HEIGHT** of that turn — high (around the mouth) or low (near the chin).
3. **CHIN** — small and rounded, small and flat, pointed, wide, or with a
   cleft/notch.

| corner / chin → | small round | small flat        | pointed  | wide                         | cleft / notch |
| --------------- | ----------- | ----------------- | -------- | ---------------------------- | ------------- |
| smooth curve    | `head1`     | `head2`, `head14` | `head11` | `head13`, `head12`; jowly `head6` | `head8`, `head9` |
| soft, high      | `head5`     | `head4`           | `head11` | `head13`                     | `head4`       |
| sharp, high     | `head3`     | `head7`           | `head3`  | `head7`                      | `head10`      |
| low (near chin) | `head12`    | `head16`          | `head15` | `head17`; boxy `head16`      | `head18`      |

**How the head works — read this first.** All 18 head drawings are the SAME
length, and they are identical from the top of the skull down to the
cheekbones: the same dome, the same temples, the same width at the
cheekbones. They differ ONLY below the cheekbones, in the jaw and chin. So:

- The head id sets the JAW: where the sides turn in (the jaw corner), how
  sharp that corner is, and how wide and what shape the chin is.
- `fatness` sets the WIDTH of the whole head, top to chin, and it is the only
  thing that makes a face look long and narrow or short and broad. No head id
  is longer or narrower than another.

Pick the jaw here and the width with `fatness` (see "Face shape and
`fatness`" under Allowed numbers). A long, narrow face is a LOW `fatness`
with a tapering jaw, never a boxy jaw at any `fatness`.

The jaws, grouped by how wide the lower face stays. faces.js draws every
jaw narrower than a real one: a real man's jaw is nearly as wide as his
cheekbones, and even the broad jaws below are narrower than that. So match
the photo by how the jaw looks RELATIVE to an ordinary face, not by its
absolute width: an ordinary lean or average jaw is one of the tapering or V
jaws; the full and broad jaws are for a jaw that is visibly heavy, square
or jowly, the kind people would describe.

Narrow, tapering jaws — the sides curve or angle in well above the chin to a
small chin. Lean, oval and long faces:
- `head1` — a smooth egg: the sides curve in continuously from the
  cheekbones, no jaw corner anywhere, to a small rounded chin.
- `head2` — an egg like `head1` that ends in a small, short, FLAT-bottomed
  chin.
- `head14` — an egg like `head1` with a slightly squared-off small chin; the
  sides stay out a touch longer before curving in.
- `head5` — the sides run fairly straight down past the mouth, then turn at a
  soft, rounded jaw corner and angle in to a narrow rounded chin: a soft
  shield / V.
- `head11` — like `head5`, but the angled lines run longer and meet in a
  narrower, more pointed chin: the most pointed soft V, a heart-shaped lower
  face.
- `head4` — an oval tapering to a small chin whose bottom is flat with a very
  faint dip in the middle.
- `head9` — an oval tapering to a small chin with a wavy double bump (a small
  W) at the bottom.

Angular V jaws — a clear, sharp jaw CORNER about level with the mouth, then
straight lines angled in to the chin. Lean faces with a defined, bony jaw:
- `head3` — sharp corners, then long straight lines to a small rounded chin:
  a strong V.
- `head7` — sharp corners, then shorter straight lines to a short flat chin:
  a chiselled V with a blunt chin.
- `head10` — fairly straight sides, defined corners, and a chin drawn as a
  clear W (two bumps with a notch): a V jaw with a cleft chin.

Full, rounded jaws — the lower face stays wide and rounds off in a U. Fleshy,
round-cheeked faces:
- `head12` — the lower face stays full nearly to the bottom, then rounds into
  a medium chin: a broad U.
- `head13` — like `head12`, a touch softer: a full, round lower face.
- `head8` — a full rounded jaw with a small double bump (a slight cleft) at
  the bottom of the chin.

Broad jaws — the lower face stays wide right down to a wide chin. A
visibly heavy, square or jowly jaw:
- `head6` — broad and soft: full, heavy, rounded cheeks and jowls carried down
  to a wide rounded chin. No corners: a jowly round face.
- `head15` — straight sides, jaw corners low near the chin, then short angled
  lines to a pointed chin: a wide jaw with a pointed chin.
- `head16` — straight sides, sharp low jaw corners and a wide, flat, slightly
  tilted chin: the boxiest, a rectangle.
- `head17` — straight sides almost to the bottom, rounded corners, and the
  widest flat chin of all: a soft square, the broadest jaw.
- `head18` — straight sides, low corners, and a wide flat chin with a notch in
  the middle: a square jaw with a cleft.

Read the jaw off the photo: where do the sides of the face stop running down
and start turning in (high, around the mouth, or low near the chin)? Is that
turn a sharp corner or a curve? Is the chin narrow and pointed, small and
flat, or wide? A face that narrows steadily from the cheekbones is one of
the narrow tapering jaws, whatever else it looks like.

### hair

Make four calls, then find the cut in the descriptions below:

1. **LENGTH** — bald, shaved or buzzed, short, medium, long.
2. **TEXTURE** — smooth/straight, wavy, tight curls, spiky, braided, locs or
   twists.
3. **SIDES** — full, faded (tapering to skin), or clipped to the skin.
4. **FRONT** — the hairline (straight, M/receding, thin crown), any part
   (side or centre), and whether the front lies flat, stands up, or falls
   onto the forehead as a fringe.

| texture / length → | shaved / buzzed                    | short                                                  | medium                                        | long                        |
| ------------------ | ---------------------------------- | ------------------------------------------------------ | --------------------------------------------- | --------------------------- |
| smooth             | `short-fade`, `short-fade-2`; `bald` | flat `short`/`short2`; clipped sides `crop`; fade `crop-fade`/`crop-fade2`; side part `parted`; centre `middle-part` | fringe `emo`/`shortBangs`; bowl `afro` | `longHair`, `shaggy1`, `shaggy2` |
| wavy               | `crop`                             | `parted`, `hair`                                       | `hair`, `messy`                               | `shaggy1`                   |
| tight curls        | `short-fade`                       | `short3`; fade `curlyFade1`/`curlyFade2`               | `curly`, `curly3`, `curly2`                   | big `afro2`                 |
| spiky / bristly    | `crop`                             | `spike4`, `messy-short`; flat-top `spike`/`high`       | `spike2`, `spike3`, `messy`                   | `messy`                     |
| braids / locs      | `cornrows`                         | `cornrows`; short twists `short3`                      | `curly3`; top-knot `dreads`                   | `cornrows` / `curly3`       |

Receding with a thin crown → `short-bald`. A tall box → `tall-fade` (faded)
or `high` (solid); a raised peak → `faux-hawk`, `fauxhawk-fade`.

Match length and texture before the style name. `hair.color` colors all of
it. Unless stated, the sides end beside the top third of the ear. `flip`
mirrors the drawing: it matters on the one-sided cuts (`emo`, `parted`,
`longHair`, `shaggy1`, `shaggy2`, `juice`, `hair`, `shortBangs`, `messy`,
`messy-short`, `dreads`) and does nothing on symmetric ones. The faded
(see-through) sides of the fade cuts are faint: on dark skin with dark hair
they can't be seen, and a fade looks like the same cut with bare sides.

Bald and shaved:
- `bald` — a bare scalp. The right id for essentially every bald player; pair
  it with a `head.shave` alpha (see Stubble below) and the shadow does the rest.
- `short-bald` — bald on top with a band of hair around the sides and back:
  the classic receding / male-pattern look. When the crown is clearly thin or
  gone but the sides are not shaved, this is the answer, not `bald`; it is one
  of the strongest likeness cues an older player has.
- `short-fade` — the whole scalp tinted with a thin, see-through layer of the
  hair color, with a soft wavy hairline. Reads as a head shaved to stubble —
  nearly bald.
- `short-fade-2` — the see-through tint of `short-fade` with a pointed widow's-peak V at
  the centre of the hairline.

Smooth short caps (solid, clean outline):
- `short` — a plain dark cap with a straight hairline, the hair running down
  both sides to the ears, lying flat with no volume on top. Draws a neat short
  cut, or hair combed flat straight back as seen from the front.
- `short2` — the plain cap of `short`, full sides, but the hairline dips down
  in the middle of the forehead in a soft M (a gentle widow's peak, or temples
  that have started to recede). Draws flat, combed-back or short hair with
  that M hairline.
- `crop` — the cap of `short` (same hairline height), but it ends at the
  temples: no sides at all, the skin bare from the temples down past the ears.
  Draws hair kept only on top with the sides clipped to the skin: "short back
  and sides", a buzz that stops at the temples.
- `crop-fade` — a smooth cap with a gently curved hairline, and the sides
  below it drawn in a faded, see-through tint of the hair color down to the
  ears. The standard short fade.
- `crop-fade2` — `crop-fade` with a flatter, straighter hairline and squared
  temple corners: a lined-up / edged-up fade. A buzz that looks like a solid
  dark cap in the photo, however short, is this or `crop-fade`, not a
  `short-fade`.
- `parted` — smooth, with a side part: a small notch in the top outline and a
  curl in the hairline to one side of centre (your left with `flip: false`),
  the hair swept up and across to the other side, highest over the part; full
  sides. Draws a side-parted cut combed up and over with height on one side:
  the classic side part, and a pompadour swept to one side.
- `middle-part` — a centre part: two smooth lobes with a notch at the top
  centre; at the forehead the hair parts in an upside-down V, with a pointed
  tip hanging down on each side of the part; full sides, a little wider than
  the head.
- `hair` — a full head of loose, wavy hair: volume on top, the front falling
  in a wave that dips onto the centre of the forehead, wavy full sides. Draws
  thick wavy hair with a forelock, not hair slicked flat.
- `emo` — a smooth cap with a long fringe swept diagonally across the
  forehead, covering one side down to the eyebrow.
- `afro` — NOT a textured afro: a SMOOTH rounded helmet with a clean outline,
  clearly wider than the head (the widest after `afro2`, wider than
  `curly2`/`curly3`), the sides down over the tops of the ears. Closer to a
  big rounded bowl cut than to a pick-out afro.

Short and textured (bumpy or spiky outline):
- `short3` — a short, dense cap of small curls: a bumpy outline all over, full
  sides. The basic short curly / textured Black hairstyle.
- `curlyFade1` — a bumpy curly top of medium height with faded (see-through)
  sides.
- `curlyFade2` — `curlyFade1` a little taller and wider (the bumpy top sticks
  out past the temples); same hairline, same faded sides.
- `blowoutFade` — the same wide, jagged mass as `curly2` (same outline and
  height, sticking out past the head on both sides down to eye level); the
  only difference is that the hair in front of each ear is cut back in a
  curve, leaving a small faded patch at the temple. Hard to tell apart from
  `curly2`, and on dark skin they look identical.
- `messy-short` — short but spiked sharply all over, like a sea urchin, with a
  sawtooth fringe: the hairline is a row of sharp teeth pointing down onto the
  forehead; spikes also stick out at the sides.
- `spike4` — short, neat spikes along the top edge, full sides, the soft-M
  hairline of `short2`. Draws a crew cut or short bristly cut that stands up a
  little.
- `spike2` — a stepped dome edged with big sharp triangular spikes, a straight
  hairline, full sides with a finely serrated edge: the `spike4` family with
  bigger spikes and a straight hairline instead of the soft M.
- `spike3` — the bushiest spiky cut: jagged spikes on top AND sticking out at
  the sides, with a soft-M hairline.
- `spike` — a block with a row of small sharp spikes along a flat top, square
  top corners and straight vertical sides: a spiky flat-top box.
- `shortBangs` — a smooth rounded bowl with a fringe of separate pointed
  strands hanging to just above the brows (skin shows between the strands);
  the sides hang straight down past the tops of the ears to about mid-ear.

Boxes (the flat-top family — unmistakable when right, badly wrong when not):
- `high` — a squared-off box of solid hair: straight vertical sides (NOT
  faded) ending beside the tops of the ears, square top corners and a flat top
  a little above the crown. It turns the rounded head into a rectangle: a
  flat-top.
- `juice` — the square box of `high` with the top slanting up from one side
  (your left with `flip: false`) to a flicked peak just past the centre, then
  a notch and a lower corner; solid full sides.
- `tall-fade` — a squared, rounded-shouldered box with a finely bumpy (fuzzy)
  top edge, a straight hairline and FADED sides; a little taller than `high`,
  and the one for a high-top fade.

Curly and afro, medium to big:
- `curly` — medium height, loose bumpy curls with a few wisps on top, full
  sides; the curls spill a little over the forehead corners. Draws tight waves
  or curls on top.
- `curly2` — a wide, jagged mass of spiky curls sticking out past the head on
  both sides (about 15% of the head's width on each side), down over the tops
  of the ears; the same size as `curly3`, spikier. `afro2` is this same
  drawing scaled up.
- `curly3` — a round, dense mass of small tight curls with a bumpy (rounded,
  not spiky) outline, as wide as `curly2`, full sides down over the tops of
  the ears. Also twists or locs, of any length.
- `afro2` — the real afro: the jagged, spiky mass of `curly2` scaled up, the
  widest hair faces.js draws (sticking out past the head by about a fifth of
  the head's width on each side), with the sides coming down over the top
  third of the ears. Only a little taller than `curly2`; `faux-hawk` and
  `dreads` are taller.

faces.js draws all hair close to the head, so every cut comes out SMALLER than
it looks in a photo. Judge big hair by its width against the face: hair that
sticks out beyond the face on each side by a third of the face's own width or
more is big, and needs `afro2` even when the curls are loose. Less than that
is `curly2` or `curly3`. In a test, a big loose afro drawn as `curly2` came
out as a modest crop. An afro takes `hairBg: none`: the hanging layer only
adds straight strands under it.

Braids, locs and raised centres:
- `cornrows` — clear vertical rows running back over the top, the sides faded.
  The only braided option: use it for any braids, tight to the scalp or
  hanging.
- `dreads` — NOT hanging locs: short faded sides with a big speckled bundle of
  locs tied up on TOP of the head (a pineapple top-knot), by far the tallest
  hair.
- `faux-hawk` — the hair raised to a tall pointed peak in the centre, the sides
  full.
- `fauxhawk-fade` — the central peak of `faux-hawk` with faded sides.

Long:
- `longHair` — smooth straight hair parted to one side (your right with `flip:
  false`), the fringe swept diagonally across the forehead over the outer end
  of one eyebrow, the sides hanging past the ears and ending in
  outward-flicked points about level with the bottom of the nose, well above
  the jaw. The longest hair id, but only `hairBg: longHair` reaches the jaw.
  Straight hair only — not locs, not curls.
- `shaggy1` — long, choppy strands swept to one side, with strand lines drawn
  on top; the sides hang in pointed strands over the ears to about earlobe
  level.
- `shaggy2` — `shaggy1`'s exact outline plus a fringe of pointed strands
  hanging over the forehead down to the eyebrows (the eyes stay clear); sides
  over the ears to earlobe level.
- `messy` — medium length, a chunky mass with a few flicked points on top and
  a ragged, notched fringe, full sides flaring slightly at the bottom.

Braids, locs and twists, whatever their length, are drawn from the TOP of
the head only, with `hairBg: none`. faces.js has no hanging locs or braids:
its hanging layer is two smooth, pointed straight strands flaring out at the
jaw, and on a braided or locked player it reads as a long straight haircut
he doesn't have. In testing, every player given it for braids or locs looked
less like himself than with `none`.
- Braids or cornrows, tight or hanging → `cornrows`.
- Locs or twists → `curly3` (a full head of them) or `short3` (short ones).
- A bun or top-knot of locs → `dreads`.
- Hair pulled back and tied BEHIND the head (a low ponytail or bun, which a
  front-on photo barely shows): draw what the front shows — braided rows →
  `cornrows`; smooth hair pulled flat → `short` or `crop`; bumpy pulled-back
  locs or curls → `short3`.

### hairBg

Hair drawn BEHIND the head, independently of the hair id. Set it from how far
the hair actually hangs, not from the style name.

- `none` — nothing behind the head. Every cut that stops above the ears.
- `longHair` — a narrow curtain of smooth, straight hair behind each cheek,
  visible from the ears down to chin level beside the jaw, each side ending in
  two or three pointed wisps flicking outward. It is drawn even under `bald`
  or a short cap. Only for STRAIGHT or
  wavy hair that really hangs to the jaw or longer (a long-haired rocker,
  hair tucked behind the ears). Never for braids, locs, twists or an afro:
  it draws them as straight hair.
- `shaggy` — a few small, thin spiky tufts poking out beside the jaw corners,
  from mouth level to the chin, well below the ears; small but visible.

On a cut that stops above the ears, any `hairBg` adds hair that is not there.
Most players need `none`: in a batch of 100 players, only a few with long
straight hair should get anything else.

### facialHair

Make three calls, then build the id from the groups below:

1. **UPPER LIP** — nothing, a thin patchy mustache, or a full mustache.
2. **CHIN** — nothing, a soul patch, a patch on the chin, a pointed or
   triangle goatee, or a tuft hanging below the chin.
3. **JAW AND CHEEKS** — nothing, sideburns, a strap along the jaw, a short
   full beard, or a long or bushy beard.

Only jaw and cheeks → a full beard (`beard2`, `beard1`, `beard3`,
`beard-point`) or a jaw strap / sideburns group. Mustache + chin, cheeks
bare → a circle beard (`fullgoatee*`, `wilt`) if they join around the mouth,
otherwise a chin-plus-mustache goatee. Chin only → a chin-only goatee.
Mustache only → `mustache1` / `mustache-thin`. Patchy young growth → the
hatch ids (`goatee-thin`, `goatee-thin-stache`, `mustache-thin`). Soft haze
with no edge → no id, set `head.shave` instead.

Every drawing except the three hatch ids is a solid shape in `hair.color` with
a thin black outline; the hatch ids are black strokes whatever `hair.color`
is. Each is one fixed drawing that only stretches sideways with `fatness`: it
does not follow the head id, and a full beard paints its own jaw, so under a
full beard the head ids look almost alike. Stubble is NOT drawn here; it is
`head.shave` (see Stubble below). Suffixes: `-stache` /
`Stache` add a mustache; `SB1` / `-sb-1` add LONG sideburns and `SB2` / `-sb-2`
SHORT ones; `soul` adds a soul patch. The absence of a suffix does not mean
the absence of a mustache: several plain ids are drawn with one.

- `none` — clean shaven.

Full beards (cheeks, jaw and chin, with a mustache):
- `beard2` — a full beard with a mustache joined to it, a strip up the side of
  the face to the temples, hanging a little below the chin; the cheek edge
  drops straight down the side of the face and turns flat into the mustache at
  the mouth corners. Same size and length as `beard1`, a touch less cheek: the
  common groomed look.
- `beard1` — `beard2`'s beard (same outline, same length) with the cheek edge
  running diagonally from the temple down to the mouth corner, so more of the
  lower cheek is covered; a thinner mustache band.
- `beard3` — the bushiest: `beard2`'s cheeks and mustache with a wide, rounded
  bottom and a ragged, bumpy outline, hanging well below the chin (about twice
  as far as `beard2`).
- `beard-point` — `beard2`'s cheeks and mustache, the sides running straight
  down to a sharp V point far below the chin, onto the neck: the longest beard
  in the set.
- `beard4` — a boxy beard on the chin and the front of the jaw only, with a
  mustache; the cheeks and the jaw back toward the ears are bare.
- `beard5` — a full beard whose chin is braided and tied off with a
  TEAM-COLORED bead. The bead is always drawn: never use it for an ordinary
  beard.
- `beard6` — a full beard with several braids across the chin, each tied off
  with a TEAM-COLORED bead. Never for an ordinary beard.
- `honest-abe-stache` — a heavy beard over the jaw and cheeks with a
  mustache: reads as a full beard.
- `honest-abe` — the heavy jaw-and-cheek beard of `honest-abe-stache` with the upper lip BARE:
  a chin curtain, the Lincoln / Amish look.

Circle beards (mustache joined around the mouth to a chin patch, cheeks
bare):
- `fullgoatee` — the narrowest circle beard: a squared ring a little wider
  than the mouth (mustache, thick bands past the mouth corners, a solid chin
  block to the bottom of the chin) with bare skin between the lower lip and
  the chin. `wilt`'s outline, not filled in.
- `fullgoatee2` — a little wider and heavier.
- `fullgoatee3` — the widest ring: the sides bulge out well past the mouth
  corners, about halfway to the edge of the face.
- `fullgoatee4` — a `fullgoatee2` ring whose chin narrows into a pointed V
  hanging below the chin.
- `fullgoatee5` — a circle beard with a braided chin tied off with a
  TEAM-COLORED bead. Never for an ordinary goatee.
- `fullgoatee6` — a circle beard with several braids on the chin, each tied
  off with a TEAM-COLORED bead. Never for an ordinary goatee.
- `wilt` — a solid box goatee: mustache and chin filled in as one heavy
  rectangle around the mouth.
- `wilt-sideburns-long` — `wilt` plus long sideburns from the temple down to
  about mouth level, curving forward onto the cheek (the `sideburns2` shape).
- `wilt-sideburns-short` — `wilt` plus short sideburns.

Chin only, no mustache:
- `soul` — a soul patch: a small triangle just under the lower lip.
- `goatee1` — a wide band along the bottom of the chin with a narrow strip
  rising from its middle to just under the lower lip: an inverted T.
- `goatee2` — a rounded triangle on the chin: narrow just under the lower lip,
  widening to a broad, rounded bottom at the chin line. Does not hang below
  the chin.
- `goatee3` — a narrow tuft hanging BELOW the chin: starts at the chin bottom
  and tapers to a ragged, split point well under it. No hair between the lip
  and the chin.
- `goatee4` — a wide crescent along the bottom edge of the chin.
- `goatee5` — a wide, bushy block covering the lower chin with a rounded
  bottom and a jagged top edge of three upward prongs, the middle one reaching
  nearly to the lip.
- `goatee7` — a thin line along the bottom of the chin.
- `goatee8` — a soul patch plus a thin line along the bottom of the chin.
- `goatee9` — a thin vertical line from the lower lip down to a thin chin line
  (an anchor shape without the mustache).
- `goatee10` — a soul patch plus a band along the bottom of the chin that dips
  to a point in the middle, just below the chin.
- `goatee17` — a soul patch plus the `goatee3` tuft: a narrow jagged point
  hanging below the chin.
- `goatee18` — a soul patch plus a wide crescent along the bottom of the chin.

Chin plus mustache:
- `goatee1-stache` — `goatee1` with a solid mustache.
- `goatee4-stache` — `goatee4` (chin crescent) with a solid mustache.
- `goatee6` — a mustache, a soul patch and a bushy, jagged chin patch.
- `goatee11` — `goatee10` plus a mustache: mustache, soul patch, and a chin
  band dipping to a point below the chin.
- `goatee12` — a mustache plus the `goatee2` chin patch: a rounded triangle,
  narrow under the lip, broad and rounded at the chin, stopping at the chin.
- `goatee15` — a mustache plus the `goatee3` tuft: a narrow jagged point
  hanging below the chin, nothing between lip and chin.
- `goatee16` — a mustache, a soul patch and the `goatee3` tuft hanging below
  the chin.
- `goatee19` — a mustache, a soul patch and a crescent along the bottom of the
  chin.
- `soul-stache` — a mustache and a soul patch.

Patchy growth (drawn as short BLACK hatch marks, not a solid shape; black
even on a blond or red-haired player):
- `goatee-thin` — short vertical BLACK hatch marks on the lower chin only
  (black whatever `hair.color` is).
- `goatee-thin-stache` — black hatch marks on the upper lip and the lower
  chin: a thin, patchy mustache and goatee. Black whatever `hair.color` is, so
  wrong for a blond or red-haired player.
- `mustache-thin` — black hatch marks on the upper lip only: reads patchy
  rather than thin; black whatever `hair.color` is.

Mustache only:
- `mustache1` — a solid, full mustache curving over the upper lip.
- `mustache1SB1` — `mustache1` plus long sideburns.
- `mustache1SB2` — `mustache1` plus short sideburns.

Jawline strips:
- `chin-strap` — a band from the temples (it doubles as sideburns) down the
  whole jaw and round the chin, widening toward the chin, with a short square
  tab sticking up at the middle of the chin; the tab stops well short of the
  lip. No mustache.
- `chin-strapStache` — `chin-strap` plus a mustache.
- `neckbeard` — NOT under the jaw: a heavy band covering the chin and the jaw
  from one jaw corner to the other, hanging a little below the chin, with a
  rounded tongue rising from its middle to just under the lower lip; the
  cheeks, the jaw back to the ears, and the upper lip are bare.
- `neckbeard2` — the heavy jawline band of `neckbeard` with a mustache.
- `neckbeardSB1` — `neckbeard` plus long sideburns.
- `neckbeardSB2` — `neckbeard` plus short sideburns.
- `neckbeard2SB1` — `neckbeard2` plus long sideburns.
- `neckbeard2SB2` — `neckbeard2` plus short sideburns.

Sideburns and mutton chops (cheeks covered, chin bare unless stated):
- `sideburns1` — long sideburns from the temple down to about mouth level,
  widening into a sharp wedge whose point aims forward at the mouth.
- `sideburns2` — long sideburns from the temple down to about mouth level,
  curving forward onto the cheek with a rounded end. The same sideburns the
  `SB1` / `-sb-1` ids add.
- `sideburns3` — short, thin sideburns beside the ears. The same sideburns the
  `SB2` / `-sb-2` ids add.
- `mutton` — mutton chops: sideburns widening down the jaw toward the mouth;
  the chin and upper lip bare.
- `muttonStache` — `mutton` plus a mustache.
- `muttonSoul` — `mutton` plus a soul patch.
- `muttonStacheSoul` — `mutton` plus a mustache and a soul patch.
- `muttonGoatee1` — `mutton` plus the `goatee1` chin patch, which meets the
  ends of the chops: a continuous beard round the jaw and chin with no
  mustache and bare skin around the mouth.
- `muttonGoatee2` — `mutton` plus the `goatee2` rounded-triangle chin patch,
  bare skin between it and the chops.
- `muttonGoatee5` — `mutton` plus the jagged-topped `goatee5` block filling
  the chin between the chops: a continuous jaw-and-chin beard with no
  mustache.
- `muttonGoatee1Stache` — `muttonGoatee1` plus a mustache.
- `muttonGoatee2Stache` — `muttonGoatee2` plus a mustache.
- `muttonGoatee5Stache` — `muttonGoatee5` plus a mustache.
- `logan` — the biggest chops: covering most of each cheek and wrapping round
  the jaw until they nearly meet under the chin, leaving a bare, rounded
  channel from the mouth down to the chin tip; no mustache.
- `loganSoul` — `logan` plus a soul patch.
- `loganGoatee2` — `logan` plus the `goatee2` rounded-triangle chin patch in
  the bare channel.
- `loganGoatee2Stache` — `loganGoatee2` plus a mustache.
- `loganGoatee3` — `logan` plus the `goatee3` tuft: a narrow jagged point
  hanging below the chin.
- `loganGoatee3soul` — `loganGoatee3` plus a soul patch.
- `loganGoatee3soulStache` — `loganGoatee3soul` plus a mustache.

Horseshoe / handlebar (a mustache with two strips running down past the
corners of the mouth to the jaw):
- `harley1` — the horseshoe alone.
- `harley2` — the horseshoe plus a soul patch.
- `harly3` — the horseshoe with the `goatee2` rounded-triangle patch filling
  the chin between the strips. Note the spelling: `harly3`, not `harley3`.
- `harley1-sb-1` — `harley1` plus long sideburns.
- `harley1-sb-2` — `harley1` plus short sideburns.
- `harley2-sb-1` — `harley2` plus long sideburns.
- `harley2-sb-2` — `harley2` plus short sideburns.
- `harly3-sb-1` — `harly3` plus long sideburns.
- `harly3-sb-2` — `harly3` plus short sideburns.

### eye

Make three calls, then find the eye (and `eye.angle` from the tilt):

1. **LID** — high (the whole iris shows), low (the lid covers the top of the
   iris: relaxed, hooded), or a heavy dark line/crease over the eye.
2. **OPENING** — normal almond, narrow, or wide (white all round the iris).
3. **TILT** of the line from inner to outer corner — up, level or down.

| lid / opening → | normal almond        | narrow                                   | wide    |
| --------------- | -------------------- | ---------------------------------------- | ------- |
| high            | `eye13`              | `eye16`                                  | `eye15` |
| low / hooded    | `eye14`              | `eye19`                                  | `eye14` |
| heavy line      | `eye12`; peaked `eye18` | `eye16`; slanting hard `eye17`        | `eye18` |

`eye.angle`: tilted up 4–8, level 0–2, down −3 to −6. The white cartoon eyes
(`eye1`–`eye11`, below) are for a deliberately cartoonish look only.

Three drawing styles.

Soft OFF-WHITE eyes with a solid black upper lid and no line along the
bottom; they read as real eyes:

- `eye13` — an off-white almond under a black arched lid line, the big dark
  iris touching the lid, white showing below and to both sides: open,
  neutral.
- `eye14` — the `eye13` almond with the upper lid lowered across the top of the
  iris: relaxed, hooded, sleepy, calm.
- `eye12` — an almond under a heavy dark lash line along the top: defined,
  intense eyes.
- `eye15` — a tall off-white eye under a high, peaked black lid arch, a small
  pupil floating in the middle with white all round: startled, staring.

The same off-white, framed by a HEAVY black lid line over the top and corners,
bottom open: narrow, strong-lidded real eyes:

- `eye16` — a flat off-white strip under a straight, heavy lid bar whose ends
  bend down at both corners, the pupil hanging from the bar: level and
  sleepy.
- `eye19` — the `eye18` shape lower and flatter: a shallow roof-shaped heavy
  lid over a narrow off-white eye, pupil pressed against it: heavy-lidded.
- `eye18` — a heavy lid line peaked like a roof (∧) over a flat-bottomed
  off-white eye, short legs down at both corners, pupil under the peak:
  wide, alert.
- `eye17` — a squared-off angular wedge: the heavy lid slants down toward the
  nose and hooks up at the outer end: the hardest, sternest look.

Pure white with a black outline; these read as a cartoon:

- `eye6` — an almond, outlined over the top, with the largest pupil of the
  white eyes tucked against the lid.
- `eye9` — a smaller, flat-bottomed almond, rounded at the outer end and
  pointed at the inner corner toward the nose; a small upright oval pupil
  pressed against the top.
- `eye4` — a wide eye with a straight flat top, a sharp outer corner and a
  deep rounded bottom (a D on its side), medium pupil: a lowered-lid,
  unimpressed look.
- `eye10` — a clean lemon-shaped white oval, the pupil floating in the middle
  with white all round. NOT an ordinary eye: it reads surprised.
- `eye2` — a small dome: arched top, flat unlined bottom.
- `eye8` — a large, lopsided white oval, rounded at the outer end and squarer
  at the inner end, a small pupil floating with white all round: a wide,
  staring look.
- `eye1` — a huge tall dome with a tall oval pupil, the most cartoonish in the
  set.
- `eye3` — a full circle with a thick bar across the middle: half-closed.
- `eye11` — a flat lid line across the top of a rounded shape, the white
  below it: half-closed.
- `eye5` — the widest eye: a wide, squarish box outlined over the top and
  sides only, with a tall VERTICAL slit pupil. Unusual; only on purpose.
- `eye7` — a flat rectangular slit: a deadpan look, flatter than any real
  narrow eye.

A laugh squint narrows the eyes: judge the eye at rest (see "Tell the face
apart from the expression").

### eyebrow

Make three calls, then find the brow (and `eyebrow.angle` from the slope):

1. **THICKNESS** — thin, medium or thick/bushy.
2. **SHAPE** — straight, a soft arch, a high/strong arch, or an angled peak.
3. **ENDS** — even width, or thick inside tapering to a thin tail; short or
   long.

| thickness / shape → | straight                          | soft arch                 | strong arch            | angled peak            |
| ------------------- | --------------------------------- | ------------------------- | ---------------------- | ---------------------- |
| thin                | `eyebrow19`; tapered `eyebrow15`  | `eyebrow18`               | `eyebrow5`             | `eyebrow17`            |
| medium              | tapered `eyebrow13`, `eyebrow3`, `eyebrow11` | `eyebrow16`, `eyebrow20` | `eyebrow5`   | `eyebrow9`, `eyebrow4` |
| thick               | `eyebrow7`; slab `eyebrow6`; wedge `eyebrow2`; forked end `eyebrow12` | `eyebrow1`, `eyebrow14` | `eyebrow14` | `eyebrow9`; scooped `eyebrow10` |
| bushy               | `eyebrow6`                        | `eyebrow8`                | `eyebrow8`             | `eyebrow8`             |

`eyebrow.angle`: outer ends higher than the inner (a V, stern) 4–10; level
0–3; outer ends lower (worried) −3 to −8.

Thickness first, then shape. The brows are drawn in `hair.color`.

- `eyebrow1` — thick and rounded at the inner end, sweeping out in a long arch
  to a thin point: the classic tapered arch.
- `eyebrow2` — a straight wedge, thin at the outer end and thickening to an
  angular cut at the inner end.
- `eyebrow3` — long and sleek: thick at the inner end, tapering to a fine
  point far out. Nearly straight.
- `eyebrow4` — a flat bar bent into a shallow chevron, peaking in the middle,
  with squared ends.
- `eyebrow5` — medium-thick, even width, bent into a clear arch with the outer
  end dropping lowest; squared inner end, rounded outer end.
- `eyebrow6` — a thick straight rectangular slab, no arch at all.
- `eyebrow7` — thick and nearly straight, with rounded, slightly bulbous
  ends: a natural heavy brow.
- `eyebrow8` — the boldest: a very thick, bushy lump with an arched top.
- `eyebrow9` — thick at the inner end, rising to a peak near the middle, then
  sloping down to a long thin tail: an angular arch.
- `eyebrow10` — thick and scooped: it sags in the middle and the outer ends
  flick up.
- `eyebrow11` — long, flat on top with a curved underside, thick at the inner
  end and tapering to a point.
- `eyebrow12` — a thick flat bar whose outer end is cut into a notched,
  forked tip.
- `eyebrow13` — medium, nearly straight, thick at the inner end and tapering
  to a long point, sloping slightly down.
- `eyebrow14` — a thick domed crescent: arched top, flat bottom, a blunt
  rounded inner end, the outer end tapering down to a point.
- `eyebrow15` — medium-thin, long, almost straight, tapered at both ends.
- `eyebrow16` — short and medium, with a slight arch.
- `eyebrow17` — short and sharply arched like a caret (^), the peak near the
  outer end, the long side sloping down toward the nose. The most unusual
  shape.
- `eyebrow18` — short, gently arched, thick at the inner end, tapering to a
  thin outer point: a shorter, thinner `eyebrow1`.
- `eyebrow19` — a thin straight bar of even width: the flattest option.
- `eyebrow20` — short and medium-thick with a slight S-wave and rounded ends.

### nose

Make three calls on every nose, each from the photo, before you pick:

1. **TIP** — plain (straight, nothing special), pointed, rounded/fleshy,
   bulbous (a ball on the end), turned up (nostrils show from the front), or
   hooked (curving down).
2. **LENGTH**, brows to tip, against the gap from the tip to the mouth —
   short, medium or long.
3. **WIDTH** at the nostrils, against the gap between the inner eye corners —
   narrow, medium or wide.

Real noses are seldom medium on all three; say which way each one leans.
Then find the drawing:

| tip            | short / narrow         | medium                     | long / big                  | wide                  |
| -------------- | ---------------------- | -------------------------- | --------------------------- | --------------------- |
| plain          | `nose10`               | `nose7`                    | `nose7` at size 1.15–1.25   | `nose11`              |
| pointed        | `nose3`                | `nose3` / `pinocchio`      | `nose9` (long, narrow)      | `nose3` at size 1.15  |
| rounded/fleshy | `nose8`                | `nose12`                   | `honker` (narrow) / `nose6` | `nose11` / `nose12`   |
| bulbous        | `nose8`                | `nose13`                   | `nose13` / `nose6`          | `nose12` / `nose6`    |
| turned up      | `nose14`               | `small`                    | `small` at size 1.15        | `nose1`               |
| hooked         | `nose4`                | `nose2`                    | `nose2` at size 1.15–1.25   | `nose2` / `nose6`     |

Very broad and flat, nostrils flared wide → `nose5`. Photo lit from one side
or the face turned three-quarters, so only one side of the nose shows as a
line → the one-sided drawings (`nose4` short, `nose9` medium, `nose2` long),
whatever the tip.

Then set `nose.size` from the length and width you called: short or narrow
0.8–0.9, medium 1, long or wide 1.1–1.25. Two men who both get `nose7` still
differ here.

- `nose7` — a single bridge line down the middle over a flat base line: an
  upside-down T. Draws a plain, straight, medium nose, seen straight on,
  with no distinct tip, width or length.
- `nose12` — a single bridge line down the middle over a full rounded nostril
  outline. Draws a long nose whose nostril wings show: a fleshy, rounded
  tip on a broad base.
- `nose6` — two parallel bridge lines running into a squared-off nostril base
  with flared wings, the biggest drawing with `nose12`. Draws a large,
  prominent nose: long AND broad, a nose people would mention.
- `honker` — a long narrow U-shaped tube. Draws a long, narrow nose whose
  rounded tip hangs low: a long, drooping nose, NOT broad.
- `nose4` — a narrow line down from the bridge that kinks at the bottom into
  a short foot slanting back toward the middle, drawn off-centre. Draws a
  short, narrow, straight nose lit from one side.
- `nose9` — a medium line ending in a small hook. Draws a narrow, straight,
  medium nose with a small defined tip, one side in shadow.
- `nose2` — a long line ending in a rounded hooked tip (a J). Draws a long
  nose with a tip that curves down and rounds under: a Roman or aquiline
  nose, or any long nose seen with side light.
- `nose13` — a big round C. Draws a bulbous, ball-like tip seen from the side:
  a big round nose end.
- `pinocchio` — a small "7": a short stroke bending sharply into a diagonal
  running down and back toward the middle, drawn off-centre. Draws a small
  sharp nose whose tip sticks out, seen from one side: a pointed or
  ski-slope nose.
- `nose11` — a rounded base outline with both nostrils drawn, no bridge.
  Draws a broad, soft nose with rounded nostrils and a low bridge.
- `nose5` — a wider, flatter base with curled nostrils: the widest drawing.
  Draws a very broad, flat nose with wide-set nostrils.
- `nose1` — a single wide wavy line (~), no bridge or nostrils. Draws a low,
  soft, fairly wide nose tip with nothing sharp about it.
- `nose3` — a plain V chevron. Draws a nose with a pointed, angular tip: a
  sharp, narrow nose end.
- `small` — a wide shallow curve under the tip. Draws a neat, small nose,
  often a little turned up.
- `nose10` — a smaller, tighter curve. Draws a very small, short nose.
- `nose14` — a tiny squared bracket (∩). Draws a small button nose, turned
  up, the nostrils showing from the front.
- `nose8` — a short stub over a small arched base. Draws a short nose with a
  rounded, slightly upturned tip: a snub nose.

In old studio portraits the front light shows a bridge on almost every face,
so a visible bridge says nothing on its own there: judge those noses by their
tip, length and width like any other.

The one-sided drawings take `flip`; put the line on the shadowed side. For
`nose4`, `nose9`, `nose2` and `pinocchio`, `flip: false` puts the line on YOUR
right as you look at the photo, `true` on your left. `nose13` is the reverse:
`false` puts its C on your LEFT. (`nose4` and `pinocchio` move to that side
as a whole.) Flip changes nothing visible on the other noses.

### mouth

Make three calls, then find the mouth:

1. **OPEN** — closed, lips just parted, teeth showing, or wide open.
2. **CORNERS** — turned down, level, or up (a smile), or one side only.
3. **LIPS / WIDTH** — thin or defined lips; a narrow or a wide mouth.

| open / corners → | down     | level                                        | up                                   | one side |
| ---------------- | -------- | -------------------------------------------- | ------------------------------------ | -------- |
| closed           | `closed` | thin `straight`; soft `mouth5`; lips defined `mouth6` | slight `smile-closed`; broad `smile4` | `side` |
| just parted      | `mouth4` | `mouth4`, `mouth2`                           | `mouth3`                             | `side`   |
| teeth showing    | `angry`  | `mouth7`, `mouth8`                           | `mouth7`; broad beaming `smile`      | `mouth7` |
| wide open        | `angry`  | `mouth`                                      | laugh `smile3`, `smile2`             | `smile2` |

(The expression rules above the list still apply: a shout mid-play is
`mouth2`, a big grin is kept one step calmer.)

Match the expression in the photo, one step calmer: this face appears on
every screen in the game, so a big grin is kept but never exaggerated. The
mappings below already include that step, so use them as written. A polite
closed-mouth smile → `smile-closed`; a slight smile with the lips just parted
→ `mouth3`. A smile with teeth showing:

- teeth showing, the mouth no wider than usual → `mouth7`;
- a broad, beaming grin, mouth stretched wide and cheeks pushed up → `smile`.
  `mouth7` is mostly black inside with lip lines stacked above and below, so
  on a beaming face it reads as an "ooh"; in testing, every big grin in a
  batch drawn as `mouth7` lost the smile.
- a full laugh, mouth wide open → `smile3`.

A mouth open mid-play (a shout, a grimace, breathing hard) is the moment,
not his look: → `mouth2`, slightly parted. Never `smile` for an open mouth
that isn't smiling, and never `angry` for effort.

Closed:
- `straight` — a short flat bar: the most minimal mouth.
- `closed` — a wider flat bar with the ends bent down: pressed, stern.
- `mouth5` — a soft, slightly wavy line with small curled ends, an upper-lip
  arc above and a lower-lip arc below: a relaxed closed mouth.
- `mouth6` — thin upper- and lower-lip arcs around a long, gently bowed line:
  a closed mouth with both lips defined.
- `smile-closed` — a clean upward U arc: a closed smile.
- `smile4` — a wide closed upward arc whose corners hook up into dimples: a
  broad closed grin, the widest mouth of all.
- `side` — a slanted line rising to one side with a kink: a one-sided smirk,
  strongly asymmetric.

Open:
- `mouth3` — a small smile with a thin white crescent of teeth under the
  upper lip, ticks at the corners, a short lower-lip line: a slight smile,
  lips just parted.
- `mouth2` — a small white slit between the lips, an upper-lip line above:
  slightly parted.
- `mouth4` — a thin flat white slit between an upper-lip arc and a lower-lip
  arc: barely parted.
- `mouth` — a wide, flat open oval (a bean dented at the top centre), white
  inside, no lip lines.
- `mouth7` — open, showing a solid band of TEETH, with an upper-lip line: the
  toothy smile.
- `mouth8` — like `mouth7` (black inside, a band of upper teeth, a lower-lip
  line) but with one line splitting the front teeth and no upper-lip line.
- `smile` — an open half-moon (flat top, round bottom), white inside: a broad
  open smile.
- `smile3` — the widest open half-moon grin in the set.
- `smile2` — a small open rounded box with little strokes at the corners: a
  laugh.
- `angry` — a wide open mouth with a wavy, clenched outline: a grimace.

### ear

Two calls: **SHAPE** (slim, round, or full and squarish) → `ear2` / `ear3` /
`ear1`; **SIZE** (small, ordinary, big or prominent) → `ear.size` 0.8–0.9 /
1 / 1.2–1.5.

The size slider matters more than the shape.

- `ear2` — the slimmest: a rounded top tapering to a narrow bottom with a
  small lobe tucked against the head (a teardrop, point down): ordinary ears.
- `ear1` — the largest shape: a rounded top, a straight vertical outer edge,
  the bottom cut back diagonally to the head (a D): full, squarish ears.
- `ear3` — a round C-shaped cup: ears that are visibly round rather than long.

### eyeLine

NOT an eyelid crease, whatever the name suggests: age and detail marks around
the eye. Each one ages the face, so a young face with no such marks takes
`none`.

- `none` — no marks.
- `line1` — two short curved marks above the inner ends of the brows: frown
  furrows.
- `line2` — crow's feet: small lines radiating from the outer eye corners.
- `line3` — a short crease under the inner half of each eye, curving up toward
  the nose: a tear-trough line, subtle bags.
- `line4` — a long, shallow ∪ curve under each whole eye: eye bags.
- `line5` — an upward-bowed arch (∩) under each eye, a little lower and wider
  than `line4`: a cheekbone / puffy lower-lid line.
- `line6` — a fine arc along the underside of each brow, curving down toward
  the nose: a heavy brow bone / deep-set eyes; at normal size it reads as a
  heavier brow.

### smileLine

The folds either side of the mouth. `smileLine.size` sets their LENGTH (1 =
short marks beside the mouth corners, 2 = long folds from the nose to the
jaw; at 0.5 they are barely visible). A full beard is drawn over them and
hides them, as it hides `chin1`/`chin2`.

- `none` — no folds (still give it `"size": 1`).
- `line1` — long parentheses `( )` curving away from the mouth: nasolabial
  folds.
- `line3` — shorter, rounder parentheses `( )`.
- `line2` — the `line1` fold drawn angular, `< >`.
- `line4` — short marks drawn the OTHER way round, `> <`, bowing in toward the
  mouth: reads as dimples, not age.

### miscLine

One slot, four unrelated things.

- `none` — nothing.
- `forehead3` — one short line across the forehead with a small dip in the
  middle: faint.
- `forehead4` — one longer line across the forehead.
- `forehead2` — two lines across the forehead, each dipping slightly in the
  middle, the upper one longer.
- `forehead1` — a Y-shaped vertical furrow between the brows.
- `forehead5` — two forehead lines plus the Y furrow: the most aged.
- `chin1` — a small arc on the chin under the lower lip: a chin crease.
- `chin2` — a tiny vertical line at the bottom of the chin: a cleft chin. A
  real identifying feature; use it when the photo shows one.
- `freckles1` — dotted freckle patches on both cheeks.
- `freckles2` — four faint brown diagonal slashes on each cheek (the hatching
  of `blush` without its pink): reads as scratches or a cartoon flush, not
  freckles.
- `blush` — rosy pink ovals on the cheeks; not something a player photo calls
  for.

### glasses

- `none` — no glasses.
- `glasses2-black` — a heavy dark bar along the top edge only (browline,
  half-rim) over rimless grey-tinted rectangular lenses: ordinary glasses.
- `glasses2-primary` — the `glasses2-black` browline bar in the team's main
  color, which can come out bright blue or red.
- `glasses2-secondary` — the `glasses2-black` browline bar in the team's
  second color.
- `glasses1-primary` — THICK, heavy, rounded dark frames, like sports
  goggles, with the side pieces in the team's main color. There is no
  `glasses1-black`.
- `glasses1-secondary` — the thick `glasses1-primary` frames with the side pieces in the
  team's second color.
- `facemask` — a clear grey-tinted protective shield over the forehead,
  cheekbones and upper nose, eyes open through cut-outs, dark straps at the
  temples; mouth and chin uncovered. Never unless you can see one.

faces.js has no thin wire-rim or round glasses: for any ordinary glasses,
wire rims included, `glasses2-black` is the closest.

Earrings, tattoos, chains and other jewelry cannot be drawn in faces.js.
Ignore them rather than reaching for a nearby option.

### accessories

- `none` — nothing.
- `headband` — a wide band across the upper forehead covering the hairline,
  arched so its ends drop to brow level at the temples: a sweatband worn low,
  in the team's main color with a second-color stripe. Always drawn in team
  colors, whatever color it is in the photo; set it anyway, since it is a
  strong likeness cue. It hides the hairline, so choose the hair from what
  shows above it and at the sides.
- `headband-high` — the same band arched higher, sitting at the hairline
  with the whole forehead showing below it: a band pushed back into the
  hair, or a thin hairband holding back big hair.
- `hat` — a baseball cap seen from the front, all in the team's second color.
  Under any cap (and `santa-hat`) faces.js REPLACES the hair: at most a short
  dark patch at the temples, for many hair ids none at all; only `hairBg` and
  facial hair still show. So a cap costs the player his hair in the drawing.
- `hat2` — the `hat` cap with a different team-colored brim.
- `hat3` — the `hat` cap with a third team-colored brim.
- `eye-black` — two black bars under the eyes.
- `santa-hat` — a red Santa hat with a white trim and pom-pom.

### body

The shoulders and neck under the jersey. Match the shoulders and neck the
photo shows.

- `body` — smooth, rounded shoulders and a slim neck.
- `body2` — shoulders with a small bump at each shoulder cap.
- `body3` — a thick neck with trapezius lines and collarbone creases: the most
  muscular.
- `body4` — the narrowest shoulders: a short neck, straight shoulder lines
  sloping down to rounded caps.
- `body5` — broad shoulders sloping in straight lines.

`jersey` — use `jersey`; ZenGM recolors and restyles it for the sport.

## Allowed numbers

Clamp to these ranges. Round to two decimals.

| field            | range       | meaning                                                               |
| ---------------- | ----------- | --------------------------------------------------------------------- |
| `fatness`        | 0 – 1       | head width. Long narrow face ≈ 0–0.15, average ≈ 0.35, heavy ≈ 0.8    |
| `body.size`      | 0.8 – 1.05  | shoulder width                                                        |
| `ear.size`       | 0.5 – 1.5   | 1.0 is normal, 1.3+ for noticeably big ears                           |
| `nose.size`      | 0.5 – 1.25  |                                                                       |
| `smileLine.size` | 0.25 – 2.25 | LENGTH of the folds: 1 short marks by the mouth, 2 long nose-to-jaw  |
| `eye.angle`      | -10 – 15    | integer. Negative = outer corner droops down                          |
| `eyebrow.angle`  | -15 – 20    | integer. Positive = raised/arched outer end                           |

`flip` (on hair, mouth, nose) is a plain boolean that mirrors that piece — pick
whichever matches the asymmetry you see, `false` if it looks symmetric. On a
one-sided nose, `false` puts the line on your right as you look at the photo
(except `nose13`, the reverse); on `side`, `false` raises the corner on your
right.

How to read the numbers off the photo:

- **Face shape and `fatness`.** `fatness` is the head's width, and the only
  control over how long or broad the face looks.

  Measure the photo: the head's length from the top of the skull (where the
  scalp would be under the hair, not the top of the hair) to the bottom of
  the chin, divided by the face's width across the cheekbones (not the ears,
  not the hair). Real heads measure longer than the drawings, so map the
  photo's number like this:

  | photo length ÷ width | `fatness` | looks like                        |
  | -------------------- | --------- | --------------------------------- |
  | 1.85 or more         | 0         | very long and narrow              |
  | about 1.75           | 0.15      | long, lean                        |
  | about 1.65           | 0.35      | average                           |
  | about 1.55           | 0.55      | broad                             |
  | about 1.45           | 0.75      | wide, full                        |
  | 1.4 or less          | 0.9 – 1   | a short, round, heavy head        |

  (The drawings themselves run from 1.65 long at `fatness` 0 to 1.33 at 1.)
  `fatness` widens the head, hair, facial hair, glasses and accessories, and
  moves the ears out, but the eyes, brows, nose, mouth and smile lines stay
  the same size and in the same place: a high `fatness` makes the features
  look small in a broad face, a low one crowds them.
  Judge it from the face itself, not from how big the man is overall — a
  huge, muscular centre can have a lean face. Pair a long face with one
  of the narrow tapering jaws, and a broad face with a full or broad one.
- **A full beard widens the drawn face.** faces.js paints the beard as a solid
  mass around the jaw, so a bearded face reads wider and squarer than the same
  face clean-shaven. With a full beard or a large goatee, judge the head shape
  from the cheekbones and temples, not from the beard's outline, and take
  `fatness` a notch lower than you otherwise would — a lean, long face with a
  full beard wants an oval head and `fatness` around 0.15, not a square one.
- **`eye.angle`.** Imagine a line from the inner corner of the eye to the outer
  corner. Level is 0 and most faces sit between 0 and 5. Outer corner clearly
  higher (upturned, almond) → 6–12; outer corner lower (downturned, hooded,
  tired-looking) → -3 to -8.
- **`eyebrow.angle`.** The slope from the inner end of the brow to the outer end.
  Positive lifts the outer ends and drops the inner ones into a V, which reads
  stern or intense; negative does the reverse and reads worried or sad. Flat,
  level brows → around 0, and most faces sit between 0 and 6. An ARCH is a shape,
  not an angle — get it from the brow id, not from this number.
- **`nose.size`.** Compare the width of the nose at the nostrils with the gap
  between the inner corners of the eyes. About equal is 1.0. Clearly wider →
  1.1–1.25 together with a broad nose from the list above. Clearly narrower or
  shorter → 0.8–0.9. The size scales the whole drawing, so a bigger size
  also makes the nose LONGER (and its line thicker). The shape group matters more than the number, so do not
  use size to turn a narrow nose id into a broad nose.
- **`ear.size`.** 1 unless the ears are a feature. The size makes the ear
  bigger about its own centre, mostly TALLER (68 units at 1, 100 at 1.5); it
  sticks out only a little further. Big or prominent ears in a front-on
  photo → 1.25–1.5; that is a big likeness cue, so do not be timid with it.
  All three ear shapes stick out equally far.

Most photos are head-and-shoulders crops, which say nothing about shoulder width
and little about true ear size. Keep the two NUMBERS `body.size` and
`ear.size` at `1` unless the photo actually shows otherwise — a visibly broad
or narrow frame, ears that clearly stick out. (The `body` and `ear` ids are
still read from the photo like any other slot.)

## Colors

`body.color` is the SKIN tone and `hair.color` is the hair. Any hex works. Start
from the nearest step on this ladder and nudge it toward the photo rather than
inventing a color from scratch. It runs light to deep, and the steps are close
enough that picking the right neighbour matters. The steps are fairly muted, so
nudge the warmth as well as the depth: a golden or reddish-brown cheek wants a
warmer, more saturated hex than its step, or the avatar comes out greyish. The
deeper steps also lean red, so a warm brown complexion wants more orange at
the same depth (tested matches: `#bf7b58`, `#b87656` medium; `#8d5638` brown).
A ruddy fair face, common with red hair, is pinker than the fair steps:
`#e8a88a` matched one.

- Very fair: `#f5dccf`, `#f2d6cb`
- Fair: `#ecc8b3`, `#e3bda5`, `#ddb7a0`
- Light warm / golden (common in East and Southeast Asian players): `#fedac7`,
  `#f0c5a3`, `#eab687`
- Light olive / tan: `#d9a886`, `#cc9a78`
- Medium: `#bb876f`, `#b07a5f`, `#aa816f`
- Medium brown: `#a67358`, `#9b6a50`, `#8d5d45`
- Brown: `#80523e`, `#7a4a36`, `#74453d`
- Deep: `#6e4030`, `#673a2a`
- Very deep: `#5a3325`, `#4a2a20` — only for the very darkest skin

Deep skin in a studio headshot is WARM, a red-brown or chocolate, never grey
or purple. Most dark-skinned players land in Brown or Deep; in testing, real
deep-skinned players matched `#6e4030` and `#673a2a`, and the older, more
purple deep colors (`#5c3937`) drew them too dark and too grey.

Read skin from the pixels, never from the player's name, nationality or
ethnicity. Arena and flash photos shift color a lot, so correct for the light
before you pick:

- Find something that should be neutral — the whites of the eyes, teeth, a white
  jersey or background — and notice how far it is pushed toward orange, blue or
  green. Take the same shift off the skin.
- Sample the **lit** part of the cheek or forehead — not a highlight blown out to
  near white, not the shadow under the jaw. The avatar is one flat fill, so it
  needs the middle of the face, not its extremes. A studio flash puts a bright
  band down the centre of the face (forehead, nose, inner cheeks) that is
  lighter than the man's skin: read the colour across the cheek between that
  band and the shadowed side, and if the two sides of the face disagree, take
  the darker of the two lit sides.
- Harsh flash washes skin out: near-white highlights on the forehead and nose,
  and skin that samples pale, pinkish and greyish on a face whose brows, hair
  and shadows plainly belong to a brown-skinned man. There the pixels
  are wrong by two or three bands, not one, so go by the whole face rather
  than the sample. Well-lit brown skin samples saturated (orange or red-brown);
  a pale, flat sample is the flash. For milder cases of a washed-out face,
  when torn between two steps, take the deeper one (a face that is not
  washed out follows "Do not over-correct deep skin" below instead). Dim or orange arena light darkens it, so take
  the lighter one. An evenly lit headshot where the white reference reads
  clean white and the skin keeps its color needs no correction: take the cheek
  color as it is.
- **Do not over-correct deep skin.** Every rule above that says "go deeper" is
  for skin the camera has WASHED OUT. Medium-brown and deep skin in a normal
  studio headshot is not washed out: its lit cheek is already the right
  color, so take it as it is and never go deeper than it. The avatar is one
  flat fill with no highlights, and a flat fill reads DARKER than the same
  color in a photo, where highlights lift it. In a test, two deep-skinned
  players were each given a color two ladder steps deeper than their lit
  cheek, and both came out visibly too dark. When unsure between two steps
  on a dark-skinned face, take the LIGHTER one.
- Hair: black `#272421`, off-black `#0f0902` / `#1c1008`, dark brown `#3D2314` /
  `#2C1608`, medium brown `#5A3825`, light brown `#CC9966`, ginger / copper
  `#94502c` (most red-haired players), vivid orange-red `#B55239` (only when
  the red is truly bright), dark / dirty blond `#b89968` (the usual adult
  blond), light ash blond `#D7BF91`, golden blond `#e9c67b` (bright, nearly
  yellow). Grey/white hair: `#9a9a9a` – `#e8e8e8`.

**`hair.color` also colors the eyebrows and every facial-hair shape.** faces.js
has no separate brow or beard color. So:

- A **bald or shaved** player still needs a `hair.color` — set it from his
  eyebrows and beard, not left at a default. Getting this wrong gives a
  black-haired man blond eyebrows.
- If the beard and the hair differ (a grey beard under dark hair, a red beard
  under brown hair), choose the color of whichever covers more of the face in
  the avatar — usually the beard when there is a full one.
- Salt-and-pepper reads as a mid grey (`#8a8a8a`–`#a8a8a8`); do not pick pure
  white unless it truly is.
- Dyed tips or highlights: use the natural color at the ROOTS. The dye covers
  a few strands, and the root color is also the brows and beard; setting it
  from the tips gives a black-bearded man light-brown eyebrows.

Leave `teamColors` exactly as shown — ZenGM overwrites it with the player's
actual team colors.

## Stubble: `head.shave`

**This is the five o'clock shadow, and on modern photos it is the single most
commonly missed slot** (old photos have their own rule below). It is an `rgba(0,0,0,A)` string that shades the beard area of the face —
jaw, chin, upper lip, cheeks — and, on a bald or closely-cropped head, the scalp
along with it. It works whether or not the player has hair.

| alpha                                  | reads as                                   |
| -------------------------------------- | ------------------------------------------ |
| `rgba(0,0,0,0)`                        | clean shaven                               |
| `rgba(0,0,0,0.1)` – `rgba(0,0,0,0.2)`  | faint shadow, a day's growth               |
| `rgba(0,0,0,0.25)` – `rgba(0,0,0,0.4)` | a clear five o'clock shadow                |
| `rgba(0,0,0,0.5)` – `rgba(0,0,0,0.65)` | heavy stubble, a very short beard          |
| above `0.7`                            | avoid — it goes to a near-solid black mask |

**`facialHair` is for GROWN hair with a defined shape and a hard edge** — a full
beard, a goatee, a chin strap, sideburns, a distinct mustache. It is NOT for
stubble. Reaching for a `beard*` id when the player just has a shadow is the
most common way to get a face wrong: it draws a solid dark shape with a crisp
outline where the photo has a soft grey haze.

Decide which one you need before you pick either:

- Soft, no clear outline, skin still visible through it, the same length all
  over → **`head.shave`**, `facialHair: none`.
- Solid, you could trace its edge, longer than a few days' growth → a
  **`facialHair`** id, and usually `shave` at 0 or very low. A short, dense
  beard with a clean line where it stops on the cheek is `beard2`, however
  short; a shave line is an edge.
- A shaped goatee or mustache sitting in a field of stubble → **both**: the
  `facialHair` id for the shaped part, plus a `shave` alpha for the haze around
  it.

Since one value covers the face and the scalp, a bald player with heavy face
stubble also gets a shadowed crown — which is normally right for a shaved head.
A bald head reads as cleanly shaved only up to about `0.1` on fair skin and
`0.2` on dark skin; above that the scalp shows a buzz-cut cap with a
hairline. On fair
skin the same alpha shows much more: visible stubble on a fair face is
`0.1`–`0.15`, heavy stubble `0.2`–`0.25`; the table's higher steps are for
medium and dark skin.

## How to choose

1. **Skin and hair color first.** They dominate the resemblance more than any
   shape slot. Judge them from an evenly-lit part of the face (cheek, forehead),
   not a shadowed jaw or a blown-out highlight.
2. **Hair.** Length and texture before style name — see the hair descriptions above.
3. **Facial hair — check for stubble FIRST.** If it's a shadow rather than grown
   hair, that's `head.shave` and `facialHair: none`; see the section above. Only
   once you've ruled that out: full beard → `beard2` (trimmed) or another
   plain `beard*`, never the beaded ones. Chin-only → a `goatee*` or
   `fullgoatee*`. Mustache only → `mustache1`, `mustache-thin`.
   Jawline strip → `chin-strap`. Sideburns → `sideburns1`–`3`, `mutton*`.
   Clean-shaven and no shadow → `none` with `shave` at 0. Ids ending in
   `Stache`, `-stache`, `SB1`/`SB2`/`-sb-1`/`-sb-2` add a mustache or sideburns
   to the base shape.
4. **Head shape** carries the face outline — round, long, square, narrow. Set
   `fatness` alongside it; the two together do most of the silhouette.
5. **Eyes, eyebrows, nose, mouth.** Higher-numbered ids are not "better", just
   different. Read the descriptions above against the photo: pick the kind of
   shape first, then the id whose description fits best. Don't agonise
   between two ids that both fit — the kind of shape carries the resemblance.
6. **Lines.** `smileLine`, `eyeLine` and `miscLine` are the age dial, and all
   three default to `none`. Young player → all `none`, or a small `smileLine`.
   30s → `smileLine` around 1.0. Veteran → `smileLine` 1.5+, a `forehead*`
   line, and `eyeLine` `line2` (crow's feet) or `line4` (eye bags) if the photo
   shows them. `chin2` is a cleft chin and `freckles1` freckles — both are
   identifying features worth setting when you can see them, at any age.
7. **Accessories/glasses only if the player actually wears them in games.** A
   headband, yes. Glasses in a photo where he is in uniform, yes. Glasses in
   a suit or street clothes, no. Never set `facemask`
   unless you can see one.
8. `jersey` — use `jersey` unless told otherwise; ZenGM recolors it.

## Old photos: black-and-white, sepia, hand-tinted, newspaper

Players from before about 1970 often have only an old photo. It has no real
color to read, so the color rules above can't be followed as written. Use
these instead.

- **Skin from brightness, not color.** Compare the skin's grey with the
  whitest white in the photo (a white jersey, the background, the eye whites)
  and the darkest dark (black hair, shadows). Skin that sits close to the
  white is fair: `#ecc8b3` / `#e3bda5`. Skin about halfway between is olive
  to medium: `#d9a886` / `#bb876f`. Skin close to the dark hair is brown to
  deep: `#8d5d45` / `#6e4030`. Always answer with a warm skin hex from the
  ladder, never a grey one. Decide it for every photo on its own, from that
  photo's own white and dark: old photos vary widely in exposure.
- **Sepia or brown-toned prints** are black-and-white with a brown cast:
  ignore the cast and read skin and hair by brightness, as above.
- **A hand-tinted or colorized photo** (an old trading card, a tinted
  portrait) has PAINTED color. The paint is usually too orange or too pink,
  and hair is often tinted one flat brown. Take the skin's depth from how
  light or dark it is, then pick the matching ladder step, not the paint's
  hue. Read tinted hair by its depth too, with the steps below: a flat
  mid-brown tint is `#5A3825`, a dark one `#3D2314`, a sandy light-brown one
  `#CC9966`, a light or yellowish one blond `#b89968`, a reddish one ginger
  `#94502c`.
- **Hair from brightness.** Black or near-black → `#272421`. Dark grey →
  dark brown `#3D2314`. Mid grey → medium brown `#5A3825`. Light-mid grey →
  light brown `#CC9966`. Light grey or near-white on a young man → blond
  `#b89968`. On an older man it may be
  grey `#9a9a9a`; judge by his age. Red hair shows as a mid grey and can't be
  told apart, so it gets the medium brown `#5A3825`.
- **Film grain, halftone dots and scratches are not stubble, freckles or
  lines.** A grainy or dotted newspaper print speckles the whole face
  evenly, and a jaw in shadow from hard studio light is lighting, not
  growth. Players then were shaved for the camera: `head.shave` stays at 0
  on an old photo unless you can see stubble texture on a lit part of the
  jaw or lip, and even then 0.1. In testing, `shave` at 0.2 on grainy
  prints drew a grey beard on clean-shaven men. Leave `freckles*`,
  `eyeLine` and `miscLine` at `none` unless the mark is clearly part of the
  face.
- **Period hairstyles.** Most men then wore short, combed hair, and the
  cuts differ in a few things you can read even on a small print: the
  HAIRLINE (straight, an M, receding), whether the hair lies FLAT or is
  raised at the front, where it is PARTED, and whether the SIDES are clipped
  to the skin. Read those, then match:
  - Combed flat straight back, a smooth cap from the front → `short`
    (straight hairline) or `short2` (an M or receding temples).
  - A side part, the hair combed across → `parted`; with a raised roll or
    wave of height swept to one side (a pompadour) → `parted` too.
  - Thick wavy hair with a wave falling onto the middle of the forehead →
    `hair`.
  - A centre part → `middle-part`.
  - Sides clipped to the skin, hair only on top (short back and sides) →
    `crop`.
  - A crew cut, short and bristly on top → `spike4`; a flat-top → `spike` or
    `high`.
  - Tight waves or curls on top → `curly`; a tousled mop → `messy`.
  - A thinning crown or receding hairline → `short-bald` or `short2`.
  When a cut fits more than one line, the most visible trait wins: real
  volume or waves standing up at the front → `hair` or `parted`, even over
  receding temples; `short`/`short2` are for hair that truly lies flat;
  `short-bald` only when the crown itself is thin.
- **Action shots, a face half hidden or turned away, a blurry newspaper
  crop:** read what you can see (the hair, the skin's brightness, a clear
  feature like big ears or a heavy brow) and keep the rest neutral. It
  still gets a real answer, never a copy-paste neutral face.

## When the photo won't support a confident call

Small, dark, blurry, side-on or heavily-shadowed photos are common. Don't stall
and don't invent detail — a wrong specific is worse than a right generic,
because I can see and correct a generic.

- Hair texture you cannot actually see on a small photo: use a SMOOTH cut
  (`crop-fade`, `short`), not a curly or spiky one. Bumpy hair drawn on a man
  with a close smooth crop is a bigger error than a smooth cap on short
  curls.
- When a feature is too small or blurred to read, take the plainest drawing
  in that slot that fits the little you can see, not an extreme. A face
  that's slightly wrong reads better than one with a hooked nose and
  squinting eyes it doesn't have. This is only for what you truly can't see.
- Where one of these slots is genuinely unreadable, leave it off: `eyeLine: none`,
  `miscLine: none`, `glasses: none`, `accessories: none`, `ear.size: 1`,
  `body.size: 1`, `flip: false`. (`eyeLine` used to be defaulted to `line1`
  here, on the assumption that the name meant an eyelid crease. It does not —
  `line1` is a frown furrow between the brows, and defaulting to it put one on
  every face in the game.)
- **Never** guess an accessory, glasses, or facial hair you cannot actually see.
  Adding one that isn't there is the most visible kind of error.
- Get skin tone, hair color, hair length and the stubble level right even on a
  bad photo — those four carry most of the resemblance and are the most
  recoverable from poor image quality.
- Then say which calls were shaky in the `Notes:` block. That is exactly what it
  is for.

## Before you answer

Check the finished object against the photo one last time:

1. Would someone who knows him recognise him? Look again at the two or three
   distinctive features you picked out — is each one clearly visible, not
   hedged into a neutral option?
2. Skin and hair color: right step on the ladder, corrected for the lighting?
   Eyebrows and beard will be drawn in `hair.color` — is that right for them too?
3. Hair: right length, texture and hairline (full, receding `short-bald`,
   shaved, bald)? `hairBg` is `none` unless long STRAIGHT hair really falls past the ears.
4. Stubble vs grown facial hair decided deliberately, with `head.shave` set?
5. Nothing added that you cannot see: no glasses, accessory, facial hair or
   age line the photo does not show — and no squint, fold or round cheek that
   is only there because he is grinning.
6. Every id is spelled exactly as listed, every number is inside its range, and
   every key from the output shape is present.
