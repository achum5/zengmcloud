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
no shading. For each slot, read every description, compare each with the
photo, and pick the one that matches best. The groupings below only put
similar drawings next to each other; no group or id is preferred.

### head

The outline of the face, from the temples down to the chin. Two things vary:
the SIDES (curving all the way down, or running straight) and the CHIN
(rounded, pointed, flat, or notched). `fatness` widens whichever you pick, so
choose the shape here and the width there.

- `head1` — a smooth egg: sides curve the whole way, widest at the
  cheekbones, a rounded chin clearly narrower than the cheeks. No corners
  anywhere.
- `head2` — an egg that tapers more toward the bottom: sides curve in below
  the cheeks to a narrower, softly rounded chin.
- `head3` — straight sides down to the cheek, then straight lines angling in
  to a small flat chin: a V / trapezoid jaw, with the jaw corners high.
- `head4` — an oval tapering to a small flat chin with a slight notch at its
  bottom.
- `head5` — an oval whose lower sides straighten into a softly angled jaw
  meeting at a rounded chin: the middle of the set, neither round, long nor
  square.
- `head6` — broad and soft: full rounded cheeks and a wide, rounded chin. A
  round face.
- `head7` — straight sides, then sharp jaw lines angling in steeply to a short
  flat chin: a strong, angular V jaw.
- `head8` — an oval with a small double bump (a shallow notch) in the middle
  of a rounded chin.
- `head9` — like `head8`, a little wider: rounded, with a small notch in the
  chin.
- `head10` — fairly straight sides, soft jaw corners, and a chin drawn as a
  clear W: two bumps with a notch between, like a cleft.
- `head11` — widest at the temples, the sides taper in fairly straight lines
  to a rounded chin: an inverted triangle / heart shape.
- `head12` — the widest and fullest outline: broad cheeks, a wide rounded
  chin, a big round-square face.
- `head13` — a broad oval with a wide rounded chin: a round face, less full
  than `head6`.
- `head14` — the longest, narrowest egg: sides curve gently to a small,
  slightly squared chin. The long thin face.
- `head15` — straight sides, clear angular jaw corners, and the chin brought
  to a flat point: a diamond-ish, angular jaw.
- `head16` — the boxiest: vertical sides, sharp jaw corners, a flat chin set
  at a slight angle. A rectangle.
- `head17` — straight sides and a broad flat chin with rounded corners: a
  soft square.
- `head18` — straight vertical sides and a wide flat chin with a small notch
  in the middle: a square jaw with a cleft.

### hair

Match length and texture before the style name. `hair.color` colors all of
it. Unless stated, the cut stops at or above the ears.

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
  both sides to the ears.
- `short2` — the plain cap of `short`, full sides, but the hairline dips down in the
  middle of the forehead in a soft M (a gentle widow's peak).
- `crop` — a smooth cap sitting high on the head with a gently curved
  hairline; the sides below it are bare skin. A buzz that stops at the temples.
- `crop-fade` — a smooth cap with a gently curved hairline, and the sides
  below it drawn in a faded, see-through tint of the hair color down to the
  ears. The standard short fade.
- `crop-fade2` — `crop-fade` with a flatter, straighter hairline and squared
  temple corners: a lined-up / edged-up fade. A buzz that looks like a solid
  dark cap in the photo, however short, is this or `crop-fade`, not a
  `short-fade`.
- `parted` — smooth, with a side part: the hair swept to one side with
  volume on top.
- `middle-part` — a centre part: two smooth lobes swept to each side, with a
  V dip in the middle of the forehead.
- `hair` — a medium, tousled cut: volume on top, a wave of fringe dipping to
  the centre of the forehead, full sides.
- `emo` — a smooth cap with a long fringe swept diagonally across the
  forehead, covering one side down to the eyebrow.
- `afro` — NOT a textured afro: a SMOOTH rounded helmet with a clean outline,
  wider than the head and down to the tops of the ears. Closer to a big
  rounded bowl cut than to a pick-out afro.

Short and textured (bumpy or spiky outline):
- `short3` — a short, dense cap of small curls: a bumpy outline all over, full
  sides. The basic short curly / textured Black hairstyle.
- `curlyFade1` — a bumpy curly top of medium height with faded (see-through)
  sides.
- `curlyFade2` — `curlyFade1`, a touch taller and rounder, the curls coming a
  little lower at the temples.
- `blowoutFade` — tall tufts and spikes on top, faded sides. Also the answer
  for a TALL pile of tight curls over tapered sides.
- `messy-short` — short but spiked sharply all over, like a sea urchin, with a
  straight hairline.
- `spike4` — short, neat spikes along the top edge, full sides, the soft-M
  hairline of `short2`.
- `spike2` — a taller row of sharp spikes along the top, a straight hairline,
  full straight sides.
- `spike3` — the bushiest spiky cut: jagged spikes on top AND sticking out at
  the sides, with a soft-M hairline.
- `spike` — a block with a row of small sharp spikes along a flat top and
  straight vertical sides: a spiky flat-top.
- `shortBangs` — a smooth cap with straight, jagged bangs hanging down over
  the forehead to the brows.

Tall boxes (flat-top family — unmistakable when right, badly wrong when not):
- `high` — a tall clean rectangle: flat top, vertical sides. The classic
  high-top fade.
- `juice` — the tall box of `high` with the top slanting up to one side into a
  flicked front edge.
- `tall-fade` — a shorter box with a rounded top edge and faded sides.

Curly and afro, medium to big:
- `curly` — medium height, loose bumpy curls with a few wisps on top, full
  sides; the curls spill a little over the forehead corners.
- `curly2` — the widest of the curly cuts, but only a little wider than the
  head: bumpy, spiky curls stopping at the tops of the ears.
- `curly3` — a round, dense mass of tight curls with a bumpy outline, full
  sides, medium volume. Also twists or locs, of any length.
- `afro2` — the real afro: a tall, wide mass with a jagged, textured outline,
  the biggest hair faces.js draws, though it still stops above the ears.

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
- `dreads` — NOT hanging locs: short faded sides with a big bundle of locs
  tied up on TOP of the head (a pineapple top-knot).
- `faux-hawk` — the hair raised to a tall pointed peak in the centre, the sides
  full.
- `fauxhawk-fade` — the central peak of `faux-hawk` with faded sides.

Long:
- `longHair` — the only hair id that itself hangs long: smooth straight hair
  swept diagonally across the forehead and falling past the ears to the jaw on
  both sides. Straight hair only — not locs, not curls.
- `shaggy1` — long, choppy strands swept to one side, falling over the ears to
  the jaw.
- `shaggy2` — shaggy, with a choppy fringe of strands over the forehead and
  eyes, falling past the ears.
- `messy` — medium length, chunky spiky pieces all over with a jagged fringe,
  full sides.

Braids, locs and twists, whatever their length, are drawn from the TOP of
the head only, with `hairBg: none`. faces.js has no hanging locs or braids:
its hanging layer is two smooth, pointed straight strands flaring out at the
jaw, and on a braided or locked player it reads as a long straight haircut
he doesn't have. In testing, every player given it for braids or locs looked
less like himself than with `none`.
- Braids or cornrows, tight or hanging → `cornrows`.
- Locs or twists → `curly3` (a full head of them) or `short3` (short ones).
- A bun or top-knot of locs → `dreads`.

### hairBg

Hair drawn BEHIND the head, independently of the hair id. Set it from how far
the hair actually hangs, not from the style name.

- `none` — nothing behind the head. Every cut that stops above the ears.
- `longHair` — two smooth, pointed straight strands hanging down both sides
  of the face behind the ears, flaring out at the jaw. Only for STRAIGHT or
  wavy hair that really hangs to the jaw or longer (a long-haired rocker,
  hair tucked behind the ears). Never for braids, locs, twists or an afro:
  it draws them as straight hair.
- `shaggy` — a few small spiky tufts poking out below the ears at the jaw;
  almost invisible.

On a cut that stops above the ears, any `hairBg` adds hair that is not there.
Most players need `none`: in a batch of 100 modern players, only a few with
long straight hair should get anything else.

### facialHair

Every drawing is a solid shape in `hair.color` with a hard edge. Stubble is
NOT drawn here; it is `head.shave` (see Stubble below). Suffixes: `-stache` /
`Stache` add a mustache; `SB1` / `-sb-1` add LONG sideburns and `SB2` / `-sb-2`
SHORT ones; `soul` adds a soul patch. The absence of a suffix does not mean
the absence of a mustache: several plain ids are drawn with one.

- `none` — clean shaven.

Full beards (cheeks, jaw and chin, with a mustache):
- `beard2` — a short, neatly trimmed full beard hugging the jaw, a thin band
  up the cheeks to the ears: the common groomed look.
- `beard1` — a big, thick full beard, heavy on the cheeks and jaw.
- `beard3` — the longest and bushiest: a full beard hanging well below the
  chin with a wide flat bottom.
- `beard-point` — a full beard whose chin comes down to a point.
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
- `fullgoatee` — the tightest: a thin ring around the mouth.
- `fullgoatee2` — a little wider and heavier.
- `fullgoatee3` — wider again, reaching toward the jaw corners.
- `fullgoatee4` — the fullest plain one, covering the whole chin.
- `fullgoatee5` — a circle beard with a braided chin tied off with a
  TEAM-COLORED bead. Never for an ordinary goatee.
- `fullgoatee6` — a circle beard with several braids on the chin, each tied
  off with a TEAM-COLORED bead. Never for an ordinary goatee.
- `wilt` — a solid box goatee: mustache and chin filled in as one heavy
  rectangle around the mouth.
- `wilt-sideburns-long` — `wilt` plus long sideburns down to the jaw.
- `wilt-sideburns-short` — `wilt` plus short sideburns.

Chin only, no mustache:
- `soul` — a soul patch: a small triangle just under the lower lip.
- `goatee1` — a patch from under the lower lip widening down to cover the
  bottom of the chin (a trapezoid).
- `goatee2` — a tall narrow spike from under the lip to below the chin: a
  dagger goatee.
- `goatee3` — a small block at the bottom of the chin only (a chin tuft).
- `goatee4` — a wide crescent along the bottom edge of the chin.
- `goatee5` — a bushy chin patch from under the lip with a ragged, jagged
  bottom edge.
- `goatee7` — a thin line along the bottom of the chin.
- `goatee8` — a soul patch plus a thin line along the bottom of the chin.
- `goatee9` — a thin vertical line from the lower lip down to a thin chin line
  (an anchor shape without the mustache).
- `goatee10` — a tiny soul patch plus a small point at the tip of the chin.
- `goatee17` — a soul patch plus a small block at the bottom of the chin.
- `goatee18` — a soul patch plus a wide crescent along the bottom of the chin.

Chin plus mustache:
- `goatee1-stache` — `goatee1` with a solid mustache.
- `goatee4-stache` — `goatee4` (chin crescent) with a solid mustache.
- `goatee6` — a mustache, a soul patch and a bushy, jagged chin patch.
- `goatee11` — a mustache, a small soul patch and a small chin point.
- `goatee12` — a mustache and a long pointed triangle goatee from the lip to
  below the chin.
- `goatee15` — a mustache and a block at the bottom of the chin, not touching
  the lip.
- `goatee16` — a mustache, a soul patch and a block at the bottom of the chin.
- `goatee19` — a mustache, a soul patch and a crescent along the bottom of the
  chin.
- `soul-stache` — a mustache and a soul patch.

Patchy growth (drawn as short hatch marks, not a solid shape):
- `goatee-thin` — hatch marks on the chin only.
- `goatee-thin-stache` — hatch marks on the upper lip and the chin: a thin,
  patchy mustache and goatee. The best match for young players' sparse growth.
- `mustache-thin` — hatch marks on the upper lip only: reads patchy rather
  than thin.

Mustache only:
- `mustache1` — a solid, full mustache curving over the upper lip.
- `mustache1SB1` — `mustache1` plus long sideburns.
- `mustache1SB2` — `mustache1` plus short sideburns.

Jawline strips:
- `chin-strap` — a thin strip following the jawline from ear to ear, with a
  thin line up to just under the lower lip. No mustache.
- `chin-strapStache` — `chin-strap` plus a mustache.
- `neckbeard` — NOT under the jaw: a thick band along the jawline and chin
  (a heavy chin strap) with a small patch under the lip; the cheeks and upper
  lip are bare.
- `neckbeard2` — the heavy jawline band of `neckbeard` with a mustache.
- `neckbeardSB1` — `neckbeard` plus long sideburns.
- `neckbeardSB2` — `neckbeard` plus short sideburns.
- `neckbeard2SB1` — `neckbeard2` plus long sideburns.
- `neckbeard2SB2` — `neckbeard2` plus short sideburns.

Sideburns and mutton chops (cheeks covered, chin bare unless stated):
- `sideburns1` — long, wide sideburns reaching down to the jaw corner.
- `sideburns2` — medium sideburns, stopping mid-cheek.
- `sideburns3` — short, thin sideburns beside the ears.
- `mutton` — mutton chops: sideburns widening down the jaw toward the mouth;
  the chin and upper lip bare.
- `muttonStache` — `mutton` plus a mustache.
- `muttonSoul` — `mutton` plus a soul patch.
- `muttonStacheSoul` — `mutton` plus a mustache and a soul patch.
- `muttonGoatee1` — `mutton` plus a chin patch (like `goatee1`), a gap of bare
  skin between them.
- `muttonGoatee2` — `mutton` plus a pointed chin goatee.
- `muttonGoatee5` — `mutton` plus a bushy, jagged chin patch.
- `muttonGoatee1Stache` — `muttonGoatee1` plus a mustache.
- `muttonGoatee2Stache` — `muttonGoatee2` plus a mustache.
- `muttonGoatee5Stache` — `muttonGoatee5` plus a mustache.
- `logan` — the biggest chops: covering most of each cheek and the jaw down
  almost to the chin; the chin and upper lip bare.
- `loganSoul` — `logan` plus a soul patch.
- `loganGoatee2` — `logan` plus a pointed chin goatee between the chops.
- `loganGoatee2Stache` — `loganGoatee2` plus a mustache.
- `loganGoatee3` — `logan` plus a small block at the bottom of the chin.
- `loganGoatee3soul` — `loganGoatee3` plus a soul patch.
- `loganGoatee3soulStache` — `loganGoatee3soul` plus a mustache.

Horseshoe / handlebar (a mustache with two strips running down past the
corners of the mouth to the jaw):
- `harley1` — the horseshoe alone.
- `harley2` — the horseshoe plus a soul patch.
- `harly3` — the horseshoe with a pointed goatee filling the chin between the
  strips. Note the spelling: `harly3`, not `harley3`.
- `harley1-sb-1` — `harley1` plus long sideburns.
- `harley1-sb-2` — `harley1` plus short sideburns.
- `harley2-sb-1` — `harley2` plus long sideburns.
- `harley2-sb-2` — `harley2` plus short sideburns.
- `harly3-sb-1` — `harly3` plus long sideburns.
- `harly3-sb-2` — `harly3` plus short sideburns.

### eye

Two drawing styles. Four ids have a soft OFF-WHITE eye with no heavy outline
and a big dark iris; they read as real eyes:

- `eye13` — a clean almond, the whole iris showing, the lid well above it:
  open and alert.
- `eye14` — the `eye13` almond with the upper lid lowered across the top of the
  iris: relaxed, hooded, sleepy, calm.
- `eye12` — an almond under a heavy dark lash line along the top: defined,
  intense eyes.
- `eye15` — a wide eye with a thin outline and a tiny pupil, white all round:
  startled.

Everything else is bright white with a thick black outline and reads as a
cartoon:

- `eye6` — an almond.
- `eye9` — a smaller almond with a sharp outer corner.
- `eye4` — a wide eye with a flat top and a rounded bottom, large pupil.
- `eye10` — a round white oval with a small pupil. NOT an ordinary eye: it
  reads surprised.
- `eye2` — a dome: arched top, flat bottom.
- `eye8` — a large rounded oval with a very thick outline.
- `eye1` — a huge tall dome, the most cartoonish in the set.
- `eye16` — a thick straight lid bar over a sliver of white: sleepy.
- `eye19` — a narrow almond under a thick lid line: heavy-lidded.
- `eye18` — a pointed almond tilted at the corners: alert, intense.
- `eye17` — a squared-off angular wedge with a flat, slanting top: the hardest,
  sternest look.
- `eye3` — a full circle with a thick bar across the middle: half-closed.
- `eye11` — a flat lid line across the top of a rounded shape, the white
  below it: half-closed.
- `eye5` — a tall box with a VERTICAL pupil. Unusual; only on purpose.
- `eye7` — a flat rectangular slit: a deadpan look, flatter than any real
  narrow eye.

A laugh squint narrows the eyes: judge the eye at rest (see "Tell the face
apart from the expression").

### eyebrow

Thickness first, then shape. The brows are drawn in `hair.color`.

- `eyebrow1` — thick and rounded at the inner end, sweeping out in a long arch
  to a thin point: the classic tapered arch.
- `eyebrow2` — a straight wedge, thin at the outer end and thickening to an
  angular cut at the inner end.
- `eyebrow3` — long and sleek: thick at the inner end, tapering to a fine
  point far out. Nearly straight.
- `eyebrow4` — a flat bar bent into a shallow chevron, peaking in the middle,
  with squared ends.
- `eyebrow5` — medium-thick, strongly arched, even width, rounded ends.
- `eyebrow6` — a thick straight rectangular slab, no arch at all.
- `eyebrow7` — thick and nearly straight, with rounded, slightly bulbous
  ends: a natural heavy brow.
- `eyebrow8` — the boldest: a very thick, bushy lump with an arched top.
- `eyebrow9` — thick at the inner end, rising to a peak at the outer third,
  then sloping down to a long thin tail: an angular arch.
- `eyebrow10` — thick and scooped: it sags in the middle and the outer ends
  flick up.
- `eyebrow11` — long, flat on top with a curved underside, thick at the inner
  end and tapering to a point.
- `eyebrow12` — a thick flat bar whose outer end is cut into a notched,
  forked tip.
- `eyebrow13` — medium, nearly straight, thick at the inner end and tapering
  to a long point, sloping slightly down.
- `eyebrow14` — a thick domed crescent: arched top, flat bottom, blunt ends.
- `eyebrow15` — medium-thin, long, almost straight, tapered at both ends.
- `eyebrow16` — short and medium, with a slight arch.
- `eyebrow17` — short and sharply arched like a caret (^), the outer end
  hooking down. The most unusual shape.
- `eyebrow18` — medium, gently arched, thick at the inner end, tapering to a
  thin outer point.
- `eyebrow19` — a thin straight bar of even width: the flattest option.
- `eyebrow20` — short and medium-thick with a slight S-wave and rounded ends.

### nose

Look at the nose's length, the width of its base against the gap between
the inner eye corners, whether a bridge line shows, and the shape of the
tip and nostrils, then pick the drawing that matches.

- `nose7` — a single bridge line down the middle over a flat base line: an
  upside-down T. A long, straight nose drawn modestly.
- `nose12` — two bridge lines plus the full rounded nostril outline: a long
  nose with a broad base.
- `nose6` — two long bridge lines with flaring nostrils: the biggest drawing
  in the set, a nose that dominates the face.
- `honker` — a long narrow U-shaped tube: long, NOT broad.
- `nose4` — a short line with a small kink at the bottom: a short, narrow
  nose seen with light from one side.
- `nose9` — a medium line ending in a small hook: a narrow straight nose
  with one side in shadow.
- `nose2` — a long line ending in a rounded hooked tip (a J): a longer
  narrow nose with a rounded tip.
- `nose13` — a big round C: a bulbous tip seen from the side.
- `pinocchio` — a slanted line with a sharp bend: the most protruding.
- `nose11` — a rounded base outline with both nostrils drawn, no bridge: a
  broad, soft nose.
- `nose5` — a wider, flatter base with curled nostrils: the widest and
  flattest.
- `nose1` — a soft horizontal squiggle, no hard edges: a small, soft nose.
- `nose3` — a plain V chevron: an angular, pointed tip.
- `small` — a wide shallow curve under the tip: a neat, small nose.
- `nose10` — a smaller, tighter curve: a very small nose.
- `nose14` — a tiny squared bracket (∩): a small button tip.
- `nose8` — a short stub over a small arched base: a short nose with a
  rounded tip.

The one-sided drawings (`nose4`, `nose9`, `nose2`, `nose13`, `pinocchio`)
take `flip`: `flip: false` puts the line on YOUR right as you look at the
photo, `flip: true` on your left; put it on the shadowed side.

### mouth

Match the expression in the photo, one step calmer: this face appears on
every screen in the game, so a big grin is kept but never exaggerated. A polite closed-mouth smile → `smile-closed` or
`mouth3`. A smile with teeth showing:

- teeth showing, the mouth no wider than usual → `mouth7`;
- a broad, beaming grin, mouth stretched wide and cheeks pushed up → `smile`.
  `mouth7` is small when drawn and reads as an "ooh" on a beaming face; in
  testing, every big grin in a batch drawn as `mouth7` lost the smile.
- a full laugh, mouth wide open → `smile3`.

A mouth open mid-play (a shout, a grimace, breathing hard) is the moment,
not his look: → `mouth2`, slightly parted. Never `smile` for an open mouth
that isn't smiling, and never `angry` for effort.

Closed:
- `straight` — a short flat bar: the most minimal mouth.
- `closed` — a wider flat bar with the ends bent down: pressed, stern.
- `mouth5` — a soft wavy line with a small upper-lip curve above: a relaxed
  closed mouth.
- `mouth6` — an upper-lip arc over a flat line: a closed mouth with the lip
  defined.
- `mouth3` — a closed upward curve with the corners tucked in: a faint,
  closed smile.
- `smile-closed` — a clean upward U arc: a closed smile.
- `smile4` — a wide closed upward arc whose corners hook up into dimples: a
  broad closed grin.
- `side` — a slanted line rising to one side with a kink: a one-sided smirk,
  strongly asymmetric.

Open:
- `mouth2` — a small white slit between the lips, an upper-lip line above:
  slightly parted.
- `mouth4` — a thin white slit with a lip line above: barely parted.
- `mouth` — a small open oval, white inside.
- `mouth7` — open, showing a solid band of TEETH, with an upper-lip line: the
  toothy smile.
- `mouth8` — the toothy `mouth7` with the gaps between the teeth drawn in.
- `smile` — an open half-moon (flat top, round bottom), white inside: a broad
  open smile.
- `smile3` — the widest open half-moon grin in the set.
- `smile2` — a small open rounded box with little strokes at the corners: a
  laugh.
- `angry` — a wide open mouth with a wavy, clenched outline: a grimace.

### ear

The size slider matters more than the shape.

- `ear2` — narrow and teardrop-shaped, angled slightly out at the bottom:
  ordinary ears.
- `ear1` — blocky, with a flat outer edge: ears that stand straight out.
- `ear3` — a round C-shaped cup: ears that are visibly round rather than long.

### eyeLine

NOT an eyelid crease, whatever the name suggests: age and detail marks around
the eye. Each one ages the face, so a young face with no such marks takes
`none`.

- `none` — no marks.
- `line1` — two short curved marks above the inner ends of the brows: frown
  furrows.
- `line2` — crow's feet: small lines radiating from the outer eye corners.
- `line3` — a long curve under each eye: pronounced eye bags.
- `line4` — a shorter, tighter curve under each eye: subtle bags.
- `line5` — curves lower down on the cheeks, over the cheekbones.
- `line6` — a fine arc just under each brow, above the eye: a heavy brow bone
  / deep-set eyes.

### smileLine

The folds either side of the mouth, scaled by `smileLine.size`.

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
- `forehead3` — one short wavy line across the forehead: faint.
- `forehead4` — one longer line across the forehead.
- `forehead2` — two wavy lines across the forehead.
- `forehead1` — a Y-shaped vertical furrow between the brows.
- `forehead5` — two forehead lines plus the Y furrow: the most aged.
- `chin1` — a small arc on the chin under the lower lip: a chin crease.
- `chin2` — a tiny vertical line at the bottom of the chin: a cleft chin. A
  real identifying feature; use it when the photo shows one.
- `freckles1` — dotted freckle patches on both cheeks.
- `freckles2` — diagonal hatch marks on the cheeks: reads like scarring, not
  freckles.
- `blush` — rosy pink ovals on the cheeks; not something a player photo calls
  for.

### glasses

- `none` — no glasses.
- `glasses2-black` — thin black frames with tinted lenses: ordinary glasses.
- `glasses2-primary` — the thin `glasses2-black` frames in the team's main color, which
  can come out bright blue or red.
- `glasses2-secondary` — the thin `glasses2-black` frames in the team's second color.
- `glasses1-primary` — THICK, heavy, rounded dark frames, like sports
  goggles, with the side pieces in the team's main color. There is no
  `glasses1-black`.
- `glasses1-secondary` — the thick `glasses1-primary` frames with the side pieces in the
  team's second color.
- `facemask` — a translucent protective mask over the WHOLE face, not
  eyewear. Never unless you can see one.

Earrings, tattoos, chains and other jewelry cannot be drawn in faces.js.
Ignore them rather than reaching for a nearby option.

### accessories

- `none` — nothing.
- `headband` — a team-colored band across the forehead at the hairline. Always
  drawn in team colors, whatever color it is in the photo; set it anyway, since
  it is a strong likeness cue. It hides the hairline, so choose the hair from
  what shows above it and at the sides.
- `headband-high` — the team-colored band worn higher, across the top of the head.
- `hat` — a team-colored baseball cap. It covers the
  crown and leaves the hair at the sides showing, so it is not a substitute for
  getting the hair right.
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
- `body4` — rounded shoulders, a little narrower at the neck.
- `body5` — broad shoulders sloping in straight lines.

`jersey` — use `jersey`; ZenGM recolors and restyles it for the sport.

## Allowed numbers

Clamp to these ranges. Round to two decimals.

| field            | range       | meaning                                                               |
| ---------------- | ----------- | --------------------------------------------------------------------- |
| `fatness`        | 0 – 1       | face/jaw width. Lean guard ≈ 0.15, average ≈ 0.4, heavy big man ≈ 0.8 |
| `body.size`      | 0.8 – 1.05  | shoulder width                                                        |
| `ear.size`       | 0.5 – 1.5   | 1.0 is normal, 1.3+ for noticeably big ears                           |
| `nose.size`      | 0.5 – 1.25  |                                                                       |
| `smileLine.size` | 0.25 – 2.25 | depth of the fold; older faces higher                                 |
| `eye.angle`      | -10 – 15    | integer. Negative = outer corner droops down                          |
| `eyebrow.angle`  | -15 – 20    | integer. Positive = raised/arched outer end                           |

`flip` (on hair, mouth, nose) is a plain boolean that mirrors that piece — pick
whichever matches the asymmetry you see, `false` if it looks symmetric. On a
one-sided nose, `false` puts the line on your right as you look at the photo;
on `side`, `false` raises the corner on your right.

How to read the numbers off the photo:

- **Face shape and `fatness`.** Compare the face's length (hairline to chin) with
  its width (across the cheekbones). Noticeably long and narrow → an oval
  `head14`/`head1`/`head2` and `fatness` 0.1–0.25; for an exceptionally long,
  thin face, `head14` at 0–0.1, the longest faces.js can draw. About as long
  as it is wide, full cheeks, soft jaw → a wider head and `fatness` 0.6+.
  Judge `fatness` from the cheeks, jowls and neck, not from how big the man is
  overall — a huge, muscular centre can have a lean face.
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
  shorter → 0.8–0.9. The shape group matters more than the number, so do not
  use size to turn a narrow nose id into a broad nose.
- **`ear.size`.** 1 unless the ears are a feature. Ears that clearly stick out
  from the head in a front-on photo → 1.25–1.5; that is a big likeness cue, so
  do not be timid with it.

Most photos are head-and-shoulders crops, which say nothing about shoulder width
and little about true ear size. **Default `body.size` and `ear.size` to `1`** and
only move them when the photo actually shows otherwise — a visibly broad or
narrow frame, ears that clearly stick out. A guess here costs more than the
default does.

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

**This is the five o'clock shadow, and it is the single most commonly missed
slot.** It is an `rgba(0,0,0,A)` string that shades the beard area of the face —
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
If the scalp should read as cleanly shaved, stay at `0.35` or below. On fair
skin even `0.2` draws a clearly grey jaw, so go lighter there.

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
   line, and `eyeLine` `line2` (crow's feet) or `line3` (eye bags) if the photo
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
  ladder, never a grey one. Decide it for EVERY photo on its own: old photos
  vary widely in exposure, and a batch where every black-and-white face got
  the same skin color was not reading any of them. Deciding each one still
  lands many fair-skinned men on the same step, and that is fine; what the
  rule forbids is not looking.
- **A hand-tinted or colorized photo** (an old trading card, a tinted
  portrait) has PAINTED color. The paint is usually too orange or too pink,
  and hair is often tinted one flat brown. Take the skin's depth from how
  light or dark it is, then pick the matching ladder step, not the paint's
  hue. Read tinted hair by its depth too, with the steps below: a flat
  mid-brown tint is `#5A3825`, a dark one `#3D2314`, a light or sandy one
  blond `#b89968`.
- **Hair from brightness.** Black or near-black → `#272421`. Dark grey →
  dark brown `#3D2314`. Mid grey → medium brown `#5A3825`. Light grey or
  near-white on a young man → blond `#b89968`. On an older man it may be
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
- **Period hairstyles.**
  - Combed or slicked back with height and a wave at the front (a
    pompadour) → `hair`.
  - A clear side part, flat or combed over → `parted`.
  - A centre part → `middle-part`.
  - Tight waves or curls on top → `curly`.
  - A short crew cut → `crop` or `short`.
  - A 1950s flat-top → `spike` (a flat top with bristle) or `high`.
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
- Where a slot is genuinely unreadable, use the plain default: `eyeLine: none`,
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
