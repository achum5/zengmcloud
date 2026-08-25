# Photo → faces.js prompt

Paste everything below the line into a **vision-capable chat AI** (Claude, ChatGPT,
Gemini) along with one clear, front-facing photo of the player. It replies with a
JSON object you paste straight into **Tools → Customize Player → Face** in ZenGM.

Not an image _generator_ — Midjourney/DALL-E/Stable Diffusion can't read a photo
and emit structured JSON. It has to be a chat model that accepts image input.

One photo per message gives the best result. If you send several at once, number
them and ask for one JSON object per photo.

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
real person.

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
them is a default or a suggestion. Read every slot off the photo. Reply in this
exact form, fence and all.

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
- **hairBg** (the mass of hair drawn BEHIND the head — only for hair that falls
  past the ears): none, longHair, shaggy
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

## What the shapes actually look like

The names above say nothing about the drawings, so here is what each group of
ids reads as. Every description below was written by RENDERING the option and
looking at it, not by reading its name — several of the names are actively
misleading (`afro` is a smooth cap, `dreads` is a top-knot, `eyeLine` is not an
eyelid crease).

Pick the GROUP from the photo first, then any id inside it. Options inside a
group differ by small amounts and are close to interchangeable; a few are called
out individually because they are distinctive enough to be a wrong answer rather
than an approximate one.

**head** — two things vary, and they are independent: how WIDE the face is, and
whether the jaw runs straight down to a flat chin or curves in to a rounded one.
`fatness` adds width on top of whichever you pick, so choose the shape here and
set the width there.

- Oval — the sides curve continuously and the chin is rounded and clearly
  narrower than the cheeks: `head1`, `head2`, `head14` (narrowest, a long egg),
  `head8`, `head9`, `head13`, `head4`, `head5` (the neutral middle of the whole
  set — use it when the photo won't say)
- In between — the sides straighten but the jaw corners stay soft: `head6`,
  `head7`, `head10`, `head11` (widest at the temples, tapering)
- Square — the sides run straight down to a flat base with corners you can
  point at: `head3`, `head15`, `head16`, `head17`, `head18`, `head12` (the
  widest and boxiest in the set)

**eye** — the sclera is drawn either bright white (reads cartoonish) or a soft
off-white (reads more natural); it is a real difference at a glance.

- Big and wide open, plenty of white: `eye1` (huge, squared-off top — the most
  cartoonish), `eye8` (large angular hexagon), `eye15` (large but a thin
  outline and a tiny pupil, reads startled), `eye2` (a plain dome), `eye4`
  (pointed almond, large pupil), `eye12`
- Ordinary almond, the neutral default: `eye10`, `eye13`, `eye6`, `eye9`
- Narrow, heavy-lidded, sleepy: `eye16` (a thick lid bar over a sliver of
  white), `eye19`, `eye14`, `eye5` (tall and narrow with an unusual VERTICAL
  pupil)
- Angled and squinting, stern or intense: `eye17` (sharp angular wedge, the
  hardest look in the set), `eye18`
- Drawn with the lid CUTTING ACROSS the eye, which reads as half-closed however
  big the shape underneath is: `eye3` (a full circle with a straight lid across
  the top), `eye11`
- `eye7` is a pure horizontal bar with no curve at all. It is a deliberate
  deadpan/slit look, not a narrow eye — do not reach for it just because the
  subject's eyes are small

**eyebrow** — thickness first, then arch.

- Thick and heavy: `eyebrow8` and `eyebrow14` (the boldest), `eyebrow7`,
  `eyebrow1`, `eyebrow5` (thick AND strongly arched), `eyebrow12` (thick with
  angular, notched ends), `eyebrow10` (thick with a wavy underside),
  `eyebrow6` (a plain thick rectangular slab, no arch at all)
- Medium: `eyebrow15`, `eyebrow9`, `eyebrow16`, `eyebrow18`, `eyebrow20`
- Thin and fine: `eyebrow3` (long and sleek, tapering to a point), `eyebrow13`,
  `eyebrow11`, `eyebrow2` (a thin angular wedge), `eyebrow19` (a thin, almost
  perfectly straight bar — the flattest option)
- Distinctly arched or peaked: `eyebrow5` (a rounded arch), `eyebrow4` (a flat
  bar with a hard chevron PEAK in the middle), `eyebrow9` (a kink toward the
  inner third), `eyebrow17` (short, and the outer end curls up into a hook —
  the most unusual shape here, so only when the photo shows it)

**nose** — read the photo for three things: is there a bridge line down the
middle, are the nostrils drawn, and how wide is the base. There is no default;
these are genuinely different noses.

- Barely drawn — a small curve or bracket, for a neat or narrow nose: `small`
  and `nose10` (a shallow downward arc), `nose14` (a small squared bracket),
  `nose8` (a short bridge stub over a small base)
- A soft horizontal squiggle, no hard edges and no bridge: `nose1`
- One clear line down ONE SIDE, ending in a hook — an angular or straight
  profile seen slightly off-centre: `nose2` (long and sloped), `nose13`,
  `nose9`, `nose4` (the shortest of them), `pinocchio` (a sharp bend, the most
  protruding)
- Angular tip with no side line: `nose3`, drawn as a plain V chevron
- A full base outline with visible NOSTRILS, for a broad fleshy nose: `nose11`,
  `nose5` (the widest and flattest)
- Long, with a bridge line running down the middle — these fill the centre of
  the face, so use them when the nose is genuinely the biggest feature on it:
  `nose6` (two lines with flaring tips), `honker` (a long narrow tube — long,
  NOT broad), `nose12` (a bridge line plus the full nostril base — the largest
  nose in the set), `nose7` (a bridge line over a wide shallow base)

**mouth** — pick the expression first. A neutral or lightly-closed mouth is
almost always right: this face appears on every screen in the game and a big
grin wears badly.

- Closed, neutral: `straight` (a short flat bar, the most minimal), `closed` (a
  wider flat bar with angled ends, reads stern/pressed), `mouth5` (a soft wavy
  lip line), `mouth6`, `mouth3` (a closed upward curve — a faint smile),
  `smile-closed` (a clean upward arc; the safe default when the subject is
  smiling politely with the mouth shut)
- Slightly open and relaxed: `mouth2`, `mouth4` (thin slivers with lip lines
  above and below), `mouth` (a small open oval)
- Open, showing a plain white gap: `smile` (a broad half-moon), `smile3` (the
  widest grin in the set)
- Open with TEETH actually drawn: `mouth7` (a solid band of teeth), `mouth8`
  (teeth with the gaps between them drawn in)
- Unusual, use only when the photo really shows it: `smile2` (a small round
  laughing "O"), `smile4` (angular strokes at the corners), `angry` (a wide
  wavy open grimace), `side` (a one-sided smirk, strongly asymmetric)

**hair** — match length and texture before you match a style name.

- Bald: `bald` — a bare scalp. The right id for essentially every bald player;
  pair it with a `head.shave` alpha (see below) and the shadow does the rest
- Shaved almost to the skin, scalp clearly showing through: `short-fade`
  (lightest), `short-fade-2`. These sit between `bald` and a buzz cut and are
  the right answer for a very close crop
- Buzzed and faded, a smooth short cap with shorter sides: `crop` (no fade
  contrast at all), `crop-fade`, `crop-fade2`, `spike4`, `curlyFade1`,
  `curlyFade2`
- Short and smooth: `short` (a plain cap with a straight hairline — the
  neutral default), `short2` (a slight quiff at the front), `parted` (a side
  part), `middle-part` (a centre part with two lobes), `hair` (tousled, with a
  peak in the fringe)
- Short and TEXTURED rather than smooth — still short, but drawn bumpy or
  spiky: `short3` (short curls round the temples), `messy-short` (short but
  strongly spiked all over), `blowoutFade` (tufted on top, faded sides),
  `shortBangs` (a jagged fringe of bangs low over the eyebrows)
- Flat-top / box — a tall block of hair with a flat top and hard faded sides.
  A very specific silhouette, unmistakable when it is right and badly wrong
  when it is not: `high` (a clean rectangle), `juice` (the same with a stepped,
  slanted front), `tall-fade` (a shorter box)
- Afro: `afro2` is the real one — big, wide and textured. `afro` is a SMOOTH
  rounded helmet with a clean outline, closer to a moderate rounded cut than to
  a pick-out afro
- Braids and locs: `cornrows` (clear vertical rows on top, faded sides),
  `dreads` (short sides with a BUNDLE of locs tied up on top — not long hanging
  locs)
- Curly, medium volume: `curly`, `curly2` (the loosest and biggest), `curly3`
- Raised in the middle: `faux-hawk` (soft, sides not shaved), `fauxhawk-fade`
  (a hard fade line at the sides)
- Spiky: `spike` (a row of sharp vertical spikes over short sides), `spike2`,
  `spike3` (bushiest)
- Long or shaggy: `longHair` (falls past the ears and frames the face),
  `shaggy1`, `shaggy2` (shorter and choppier), `messy` (chunky pieces over the
  forehead), `emo` (a long fringe swept over one eyebrow) — and ONLY for these
  set `hairBg` to `longHair` or `shaggy`; everything above keeps `hairBg: none`

`hairBg` draws its mass INDEPENDENTLY of the hair id, so setting it on a short
cut adds hair behind the head that the cut in front does not explain — it comes
out looking like a mullet. That is the only thing to be careful about here:
`longHair` hangs to about jaw level, `shaggy` is a little shorter and rougher.

**facialHair** — the families, since 83 ids is far more than the number of
actual looks.

- Full beard, mustache included, heaviest first: `beard1`, `beard3`, `beard5`,
  `beard2`, `beard6`, `beard-point` (drawn to a point at the chin), `beard4`
  (the lightest — patchy and jaw-hugging)
- Circle beard — mustache joined to a chin patch, nothing on the jaw:
  `fullgoatee` (tightest) through `fullgoatee2`, `fullgoatee3`, `fullgoatee4`,
  `fullgoatee5` to `fullgoatee6` (fullest, reaching the neck)
- Chin only, no mustache, smallest first: `soul` (a soul patch — a small
  triangle under the lip), `goatee9`, `goatee10`, `goatee7`, `goatee3`,
  `goatee17`, `goatee8`, `goatee2` (a narrow vertical strip), `goatee1`,
  `goatee5`, `goatee18`, `goatee4`
- Mustache only: `mustache1` (solid and full), `mustache-thin` (drawn as sparse
  hatch marks — reads patchy rather than thin)
- Patchy chin growth, drawn as hatch marks rather than a solid shape:
  `goatee-thin`, `goatee-thin-stache`
- Jawline strip: `chin-strap` (no mustache), `chin-strapStache` (with one)
- Chin curtain, jaw and under-chin with the upper lip left BARE — a Lincoln:
  `honest-abe`. `honest-abe-stache` adds the mustache and stops being one
- Below the jaw only, nothing on the face itself: `neckbeard`, `neckbeard2`
- Sideburns alone, longest first: `sideburns1`, `sideburns2`, `sideburns3`
- Mutton chops — wide sideburns running down the jaw toward the mouth:
  `mutton`, `logan` (the biggest), and the `muttonGoatee*` / `loganGoatee*`
  variants which add a chin patch
- Horseshoe / handlebar — a mustache with two strips running down past the
  corners of the mouth: `harley1`, `harley2` (adds a soul patch), `harly3`
  (adds a chin patch — note the spelling, it is `harly3`, not `harley3`)
- Box goatee — a hard rectangular block around the mouth and chin: `wilt`

The `-stache`, `Stache`, `SB1`/`SB2`/`-sb-1`/`-sb-2` and `soul` suffixes add a
mustache, sideburns (1 = long, 2 = short) or a soul patch to the base shape.
**The absence of a suffix does not mean the absence of a mustache** — several
plain ids (`goatee11`, `goatee12`, `goatee15`, `goatee16`, `goatee19`,
`goatee6`) are drawn with one anyway. If the subject has no mustache, prefer an
id from the "chin only" list above, which is the one that has actually been
checked.

**eyeLine** is NOT an eyelid crease, whatever the name suggests — it is a set of
age and detail marks around the eye, and `none` is the neutral default. Adding
one to a young face ages it for no reason.

- `line1` — a small vertical furrow at the inner end of each brow (a frown line)
- `line2` — crow's feet, radiating from the outer corners
- `line3` — a long curve under each eye; pronounced eye bags
- `line4` — a shorter, tighter version of the same, subtle
- `line5` — long lines running from beside the nose down across the cheeks
- `line6` — a fine line tucked under the lower edge of each brow

**smileLine** — the nasolabial folds either side of the mouth, scaled by
`smileLine.size`. `line1` and `line3` are parentheses `( )` curving away from
the mouth, `line3` the wider and rounder; `line2` is the same shape drawn
angular, `< >`. `line4` is drawn the OTHER WAY ROUND, `> <`, bowing in toward
the mouth — it reads as dimples rather than as age, so do not treat it as
interchangeable with the rest.

**miscLine** — one slot, four unrelated things.

- Forehead lines, least to most: `forehead3` (a faint short line), `forehead4`
  (one long line), `forehead2` (two lines), `forehead1` (a Y-shaped furrow
  between the brows), `forehead5` (a line plus that furrow — the most aged)
- `chin1` — a shallow crease under the lower lip
- `chin2` — a short VERTICAL line below the mouth: a cleft chin. This is a real
  identifying feature, so use it when the photo shows one
- `freckles1` — dotted freckle patches on the cheeks. `freckles2` is drawn as
  diagonal hatch marks and reads more like scarring than freckles
- `blush` — rosy patches on both cheeks; not something a player photo calls for

**glasses** — two frames, in colors.

- `glasses1-primary`, `glasses1-secondary` — THICK, heavy, rounded frames
- `glasses2-black` — thin rectangular frames in plain black
- `glasses2-primary`, `glasses2-secondary` — the same thin frames tinted from
  the team's colors, which can come out bright blue or red. `glasses2-black` is
  the safe choice for ordinary glasses; there is no `glasses1-black`
- `facemask` is a translucent protective mask over the WHOLE face, not eyewear

**accessories** — `hat`, `hat2` and `hat3` are the same team-colored cap with
different brim undersides, and they cover the crown while leaving the hair at
the sides showing, so a cap is not a substitute for getting the hair right.
`headband` sits at the hairline and `headband-high` an inch above it.
`eye-black` is two black bars under the eyes. `santa-hat` is what it sounds
like.

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
whichever matches the asymmetry you see, `false` if it looks symmetric.

Most photos are head-and-shoulders crops, which say nothing about shoulder width
and little about true ear size. **Default `body.size` and `ear.size` to `1`** and
only move them when the photo actually shows otherwise — a visibly broad or
narrow frame, ears that clearly stick out. A guess here costs more than the
default does.

## Colors

`body.color` is the SKIN tone and `hair.color` is the hair. Any hex works; these
are the library's own anchors, so start from the nearest one and nudge it toward
the photo rather than inventing a color from scratch.

- Light skin: `#f2d6cb`, `#ddb7a0`
- East/Southeast Asian skin: `#fedac7`, `#f0c5a3`, `#eab687`
- Medium/brown skin: `#bb876f`, `#aa816f`, `#a67358`
- Deep skin: `#ad6453`, `#74453d`, `#5c3937`
- Hair: black `#272421`, off-black `#0f0902` / `#1c1008`, dark brown `#3D2314` /
  `#2C1608`, medium brown `#5A3825`, light brown `#CC9966`, auburn `#B55239`,
  blond `#e9c67b`, dirty blond `#D7BF91`. Grey/white hair: `#9a9a9a` – `#e8e8e8`.

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
  **`facialHair`** id, and usually `shave` at 0 or very low.
- A shaped goatee or mustache sitting in a field of stubble → **both**: the
  `facialHair` id for the shaped part, plus a `shave` alpha for the haze around
  it.

Since one value covers the face and the scalp, a bald player with heavy face
stubble also gets a shadowed crown — which is normally right for a shaved head.
If the scalp should read as cleanly shaved, stay at `0.35` or below.

## How to choose

1. **Skin and hair color first.** They dominate the resemblance more than any
   shape slot. Judge them from an evenly-lit part of the face (cheek, forehead),
   not a shadowed jaw or a blown-out highlight.
2. **Hair.** Length and texture before style name — see the hair groups above.
3. **Facial hair — check for stubble FIRST.** If it's a shadow rather than grown
   hair, that's `head.shave` and `facialHair: none`; see the section above. Only
   once you've ruled that out: full beard → `beard1`–`beard6`. Chin-only → a
   `goatee*` or `fullgoatee*`. Mustache only → `mustache1`, `mustache-thin`.
   Jawline strip → `chin-strap`. Sideburns → `sideburns1`–`3`, `mutton*`.
   Clean-shaven and no shadow → `none` with `shave` at 0. Ids ending in
   `Stache`, `-stache`, `SB1`/`SB2`/`-sb-1`/`-sb-2` add a mustache or sideburns
   to the base shape.
4. **Head shape** carries the face outline — round, long, square, narrow. Set
   `fatness` alongside it; the two together do most of the silhouette.
5. **Eyes, eyebrows, nose, mouth.** Higher-numbered ids are not "better", just
   different. Use the groups above: pick the group the photo puts you in, then
   any id inside it. Don't agonise between neighbours in the same group — they
   barely differ, and the group is the part that carries the resemblance.
6. **Lines.** `smileLine`, `eyeLine` and `miscLine` are the age dial, and all
   three default to `none`. Young player → all `none`, or a small `smileLine`.
   30s → `smileLine` around 1.0. Veteran → `smileLine` 1.5+, a `forehead*`
   line, and `eyeLine` `line2` (crow's feet) or `line3` (eye bags) if the photo
   shows them. `chin2` is a cleft chin and `freckles1` freckles — both are
   identifying features worth setting when you can see them, at any age.
7. **Accessories/glasses only if the player actually wears them in games.** A
   headband, yes. Glasses from a press-conference photo, no. Never set `facemask`
   unless you can see one.
8. `jersey` — use `jersey` unless told otherwise; ZenGM recolors it.

## When the photo won't support a confident call

Small, dark, blurry, side-on or heavily-shadowed photos are common. Don't stall
and don't invent detail — a wrong specific is worse than a right generic,
because I can see and correct a generic.

- Pick the **middle of the group**, not an extreme, whenever you're unsure which
  group applies. A neutral face that's slightly wrong everywhere reads better
  than one with a hooked nose and squinting eyes it doesn't have.
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
