# Photo → faces.js prompt

Paste everything below the line into a **vision-capable chat AI** (Claude, ChatGPT,
Gemini) along with one clear, front-facing photo of the player. It replies with a
JSON object you paste straight into **Tools → Customize Player → Face** in ZenGM.

Not an image *generator* — Midjourney/DALL-E/Stable Diffusion can't read a photo
and emit structured JSON. It has to be a chat model that accepts image input.

One photo per message gives the best result. If you send several at once, number
them and ask for one JSON object per photo.

If ZenGM says **Invalid JSON**, it's almost always curly quotes (`“` `”` instead of
`"`) — some chat apps and phone keyboards swap them in on copy. Replace every
curly quote with a straight one and it'll paste fine.

---

You are converting a photograph of a person into a **faces.js** `FaceConfig`
object (faces.js v5, the cartoon-avatar library used by ZenGM / Basketball GM).

Look at the attached photo and pick the option in each slot that best matches the
real person. Output **only** a single JSON object — no markdown fence, no prose,
no comments, no trailing commas. Every key listed below must be present.

Quote every key and string with a plain ASCII double quote (`"`, U+0022). Curly
quotes (`“` `”`) are not valid JSON and the game rejects the whole object.

## Output shape

```
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
- **eyeLine** (crease/eyelid line): none, line1, line2, line3, line4, line5, line6
- **smileLine** (nasolabial folds): none, line1, line2, line3, line4
- **miscLine**: none, chin1, chin2, forehead1, forehead2, forehead3, forehead4,
  forehead5, freckles1, freckles2
- **glasses**: none, glasses1-primary, glasses1-secondary, glasses2-black,
  glasses2-primary, glasses2-secondary, facemask
- **accessories**: none, headband, headband-high, hat, hat2, hat3, eye-black,
  santa-hat

Do NOT use any id beginning with `female` unless the subject is a woman; those
exist only in eye, eyebrow, hair, hairBg and head.

## Allowed numbers

Clamp to these ranges. Round to two decimals.

| field | range | meaning |
|---|---|---|
| `fatness` | 0 – 1 | face/jaw width. Lean guard ≈ 0.15, average ≈ 0.4, heavy big man ≈ 0.8 |
| `body.size` | 0.8 – 1.05 | shoulder width |
| `ear.size` | 0.5 – 1.5 | 1.0 is normal, 1.3+ for noticeably big ears |
| `nose.size` | 0.5 – 1.25 | |
| `smileLine.size` | 0.25 – 2.25 | depth of the fold; older faces higher |
| `eye.angle` | -10 – 15 | integer. Negative = outer corner droops down |
| `eyebrow.angle` | -15 – 20 | integer. Positive = raised/arched outer end |

`flip` (on hair, mouth, nose) is a plain boolean that mirrors that piece — pick
whichever matches the asymmetry you see, `false` if it looks symmetric.

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

| alpha | reads as |
|---|---|
| `rgba(0,0,0,0)` | clean shaven |
| `rgba(0,0,0,0.1)` – `rgba(0,0,0,0.2)` | faint shadow, a day's growth |
| `rgba(0,0,0,0.25)` – `rgba(0,0,0,0.4)` | a clear five o'clock shadow |
| `rgba(0,0,0,0.5)` – `rgba(0,0,0,0.65)` | heavy stubble, a very short beard |
| above `0.7` | avoid — it goes to a near-solid black mask |

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
2. **Hair.** Match length and texture before style name. Bald → `bald` or
   `short-bald` (+ a shave alpha). Buzzed/faded → `crop`, `crop-fade`,
   `short-fade`, `tall-fade`, `blowoutFade`. Afro → `afro`, `afro2`, `high`.
   Braids/locs → `cornrows`, `dreads`, `juice`. Curly → `curly`, `curly2`,
   `curly3`, `curlyFade1`, `curlyFade2`. Straight short → `short`, `short2`,
   `short3`, `parted`, `middle-part`. Long/shoulder-length → `longHair`,
   `shaggy1`, `shaggy2` — and only then set `hairBg` to `longHair` or `shaggy`.
   Everything else keeps `hairBg: none`.
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
   different — pick on shape (wide/narrow, thick/thin, arched/flat, hooked/flat/
   broad). Set the mouth to a neutral or lightly-smiling id unless the photo has
   a strong expression; a big grin looks wrong on every other screen in the game.
6. **Lines.** `smileLine` and `miscLine` are the age dial. Young player → both
   `none` or a small `smileLine`. 30s → `smileLine` around 1.0. Veteran →
   `smileLine` 1.5+ plus a `forehead*` line. `freckles1`/`freckles2` only if the
   photo clearly shows freckles.
7. **Accessories/glasses only if the player actually wears them in games.** A
   headband, yes. Glasses from a press-conference photo, no. Never set `facemask`
   unless you can see one.
8. `jersey` — use `jersey` unless told otherwise; ZenGM recolors it.

Return the JSON object and nothing else.
