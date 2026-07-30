# faceFromPhoto

Turns real player photographs into [faces.js](https://github.com/zengm-games/facesjs)
avatars, in bulk, and writes them back into a league.

Everything runs **on your machine** against an exported league file. Nothing in
the app changes, and no API key is needed — the vision step is a normal Claude
chat you paste into.

## Why it's shaped this way

`FaceConfig` isn't freeform art: it's sixteen slots, each an id from a fixed
list, plus a few colors and numbers. So this is attribute prediction over a
known vocabulary, and the work splits three ways by what's actually good at
each part:

| part                                         | done by                         | why                                                                                          |
| -------------------------------------------- | ------------------------------- | -------------------------------------------------------------------------------------------- |
| skin + hair color                            | **pixels** (`sampleColors.mjs`) | exact, free, and the biggest single contributor to likeness at avatar size                   |
| face shape, hair style, facial hair, glasses | **a vision chat**               | needs judgement, and the catalog sheets turn opaque ids like `head7` into something pickable |
| everything else                              | **`generate()`**                | invisible at 40px; a coherent random value is fine                                           |

Asking the chat for a hex gets you something plausible and wrong. Asking it
which of 21 head shapes matches a face is exactly what it's for. Each step does
only what it's best at.

## Setup (do this first)

These are scripts inside the repo, run from the **repo root**, and they need the
repo's `node_modules` — `node tools/faceFromPhoto/...` from your home directory
just gets `Cannot find module`.

```sh
git clone https://github.com/achum5/zengmcloud.git
cd zengmcloud
```

Node 24+ is required (`engines` says `^24.0.0`).

Only two packages are actually needed — `facesjs` and `playwright` — so you can
skip the app's full toolchain:

```sh
npm install --no-save facesjs playwright
npx playwright install chromium
```

Or, if you want the whole project anyway, `pnpm install` (the declared package
manager) does it and fetches the browser via its postinstall.

Two of the steps need neither: `downloadImages.mjs` and `applyFaces.mjs` use
only Node builtins, so if all you want is the photos or the final write-back,
they run with nothing installed.

Then export your league from the game (Tools → Export League, with players
included) and point the first command at wherever it landed — the examples below
say `league.json`, but it'll really be something like
`~/Downloads/BBGM_League_1_2005.json`.

## The flow

Every command below runs from the repo root.

```sh
# 0. One-time: build the menus the chat picks from.
node tools/faceFromPhoto/renderCatalog.mjs          # -> out/catalog.html, to browse
node tools/faceFromPhoto/buildPromptKit.mjs         # -> prompt-kit/ (instructions + sheets)

# 1. Get the photos out of your league.
node tools/faceFromPhoto/downloadImages.mjs league.json player-images/

# 2. Measure the colors. No model, no cost, exact.
node tools/faceFromPhoto/sampleColors.mjs player-images/ colors.json

# 3. In a Claude chat: paste prompt-kit/INSTRUCTIONS.md, attach the
#    catalog-*.png sheets, then attach a batch of photos. Save the JSON it
#    replies with as specs.json. Repeat per batch and merge.

# 4. Specs + measured colors -> real FaceConfigs. The pixels win over the chat.
node tools/faceFromPhoto/specsToFaces.mjs specs.json faces.json colors.json

# 5. Look at them before committing to anything.
node tools/faceFromPhoto/renderFaces.mjs faces.json review.png

# 6. Write them into the league, then re-import the result.
node tools/faceFromPhoto/applyFaces.mjs league.json faces.json league-with-faces.json
```

`faces.json` is keyed by Basketball-Reference id, so it's league-independent —
build it once and it re-skins any league or re-import.

## Single player

`buildFace.mjs <spec.json>` renders one spec beside its photo, which is the
quickest way to iterate on a mapping. `paul-pierce.json` and `mike-miller.json`
are worked examples.

## Notes

- **Run steps 1 and 2 locally.** Image hosts are usually unreachable from a
  sandbox, and browsers won't let a page read pixels from a cross-origin image
  anyway. Downloading server-side sidesteps both.
- `Cannot find module '.../tools/faceFromPhoto/...'` means you are not in the
  repo root. `cd` into the clone first.
- `Cannot find package 'playwright'` or `'facesjs'` means the install above was
  skipped, and `Executable doesn't exist at .../chrome` means
  `npx playwright install chromium` was.
- `downloadImages.mjs` is resumable, and `sampleColors.mjs` is deterministic, so
  re-running after adding players is cheap.
- Photos with the background already removed (see `debgLeague.py`) sample more
  accurately, because the subject is exactly the opaque pixels.
- Anything the chat omits or gets wrong falls back to a coherent `generate()`
  default. A sparse or partly-bad spec still produces a well-formed face — it
  never breaks, it just looks less like him.
