# AI player headshot prompt (media day, faces.js cartoon, transparent background)

The in-app version of this prompt lives in the player editor: Player page →
edit → "Generate a player image" → **Media day headshot (transparent
background)** (built in `src/worker/util/getPlayerImageMoments.ts`, which
fills in the player's details and their current team's name/colors
automatically). This doc is the reference copy plus the background-removal
notes.

## The prompt shape

> A media day headshot of [PLAYER DETAILS]: chest-up, facing the camera with a
> slight confident smile, wearing the [TEAM] jersey (team colors [COLORS])
> with authentic team lettering across the chest. Draw it in the clean, flat
> cartoon-avatar style of Basketball GM (faces.js): simple bold vector shapes,
> solid flat colors, minimal shading, front-facing and stylized. NOT
> photorealistic. Render it as a die-cut sticker style game asset: the player
> only, on a transparent background, with no border, no backdrop, and no
> shadow.

## Why the transparency phrasing looks like this (learned the hard way)

Asking ChatGPT for a photorealistic portrait "isolated on a fully transparent
background, PNG with alpha channel, no backdrop, no shadows…" produces a
**painted fake checkerboard**, not real transparency — image models paint what
you describe, and photorealistic prompts almost never trigger the real alpha
path. What actually works from the ChatGPT UI:

1. **Flat/sticker/asset styling** — "die-cut sticker", icon, flat vector. The
   cartoon style doubles as the transparency enabler.
2. **One short "transparent background" mention** — not a paragraph about
   alpha channels. Never mention checkerboards.
3. Real transparency is still not guaranteed from the chat UI. To verify a
   result: put a bright solid layer behind the PNG — if you see the color
   through it, the alpha is real.

Guaranteed alternatives:

- **OpenAI API** (`gpt-image-1`) has an actual `background: "transparent"`
  parameter — real alpha every time.
- **Chroma-key fallback for any generator**: replace the last sentence with
  _"on a solid flat chroma-key green background (#00FF00), no shadows, no
  gradient"_ and strip it afterwards with remove.bg, Photoshop, or `rembg`
  (Python). This is the route that scales for batch-generating whole rosters.

Generated headshots live in `playerFaces/`.
