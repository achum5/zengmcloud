# AI player headshot prompt (media day, transparent background)

The prompt template for generating player headshot photos (media-day style,
subject only, transparent background) with an AI image generator.

## The prompt

> Professional NBA media day headshot photograph of a basketball player,
> chest-up framing, facing the camera with a slight confident smile. He is
> wearing a [TEAM COLORS] basketball jersey with "[TEAM NAME]" across the
> chest. Studio lighting: soft key light, even exposure, sharp focus on the
> face, shallow depth of field. Shot on an 85mm lens. Isolated subject cut out
> on a fully transparent background, PNG with alpha channel, no backdrop, no
> shadows, no floor, no environment — nothing behind the player. Clean crisp
> edges around hair and shoulders.

Fill in the placeholders per player:

- `[TEAM COLORS]` — e.g. "green and white Celtics"
- `[TEAM NAME]` — e.g. "Celtics"
- Optionally append character details for consistency:
  `[SKIN TONE], [AGE]-year-old, [HAIR STYLE], [BUILD]`

## Transparency caveat — read before batch-generating

Most image models (DALL·E, Midjourney, standard Stable Diffusion,
Gemini/Imagen) **cannot output a true alpha channel** no matter what the
prompt says — they paint a fake checkerboard or a plain backdrop instead.
Two reliable routes:

1. **Native transparency support** — Recraft, Ideogram (background remover),
   or SDXL with a transparency/LayerDiffuse pipeline. The prompt above works
   as-is and yields a real PNG alpha.
2. **Two-step (works with any generator)** — replace the last two sentences
   with: _"isolated on a solid flat chroma-key green background (#00FF00),
   no shadows, no gradient"_ — then strip the background with remove.bg,
   Photoshop, or programmatically with `rembg` (Python). Solid-color plates
   cut out much cleaner than trying to force "transparent" in the prompt.

For batch generation across league rosters, route 2 with `rembg` scales best.

Generated headshots live in `playerFaces/`.
