// STUBBLE THE COLOUR OF THE HAIR IT GROWS FROM.
//
// facesjs draws a five o'clock shadow - on the jaw and on a shaved scalp - as
// one translucent colour, and everything that sets it (the library and the
// realistic-faces aging alike) sets it to black. On dark hair that is right.
// On a blond or red-haired player it is not: translucent black over pale skin
// is grey, so a fair-haired man with a goatee got a grey smudge round his
// mouth and a grey cap for a shaved head.
//
// So the shadow is tinted toward the hair at draw time. Nothing stored
// changes - an old league renders right without a rewrite of every face, and
// there is no synced traffic for it. It is also idempotent: the result depends
// only on the hair colour and the shadow's strength, so running it on a face
// it has already run on returns the same thing.

const ALPHA = /rgba\((?:\s*[\d.]+\s*,){3}\s*([\d.]+)\s*\)/;

const hexToRgb = (hex: string): [number, number, number] | undefined => {
	const match = /^#([\da-f]{6})$/i.exec(hex.trim());
	if (!match) {
		return undefined;
	}
	const n = Number.parseInt(match[1]!, 16);
	return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
};

// Below this the hair is dark enough that black stubble is what it looks like.
const DARK_HAIR = 0.2;
// How far the tint reaches toward the hair's own colour, which is darkened a
// little because stubble is sparser than hair and shows the skin through it.
const HAIR_DARKEN = 0.6;

export const stubbleShave = (
	shave: string | undefined,
	hairColor: string | undefined,
): string | undefined => {
	const alphaMatch = ALPHA.exec(shave ?? "");
	const rgb = hexToRgb(hairColor ?? "");
	if (!alphaMatch || !rgb) {
		return shave;
	}
	const alpha = Number.parseFloat(alphaMatch[1]!);
	if (!(alpha > 0)) {
		return shave;
	}
	const luminance = (0.2126 * rgb[0] + 0.7152 * rgb[1] + 0.0722 * rgb[2]) / 255;
	if (luminance < DARK_HAIR) {
		return `rgba(0,0,0,${alpha})`;
	}
	const t = Math.min(1, (luminance - DARK_HAIR) / 0.35);
	const [r, g, b] = rgb.map((v) => Math.round(v * HAIR_DARKEN * t));
	return `rgba(${r},${g},${b},${alpha})`;
};
