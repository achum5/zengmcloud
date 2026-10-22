// Colors for the field and the stadium: parsed, mixed, shaded - the same
// arithmetic the 3D court's figures use (see court3d/figure.ts), kept here so
// painting the field does not pull in the court's players.

export type RGB = [number, number, number];

export const parseColor = (c: string): RGB => {
	const m = /^#?([\da-f]{6})$/i.exec(c.trim());
	if (m) {
		const n = Number.parseInt(m[1]!, 16);
		return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
	}
	const s = /^#?([\da-f]{3})$/i.exec(c.trim());
	if (s) {
		const h = s[1]!;
		return [0, 1, 2].map((i) => Number.parseInt(h[i]! + h[i]!, 16)) as RGB;
	}
	const r = /rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)/i.exec(c);
	if (r) {
		return [Number(r[1]), Number(r[2]), Number(r[3])];
	}
	return [128, 128, 128];
};

const css = ([r, g, b]: RGB): string =>
	`rgb(${Math.round(r)}, ${Math.round(g)}, ${Math.round(b)})`;

// How light a color is, 0 (black) to 1 (white).
export const luminance = (c: string): number => {
	const [r, g, b] = parseColor(c);
	return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
};

// A color a little darker (f < 0) or lighter (f > 0).
export const shade = (c: string, f: number): string => {
	const [r, g, b] = parseColor(c);
	const t = f < 0 ? 0 : 255;
	const k = Math.abs(f);
	return css([r + (t - r) * k, g + (t - g) * k, b + (t - b) * k]);
};

// Part way from one color to another.
export const mix = (a: string, b: string, f: number): string => {
	const pa = parseColor(a);
	const pb = parseColor(b);
	return css([
		pa[0] + (pb[0] - pa[0]) * f,
		pa[1] + (pb[1] - pa[1]) * f,
		pa[2] + (pb[2] - pa[2]) * f,
	]);
};
