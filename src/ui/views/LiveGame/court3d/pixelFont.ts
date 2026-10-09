// A 5 x 7 pixel font, capitals, digits and a little punctuation - for words
// drawn into the pixel-art picture itself (a name under the ball handler).

const G: Record<string, string> = {
	A: "01110 10001 10001 11111 10001 10001 10001",
	B: "11110 10001 10001 11110 10001 10001 11110",
	C: "01110 10001 10000 10000 10000 10001 01110",
	D: "11110 10001 10001 10001 10001 10001 11110",
	E: "11111 10000 10000 11110 10000 10000 11111",
	F: "11111 10000 10000 11110 10000 10000 10000",
	G: "01110 10001 10000 10111 10001 10001 01111",
	H: "10001 10001 10001 11111 10001 10001 10001",
	I: "01110 00100 00100 00100 00100 00100 01110",
	J: "00111 00010 00010 00010 00010 10010 01100",
	K: "10001 10010 10100 11000 10100 10010 10001",
	L: "10000 10000 10000 10000 10000 10000 11111",
	M: "10001 11011 10101 10101 10001 10001 10001",
	N: "10001 10001 11001 10101 10011 10001 10001",
	O: "01110 10001 10001 10001 10001 10001 01110",
	P: "11110 10001 10001 11110 10000 10000 10000",
	Q: "01110 10001 10001 10001 10101 10010 01101",
	R: "11110 10001 10001 11110 10100 10010 10001",
	S: "01111 10000 10000 01110 00001 00001 11110",
	T: "11111 00100 00100 00100 00100 00100 00100",
	U: "10001 10001 10001 10001 10001 10001 01110",
	V: "10001 10001 10001 10001 10001 01010 00100",
	W: "10001 10001 10001 10101 10101 10101 01010",
	X: "10001 10001 01010 00100 01010 10001 10001",
	Y: "10001 10001 01010 00100 00100 00100 00100",
	Z: "11111 00001 00010 00100 01000 10000 11111",
	"0": "01110 10001 10011 10101 11001 10001 01110",
	"1": "00100 01100 00100 00100 00100 00100 01110",
	"2": "01110 10001 00001 00010 00100 01000 11111",
	"3": "11111 00010 00100 00010 00001 10001 01110",
	"4": "00010 00110 01010 10010 11111 00010 00010",
	"5": "11111 10000 11110 00001 00001 10001 01110",
	"6": "00110 01000 10000 11110 10001 10001 01110",
	"7": "11111 00001 00010 00100 01000 01000 01000",
	"8": "01110 10001 10001 01110 10001 10001 01110",
	"9": "01110 10001 10001 01111 00001 00010 01100",
	".": "00000 00000 00000 00000 00000 01100 01100",
	"-": "00000 00000 00000 01110 00000 00000 00000",
	"'": "00100 00100 01000 00000 00000 00000 00000",
	" ": "00000 00000 00000 00000 00000 00000 00000",
};

const GLYPHS = new Map(
	Object.entries(G).map(([ch, rows]) => [ch, rows.split(" ")]),
);

// Letters this font has no glyph for (accents and the like) as their plain
// capitals, anything else left out.
const clean = (text: string): string =>
	text
		.normalize("NFD")
		.replace(/[\u0300-\u036f]/g, "")
		.toUpperCase()
		.replace(/[^\d '.A-Z-]/g, "");

export const pixelTextWidth = (text: string, scale = 1): number => {
	const t = clean(text);
	return t.length > 0 ? (t.length * 6 - 1) * scale : 0;
};

// Draws the text with its top left at (x, y), each font pixel a `scale` by
// `scale` square of canvas pixels.
export const drawPixelText = (
	ctx: CanvasRenderingContext2D,
	text: string,
	x: number,
	y: number,
	color: string,
	scale = 1,
) => {
	const t = clean(text);
	ctx.fillStyle = color;
	let cx = Math.round(x);
	const cy = Math.round(y);
	for (const ch of t) {
		const rows = GLYPHS.get(ch);
		if (rows) {
			for (let r = 0; r < 7; r++) {
				const row = rows[r]!;
				for (let c = 0; c < 5; c++) {
					if (row[c] === "1") {
						ctx.fillRect(cx + c * scale, cy + r * scale, scale, scale);
					}
				}
			}
		}
		cx += 6 * scale;
	}
};
