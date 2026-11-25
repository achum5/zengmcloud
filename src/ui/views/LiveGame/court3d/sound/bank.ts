import type { CueKind } from "./cues.ts";
import type { ReactionKind } from "./crowd.ts";

// THE SOUNDS THEMSELVES: real recordings (free to use - CC0 - and kept in
// public/audio/game), a few takes of each so nothing repeats exactly; and,
// for any not chosen yet, a rough stand-in made here so the rest can be
// heard in its place.

export type BedLevel = 0 | 1 | 2 | 3;
export type Slot =
	| CueKind
	| "whistleLong"
	// The crowd's sound at four levels: a murmur, into it, loud, the
	// building shaking (see engine.ts).
	| `bed${BedLevel}`
	| ReactionKind;

export type Bank = Map<Slot, AudioBuffer[]>;

// The recordings chosen for each sound (paths under public/).
export const SOUND_FILES: Partial<Record<Slot, string[]>> = {};

const SLOTS: Slot[] = [
	"dribble",
	"floor",
	"squeak",
	"swish",
	"rim",
	"dunk",
	"block",
	"whistle",
	"whistleLong",
	"horn",
	"buzzer",
	"bed0",
	"bed1",
	"bed2",
	"bed3",
	"cheer",
	"roar",
	"groan",
	"ooh",
	"boo",
	"applause",
];

// STAND-INS: noise and tones shaped roughly like the real thing.

const rngOf = (seed: number) => {
	let s = seed >>> 0 || 1;
	return () => {
		s ^= s << 13;
		s ^= s >>> 17;
		s ^= s << 5;
		return (s >>> 0) / 4294967296;
	};
};

// Pink noise (a building full of people is nearer pink than white).
const pink = (n: number, seed: number): Float32Array => {
	const r = rngOf(seed);
	const out = new Float32Array(n);
	let b0 = 0;
	let b1 = 0;
	let b2 = 0;
	for (let i = 0; i < n; i++) {
		const w = r() * 2 - 1;
		b0 = 0.99765 * b0 + w * 0.099046;
		b1 = 0.963 * b1 + w * 0.2965164;
		b2 = 0.57 * b2 + w * 1.0526913;
		out[i] = (b0 + b1 + b2 + w * 0.1848) * 0.2;
	}
	return out;
};
const white = (n: number, seed: number): Float32Array => {
	const r = rngOf(seed);
	const out = new Float32Array(n);
	for (let i = 0; i < n; i++) {
		out[i] = r() * 2 - 1;
	}
	return out;
};

// A two-pole band (or low) pass, in place.
const band = (
	x: Float32Array,
	sr: number,
	f: number,
	q: number,
	low = false,
) => {
	const w = (2 * Math.PI * f) / sr;
	const alpha = Math.sin(w) / (2 * q);
	const cos = Math.cos(w);
	const a0 = 1 + alpha;
	const b0 = low ? (1 - cos) / 2 : alpha;
	const b1 = low ? 1 - cos : 0;
	const b2 = low ? (1 - cos) / 2 : -alpha;
	const a1 = -2 * cos;
	const a2 = 1 - alpha;
	let x1 = 0;
	let x2 = 0;
	let y1 = 0;
	let y2 = 0;
	for (let i = 0; i < x.length; i++) {
		const x0 = x[i]!;
		const y0 = (b0 * x0 + b1 * x1 + b2 * x2 - a1 * y1 - a2 * y2) / a0;
		x2 = x1;
		x1 = x0;
		y2 = y1;
		y1 = y0;
		x[i] = y0;
	}
	return x;
};

const shape = (
	x: Float32Array,
	sr: number,
	up: number,
	hold: number,
	down: number,
) => {
	for (let i = 0; i < x.length; i++) {
		const s = i / sr;
		const e =
			s < up
				? s / up
				: s < up + hold
					? 1
					: Math.exp(-(s - up - hold) / Math.max(0.001, down));
		x[i]! *= e;
	}
	return x;
};

const add = (into: Float32Array, x: Float32Array, k = 1) => {
	for (let i = 0; i < into.length && i < x.length; i++) {
		into[i]! += x[i]! * k;
	}
	return into;
};

const tone = (n: number, sr: number, hz: number[], decay: number[]) => {
	const out = new Float32Array(n);
	for (const [j, f] of hz.entries()) {
		const d = decay[j] ?? decay[0]!;
		for (let i = 0; i < n; i++) {
			out[i]! += Math.sin((2 * Math.PI * f * i) / sr) * Math.exp(-i / sr / d);
		}
	}
	return out;
};

const normal = (x: Float32Array, peak: number) => {
	let m = 0;
	for (const v of x) {
		m = Math.max(m, Math.abs(v));
	}
	if (m > 0) {
		for (let i = 0; i < x.length; i++) {
			x[i]! *= peak / m;
		}
	}
	return x;
};

// A loop with no seam: its last bit faded into its first.
const looped = (x: Float32Array, sr: number) => {
	const fade = Math.floor(sr * 0.25);
	const n = x.length - fade;
	const out = x.slice(0, n);
	for (let i = 0; i < fade; i++) {
		const u = i / fade;
		out[i] = x[i]! * u + x[n + i]! * (1 - u);
	}
	return out;
};

const made = (sr: number, slot: Slot, take: number): Float32Array => {
	const seed = SLOTS.indexOf(slot) * 97 + take * 13 + 1;
	const n = (s: number) => Math.floor(sr * s);
	switch (slot) {
		case "dribble":
		case "floor": {
			const x = tone(n(0.25), sr, [88 + take * 6, 176], [0.03, 0.015]);
			add(
				x,
				shape(
					band(white(n(0.25), seed), sr, 2500, 0.8),
					sr,
					0.001,
					0.002,
					0.008,
				),
				0.25,
			);
			add(
				x,
				shape(
					band(pink(n(0.25), seed + 1), sr, 400, 0.7, true),
					sr,
					0.002,
					0,
					0.09,
				),
				0.3,
			);
			return normal(x, slot === "floor" ? 0.9 : 0.8);
		}
		case "squeak": {
			// (A rubber chirp: a tone bent up through a few kHz, short.)
			const len = 0.06 + 0.03 * take;
			const x = new Float32Array(n(len));
			const f0 = 1500 + 350 * take;
			const f1 = f0 * 1.6;
			let ph = 0;
			for (let i = 0; i < x.length; i++) {
				const u = i / x.length;
				const f = f0 + (f1 - f0) * Math.sin((u * Math.PI) / 2);
				ph += (2 * Math.PI * f) / sr;
				x[i] = Math.sin(ph) + 0.35 * Math.sin(2 * ph) + 0.15 * Math.sin(3 * ph);
			}
			add(x, band(white(x.length, seed), sr, f1, 2), 0.15);
			return normal(shape(x, sr, 0.004, len * 0.5, len * 0.25), 0.5);
		}
		case "swish":
			return normal(
				shape(band(white(n(0.55), seed), sr, 5200, 0.9), sr, 0.03, 0.05, 0.16),
				0.5,
			);
		case "rim": {
			const x = tone(
				n(0.9),
				sr,
				[523, 1187, 1873, 2650],
				[0.25, 0.18, 0.12, 0.08],
			);
			add(
				x,
				shape(band(white(n(0.9), seed), sr, 3000, 1), sr, 0.001, 0, 0.01),
				0.5,
			);
			return normal(x, 0.75);
		}
		case "dunk": {
			const x = tone(
				n(1.1),
				sr,
				[70, 523, 1187, 1873],
				[0.08, 0.35, 0.25, 0.15],
			);
			const rattle = shape(
				band(white(n(1.1), seed), sr, 1800, 2),
				sr,
				0.005,
				0.1,
				0.3,
			);
			for (let i = 0; i < rattle.length; i++) {
				rattle[i]! *= 0.5 + 0.5 * Math.sin((2 * Math.PI * 23 * i) / sr);
			}
			return normal(add(x, rattle, 0.6), 0.95);
		}
		case "block":
			return normal(
				shape(
					band(white(n(0.18), seed), sr, 1600, 1.2),
					sr,
					0.001,
					0.005,
					0.03,
				),
				0.7,
			);
		case "whistle":
		case "whistleLong": {
			const len = slot === "whistle" ? 0.35 : 0.95;
			const x = tone(n(len), sr, [2900, 3150, 3420], [10, 10, 10]);
			for (let i = 0; i < x.length; i++) {
				x[i]! *= 0.75 + 0.25 * Math.sin((2 * Math.PI * 31 * i) / sr);
			}
			return normal(shape(x, sr, 0.012, len - 0.08, 0.025), 0.55);
		}
		case "horn":
		case "buzzer": {
			const len = slot === "horn" ? 1.5 : 0.9;
			const base = slot === "horn" ? [220, 277] : [440];
			const x = new Float32Array(n(len));
			for (const f of base) {
				for (let h = 1; h <= 9; h++) {
					add(x, tone(n(len), sr, [f * h], [100]), 1 / h);
				}
			}
			return normal(shape(x, sr, 0.01, len - 0.1, 0.04), 0.6);
		}
		case "bed0":
		case "bed1":
		case "bed2":
		case "bed3": {
			const k = Number(slot.slice(3));
			const x = band(
				pink(n(6.25), seed),
				sr,
				[700, 1300, 2400, 4200][k]!,
				0.5,
				true,
			);
			const r = rngOf(seed + 5);
			let a = 1;
			for (let i = 0; i < x.length; i++) {
				if (i % 2400 === 0) {
					a = 0.8 + 0.4 * r();
				}
				x[i]! *= a;
			}
			return normal(looped(x, sr), [0.25, 0.4, 0.6, 0.85][k]!);
		}
		case "cheer":
			return normal(
				shape(band(pink(n(2.6), seed), sr, 1600, 0.5), sr, 0.2, 0.9, 0.6),
				0.8,
			);
		case "roar":
			return normal(
				shape(band(pink(n(3.6), seed), sr, 2200, 0.35), sr, 0.15, 1.6, 0.9),
				0.95,
			);
		case "groan":
			return normal(
				shape(band(pink(n(1.6), seed), sr, 380, 1.2), sr, 0.15, 0.4, 0.4),
				0.6,
			);
		case "ooh":
			return normal(
				shape(band(pink(n(1.2), seed), sr, 520, 1.4), sr, 0.12, 0.35, 0.3),
				0.6,
			);
		case "boo":
			return normal(
				shape(band(pink(n(1.8), seed), sr, 300, 1.5), sr, 0.2, 0.8, 0.4),
				0.6,
			);
		case "applause": {
			const x = new Float32Array(n(3));
			const r = rngOf(seed);
			const clap = shape(
				band(white(n(0.02), seed), sr, 2200, 1.5),
				sr,
				0.0005,
				0,
				0.004,
			);
			for (let k = 0; k < 900; k++) {
				const at = Math.floor(r() * (x.length - clap.length));
				for (let i = 0; i < clap.length; i++) {
					x[at + i]! += clap[i]! * (0.4 + 0.6 * r());
				}
			}
			return normal(shape(x, sr, 0.3, 1.6, 0.6), 0.6);
		}
	}
	return new Float32Array(1);
};

const TAKES = 3;

// (A slot at a time, the page let draw between.)
const standIns = async (ctx: BaseAudioContext, bank: Bank, slots: Slot[]) => {
	const sr = ctx.sampleRate;
	for (const slot of slots) {
		await new Promise((go) => setTimeout(go, 0));
		const takes = slot.startsWith("bed") || slot === "horn" ? 1 : TAKES;
		const bufs: AudioBuffer[] = [];
		for (let k = 0; k < takes; k++) {
			const data = made(sr, slot, k);
			const buf = ctx.createBuffer(1, data.length, sr);
			buf.copyToChannel(data as Float32Array<ArrayBuffer>, 0);
			bufs.push(buf);
		}
		bank.set(slot, bufs);
	}
};

const decode = async (ctx: BaseAudioContext, url: string) => {
	const res = await fetch(url);
	if (!res.ok) {
		throw new Error(`${url}: ${res.status}`);
	}
	return ctx.decodeAudioData(await res.arrayBuffer());
};

// Everything, for a device that has turned sound on: the recordings where
// there are some (a stand-in for any that won't come), stand-ins for the
// rest.
export const loadBank = async (ctx: BaseAudioContext): Promise<Bank> => {
	const bank: Bank = new Map();
	const missing: Slot[] = [];
	await Promise.all(
		SLOTS.map(async (slot) => {
			const urls = SOUND_FILES[slot];
			if (!urls || urls.length === 0) {
				missing.push(slot);
				return;
			}
			const bufs = (
				await Promise.all(
					urls.map((u) => decode(ctx, u).catch(() => undefined)),
				)
			).filter((b): b is AudioBuffer => !!b);
			if (bufs.length > 0) {
				bank.set(slot, bufs);
			} else {
				missing.push(slot);
			}
		}),
	);
	await standIns(ctx, bank, missing);
	return bank;
};
