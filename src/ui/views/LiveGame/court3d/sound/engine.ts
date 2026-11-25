import type { CourtTimeline } from "../director.ts";
import type { Bank, BedLevel, Slot } from "./bank.ts";
import { cuesOf, firstCueAt, type Cue } from "./cues.ts";
import { crowdOf, levelAt, type CrowdNight, type Reaction } from "./crowd.ts";

// THE GAME'S SOUND, PLAYED: each sound on the floor and each roar of the
// crowd put on the device's own sound clock just ahead of the picture (see
// cues.ts and crowd.ts for what and when), and the crowd's level set every
// frame. It runs on the device's audio thread; the page only looks up the
// next tenth of a second of the timeline each frame.

// How far ahead of the picture each sound is put down (ms of real time).
const LOOK_MS = 120;
// A jump ahead further than this (ms of timeline) is a cut - a skip, the
// next of the key plays, joining late: what it jumped past goes unheard.
const JUMP_MS = 1500;
// Faster than this, only the big sounds: no dribbles, no hops of the ball.
const FINE_MOST = 1.6;
// Faster than this (a stretch run through fast), only the horn and the
// crowd's level.
const ANY_MOST = 3.2;
// Never more sounds at once than this: a scramble's are let go first.
const VOICES_MOST = 24;
// How quickly the crowd's level follows (s, a time constant).
const CROWD_EASE = 0.12;
// The four layers of the crowd, from a murmur up (see bank.ts).
const BEDS: BedLevel[] = [0, 1, 2, 3];
// How loud each sort of sound is, against the others.
const MIX: Partial<Record<Slot, number>> = {
	dribble: 0.45,
	floor: 0.45,
	swish: 0.6,
	rim: 0.6,
	dunk: 0.9,
	block: 0.6,
	whistle: 0.55,
	whistleLong: 0.6,
	horn: 0.55,
	buzzer: 0.5,
	cheer: 0.7,
	roar: 0.85,
	groan: 0.55,
	ooh: 0.55,
	boo: 0.55,
	applause: 0.55,
};
// The road fans: a few of them, off in a corner.
const ROAD_GAIN = 0.35;
const ROAD_PAN = 0.55;
// How wide the floor sounds across the picture: this many feet off the
// middle of the camera's view is all the way to one side, at the most this
// far over.
const PAN_FEET = 40;
const PAN_MOST = 0.7;

// The same take, pitch and level for the same moment every time it plays.
const hash01 = (t: number, k: number) => {
	let h = Math.imul(Math.round(t * 7) ^ (k * 0x9e3779b1), 0x85ebca6b);
	h ^= h >>> 13;
	h = Math.imul(h, 0xc2b2ae35);
	h ^= h >>> 16;
	return (h >>> 0) / 4294967296;
};

const slotOf = (c: Cue): Slot =>
	c.kind === "whistle" && c.long ? "whistleLong" : c.kind;

// The weight of each crowd layer at a level (0 to 1): between the two
// either side of it, evenly loud through the change.
const bedWeights = (level: number): number[] => {
	const p = Math.min(BEDS.length - 1, Math.max(0, level * (BEDS.length - 1)));
	const k = Math.floor(p);
	const u = p - k;
	return BEDS.map((_, j) =>
		j === k
			? Math.cos((u * Math.PI) / 2)
			: j === k + 1
				? Math.sin((u * Math.PI) / 2)
				: 0,
	);
};

export type SoundFrame = {
	tl: CourtTimeline;
	// Where the picture is (timeline ms), and how fast it is going through
	// it (times real time; 0 standing still).
	t: number;
	rate: number;
	// Playing: not paused, nothing shown again in place of the game.
	on: boolean;
	// Where along the floor the camera is looking (feet).
	camX: number;
};

export class GameSound {
	private readonly ctx: BaseAudioContext;
	private readonly out: GainNode;
	private readonly floor: GainNode;
	private readonly crowd: GainNode;
	private readonly bedGains: GainNode[];
	private bedSources: AudioBufferSourceNode[] = [];
	private bank: Bank | undefined;
	private readonly night: CrowdNight;
	private tl: CourtTimeline | undefined;
	// Timeline ms put down up to, and where the picture was last.
	private next = 0;
	private last = 0;
	private cue = 0;
	private reaction = 0;
	private voices = 0;
	private camX = 47;
	private level = -1;
	private levelSet = -Infinity;
	private dead = false;

	constructor(ctx: BaseAudioContext, dest: AudioNode, night: CrowdNight) {
		this.ctx = ctx;
		this.night = night;
		this.out = ctx.createGain();
		this.out.gain.value = 0;
		this.out.connect(dest);
		this.floor = ctx.createGain();
		this.floor.connect(this.out);
		this.crowd = ctx.createGain();
		this.crowd.connect(this.out);
		this.bedGains = BEDS.map(() => {
			const g = ctx.createGain();
			g.gain.value = 0;
			g.connect(this.crowd);
			return g;
		});
	}

	// The sounds, once they are here: the crowd's layers going from now on,
	// each from its own point in its loop.
	setBank(bank: Bank) {
		if (this.dead) {
			return;
		}
		this.bank = bank;
		for (const [j, level] of BEDS.entries()) {
			const buf = bank.get(`bed${level}`)?.[0];
			if (!buf) {
				continue;
			}
			const src = this.ctx.createBufferSource();
			src.buffer = buf;
			src.loop = true;
			src.connect(this.bedGains[j]!);
			src.start(0, (j * 0.37 * buf.duration) % buf.duration);
			this.bedSources.push(src);
		}
	}

	private play(
		slot: Slot,
		when: number,
		gain: number,
		pan: number,
		key: number,
		bus: GainNode,
		fine: boolean,
	) {
		const takes = this.bank?.get(slot);
		if (!takes || takes.length === 0 || gain <= 0.001) {
			return;
		}
		if (this.voices >= VOICES_MOST && fine) {
			return;
		}
		const ctx = this.ctx;
		const src = ctx.createBufferSource();
		src.buffer = takes[Math.floor(hash01(key, 1) * takes.length)]!;
		// (Each a little different: a few percent in pitch and level.)
		src.playbackRate.value = 0.96 + 0.08 * hash01(key, 2);
		const g = ctx.createGain();
		g.gain.value = gain * (0.88 + 0.24 * hash01(key, 3));
		src.connect(g);
		let tail: AudioNode = g;
		if (pan !== 0 && typeof ctx.createStereoPanner === "function") {
			const p = ctx.createStereoPanner();
			p.pan.value = pan;
			g.connect(p);
			tail = p;
		}
		tail.connect(bus);
		this.voices++;
		src.onended = () => {
			this.voices--;
			src.disconnect();
			tail.disconnect();
		};
		src.start(Math.max(ctx.currentTime, when));
	}

	private panOf(x: number | undefined, camX: number) {
		return x === undefined
			? 0
			: Math.max(-PAN_MOST, Math.min(PAN_MOST, (x - camX) / PAN_FEET));
	}

	// Each cue and roar from `from` up to `to` (timeline ms), the picture at
	// t on the sound clock at `now`, going `rate` times real time.
	private putDown(
		tl: CourtTimeline,
		from: number,
		to: number,
		t: number,
		now: number,
		rate: number,
		camAt: (t: number) => number,
	) {
		const cues = cuesOf(tl);
		const at = (x: number) => now + Math.max(0, x - t) / rate / 1000;
		for (; this.cue < cues.length && cues[this.cue]!.t < to; this.cue++) {
			const c = cues[this.cue]!;
			if (c.t < from) {
				continue;
			}
			if (rate > ANY_MOST && c.kind !== "horn") {
				continue;
			}
			if (rate > FINE_MOST && c.fine) {
				continue;
			}
			const slot = slotOf(c);
			this.play(
				slot,
				at(c.t),
				(MIX[slot] ?? 0.5) * (0.4 + 0.6 * c.hard),
				this.panOf(c.x, camAt(c.t)),
				c.t,
				this.floor,
				!!c.fine,
			);
		}
		const reactions: Reaction[] = crowdOf(tl).reactions;
		for (
			;
			this.reaction < reactions.length && reactions[this.reaction]!.t < to;
			this.reaction++
		) {
			const r = reactions[this.reaction]!;
			if (r.t < from || rate > ANY_MOST) {
				continue;
			}
			this.play(
				r.kind,
				at(r.t),
				(MIX[r.kind] ?? 0.5) * r.size * (r.home ? 1 : ROAD_GAIN),
				r.home ? 0 : hash01(r.t, 4) < 0.5 ? -ROAD_PAN : ROAD_PAN,
				r.t + 0.5,
				this.crowd,
				false,
			);
		}
	}

	// Picked up again at t: nothing before it played.
	private jumpTo(tl: CourtTimeline, t: number) {
		this.cue = firstCueAt(cuesOf(tl), t);
		const reactions = crowdOf(tl).reactions;
		let lo = 0;
		let hi = reactions.length;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if (reactions[mid]!.t < t) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		this.reaction = lo;
		this.next = t;
		this.last = t;
	}

	private crowdTo(level: number, when: number, ease: boolean) {
		// (Each frame only if it moved: the sound clock's list of changes
		// kept short.)
		if (
			ease &&
			Math.abs(level - this.level) < 0.01 &&
			when - this.levelSet < 0.25
		) {
			return;
		}
		this.level = level;
		this.levelSet = when;
		const w = bedWeights(level);
		for (const [j, g] of this.bedGains.entries()) {
			if (ease) {
				g.gain.setTargetAtTime(w[j]!, when, CROWD_EASE);
			} else {
				g.gain.linearRampToValueAtTime(w[j]!, when);
			}
		}
	}

	// ONE FRAME, as the picture is drawn.
	tick(f: SoundFrame) {
		if (this.dead || !this.bank) {
			return;
		}
		const now = this.ctx.currentTime;
		const { tl, t, rate } = f;
		this.camX = f.camX;
		if (tl !== this.tl) {
			this.tl = tl;
			this.jumpTo(tl, t);
		}
		// The crowd as the moment has it, whatever else.
		this.crowdTo(tl.warmup ? 0.12 : levelAt(tl, t, this.night), now, true);
		if (!f.on || rate <= 0) {
			this.out.gain.setTargetAtTime(f.on ? 1 : 0, now, 0.1);
			this.jumpTo(tl, t);
			return;
		}
		this.out.gain.setTargetAtTime(1, now, 0.15);
		if (t < this.last - 30 || t > this.next + JUMP_MS) {
			this.jumpTo(tl, t);
		}
		this.last = t;
		// (Sent a little early by however long the device takes to get it
		// out - a Bluetooth earbud's wait, say - so it lands with its
		// picture.)
		const late = Math.min(
			0.1,
			(this.ctx as Partial<AudioContext>).outputLatency ?? 0,
		);
		const to = t + LOOK_MS * rate;
		if (to > this.next) {
			const camX = this.camX;
			this.putDown(tl, this.next, to, t, now - late, rate, () => camX);
			this.next = to;
		}
	}

	// A FILM'S SOUND (see film tooling): all of [t0, t1] at real speed onto
	// a sound clock starting at 0 - the crowd's level set every tenth of a
	// second - for an OfflineAudioContext to render.
	putDownAll(
		tl: CourtTimeline,
		t0: number,
		t1: number,
		camAt: (t: number) => number,
	) {
		if (!this.bank) {
			return;
		}
		this.tl = tl;
		this.jumpTo(tl, t0);
		this.out.gain.setValueAtTime(1, 0);
		for (let t = t0; t <= t1; t += 100) {
			this.crowdTo(
				tl.warmup ? 0.12 : levelAt(tl, t, this.night),
				(t - t0) / 1000,
				false,
			);
		}
		this.putDown(tl, t0, t1, t0, 0, 1, camAt);
	}

	dispose() {
		this.dead = true;
		const now = this.ctx.currentTime;
		this.out.gain.cancelScheduledValues(now);
		this.out.gain.setTargetAtTime(0, now, 0.05);
		const out = this.out;
		const sources = this.bedSources;
		setTimeout(() => {
			for (const s of sources) {
				try {
					s.stop();
				} catch {
					// (Never started.)
				}
				s.disconnect();
			}
			out.disconnect();
		}, 300);
	}
}
