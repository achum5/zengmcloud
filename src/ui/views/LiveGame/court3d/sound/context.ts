import { soundHere } from "./setting.ts";

// THE DEVICE'S SOUND: one for the page, made (or woken) from a tap - the
// only time a browser lets a page start making sound - and put to sleep
// while the page is out of sight.

let ctx: AudioContext | undefined;

type AudioSessionLike = { type: string };
const sessionOf = (): AudioSessionLike | undefined =>
	typeof navigator === "undefined"
		? undefined
		: (navigator as unknown as { audioSession?: AudioSessionLike })
				.audioSession;

// (Heard like a video on a phone - with the ringer switched off too - only
// while it is on; back to the page's usual way after.)
const setSession = (type: "playback" | "auto") => {
	const session = sessionOf();
	if (session && session.type !== type) {
		try {
			session.type = type;
		} catch {
			// (An older phone: it keeps its own way.)
		}
	}
};

export const soundContext = (): AudioContext | undefined => ctx;

// From a tap: up and running.
export const wakeSound = (): AudioContext | undefined => {
	if (typeof AudioContext === "undefined") {
		return undefined;
	}
	if (!ctx) {
		try {
			ctx = new AudioContext({ latencyHint: "interactive" });
		} catch {
			return undefined;
		}
	}
	setSession("playback");
	if (ctx.state !== "running") {
		ctx.resume().catch(() => {});
	}
	return ctx;
};

// Turned off: quiet at once, and the phone's own music let be.
export const sleepSound = () => {
	if (ctx && ctx.state === "running") {
		ctx.suspend().catch(() => {});
	}
	setSession("auto");
};

const TAPS = ["pointerdown", "pointerup", "touchend", "keydown"] as const;

// While the court is up with sound on: woken by the first tap anywhere on
// the page (the setting may have been on since before it loaded), asleep
// while the page is hidden. Returns how to stop.
export const keepSoundAwake = (): (() => void) => {
	if (typeof document === "undefined") {
		return () => {};
	}
	const tap = () => {
		if (soundHere() && document.visibilityState === "visible") {
			wakeSound();
		}
	};
	const seen = () => {
		if (document.visibilityState === "hidden") {
			if (ctx && ctx.state === "running") {
				ctx.suspend().catch(() => {});
			}
		} else if (soundHere() && ctx) {
			ctx.resume().catch(() => {});
		}
	};
	const opts = { capture: true, passive: true } as const;
	// (A touch counts as a tap once the finger lifts.)
	for (const kind of TAPS) {
		document.addEventListener(kind, tap, opts);
	}
	document.addEventListener("visibilitychange", seen);
	return () => {
		for (const kind of TAPS) {
			document.removeEventListener(kind, tap, opts);
		}
		document.removeEventListener("visibilitychange", seen);
	};
};
