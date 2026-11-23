import { createRef, useState } from "react";
import type { BugRefs } from "./ScoreBug.tsx";
import { SHOT_LOW_INK } from "./scoreBug.ts";

// THE SCORE BUG AS THE FRAMES HAVE IT, any sport's (see ScoreBug): what on
// it changes from moment to moment - the game clock, the clock after it,
// each side's fouls, which side has the ball, football's down and distance
// - written straight into it by the frame loop, not rendered, and only
// what has changed since it last was.

export type BugNow = {
	clock: string;
	// The clock after the game clock (the shot clock, the play clock): ""
	// none, off the bug. `low`: in its last seconds - in red.
	shot: string;
	low: boolean;
	// Each side's fouls this period, or BONUS, in the bug's order (visitors
	// first) - where the sport keeps them. And as numbers, and whether each
	// side is in the bonus, for a league's own bug to put its own way.
	fouls?: [string, string];
	counts?: [number, number];
	bonus?: [boolean, boolean];
	// The side with the ball, in the bug's order (-1 neither).
	ball: number;
	// The down and distance, where the sport has them - and `flag`, a flag
	// out on the play: FLAG in their place.
	down?: string;
	flag?: boolean;
};

// The bug's elements the frames write into, one set a game for good: the
// clocks, each side's fouls and the ball's mark ([visitors, home]) - and,
// `down`, the down and distance.
export const useBugRefs = (down = false): BugRefs =>
	useState((): BugRefs => ({
		clock: createRef(),
		shot: createRef(),
		fouls: [createRef(), createRef()],
		ball: [createRef(), createRef()],
		bonus: [createRef(), createRef()],
		...(down ? { down: createRef() } : {}),
	}))[0];

// What was last written, and into which of the bug's elements: a new bug in
// its place (the league's own put up, say) is written afresh.
export type BugShown = {
	into: (HTMLElement | null)[];
	clock: string;
	shot: string;
	// The clock after the game clock's own color, kept while it shows red.
	shotInk: string | undefined;
	fouls: [string, string];
	bonus: [boolean, boolean];
	ball: number;
	down: string;
};

export const bugShown = (): BugShown => ({
	into: [],
	clock: "",
	shot: "",
	shotInk: undefined,
	fouls: ["", ""],
	bonus: [false, false],
	ball: -1,
	down: "",
});

// A league's own bug's way with the clocks (data-colon): under a minute,
// ":09.2" - and the shot clock ":24".
const withColon = (el: HTMLElement, text: string) => {
	if (el.dataset.colon === undefined) {
		return text;
	}
	const m = /^(\d+)(\.\d)?$/.exec(text);
	if (!m || (m[2] !== undefined && el.dataset.colon === "shot")) {
		return text;
	}
	return `:${m[1]!.padStart(2, "0")}${m[2] ?? ""}`;
};

// A side's fouls as a league's own bug puts them (data-fmt, "FOULS: {n}";
// data-limit, once the other side is in the bonus) - or as they come.
const foulsText = (
	el: HTMLElement,
	now: BugNow,
	i: 0 | 1,
): string | undefined => {
	const { fmt, limit } = el.dataset;
	if ((fmt === undefined && limit === undefined) || !now.counts) {
		return now.fouls?.[i];
	}
	if (limit !== undefined && now.bonus?.[i === 0 ? 1 : 0]) {
		return limit;
	}
	return (fmt ?? "FOULS {n}").replace("{n}", String(now.counts[i]));
};

const BONUS_INK = "#f2c14e";
const FLAG_BG = "#f4cb16";
const FLAG_INK = "#141414";
const FLAG_PAD = "0.1em 0.45em";

export const writeBug = (refs: BugRefs, shown: BugShown, now: BugNow) => {
	const into = [
		refs.clock.current,
		refs.shot.current,
		refs.fouls[0].current,
		refs.fouls[1].current,
		refs.ball[0].current,
		refs.ball[1].current,
		refs.down?.current ?? null,
		refs.bonus?.[0].current ?? null,
		refs.bonus?.[1].current ?? null,
	];
	if (into.some((el, i) => el !== shown.into[i])) {
		Object.assign(shown, bugShown(), { into });
	}
	const clockEl = refs.clock.current;
	if (now.clock !== shown.clock && clockEl) {
		shown.clock = now.clock;
		clockEl.textContent = withColon(clockEl, now.clock);
	}
	const shotEl = refs.shot.current;
	if (now.shot !== shown.shot && shotEl) {
		shown.shot = now.shot;
		shotEl.textContent = withColon(shotEl, now.shot);
		shotEl.style.display = now.shot ? "flex" : "none";
		// (Red in its last seconds - and back to its own color after. A
		// league's own bug says its own way: data-low, data-low-background.)
		shown.shotInk ??= shotEl.style.color;
		shotEl.style.color = now.low
			? (shotEl.dataset.low ?? SHOT_LOW_INK)
			: shown.shotInk;
		if (shotEl.dataset.lowBackground !== undefined) {
			shotEl.style.background = now.low
				? shotEl.dataset.lowBackground
				: (shotEl.dataset.background ?? "");
		}
	}
	if (now.fouls) {
		([0, 1] as const).forEach((i) => {
			const el = refs.fouls[i].current;
			const text = el ? foulsText(el, now, i) : undefined;
			if (el && text !== undefined && text !== shown.fouls[i]) {
				shown.fouls[i] = text;
				el.textContent = text;
				el.style.color =
					text === "BONUS" && el.dataset.fmt === undefined ? BONUS_INK : "";
			}
		});
	}
	([0, 1] as const).forEach((i) => {
		const el = refs.bonus?.[i].current;
		const on = now.bonus?.[i] === true;
		if (el && on !== shown.bonus[i]) {
			shown.bonus[i] = on;
			el.style.visibility = on ? "visible" : "hidden";
			// (Its fouls moved over to make room, where the bug says.)
			const fouls = refs.fouls[i].current;
			const shift = fouls?.dataset.bonusShift;
			if (fouls && shift !== undefined) {
				fouls.style.transform = on ? `translateX(${shift})` : "";
			}
		}
	});
	if (now.ball !== shown.ball) {
		shown.ball = now.ball;
		refs.ball.forEach((ref, i) => {
			if (ref.current) {
				ref.current.style.visibility = i === now.ball ? "visible" : "hidden";
			}
		});
	}
	const downEl = refs.down?.current;
	if (now.down !== undefined && downEl) {
		const down = now.flag ? "FLAG" : now.down;
		if (down !== shown.down) {
			shown.down = down;
			downEl.textContent = down;
			// (A league's own bug's bare text given a box of its own.)
			const st = downEl.style;
			downEl.dataset.was ??= [st.background, st.color, st.padding].join("|");
			const [bg, ink, pad] = downEl.dataset.was.split("|");
			const flag = now.flag === true;
			st.background = flag ? FLAG_BG : (bg ?? "");
			st.color = flag ? FLAG_INK : (ink ?? "");
			st.fontWeight = flag ? "700" : "";
			st.padding = flag && !pad ? FLAG_PAD : (pad ?? "");
			st.borderRadius = flag && !pad ? "0.15em" : "";
		}
	}
};

// The game clock alone, as the page keeps it, while there is no picture yet
// to write it.
export const writePageClock = (refs: BugRefs, text: string) => {
	const el = refs.clock.current;
	if (el) {
		el.textContent = text;
	}
};
