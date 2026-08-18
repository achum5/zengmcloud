// EXACTLY ONCE PER SCHEDULE DAY, ENFORCED BY DATA.
//
// The end-of-day block in play.ts has per-day semantics: injury and
// trade-cooldown countdowns, the tragic-death roll, free agent demand decay
// and signings, AI trades. In a shared league one schedule day can be simmed
// in SLICES on different devices - someone live-sims their own game, the day
// sim runs the rest - and each slice decides "the day is over" from its own
// local schedule. The claim fence and the caught-up guard make running that
// block twice for one day hard to arrange; a real league then produced a
// player whose injury countdown skipped a day with no off day to explain it,
// and who suited up a game early - which is exactly what a double countdown
// looks like. Hard is not impossible.
//
// So instead of trusting protocol reasoning, the day's identity is stamped
// into a replicated game attribute in the same write as the countdown, and a
// stamped day is never counted again - on any device, in any ordering, from
// any future feature that slices days in a new way. Solo leagues are
// untouched: schedule days only ever advance there, so the stamp never
// matches.
//
// The comparison is equality, not ordering, on purpose: the fence already
// refuses old days, and an ordering rule would need to know how day numbers
// behave across phases and seasons. Equality only needs "the same day must
// not be counted twice", which is the entire requirement.

export type CountdownDay = {
	season: number;
	phase: number;
	day: number;
};

export const dayAlreadyCounted = (
	lastCounted: CountdownDay | undefined,
	current: CountdownDay | undefined,
): boolean =>
	lastCounted !== undefined &&
	current !== undefined &&
	lastCounted.season === current.season &&
	lastCounted.phase === current.phase &&
	lastCounted.day === current.day;
