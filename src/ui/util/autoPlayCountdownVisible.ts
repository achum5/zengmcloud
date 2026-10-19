// Should the auto-play countdown be on screen at all?
//
// Its own module rather than a helper inside the component, because the
// component imports the scheduler, which imports the worker bridge, which
// cannot be constructed in a test. The same reason autoPlayDeferral.ts is
// separate from the scheduler that uses it.

// How long the clock may sit on 0:00 (the sim it counted down to is running)
// before it is taken for a sim that is not coming - the schedule stopped at the
// end of the regular season under a held snapshot, or the "off" never landed.
export const OVERDUE_GRACE_MS = 60_000;

export const autoPlayCountdownVisible = ({
	enabled,
	nextRunAt,
	gated,
	now,
}: {
	enabled: boolean;
	nextRunAt: number | undefined;
	// The room is being asked to ready up, so the schedule is not what happens
	// next - see the note in AutoPlayCountdown.tsx.
	gated: boolean;
	now: number;
}): boolean =>
	enabled &&
	nextRunAt !== undefined &&
	!gated &&
	now < nextRunAt + OVERDUE_GRACE_MS;
