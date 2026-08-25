// Should the auto-play countdown be on screen at all?
//
// Its own module rather than a helper inside the component, because the
// component imports the scheduler, which imports the worker bridge, which
// cannot be constructed in a test. The same reason autoPlayDeferral.ts is
// separate from the scheduler that uses it.
export const autoPlayCountdownVisible = ({
	enabled,
	nextRunAt,
	gated,
}: {
	enabled: boolean;
	nextRunAt: number | undefined;
	// The room is being asked to ready up, so the schedule is not what happens
	// next - see the note in AutoPlayCountdown.tsx.
	gated: boolean;
}): boolean => enabled && nextRunAt !== undefined && !gated;
