// When to tell the user their device is missing shared league data.
//
// The condition itself is common and usually harmless: the device notices a gap
// and there is no usable checkpoint to heal from YET. The heal needs whoever is
// in charge of simming to have the app open for a couple of minutes, and then it
// happens on its own. Warning on first sight meant a persistent red error on
// every launch during an ordinary, self-resolving gap - including right after a
// reimport, when the gap is expected.
//
// So the warning is gated on the gap OUTLASTING the wait the message describes,
// measured with a durable timestamp so it survives reloads, and shown once per
// session rather than once per heal attempt.

export const MISSING_DATA_WARN_AFTER_MS = 30 * 60 * 1000;

export type MissingDataWarningDecision = {
	// What to persist as the "missing since" stamp.
	since: number;
	// Whether to show the user the warning right now.
	warn: boolean;
};

export const decideMissingDataWarning = ({
	since,
	alreadyWarned,
	now,
	warnAfterMs = MISSING_DATA_WARN_AFTER_MS,
}: {
	// The stored stamp, or undefined if this is the first sighting.
	since: number | undefined;
	// Whether this session has already shown the warning.
	alreadyWarned: boolean;
	now: number;
	warnAfterMs?: number;
}): MissingDataWarningDecision => {
	// First sighting, or a stamp from the future because the system clock moved
	// backwards - which would otherwise suppress the warning indefinitely. Either
	// way, start the clock now and stay quiet.
	if (since === undefined || since > now) {
		return { since: now, warn: false };
	}

	return {
		since,
		warn: !alreadyWarned && now - since >= warnAfterMs,
	};
};
