import { useLocal } from "./local.ts";

// Shared check for "is this device currently barred from advancing the league".
// Used to disable sim / advance buttons while another device holds the wheel, or
// while we're reconnecting/offline. The worker enforces the same rule - this is
// purely for UX (disabled buttons + a tooltip). Draft PICKS are exempt and must
// NOT use this (any user drafts their own team on the clock).
export const useWheelLocked = (): {
	locked: boolean;
	reason: string | undefined;
} => {
	const { mpSyncActive, mpSyncIsHost, mpSyncHostName, mpSyncReconnecting } =
		useLocal([
			"mpSyncActive",
			"mpSyncIsHost",
			"mpSyncHostName",
			"mpSyncReconnecting",
		]);

	const locked = mpSyncReconnecting || (mpSyncActive && !mpSyncIsHost);
	const reason = !locked
		? undefined
		: mpSyncReconnecting
			? "Reconnecting to the league…"
			: `${mpSyncHostName ?? "Another device"} is simming`;

	return { locked, reason };
};
