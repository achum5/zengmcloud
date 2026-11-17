import type { AdvanceClaimDoc } from "./advanceClaimPolicy.ts";

// A STEP ANOTHER DEVICE IS RUNNING. A ready-up step (a pick, a phase change,
// a free agency day) is run by whichever device wins its claim, which can be a
// league mate's phone rather than the one in charge of simming - and only the
// device in charge can stamp the room busy. So the claim itself is the busy
// signal: while another device holds one it hasn't completed, within its
// lease, league mates' edits wait, and the Play menu on the device in charge
// doesn't run the same step alongside it. Fed by draftReady's listener on the
// claim document.

let claim: AdvanceClaimDoc | undefined;
let leaseMs = 0;

export const setStepClaim = (doc: AdvanceClaimDoc | undefined, lease: number) => {
	claim = doc;
	leaseMs = lease;
};

export const clearStepClaim = () => {
	claim = undefined;
};

// The step another device claimed and is still running, if one is.
export const foreignStepClaim = (
	clientId: string,
	now = Date.now(),
): AdvanceClaimDoc | undefined => {
	if (
		claim === undefined ||
		claim.completed ||
		claim.holderId === clientId ||
		now - claim.at > leaseMs
	) {
		return undefined;
	}
	return claim;
};
