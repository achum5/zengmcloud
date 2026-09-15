// The result of checking a bring-your-own Firebase project, shared by the
// worker that runs the checks and the UI that draws them as a checklist.

// In dependency order: each step is only reachable if the one before it passed.
export type PreflightStep = "project" | "auth" | "write" | "read" | "cleanup";

export const PREFLIGHT_STEP_LABELS: Record<PreflightStep, string> = {
	project: "Config reaches your project",
	auth: "Anonymous sign-in is on",
	write: "Your league can write",
	read: "Your league-mates can read",
	cleanup: "Cleanup works",
};

export type PreflightResult = {
	ok: boolean;
	steps: { step: PreflightStep; ok: boolean }[];
	// Set only on a failure: what is wrong, in the user's terms, and what to do.
	problem?: { title: string; fix: string };
	// The console page that fixes it, plus whether the rules are worth offering
	// to copy alongside.
	link?: { label: string; url: "auth" | "firestore" | "rules"; rules?: true };
	// Firebase's own words, kept for the rare case none of the above fits.
	detail?: string;
};
