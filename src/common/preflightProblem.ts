import type { PreflightResult, PreflightStep } from "./preflight.ts";

// TURNING FIREBASE'S ERROR CODES INTO SOMETHING A PERSON CAN ACT ON.
//
// Four things have to be true for a bring-your-own project to work, and a user
// does all four by hand in a console: the project exists, it has a Firestore
// database, anonymous sign-in is on, and the security rules are published. Miss
// one and the SDK says "auth/operation-not-allowed" or "permission-denied",
// which tells someone setting this up for the first time nothing at all.
//
// Kept pure, and separate from the code that does the network calls, for one
// reason: this table is the part that has to be RIGHT, and a live Firebase
// project is exactly the thing a test cannot have. Every branch below is
// exercised in preflightProblem.test.ts against the error shapes the SDK really
// throws.

type Problem = {
	problem: { title: string; fix: string };
	link?: PreflightResult["link"];
};

// Firebase puts the useful part in `code` ("auth/operation-not-allowed",
// "permission-denied") and the prose in `message`. Match on both: the database
// -missing case is only distinguishable from its message.
export const errorText = (error: unknown): string => {
	const parts: string[] = [];
	const code = (error as { code?: unknown })?.code;
	if (typeof code === "string") {
		parts.push(code);
	}
	const message = (error as { message?: unknown })?.message;
	if (typeof message === "string") {
		parts.push(message);
	}
	if (parts.length === 0) {
		parts.push(String(error));
	}
	return parts.join(" ").toLowerCase();
};

export const classifyPreflightError = (
	step: PreflightStep,
	error: unknown,
): Problem => {
	const text = errorText(error);

	// A dead connection looks like every other failure until you read the code,
	// and telling someone to turn on a setting that is already on wastes the
	// whole setup. Checked first, at every step.
	if (text.includes("network") || text.includes("unavailable")) {
		return {
			problem: {
				title: "Couldn't reach Firebase",
				fix: "Check your connection and try again.",
			},
		};
	}

	if (step === "auth") {
		if (text.includes("api-key") || text.includes("invalid-api-key")) {
			return {
				problem: {
					title: "That API key isn't valid for this project",
					fix: "Re-copy the config from Project settings → General → Your apps.",
				},
			};
		}
		// operation-not-allowed (the provider is off) and
		// configuration-not-found (Authentication was never set up) are the same
		// fix: turn on Anonymous.
		return {
			problem: {
				title: "Anonymous sign-in is off",
				fix: "Turn on the Anonymous provider, then check again.",
			},
			link: { label: "Open Authentication", url: "auth" },
		};
	}

	// Firestore reports a project with no database as not-found, with the prose
	// naming the database. Distinct from "your rules said no", and a completely
	// different fix.
	if (
		text.includes("does not exist") ||
		(text.includes("not-found") && text.includes("database"))
	) {
		return {
			problem: {
				title: "This project has no Firestore database yet",
				fix: "Create one - production mode is fine, you'll publish rules next.",
			},
			link: { label: "Open Firestore", url: "firestore" },
		};
	}

	if (step === "read") {
		return {
			problem: {
				title: "Your rules allow writing but not reading",
				fix: "Publish the rules below exactly as they are - league-mates have to read what you write.",
			},
			link: { label: "Open Rules", url: "rules", rules: true },
		};
	}

	return {
		problem: {
			title: "Your security rules don't allow this yet",
			fix: "Copy the rules below into the Rules tab and publish them.",
		},
		link: { label: "Open Rules", url: "rules", rules: true },
	};
};
