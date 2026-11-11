import type { SigningHow } from "./types.ts";

// REALISTIC FRONT OFFICES' CONTRACT RULES, IN WORDS (CBA-lite, free agency in
// stages; docs/free-agency-overhaul.md). Short labels for tables, each with
// its spelled-out title for a tooltip, and the phrases a signing's event text
// ends with.

export type SigningPath = NonNullable<SigningHow["path"]>;

export const PATHS: Record<SigningPath, { short: string; long: string }> = {
	room: { short: "Room", long: "Cap room" },
	bird: { short: "Bird", long: "Bird rights" },
	midLevel: { short: "MLE", long: "Mid-level exception" },
	taxpayerMidLevel: { short: "TMLE", long: "Taxpayer mid-level exception" },
	minimum: { short: "Min", long: "Minimum contract" },
};

// Every abbreviation the pages use, spelled out.
export const ABBREVS = {
	PO: "Player option",
	TO: "Team option",
	UFA: "Unrestricted free agent",
	RFA: "Restricted free agent",
	MLE: "Mid-level exception",
	TMLE: "Taxpayer mid-level exception",
	Bird: "Bird rights",
	Room: "Cap room",
	Min: "Minimum contract",
	Ext: "Extension",
	"Prove-it": "Prove-it deal: one season, no Bird rights",
} as const;

export type CapStatusKey = "under" | "over" | "apron" | "hardCap";

export const CAP_STATUS: Record<CapStatusKey, { label: string }> = {
	under: { label: "Under the cap" },
	over: { label: "Over the cap" },
	// Over the luxury tax line (the key is the old apron's).
	apron: { label: "Over the tax" },
	hardCap: { label: "At the hard cap" },
};

// The phrases an event's text ends with: "(mid-level, team option)".
export const howPhrases = (how: SigningHow | undefined) => {
	if (!how) {
		return [];
	}
	const out: string[] = [];
	if (how.offerSheet === "matched") {
		out.push("matched offer sheet");
	} else if (how.offerSheet === "notMatched") {
		out.push("offer sheet, not matched");
	}
	if (how.path === "room") {
		out.push("cap room");
	} else if (how.path === "bird") {
		out.push("Bird rights");
	} else if (how.path === "midLevel") {
		out.push("mid-level");
	} else if (how.path === "taxpayerMidLevel") {
		out.push("taxpayer mid-level");
	} else if (how.path === "minimum") {
		out.push("minimum");
	}
	if (how.option) {
		out.push(`${how.option} option`);
	}
	if (how.proveIt) {
		out.push("prove-it deal");
	}
	if (how.chance !== undefined) {
		out.push(`${chancePercent(how.chance)} odds`);
	}
	return out;
};

// "64%", "<1%".
export const chancePercent = (chance: number) =>
	`${chance > 0 && chance < 0.01 ? "<1" : Math.round(chance * 100)}%`;

// One short method for a table: "MLE", "Bird", "Ext", "RFA match".
export const howMethod = (how: SigningHow | undefined) => {
	if (!how) {
		return undefined;
	}
	if (how.offerSheet === "matched") {
		return { short: "RFA match", long: "Offer sheet matched" };
	}
	if (how.path) {
		return PATHS[how.path];
	}
	return undefined;
};

// Marks for a contract's seasons: the option on its last one, a prove-it deal.
export const optionAbbrev = (option: "player" | "team") =>
	option === "player" ? "PO" : "TO";
