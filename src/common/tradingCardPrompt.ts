import type { FaceConfig } from "facesjs";
import { cardErasById, cardSetsById } from "./tradingCards.ts";

// Builds the two prompts - card front and card back - that get pasted into an
// image model. Pure, so the whole thing is testable without a league.
//
// The hard rule threaded through both: the SET is a look and the SEASON is what
// the card depicts. A 1985-86 Star Company design showing a 2026 season is
// correct and deliberate, and 2026 is the year that goes on the card. Models
// want very badly to "fix" that, so it is stated more than once.

export type CardStatRow = {
	season: number;
	abbrev: string;
	gp: number;
	gs: number;
	min: number;
	pts: number;
	trb: number;
	ast: number;
	stl: number;
	blk: number;
	fgp: number;
	tpp: number;
	ftp: number;
};

export type CardSubject = {
	name: string;
	pos: string;
	jerseyNumber?: string;
	heightIn: number;
	weightLbs: number;
	age?: number;
	bornYear?: number;
	bornLoc?: string;
	college?: string;
	draft?: { year: number; round: number; pick: number; teamName?: string };
	// The franchise the card depicts him on, in the depicted season.
	teamName: string;
	teamColors?: string[];
	season: number;
	face?: FaceConfig;
	awards: string[];
	// Season by season, through the depicted season only. A card printed in a
	// season cannot know what came after it.
	stats: CardStatRow[];
	career?: Omit<CardStatRow, "season" | "abbrev">;
};

const height = (inches: number): string =>
	inches > 0 ? `${Math.floor(inches / 12)}'${inches % 12}"` : "";

// The league is fictional and the model must not reach for anything it knows
// about a real player who happens to share a name. The one exception is the
// uniform, which is the whole point of picking a season.
const FICTION = `THIS IS A FICTIONAL LEAGUE. The player and team names may coincide with real people and real franchises, but they are NOT them and share no history. Do not use any real-world knowledge about this player - no real face, no real physical likeness, no real team history, no real awards, no real signature moves, no real jersey number. Everything about the person comes from the data below and nothing else.

THE ONE EXCEPTION is the uniform. For the jersey, DO use your real-world memory of what that franchise actually wore in that season, as described below.`;

const faceBlock = (subject: CardSubject): string => {
	if (!subject.face) {
		return "";
	}
	return `\n\n## The player's face\n\nA screenshot of this player's face is attached - match it exactly. It is a faces.js cartoon avatar, and the card must render him in that same flat vector cartoon style: clean shapes, flat fills, no photorealistic skin or hair texture.

This is the exact faces.js FaceConfig behind that screenshot. Where the screenshot is ambiguous, read the answer off this instead:

\`\`\`json
${JSON.stringify(subject.face, null, 1)}
\`\`\``;
};

const statTable = (subject: CardSubject): string => {
	if (subject.stats.length === 0) {
		return "No professional statistics yet - this is a pre-debut card, so the back carries the biography and scouting information only, with no stat grid.";
	}

	const header =
		"SEASON  TEAM   GP   GS   MIN   PTS   TRB   AST   STL   BLK   FG%   3P%   FT%";
	const pad = (value: string | number, width: number) =>
		String(value).padStart(width);
	const row = (
		r:
			| CardStatRow
			| (Omit<CardStatRow, "season" | "abbrev"> & {
					season: string;
					abbrev: string;
			  }),
	) =>
		[
			pad(r.season, 6),
			pad(r.abbrev, 5),
			pad(r.gp, 4),
			pad(r.gs, 4),
			pad(r.min.toFixed(1), 5),
			pad(r.pts.toFixed(1), 5),
			pad(r.trb.toFixed(1), 5),
			pad(r.ast.toFixed(1), 5),
			pad(r.stl.toFixed(1), 5),
			pad(r.blk.toFixed(1), 5),
			pad(r.fgp.toFixed(1), 5),
			pad(r.tpp.toFixed(1), 5),
			pad(r.ftp.toFixed(1), 5),
		].join(" ");

	const lines = [header, ...subject.stats.map((r) => row(r))];
	if (subject.career) {
		lines.push(row({ ...subject.career, season: "CAREER", abbrev: "" }));
	}
	return lines.join("\n");
};

const bioLines = (subject: CardSubject): string[] => {
	const lines: string[] = [`Name: ${subject.name}`, `Position: ${subject.pos}`];
	if (subject.jerseyNumber !== undefined && subject.jerseyNumber !== "") {
		lines.push(`Jersey number: ${subject.jerseyNumber}`);
	}
	lines.push(`Team: ${subject.teamName}`);
	const hw = [height(subject.heightIn), `${subject.weightLbs} lbs`]
		.filter(Boolean)
		.join(", ");
	if (hw) {
		lines.push(`Height/weight: ${hw}`);
	}
	if (subject.age !== undefined) {
		lines.push(`Age in the depicted season: ${subject.age}`);
	}
	if (subject.bornYear !== undefined) {
		lines.push(
			`Born: ${subject.bornYear}${subject.bornLoc ? ` in ${subject.bornLoc}` : ""}`,
		);
	}
	if (subject.college) {
		lines.push(`College: ${subject.college}`);
	}
	if (subject.draft) {
		const { year, round, pick, teamName } = subject.draft;
		lines.push(
			round > 0
				? `Drafted: ${year}, round ${round}, pick ${pick}${teamName ? ` by the ${teamName}` : ""}`
				: `Drafted: undrafted (${year})`,
		);
	}
	return lines;
};

const designBlock = (setId: string): string => {
	const set = cardSetsById.get(setId);
	if (!set) {
		return "";
	}
	const era = cardErasById.get(set.era);

	const fields: [string, string | undefined][] = [
		["Stock and finish", set.stock],
		["Border", set.border],
		["Photography", set.photography],
		["Background", set.background],
		["Typography", set.typography],
		["Logos and layout", set.layout],
		["Era markers", set.markers],
	];

	const lines = fields
		.filter((entry): entry is [string, string] => entry[1] !== undefined)
		.map(([label, value]) => `- ${label}: ${value}`);

	// A card of this age rendered factory-perfect reads as a reproduction of
	// itself. The era's wear is as much a period marker as the design is.
	if (era) {
		lines.push(`- Condition and age: ${era.wear}`);
	}

	return `## The card design: ${set.label}

Era design language - ${era?.label ?? ""}: ${era?.language ?? ""}

${lines.join("\n")}`;
};

// Most cards are 2.5 x 3.5. The ones that are not - the 1969-71 "tall boys" -
// are defined by not being, so the shape has to survive into the prompt.
const shapeOf = (setId: string): string =>
	cardSetsById.get(setId)?.proportions ??
	"standard trading card proportions (2.5 x 3.5, portrait)";

const jerseyBlock = (subject: CardSubject): string =>
	`## The uniform

He is wearing the ${subject.teamName} uniform as that franchise actually wore it in ${subject.season} - that real-world design, its real colors, its real wordmark and striping. If you do not know that franchise's ${subject.season} uniform, use the most recent real design you do know for them. This is the only place real-world knowledge belongs on this card.${
		subject.jerseyNumber
			? ` The number on the jersey is ${subject.jerseyNumber}.`
			: ""
	}`;

export const buildCardFrontPrompt = (
	setId: string,
	variantId: string,
	subject: CardSubject,
): string => {
	const set = cardSetsById.get(setId);
	if (!set) {
		return "";
	}
	const variant = set.variants.find((v) => v.id === variantId);

	const variantBlock =
		variant && variant.treatment
			? `\n\n## This particular card: ${variant.label}\n\n${variant.treatment}`
			: "";

	return `Generate the FRONT of a single basketball trading card, as one image. Output only the card - the full card, edge to edge, nothing cropped off, ${shapeOf(setId)}, no hand holding it, no background scene around it, no packaging.

${FICTION}

${designBlock(setId)}${variantBlock}

## The season on the card

The design above is a ${set.label} design. That is the LOOK ONLY. This card depicts the **${subject.season}** season, and **${subject.season}** is the year that must be printed on the card wherever the design puts a year. Do not change the year to match the design's own era. A ${set.label} design showing ${subject.season} is exactly what is wanted.

## The photograph

The player image is shot as a real press photographer would shoot it: from the sideline or the baseline, at court level, during an actual game, with a long lens - the player mid-action, the crowd and the arena falling out of focus behind him. Natural arena lighting.

But it is RENDERED in flat faces.js cartoon style, not photorealism: the player, and anyone visible behind him, are drawn as clean flat vector shapes with solid fills. Think of a cartoon illustration composed exactly the way a real courtside photograph would be composed.

${jerseyBlock(subject)}

## The player

${bioLines(subject).join("\n")}${
		subject.awards.length > 0
			? `\nHonors through ${subject.season}: ${subject.awards.join(", ")}`
			: ""
	}${faceBlock(subject)}

## Text on the card

The card front shows his name (${subject.name}), his position (${subject.pos}), and his team (${subject.teamName}), laid out the way this set lays them out. Spell the name exactly as written. Do not invent extra text, taglines, or logos that the design description above does not call for.`;
};

export const buildCardBackPrompt = (
	setId: string,
	variantId: string,
	subject: CardSubject,
): string => {
	const set = cardSetsById.get(setId);
	if (!set) {
		return "";
	}
	const variant = set.variants.find((v) => v.id === variantId);

	return `Generate the BACK of a single basketball trading card, as one image. Output only the card - the full card, edge to edge, ${shapeOf(setId)}, nothing around it.

This is the back of a ${set.label}${variant && variant.id !== "base" ? ` ${variant.label}` : ""} card for ${subject.name}, depicting the ${subject.season} season.

${FICTION}

## The back design

${set.back}

Era design language: ${cardErasById.get(set.era)?.language ?? ""}

Lay it out the way a real card back of this era is laid out - the card number in its usual corner, the biography block, and the statistics grid filling most of the card. Keep the back's palette consistent with the front's.

## The season on the card

**${subject.season}** is the season this card depicts and the year printed on it, regardless of what era the ${set.label} design comes from.

## Biography block

${bioLines(subject).join("\n")}${
		subject.awards.length > 0
			? `\nHonors through ${subject.season}: ${subject.awards.join(", ")}`
			: ""
	}

## Statistics grid

Reproduce these numbers EXACTLY as given - every row, every column, no rounding changes, no invented seasons, no extrapolation past ${subject.season}. These are per-game averages. This is the single most important part of the card; a stat grid with made-up numbers ruins it.

\`\`\`
${statTable(subject)}
\`\`\`

If the era's design would not fit this many columns, drop columns from the right rather than inventing a compressed layout, and never drop a season row.`;
};
