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

// The attached screenshot is the whole description of the face.
//
// This used to also dump the raw faces.js FaceConfig as JSON, on the theory
// that it would settle anything the screenshot left ambiguous. It settles
// nothing: an image model cannot run the renderer, so a wall of numbers like
// eyeLine and fatness is not something it can turn into a face - it just
// crowds out the instructions that do work. The picture is the spec.
//
// And it is a spec for WHO HE IS, not for what his face is doing. It used to
// say "match it exactly", which got taken literally: the headshot's blank
// stare and slack open mouth were copied straight onto a player mid-drive,
// which looks wrong in a way nothing else on the card can rescue. Identity is
// fixed; expression and head angle belong to the action.
const faceBlock = (subject: CardSubject): string => {
	if (!subject.face) {
		return "";
	}
	return `\n\n## The player's face\n\nA screenshot of this player is attached. It is a HEADSHOT - a straight-on, neutral, unposed reference, the way a roster photo is.

Use it for WHO HE IS, and match these exactly: skin tone, face shape, hair (style, colour, hairline), facial hair, eyebrows, eye shape and colour, and any accessories he is wearing.

Do NOT copy the expression, the mouth, or the head angle from it. Those belong to the action, not to the reference - a blank stare and a slack open mouth on a player driving to the rim is the single thing that most makes one of these look wrong. Give him the face the moment calls for: eyes on the play, jaw set, mouth closed or open with effort, head turned wherever he is actually looking. He should be recognisably the same person and visibly in the middle of playing.

He is a faces.js cartoon avatar, so render him in that same flat vector cartoon style: clean shapes, flat fills, no photorealistic skin or hair texture.`;
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

// The physical card, stated the SAME WAY in both prompts and given its own
// section in each.
//
// The two prompts are pasted into an image model separately, so nothing but
// this keeps them the same size - and a front and a back that are different
// shapes are not a card. It used to be one clause buried in the first sentence,
// which lost every argument it had with the back description: eight of the sets
// in the catalogue have a back that reads horizontally, the model took that as
// the shape of the card rather than the shape of the layout, and returned a
// landscape back to go with a portrait front.
//
// Size is given three ways - inches, ratio, pixels - because different models
// listen to different ones.
//
// Most cards are 2.5 x 3.5. The ones that are not (the 1969-71 "tall boys") are
// defined by not being, so their shape has to survive into the prompt too.
const shapeBlock = (setId: string): string => {
	const proportions = cardSetsById.get(setId)?.proportions;
	return `## The card's size and shape

${
	proportions ??
	"2.5 x 3.5 inches, PORTRAIT - taller than it is wide, a 5:7 ratio. Render the image at 1024 x 1434 pixels."
}

The front and the back of this card are generated as two separate images and MUST come out at identical dimensions. Output the full card edge to edge at exactly these proportions: nothing cropped off, no hand holding it, no background scene around it, no packaging, no drop shadow, no angled mockup.`;
};

// The back's own note. Eight sets in the catalogue describe a back that reads
// horizontally, and that is true of the real cards - but it describes the
// LAYOUT printed on the card, not the card, which is the same 2.5 x 3.5
// portrait rectangle as the front. You turn a real one sideways to read it.
const BACK_ORIENTATION = `If the back design described below reads horizontally, that is the layout, not the card. The card is still the portrait shape given above; the horizontal layout is printed rotated a quarter turn onto it, exactly as the real card is, which is why you turn one sideways to read the back. Do not output a landscape image - a back that is a different shape from the front is wrong no matter how good it looks.`;

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

	return `Generate the FRONT of a single basketball trading card, as one image. Output only the card, nothing else.

${shapeBlock(setId)}

${FICTION}

${designBlock(setId)}${variantBlock}

## The season on the card

The design above is a ${set.label} design. That is the LOOK ONLY. This card depicts the **${subject.season}** season, and **${subject.season}** is the year that must be printed on the card wherever the design puts a year. Do not change the year to match the design's own era. A ${set.label} design showing ${subject.season} is exactly what is wanted.

## The photograph

A CANDID shot, not a portrait. This is a professional sports photographer sitting courtside at a live NBA game, shooting this player in the middle of PLAYING BASKETBALL. He is doing something on the court - driving, rising for a jumper, finishing at the rim, defending, coming down with a rebound, running the floor - and he does not know the camera is there. No posing, no looking into the lens, no smiling at the camera, no arms folded, no ball resting on the hip, no studio backdrop.

Shot from the sideline or the baseline, at court level, with a long lens: the player caught mid-action and filling the frame, the crowd and the arena falling out of focus behind him. Natural arena lighting.${
		set.photography?.toLowerCase().includes("posed")
			? " EXCEPTION for this particular set: its Photography note above calls for a posed shot, and a few early designs really were made that way - follow the set."
			: ""
	}

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

	return `Generate the BACK of a single basketball trading card, as one image. Output only the card, nothing else.

This is the back of a ${set.label}${variant && variant.id !== "base" ? ` ${variant.label}` : ""} card for ${subject.name}, depicting the ${subject.season} season. It has to pair with a front generated separately from the same design.

${shapeBlock(setId)}

${BACK_ORIENTATION}

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
