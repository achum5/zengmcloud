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

// An achievement card is an ordinary card with up to three things swapped in:
// a different photograph (draft night, a college shot, a title celebration),
// a different outfit to go with it, and a flag naming what it commemorates.
// Everything else - the set, the shape, the fiction rule, the stat grid -
// stays exactly the ordinary card's, which is why this is an override and not
// a second prompt builder.
export type CardPromptOverride = {
	// Replaces the candid in-game action on the front.
	photograph?: string;
	// Replaces the uniform section on the front, for scenes where he is not in
	// his pro game uniform (a draft-night suit, a college jersey).
	uniform?: string;
	// What the card commemorates, e.g. "Finals MVP, 2027". Adds a flag to the
	// front and a line to the back.
	achievement?: string;
	// Build the version that hosted image models will actually accept: no real
	// person, no reproduced team or brand marks, no named league. See
	// FICTION_SAFE.
	safeMode?: boolean;
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

// SAFE MODE.
//
// Hosted image models refuse prompts on sight for two things this card asks for
// by default: depicting a real, identifiable person, and reproducing a real
// organisation's logos and wordmarks. The prompt above walks straight into both
// - it says the names "may coincide with real people", and then explicitly asks
// for a franchise's real wordmark - and in a league running real player names
// that reads as "draw me this famous athlete in his actual team's kit".
//
// Safe mode makes the same card without either. The player is stated as a
// fictional cartoon character with the attached avatar as his whole reference,
// the uniform is invented from the team's colours rather than reproduced, and
// the league is "professional basketball" rather than a named one. The design,
// the era, the stat grid and everything else that makes the card what it is are
// untouched.
const FICTION_SAFE = `EVERYONE ON THIS CARD IS A FICTIONAL CHARACTER from a basketball video game. Nobody here is a real person, and nothing here belongs to a real organisation.

Build the player only from the written description and the attached cartoon avatar below. Do not base him on anyone real, and do not reproduce any real team's or real company's logo, wordmark, or uniform - every mark on this card is invented from the descriptions given.

Where the design notes below name a card company or an era, they are describing a VISUAL STYLE and nothing else. Do not print any real company's name or logo anywhere on the card.`;

// The safe uniform: the team's colours, and a design made up to fit them.
const jerseyBlockSafe = (subject: CardSubject): string => {
	const colors =
		subject.teamColors && subject.teamColors.length > 0
			? ` Their colours are ${subject.teamColors.join(", ")}.`
			: "";
	return `## The uniform

He wears the ${subject.teamName} uniform.${colors} INVENT the design rather than copying one: a clean, classic basketball uniform in those colours, with the team name in simple athletic lettering across the chest and plain contrasting trim. Do not reproduce any real team's logo, wordmark, or uniform design.${
		subject.jerseyNumber
			? ` The number on the jersey is ${subject.jerseyNumber}.`
			: ""
	}`;
};

// "1996-97 Topps Chrome" -> "1996-97". Multi-year brands have no year in the
// label, so it comes off `since`, which is the ending year of the first season.
const YEAR_PREFIX = /^(\d{4}(?:-\d{2})?)\s/;
const neutralSetLabel = (set: { label: string; since: number }): string => {
	const match = YEAR_PREFIX.exec(set.label);
	if (match) {
		return `${match[1]} style`;
	}
	return `${set.since - 1}-${String(set.since % 100).padStart(2, "0")} style`;
};

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
const faceBlock = (subject: CardSubject, safeMode = false): string => {
	if (!subject.face) {
		return "";
	}
	return `\n\n## The player's face\n\n${
		safeMode
			? "A screenshot of this character's CARTOON AVATAR is attached - a flat vector drawing, not a photograph of anyone. It is his complete reference."
			: "A screenshot of this player is attached. It is a HEADSHOT - a straight-on, neutral, unposed reference, the way a roster photo is."
	}

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

const designBlock = (setId: string, safeMode = false): string => {
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

	// Safe mode drops the brand from the heading and the era's name (several read
	// "The Fanatics/Topps Return" and the like). The design description itself is
	// kept word for word - it is what makes the card look like the card, and a
	// period design is a style, not a trademark.
	return `## The card design: ${safeMode ? neutralSetLabel(set) : set.label}

Era design language${safeMode ? "" : ` - ${era?.label ?? ""}`}: ${era?.language ?? ""}

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
// Careful with the wording here: it must NOT say "portrait". Almost every card
// is, but the 1980-81 Topps three-panel card is landscape, and telling that one
// its back is portrait contradicts the shape section three lines above it.
const BACK_ORIENTATION = `If the back design described below reads horizontally, that is the layout, not the card. The card keeps exactly the shape and orientation given above; a horizontal layout on a taller-than-wide card is printed rotated a quarter turn onto it, exactly as the real card is, which is why you turn one sideways to read the back. Do not change the card's orientation to suit the layout - a back that is a different shape from the front is wrong no matter how good it looks.`;

const jerseyBlock = (subject: CardSubject): string =>
	`## The uniform

He is wearing the ${subject.teamName} uniform as that franchise actually wore it in ${subject.season} - that real-world design, its real colors, its real wordmark and striping. If you do not know that franchise's ${subject.season} uniform, use the most recent real design you do know for them. This is the only place real-world knowledge belongs on this card.${
		subject.jerseyNumber
			? ` The number on the jersey is ${subject.jerseyNumber}.`
			: ""
	}`;

// ONE action per card, chosen here rather than left to the model.
//
// The prompt used to list six possibilities and let it pick. It picked the
// same one nearly every time - the player driving with the ball - and no
// wording fixes that, because every card is generated in a fresh chat with no
// memory of the last one. "Vary it" is an instruction with nothing to vary
// against. The variety has to live on this side, where the previous cards are
// actually known.
//
// Eligibility is by position, so the variety is also plausible: a centre gets
// post-ups, lobs and blocks, a point guard gets step-backs and cross-court
// passes, and neither gets handed the other's card.

// Deliberately spread across what a basketball game actually contains, not
// just scoring: shooting, finishing, passing, defending, rebounding, running,
// hustling, reacting. A pool that is nine kinds of jump shot is only a little
// less repetitive than one action.
const ANY_ACTION = [
	// With the ball
	"finishing at the rim through contact, body twisting away from the defender",
	"rising for a pull-up jumper, defender a half-step late",
	"driving baseline with a defender on his hip",
	"caught mid-air on a floater over a bigger defender",
	"catching the ball on the move and squaring up in one motion",
	"absorbing contact on the way up, arm extended through the foul",
	"bringing the ball up against full-court pressure, shielding it with his off arm",
	"exploding out of a jab step past his man",
	"gathering off two feet in the lane, ball tucked",
	"losing his defender on a hard change of direction",
	"setting his feet behind the arc as the pass arrives",
	"running off a handoff at the elbow",
	"rising with a defender's hand in his face",
	// Shooting, and the beat after it
	"releasing a shot with the follow-through still held, eyes on the rim",
	"landing after a shot, still watching the flight of the ball",
	"hanging on the follow-through of a deep shot, one arm up",
	// Defence
	"sliding his feet on defence, low, both hands active",
	"contesting a shot with one hand straight up, no jump",
	"reaching in for a steal without leaving his feet",
	"hedging out on a screen with his arms wide",
	"leaping to deflect a passing lane",
	"taking a charge, body already falling backwards",
	"planted in a defensive stance with the ball-handler right in front of him",
	"shouting instructions to a teammate while backpedalling on defence",
	// Rebounding
	"coming down with a rebound at its highest point, both hands on the ball",
	"tangled with a defender for a rebound, both hands on the ball",
	"reaching back to save a rebound to a teammate",
	// Running and hustle
	"sprinting the floor in transition, head up, ball out in front",
	"finishing a fast break with nobody within ten feet",
	"fighting through a screen, shoulder into the screener",
	"diving after a loose ball, both bodies low",
	"jumping to save a ball from going out, body already over the sideline",
	// Reaction
	"pointing back downcourt at a teammate after a made basket",
	"celebrating a basket mid-stride, fist clenched, teammates behind him",
];

const GUARD_ACTION = [
	"crossing a defender over, the defender's weight going the wrong way",
	"stepping back into a jumper as the defender lunges past",
	"whipping a cross-court pass, shoulders still turned the other way",
	"splitting a double team with the ball kept low",
	"picking a pass off in the lane and turning upcourt",
	"pulling up in transition off one dribble",
	"releasing a three from the top of the key over a closeout",
	"hanging back on the dribble at the top, reading the defence",
	"throwing a lob toward the rim, eyes up",
	"going behind his back to change direction",
	"snaking a pick and roll back across the lane",
	"dropping a bounce pass through traffic to the roll man",
	"hesitating at the free-throw line, his defender frozen",
	"pressuring the ball ninety feet from the basket",
	"spinning out of a trap in the corner",
	"throwing an outlet pass the length of the floor",
	"shooting a sidestep three from the wing, heels near the line",
	"stealing the ball off the dribble and taking it the other way",
	"running the break three-on-two with his head up",
	"coming off a screen with his feet already set to shoot",
	"drawing two defenders and kicking it out to the corner",
	"crossing half court with one hand raised, calling the play",
	"slipping under a big to finish on the far side of the rim",
	"rising for a jumper at the elbow with a hand in his face",
];

const BIG_ACTION = [
	"posting up with a forearm into the defender's chest, ball held high",
	"rising to block a shot at its apex",
	"catching a lob above the rim",
	"cocking the ball back for a two-handed dunk",
	"tipping a missed shot back up with one hand",
	"boxing out under the rim, arms wide",
	"turning over his shoulder into a hook shot",
	"rolling hard to the rim with a hand up for the pass",
	"setting a screen at the elbow, feet planted, braced for contact",
	"ripping a defensive rebound down out of traffic, elbows out",
	"sealing his man under the basket with a wide base",
	"swatting a shot away, still in the air",
	"finishing a putback before coming back down",
	"stepping out to shoot from the top of the key",
	"facing up from the elbow, ball held above his head",
	"drop-stepping baseline into the lane",
	"altering a shot at the rim without fouling, arm straight up",
	"running the floor ahead of the guards on a break",
	"catching the ball in the deep post with both hands",
	"spinning off his man into the middle of the lane",
	"holding the ball high over a smaller defender, looking to pass out",
	"hammering home an alley-oop with one hand",
	"contesting at the rim as a guard tries to finish over him",
	"stepping into a short jumper from the free-throw line",
];

const WING_ACTION = [
	"elevating over a closeout from the corner",
	"cutting backdoor and catching it in stride",
	"spinning baseline into the lane",
	"running a defender off a screen and catching it ready to shoot",
	"stripping the ball on a drive from the weak side",
	"soaring in from the wing to finish above the rim",
	"filling the lane on a break and rising for the finish",
	"shooting a corner three with his toes behind the line",
	"chasing a layup down from behind for the block",
	"attacking a closeout with one long dribble",
	"curling off a screen into the middle of the floor",
	"backing a smaller defender down on the block",
	"crashing the offensive glass from the wing",
	"switching onto a guard and staying in front of him",
	"catching and rising from the wing without a dribble",
	"driving the middle and going up through two defenders",
	"leaking out early and catching the outlet in stride",
	"denying his man the ball, arm extended into the passing lane",
	"flying in from the weak side to tip a miss back in",
	"planting hard and pulling up from mid-range",
];

// ZenGM positions: PG G SG GF SF F PF FC C.
const actionPool = (pos: string): string[] => {
	const p = pos.toUpperCase();
	if (p === "C" || p === "FC" || p === "PF") {
		return [...ANY_ACTION, ...BIG_ACTION];
	}
	if (p === "PG" || p === "G" || p === "SG") {
		return [...ANY_ACTION, ...GUARD_ACTION];
	}
	if (p === "SF" || p === "GF" || p === "F") {
		return [...ANY_ACTION, ...WING_ACTION];
	}
	return ANY_ACTION;
};

// Stable when nothing distinguishes two cards, so a prompt copied twice reads
// the same; different across players, seasons, sets and variants, so building
// out a set does not produce the same photograph nine times.
const hashCard = (subject: CardSubject, setId: string, variantId: string) => {
	const key = `${subject.name}|${subject.season}|${setId}|${variantId}`;
	let h = 2166136261;
	for (let i = 0; i < key.length; i++) {
		h ^= key.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	return h >>> 0;
};

export const buildCardFrontPrompt = (
	setId: string,
	variantId: string,
	subject: CardSubject,
	// Re-roll the photograph without changing anything else about the card.
	// Omitted, the action is derived from the card itself (see hashCard); the
	// app passes a fresh number each time the prompts are built so pressing the
	// button again gives a different shot.
	actionSeed?: number,
	override?: CardPromptOverride,
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

	const safeMode = override?.safeMode ?? false;

	const pool = actionPool(subject.pos);
	const action =
		pool[(actionSeed ?? hashCard(subject, setId, variantId)) % pool.length]!;

	// The scene: either the standard candid in-game action, or whatever the
	// override puts in its place. The cartoon-render rule below applies to both.
	const photographBody = override?.photograph
		? override.photograph
		: `A CANDID shot, not a portrait. This is a professional sports photographer sitting courtside at a live ${
				safeMode ? "" : "NBA "
			}game, shooting this player in the middle of PLAYING BASKETBALL, and he does not know the camera is there. No posing, no looking into the lens, no smiling at the camera, no arms folded, no ball resting on the hip, no studio backdrop.

**THE MOMENT ON THIS CARD: ${action}.** Shoot that, not a generic drive with the ball - it is the specific thing that separates this card from every other one in the set, so build the whole frame around it.

Shot from the sideline or the baseline, at court level, with a long lens: the player caught mid-action and filling the frame, the crowd and the arena falling out of focus behind him. Natural arena lighting.${
				set.photography?.toLowerCase().includes("posed")
					? " EXCEPTION for this particular set: its Photography note above calls for a posed shot, and a few early designs really were made that way - follow the set."
					: ""
			}`;

	const uniformBlock = override?.uniform
		? `## The uniform\n\n${override.uniform}`
		: safeMode
			? jerseyBlockSafe(subject)
			: jerseyBlock(subject);

	const achievementBlock = override?.achievement
		? `\n\n## The achievement

This card commemorates: **${override.achievement}**. Print one small flag on the front reading exactly "${override.achievement}" - a banner, ribbon, badge, or text strip styled the way this set would style an award or subset marker, consistent with the design language above. Keep it secondary to his name and team.`
		: "";

	return `Generate the FRONT of a single basketball trading card, as one image. Output only the card, nothing else.

${shapeBlock(setId)}

${safeMode ? FICTION_SAFE : FICTION}

${designBlock(setId, safeMode)}${variantBlock}

## The season on the card

The design above is a ${safeMode ? neutralSetLabel(set) : set.label} design. That is the LOOK ONLY. This card depicts the **${subject.season}** season, and **${subject.season}** is the year that must be printed on the card wherever the design puts a year. Do not change the year to match the design's own era. A ${safeMode ? neutralSetLabel(set) : set.label} design showing ${subject.season} is exactly what is wanted.

## The photograph

${photographBody}

But it is RENDERED in flat faces.js cartoon style, not photorealism: the player, and anyone visible behind him, are drawn as clean flat vector shapes with solid fills. Think of a cartoon illustration composed exactly the way a real courtside photograph would be composed.

${uniformBlock}

## The player

${bioLines(subject).join("\n")}${
		subject.awards.length > 0
			? `\nHonors through ${subject.season}: ${subject.awards.join(", ")}`
			: ""
	}${faceBlock(subject, safeMode)}

## Text on the card

The card front shows his name (${subject.name}), his position (${subject.pos}), and his team (${subject.teamName}), laid out the way this set lays them out. Spell the name exactly as written. Do not invent extra text, taglines, or logos that the design description above does not call for.${achievementBlock}`;
};

export const buildCardBackPrompt = (
	setId: string,
	variantId: string,
	subject: CardSubject,
	override?: Pick<CardPromptOverride, "achievement" | "safeMode">,
): string => {
	const set = cardSetsById.get(setId);
	if (!set) {
		return "";
	}
	const variant = set.variants.find((v) => v.id === variantId);
	const safeMode = override?.safeMode ?? false;
	const label = safeMode ? neutralSetLabel(set) : set.label;

	return `Generate the BACK of a single basketball trading card, as one image. Output only the card, nothing else.

This is the back of a ${label}${variant && variant.id !== "base" ? ` ${variant.label}` : ""} card for ${subject.name}, depicting the ${subject.season} season. It has to pair with a front generated separately from the same design.

${shapeBlock(setId)}

${BACK_ORIENTATION}

${safeMode ? FICTION_SAFE : FICTION}

## The back design

${set.back}

Era design language: ${cardErasById.get(set.era)?.language ?? ""}

Lay it out the way a real card back of this era is laid out - the card number in its usual corner, the biography block, and the statistics grid filling most of the card. Keep the back's palette consistent with the front's.

## The season on the card

**${subject.season}** is the season this card depicts and the year printed on it, regardless of what era the ${label} design comes from.

## Biography block

${bioLines(subject).join("\n")}${
		subject.awards.length > 0
			? `\nHonors through ${subject.season}: ${subject.awards.join(", ")}`
			: ""
	}${
		override?.achievement
			? `\n\nThis card commemorates: **${override.achievement}**. Work one line into the biography area saying so, set in the design's own typography.`
			: ""
	}

## Statistics grid

Reproduce these numbers EXACTLY as given - every row, every column, no rounding changes, no invented seasons, no extrapolation past ${subject.season}. These are per-game averages. This is the single most important part of the card; a stat grid with made-up numbers ruins it.

\`\`\`
${statTable(subject)}
\`\`\`

If the era's design would not fit this many columns, drop columns from the right rather than inventing a compressed layout, and never drop a season row.`;
};
