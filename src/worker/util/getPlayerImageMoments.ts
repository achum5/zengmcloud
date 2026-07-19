import type { PlayerWithoutKey } from "../../common/types.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";

// A ready-to-copy AI image prompt for one real moment in a player's career, plus
// a short label for the editor's dropdown. All prompts describe a Basketball GM
// faces.js-style CARTOON illustration - never photorealistic - so generated
// images match the game's art style.
export type ImageMoment = { key: string; label: string; prompt: string };

const STYLE =
	"Draw it in the clean, flat cartoon-avatar style of Basketball GM (faces.js): simple bold vector shapes, solid flat colors, minimal shading, front-facing and stylized. NOT photorealistic.";

const stripHtml = (s: string) => s.replace(/<[^>]*>/g, "").trim();

// The jersey number the player wore in a given season (from that season's stats
// row), falling back to their current number.
const jerseyForSeason = (
	p: PlayerWithoutKey,
	season: number,
): string | undefined => {
	for (const s of (p.stats ?? []) as any[]) {
		if (s.season === season && s.jerseyNumber) {
			return s.jerseyNumber;
		}
	}
	return p.jerseyNumber;
};

// "Name, a basketball PG (age 24, jersey #23, 6'6\", 215 lbs)" - the physical
// details AS OF the given season, so a moment reads correctly for the player at
// that point in their career (age and jersey change over time; height/weight are
// fixed).
export const describePlayerSubject = (
	p: PlayerWithoutKey,
	pos: string,
	season: number,
): string => {
	const name = `${p.firstName} ${p.lastName}`.trim() || "the player";
	const parts: string[] = [];
	const bornYear = p.born?.year;
	if (typeof bornYear === "number" && season >= bornYear) {
		parts.push(`age ${season - bornYear}`);
	}
	const jersey = jerseyForSeason(p, season);
	if (jersey) {
		parts.push(`jersey #${jersey}`);
	}
	if (p.hgt) {
		parts.push(`${Math.floor(p.hgt / 12)}'${p.hgt % 12}"`);
	}
	if (p.weight) {
		parts.push(`${p.weight} lbs`);
	}
	return `${name}, a basketball ${pos}${
		parts.length > 0 ? ` (${parts.join(", ")})` : ""
	}`;
};

// Awards worth their own moment, mapped to how the prompt should describe the
// scene. Anything not listed (All-League/All-Defensive teams, etc.) is skipped
// to keep the list tight.
const AWARD_SCENES: Record<string, (team: string | undefined) => string> = {
	"Won Championship": (team) =>
		`celebrating winning the championship${team ? ` with the ${team}` : ""}, holding the trophy with confetti falling`,
	"Most Valuable Player": () =>
		`accepting the league MVP trophy at a podium, big smile`,
	"Finals MVP": () =>
		`holding the Finals MVP trophy on the court after the win`,
	"Defensive Player of the Year": () =>
		`accepting the Defensive Player of the Year trophy`,
	"Rookie of the Year": () => `accepting the Rookie of the Year trophy`,
	"Sixth Man of the Year": () => `accepting the Sixth Man of the Year trophy`,
	"Most Improved Player": () => `accepting the Most Improved Player trophy`,
	"All-Star": (team) =>
		`at the All-Star Game in an All-Star jersey${team ? `, still repping the ${team}` : ""}`,
};

// Build the list of per-player image moments from a raw player object plus its
// playerFeat events (notable single-game performances). Team names are resolved
// as of the season each moment happened, so relocations read correctly.
export const getPlayerImageMoments = async (
	p: PlayerWithoutKey,
	pos: string,
	feats: { season?: number; text?: string }[],
): Promise<ImageMoment[]> => {
	const moments: ImageMoment[] = [];

	const teamName = async (
		tid: number | undefined,
		season: number,
	): Promise<string | undefined> => {
		if (tid === undefined || tid < 0) {
			return undefined;
		}
		const info = await getTeamInfoBySeason(tid, season);
		return info ? `${info.region} ${info.name}` : undefined;
	};

	// 1. Draft night.
	if (p.draft && p.draft.round > 0) {
		const team = await teamName(p.draft.tid, p.draft.year);
		if (team) {
			moments.push({
				key: `draft`,
				label: `Draft night — R${p.draft.round} P${p.draft.pick}, ${team} (${p.draft.year})`,
				prompt: `A cartoon illustration of ${describePlayerSubject(p, pos, p.draft.year)}, on draft night: standing on stage smiling after being drafted in round ${p.draft.round} (pick ${p.draft.pick}) by the ${team} in ${p.draft.year}, wearing a ${team} draft cap and holding up a ${team} jersey. ${STYLE}`,
			});
		}
	}

	// 2. Transactions - introduced to a new team at a press conference.
	for (const t of p.transactions ?? []) {
		if (t.type !== "trade" && t.type !== "freeAgent") {
			continue;
		}
		const team = await teamName(t.tid, t.season);
		if (!team) {
			continue;
		}
		const how =
			t.type === "trade"
				? `after being traded to the ${team}`
				: `after signing with the ${team}`;
		moments.push({
			key: `txn-${t.season}-${t.tid}-${t.type}`,
			label:
				t.type === "trade"
					? `Traded to ${team} (${t.season})`
					: `Signed with ${team} (${t.season})`,
			prompt: `A cartoon illustration of ${describePlayerSubject(p, pos, t.season)} at an introductory press conference ${how} in ${t.season}, smiling while holding up a ${team} jersey next to the team logo. ${STYLE}`,
		});
	}

	// 3. Awards / season highlights. The award carries no team, so resolve it
	// from the player's stats row for that season.
	const teamForSeason = (season: number): number | undefined => {
		const rows = (p.stats ?? []).filter(
			(s: any) => s.season === season && !s.playoffs,
		);
		return rows.length > 0 ? rows.at(-1)!.tid : undefined;
	};
	for (const award of p.awards ?? []) {
		const scene = AWARD_SCENES[award.type];
		if (!scene) {
			continue;
		}
		const team = await teamName(teamForSeason(award.season), award.season);
		moments.push({
			key: `award-${award.season}-${award.type}`,
			label: `${award.type} (${award.season})`,
			prompt: `A cartoon illustration of ${describePlayerSubject(p, pos, award.season)} ${scene(team)}, in ${award.season}. ${STYLE}`,
		});
	}

	// 4. Notable games (playerFeat events) - most recent handful only.
	for (const feat of feats.slice(-8)) {
		const text = stripHtml(feat.text ?? "");
		if (!text) {
			continue;
		}
		moments.push({
			key: `feat-${feat.season}-${text.slice(0, 20)}`,
			label: `Big game${feat.season ? ` (${feat.season})` : ""}`,
			prompt: `A cartoon illustration of ${describePlayerSubject(p, pos, feat.season ?? 0)} in the middle of a standout game — ${text} — celebrating on the court. ${STYLE}`,
		});
	}

	return moments;
};

// A starting prompt for the "Customize" option, already carrying the player's
// current physical details and the cartoon style so a hand-written prompt still
// matches the game's art. The user edits the middle to describe the scene.
export const customImagePromptSeed = (
	p: PlayerWithoutKey,
	pos: string,
	team: string | undefined,
	season: number,
): string =>
	`A cartoon illustration of ${describePlayerSubject(p, pos, season)}${
		team ? ` on the ${team}` : ""
	}, . ${STYLE}`;
