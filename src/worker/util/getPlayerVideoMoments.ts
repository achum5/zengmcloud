import type { PlayerWithoutKey } from "../../common/types.ts";
import { g } from "./index.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";

// A ready-to-copy AI VIDEO prompt for one real moment in a player's career, plus
// a short label for the editor's dropdown. Prompts are written for an AI video
// generator (Sora/Runway/etc.) and carry full on-court context - who else is
// likely on the floor, with heights, weights, and jersey numbers - so the clip
// looks like the actual game situation.
export type VideoMoment = { key: string; label: string; prompt: string };

// The house art direction, shared by every video prompt: cartoon (faces.js)
// broadcast-highlight look, never photorealistic, no fabricated on-screen text.
const VIDEO_STYLE =
	"Render it as a dynamic, broadcast-style basketball highlight clip animated in the clean, flat cartoon-avatar art style of Basketball GM (faces.js): bold vector shapes, solid flat colors, minimal shading, stylized players - NOT photorealistic. Use lively camera work (tracking shots, a slow-motion beat on the key moment), a packed arena crowd, and authentic team colors and jersey numbers on the uniforms. Do NOT render any real-world brand logos, and do NOT add any on-screen captions, name plates, stat cards, scorebugs with text, or watermarks - jersey numbers and team colors are the only lettering allowed.";

const stripHtml = (s: string) => s.replace(/<[^>]*>/g, "").trim();

const feet = (hgt: number | undefined) =>
	hgt ? `${Math.floor(hgt / 12)}'${hgt % 12}"` : undefined;

// The jersey number the player wore in a given season, else their current one.
const jerseyForSeason = (
	p: { stats?: any[]; jerseyNumber?: string },
	season: number,
): string | undefined => {
	for (const s of p.stats ?? []) {
		if (s.season === season && s.jerseyNumber) {
			return s.jerseyNumber;
		}
	}
	return p.jerseyNumber;
};

// "Jayson Tatum (SF, 6'8", 210 lbs, #0)" - a compact, physical one-liner for any
// player on the floor, using their details as of `season`.
const describeOnCourt = (
	p: { firstName?: string; lastName?: string; hgt?: number; weight?: number; stats?: any[]; jerseyNumber?: string },
	pos: string | undefined,
	season: number,
): string => {
	const name = `${p.firstName ?? ""} ${p.lastName ?? ""}`.trim() || "a player";
	const bits = [
		pos,
		feet(p.hgt),
		p.weight ? `${p.weight} lbs` : undefined,
		(() => {
			const j = jerseyForSeason(p, season);
			return j ? `#${j}` : undefined;
		})(),
	].filter(Boolean);
	return bits.length > 0 ? `${name} (${bits.join(", ")})` : name;
};

// The rotation a team most likely had on the floor in a season: players with the
// most minutes for that (season, tid), richest-detail first. `posOf` maps a pid
// to its position string (computed by the caller, which has the ratings).
const rotationForSeasonTid = (
	allPlayers: PlayerWithoutKey[],
	tid: number,
	season: number,
	posOf: (pid: number) => string | undefined,
	excludePid: number | undefined,
	limit: number,
): string[] => {
	const rows: { p: PlayerWithoutKey; min: number }[] = [];
	for (const p of allPlayers) {
		if (excludePid !== undefined && (p as any).pid === excludePid) {
			continue;
		}
		let min = 0;
		let played = false;
		for (const s of (p.stats ?? []) as any[]) {
			if (s.season === season && s.tid === tid && !s.playoffs && s.gp > 0) {
				min += s.min ?? 0;
				played = true;
			}
		}
		if (played) {
			rows.push({ p, min });
		}
	}
	rows.sort((a, b) => b.min - a.min);
	return rows
		.slice(0, limit)
		.map(({ p }) => describeOnCourt(p, posOf((p as any).pid), season));
};

// Pull the opponent's team NAME out of a feat/clutch line (they end "... win over
// the Cavaliers." / "loss to the Lakers." / "tie with the Nets."), so we can
// resolve the opposing roster.
export const opponentNameFromText = (text: string): string | undefined => {
	const m = /(?:over the|to the|with the|against the) ([^.,]+?)[.,]?\s*$/i.exec(
		text.replace(/\s+/g, " ").trim(),
	);
	return m ? m[1]!.trim() : undefined;
};

const AWARD_SCENES: Record<string, string> = {
	"Won Championship":
		"a euphoric championship celebration: the final buzzer, players storming the court, confetti raining down, the team hoisting the trophy",
	"Most Valuable Player":
		"an MVP tribute reel: signature plays intercut with the player accepting the MVP trophy at a podium to a roaring crowd",
	"Finals MVP":
		"a Finals MVP montage ending with the player holding the Finals MVP trophy at center court amid confetti",
	"Defensive Player of the Year":
		"a defensive highlight reel - blocks, steals, and lockdown stops - ending at the Defensive Player of the Year podium",
	"Rookie of the Year":
		"a rookie-season highlight reel ending with the Rookie of the Year trophy presentation",
	"Sixth Man of the Year":
		"a bench-spark highlight reel ending with the Sixth Man of the Year trophy",
	"Most Improved Player":
		"a breakout-season highlight montage ending with the Most Improved Player trophy",
	"All-Star": "an All-Star Game highlight reel of the player in an All-Star jersey",
};

export const getPlayerVideoMoments = async (
	p: PlayerWithoutKey,
	pos: string,
	feats: { season?: number; text?: string }[],
	allPlayers: PlayerWithoutKey[],
	posOf: (pid: number) => string | undefined,
): Promise<VideoMoment[]> => {
	const moments: VideoMoment[] = [];
	const pid = (p as any).pid as number | undefined;

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

	// Opponent name -> tid, from the current team names (leagues rarely relocate,
	// so this resolves the great majority of opponents to a real roster).
	const teamInfoCache = g.get("teamInfoCache");
	const oppTidByName = new Map<string, number>();
	teamInfoCache.forEach((t, tid) => {
		if (t) {
			oppTidByName.set(t.name.toLowerCase(), tid);
			oppTidByName.set(`${t.region} ${t.name}`.toLowerCase(), tid);
		}
	});

	// The player's own team in a given season (from a non-playoff stats row).
	const ownTidForSeason = (season: number): number | undefined => {
		const rows = (p.stats ?? []).filter(
			(s: any) => s.season === season && !s.playoffs && s.gp > 0,
		);
		return rows.length > 0 ? rows.at(-1)!.tid : undefined;
	};

	// The full "who's on the floor" block for a game moment in `season`, given the
	// player's team and (optionally) the opponent parsed from the feat text.
	const onCourtBlock = async (
		season: number,
		ownTid: number | undefined,
		oppText: string | undefined,
	): Promise<string> => {
		const lines: string[] = [
			`The star: ${describeOnCourt(p, pos, season)}.`,
		];
		if (ownTid !== undefined) {
			const own = await teamName(ownTid, season);
			const mates = rotationForSeasonTid(
				allPlayers,
				ownTid,
				season,
				posOf,
				pid,
				7,
			);
			if (mates.length > 0) {
				lines.push(
					`His ${own ?? "team"} teammates most likely on the floor: ${mates.join("; ")}.`,
				);
			}
		}
		const oppName = oppText ? opponentNameFromText(oppText) : undefined;
		if (oppName) {
			const oppTid = oppTidByName.get(oppName.toLowerCase());
			const oppRoster =
				oppTid !== undefined
					? rotationForSeasonTid(allPlayers, oppTid, season, posOf, undefined, 7)
					: [];
			if (oppRoster.length > 0) {
				lines.push(
					`The opposing ${oppName} rotation on the floor: ${oppRoster.join("; ")}.`,
				);
			} else {
				lines.push(`The opponent is the ${oppName}; depict their starting five.`);
			}
		}
		return lines.join(" ");
	};

	// Split feats into clutch game moments vs statistical feats by their wording.
	const isClutch = (text: string) =>
		/to (force|win|tie)\b|buzzer|game[- ]?winn|with [\d.]+ seconds|clutch/i.test(
			text,
		);

	const clutch: typeof feats = [];
	const statFeats: typeof feats = [];
	for (const feat of feats) {
		const text = stripHtml(feat.text ?? "");
		if (!text) {
			continue;
		}
		(isClutch(text) ? clutch : statFeats).push({ ...feat, text });
	}

	// 1. Clutch moments - a single dramatic, cinematic sequence each.
	for (const [i, feat] of clutch.entries()) {
		const season = feat.season ?? 0;
		const text = feat.text!;
		const ownTid = ownTidForSeason(season);
		const onCourt = await onCourtBlock(season, ownTid, text);
		const opp = opponentNameFromText(text);
		moments.push({
			key: `clutch-${season}-${i}`,
			label: `Clutch moment${opp ? ` vs ${opp}` : ""}${season ? ` (${season})` : ""}`,
			prompt: `${VIDEO_STYLE}\n\nTHE MOMENT: ${text} Build the whole clip toward this single dramatic play, with a slow-motion beat as the shot goes in and the crowd erupts.\n\nON THE COURT: ${onCourt}\n\nDIRECTION: Set the scene late in a tight game, run the possession that leads to the play, hit slow-motion on the decisive moment, then cut to the bench and crowd exploding and teammates mobbing him.`,
		});
	}

	// 2. Statistical feats - fast-cut HIGHLIGHT REELS (triple-doubles, big nights).
	for (const [i, feat] of statFeats.entries()) {
		const season = feat.season ?? 0;
		const text = feat.text!;
		const ownTid = ownTidForSeason(season);
		const onCourt = await onCourtBlock(season, ownTid, text);
		const triple = /triple-double|quadruple/i.test(text);
		const opp = opponentNameFromText(text);
		moments.push({
			key: `feat-${season}-${i}`,
			label: `${triple ? "Triple-double" : "Big game"} reel${opp ? ` vs ${opp}` : ""}${season ? ` (${season})` : ""}`,
			prompt: `${VIDEO_STYLE}\n\nTHE PERFORMANCE: ${text} Make this a fast-cut highlight REEL of that game - a rapid montage of his best plays that add up to the stat line (drives, dunks, threes, dimes, boards, blocks as the numbers call for), with quick slow-motion accents and the crowd rising.\n\nON THE COURT: ${onCourt}\n\nDIRECTION: Open on the arena, then rattle off 6-8 quick highlight clips of the star doing the things in the stat line, escalating in energy, and end on a signature celebration.`,
		});
	}

	// 3. Awards / championships - cinematic tribute videos.
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
		const season = award.season;
		const team = await teamName(teamForSeason(season), season);
		const ownTid = teamForSeason(season);
		const mates =
			ownTid !== undefined
				? rotationForSeasonTid(allPlayers, ownTid, season, posOf, pid, 7)
				: [];
		moments.push({
			key: `award-${season}-${award.type}`,
			label: `${award.type} (${season})`,
			prompt: `${VIDEO_STYLE}\n\nTHE STORY: A ${season} tribute video for ${describeOnCourt(p, pos, season)}${team ? ` of the ${team}` : ""} - ${scene}.\n\nON THE COURT: ${
				mates.length > 0
					? `His teammates featured in the clips: ${mates.join("; ")}.`
					: "Feature his teammates around him."
			}\n\nDIRECTION: Intercut signature plays with the celebration/ceremony; end on a triumphant hero shot.`,
		});
	}

	// 4. Draft night.
	if (p.draft && p.draft.round > 0) {
		const team = await teamName(p.draft.tid, p.draft.year);
		if (team) {
			moments.push({
				key: "draft",
				label: `Draft night — R${p.draft.round} P${p.draft.pick}, ${team} (${p.draft.year})`,
				prompt: `${VIDEO_STYLE}\n\nTHE STORY: Draft night ${p.draft.year} - ${describeOnCourt(p, pos, p.draft.year)} hears his name called at pick ${p.draft.pick} (round ${p.draft.round}) by the ${team}, stands, hugs his family, walks across the stage, shakes hands, and holds up a ${team} jersey and cap for the cameras.\n\nDIRECTION: Green room reaction, the walk to the stage, the jersey reveal, flashing cameras and a cheering arena.`,
			});
		}
	}

	// 5. A full career montage.
	{
		const season = g.get("season");
		const currentTid = ownTidForSeason(season) ?? p.tid;
		const team = await teamName(currentTid, season);
		moments.push({
			key: "career-montage",
			label: "Career montage",
			prompt: `${VIDEO_STYLE}\n\nTHE STORY: A sweeping career highlight montage of ${describeOnCourt(p, pos, season)}${team ? `, currently on the ${team}` : ""} - his signature moves, biggest baskets, and celebrations across the years, escalating to a triumphant finish.\n\nDIRECTION: Fast-cut his best plays, build energy, and end on a hero shot with the crowd on its feet.`,
		});
	}

	return moments;
};

// Seed for the "Customize" option: the house style plus the star's details, with
// a blank middle for the user to describe their own scene.
export const customVideoPromptSeed = (
	p: PlayerWithoutKey,
	pos: string,
	team: string | undefined,
	season: number,
): string =>
	`${VIDEO_STYLE}\n\nTHE MOMENT: ${describeOnCourt(p, pos, season)}${
		team ? ` of the ${team}` : ""
	} — describe the scene here.\n\nDIRECTION: `;
