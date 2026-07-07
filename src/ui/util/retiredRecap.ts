import type {
	RetiredCareerLine,
	RetiredPlayer,
	RetiredPlayersData,
} from "../../worker/util/getRetiredPlayersForRecap.ts";

// Instructions for the retirement writeups. The key idea: length must follow
// the career. A 20-year Hall of Famer earns a full retrospective; a fringe role
// player gets a paragraph; an undrafted player who never logged a game gets a
// sentence or two. The AI is told this explicitly.
const INSTRUCTIONS = `You are an expert basketball writer. Write a retirement writeup for EACH player listed below — the kind of career retrospective published when a player hangs it up.

CRITICAL — scale the length to the career. Do not give everyone the same treatment:
- Hall of Famers and decorated stars: a full retrospective (several paragraphs) — the arc of the career, peak, signature seasons, accolades, legacy, where they rank.
- Solid long-tenured players: a couple of tight paragraphs.
- Role players / journeymen: a short paragraph.
- Players who barely played, and especially undrafted players who never logged a single game: one or two honest sentences. Do not invent a career that isn't there.

Use the data provided: career and playoff averages, the season-by-season arc, every team, the draft slot, age, college/country, peak rating, awards and rings. Do NOT list the raw data back — write prose.

Follow these rules EXACTLY:
- Reply in GitHub-flavored Markdown only. No preamble, no closing summary, no text outside the per-player writeups.
- Begin every player's writeup with a line containing ONLY this marker: <!--player:ID--> (replace ID with that player's number, shown as "PLAYER <ID>" below). This is how each writeup is filed to the correct player — never omit it, never change it.
- After the marker, lead with a bold one-line headline, then the writeup at the length the career warrants.
- Bold the player's name on first mention. Put exactly one blank line between players.`;

const heightText = (inches: number | undefined): string | undefined => {
	if (typeof inches !== "number" || inches <= 0) {
		return undefined;
	}
	return `${Math.floor(inches / 12)}'${inches % 12}"`;
};

const careerLineText = (
	label: string,
	line: RetiredCareerLine | undefined,
): string | undefined => {
	if (!line) {
		return undefined;
	}
	const shooting = [
		typeof line.fgp === "number" ? `${line.fgp}% FG` : undefined,
		typeof line.tpp === "number" ? `${line.tpp}% 3P` : undefined,
		typeof line.ftp === "number" ? `${line.ftp}% FT` : undefined,
	]
		.filter(Boolean)
		.join(", ");
	const extras = [
		typeof line.stl === "number" ? `${line.stl} STL` : undefined,
		typeof line.blk === "number" ? `${line.blk} BLK` : undefined,
		typeof line.per === "number" ? `${line.per} PER` : undefined,
	]
		.filter(Boolean)
		.join(", ");
	return `${label}: ${line.pts}/${line.trb}/${line.ast}${
		shooting ? ` on ${shooting}` : ""
	}${extras ? ` (${extras})` : ""} over ${line.gp} G`;
};

const draftText = (p: RetiredPlayer): string => {
	if (!p.draft) {
		return "Draft: unknown";
	}
	if (p.draft.undrafted) {
		return `Draft: undrafted (${p.draft.year})`;
	}
	return `Draft: ${p.draft.year}, round ${p.draft.round}, pick ${p.draft.pick}`;
};

const playerBlock = (p: RetiredPlayer): string => {
	const bio = [
		p.pos,
		typeof p.ageAtRetirement === "number"
			? `retired at ${p.ageAtRetirement}`
			: undefined,
		heightText(p.heightIn),
		p.college ? `college: ${p.college}` : undefined,
		p.country ? p.country : undefined,
		typeof p.peakOvr === "number" ? `peak ${p.peakOvr} ovr` : undefined,
	]
		.filter(Boolean)
		.join(", ");

	const lines = [
		`### PLAYER ${p.pid}: ${p.name}${p.hof ? " — HALL OF FAMER" : ""}`,
		bio,
		draftText(p),
	];

	if (p.neverPlayed) {
		lines.push("Never played a game in the league.");
	} else {
		lines.push(
			`Career: ${p.seasonsPlayed} season${
				p.seasonsPlayed === 1 ? "" : "s"
			}, ${p.totalGP} games${
				p.firstSeason && p.lastSeason
					? ` (${p.firstSeason}–${p.lastSeason})`
					: ""
			}${p.rings > 0 ? `, ${p.rings} championship${p.rings === 1 ? "" : "s"}` : ""}.`,
		);
		const career = careerLineText("Career per game", p.career);
		if (career) {
			lines.push(career);
		}
		const playoffs = careerLineText("Playoffs per game", p.playoffs);
		if (playoffs) {
			lines.push(playoffs);
		}
		if (p.teams.length > 0) {
			lines.push(
				`Teams: ${p.teams
					.map((t) =>
						t.from === t.to
							? `${t.abbrev} (${t.from})`
							: `${t.abbrev} (${t.from}–${t.to})`,
					)
					.join(", ")}`,
			);
		}
		if (p.awards.length > 0) {
			lines.push(
				`Awards: ${p.awards
					.map((a) => (a.count > 1 ? `${a.type} ×${a.count}` : a.type))
					.join("; ")}`,
			);
		}
		if (p.bySeason.length > 0) {
			const arc = p.bySeason
				.map(
					(s) =>
						`${s.season} ${s.abbrev ?? ""} ${s.pts}/${s.trb}/${s.ast}${
							typeof s.per === "number" ? ` (${s.per} PER)` : ""
						} in ${s.gp} G`,
				)
				.join("; ");
			lines.push(`Season by season: ${arc}`);
		}
	}

	return lines.filter(Boolean).join("\n");
};

// The full prompt: instructions + every retired player's career data.
export const buildRetiredRecapPrompt = (data: RetiredPlayersData): string => {
	const blocks = data.players.map(playerBlock).join("\n\n");
	return `${INSTRUCTIONS}

---

${data.players.length} player${
		data.players.length === 1 ? "" : "s"
	} retired after the ${data.season} season:

${blocks}`;
};

// Split a pasted AI response into { pid → writeup markdown } by its markers.
export const parseRetiredRecaps = (text: string): Map<number, string> => {
	const result = new Map<number, string>();
	const re = /<!--\s*player:\s*(\d+)\s*-->/g;
	const markers = [...text.matchAll(re)];

	for (let i = 0; i < markers.length; i++) {
		const marker = markers[i]!;
		const pid = Number(marker[1]);
		const start = marker.index + marker[0].length;
		const end = i + 1 < markers.length ? markers[i + 1]!.index : text.length;
		const recap = text.slice(start, end).trim();
		if (recap) {
			result.set(pid, recap);
		}
	}

	return result;
};
