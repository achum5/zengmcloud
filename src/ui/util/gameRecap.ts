import type {
	RecapGame,
	RecapPlayer,
	RecapTeam,
} from "../../worker/util/getDayGamesForRecap.ts";

// The instructions half of the prompt. Kept as a single editable constant so it
// can be swapped for a different writing brief without touching the data-baking
// logic below.
const INSTRUCTIONS = `You are an expert basketball beat writer. Write a lively, ESPN-style recap for EACH game listed below.

Follow these rules EXACTLY:
- Reply in GitHub-flavored Markdown only. No preamble, no closing summary, no text outside the per-game recaps.
- Begin every recap with a line containing ONLY this marker: <!--game:ID--> (replace ID with that game's number, shown as "GAME <ID>" below). This is how each recap is filed to the correct game — never omit it, never change it.
- After the marker, lead with a bold one-line headline, then 1–3 tight paragraphs (~60–120 words total).
- Weave the notable numbers into the prose; do not paste a stat table. Bold standout players with **name**.
- Put exactly one blank line between games.`;

// Strip any HTML tags (ZenGM's clutch-play strings contain <a> links).
const stripHtml = (s: string): string =>
	s
		.replace(/<[^>]*>/g, "")
		.replace(/\s+/g, " ")
		.trim();

const playerLine = (p: RecapPlayer): string =>
	`- ${p.name}: ${p.pts} PTS, ${p.reb} REB, ${p.ast} AST, ${p.stl} STL, ${p.blk} BLK, ${p.tov} TO (${p.fg}/${p.fga} FG, ${p.tp}/${p.tpa} 3P, ${p.ft}/${p.fta} FT, ${p.min} min)`;

const teamBlock = (t: RecapTeam): string =>
	[
		`${t.abbrev} — ${t.region} ${t.name} (${t.pts} pts):`,
		...t.players.map(playerLine),
	].join("\n");

const gameBlock = (game: RecapGame): string => {
	// teams[0] is the home team in ZenGM; list the visitor first ("away @ home").
	const [home, away] = game.teams;
	const winner = game.teams.find((t) => t.tid === game.winnerTid);
	const ot =
		game.overtimes > 0
			? ` (${game.overtimes === 1 ? "OT" : `${game.overtimes}OT`})`
			: "";

	const lines = [
		`### GAME ${game.gid}: ${away.region} ${away.name} @ ${home.region} ${home.name}${ot}`,
		`Final: ${away.abbrev} ${away.pts}, ${home.abbrev} ${home.pts}${
			winner ? ` — ${winner.region} ${winner.name} win` : ""
		}`,
		"",
		teamBlock(away),
		"",
		teamBlock(home),
	];

	if (game.clutchPlays.length > 0) {
		lines.push(
			"",
			"Notable plays:",
			...game.clutchPlays.map((c) => `- ${stripHtml(c)}`),
		);
	}

	return lines.join("\n");
};

// The full prompt: instructions + every game's data, ready for the clipboard.
export const buildRecapPrompt = (
	games: RecapGame[],
	dayLabel: string,
): string => {
	const blocks = games.map(gameBlock).join("\n\n");
	return `${INSTRUCTIONS}

---

${games.length} game${games.length === 1 ? "" : "s"} to recap (${dayLabel}):

${blocks}`;
};

// Split a pasted AI response into { gid → recap markdown } by its game markers.
// Everything between one marker and the next belongs to that game.
export const parseRecaps = (text: string): Map<number, string> => {
	const result = new Map<number, string>();
	const re = /<!--\s*game:\s*(\d+)\s*-->/g;
	const markers = [...text.matchAll(re)];

	for (let i = 0; i < markers.length; i++) {
		const marker = markers[i]!;
		const gid = Number(marker[1]);
		const start = marker.index + marker[0].length;
		const end = i + 1 < markers.length ? markers[i + 1]!.index : text.length;
		const recap = text.slice(start, end).trim();
		if (recap) {
			result.set(gid, recap);
		}
	}

	return result;
};
