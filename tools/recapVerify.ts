// Cross-check generated recap prose against the box scores it was built from.
// Prose that reads well can still be wrong - a superlative measured over the
// wrong set of players reads perfectly and is simply false - so this asserts
// the claims against the data instead of against taste.
//
//   node tools/recapVerify.ts <data.json>
//
// The JSON is a capture of getDayGamesForRecap output, the same format
// tools/recapSample.ts reads. Not part of the app; a development aid.
import fs from "node:fs";
import {
	beginRecapBatch,
	endRecapBatch,
	getAutoDayRecap,
	getAutoRecap,
} from "../src/worker/util/getAutoRecap.ts";

const file = process.argv[2]!;
const data = JSON.parse(fs.readFileSync(file, "utf8")) as any[];

type Problem = { day: number; kind: string; detail: string; text: string };
const problems: Problem[] = [];

for (const entry of data) {
	const { day, games } = entry;
	beginRecapBatch();
	const notes: { g: any; text: string }[] = [];
	try {
		for (const g of games) {
			notes.push({ g, text: getAutoRecap(g) });
		}
	} finally {
		endRecapBatch();
	}
	const dayText = getAutoDayRecap({
		season: 2026,
		day,
		playoffs: games.some((g: any) => g.playoffs),
		games,
		standings: entry.standings?.find?.((s: any) => s.day === day),
	});

	// --- Per game -----------------------------------------------------------
	for (const { g, text } of notes) {
		const byName = new Map<string, any>();
		for (const t of g.teams) {
			for (const p of t.players) {
				byName.set(p.name, { p, t });
			}
		}
		const hi = Math.max(g.teams[0].pts, g.teams[1].pts);
		const lo = Math.min(g.teams[0].pts, g.teams[1].pts);

		// Any "NNN-NN" score in the prose must be this game's final.
		for (const m of text.matchAll(/\b(\d{2,3})-(\d{2,3})\b/g)) {
			const a = Number(m[1]);
			const b = Number(m[2]);
			// Only check pairs that look like a final score (both >= 60).
			if (a >= 60 && b >= 60 && !(a === hi && b === lo)) {
				problems.push({
					day,
					kind: "score",
					detail: `${a}-${b} is not the final (${hi}-${lo})`,
					text,
				});
			}
		}

		// "<Name> ... N points" must match his box score.
		for (const m of text.matchAll(
			/([A-Z]\S*(?: [A-Z]\S*){0,2}) (?:scored|had|posted|put up|went for|finished with|racked up|poured in|added|chipped in|contributed|kicked in|tacked on|supplied|erupted for|piled up|produced) (\d+) points/g,
		)) {
			const rec = byName.get(m[1]!);
			if (rec && rec.p.pts !== Number(m[2])) {
				problems.push({
					day,
					kind: "player pts",
					detail: `${m[1]} credited ${m[2]}, box score says ${rec.p.pts}`,
					text,
				});
			}
		}

		// "the best mark on the floor" must belong to the game's +/- leader.
		const pm = text.match(
			/([A-Z]\S*(?: [A-Z]\S*){0,2}) (?:finished|was) \+(\d+)[^.]*best mark on the floor/,
		);
		if (pm) {
			let best = -Infinity;
			for (const t of g.teams) {
				for (const p of t.players) {
					if (typeof p.pm === "number" && p.pm > best) {
						best = p.pm;
					}
				}
			}
			if (best > Number(pm[2])) {
				problems.push({
					day,
					kind: "plus-minus",
					detail: `${pm[1]} +${pm[2]} called best, but +${best} exists`,
					text,
				});
			}
		}
	}

	// --- Day wrap -----------------------------------------------------------
	const realGames = games.filter((g: any) => !g.allStar);
	const allPlayers = realGames.flatMap((g: any) =>
		g.teams.flatMap((t: any) => t.players),
	);
	const maxPts = Math.max(...allPlayers.map((p: any) => p.pts));
	const lead = dayText.match(
		/([A-Z]\S*(?: [A-Z]\S*){0,2}) led all scorers with (\d+)/,
	);
	if (lead && Number(lead[2]) !== maxPts) {
		problems.push({
			day,
			kind: "led all scorers",
			detail: `${lead[1]} credited ${lead[2]}, slate high is ${maxPts}`,
			text: dayText,
		});
	}

	const oneSided = dayText.match(
		/most one-sided result was .*? (\d+)-point win/,
	);
	if (oneSided) {
		const maxMargin = Math.max(
			...realGames.map((g: any) => Math.abs(g.teams[0].pts - g.teams[1].pts)),
		);
		if (Number(oneSided[1]) !== maxMargin) {
			problems.push({
				day,
				kind: "biggest margin",
				detail: `claimed ${oneSided[1]}, slate max is ${maxMargin}`,
				text: dayText,
			});
		}
	}

	const combined = dayText.match(
		/combined for (\d+) points, the most of any game/,
	);
	if (combined) {
		const maxTotal = Math.max(
			...realGames.map((g: any) => g.teams[0].pts + g.teams[1].pts),
		);
		if (Number(combined[1]) !== maxTotal) {
			problems.push({
				day,
				kind: "biggest total",
				detail: `claimed ${combined[1]}, slate max is ${maxTotal}`,
				text: dayText,
			});
		}
	}
}

const byKind = new Map<string, Problem[]>();
for (const p of problems) {
	byKind.set(p.kind, [...(byKind.get(p.kind) ?? []), p]);
}
console.log(`checked ${data.length} days; ${problems.length} problems`);
for (const [kind, list] of byKind) {
	console.log(`\n### ${kind} (${list.length})`);
	for (const p of list.slice(0, 3)) {
		console.log(`  day ${p.day}: ${p.detail}`);
	}
}
