import { PHASE } from "../../common/constants.ts";
import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import { processAssets } from "../views/tradeSummary.ts";
import { getTeamInfoBySeason } from "../util/getTeamInfoBySeason.ts";
import type { DiscriminateUnion, EventBBGM, Phase } from "../../common/types.ts";

// A plain-text dump of every trade over the last N seasons, rich enough to judge
// whether the CPU trade AI is behaving like real life: for each side it shows the
// team's record at the time (contender vs. rebuilder), exactly what talent / age /
// contract / picks it took on, a short win% trajectory over the following seasons
// (did the move pay off?), and the retrospective win shares each received asset
// went on to produce for that team (the "who won the trade" ground truth). Copied
// from the Transactions page for pasting into a review.

type TradeEvent = DiscriminateUnion<EventBBGM, "type", "trade">;

const phaseLabel = (phase: Phase): string => {
	switch (phase) {
		case PHASE.PRESEASON:
			return "preseason";
		case PHASE.REGULAR_SEASON:
			return "in-season";
		case PHASE.AFTER_TRADE_DEADLINE:
			return "post-deadline";
		case PHASE.PLAYOFFS:
			return "playoffs";
		case PHASE.DRAFT_LOTTERY:
			return "lottery";
		case PHASE.DRAFT:
			return "draft";
		case PHASE.AFTER_DRAFT:
			return "after-draft";
		case PHASE.RESIGN_PLAYERS:
			return "re-sign";
		case PHASE.FREE_AGENCY:
			return "free-agency";
		default:
			return `phase${phase}`;
	}
};

// Team abbrev as of a given season (falls back to the current cache / "???").
const getTeamAbbrev = async (tid: number, season: number): Promise<string> => {
	const info = await getTeamInfoBySeason(tid, season);
	return info?.abbrev ?? g.get("teamInfoCache")[tid]?.abbrev ?? "???";
};

// A team's record + win% for one season, or undefined if it didn't play then.
const seasonRecord = async (tid: number, season: number) => {
	const ts = await idb.getCopy.teamSeasons({ season, tid }, "noCopyCache");
	if (!ts || (ts.won === 0 && ts.lost === 0 && ts.tied === 0 && ts.otl === 0)) {
		return undefined;
	}
	return { won: ts.won, lost: ts.lost, winp: helpers.calcWinp(ts) };
};

// "0.512→0.548→0.601" over [season .. season+2], skipping seasons not yet played.
const trajectory = async (tid: number, season: number): Promise<string> => {
	const parts: string[] = [];
	for (let s = season; s <= season + 2; s++) {
		const rec = await seasonRecord(tid, s);
		parts.push(rec ? rec.winp.toFixed(3) : "—");
	}
	return parts.join("→");
};

type Asset = Awaited<ReturnType<typeof processAssets>>[number];

const outcomeNote = (a: Asset): string => {
	if (a.type === "deletedPlayer" || a.type === "unrealizedPick") {
		return "";
	}
	const o = a.outcome;
	if (!o) {
		return "";
	}
	switch (o.type) {
		case "stillOnTeam":
			return " [kept]";
		case "retired":
			return ` [retired ${o.season}]`;
		case "trade":
			return ` [traded ${o.season}→${o.abbrev}]`;
		case "freeAgent":
			return ` [left FA ${o.season}]`;
		case "sisyphus":
		case "godMode":
			return ` [${o.type} ${o.season}]`;
		case "tradeBeforeDraft":
			return " [traded pre-draft]";
		default:
			return "";
	}
};

const fmtAsset = (a: Asset): string => {
	if (a.type === "player") {
		return `${a.name} (${a.age}yo ${a.pos}, ${a.ovr}ovr/${a.pot}pot, $${a.contract.amount}/'${a.contract.exp}) →${a.statTeam.toFixed(1)}WS${outcomeNote(a)}`;
	}
	if (a.type === "deletedPlayer") {
		return `${a.name} (deleted, $${a.contract.amount}/'${a.contract.exp})`;
	}
	const via = a.abbrev ? ` via ${a.abbrev}` : "";
	if (a.type === "realizedPick") {
		return `${a.season} R${a.round}${via} pick #${a.pick} → ${a.name} (${a.age}yo ${a.pos}, ${a.ovr}ovr/${a.pot}pot) →${a.statTeam.toFixed(1)}WS${outcomeNote(a)}`;
	}
	// unrealizedPick
	return `${a.season} R${a.round}${via} pick`;
};

// Decision-time summary of what a side took on: how much talent (summed ovr),
// average age, how many picks, total incoming salary, and the WS it ultimately got.
const sideSummary = (assets: Asset[]): string => {
	let players = 0;
	let talent = 0;
	let ageSum = 0;
	let picks = 0;
	let salary = 0;
	let wsOut = 0;
	for (const a of assets) {
		if (a.type === "player" || a.type === "realizedPick") {
			players += 1;
			talent += a.ovr;
			ageSum += a.age;
			wsOut += a.statTeam;
		}
		if (a.type === "player" || a.type === "deletedPlayer") {
			salary += a.contract.amount;
		}
		if (a.type === "realizedPick" || a.type === "unrealizedPick") {
			picks += 1;
		}
	}
	const avgAge = players > 0 ? (ageSum / players).toFixed(1) : "—";
	return `${players}plr talent=${talent} avgAge=${avgAge} picks=${picks} $in=${salary} → ${wsOut.toFixed(1)}WS`;
};

export const getTradeHistoryDump = async (numSeasons = 5): Promise<string> => {
	const currentSeason = g.get("season");
	const startSeason = currentSeason - numSeasons + 1;
	const userTid = g.get("userTid");

	// Pull trade events for the window, season by season (bounds reads instead of
	// scanning the whole event log of a long-running league).
	const events: TradeEvent[] = [];
	for (let s = startSeason; s <= currentSeason; s++) {
		const seasonEvents = await idb.getCopies.events({ season: s }, "noCopyCache");
		for (const event of seasonEvents) {
			if (
				event.type === "trade" &&
				event.teams &&
				event.phase !== undefined &&
				event.tids.length === 2
			) {
				events.push(event as TradeEvent);
			}
		}
	}
	// Newest first.
	events.sort((a, b) => b.eid - a.eid);

	const lines: string[] = [];
	lines.push(
		`=== TRADE HISTORY — seasons ${startSeason}-${currentSeason} (${events.length} trades) ===`,
	);
	lines.push(
		"Per side: [record at trade] win% trajectory (this→+1→+2 seasons); assets show ovr/pot & contract at trade time, then WS produced for the receiving team.",
	);
	lines.push("");

	let n = 0;
	for (const event of events) {
		n += 1;
		try {
			await appendTrade(lines, event, n, userTid);
		} catch (error) {
			lines.push(`#${n} [${event.season}] (could not reconstruct: ${(error as Error).message})`);
			lines.push("");
		}
	}

	if (events.length === 0) {
		lines.push("(No trades found in this window.)");
	}

	return lines.join("\n");
};

// Reconstruct one trade and append its block to `lines`.
const appendTrade = async (
	lines: string[],
	event: TradeEvent,
	n: number,
	userTid: number,
) => {
	{
		const [tidA, tidB] = event.tids as [number, number];
		const phase = event.phase!;

		const recA = await seasonRecord(tidA, event.season);
		const recB = await seasonRecord(tidB, event.season);
		const infoA = await getTeamAbbrev(tidA, event.season);
		const infoB = await getTeamAbbrev(tidB, event.season);
		const trajA = await trajectory(tidA, event.season);
		const trajB = await trajectory(tidB, event.season);

		const assetsA = await processAssets(event, 0);
		const assetsB = await processAssets(event, 1);

		const flag =
			tidA === userTid || tidB === userTid ? "  ⟨involves your team⟩" : "";
		const recAStr = recA
			? `${recA.won}-${recA.lost} ${recA.winp.toFixed(3)}`
			: "no record";
		const recBStr = recB
			? `${recB.won}-${recB.lost} ${recB.winp.toFixed(3)}`
			: "no record";

		lines.push(
			`#${n} [${event.season} ${phaseLabel(phase)}] ${infoA} (${recAStr}) ↔ ${infoB} (${recBStr})${flag}`,
		);
		lines.push(`   ${infoA} traj ${trajA} | ${infoB} traj ${trajB}`);
		// The AI's own reasoning, stamped at the moment of the deal (newer trades).
		if (event.aiTrade) {
			const ai = event.aiTrade;
			const initAbbrev = ai.initiatorTid === tidA ? infoA : infoB;
			lines.push(
				`   AI: ${infoA}=${ai.tiers[0]} ${infoB}=${ai.tiers[1]} init=${initAbbrev} dv=${ai.dv} why=${ai.motivation}`,
			);
		}
		lines.push(
			`   ${infoA} gets: ${assetsA.length ? assetsA.map(fmtAsset).join(" | ") : "nothing"}`,
		);
		lines.push(`     └ ${sideSummary(assetsA)}`);
		lines.push(
			`   ${infoB} gets: ${assetsB.length ? assetsB.map(fmtAsset).join(" | ") : "nothing"}`,
		);
		lines.push(`     └ ${sideSummary(assetsB)}`);
		lines.push("");
	}
};

export default getTradeHistoryDump;
