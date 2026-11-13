import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import type { ViewArgs } from "../util/defineView.ts";
import { processEvents } from "./news.ts";

// The new interface's home (the League Hub), on top of the dashboard: the
// inbox with subjects, the team's own news, its schedule with results, the
// league's latest scores and the season's key dates.

const plain = (html: string) =>
	html
		.replaceAll(/<[^>]*>/g, " ")
		.replaceAll("&nbsp;", " ")
		.replaceAll(/\s+/g, " ")
		.trim();

// Which of these games have a replay saved (see game/play.ts for when one
// is): watched live, or kept by the league's Auto-Save Replays setting.
const savedReplays = async (gids: number[]) => {
	const saved = new Set<number>();
	if (gids.length === 0) {
		return saved;
	}
	for (const gid of gids) {
		if (await idb.cache.liveGamePlayByPlay.get(gid)) {
			saved.add(gid);
		}
	}
	try {
		const keys: number[] = await (idb.league as any).getAllKeys(
			"liveGamePlayByPlay",
			IDBKeyRange.bound(Math.min(...gids), Math.max(...gids)),
		);
		for (const gid of keys) {
			if (gids.includes(gid)) {
				saved.add(gid);
			}
		}
	} catch {
		// Only what the cache holds, then.
	}
	return saved;
};

export const updateHub = async ({ updateEvents }: ViewArgs) => {
	if (
		!updateEvents.has("firstRun") &&
		!updateEvents.has("gameSim") &&
		!updateEvents.has("newPhase") &&
		!updateEvents.has("playerMovement")
	) {
		return;
	}

	const userTid = g.get("userTid");
	const season = g.get("season");

	const hubMessages = (await idb.merged.messages.getAll({ limit: 12 }))
		.reverse()
		.map((message) => ({
			mid: message.mid,
			from: message.from,
			subject: message.subject ?? "",
			read: message.read,
			year: message.year,
			preview: plain(message.text).slice(0, 180),
		}));

	const eventsAll = await idb.merged.events.getAll({ season });
	eventsAll.reverse();
	const teamNews = (
		await processEvents(eventsAll, { level: "all", limit: 16, tid: userTid })
	).map((event) => ({
		eid: event.eid,
		type: event.type,
		text: event.text,
	}));

	const schedule = await idb.cache.schedule.getAll();
	const nextDay =
		schedule.length > 0 ? Math.min(...schedule.map((s) => s.day)) : undefined;
	const upcoming = schedule
		.filter((s) => s.homeTid === userTid || s.awayTid === userTid)
		.sort((a, b) => a.day - b.day)
		.slice(0, 12)
		.map((s) => ({
			day: s.day,
			home: s.homeTid === userTid,
			opp: s.homeTid === userTid ? s.awayTid : s.homeTid,
		}));
	const keyDates = schedule
		.filter((s) => s.homeTid < 0)
		.map((s) => ({
			day: s.day,
			type:
				s.homeTid === -3 ? ("tradeDeadline" as const) : ("allStar" as const),
		}));

	// The cache holds every game of the current season.
	const games = (await idb.cache.games.getAll()).filter(
		(game) => game.season === season,
	);
	const played = games
		.filter((game) => game.teams.some((t) => t.tid === userTid))
		.sort((a, b) => (a.day ?? 0) - (b.day ?? 0) || a.gid - b.gid)
		.slice(-12)
		.map((game) => {
			const me = game.teams[0].tid === userTid ? 0 : 1;
			const them = me === 0 ? 1 : 0;
			return {
				gid: game.gid,
				day: game.day,
				home: me === 0,
				opp: game.teams[them].tid,
				pts: game.teams[me].pts,
				oppPts: game.teams[them].pts,
				playoffs: game.playoffs,
			};
		});
	const lastDay = games.reduce(
		(max, game) => Math.max(max, game.day ?? -Infinity),
		-Infinity,
	);
	const lastGames = games.filter((game) => game.day === lastDay);
	const replays = await savedReplays(lastGames.map((game) => game.gid));
	const mine = (game: (typeof lastGames)[number]) =>
		game.teams.some((t) => t.tid === userTid);
	const margin = (game: (typeof lastGames)[number]) =>
		Math.abs(game.teams[0].pts - game.teams[1].pts);
	// The user's game first, then the ones with highlights to watch, closest
	// first.
	const latestScores = lastGames
		.map((game, i) => ({ game, i }))
		.sort(
			(a, b) =>
				Number(mine(b.game)) - Number(mine(a.game)) ||
				Number(replays.has(b.game.gid)) - Number(replays.has(a.game.gid)) ||
				(replays.has(a.game.gid) ? margin(a.game) - margin(b.game) : 0) ||
				a.i - b.i,
		)
		.slice(0, 8)
		.map(({ game }) => ({
			gid: game.gid,
			home: { tid: game.teams[0].tid, pts: game.teams[0].pts },
			away: { tid: game.teams[1].tid, pts: game.teams[1].pts },
			replay: replays.has(game.gid),
		}));

	const numRoster = (
		await idb.cache.players.indexGetAll("playersByTid", userTid)
	).length;

	return {
		numRoster,
		maxRosterSize: g.get("maxRosterSize"),
		minRosterSize: g.get("minRosterSize"),
		hubMessages,
		teamNews,
		upcoming,
		played,
		keyDates,
		nextDay,
		latestScores,
	};
};
