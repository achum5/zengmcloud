import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import {
	biggestGame,
	networkFor,
	nightOf,
	strengthOf,
} from "../../common/nationalBroadcasts.ts";
import type { Game, NationalBroadcast } from "../../common/types.ts";
import { standingsAsOf } from "./liveGameInfo.ts";

// THE NATIONAL NETWORK A GAME IS ON, if it's on one (see networkFor): for
// the regular season, its night's games are all played by the time it's
// watched, so the night's biggest is known - by the records going into the
// night, and the teams' ratings as the season began. Cosmetic, so nothing
// when it can't be told.
export const nationalBroadcastOf = async (
	game: Pick<
		Game,
		"gid" | "season" | "day" | "date" | "playoffs" | "finals" | "teams"
	> & { exhibition?: boolean },
): Promise<NationalBroadcast | undefined> => {
	const networks = g.get("nationalBroadcasts");
	const tids: [number, number] = [game.teams[0].tid, game.teams[1].tid];
	if (
		!Array.isArray(networks) ||
		networks.length === 0 ||
		tids.some((tid) => tid < 0) ||
		game.exhibition
	) {
		return undefined;
	}
	try {
		const base = {
			season: game.season,
			playoffs: game.playoffs,
			finals: game.finals,
			tids,
		};
		if (game.playoffs) {
			return networkFor(networks, base, undefined, false);
		}
		const played = (await idb.merged.games.getAll({
			season: game.season,
		})) as Game[];
		const regular = played.filter(
			(p) =>
				!p.playoffs &&
				!(p as any).exhibition &&
				p.teams[0].tid >= 0 &&
				p.teams[1].tid >= 0,
		);
		const night = regular
			.filter((p) => p.gid !== game.gid && p.day === game.day)
			.map((p) => ({ gid: p.gid, tids: [p.teams[0].tid, p.teams[1].tid] }))
			.concat([{ gid: game.gid, tids }]) as {
			gid: number;
			tids: [number, number];
		}[];

		// How each team was rated as the season began, 0 the worst to 1 the
		// best.
		const teamSeasons = await idb.merged.teamSeasons.getAll({
			season: game.season,
		});
		const ovrs = teamSeasons
			.filter((ts) => ts.ovrStart !== undefined)
			.sort((a, b) => a.ovrStart! - b.ovrStart!);
		const rated = new Map(
			ovrs.map((ts, i) => [
				ts.tid,
				ovrs.length > 1 ? i / (ovrs.length - 1) : 0.5,
			]),
		);
		const records = standingsAsOf(game, regular, false);
		const strength = (tid: number) =>
			strengthOf(records.get(tid), rated.get(tid) ?? 0.5);

		const biggest = biggestGame(night, strength)?.gid === game.gid;
		const days = regular
			.map((p) => p.day)
			.filter((d): d is number => d !== undefined);
		const openingDay = days.length > 0 ? Math.min(...days) : game.day;
		return networkFor(networks, base, nightOf(game, openingDay), biggest);
	} catch {
		return undefined;
	}
};
