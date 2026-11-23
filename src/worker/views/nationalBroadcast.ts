import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import {
	biggestGame,
	networkFor,
	nightOf,
	strengthOf,
} from "../../common/nationalBroadcasts.ts";
import type {
	Game,
	NationalBroadcast,
	NationalTag,
	ScheduleGame,
} from "../../common/types.ts";
import { PHASE } from "../../common/constants.ts";
import { standingsAsOf } from "./liveGameInfo.ts";
import { PIC, pictureById } from "../util/courtPictures.ts";

// How each team was rated as the season began, 0 the worst to 1 the best.
const ratedOf = (teamSeasons: { tid: number; ovrStart?: number }[]) => {
	const ovrs = teamSeasons
		.filter((ts) => ts.ovrStart !== undefined)
		.sort((a, b) => a.ovrStart! - b.ovrStart!);
	return new Map(
		ovrs.map((ts, i) => [
			ts.tid,
			ovrs.length > 1 ? i / (ovrs.length - 1) : 0.5,
		]),
	);
};

const isRegular = (p: Game) =>
	!p.playoffs &&
	!(p as any).exhibition &&
	p.teams[0].tid >= 0 &&
	p.teams[1].tid >= 0;

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
		const regular = played.filter(isRegular);
		const night = regular
			.filter((p) => p.gid !== game.gid && p.day === game.day)
			.map((p) => ({ gid: p.gid, tids: [p.teams[0].tid, p.teams[1].tid] }))
			.concat([{ gid: game.gid, tids }]) as {
			gid: number;
			tids: [number, number];
		}[];

		const rated = ratedOf(
			await idb.merged.teamSeasons.getAll({ season: game.season }),
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

// THE NATIONAL GAMES TO COME, by gid (see networkFor): of the schedule, the
// games on a network. A night's biggest is told as nationalBroadcastOf will
// tell it once it's played, by the records as they stand - so the next
// night's is what it will be, and one further out can still change as the
// records do.
export const nationalSchedule = async (
	schedule: ScheduleGame[],
): Promise<Map<number, NationalBroadcast>> => {
	const out = new Map<number, NationalBroadcast>();
	const networks = g.get("nationalBroadcasts");
	if (!Array.isArray(networks) || networks.length === 0) {
		return out;
	}
	try {
		const season = g.get("season");
		const games = schedule.filter((s) => s.homeTid >= 0 && s.awayTid >= 0);
		if (g.get("phase") === PHASE.PLAYOFFS) {
			for (const s of games) {
				const n = networkFor(
					networks,
					{
						season,
						playoffs: true,
						finals: s.finals,
						tids: [s.homeTid, s.awayTid],
					},
					undefined,
					false,
				);
				if (n) {
					out.set(s.gid, n);
				}
			}
			return out;
		}

		const teamSeasons = await idb.merged.teamSeasons.getAll({ season });
		const rated = ratedOf(teamSeasons);
		const records = new Map(teamSeasons.map((ts) => [ts.tid, ts]));
		const strength = (tid: number) =>
			strengthOf(records.get(tid), rated.get(tid) ?? 0.5);

		const days = (await idb.cache.games.getAll())
			.filter((p) => p.season === season && isRegular(p))
			.map((p) => p.day)
			.concat(games.map((s) => s.day))
			.filter((d): d is number => d !== undefined);
		const openingDay = days.length > 0 ? Math.min(...days) : undefined;

		const nights = Map.groupBy(games, (s) => s.day);
		for (const night of nights.values()) {
			const biggest = biggestGame(
				night.map((s) => ({
					s,
					gid: s.gid,
					tids: [s.homeTid, s.awayTid] as [number, number],
				})),
				strength,
			);
			if (!biggest) {
				continue;
			}
			const n = networkFor(
				networks,
				{ season, tids: biggest.tids },
				nightOf(biggest.s, openingDay),
				true,
			);
			if (n) {
				out.set(biggest.gid, n);
			}
		}
	} catch {}
	return out;
};

// What a preview shows of a game's network: its name, and its logo ready to
// show. `logos`: the ones already looked up.
export const nationalTag = async (
	n: NationalBroadcast,
	logos = new Map<string, string | undefined>(),
): Promise<NationalTag> => {
	let logo = n.logo;
	if (logo?.startsWith(PIC)) {
		if (!logos.has(logo)) {
			let url;
			try {
				url = (await pictureById(logo.slice(PIC.length)))?.url;
			} catch {}
			logos.set(logo, url);
		}
		logo = logos.get(logo);
	}
	return { network: n.network, ...(logo !== undefined ? { logo } : {}) };
};

// The tags of these games that are national, by gid.
export const nationalTags = async (schedule: ScheduleGame[]) => {
	const out = new Map<number, NationalTag>();
	const logos = new Map<string, string | undefined>();
	for (const [gid, n] of await nationalSchedule(schedule)) {
		out.set(gid, await nationalTag(n, logos));
	}
	return out;
};
