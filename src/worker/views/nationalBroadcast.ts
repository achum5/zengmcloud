import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import {
	nationalNight,
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
import { PIC, pictureById } from "../util/courtPictures.ts";
import { GENERIC_NATIONAL } from "../../common/genericNational.ts";

// The league's networks: its team looks file's, or without them, ours (see
// GENERIC_NATIONAL).
const nationalNetworks = (): NationalBroadcast[] => {
	const networks = g.get("nationalBroadcasts");
	if (Array.isArray(networks) && networks.length > 0) {
		return networks;
	}
	return __SPORT === "basketball" ? [GENERIC_NATIONAL] : [];
};

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

// Two teams of one division, by their team seasons.
const rivalsBy =
	(teamSeasons: { tid: number; cid: number; did: number }[]) =>
	(a: number, b: number) => {
		const ta = teamSeasons.find((ts) => ts.tid === a);
		const tb = teamSeasons.find((ts) => ts.tid === b);
		return !!ta && !!tb && ta.cid === tb.cid && ta.did === tb.did;
	};

// The season's first night, of the dates given.
const firstDate = (dates: (string | undefined)[]) => {
	let first: string | undefined;
	for (const date of dates) {
		if (date !== undefined && (first === undefined || date < first)) {
			first = date;
		}
	}
	return first;
};

const isRegular = (p: Game) =>
	!p.playoffs &&
	!(p as any).exhibition &&
	p.teams[0].tid >= 0 &&
	p.teams[1].tid >= 0;

type NightGame = {
	gid: number;
	day: number | undefined;
	date?: string;
	tids: [number, number];
};

// THE REGULAR SEASON'S NATIONAL GAMES, by gid (see nationalNight), of the
// nights `wanted`: every night's games, played and still to play - a game
// watched live is played alone, the rest of its night after (see
// actions.liveGame), so the night is never just what's been played, or just
// what's left. Each team as it stood going into the night. The one source
// for a game's network, on the schedule as in the game. (`extra`: games
// known besides - one a follower is handed before its own games have it,
// the schedule a view was given.)
const regularSeasonPicks = async (
	season: number,
	wanted: (day: number | undefined) => boolean,
	extra: NightGame[] = [],
): Promise<Map<number, NationalBroadcast>> => {
	const out = new Map<number, NationalBroadcast>();
	const networks = nationalNetworks();
	if (networks.length === 0) {
		return out;
	}
	const played = ((await idb.merged.games.getAll({ season })) as Game[]).filter(
		isRegular,
	);
	const games = new Map<number, NightGame>();
	for (const p of played) {
		games.set(p.gid, {
			gid: p.gid,
			day: p.day,
			date: p.date,
			tids: [p.teams[0].tid, p.teams[1].tid],
		});
	}
	if (
		season === g.get("season") &&
		g.get("phase") >= PHASE.REGULAR_SEASON &&
		g.get("phase") < PHASE.PLAYOFFS
	) {
		for (const s of await idb.cache.schedule.getAll()) {
			if (s.homeTid >= 0 && s.awayTid >= 0 && !games.has(s.gid)) {
				games.set(s.gid, {
					gid: s.gid,
					day: s.day,
					date: s.date,
					tids: [s.homeTid, s.awayTid],
				});
			}
		}
	}
	for (const p of extra) {
		if (!games.has(p.gid)) {
			games.set(p.gid, p);
		}
	}
	const all = [...games.values()];

	const teamSeasons = await idb.merged.teamSeasons.getAll({ season });
	const rated = ratedOf(teamSeasons);
	const rivals = rivalsBy(teamSeasons);
	const days = all
		.map((p) => p.day)
		.filter((d): d is number => d !== undefined);
	const openingDay = days.length > 0 ? Math.min(...days) : undefined;
	const openingDate = firstDate(all.map((p) => p.date));

	// Going into a night: what was played before it.
	const recordsInto = (day: number | undefined) => {
		const records = new Map<
			number,
			{ won: number; lost: number; tied: number }
		>();
		for (const p of played) {
			if (day !== undefined && (p.day === undefined || p.day >= day)) {
				continue;
			}
			for (const i of [0, 1] as const) {
				const tid = p.teams[i].tid;
				const r = records.get(tid) ?? { won: 0, lost: 0, tied: 0 };
				records.set(tid, r);
				const diff = p.teams[i].pts - p.teams[i === 0 ? 1 : 0].pts;
				if (diff > 0) {
					r.won += 1;
				} else if (diff < 0) {
					r.lost += 1;
				} else {
					r.tied += 1;
				}
			}
		}
		return records;
	};

	const nights = Map.groupBy(
		all.filter((p) => wanted(p.day)),
		(p) => p.day,
	);
	for (const [day, night] of nights) {
		const records = recordsInto(day);
		const strength = (tid: number) =>
			strengthOf(records.get(tid), rated.get(tid) ?? 0.5);
		const date = night.find((p) => p.date !== undefined)?.date;
		const picks = nationalNight(
			networks,
			night,
			{
				date,
				night: nightOf({ date, day }, openingDay),
				openingDate,
			},
			strength,
			rivals,
		);
		for (const [gid, n] of picks) {
			out.set(gid, n);
		}
	}
	return out;
};

// THE NATIONAL NETWORK A GAME IS ON, if it's on one (see
// regularSeasonPicks; networkFor in the playoffs). Cosmetic, so nothing
// when it can't be told.
export const nationalBroadcastOf = async (
	game: Pick<
		Game,
		"gid" | "season" | "day" | "date" | "playoffs" | "finals" | "teams"
	> & { exhibition?: boolean },
): Promise<NationalBroadcast | undefined> => {
	const networks = nationalNetworks();
	const tids: [number, number] = [game.teams[0].tid, game.teams[1].tid];
	if (networks.length === 0 || tids.some((tid) => tid < 0) || game.exhibition) {
		return undefined;
	}
	try {
		if (game.playoffs) {
			return networkFor(
				networks,
				{
					season: game.season,
					playoffs: game.playoffs,
					finals: game.finals,
					tids,
				},
				undefined,
				false,
			);
		}
		const picks = await regularSeasonPicks(
			game.season,
			(day) => day === game.day,
			[{ gid: game.gid, day: game.day, date: game.date, tids }],
		);
		return picks.get(game.gid);
	} catch {
		return undefined;
	}
};

// THE NATIONAL GAMES TO COME, by gid: of the schedule, the games on a
// network - told as the game itself will tell them (regularSeasonPicks), so
// the next night's are what they will be, and one further out can still
// change as the records do.
export const nationalSchedule = async (
	schedule: ScheduleGame[],
): Promise<Map<number, NationalBroadcast>> => {
	const out = new Map<number, NationalBroadcast>();
	const networks = nationalNetworks();
	if (networks.length === 0) {
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

		const days = new Set(games.map((s) => s.day));
		const picks = await regularSeasonPicks(
			season,
			(day) => day !== undefined && days.has(day),
			games.map((s) => ({
				gid: s.gid,
				day: s.day,
				date: s.date,
				tids: [s.homeTid, s.awayTid],
			})),
		);
		for (const s of games) {
			const n = picks.get(s.gid);
			if (n) {
				out.set(s.gid, n);
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
	const ready = async (logo: string | undefined) => {
		if (logo?.startsWith(PIC)) {
			if (!logos.has(logo)) {
				let url;
				try {
					url = (await pictureById(logo.slice(PIC.length)))?.url;
				} catch {}
				logos.set(logo, url);
			}
			return logos.get(logo);
		}
		return logo;
	};
	const logo = await ready(n.logo);
	const logoDark = await ready(n.logoDark);
	return {
		network: n.network,
		...(logo !== undefined ? { logo } : {}),
		...(logoDark !== undefined ? { logoDark } : {}),
	};
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
