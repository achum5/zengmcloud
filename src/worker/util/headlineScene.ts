import { idb } from "../db/index.ts";
import { helpers } from "../../common/helpers.ts";
import { courtOf } from "../../common/stockCourts.ts";
import type { HeadlineSceneSpec } from "../../common/headlineScene.ts";
import { takeArenaLooks } from "../core/game/replayLooks.ts";
import { resolveCourt } from "./courtPictures.ts";
import { inferRaceFromFace } from "./realisticFaces.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";
import fuzzRating from "../core/player/fuzzRating.ts";
import valueCombineOvrPot from "../core/player/valueCombineOvrPot.ts";
import {
	bestAvailable,
	bigBoard,
	nextPicks,
	overallPick,
	type BoardMember,
	type Slot,
} from "../../common/draftNight.ts";
import type { Player } from "../../common/types.ts";
import { PHASE, PLAYER } from "../../common/constants.ts";
import getOrder from "../core/draft/getOrder.ts";
import g from "./g.ts";

// What the scouts made of a prospect going in: his ratings that year, as
// fuzzed as they were, his potential counted for as much as his age says
// (see player.value) - the big board's order.
const draftValue = (p: Player, season: number): number => {
	const r = p.ratings.find((x) => x.season === season) ?? p.ratings[0];
	return valueCombineOvrPot(
		fuzzRating(r.ovr, r.fuzz),
		fuzzRating(r.pot, r.fuzz),
		season - p.born.year,
	);
};

// A draft class on the board: each of them, where he went (or that he is
// still there), and every slot of the draft that is known - a man taken
// there, or (the draft still going) a pick still to be made.
const boardOf = async (season: number) => {
	const klass = await idb.merged.players.getAll({ draftYear: season });
	const members: BoardMember[] = klass
		.filter((q) => q.draft.year === season)
		.map((q) => {
			const r = q.ratings.find((x) => x.season === season) ?? q.ratings[0];
			return {
				pid: q.pid,
				name: `${q.firstName} ${q.lastName}`,
				pos: r.pos,
				college: q.college || undefined,
				round: q.draft.round,
				pick: q.draft.pick,
				value: draftValue(q, season),
			};
		});
	// Every slot of the draft that is known: a man taken there, or - the
	// draft still going - a pick still to be made.
	const slots: Slot[] = klass
		.filter((q) => q.draft.year === season && q.draft.round >= 1)
		.map((q) => ({
			round: q.draft.round,
			pick: q.draft.pick,
			tid: q.draft.tid,
		}));
	for (const dp of await idb.cache.draftPicks.getAll()) {
		if (dp.season === season && dp.pick >= 1) {
			slots.push({ round: dp.round, pick: dp.pick, tid: dp.tid });
		}
	}
	return { klass, members, slots };
};

// The teams up after a pick, as they were that year.
const upNext = async (
	slots: Slot[],
	at: { round: number; pick: number },
	season: number,
) => {
	const next = [];
	for (const s of nextPicks(slots, at, 6)) {
		const info = await getTeamInfoBySeason(s.tid, season);
		if (info) {
			next.push({
				overall: overallPick(slots, s),
				abbrev: info.abbrev,
				region: info.region,
				name: info.name,
				colors: info.colors,
				imgURL: info.imgURL,
				imgURLSmall: info.imgURLSmall,
			});
		}
	}
	return next;
};

// THE DRAFT-NIGHT BROADCAST'S PART (see court3d/DraftBroadcast.tsx): the
// class he came out of as it stood at his pick - who was up next, who was
// left on the board - and the true things said about him on the way to the
// stage.
const draftNight = async (p: Player, season: number) => {
	const at = { round: p.draft.round, pick: p.draft.pick };
	const { members, slots } = await boardOf(season);
	const next = await upNext(slots, at, season);
	const via =
		p.draft.originalTid !== p.draft.tid
			? await getTeamInfoBySeason(p.draft.originalTid, season)
			: undefined;
	return {
		overall: overallPick(slots, at),
		rank: bigBoard(members).get(p.pid),
		best: bestAvailable(members, at, 5),
		next,
		via: via ? `${via.region} ${via.name}` : undefined,
		age: season - p.born.year,
		hgt: p.hgt,
		weight: p.weight,
		college: p.college || undefined,
		born: p.born.loc || undefined,
		relatives: p.relatives
			.filter((r) => r.type === "father" || r.type === "brother")
			.map((r) => ({ type: r.type, name: r.name })),
	};
};

// A team as it looked in a season: its colors, logo and floor.
const teamThen = async (tid: number, season: number) => {
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		return undefined;
	}
	const then = (await idb.merged.teamSeasons.getAll({ tid: t.tid })).find(
		(ts) => ts.season === season,
	);
	return {
		tid: t.tid,
		abbrev: then?.abbrev ?? t.abbrev,
		region: then?.region ?? t.region,
		name: then?.name ?? t.name,
		colors: then?.colors ?? t.colors,
		jersey: then?.jersey ?? t.jersey,
		imgURL: then?.imgURL ?? t.imgURL,
		imgURLSmall: then?.imgURLSmall ?? t.imgURLSmall,
		jerseySkins: t.jerseySkins,
		// (Its own court, or its stock floor - see stockCourts - with its
		// uploaded pictures filled in, to draw.)
		court: await resolveCourt(courtOf(t)),
	};
};

// WHAT A HEADLINE'S SCENE IS MADE FROM (see HeadlineScene.tsx): for a draft
// pick, the team that took him as it looked that year - its colors, logo
// and floor, its building - and where he went. Nothing, if there is no scene
// to make of it (he went undrafted, his team is gone).
export const headlineScene = async (spec: HeadlineSceneSpec) => {
	if (__SPORT !== "basketball" || spec.type !== "draftPick") {
		return undefined;
	}
	const p = await idb.merged.players.get(spec.pid);
	// (A player whose face is a photo is a silhouette on the court: no
	// scene of him.)
	if (!p || p.draft.round < 1 || p.draft.tid < 0 || p.imgURL) {
		return undefined;
	}
	const season = p.draft.year;
	const team = await teamThen(p.draft.tid, season);
	if (!team) {
		return undefined;
	}
	const ratings = p.ratings.find((r) => r.season === season) ?? p.ratings[0];
	return {
		type: "draftPick" as const,
		season,
		round: p.draft.round,
		pick: p.draft.pick,
		arena: await takeArenaLooks(team.tid, season),
		team,
		player: {
			pid: p.pid,
			name: `${p.firstName} ${p.lastName}`,
			pos: ratings?.pos,
			jerseyNumber: helpers.getJerseyNumber(p),
			// (His family at his table looking like his family.)
			race: p.face ? inferRaceFromFace(p.face) : undefined,
		},
		draftNight: await draftNight(p, season),
	};
};

// THE ROOM BETWEEN PICKS (see court3d/ClockStage.tsx): the team on the clock
// as it looks this year, the picks after it, the best still on the board -
// and, for the cameras to find at their tables, the first few of those. Only
// a draft of this year's class, still going.
export const draftClock = async () => {
	if (__SPORT !== "basketball" || g.get("phase") !== PHASE.DRAFT) {
		return undefined;
	}
	const dp = (await getOrder())[0];
	if (!dp || dp.pick < 1) {
		return undefined;
	}
	const season = g.get("season");
	const team = await teamThen(dp.tid, season);
	if (!team) {
		return undefined;
	}
	const at = { round: dp.round, pick: dp.pick };
	const { klass, members, slots } = await boardOf(season);
	const board = bigBoard(members);
	const left = klass
		.filter((q) => q.draft.year === season && q.tid === PLAYER.UNDRAFTED)
		.sort(
			(a, b) => (board.get(a.pid) ?? Infinity) - (board.get(b.pid) ?? Infinity),
		);
	return {
		season,
		round: dp.round,
		pick: dp.pick,
		overall: overallPick(slots, at),
		arena: await takeArenaLooks(team.tid, season),
		team,
		next: await upNext(slots, at, season),
		best: bestAvailable(members, at, 5),
		// (A face that is a photo is a silhouette in the room: not one of
		// those the cameras go to.)
		prospects: left
			.filter((q) => !q.imgURL)
			.slice(0, 3)
			.map((q) => {
				const r = q.ratings.find((x) => x.season === season) ?? q.ratings[0];
				return {
					pid: q.pid,
					name: `${q.firstName} ${q.lastName}`,
					pos: r?.pos,
					college: q.college || undefined,
					rank: board.get(q.pid),
					race: q.face ? inferRaceFromFace(q.face) : undefined,
				};
			}),
	};
};
