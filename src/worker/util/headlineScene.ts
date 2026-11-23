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

// THE DRAFT-NIGHT BROADCAST'S PART (see court3d/DraftBroadcast.tsx): the
// class he came out of as it stood at his pick - who was up next, who was
// left on the board - and the true things said about him on the way to the
// stage.
const draftNight = async (p: Player, season: number) => {
	const at = { round: p.draft.round, pick: p.draft.pick };
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
	const t = await idb.cache.teams.get(p.draft.tid);
	if (!t) {
		return undefined;
	}
	const season = p.draft.year;
	const then = (await idb.merged.teamSeasons.getAll({ tid: t.tid })).find(
		(ts) => ts.season === season,
	);
	const ratings = p.ratings.find((r) => r.season === season) ?? p.ratings[0];
	return {
		type: "draftPick" as const,
		season,
		round: p.draft.round,
		pick: p.draft.pick,
		arena: await takeArenaLooks(t.tid, season),
		team: {
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
		},
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
