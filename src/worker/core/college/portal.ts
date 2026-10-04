import { PHASE, PLAYER } from "../../../common/constants.ts";
import { collegeFinalSeason } from "../../../common/college.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { player } from "../index.ts";
import { collegeNilForPercentile, collegeStarsForPercentile } from "./util.ts";
import { newRecruiting } from "./recruiting.ts";
import { roundNil } from "./negotiation.ts";
import { baseInterest, getTeamCtxs } from "./teams.ts";
import type { Conditions, Player } from "../../../common/types.ts";

// THE TRANSFER PORTAL
//
// When the retention period ends, each returning player rolls against his
// portal risk (see retention.ts). Those who go are recruited over the
// offseason weeks like high schoolers - same board, same negotiation - and
// sign as soon as they commit. His old school can recruit him back.
// Whoever is still unsigned when the preseason starts is a walk-on, if
// anyone has room for him.

export const collegeOpenPortal = async (conditions: Conditions) => {
	const season = g.get("season");
	const nilScale = g.get("collegeNilScale");
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);

	// Stars and asking price by where he ranks among every player in college.
	const everyone = [...players].sort((a, b) => b.value - a.value);
	const pctByPid = new Map(
		everyone.map((p, i) => [p.pid, i / everyone.length]),
	);

	const entered: Player[] = [];
	for (const p of players) {
		const r = p.collegeRetention;
		if (r?.season === season && Math.random() < r.risk) {
			entered.push(p);
		}
		if (r) {
			delete p.collegeRetention;
			await idb.cache.players.put(p);
		}
	}

	entered.sort((a, b) => b.value - a.value);
	const byTid = new Map<number, Player[]>();
	for (const [i, p] of entered.entries()) {
		const oldTid = p.tid;
		const pct = pctByPid.get(p.pid) ?? 1;
		p.recruiting = {
			...newRecruiting(
				Math.max(2, collegeStarsForPercentile(pct)),
				i + 1,
				roundNil(collegeNilForPercentile(pct) * nilScale),
			),
			portalFrom: oldTid,
		};
		// His old school knows exactly what he is.
		p.recruiting.scout[oldTid] = 1000;
		// Promises for next season don't follow him.
		p.collegePromises = (p.collegePromises ?? []).filter(
			(promise) => promise.season <= season,
		);
		p.tid = PLAYER.FREE_AGENT;
		p.contract = { amount: p.recruiting.ask, exp: season + 1 };
		p.transactions ??= [];
		await idb.cache.players.put(p);

		const list = byTid.get(oldTid) ?? [];
		list.push(p);
		byTid.set(oldTid, list);
	}

	for (const [tid, list] of byTid) {
		logEvent(
			{
				type: "release",
				text: list
					.map(
						(p) =>
							`<a href="${helpers.leagueUrl(["player", p.pid])}">${p.firstName} ${p.lastName}</a> entered the transfer portal.`,
					)
					.join("<br>"),
				showNotification: tid === g.get("userTid"),
				pids: list.map((p) => p.pid),
				tids: [tid],
				saveToDb: false,
			},
			conditions,
		);
	}
};

// Preseason: schools still short of a full roster take the best players left
// in the portal or among the walk-ons, and the portal closes.
export const collegePreseasonFill = async (conditions: Conditions) => {
	const season = g.get("season");
	const freeAgents = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
	).sort((a, b) => b.value - a.value);
	const ctxs = await getTeamCtxs([]);

	for (const p of freeAgents) {
		const open = [...ctxs.values()].filter(
			(ctx) =>
				ctx.open > 0 &&
				!(ctx.user && !ctx.auto) &&
				p.recruiting?.portalFrom !== ctx.tid,
		);
		if (open.length === 0) {
			break;
		}
		const scored = open.map((ctx) => ({
			ctx,
			score: baseInterest(p, ctx, 5) + Math.random() * 10,
		}));
		scored.sort((a, b) => b.score - a.score);
		const ctx = scored[0]!.ctx;
		ctx.open -= 1;
		await player.sign(
			p,
			ctx.tid,
			{ amount: 5, exp: season + 4 },
			PHASE.PRESEASON,
		);
		delete p.recruiting;
		await idb.cache.players.put(p);
	}

	// The portal closes. Walk-ons who have gone a whole year unsigned, or are
	// out of eligibility, move on.
	for (const p of await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	)) {
		const final = collegeFinalSeason(p);
		if (p.yearsFreeAgent >= 1 || (final !== undefined && final <= season)) {
			await player.retire(p, conditions, { logRetiredEvent: false });
			await idb.cache.players.put(p);
		} else if (p.recruiting) {
			delete p.recruiting;
			await idb.cache.players.put(p);
		}
	}
};
