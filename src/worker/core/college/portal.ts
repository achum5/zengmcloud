import { PHASE, PLAYER } from "../../../common/constants.ts";
import { collegeFinalSeason } from "../../../common/college.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { player } from "../index.ts";
import { getNumPlayersPerTeam } from "../league/create/createRandomPlayers.ts";
import { collegeNilForPercentile, collegeStarsForPercentile } from "./util.ts";
import { baseInterest, getTeamCtxs, newRecruiting } from "./recruiting.ts";
import type { Conditions, Player } from "../../../common/types.ts";

// THE TRANSFER PORTAL
//
// At the end of the season, after the seniors and early entrants are gone,
// some players with eligibility left go looking for a new school: guys who
// barely played, and standouts at small programs who think they can play
// bigger. They're recruited over the offseason like high schoolers (same
// board, same offers and NIL), and sign as soon as they commit. Whoever is
// still unsigned when the preseason starts is a walk-on, if anyone has room.

const portalChance = (
	p: Player,
	mpg: number,
	rankOnTeam: number,
	prestige: number,
) => {
	let chance = 0.03;
	if (mpg < 8) {
		chance += 0.2;
	} else if (mpg < 15) {
		chance += 0.08;
	}
	// A star at a small school, hoping to move up.
	if (rankOnTeam <= 1 && prestige < 45) {
		chance += 0.12;
	}
	return chance;
};

export const collegeTransferPortal = async (conditions: Conditions) => {
	const season = g.get("season");
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);

	const entered: Player[] = [];
	const everyone: Player[] = [];
	for (const t of teams) {
		const roster = (
			await idb.cache.players.indexGetAll("playersByTid", t.tid)
		).sort((a, b) => b.value - a.value);
		everyone.push(...roster);
		for (const [rankOnTeam, p] of roster.entries()) {
			const final = collegeFinalSeason(p);
			if (final !== undefined && final <= season) {
				continue;
			}
			let min = 0;
			let gp = 0;
			for (const row of p.stats) {
				if (row.season === season && !row.playoffs) {
					min += row.min;
					gp += row.gp;
				}
			}
			const mpg = gp > 0 ? min / gp : 0;
			if (Math.random() < portalChance(p, mpg, rankOnTeam, t.prestige ?? 30)) {
				entered.push(p);
			}
		}
	}

	// Stars and asking price by where he ranks among every player in college.
	everyone.sort((a, b) => b.value - a.value);
	const pctByPid = new Map(
		everyone.map((p, i) => [p.pid, i / everyone.length]),
	);

	entered.sort((a, b) => b.value - a.value);
	const byTid = new Map<number, Player[]>();
	for (const [i, p] of entered.entries()) {
		const oldTid = p.tid;
		const pct = pctByPid.get(p.pid) ?? 1;
		p.recruiting = {
			...newRecruiting(
				Math.max(2, collegeStarsForPercentile(pct)),
				i + 1,
				collegeNilForPercentile(pct),
			),
			portalFrom: oldTid,
		};
		p.tid = PLAYER.FREE_AGENT;
		p.contract = { amount: p.recruiting.ask, exp: season };
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
export const collegePreseasonFill = async () => {
	const season = g.get("season");
	const freeAgents = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
	).sort((a, b) => b.value - a.value);
	const ctxs = await getTeamCtxs([]);
	const scholarships = getNumPlayersPerTeam();

	for (const ctx of ctxs.values()) {
		const roster = await idb.cache.players.indexGetAll(
			"playersByTid",
			ctx.t.tid,
		);
		ctx.open = Math.max(0, scholarships - roster.length);
	}

	for (const p of freeAgents) {
		const open = [...ctxs.values()].filter(
			(ctx) =>
				ctx.open > 0 &&
				!(ctx.user && !ctx.auto) &&
				p.recruiting?.portalFrom !== ctx.t.tid,
		);
		if (open.length === 0) {
			break;
		}
		if (!p.recruiting) {
			p.recruiting = newRecruiting(1, 0, 5);
		}
		open.sort(
			(a, b) =>
				baseInterest(p, b) +
				Math.random() * 10 -
				(baseInterest(p, a) + Math.random() * 10),
		);
		const ctx = open[0]!;
		ctx.open -= 1;
		await player.sign(
			p,
			ctx.t.tid,
			{ amount: Math.min(p.recruiting.ask, 25), exp: season },
			PHASE.PRESEASON,
		);
		delete p.recruiting;
		await idb.cache.players.put(p);
	}

	// The portal closes.
	for (const p of await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	)) {
		if (p.recruiting) {
			delete p.recruiting;
			await idb.cache.players.put(p);
		}
	}
};
