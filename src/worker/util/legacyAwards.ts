import type { Awards } from "../../common/types.ts";
import { idb } from "../db/index.ts";
import g from "./g.ts";

// THE OLD SHAPE OF AN AWARDS ROW, READ OFF THE NEW ONE.
//
// Awards are now a list of user-defined awards (see common/types.ts), each with
// a shortName, a formula and a winner list of pids. Several things in this fork
// were written against the row as it used to be - `awards.mvp`, `awards.allLeague`
// - and mean exactly what they say: the league's MVP, its All-League teams. So
// they are read off the new list by the identity every default award still
// carries (shortName, actAs, numTeams), and a custom league with its own award
// list gets whatever of those it kept and nothing for what it dropped.
//
// League-level awards only: a conference MVP is a different award.

export type LegacyAwardWinner = {
	pid: number;
	tid: number;
	name: string;
	abbrev?: string;
};

export type LegacyAwards = {
	season: number;
	mvp?: LegacyAwardWinner;
	finalsMvp?: LegacyAwardWinner;
	dpoy?: LegacyAwardWinner;
	smoy?: LegacyAwardWinner;
	mip?: LegacyAwardWinner;
	roy?: LegacyAwardWinner;
	allLeague: { title: string; players: LegacyAwardWinner[] }[];
	allDefensive: { title: string; players: LegacyAwardWinner[] }[];
	allRookie: LegacyAwardWinner[];
};

const INDIVIDUAL: Record<string, keyof LegacyAwards> = {
	MVP: "mvp",
	FMVP: "finalsMvp",
	DPOY: "dpoy",
	SMOY: "smoy",
	MIP: "mip",
	ROY: "roy",
};

const TEAM_TITLES = ["First Team", "Second Team", "Third Team"];

type Ref = { pid: number; tid: number };

// The pids and tids, before any names are looked up.
const legacyRefs = (awards: Awards) => {
	const out: {
		season: number;
		individual: Partial<Record<keyof LegacyAwards, Ref>>;
		allLeague: Ref[][];
		allDefensive: Ref[][];
		allRookie: Ref[];
	} = {
		season: awards.season,
		individual: {},
		allLeague: [],
		allDefensive: [],
		allRookie: [],
	};

	for (const award of awards.awards) {
		if (award.group !== undefined) {
			continue;
		}
		if (award.numTeams === undefined) {
			// A renamed award keeps its role through actAs; an unchanged one
			// through its shortName.
			const key =
				INDIVIDUAL[award.shortName] ??
				(award.actAs === "mvp"
					? "mvp"
					: award.actAs === "roy"
						? "roy"
						: undefined);
			if (!key || out.individual[key]) {
				continue;
			}
			const winner = award.winner[0];
			if (winner && winner.pid !== undefined && winner.tid !== undefined) {
				out.individual[key] = { pid: winner.pid, tid: winner.tid };
			}
			continue;
		}

		const teams = award.winner.map((team) =>
			team
				.filter(
					(p): p is Ref & { pos?: string } =>
						p.pid !== undefined && p.tid !== undefined,
				)
				.map((p) => ({ pid: p.pid, tid: p.tid })),
		);
		if (award.shortName === "ALL" && out.allLeague.length === 0) {
			out.allLeague = teams;
		} else if (award.shortName === "DEF" && out.allDefensive.length === 0) {
			out.allDefensive = teams;
		} else if (award.shortName === "ALR" && out.allRookie.length === 0) {
			out.allRookie = teams.flat();
		}
	}

	return out;
};

// The row with every winner named, for prose and cards. One player lookup per
// distinct pid; a pid the league no longer has (deleted player) is dropped.
export const legacyAwardsWithNames = async (
	awards: Awards,
): Promise<LegacyAwards> => {
	const refs = legacyRefs(awards);
	const pids = new Set<number>();
	for (const ref of Object.values(refs.individual)) {
		pids.add(ref.pid);
	}
	for (const team of [...refs.allLeague, ...refs.allDefensive]) {
		for (const ref of team) {
			pids.add(ref.pid);
		}
	}
	for (const ref of refs.allRookie) {
		pids.add(ref.pid);
	}

	const names = new Map<number, string>();
	for (const pid of pids) {
		const p = await idb.getCopy.players({ pid }, "noCopyCache");
		if (p) {
			names.set(pid, `${p.firstName} ${p.lastName}`.trim());
		}
	}
	const teamInfoCache = g.get("teamInfoCache");
	const named = (ref: Ref): LegacyAwardWinner | undefined => {
		const name = names.get(ref.pid);
		if (name === undefined) {
			return undefined;
		}
		return {
			pid: ref.pid,
			tid: ref.tid,
			name,
			abbrev: teamInfoCache[ref.tid]?.abbrev,
		};
	};
	const namedTeam = (team: Ref[]) =>
		team.map(named).filter((p): p is LegacyAwardWinner => !!p);

	const out: LegacyAwards = {
		season: refs.season,
		allLeague: refs.allLeague.map((team, i) => ({
			title: TEAM_TITLES[i] ?? `Team ${i + 1}`,
			players: namedTeam(team),
		})),
		allDefensive: refs.allDefensive.map((team, i) => ({
			title: TEAM_TITLES[i] ?? `Team ${i + 1}`,
			players: namedTeam(team),
		})),
		allRookie: namedTeam(refs.allRookie),
	};
	for (const [key, ref] of Object.entries(refs.individual) as [
		"mvp" | "finalsMvp" | "dpoy" | "smoy" | "mip" | "roy",
		Ref,
	][]) {
		const p = named(ref);
		if (p) {
			out[key] = p;
		}
	}
	return out;
};

// Just the pids, for settling a bet: who won, not what he is called.
export const legacyAwardPids = (awards: Awards) => {
	const refs = legacyRefs(awards);
	return {
		individual: refs.individual,
		allLeague: refs.allLeague,
		allDefensive: refs.allDefensive,
		allRookie: refs.allRookie,
	};
};
