// WHICH CONFERENCE A TEAM IS IN, answered one way.
//
// A conference is identified by its cid and nothing else. Teams carry a cid,
// team seasons carry the cid they played under, divisions carry the cid they
// belong to - and every one of those is a REFERENCE into the league's `confs`
// list, which is the single place a conference's name, abbreviation and logo
// live. Nothing copies those onto a team; anything that shows a team's
// conference looks it up here by cid, so renaming a conference or giving it a
// logo is one edit that shows up everywhere at once.
//
// Pure, so the lookup is the same in the worker and the UI.

import type { Conf } from "./types.ts";

// What a page needs to show a conference: the parts of a Conf that draw.
export type ConfIdentity = Pick<Conf, "cid" | "name" | "abbrev" | "imgURL">;

export const confByCid = (
	confs: readonly Conf[],
	cid: number | undefined,
): ConfIdentity | undefined => {
	if (cid === undefined) {
		return undefined;
	}
	const conf = confs.find((c) => c.cid === cid);
	if (!conf) {
		return undefined;
	}
	return {
		cid: conf.cid,
		name: conf.name,
		abbrev: conf.abbrev,
		imgURL: conf.imgURL,
	};
};

// A logo URL as stored: trimmed, and absent rather than empty, so a cleared
// field in the editor removes the logo instead of saving "".
export const normalizeConfImgURL = (
	imgURL: string | undefined,
): string | undefined => {
	const trimmed = imgURL?.trim();
	return trimmed ? trimmed : undefined;
};

// THE RULES THAT KEEP cid A SINGLE SOURCE OF TRUTH, checked in one place.
//
// A division belongs to exactly one conference, by cid. A team belongs to
// exactly one division, by did - and its own cid must be that division's, so
// nothing that reads t.cid can ever disagree with the standings, the schedule
// or the bracket, all of which walk did. The first violation found is
// returned as a sentence a person can act on; undefined means consistent.
// Used by the editor before it saves and by the worker before it writes, so
// a bad edit is refused where it is made rather than discovered later.
export const confsDivsTeamsProblem = (
	confs: readonly { cid: number; name: string }[],
	divs: readonly { cid: number; did: number; name: string }[],
	teams: readonly { tid: number; cid: number; did: number; abbrev?: string }[],
): string | undefined => {
	if (confs.length === 0) {
		return "There must be at least one conference.";
	}
	if (divs.length === 0) {
		return "There must be at least one division.";
	}
	const cids = new Set<number>();
	for (const conf of confs) {
		if (cids.has(conf.cid)) {
			return `Two conferences share the id ${conf.cid}.`;
		}
		cids.add(conf.cid);
	}
	const dids = new Set<number>();
	const confOfDiv = new Map<number, number>();
	for (const div of divs) {
		if (dids.has(div.did)) {
			return `Two divisions share the id ${div.did}.`;
		}
		dids.add(div.did);
		if (!cids.has(div.cid)) {
			return `The ${div.name} division points at a conference that does not exist.`;
		}
		confOfDiv.set(div.did, div.cid);
	}
	for (const t of teams) {
		const label = t.abbrev ?? `team ${t.tid}`;
		const cid = confOfDiv.get(t.did);
		if (cid === undefined) {
			return `${label} is in a division that does not exist.`;
		}
		if (cid !== t.cid) {
			return `${label} is in a division of one conference but assigned to another.`;
		}
	}
	return undefined;
};
