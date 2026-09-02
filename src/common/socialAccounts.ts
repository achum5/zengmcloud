// WHO HAS AN ACCOUNT, AND WHERE THAT ACCOUNT COMES FROM.
//
// Every player and every team in the league has an account, plus a cast of
// media and fan accounts. That is well over five hundred in a normal league,
// and storing five hundred rows that mostly say "this player is a player"
// would put the whole roster into the room checkpoint for no reason - the
// checkpoint is read whole and parsed whole on every restore, and it is
// already the most expensive thing a device can do.
//
// So accounts are IMPLICIT BY DEFAULT. A player's account is derived from the
// player: the league already knows his name, his team, his age and his mood
// traits, which is everything the account needs. Nothing is written unless a
// person actually changes something.
//
// The store therefore holds exactly three kinds of row, and nothing else:
//
//   OVERRIDE   an implicit account someone edited. Sparse: only the fields
//              they touched. Keyed by the same id the resolver derives, so it
//              finds its target without a search.
//   TOMBSTONE  an implicit account someone removed. `removed: true` and
//              nothing else. Removing is a row rather than an absence because
//              the account would otherwise be re-derived on the next read.
//   EXPLICIT   an account someone created that has no counterpart in the
//              league - every media and fan account, and anything custom.
//
// A fresh league therefore stores the media and fan cast (a few dozen rows)
// and nothing else, and stays that way until the user starts editing.

import type {
	SocialPersonality,
	SocialPersonalityOverride,
} from "./socialPersonality.ts";
import {
	archetypeById,
	personalityForPlayer,
	resolvePersonality,
} from "./socialPersonality.ts";
import { mediaCastAccounts } from "./socialMediaCast.ts";

export type SocialAccountKind = "player" | "team" | "media";

// A stored row. Everything except `id` and `kind` is optional, because the
// common case is a sparse override of an account that already exists.
export type SocialAccount = {
	// "p:<pid>", "t:<tid>", or "m:<uuid>" - see accountId. Client-generated for
	// media accounts so two devices adding one independently never collide,
	// exactly as images and trading cards do.
	id: string;
	kind: SocialAccountKind;

	// Profile. Absent means "derive it".
	handle?: string;
	name?: string;
	bio?: string;
	avatarUrl?: string;
	coverUrl?: string;

	// Relation to the league. Set for implicit accounts by derivation, and by
	// the user for media accounts ("the Boston beat writer").
	pid?: number;
	tid?: number;

	archetypeId?: string;
	personality?: SocialPersonalityOverride;

	// A removed account. Kept as a row so the resolver knows not to re-derive
	// it; the user can restore it by deleting the row.
	removed?: boolean;

	createdAt?: number;
	editedAt?: number;
};

// What the rest of the app actually uses: an account with every field filled
// in, whether it came from a stored row or from the league.
export type ResolvedSocialAccount = {
	id: string;
	kind: SocialAccountKind;
	handle: string;
	name: string;
	bio: string;
	pid?: number;
	tid?: number;
	archetypeId: string;
	personality: SocialPersonality;
	avatarUrl?: string;
	coverUrl?: string;
	// True when nothing about this account is stored, so the editor can show
	// "this is a default" and offer a reset that just deletes the row.
	implicit: boolean;
};

export const playerAccountId = (pid: number) => `p:${pid}`;
export const teamAccountId = (tid: number) => `t:${tid}`;

// True for any id the resolver derives rather than stores: players, teams and
// the media cast. A row with one of these ids is only ever an override.
export const isDerivedAccountId = (id: string) =>
	id.startsWith("p:") || id.startsWith("t:") || id.startsWith("m:cast:");

// ---------------------------------------------------------------- HANDLES
//
// A handle is the account's URL, so it has to be unique across the league and
// STABLE across devices - two people looking at the same league must resolve
// the same link. That rules out anything derived from insertion order or a
// random suffix.
//
// Uniqueness is resolved by walking accounts in a fixed order (teams, then
// players by pid, then media accounts by id) and giving a collision the next
// free numeric suffix. Same league, same order, same answer everywhere.

const HANDLE_MAX = 15;

export const baseHandle = (name: string): string => {
	const stripped = name
		.normalize("NFD")
		// Drop combining marks so "Nenê" and "Biedriņš" produce ASCII handles
		// rather than characters that have to be percent-encoded in a URL.
		.replaceAll(/[̀-ͯ]/g, "")
		.replaceAll(/[^\dA-Za-z]/g, "");
	const trimmed = stripped.slice(0, HANDLE_MAX);
	// Never empty: a name written entirely in a script this strips would
	// otherwise produce an unroutable account.
	return trimmed.length > 0 ? trimmed : "account";
};

// Assign unique handles, preferring each account's own choice. An explicitly
// set handle wins its spot; derived ones queue behind it.
export const assignHandles = <T extends { id: string; name: string }>(
	accounts: readonly T[],
	explicit: ReadonlyMap<string, string>,
): Map<string, string> => {
	const out = new Map<string, string>();
	const taken = new Set<string>();

	// Explicit handles are claimed FIRST, all of them, before any derived one
	// is considered. Otherwise a derived handle could squat on the name a user
	// deliberately typed for another account, and the user's own edit would be
	// the one that got a "2" stuck on the end.
	for (const account of accounts) {
		const wanted = explicit.get(account.id);
		if (wanted === undefined) {
			continue;
		}
		const cleaned = baseHandle(wanted);
		let handle = cleaned;
		let n = 2;
		while (taken.has(handle.toLowerCase())) {
			handle = `${cleaned.slice(0, HANDLE_MAX - String(n).length)}${n}`;
			n += 1;
		}
		taken.add(handle.toLowerCase());
		out.set(account.id, handle);
	}

	for (const account of accounts) {
		if (out.has(account.id)) {
			continue;
		}
		const cleaned = baseHandle(account.name);
		let handle = cleaned;
		let n = 2;
		while (taken.has(handle.toLowerCase())) {
			handle = `${cleaned.slice(0, HANDLE_MAX - String(n).length)}${n}`;
			n += 1;
		}
		taken.add(handle.toLowerCase());
		out.set(account.id, handle);
	}

	return out;
};

// ---------------------------------------------------------------- RESOLVING

export type ImplicitPlayer = {
	pid: number;
	name: string;
	tid: number;
	pos?: string;
	age: number;
	ovr: number;
	experience: number;
	moodTraits: readonly ("F" | "L" | "$" | "W")[];
	// Whether he is still on a roster. Retired players keep their accounts -
	// people do not delete their accounts when they stop playing - but they
	// post far less, which the generator reads off this.
	retired?: boolean;
};

export type ImplicitTeam = {
	tid: number;
	region: string;
	name: string;
	abbrev: string;
	imgURL?: string;
	disabled?: boolean;
};

const playerBio = (p: ImplicitPlayer, teamName: string | undefined): string => {
	const parts: string[] = [];
	if (p.pos) {
		parts.push(p.pos);
	}
	if (p.retired) {
		parts.push("Retired");
	} else if (teamName) {
		parts.push(teamName);
	}
	return parts.join(" · ");
};

// Build every account in the league, stored rows merged over derived defaults.
// Pure: hand it the league's players, teams and stored rows and it returns the
// same list on every device, in a stable order.
export const resolveAccounts = ({
	players,
	teams,
	stored,
}: {
	players: readonly ImplicitPlayer[];
	teams: readonly ImplicitTeam[];
	stored: readonly SocialAccount[];
}): ResolvedSocialAccount[] => {
	const byId = new Map<string, SocialAccount>();
	for (const row of stored) {
		byId.set(row.id, row);
	}

	type Draft = {
		id: string;
		kind: SocialAccountKind;
		name: string;
		bio: string;
		pid?: number;
		tid?: number;
		archetypeId: string;
		derivedPersonality: SocialPersonalityOverride | undefined;
		row: SocialAccount | undefined;
		implicit: boolean;
	};

	const drafts: Draft[] = [];
	const teamName = new Map<number, string>();
	for (const t of teams) {
		teamName.set(t.tid, `${t.region} ${t.name}`);
	}

	// TEAMS FIRST, so a team's handle beats a player's when both reduce to the
	// same string - a franchise is the more findable of the two.
	for (const t of teams) {
		const id = teamAccountId(t.tid);
		const row = byId.get(id);
		if (row?.removed || t.disabled) {
			continue;
		}
		drafts.push({
			id,
			kind: "team",
			name: `${t.region} ${t.name}`,
			bio: `Official account of the ${t.region} ${t.name}.`,
			tid: t.tid,
			archetypeId: "teamOfficial",
			derivedPersonality: undefined,
			row,
			implicit: row === undefined,
		});
	}

	// THE MEDIA AND FAN CAST, derived from the teams the same way the team
	// accounts were. Ahead of the players so a beat writer's handle never
	// changes because somebody signed a free agent with a similar name.
	for (const member of mediaCastAccounts(teams)) {
		const row = byId.get(member.id);
		if (row?.removed) {
			continue;
		}
		drafts.push({
			id: member.id,
			kind: "media",
			name: member.name,
			bio: member.bio,
			tid: member.tid,
			archetypeId: member.archetypeId,
			derivedPersonality: undefined,
			row,
			implicit: row === undefined,
		});
	}

	for (const p of players) {
		const id = playerAccountId(p.pid);
		const row = byId.get(id);
		if (row?.removed) {
			continue;
		}
		drafts.push({
			id,
			kind: "player",
			name: p.name,
			bio: playerBio(p, teamName.get(p.tid)),
			pid: p.pid,
			tid: p.tid,
			archetypeId: "player",
			// A player's voice comes from what the league already knows about
			// him - see personalityForPlayer. Sits under any edit the user
			// makes, so changing his bio never silently rewrites his voice.
			derivedPersonality: personalityForPlayer({
				moodTraits: p.moodTraits,
				age: p.age,
				ovr: p.ovr,
				experience: p.experience,
			}),
			row,
			implicit: row === undefined,
		});
	}

	// Explicit accounts last: they are the ones with hand-typed handles, and
	// assignHandles claims those before any derived handle anyway. Skipping by
	// "already drafted" rather than by id prefix, because a stored row for a
	// derived account is an OVERRIDE of it, not a second account beside it -
	// and the cast shares the media prefix with hand-made accounts.
	const drafted = new Set(drafts.map((draft) => draft.id));
	for (const row of stored) {
		if (row.removed || byId.get(row.id) !== row) {
			continue;
		}
		// Also skip a row whose target is gone - a player who was purged, or a
		// team that was disabled. Its override should lapse with the thing it
		// described, not come back as a nameless account of its own.
		if (drafted.has(row.id) || isDerivedAccountId(row.id)) {
			continue;
		}
		drafts.push({
			id: row.id,
			kind: row.kind,
			name: row.name ?? "Unnamed account",
			bio: row.bio ?? "",
			pid: row.pid,
			tid: row.tid,
			archetypeId: row.archetypeId ?? "beatWriter",
			derivedPersonality: undefined,
			row,
			implicit: false,
		});
	}

	const explicitHandles = new Map<string, string>();
	for (const draft of drafts) {
		const handle = draft.row?.handle;
		if (handle !== undefined && handle !== "") {
			explicitHandles.set(draft.id, handle);
		}
	}
	const handles = assignHandles(drafts, explicitHandles);

	return drafts.map((draft) => {
		const row = draft.row;
		const archetypeId = row?.archetypeId ?? draft.archetypeId;
		return {
			id: draft.id,
			kind: draft.kind,
			handle: handles.get(draft.id)!,
			name: row?.name ?? draft.name,
			bio: row?.bio ?? draft.bio,
			pid: row?.pid ?? draft.pid,
			tid: row?.tid ?? draft.tid,
			archetypeId,
			personality: resolvePersonality({
				archetype: archetypeById(archetypeId),
				// Derived traits sit BETWEEN the archetype and the user's edit,
				// so a hand-edited field always wins over a heuristic.
				override: mergeOverrides(draft.derivedPersonality, row?.personality),
			}),
			avatarUrl: row?.avatarUrl,
			coverUrl: row?.coverUrl,
			implicit: draft.implicit,
		};
	});
};

const mergeOverrides = (
	...overrides: (SocialPersonalityOverride | undefined)[]
): SocialPersonalityOverride | undefined => {
	const present = overrides.filter((o) => o !== undefined);
	if (present.length === 0) {
		return undefined;
	}
	let out: SocialPersonalityOverride = {};
	for (const override of present) {
		const { topics, ...rest } = override;
		out = {
			...out,
			...rest,
			topics: topics ? { ...out.topics, ...topics } : out.topics,
		};
	}
	return out;
};
