// WHAT A POST POINTS AT.
//
// A post already knows exactly who and what it is about - it carries pids,
// tids, a gid and an event type, which is how a player page finds the posts
// about him. This turns that into the things a reader should be able to click:
// the player named in the sentence, the club, the box score of the game it is
// reacting to, the transactions page for a trade.
//
// MATCHING IS BY ID, NEVER BY GUESSING AT THE PROSE. A league contains a Mr.
// Green and a team called the Kings, and a matcher loose enough to find a name
// in a sentence is loose enough to link the wrong one - or to turn the word
// "heat" into the Miami Heat. So only the names of the people and clubs a post
// is genuinely about are candidates, and everything else in the sentence is
// left as text.

export type SocialLinkTarget =
	| { kind: "player"; pid: number }
	| { kind: "team"; tid: number }
	| { kind: "game"; gid: number; tid: number }
	| { kind: "transactions"; tid: number }
	| { kind: "standings" }
	| { kind: "playoffs" };

export type InlineLink = {
	// The exact run of text to turn into a link.
	label: string;
	target: SocialLinkTarget;
};

export type LinkablePost = {
	pids: number[];
	tids: number[];
	pid?: number;
	tid?: number;
	gid?: number;
	eventType: string;
};

// region and name are optional because some callers carry only enough of a
// team to draw its crest. A club with no name on hand still links by abbrev.
export type LinkableTeam = {
	tid: number;
	abbrev: string;
	region?: string;
	name?: string;
};

// A name is only worth linking if it is long enough to be a name. One- and
// two-character "names" come from malformed data and would match half the
// sentence.
const MIN_LABEL = 3;

export const inlineLinks = (
	post: LinkablePost,
	playerNames: Record<number, string>,
	teams: readonly LinkableTeam[],
): InlineLink[] => {
	const out: InlineLink[] = [];
	const seen = new Set<string>();
	const add = (label: string | undefined, target: SocialLinkTarget) => {
		if (label === undefined || label.length < MIN_LABEL) {
			return;
		}
		const key = label.toLowerCase();
		if (seen.has(key)) {
			return;
		}
		seen.add(key);
		out.push({ label, target });
	};

	const pids = new Set(post.pids);
	if (post.pid !== undefined) {
		pids.add(post.pid);
	}
	for (const pid of pids) {
		add(playerNames[pid], { kind: "player", pid });
	}

	const tids = new Set(post.tids);
	if (post.tid !== undefined) {
		tids.add(post.tid);
	}
	const byTid = new Map(teams.map((t) => [t.tid, t]));
	for (const tid of tids) {
		const team = byTid.get(tid);
		if (!team) {
			continue;
		}
		// Longest first, so "Toronto Raptors" is matched before "Raptors" and
		// the shorter one does not eat the front of the longer.
		if (team.region !== undefined && team.name !== undefined) {
			add(`${team.region} ${team.name}`, { kind: "team", tid });
		}
		add(team.name, { kind: "team", tid });
		add(team.abbrev, { kind: "team", tid });
	}

	return out.sort((a, b) => b.label.length - a.label.length);
};

// A hashtag is written from a club's abbrev or nickname (#TOR, #Raptors), and
// sometimes with a word stuck on the front or back (#GoPacers, #PacersNation).
// The tag is matched against every club rather than only the post's, because a
// tag is the one place a post names a team it is not otherwise about.
//
// THE LOOSE MATCH IS THE DANGEROUS ONE. "contains the nickname" turns #Heater
// into the Miami Heat and #Kingston into the Sacramento Kings, because those
// nicknames are ordinary words. So a longer tag has to name the club at a seam:
// either the capital letter that starts a camel-cased word, or - for a tag that
// has been flattened to lower case by a casual voice - the very start or end of
// it, and then only for a nickname long enough not to collide by accident.
const LOOSE_SUFFIX_MIN = 5;
const LOOSE_PREFIX_MIN = 6;

export const teamForHashtag = (
	tag: string,
	teams: readonly LinkableTeam[],
): number | undefined => {
	const raw = tag.replace(/^#/, "");
	const bare = raw.toLowerCase();
	if (bare.length < 2) {
		return undefined;
	}

	const exact = teams.find(
		(t) => t.abbrev.toLowerCase() === bare || t.name?.toLowerCase() === bare,
	);
	if (exact) {
		return exact.tid;
	}

	const cased = raw !== bare;
	const loose = teams.find((t) => {
		const nick = t.name?.toLowerCase();
		if (nick === undefined || nick.length < 4 || bare === nick) {
			return false;
		}
		const at = bare.indexOf(nick);
		if (at < 0) {
			return false;
		}
		if (cased) {
			// A seam in the original casing: the nickname begins a word, and
			// what follows it begins another one (or nothing does).
			const startsWord = raw[at] === raw[at]!.toUpperCase();
			const after = raw[at + nick.length];
			const endsWord = after === undefined || after === after.toUpperCase();
			return startsWord && endsWord;
		}
		// Flattened to lower case, so there are no seams left to read - only the
		// ends of the tag. The front is the riskier end, because that is where
		// an ordinary word grows a suffix: #kingston opens with "kings" and has
		// nothing to do with Sacramento, while #gopacers closes with "pacers"
		// and plainly does. So a prefix has to be a longer nickname than a
		// suffix does.
		return bare.endsWith(nick)
			? nick.length >= LOOSE_SUFFIX_MIN
			: bare.startsWith(nick) && nick.length >= LOOSE_PREFIX_MIN;
	});
	return loose?.tid;
};

// THE ONE LINK THAT BELONGS UNDER THE POST, for the thing the prose has no
// natural anchor for. A reaction to a game rarely contains a clickable phrase
// meaning "that game", so the box score goes underneath instead.
export const contextLink = (
	post: LinkablePost,
	teams: readonly LinkableTeam[],
): { label: string; target: SocialLinkTarget } | undefined => {
	const tid = post.tids[0] ?? post.tid;
	if (post.gid !== undefined && tid !== undefined && tid >= 0) {
		return { label: "Box score", target: { kind: "game", gid: post.gid, tid } };
	}
	switch (post.eventType) {
		case "trade":
		case "signing":
		case "release":
		case "draft":
		case "retirement":
			return tid !== undefined && tid >= 0
				? { label: "Transactions", target: { kind: "transactions", tid } }
				: undefined;
		case "standings":
			return { label: "Standings", target: { kind: "standings" } };
		case "playoffs":
			return { label: "Playoffs", target: { kind: "playoffs" } };
		default:
			return undefined;
	}
};
