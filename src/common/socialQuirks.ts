// WHAT MAKES ONE ACCOUNT SOUND LIKE ONE PERSON.
//
// Tone and the voice dials decide how a KIND of account writes. They cannot
// decide how THIS account writes, and that gap is what a reader notices over
// a month of scrolling: every homer fan typing in the same lowercase, every
// beat writer signing off the same way. Real timelines are full of small,
// stable tics - the one who always adds the team hashtag, the one who trails
// off with ellipses, the one who cannot end a sentence without an exclamation
// mark - and they are what let you recognise a poster before you read the
// name.
//
// So every account gets a handful, drawn ONCE from its own id and never
// changing. Nothing here is stored; the id is the seed. That is also why the
// quirks are deliberately mild and sparse: most accounts get one or none,
// because a feed where everybody has a gimmick is as fake as one where nobody
// does.

import { hashSeed, rngFromSeed } from "./phrasePool.ts";
import type { SocialTone } from "./socialPersonality.ts";

export type SocialQuirks = {
	// A hashtag this account tacks onto some of its posts, and how often.
	hashtag?: string;
	hashtagRate: number;
	// Trails off with ellipses between sentences.
	ellipses: boolean;
	// Ends good news with an exclamation mark.
	exclaims: boolean;
	// Added to the archetype's emoji chance. Most accounts get zero; a few are
	// emoji people, which is exactly how it is.
	emojiBoost: number;
	// Multipliers on how often this account prefaces or signs off a line.
	// Some people always clear their throat first; some never do.
	openerRate: number;
	closerRate: number;
};

// An account with no habits at all: it neither prefaces nor signs off. Every
// real account gets its rates from quirksFor; this is for hand-built ones,
// which is to say tests, where a line should come out exactly as written.
export const NO_QUIRKS: SocialQuirks = {
	hashtagRate: 0,
	ellipses: false,
	exclaims: false,
	emojiBoost: 0,
	openerRate: 0,
	closerRate: 0,
};

const FAN_TONES = new Set<SocialTone>(["hype", "doom", "unhinged"]);
const CASUAL_TONES = new Set<SocialTone>(["hype", "doom", "unhinged", "snark"]);
const DRY_TONES = new Set<SocialTone>(["wire", "wonk"]);

const tagWord = (text: string) => text.replaceAll(/[^\dA-Za-z]/g, "");

export const quirksFor = ({
	id,
	kind,
	tone,
	team,
}: {
	id: string;
	kind: "player" | "team" | "media";
	tone: SocialTone;
	team?: { name: string; abbrev: string };
}): SocialQuirks => {
	const rng = rngFromSeed(hashSeed(`quirks|${id}`));
	// mulberry32's first draw is close to a function of the seed; burn it so
	// neighbouring ids do not share their first quirk.
	rng();

	const out: SocialQuirks = {
		...NO_QUIRKS,
		openerRate: 0.5 + rng(),
		closerRate: 0.5 + rng(),
	};

	if (team) {
		// The rah-rah forms are for accounts that would actually type them. A
		// doomer signing off "#GoCyclones" is the kind of mismatch that undoes
		// a whole account's voice in four characters.
		const cheerful = tone !== "doom" && tone !== "snark" && tone !== "wonk";
		const forms = [
			`#${tagWord(team.name)}`,
			`#${team.abbrev}`,
			...(cheerful
				? [`#Go${tagWord(team.name)}`, `#${tagWord(team.name)}Nation`]
				: []),
		];
		const pickForm = () => forms[Math.floor(rng() * forms.length)]!;

		if (kind === "team" && rng() < 0.6) {
			// A franchise uses the plain one, and not on every post.
			out.hashtag = rng() < 0.7 ? forms[0] : forms[1];
			out.hashtagRate = 0.25;
		} else if (FAN_TONES.has(tone) && rng() < 0.45) {
			out.hashtag = pickForm();
			out.hashtagRate = 0.25 + rng() * 0.2;
		} else if ((tone === "beat" || tone === "wire") && rng() < 0.2) {
			out.hashtag = forms[1];
			out.hashtagRate = 0.15;
		} else if (kind === "player" && rng() < 0.15) {
			out.hashtag = pickForm();
			out.hashtagRate = 0.2;
		}
	}

	if (CASUAL_TONES.has(tone) && rng() < 0.12) {
		out.ellipses = true;
	}
	if ((tone === "hype" || tone === "unhinged") && rng() < 0.22) {
		out.exclaims = true;
	} else if (tone === "corporate" && rng() < 0.1) {
		out.exclaims = true;
	}
	if (!DRY_TONES.has(tone) && rng() < 0.08) {
		out.emojiBoost = 0.18;
	}

	return out;
};
