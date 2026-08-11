import type { CardPromptOverride } from "./tradingCardPrompt.ts";

// Achievement cards: one trading card per achievement a player earned in a
// season - the major awards, All-Star selections, the named All-League /
// All-Defensive / All-Rookie teams, the champions' key players, and the top
// draft picks of each class.
//
// There is no queue store and no hook anywhere in the sim. The set of cards a
// season SHOULD have is derived from the season's own records (the awards row,
// the All-Star teams, the playoff bracket, the draft class), each card's id is
// deterministic, and "what's left to make" is that set minus the ids already
// in the synced tradingCards store. Finished cards therefore sync to every
// device for free, and the pending list shrinks the same way on all of them.

export type AchievementKind =
	| "mvp"
	| "dpoy"
	| "smoy"
	| "mip"
	| "roy"
	| "finalsMvp"
	| "allLeague1"
	| "allLeague2"
	| "allLeague3"
	| "allDefensive1"
	| "allDefensive2"
	| "allDefensive3"
	| "allRookie"
	| "allStar"
	| "champion"
	| "draft";

export type AchievementCardSpec = {
	// Deterministic, so two devices working the same season derive the same ids
	// and a card either of them saves crosses the other's off the list.
	id: string;
	pid: number;
	name: string;
	season: number;
	kind: AchievementKind;
	// What goes on the card's flag, e.g. "Finals MVP" or "1st Overall Pick".
	label: string;
};

// How many picks off the top of each draft get a card (a Global Setting).
export const DEFAULT_ACHIEVEMENT_DRAFT_PICKS = 3;

// A championship is a team achievement; the cards go to the players who
// actually carried it, by playoff minutes.
export const CHAMPION_CARD_PLAYERS = 5;

export const achievementCardId = (
	season: number,
	kind: AchievementKind,
	pid: number,
): string => `ach-${season}-${kind}-${pid}`;

type Named = { pid: number; name: string };

const LABELS: Record<Exclude<AchievementKind, "draft">, string> = {
	mvp: "Most Valuable Player",
	finalsMvp: "Finals MVP",
	dpoy: "Defensive Player of the Year",
	smoy: "Sixth Man of the Year",
	mip: "Most Improved Player",
	roy: "Rookie of the Year",
	allLeague1: "First Team All-League",
	allLeague2: "Second Team All-League",
	allLeague3: "Third Team All-League",
	allDefensive1: "First Team All-Defensive",
	allDefensive2: "Second Team All-Defensive",
	allDefensive3: "Third Team All-Defensive",
	allRookie: "All-Rookie Team",
	allStar: "All-Star",
	champion: "League Champion",
};

export type SeasonAchievementInput = {
	season: number;
	// The awards row for the season, structurally (so the worker can hand the
	// stored row straight in). null mirrors what JSON round-trips undefined to.
	awards?: {
		mvp?: Named | null;
		dpoy?: Named | null;
		smoy?: Named | null;
		mip?: Named | null;
		roy?: Named | null;
		finalsMvp?: Named | null;
		allLeague?: { players: Named[] }[];
		allDefensive?: { players: Named[] }[];
		allRookie?: Named[];
	};
	allStars?: Named[];
	// The champions' roster ordered best-first (the caller sorts by playoff
	// minutes); capped at CHAMPION_CARD_PLAYERS here.
	champions?: Named[];
};

// The full expected card set for a season, in the order they'll be offered:
// the individual awards first, then the named teams, then the All-Stars, then
// the champions. Duplicate pids WITHIN a kind collapse; the same player across
// kinds is the point (an MVP season is also an All-League season - two cards).
export const deriveSeasonAchievementCards = (
	input: SeasonAchievementInput,
): AchievementCardSpec[] => {
	const out: AchievementCardSpec[] = [];
	const seen = new Set<string>();
	const push = (
		kind: Exclude<AchievementKind, "draft">,
		p: Named | null | undefined,
	) => {
		if (!p || typeof p.pid !== "number") {
			return;
		}
		const id = achievementCardId(input.season, kind, p.pid);
		if (seen.has(id)) {
			return;
		}
		seen.add(id);
		out.push({
			id,
			pid: p.pid,
			name: p.name,
			season: input.season,
			kind,
			label: LABELS[kind],
		});
	};

	const a = input.awards;
	push("mvp", a?.mvp);
	push("finalsMvp", a?.finalsMvp);
	push("dpoy", a?.dpoy);
	push("smoy", a?.smoy);
	push("mip", a?.mip);
	push("roy", a?.roy);

	const teamKinds = {
		allLeague: ["allLeague1", "allLeague2", "allLeague3"],
		allDefensive: ["allDefensive1", "allDefensive2", "allDefensive3"],
	} as const;
	for (const base of ["allLeague", "allDefensive"] as const) {
		(a?.[base] ?? []).forEach((team, i) => {
			const kind = teamKinds[base][i];
			if (kind) {
				for (const p of team.players ?? []) {
					push(kind, p);
				}
			}
		});
	}
	for (const p of a?.allRookie ?? []) {
		push("allRookie", p);
	}

	for (const p of input.allStars ?? []) {
		push("allStar", p);
	}

	for (const p of (input.champions ?? []).slice(0, CHAMPION_CARD_PLAYERS)) {
		push("champion", p);
	}

	return out;
};

const ordinal = (n: number): string => {
	const rem100 = n % 100;
	const suffix =
		rem100 >= 11 && rem100 <= 13
			? "th"
			: n % 10 === 1
				? "st"
				: n % 10 === 2
					? "nd"
					: n % 10 === 3
						? "rd"
						: "th";
	return `${n}${suffix}`;
};

// The draft class's expected cards: the top numPicks of round 1, by pick.
// Before the draft is run nobody has a round-1 pick number yet, so this is
// naturally empty and the widget stays off the page.
export const deriveDraftAchievementCards = ({
	season,
	picks,
	numPicks,
}: {
	season: number;
	picks: { pid: number; name: string; pick: number }[];
	numPicks: number;
}): AchievementCardSpec[] => {
	if (numPicks <= 0) {
		return [];
	}
	const seen = new Set<number>();
	return picks
		.filter((p) => p.pick >= 1 && p.pick <= numPicks)
		.sort((a, b) => a.pick - b.pick)
		.filter((p) => {
			if (seen.has(p.pid)) {
				return false;
			}
			seen.add(p.pid);
			return true;
		})
		.map((p) => ({
			id: achievementCardId(season, "draft", p.pid),
			pid: p.pid,
			name: p.name,
			season,
			kind: "draft" as const,
			label: `${ordinal(p.pick)} Overall Pick`,
		}));
};

// Draft cards come in two scenes, chosen by a button in the modal.
export type DraftCardScene = "draftNight" | "college";

// A small pool so a class's college shots don't all come back identical;
// seeded by the player so re-copying the same card's prompt stays stable.
const COLLEGE_ACTIONS = [
	"rising for a jumper over a hand in his face, the shot clock winding down",
	"driving the lane through traffic, ball protected, eyes on the rim",
	"celebrating a made three back down the floor, the student section on its feet",
	"elevating for a one-handed finish in transition",
	"sliding into a defensive stance at the top of the key, arms wide",
	"cutting through the lane and catching a pass in stride",
];

// What each achievement swaps into the card prompts. Award and named-team
// cards keep the normal in-game photograph and just gain the flag; champion
// cards trade the game action for the celebration; draft cards replace the
// scene AND the uniform, since he hasn't played a pro minute yet.
export const achievementPromptOverride = (
	spec: Pick<AchievementCardSpec, "kind" | "label" | "season" | "pid">,
	subject: { teamName: string; college?: string; teamColors?: string[] },
	scene?: DraftCardScene,
	// See FICTION_SAFE in tradingCardPrompt.ts. The draft-night scene is the one
	// override that asks for real team merchandise, so it has to know.
	safeMode = false,
): CardPromptOverride => {
	const achievement = `${spec.label}, ${spec.season}`;

	if (spec.kind === "draft") {
		if (scene === "college") {
			const action =
				COLLEGE_ACTIONS[
					Math.abs(spec.pid + spec.season) % COLLEGE_ACTIONS.length
				]!;
			return {
				achievement,
				photograph: `A college game action shot from just before the draft: ${action}. Candid, shot courtside with a long lens in a packed college arena, the crowd falling out of focus behind him.`,
				uniform: `He is in COLLEGE here, not the pros. He wears a college basketball uniform${
					subject.college
						? ` with "${subject.college}" in classic collegiate lettering across the chest`
						: ""
				} - do NOT reproduce any real university's actual uniform or logos. Invent a clean, classic collegiate look: simple striping, traditional cut, colors of your choosing that read as a college program.`,
			};
		}
		const colors =
			subject.teamColors && subject.teamColors.length > 0
				? ` in the team's colours (${subject.teamColors.join(", ")})`
				: "";
		return {
			achievement,
			photograph: `Draft night: he is on the draft stage moments after being taken with the ${spec.label.replace(" Overall Pick", "").toLowerCase()} pick - beaming under the stage lights, shaking an official's hand or holding the team jersey up to a wall of camera flashes, the ${spec.season} draft's stage graphics glowing on the boards behind him${
				safeMode
					? " (invent those graphics; copy no real ones)"
					: " with team marks"
			}.`,
			uniform: safeMode
				? `He is NOT in a game uniform. He wears a sharp tailored suit and a plain draft-night cap${colors}, with a jersey held up for the cameras carrying the team name in simple athletic lettering. INVENT the cap and jersey design - do not reproduce any real team's logo, wordmark, or merchandise.`
				: `He is NOT in a game uniform. He wears a sharp tailored suit and the ${subject.teamName} draft-night cap, with a ${subject.teamName} jersey held up for the cameras - the cap and jersey use that franchise's real ${spec.season} design, which is the one place real-world knowledge belongs on this card.`,
		};
	}

	if (spec.kind === "champion") {
		return {
			achievement,
			photograph: `The seconds after winning the championship: confetti falling through the arena lights, and he is mid-celebration - arms up and roaring, hugging a teammate, or hoisting the trophy overhead - still in full uniform on the court he just won it on.`,
		};
	}

	return { achievement };
};
