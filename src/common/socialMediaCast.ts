// THE MEDIA AND FAN CAST.
//
// A feed of nothing but players and franchises reads like a press-release
// wall. The voices that make it feel like a timeline - the insider who breaks
// things, the beat writer who was actually at the game, the fan who has
// already fired the coach - have no counterpart in the league, so there is
// nothing to derive them FROM the way a player's account is derived from the
// player.
//
// The obvious answer is to write them into the store when a league is made.
// That answer is wrong here. Rows in the store go into the room checkpoint,
// which is read whole and parsed whole on every restore and is already the
// most expensive thing a device can do; and it would leave every league that
// existed before this feature with no cast at all unless a migration wrote
// one, which is a lot of moving parts for a list that never changes.
//
// So the cast is DERIVED, exactly like players and teams: generated from the
// team list and nothing else, identical on every device because every choice
// in here is seeded by the team's own id. Editing one still writes a single
// sparse row, and removing one still writes a tombstone, because the ids are
// stable - which is the whole reason this works.

import { hashSeed, rngFromSeed, seededShuffle } from "./phrasePool.ts";
import type { ImplicitTeam } from "./socialAccounts.ts";

export type CastAccount = {
	id: string;
	name: string;
	bio: string;
	archetypeId: string;
	tid?: number;
};

// Fictional. Common enough to read as a real byline, generic enough not to be
// anyone in particular - the same bargain the game's own name generator makes.
const FIRST_NAMES = [
	"Dana",
	"Priya",
	"Colin",
	"Yusuf",
	"Marta",
	"Corinne",
	"Malik",
	"Ines",
	"Desmond",
	"Nadia",
	"Grant",
	"Lucia",
	"Emeka",
	"Sloane",
	"Hector",
	"Wren",
	"Tobias",
	"Camille",
	"Jonah",
	"Isabel",
	"Kai",
	"Rosalind",
	"Andre",
	"Petra",
	"Reggie",
	"Anneke",
	"Silas",
	"Noor",
	"Beatriz",
	"Wallace",
	"Ingrid",
	"Rafael",
	"Delia",
	"Otto",
	"Simone",
	"Bruno",
	"Halima",
	"Everett",
	"Josefina",
	"Lars",
	"Anaya",
	"Curtis",
];

const LAST_NAMES = [
	"Whitfield",
	"Ramachandran",
	"Vasquez",
	"Holbrook",
	"Amaechi",
	"Renner",
	"Okonkwo",
	"Sandoval",
	"Halloran",
	"Novak",
	"Bright",
	"Adeyemi",
	"Cardoso",
	"Castellano",
	"Mbeki",
	"Sutcliffe",
	"Larkin",
	"Prentice",
	"Osei",
	"Vance",
	"Delgado",
	"Fairbanks",
	"Ibarra",
	"Quill",
	"Hargrave",
	"Moreau",
	"Tanaka",
	"Ellery",
	"Brackett",
	"Nakamura",
	"Solano",
	"Whitlock",
	"Beaumont",
	"Pryor",
	"Achebe",
	"Kowalski",
	"Tremblay",
	"Escobar",
	"Lindqvist",
	"Marchetti",
];

// Per-team brands. Each list is walked with a per-team offset, so two teams
// almost never land on the same shape - and when they do the region makes them
// read differently anyway.
const RADIO_BRANDS = [
	"{region} Sports Tonight",
	"The {name} Hour",
	"{abbrev} Postgame Live",
	"Talking {name}",
	"{region} Sports Radio",
	"The {name} Roundtable",
	"{abbrev} Drive Time",
	"{region} Tipoff Show",
];

const HOMER_BRANDS = [
	"{name} Til I Die",
	"{name} Nation",
	"{abbrev} Believer",
	"All In On The {name}",
	"{name} Faithful",
	"{region} Ride Or Die",
	"{abbrev} Truther",
	"{name} Til The Wheels Fall Off",
];

const DOOMER_BRANDS = [
	"{name} Doom Watch",
	"Suffering In {region}",
	"{abbrev} Pain Index",
	"Same Old {name}",
	"{name} Misery Index",
	"Trust The Process ({abbrev})",
	"{region} Doomer",
	"Blow It Up ({abbrev})",
];

const BEAT_OUTLETS = [
	"the {region} beat",
	"the {name}",
	"the {name} beat",
	"{region} basketball",
];

const FILM_BRANDS = [
	"{name} Film Room",
	"{abbrev} Clips",
	"{name} Rewatch",
	"{abbrev} Film Study",
];

const fill = (pattern: string, t: ImplicitTeam) =>
	pattern
		.replaceAll("{region}", t.region)
		.replaceAll("{name}", t.name)
		.replaceAll("{abbrev}", t.abbrev);

// The league-wide voices. One of each: a second national insider would just
// break the same news twice.
const NATIONAL_CAST: readonly Omit<CastAccount, "id">[] = [
	{
		name: "Marcus Boone",
		bio: "Reporting on the league. Everything here is confirmed before it is posted.",
		archetypeId: "insider",
	},
	{
		name: "League Wire",
		bio: "Results, transactions and milestones as they happen. Automated.",
		archetypeId: "aggregator",
	},
	{
		name: "Renata Ferreira",
		bio: "Columnist. Opinions are the job.",
		archetypeId: "nationalPundit",
	},
	{
		name: "Hoops Numbers",
		bio: "Ratings, efficiency and the occasional chart. Not affiliated with any team.",
		archetypeId: "analytics",
	},
	{
		name: "The Cap Sheet",
		bio: "Contracts, exceptions and the math nobody wants to do.",
		archetypeId: "capNerd",
	},
	{
		name: "Draft Room",
		bio: "Prospects, boards and mock drafts all year.",
		archetypeId: "draftHead",
	},
	{
		name: "Box Score History",
		bio: "This day in league history. Records, streaks and the games people forgot.",
		archetypeId: "historian",
	},
	{
		name: "ratio merchant",
		bio: "here for the replies",
		archetypeId: "troll",
	},
	{
		name: "Theo Doyle",
		bio: "National basketball writer. Was at the game.",
		archetypeId: "beatWriter",
	},
	{
		name: "Sloane Kirby",
		bio: "Talking about basketball, mostly loudly.",
		archetypeId: "localRadio",
	},
];

// A person name for slot `n`, unique across the cast because both pools are
// shuffled once with the same seed and then walked by index.
const personName = (order: number) => {
	const rng = rngFromSeed(hashSeed("socialCast|names"));
	const first = seededShuffle(rng, FIRST_NAMES);
	const last = seededShuffle(rng, LAST_NAMES);
	return `${first[order % first.length]} ${
		last[(order * 7 + 3) % last.length]
	}`;
};

// The whole cast for a league. Pure: same teams in, same accounts out, on
// every device and on every call.
export const mediaCastAccounts = (
	teams: readonly ImplicitTeam[],
): CastAccount[] => {
	const out: CastAccount[] = [];

	for (const [i, member] of NATIONAL_CAST.entries()) {
		out.push({ ...member, id: `m:cast:nat${i}` });
	}

	const live = teams.filter((t) => !t.disabled);

	// Beat writers are named people, and their names must not repeat across
	// teams, so they are handed slots from one shared sequence. Offset past
	// the national writers, who took the first ones.
	let personSlot = NATIONAL_CAST.length;

	for (const t of live) {
		// One stream per slot rather than one per team. Sharing a stream made
		// neighbouring team ids draw the same brand for every slot at once,
		// which is exactly the "cheap and redundant" the feed is trying to
		// avoid - six teams in a row cannot all run the Tipoff Show.
		const pick = <T>(slot: string, arr: readonly T[]) => {
			const rng = rngFromSeed(
				hashSeed(`socialCast|${slot}|${t.tid}|${t.abbrev}`),
			);
			// mulberry32's first output is close to a function of the seed, so
			// low-entropy seeds like these correlate. Burn one.
			rng();
			return arr[Math.floor(rng() * arr.length)]!;
		};

		const writer = personName(personSlot);
		personSlot += 1;

		out.push(
			{
				id: `m:cast:beat:${t.tid}`,
				name: writer,
				bio: `Covering ${fill(pick("beat", BEAT_OUTLETS), t)}.`,
				archetypeId: "beatWriter",
				tid: t.tid,
			},
			{
				id: `m:cast:radio:${t.tid}`,
				name: fill(pick("radio", RADIO_BRANDS), t),
				bio: `Daily ${t.name} talk. Callers welcome.`,
				archetypeId: "localRadio",
				tid: t.tid,
			},
			{
				id: `m:cast:homer:${t.tid}`,
				name: fill(pick("homer", HOMER_BRANDS), t),
				bio: `${t.name} fan account. We are winning the title.`,
				archetypeId: "homerFan",
				tid: t.tid,
			},
			{
				id: `m:cast:doomer:${t.tid}`,
				name: fill(pick("doomer", DOOMER_BRANDS), t),
				bio: `${t.name} fan. It is never going to work.`,
				archetypeId: "doomerFan",
				tid: t.tid,
			},
			// One analytics voice per team, because "the numbers say" reads
			// completely differently coming from someone who only watches
			// one team.
			{
				id: `m:cast:film:${t.tid}`,
				name: fill(pick("film", FILM_BRANDS), t),
				bio: `Numbers and film on the ${t.name}.`,
				archetypeId: "analytics",
				tid: t.tid,
			},
		);
	}

	return out;
};
