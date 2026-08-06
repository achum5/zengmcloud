// Room codes are the shared secret for a league: anyone who knows one can join
// that room. They are typed by hand on phones and read aloud over voice chat,
// so a generated one has to be memorable and unambiguous rather than random
// noise - and, above all, unlikely to collide with a code somebody already
// used. Short numeric codes ("1", "2000") are the failure case: they are the
// first thing anyone types, so two unrelated leagues pick the same one.

const ADJECTIVES = [
	"amber",
	"brisk",
	"clever",
	"dusty",
	"eager",
	"fierce",
	"golden",
	"hidden",
	"iron",
	"jolly",
	"keen",
	"lucky",
	"mellow",
	"noble",
	"olive",
	"proud",
	"quiet",
	"rapid",
	"silver",
	"tidy",
	"upbeat",
	"vivid",
	"witty",
	"zesty",
];

const NOUNS = [
	"anchor",
	"bison",
	"comet",
	"dagger",
	"ember",
	"falcon",
	"grove",
	"harbor",
	"ibex",
	"jetty",
	"kettle",
	"lantern",
	"meadow",
	"nimbus",
	"otter",
	"prairie",
	"quarry",
	"ridge",
	"summit",
	"timber",
	"umbra",
	"valley",
	"willow",
	"zenith",
];

const pick = <T>(arr: T[]): T => arr[Math.floor(Math.random() * arr.length)]!;

// e.g. "brisk-falcon-482". Roughly 24 * 24 * 900 combinations, which is plenty
// for a hobby project's namespace and short enough to read out loud.
export const generateRoomCode = (): string =>
	`${pick(ADJECTIVES)}-${pick(NOUNS)}-${100 + Math.floor(Math.random() * 900)}`;

// Codes that are almost certainly a collision waiting to happen. Purely
// advisory - the app never refuses one - but worth saying out loud before a
// league is bound to it, because rebinding later means everyone rejoins.
export const roomCodeWarning = (code: string): string | undefined => {
	const trimmed = code.trim();
	if (trimmed === "") {
		return undefined;
	}
	if (/^\d+$/.test(trimmed)) {
		return "Numbers alone are the first thing everyone tries, so another league may already be using this code. Something distinctive is safer.";
	}
	if (trimmed.length < 5) {
		return "Short codes collide with other people's leagues. Something longer is safer.";
	}
	return undefined;
};
