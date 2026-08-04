// The catalogue of real basketball card sets, 1983-84 through 2025-26, and
// what each one looks like.
//
// Two independent axes make a card here. The SET is the look - a 1985-86 Star
// Company design is a 1985-86 Star Company design no matter who is on it. The
// SEASON is what the card depicts, and it is the year that gets printed on the
// card. A 1985-86 Star design showing a 2026 season says 2026 on it. That is
// the point: the set is a style, not a date.
//
// Every design description here is fed verbatim into the image prompt, so it is
// written for a model to render from rather than for a person to read. Where
// the source research flagged low confidence, the text stays deliberately
// general instead of inventing a specific that would render wrong.

export type CardEraId =
	| "star"
	| "junkWax"
	| "premium"
	| "rpa"
	| "panini"
	| "fanatics";

export type CardEra = {
	id: CardEraId;
	label: string;
	// The design language of the whole era, prepended to the set's own
	// description so a model has the general register before the specifics.
	language: string;
};

export const CARD_ERAS: CardEra[] = [
	{
		id: "star",
		label: "Star Company & Fleer's Return (1983-1989)",
		language:
			"Simple solid single-color or red-white-and-blue borders; posed or plain action photos with no cutouts; thin low-gloss cardboard prone to chipping at the corners; block or plain typography; a small team logo in a corner; simple card backs printed in one or two flat colors on white. It reads as 1980s through border chipping, slightly off centering, muted ink, and a single dominant photo with no graphic treatment.",
	},
	{
		id: "junkWax",
		label: "The Junk-Wax Boom (1989-1995)",
		language:
			"An explosion of color and graphic experimentation; computer-generated abstract and geometric backgrounds; airbrushed logos; gold-foil nameplates arriving mid-era; glossy stock; very large sets and obvious overproduction. It reads as early 1990s through digital gradients, neon geometrics, foil stripes, and busy backgrounds.",
	},
	{
		id: "premium",
		label: "The Premium & Technology Era (1995-2003)",
		language:
			"Technology as the selling point: chromium and foilboard stock, refractive rainbow finishes, acetate and die-cut construction, etched metallic surfaces, and celestial or sci-fi imagery that abandons literal basketball settings. Tiered base sets and serial numbering arrive. It reads as late 1990s through reflective surfaces, metallic etching, and over-designed abstract backgrounds.",
	},
	{
		id: "rpa",
		label: "Patches, Autos & the RPA (2003-2009)",
		language:
			"Luxury materials and restraint in the graphics: thick premium stock, thin gold-foil framing, clean typography, tight portrait photography on plain or softly blurred backgrounds, and windows cut into the card for jersey swatches and on-card signatures. Everything is serial numbered. It reads as mid-2000s through understated elegance and physical embellishment rather than printed effects.",
	},
	{
		id: "panini",
		label: "The Panini Exclusive Era (2009-2025)",
		language:
			"Chromium is the default surface and the colored parallel rainbow is the organizing idea: the same card exists in dozens of finishes, each rarer than the last. Players are cut out and isolated against patterned or abstract chrome rather than photographed in a scene. It reads as modern through the isolated cut-out player, saturated reflective color, and heavy foil patterning.",
	},
	{
		id: "fanatics",
		label: "The Fanatics/Topps Return (2025 onward)",
		language:
			"A return to Topps' house style under Fanatics: paper flagship base cards with clean framing, chromium companions carrying the refractor heritage back into basketball, and retro inserts quoting the brand's own older designs. It reads as current through crisp modern photography inside a deliberately classic frame.",
	},
];

// How one card differs from the set's plain base card. `treatment` is prompt
// text describing the physical difference, so "" means it IS the base card.
export type CardVariant = {
	id: string;
	label: string;
	treatment: string;
};

export type CardSet = {
	id: string;
	// What shows in the picker and on the card title, e.g. "1996-97 Topps
	// Chrome". For the multi-year Panini brands this is the brand name and the
	// first year it existed is in `since`.
	label: string;
	brand: string;
	era: CardEraId;
	// First season the set existed, used only for ordering the picker.
	since: number;
	stock: string;
	border?: string;
	photography?: string;
	background?: string;
	typography?: string;
	layout?: string;
	back: string;
	markers?: string;
	variants: CardVariant[];
};

const BASE: CardVariant = { id: "base", label: "Base", treatment: "" };

const RC = (note: string): CardVariant => ({
	id: "rc",
	label: "Rookie Card",
	treatment: `This is the player's rookie card. ${note}`,
});

export const CARD_SETS: CardSet[] = [
	// ---------------------------------------------------------------- ERA 1
	{
		id: "1983-84-star",
		label: "1983-84 Star Company",
		brand: "Star Company",
		era: "star",
		since: 1984,
		stock:
			"Thin matte cardboard, low print quality, visibly prone to miscutting and ink saturation problems.",
		border:
			"A solid single-color border in the player's team color running the full perimeter - red for a red team, green for a green team.",
		photography: "A posed or plain action color photo, large and centered.",
		background: "Photographic, shot in the arena, with no graphic treatment.",
		typography: "White text; player name and information in plain lettering.",
		layout:
			"Circular team logo at the lower left; the Star logo in the upper-right corner.",
		back: "Stats and biographical information printed in a lighter shade of the same color as the front border.",
		markers:
			"The colored border chips easily at the corners. Distributed in sealed mail-order team bags rather than packs.",
		variants: [
			BASE,
			RC(
				"Star cards are extended rookie cards, marked XRC by collectors, with no logo or emblem on the card itself.",
			),
		],
	},
	{
		id: "1984-85-star",
		label: "1984-85 Star Company",
		brand: "Star Company",
		era: "star",
		since: 1985,
		stock: "The same thin matte stock, with chronic centering problems.",
		border:
			'A solid team-color border, with "Star \'85" printed in the top-right corner.',
		photography: "A posed or plain action color image.",
		background: "Photographic, shot in the arena.",
		typography: "White lettering.",
		layout: "Circular team logo at the bottom left, Star logo at the top right.",
		back: "A reddish-pink background with red or team-color text.",
		markers: "Organized numerically by team. Heavily counterfeited.",
		variants: [BASE, RC("An extended rookie card (XRC), with no RC emblem.")],
	},
	{
		id: "1985-86-star",
		label: "1985-86 Star Company",
		brand: "Star Company",
		era: "star",
		since: 1986,
		stock: "The same thin matte stock.",
		border: "A solid team-color border, same formula as the prior two years.",
		photography: "Posed or plain action.",
		background: "Photographic.",
		typography: "White lettering.",
		layout: "Circular team logo at the bottom left, Star logo at the top right.",
		back: "White with team-color print.",
		markers: "Star's last licensed year.",
		variants: [
			BASE,
			RC("An extended rookie card (XRC), with no RC emblem."),
			{
				id: "white-border",
				label: "White border variation",
				treatment:
					"The team-color border is replaced with white - a known variation on some team sets.",
			},
		],
	},
	{
		id: "1986-87-fleer",
		label: "1986-87 Fleer",
		brand: "Fleer",
		era: "star",
		since: 1987,
		stock: "Standard matte cardboard, a gum-pack issue.",
		border:
			"The iconic red, white and blue perimeter border, with a thin gold-yellow inner frame holding the photo and the nameplate.",
		photography: "A full-color action photo, centered.",
		background: "Photographic, shot in the arena.",
		typography:
			"Player name, team and position in white on a blue nameplate bar across the bottom.",
		layout:
			'The Fleer "Premier" crown logo with a small ribbon in an upper corner.',
		back: "Red and blue print on white stock; a simple bio, yearly stats, and the team logo.",
		markers:
			"A 132-card set, the first pack-issued basketball set in years. The dark borders show chipping.",
		variants: [
			BASE,
			RC("No rookie emblem - in this era the first pack-issued card IS the RC."),
			{
				id: "sticker",
				label: "Sticker insert",
				treatment:
					"A white-bordered sticker with a different, simpler design than the base card - one per pack.",
			},
		],
	},
	{
		id: "1987-88-fleer",
		label: "1987-88 Fleer",
		brand: "Fleer",
		era: "star",
		since: 1988,
		stock: "Standard cardboard.",
		border: "White-bordered front with team-color accent stripes.",
		photography: "An action color photo.",
		typography: "A block-lettering nameplate.",
		back: "Simple two-color print on white with a bio and yearly stats.",
		markers: "Set sizes were growing as competition arrived.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "sticker",
				label: "Sticker insert",
				treatment: "A white-bordered sticker, one per pack.",
			},
		],
	},
	{
		id: "1988-89-fleer",
		label: "1988-89 Fleer",
		brand: "Fleer",
		era: "star",
		since: 1989,
		stock: "Standard cardboard.",
		border: "White-bordered front with team-color accent stripes.",
		photography: "An action color photo.",
		typography: "A block-lettering nameplate.",
		back: "Simple two-color print on white with a bio and yearly stats.",
		markers: "A 168-card set.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "sticker",
				label: "Sticker insert",
				treatment: "A white-bordered sticker, one per pack.",
			},
		],
	},

	// ---------------------------------------------------------------- ERA 2
	{
		id: "1989-90-hoops",
		label: "1989-90 Hoops",
		brand: "NBA Hoops",
		era: "junkWax",
		since: 1990,
		stock: "Standard mass-produced cardboard with a light gloss.",
		border:
			"A white card face with a border graphic shaped like the free-throw lane and key of a basketball court, rendered in one of the team's colors.",
		photography: "A color action photo.",
		background: "In-arena.",
		typography:
			"Player name in black lettering above the photo; team nickname at the bottom.",
		layout: "The court-key motif frames the image.",
		back: "A player headshot, bio and stats on a pale-yellow background with white borders.",
		markers: "353 cards over two series. The first NBA Hoops product.",
		variants: [
			BASE,
			RC("Some rookies in this set are short prints."),
			{
				id: "superstars",
				label: "Superstars box-set",
				treatment:
					"A yellow-bordered box-set variant rather than a true parallel.",
			},
		],
	},
	{
		id: "1990-91-hoops",
		label: "1990-91 Hoops",
		brand: "NBA Hoops",
		era: "junkWax",
		since: 1991,
		stock: "Standard cardboard.",
		border: "The same team-colored court-key border motif as the debut set.",
		photography: "Color action.",
		typography: "Same layout as 1989-90.",
		back: "A small generic portrait and stats.",
		markers: "Heavily overproduced.",
		variants: [
			BASE,
			RC("A lottery-pick rookie card from the Series 2 subset."),
			{
				id: "playoff",
				label: "Playoff card",
				treatment: "A Series 2 playoff subset card.",
			},
			{
				id: "team-art",
				label: "Team art card",
				treatment: "A Series 2 illustrated team art card.",
			},
		],
	},
	{
		id: "1990-91-skybox",
		label: "1990-91 SkyBox",
		brand: "SkyBox",
		era: "junkWax",
		since: 1991,
		stock: "Glossy premium-feeling cardboard.",
		border: "A gold border framing the photo.",
		photography:
			"An action shot of the player cut out and placed against a graphic background.",
		background:
			"THE SIGNATURE ELEMENT: a computer-generated abstract background - geometric shapes, vivid gradients, futuristic patterns, star fields, swirls, and glowing orange basketballs trailing speed lines. Every player gets a different color scheme.",
		typography:
			"Player name in an elegant font on a black nameplate stripe across the bottom.",
		layout:
			"Team logo superimposed at the lower-left corner; the photo bordered in gold.",
		back: "A large action or personality photo, gold borders on white, and a stats box comparing the player to positional averages.",
		markers:
			'423 cards over two series. The unmistakable "robo-card" digital look; basketball\'s first premium brand.',
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1990-91-fleer",
		label: "1990-91 Fleer",
		brand: "Fleer",
		era: "junkWax",
		since: 1991,
		stock: "Standard cardboard.",
		border:
			"A white card face with a two-color outer border: RED across the top and bottom, BLUE down both sides.",
		photography: "A color action photo inside a white inner border.",
		background: "In-arena.",
		typography: "Player name and position below the photo.",
		layout: "Team logo superimposed at the upper left of the photo.",
		back: "Printed in black, gray and yellow with a bio and stats.",
		markers: "198 cards; classic junk-wax.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "rookie-sensations",
				label: "Rookie Sensations insert",
				treatment:
					"An insert card with its own bolder design, distinct from the base card.",
			},
		],
	},
	{
		id: "1991-92-fleer",
		label: "1991-92 Fleer",
		brand: "Fleer",
		era: "junkWax",
		since: 1992,
		stock: "Standard cardboard.",
		border:
			"A major redesign: an off-center vertical stripe about three-quarters of an inch wide runs down the LEFT edge in blue, checkered with repeating black NBA logos, and acts as the left border. The photo shifts right and dominates. A thin gray stripe plus a thicker blue one on the right, a red stripe along the bottom, gray and red stripes across the top. Heavy yellow accenting throughout.",
		photography: "A large right-shifted action photo.",
		background: "In-arena.",
		typography:
			"Team logo, player name and position in white lettering inside the blue left stripe.",
		back: "Full color - the first Fleer basketball set with color backs - showing a headshot and a small action shot over a hardwood-floor background.",
		markers: "400 cards.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "autographed",
				label: "Autographed insert",
				treatment:
					"A hand-signed card - the first basketball set to insert autographs into packs.",
			},
		],
	},
	{
		id: "1991-92-upper-deck",
		label: "1991-92 Upper Deck",
		brand: "Upper Deck",
		era: "junkWax",
		since: 1992,
		stock:
			"White high-quality stock with Upper Deck's premium gloss and an anti-counterfeit hologram on the back.",
		border: "White borders.",
		photography:
			"Sharp full-color action photography - the brand's calling card, noticeably better than its contemporaries.",
		typography: "A clean, understated nameplate.",
		back: "A second full-color photo, stats, and the Upper Deck hologram.",
		markers: "Upper Deck's basketball debut.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1992-93-topps",
		label: "1992-93 Topps",
		brand: "Topps",
		era: "junkWax",
		since: 1993,
		stock: "Standard white cardboard.",
		border:
			"A white card face; the color action photo is framed by two color border stripes wrapping the card, with the player name and team in two team-colored bars across the bottom of the picture.",
		photography: "Color action.",
		typography: "Team-colored name and team bars.",
		layout: "Clean and simple.",
		back: "Horizontal, with a color close-up mugshot, a bio on a light-blue panel, and stats plus a profile on a yellow panel.",
		markers: "396 cards over two series; Topps' return to basketball.",
		variants: [
			BASE,
			{
				id: "draft-pix",
				label: "Draft Pix rookie",
				treatment:
					"A gold-foil \"'92 Draft Pix\" emblem stamped on the front marks this as a rookie.",
			},
			{
				id: "highlight",
				label: "Highlight subset",
				treatment: "A highlight subset card with its own layout.",
			},
			{
				id: "all-star",
				label: "All-Star subset",
				treatment: "An All-Star subset card with its own layout.",
			},
		],
	},
	{
		id: "1992-93-stadium-club",
		label: "1992-93 Topps Stadium Club",
		brand: "Topps",
		era: "junkWax",
		since: 1993,
		stock: "Premium glossy full-bleed stock.",
		border: "Borderless - the color action photo runs edge to edge.",
		photography: "Full-bleed action photography.",
		typography:
			"Team name and player name in gold-foil stripes cutting across the bottom, intersecting the Stadium Club logo.",
		layout: "Stadium Club logo and gold foil along the bottom.",
		back: "Horizontal: bio, a skills rating system, prior-season and career stats, and a miniature image of the player's first card, over a basketball-in-net background.",
		markers: "400 cards.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "members-only",
				label: "Members Only",
				treatment: 'A gold "Members Only" stamp added to the front.',
			},
			{
				id: "beam-team",
				label: "Beam Team insert",
				treatment:
					"The signature insert: a laser-show theme with beams of light forming the side borders and the player lit from below.",
			},
		],
	},
	{
		id: "1993-94-finest",
		label: "1993-94 Topps Finest",
		brand: "Topps",
		era: "junkWax",
		since: 1994,
		stock:
			"THE SIGNATURE: the first basketball chromium set - all-chrome foilboard stock with a reflective mirror finish.",
		border:
			"A bold, busy, colorful multi-shape design with a blue nameplate in the upper-right corner.",
		photography: "An etched player image over colorful geometric shapes.",
		background: "Abstract multi-color shapes and patterns.",
		typography: "Blue nameplate top right; Topps and Finest logos at bottom.",
		back: "Chrome.",
		markers:
			"220 cards. A divisional subset uses a distinctive brick-wall background. This set invented the refractor.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "refractor",
				label: "Refractor",
				treatment:
					"THE FIRST REFRACTOR: the chrome surface throws a rainbow shine when tilted, with a prismatic sheen across the whole card. Nothing on the card says it is a refractor - it is identifiable only by the finish.",
			},
			{
				id: "brick",
				label: "Brick card",
				treatment: "The divisional subset design, over a brick-wall background.",
			},
		],
	},
	{
		id: "1993-94-ultra",
		label: "1993-94 Fleer Ultra",
		brand: "Fleer",
		era: "junkWax",
		since: 1994,
		stock: "Premium stock with heavy foil treatment.",
		photography: "Sharp etched player photos.",
		back: "Foil-accented, with stats and a second photo.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "power-in-the-key",
				label: "Power in the Key",
				treatment:
					"The signature insert: a deeply textured colorful foil background depicting a basketball court key, with the player etched into the foreground, a foil Ultra logo in an upper corner and the player's name in foil at the lower corner.",
			},
		],
	},
	{
		id: "1994-95-ultra",
		label: "1994-95 Fleer Ultra",
		brand: "Fleer",
		era: "junkWax",
		since: 1995,
		stock: "Foil-accented premium stock, prone to chipping.",
		photography: "Etched action photography.",
		back: "Foil-accented with stats.",
		variants: [
			BASE,
			RC("An All-Rookie subset card."),
			{
				id: "rebound-kings",
				label: "Rebound Kings",
				treatment: "An insert with its own bold foil design.",
			},
			{
				id: "defensive-gems",
				label: "Defensive Gems",
				treatment: "An insert printed on 100% etched foil.",
			},
			{
				id: "pro-visions",
				label: "Pro-Visions",
				treatment:
					"A painted-artwork insert - the player rendered as an illustration rather than a photograph.",
			},
		],
	},
	{
		id: "1994-95-collectors-choice",
		label: "1994-95 Upper Deck Collector's Choice",
		brand: "Upper Deck",
		era: "junkWax",
		since: 1995,
		stock: "Standard budget-tier stock with a light gloss.",
		border: "White-bordered fronts.",
		photography: "Color action shots.",
		typography: "Player name, team and position in a lower corner.",
		back: "Another color photo with stats.",
		markers: "420 cards over two series; a low-cost mass-market set.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1994-95-emotion",
		label: "1994-95 SkyBox Emotion",
		brand: "SkyBox",
		era: "junkWax",
		since: 1995,
		stock: "Premium stock.",
		photography:
			"A cleanly isolated player over a designed background, in the mid-1990s premium idiom.",
		typography:
			'Each card carries a single word describing the player, set in large type - the "emotion" conceit of the set.',
		back: "Premium, with stats and a second image.",
		markers: "A 121-card set, well regarded for its design.",
		variants: [BASE, RC("No rookie emblem.")],
	},

	// ---------------------------------------------------------------- ERA 3
	{
		id: "1996-97-topps-chrome",
		label: "1996-97 Topps Chrome",
		brand: "Topps",
		era: "premium",
		since: 1997,
		stock: "Chromium - a chrome rendering of the flagship Topps base design.",
		border: "Follows the flagship Topps layout of the year, executed in chrome.",
		photography: "An action photo.",
		back: "Chrome-backed, with stats.",
		markers: '220 cards; the set that created the "chromie" collector.',
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "refractor",
				label: "Refractor",
				treatment:
					"A rainbow reflective finish across the chrome surface, shifting color as the card tilts.",
			},
			{
				id: "profiles",
				label: "ProFiles insert",
				treatment: "An insert with its own chrome design.",
			},
			{
				id: "youthquake",
				label: "YouthQuake insert",
				treatment: "A young-player insert with its own chrome design.",
			},
		],
	},
	{
		id: "1996-97-finest",
		label: "1996-97 Topps Finest",
		brand: "Topps",
		era: "premium",
		since: 1997,
		stock: "Chromium foilboard, shipped with a protective peel coating.",
		border:
			"A tiered design - the base set is split into Bronze, Silver and Gold rarity tiers, each with its own treatment.",
		photography: "Etched action over a metallic surface.",
		back: "Chrome.",
		markers: "291 cards across two series; scarcity built into the base set.",
		variants: [
			{ id: "bronze", label: "Bronze tier", treatment: "The common tier." },
			{
				id: "silver",
				label: "Silver tier",
				treatment: "The middle tier, with a brighter silver metallic treatment.",
			},
			{
				id: "gold",
				label: "Gold tier",
				treatment: "The rarest tier, with a gold metallic treatment.",
			},
			{
				id: "heirs",
				label: "Heirs subset",
				treatment: "A young-star subset with its own design.",
			},
		],
	},
	{
		id: "1996-97-bowmans-best",
		label: "1996-97 Bowman's Best",
		brand: "Topps",
		era: "premium",
		since: 1997,
		stock: "Chromium.",
		border:
			"Two designs in one set - one treatment for veterans, a separate one for rookies and prospects.",
		photography: "Isolated action over a metallic background.",
		back: "Chrome, with stats.",
		variants: [
			BASE,
			RC("Uses the set's separate rookie/prospect design."),
			{
				id: "refractor",
				label: "Refractor",
				treatment: "A rainbow reflective finish across the chrome.",
			},
			{
				id: "atomic-refractor",
				label: "Atomic Refractor",
				treatment:
					"A refractor with a plaid-like cross-hatched pattern woven through the rainbow finish.",
			},
		],
	},
	{
		id: "1997-98-metal-universe",
		label: "1997-98 SkyBox Metal Universe",
		brand: "SkyBox",
		era: "premium",
		since: 1998,
		stock: "Etched metallic foilboard.",
		border: "Abstract and comic-book styled, designed by comic artists.",
		photography:
			"The player composited into a wild celestial, space or comic-book scene.",
		background:
			"THE SIGNATURE: over-the-top abstract celestial and space themes rather than anything basketball-literal - nebulas, planets, cosmic architecture.",
		typography: "Stylized comic lettering.",
		back: "Metallic, with the artist credited.",
		markers: "125 cards.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "pmg-green",
				label: "Precious Metal Gems (Green)",
				treatment:
					"THE GRAIL PARALLEL: the entire card surface becomes a field of green gemstone-like faceted foil. Extremely condition-sensitive - the foil visibly flakes and chips at the edges.",
			},
			{
				id: "pmg-red",
				label: "Precious Metal Gems (Red)",
				treatment:
					"The entire card surface becomes a field of red gemstone-like faceted foil, flaking at the edges.",
			},
			{
				id: "platinum-portrait",
				label: "Platinum Portrait",
				treatment: "A silver insert with the player's image hole-punched out.",
			},
			{
				id: "silver-slam",
				label: "Silver Slam",
				treatment:
					"A black-and-white player over refractor-like foilboard with purple and orange backgrounds.",
			},
			{
				id: "planet-metal",
				label: "Planet Metal",
				treatment:
					'A quasar swirl behind a black-and-white basketball, with a large letter "M" dominating the design.',
			},
			{
				id: "titanium",
				label: "Titanium",
				treatment: "A die-cut acetate card with blue foil.",
			},
		],
	},
	{
		id: "1997-98-ex2001",
		label: "1997-98 SkyBox E-X2001",
		brand: "SkyBox",
		era: "premium",
		since: 1998,
		stock:
			"THE SIGNATURE: clear acetate and plastic combined with cardboard, with a die-cut player image layered into the construction, plus foil accents.",
		border: "Die-cut, with a borderless feel.",
		photography:
			"The die-cut player image is the whole card, floating over the transparent acetate.",
		back: "Visible through the acetate; stats printed on the opaque portion.",
		markers: "An 82-card hobby-exclusive set sold in two-card packs.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "ec-future",
				label: "Essential Credentials Future",
				treatment: "A pink-themed parallel, serial numbered.",
			},
			{
				id: "ec-now",
				label: "Essential Credentials Now",
				treatment: "A green-themed parallel, serial numbered.",
			},
			{
				id: "jambalaya",
				label: "Jambalaya",
				treatment:
					"A celebrated die-cut insert with an intricate cut silhouette and swirling color.",
			},
			{
				id: "gravity-denied",
				label: "Gravity Denied",
				treatment:
					"A card built around a rivet, letting two images show or combine into one.",
			},
		],
	},
	{
		id: "1997-98-flair-showcase",
		label: "1997-98 Flair Showcase",
		brand: "Fleer",
		era: "premium",
		since: 1998,
		stock: "Thick premium stock.",
		border:
			'THE SIGNATURE: tiered "Rows" - the same player appears across four rows, each with a different design and escalating rarity, Row 3 being common and Row 0 the rarest.',
		photography: "Premium isolated portraiture over a designed field.",
		back: "Premium, with stats.",
		variants: [
			{ id: "row3", label: "Row 3", treatment: "The common row." },
			{ id: "row2", label: "Row 2", treatment: "The second tier." },
			{ id: "row1", label: "Row 1", treatment: "The third tier." },
			{
				id: "row0",
				label: "Row 0",
				treatment: "The rarest row, serial numbered, with the richest design.",
			},
			{
				id: "legacy",
				label: "Legacy Collection",
				treatment: "A serial-numbered parallel of the row, with added foil.",
			},
		],
	},
	{
		id: "1997-98-sp-authentic",
		label: "1997-98 SP Authentic",
		brand: "Upper Deck",
		era: "premium",
		since: 1998,
		stock: "Premium thick stock.",
		border:
			"A clean white-based premium design with gold-foil accents, restrained next to its contemporaries.",
		photography:
			"The player sharp against a softly blurred background, holding focus.",
		back: "Clean, with stats and a second image.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1997-98-skybox-premium",
		label: "1997-98 SkyBox Premium",
		brand: "SkyBox",
		era: "premium",
		since: 1998,
		stock: "Premium stock with gold-foil accents.",
		photography: "The player as the primary focus against a blurred background.",
		back: "Premium, with stats.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "star-rubies",
				label: "Star Rubies",
				treatment:
					"A refractor-like finish worked in red foil, serial numbered.",
			},
		],
	},
	{
		id: "1998-99-sp-authentic",
		label: "1998-99 SP Authentic",
		brand: "Upper Deck",
		era: "premium",
		since: 1999,
		stock: "Premium thick stock.",
		border: "Clean white-based premium design with gold-foil accents.",
		photography: "Sharp player against a soft background.",
		back: "Clean, with stats.",
		variants: [
			BASE,
			RC("A serial-numbered rookie card."),
			{
				id: "sign-of-the-times",
				label: "Sign of the Times",
				treatment:
					"A landmark on-card autograph insert - the signature written directly on the card surface in bold ink across the photo.",
			},
			{
				id: "first-class",
				label: "First Class",
				treatment: "A die-cut insert with an ornate cut edge.",
			},
		],
	},
	{
		id: "1998-99-ionix",
		label: "1998-99 Upper Deck Ionix",
		brand: "Upper Deck",
		era: "premium",
		since: 1999,
		stock:
			'THE SIGNATURE: high-tech holographic foil "Ionix" stock with a futuristic science-fiction design language.',
		background: "Energy fields, circuitry and abstract sci-fi structures.",
		photography: "The player cut out and set into the technological scene.",
		back: "Holographic, with stats.",
		markers: "An 80-card base set.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "reciprocal",
				label: "Reciprocal",
				treatment:
					"A serial-numbered parallel with an inverted foil treatment.",
			},
			{
				id: "kinetix",
				label: "Kinetix",
				treatment: "An insert with an energy-motion design.",
			},
			{
				id: "warp-zone",
				label: "Warp Zone",
				treatment: "An insert built around a tunnel-of-light motif.",
			},
		],
	},
	{
		id: "1998-99-gold-label",
		label: "1998-99 Topps Gold Label",
		brand: "Topps",
		era: "premium",
		since: 1999,
		stock: "Foil-heavy premium stock.",
		border:
			"Gold-dominated framing, with the base set split into Class 1, 2 and 3 tiers.",
		photography: "Isolated player over gold foil.",
		back: "Foil, with stats.",
		variants: [
			{ id: "class1", label: "Class 1", treatment: "The common class." },
			{
				id: "class2",
				label: "Class 2",
				treatment: "The second class, scarcer, with a varied pose.",
			},
			{
				id: "class3",
				label: "Class 3",
				treatment: "The rarest class.",
			},
			{
				id: "red-label",
				label: "Red Label",
				treatment: "A red-foil parallel, serial numbered.",
			},
			{
				id: "black-label",
				label: "Black Label",
				treatment: "A black-foil parallel, the scarcest, serial numbered.",
			},
		],
	},

	// ---------------------------------------------------------------- ERA 4
	{
		id: "2003-04-exquisite",
		label: "2003-04 Upper Deck Exquisite Collection",
		brand: "Upper Deck",
		era: "rpa",
		since: 2004,
		stock: "Ultra-thick luxurious premium stock with gold-foil framing.",
		border: "An elegant thin gold frame.",
		photography: "A tight portrait or cropped action shot.",
		typography: "Gold foil, small and restrained.",
		back: "Minimal and premium - a short bio, a small stat line, and the serial number.",
		markers:
			"A 78-card base set where every single card is serial numbered. The product that created the modern high-end category.",
		variants: [
			{
				id: "base",
				label: "Base (serial numbered)",
				treatment: "Serial numbered on the front in gold foil.",
			},
			{
				id: "gold",
				label: "Gold parallel",
				treatment: "Heavier gold framing, numbered to a very low print run.",
			},
			{
				id: "jersey",
				label: "Jersey card",
				treatment:
					"A rectangular window cut into the card holds a swatch of jersey fabric.",
			},
			{
				id: "patch",
				label: "Patch card",
				treatment:
					"A window in the card holds a multi-color jersey patch showing seams and lettering.",
			},
			{
				id: "rpa",
				label: "Rookie Patch Auto",
				treatment:
					"THE FOUNDING RPA: a large window holding a multi-color player-worn jersey patch, with an on-card autograph signed directly onto the card surface beside it, serial numbered in gold foil.",
			},
			{
				id: "rainbow",
				label: "Rainbow 1/1",
				treatment: 'A one-of-one parallel stamped "1/1".',
			},
		],
	},
	{
		id: "2003-04-topps-chrome",
		label: "2003-04 Topps Chrome",
		brand: "Topps",
		era: "rpa",
		since: 2004,
		stock: "Chromium.",
		border: "The flagship Topps layout of the year rendered in chrome.",
		photography: "Action photography.",
		back: "Chrome, with stats.",
		variants: [
			BASE,
			RC('Carries the standardized "RC" rookie shield logo.'),
			{
				id: "refractor",
				label: "Refractor",
				treatment: "A rainbow reflective finish across the chrome.",
			},
			{
				id: "gold-refractor",
				label: "Gold Refractor",
				treatment: "A gold-tinted refractor, serial numbered.",
			},
		],
	},
	{
		id: "2005-06-sp-game-used",
		label: "2005-06 Upper Deck SP Game Used",
		brand: "Upper Deck",
		era: "rpa",
		since: 2006,
		stock: "Thick premium stock.",
		border: "A clean, restrained frame with foil accents.",
		photography: "A cropped action shot on a plain or lightly textured field.",
		back: "Minimal, with a short stat line and the serial number.",
		markers: "Built around jersey and patch windows; everything numbered.",
		variants: [
			BASE,
			{
				id: "jersey",
				label: "Jersey card",
				treatment: "A window in the card holds a swatch of jersey fabric.",
			},
			{
				id: "patch",
				label: "Patch card",
				treatment: "A window holds a multi-color jersey patch with seams.",
			},
			{
				id: "auto",
				label: "Autograph",
				treatment: "An on-card autograph signed across the front.",
			},
		],
	},
	{
		id: "2007-08-fleer-retro",
		label: "2007-08 Fleer (Upper Deck retro)",
		brand: "Upper Deck",
		era: "rpa",
		since: 2008,
		stock: "Matte cardboard, deliberately imitating the 1980s.",
		border:
			"A deliberate homage to the 1986-87 Fleer design: red, white and blue perimeter border with a thin gold-yellow inner frame.",
		photography: "A color action photo, centered.",
		typography: "Player name on a blue nameplate bar across the bottom.",
		back: "Red and blue print on white, retro-styled.",
		markers:
			"A retro set issued by Upper Deck under the Fleer name after Fleer's liquidation, not by an independent Fleer.",
		variants: [BASE, RC('Carries the "RC" rookie shield.')],
	},
	{
		id: "2007-08-luxury-box",
		label: "2007-08 Topps Luxury Box",
		brand: "Topps",
		era: "rpa",
		since: 2008,
		stock: "Thick premium stock.",
		border:
			'A luxury-suite conceit: the base set is tiered into "seats" of escalating exclusivity, each tier with its own framing.',
		photography: "A tight isolated portrait.",
		back: "Minimal, with the serial number.",
		variants: [
			{ id: "base", label: "Base seat", treatment: "The common tier." },
			{
				id: "mezzanine",
				label: "Mezzanine",
				treatment: "A middle tier with richer framing.",
			},
			{
				id: "courtside",
				label: "Courtside",
				treatment: "The premium tier, low serial numbered.",
			},
		],
	},

	// ---------------------------------------------------------------- ERA 5
	{
		id: "2012-13-prizm",
		label: "2012-13 Panini Prizm (debut)",
		brand: "Panini",
		era: "panini",
		since: 2013,
		stock:
			"THE SIGNATURE: chromium - Panini's first refractor-style product. Slick, etched, tactile.",
		border:
			"A simple full silver border, with the player image printed past the silver so the figure appears to pop out of the frame. Notably flat - it does not curl.",
		photography: "An isolated action photo, the player cut from his background.",
		typography: "Clean and minimal.",
		back: "Standard, with a short stat line.",
		markers: "A 300-card base set. Base rookies carry no serial numbers.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "silver",
				label: "Silver Prizm",
				treatment:
					'THE DEFAULT PRIZM: a silver rainbow refractive finish across the whole card. When a collector says "his Prizm," this is the one.',
			},
			{
				id: "green",
				label: "Green Prizm",
				treatment: "A green-tinted prismatic finish.",
			},
			{
				id: "gold",
				label: "Gold Prizm /10",
				treatment:
					'A gold prismatic finish, serial numbered to 10 and stamped "/10".',
			},
			{
				id: "auto",
				label: "Autograph Prizm",
				treatment:
					"A prismatic card with an on-card autograph, serial numbered.",
			},
		],
	},
	{
		id: "prizm",
		label: "Panini Prizm",
		brand: "Panini",
		era: "panini",
		since: 2013,
		stock: "Chromium - slick, etched, reflective.",
		border:
			"A silver frame with the player image breaking past it. Later years vary the frame graphic but keep the isolated-player-on-chrome idea.",
		photography: "An isolated action photo cut from its background.",
		typography: "Clean sans-serif with the Prizm logo.",
		back: "Standard, with a short stat line and a small portrait.",
		markers:
			"The defining modern set. The parallel rainbow is the whole point - the same card exists in dozens of colors and patterns.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "silver",
				label: "Silver Prizm",
				treatment:
					"THE DEFAULT PRIZM: a silver rainbow refractive finish across the whole card.",
			},
			{
				id: "red",
				label: "Red /299",
				treatment: 'A red prismatic finish, stamped "/299".',
			},
			{
				id: "blue",
				label: "Blue /199",
				treatment: 'A blue prismatic finish, stamped "/199".',
			},
			{
				id: "mojo",
				label: "Mojo",
				treatment:
					"A prismatic finish with a large starburst pattern radiating through it.",
			},
			{
				id: "disco",
				label: "Disco",
				treatment:
					"A prismatic finish patterned with a field of small circles.",
			},
			{
				id: "snakeskin",
				label: "Snakeskin",
				treatment: "A prismatic finish patterned with a reptile-scale texture.",
			},
			{
				id: "hyper",
				label: "Hyper",
				treatment: "A prismatic finish streaked with diagonal light bands.",
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'A gold prismatic finish, stamped "/10".',
			},
			{
				id: "black",
				label: "Black 1/1",
				treatment: 'A black prismatic finish, stamped "1/1".',
			},
		],
	},
	{
		id: "national-treasures",
		label: "Panini National Treasures",
		brand: "Panini",
		era: "panini",
		since: 2010,
		stock: "Thick premium white or light-colored stock.",
		border:
			"Sleek and modern - a large clean field with thin colored accent lines framing the elements.",
		photography: "A cleanly isolated player, no background scene.",
		back: "Minimal and premium, with the serial number.",
		markers: "Every card in the product is numbered to 99 or fewer.",
		variants: [
			{
				id: "base",
				label: "Base /99",
				treatment: 'Serial numbered, stamped "/99".',
			},
			{
				id: "rpa",
				label: "Rookie Patch Auto /99",
				treatment:
					"THE BENCHMARK MODERN RPA: a large window holding a big multi-color jersey patch, with a bright-blue on-card autograph signed directly beside it, stamped \"/99\".",
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'Gold accent lines and framing, stamped "/10".',
			},
			{
				id: "logoman",
				label: "Logoman 1/1",
				treatment:
					'A one-of-one whose patch window holds the league logo patch cut from a jersey, with an on-card autograph, stamped "1/1".',
			},
		],
	},
	{
		id: "flawless",
		label: "Panini Flawless",
		brand: "Panini",
		era: "panini",
		since: 2013,
		stock: "Ultra-premium thick stock; the product ships in a metal briefcase.",
		border:
			"Restrained luxury framing built around an embedded real gemstone set into the card face.",
		photography: "A tight isolated portrait.",
		back: "Minimal, with the serial number.",
		markers: "Every base card is numbered to 25 or fewer.",
		variants: [
			{
				id: "base",
				label: "Base /20",
				treatment:
					"A real cut diamond set into a bezel on the card face, serial numbered.",
			},
			{
				id: "patch",
				label: "Game-worn patch",
				treatment:
					"A window holding a genuinely game-worn multi-color patch with visible wear.",
			},
			{
				id: "tag-auto",
				label: "Championship Tag Auto",
				treatment:
					"A window holding a jersey manufacturer's tag, with an on-card autograph.",
			},
			{
				id: "logoman",
				label: "Logoman 1/1",
				treatment: 'A one-of-one league-logo patch card, stamped "1/1".',
			},
		],
	},
	{
		id: "immaculate",
		label: "Panini Immaculate Collection",
		brand: "Panini",
		era: "panini",
		since: 2013,
		stock: "Premium thick stock; an elegant homage to 2000s Exquisite.",
		border: "Understated framing with gold trim, everything restrained.",
		photography: "A tight isolated portrait or cropped action shot.",
		back: "Minimal, with the serial number.",
		markers: "Every card is numbered. The swatches are unusually large.",
		variants: [
			{
				id: "base",
				label: "Base",
				treatment: "Serial numbered, with gold trim.",
			},
			{
				id: "jumbo",
				label: "Jumbo swatch",
				treatment:
					"An oversized jersey swatch dominating the card, sometimes with a wave-patterned weave.",
			},
			{
				id: "rpa",
				label: "Rookie Patch Auto",
				treatment:
					"A large patch window with gold trimming and an on-card autograph, serial numbered.",
			},
			{
				id: "logoman",
				label: "Logoman Auto 1/1",
				treatment:
					'A one-of-one with an oversized league-logo patch and an on-card autograph, stamped "1/1".',
			},
		],
	},
	{
		id: "donruss",
		label: "Panini Donruss",
		brand: "Panini",
		era: "panini",
		since: 2010,
		stock: "Standard paper base stock.",
		border:
			"The Donruss house look, varying by year but always built around a bold graphic frame and the brand's heavy logo treatment.",
		photography: "Action photography.",
		typography: "Large brand lettering.",
		back: "Paper, with a full stat line and a short write-up.",
		markers:
			"The Rated Rookie logo is one of the most recognizable marks in the hobby.",
		variants: [
			BASE,
			{
				id: "rated-rookie",
				label: "Rated Rookie",
				treatment:
					'THE ICONIC TREATMENT: the black-and-white "Rated Rookie" banner and logo across the card, marking a first-year player.',
			},
			{
				id: "press-proof",
				label: "Press Proof",
				treatment: "A foil-accented parallel, serial numbered.",
			},
			{
				id: "net-marvels",
				label: "Net Marvels",
				treatment: "An insert with its own bold graphic design.",
			},
			{
				id: "elite",
				label: "Elite",
				treatment: "A foil insert with a premium treatment.",
			},
		],
	},
	{
		id: "optic",
		label: "Panini Donruss Optic",
		brand: "Panini",
		era: "panini",
		since: 2017,
		stock:
			'THE SIGNATURE: "Optichrome" - the Donruss base design executed on metallic reflective chromium.',
		border: "Matches the flagship Donruss frame of the year, but on chrome.",
		photography: "Action photography over a reflective surface.",
		typography: "Donruss lettering with Rated Rookie branding where it applies.",
		back: "Chrome, with a stat line.",
		markers: "Second only to Prizm in modern rookie prestige.",
		variants: [
			BASE,
			{
				id: "rated-rookie",
				label: "Rated Rookie",
				treatment: 'The "Rated Rookie" banner and logo, on chrome.',
			},
			{
				id: "holo",
				label: "Holo",
				treatment:
					"The Optic base refractor: a silver rainbow holographic finish across the card.",
			},
			{
				id: "red",
				label: "Red /99",
				treatment: 'A red holographic finish, stamped "/99".',
			},
			{
				id: "blue",
				label: "Blue /49",
				treatment: 'A blue holographic finish, stamped "/49".',
			},
			{
				id: "cracked-ice",
				label: "Cracked Ice /25",
				treatment:
					'A holographic finish shattered into a cracked-glass pattern, stamped "/25".',
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'A gold holographic finish, stamped "/10".',
			},
			{
				id: "gold-vinyl",
				label: "Gold Vinyl 1/1",
				treatment: 'A deep gold one-of-one, stamped "1/1".',
			},
			{
				id: "downtown",
				label: "Downtown",
				treatment:
					"A short-printed illustrated insert placing the player over a stylized cityscape of his team's city, drawn rather than photographed.",
			},
		],
	},
	{
		id: "select",
		label: "Panini Select",
		brand: "Panini",
		era: "panini",
		since: 2014,
		stock: "Opti-chrome chromium, glossy and reflective.",
		border:
			"THE SIGNATURE: a three-tier base set where each tier has a completely different design and escalating rarity - Concourse the common tier, Premier Level in the middle and often carrying die-cut elements, Courtside the rarest and most elaborate.",
		photography: "Isolated action over reflective chrome.",
		back: "Chrome, with a stat line.",
		variants: [
			{
				id: "concourse",
				label: "Concourse",
				treatment: "The common tier, the plainest of the three designs.",
			},
			{
				id: "premier",
				label: "Premier Level",
				treatment: "The middle tier, with a richer frame and die-cut edges.",
			},
			{
				id: "courtside",
				label: "Courtside",
				treatment:
					"The rarest tier, with the most elaborate design of the three.",
			},
			{
				id: "silver",
				label: "Silver",
				treatment: "A silver prismatic finish over the tier's design.",
			},
			{
				id: "tri-color",
				label: "Tri-Color",
				treatment: "A prismatic finish split into three color bands.",
			},
			{
				id: "zebra",
				label: "Zebra",
				treatment: "A prismatic finish patterned in black-and-white stripes.",
			},
			{
				id: "tie-dye",
				label: "Tie-Dye /25",
				treatment: 'A swirled multi-color prismatic finish, stamped "/25".',
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'A gold prismatic finish, stamped "/10".',
			},
			{
				id: "black",
				label: "Black 1/1",
				treatment: 'A black prismatic finish, stamped "1/1".',
			},
		],
	},
	{
		id: "mosaic",
		label: "Panini Mosaic",
		brand: "Panini",
		era: "panini",
		since: 2020,
		stock:
			"THE SIGNATURE: chromium with a mosaic pattern of small tessellated tiles worked into the finish itself, catching light tile by tile.",
		border: "A graphic frame over the tiled chrome field.",
		photography: "An isolated action photo over the mosaic surface.",
		back: "Chrome, with a stat line.",
		markers:
			"The tile texture is what tells it apart from Prizm, Optic and Select.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "silver",
				label: "Silver Mosaic",
				treatment: "A silver prismatic finish over the mosaic tiling.",
			},
			{
				id: "reactive",
				label: "Reactive",
				treatment: "A high-contrast color-shifting mosaic finish.",
			},
			{
				id: "pink-camo",
				label: "Pink Camo",
				treatment: "A pink camouflage pattern over the mosaic tiling.",
			},
			{
				id: "peacock",
				label: "Peacock",
				treatment: "A blue-green iridescent mosaic finish.",
			},
			{
				id: "genesis",
				label: "Genesis",
				treatment: "A dark swirling mosaic finish.",
			},
		],
	},
	{
		id: "revolution",
		label: "Panini Revolution",
		brand: "Panini",
		era: "panini",
		since: 2017,
		stock:
			"THE SIGNATURE: bright shiny foil stock carrying bold, over-the-top abstract designs, descended from 1990s Pacific.",
		background: "Loud abstract foil patterning behind the player.",
		photography: "The player set over the bright foil field.",
		back: "Foil, with a stat line.",
		markers:
			"The parallels change the FOIL PATTERN in the background, not just its color - that pattern-based rainbow is the brand's identity.",
		variants: [
			BASE,
			{
				id: "cosmic",
				label: "Cosmic",
				treatment: "The background foil becomes a starfield and nebula pattern.",
			},
			{
				id: "sunburst",
				label: "Sunburst",
				treatment: "The background foil becomes a radiating starburst.",
			},
			{
				id: "fractal",
				label: "Fractal",
				treatment: "The background foil becomes a repeating fractal pattern.",
			},
			{
				id: "cubic",
				label: "Cubic /50",
				treatment:
					'The background foil becomes a field of three-dimensional cubes, stamped "/50".',
			},
			{
				id: "lava",
				label: "Lava /10",
				treatment:
					'The background foil becomes a molten flowing pattern, stamped "/10".',
			},
			{
				id: "galactic",
				label: "Galactic",
				treatment:
					"An extremely short-printed case hit with a deep-space foil pattern, usually unnumbered.",
			},
			{
				id: "kaleido",
				label: "Kaleido 1/1",
				treatment: 'A kaleidoscopic foil pattern, stamped "1/1".',
			},
			{
				id: "vertical-kaboom",
				label: "Vertical Kaboom!",
				treatment:
					"An illustrated player - drawn, not photographed - over an exploding burst of foil color filling the card.",
			},
			{
				id: "liftoff",
				label: "Liftoff!",
				treatment: "A die-cut insert with a rocket-launch motif.",
			},
		],
	},
	{
		id: "crown-royale",
		label: "Panini Crown Royale",
		brand: "Panini",
		era: "panini",
		since: 2010,
		stock: "Foil stock with crystal-faceted background patterning.",
		border:
			"THE SIGNATURE: the card itself is DIE-CUT along the top into the shape of a CROWN - it is not a rectangle. The crown points rise above the shoulders of the design.",
		photography: "The player set over a faceted foil field.",
		back: "Foil, cut to the same crown shape, with a stat line.",
		variants: [
			{
				id: "base",
				label: "Base Crystal",
				treatment: "The standard crown die-cut with a crystal foil field.",
			},
			{
				id: "blue",
				label: "Blue /99",
				treatment: 'A blue crystal field, stamped "/99".',
			},
			{
				id: "red",
				label: "Red /49",
				treatment: 'A red crystal field, stamped "/49".',
			},
			{
				id: "purple",
				label: "Purple /25",
				treatment: 'A purple crystal field, stamped "/25".',
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'A gold crystal field, stamped "/10".',
			},
			{
				id: "platinum",
				label: "Platinum 1/1",
				treatment: 'A platinum crystal field, stamped "1/1".',
			},
			{
				id: "silhouettes",
				label: "Silhouettes",
				treatment:
					"A die-cut player figure over an oversized jersey swatch in a shield-and-crown shaped window, with an on-card autograph.",
			},
			{
				id: "kaboom",
				label: "Kaboom!",
				treatment:
					"THE MARQUEE CHASE: the player drawn as a comic-book illustration over an explosive burst of concentric foil color radiating out from behind him.",
			},
		],
	},
	{
		id: "court-kings",
		label: "Panini Court Kings",
		brand: "Panini",
		era: "panini",
		since: 2011,
		stock: "Textured stock evoking artist's canvas.",
		border:
			"THE SIGNATURE: a painterly fine-art aesthetic - this product looks like a gallery piece, not a photograph.",
		photography:
			"An illustrated, painted-looking portrait rather than a straight photo - visible brush strokes, impressionist handling.",
		background: "Abstract color washes and brush-stroke fields.",
		typography: "The Court Kings logo integrated into the artwork.",
		back: "Canvas-textured, with a stat line.",
		variants: [
			BASE,
			{
				id: "canvas",
				label: "Canvas",
				treatment: "A parallel with a pronounced woven canvas texture.",
			},
			{
				id: "rookies-i",
				label: "Rookies I",
				treatment:
					"The first of several distinct rookie artworks for the same player.",
			},
			{
				id: "brush-strokes",
				label: "Brush Strokes",
				treatment:
					"An insert built from thick visible brush strokes forming the player.",
			},
			{
				id: "water-color",
				label: "Water Color",
				treatment: "An insert rendered as a loose watercolor wash.",
			},
			{
				id: "blank-slate",
				label: "Blank Slate",
				treatment:
					"A minimalist insert stripped to just the player, his basic information, and the logo on an empty field.",
			},
		],
	},
	{
		id: "spectra",
		label: "Panini Spectra",
		brand: "Panini",
		era: "panini",
		since: 2015,
		stock:
			"Premium Optichrome chromium on noticeably thicker stock than Prizm.",
		border: "A frame worked into an abstract patterned chrome field.",
		photography: "An isolated action photo over patterned chrome.",
		background: "Cosmic and psychedelic multi-color chrome patterning.",
		back: "Chrome, with a stat line.",
		variants: [
			BASE,
			{
				id: "silver",
				label: "Silver",
				treatment: "A silver prismatic finish.",
			},
			{
				id: "celestial",
				label: "Celestial /149",
				treatment: 'A deep-space prismatic finish, stamped "/149".',
			},
			{
				id: "interstellar",
				label: "Interstellar /99",
				treatment: 'A star-streaked prismatic finish, stamped "/99".',
			},
			{
				id: "psychedelic",
				label: "Psychedelic",
				treatment: "A swirling multi-color prismatic finish.",
			},
			{
				id: "meta",
				label: "Meta /25",
				treatment: 'A high-contrast prismatic finish, stamped "/25".',
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'A gold prismatic finish, stamped "/10".',
			},
			{
				id: "nebula",
				label: "Nebula 1/1",
				treatment: 'A nebula-patterned one-of-one, stamped "1/1".',
			},
			{
				id: "color-blast",
				label: "Color Blast",
				treatment:
					"A short-printed insert with the player illustrated over an explosion of saturated color filling the whole card.",
			},
		],
	},
	{
		id: "obsidian",
		label: "Panini Obsidian",
		brand: "Panini",
		era: "panini",
		since: 2019,
		stock:
			"THE SIGNATURE: chromium built on BLACK obsidian stock, intricately etched, with a metallic gloss and neon accents along the edges.",
		border:
			'A "caldera" framing - a volcanic-crater shape ringing the isolated player.',
		background: "Etched impressionist lines radiating across the black field.",
		photography: "An isolated player inside the caldera frame.",
		back: "Black chrome, with a stat line.",
		markers:
			"The dark side of chrome - black and neon volcanic imagery is what separates it from Spectra.",
		variants: [
			BASE,
			{
				id: "orange-flood",
				label: "Orange Electric Etch Flood",
				treatment:
					"THE SIGNATURE PARALLEL: orange color floods along the etched lines across the black base, as though poured into the engraving.",
			},
			{
				id: "purple-flood",
				label: "Purple Flood",
				treatment: "Purple floods along the etched lines across the black base.",
			},
			{
				id: "green-flood",
				label: "Green Flood",
				treatment: "Green floods along the etched lines across the black base.",
			},
			{
				id: "gold-flood",
				label: "Gold Flood /10",
				treatment: 'Gold floods along the etched lines, stamped "/10".',
			},
			{
				id: "vibrant-mojo",
				label: "Vibrant Mojo 1/1",
				treatment: 'A full-spectrum one-of-one, stamped "1/1".',
			},
			{
				id: "vitreous",
				label: "Vitreous",
				treatment: "A short-printed die-cut insert with a glassy finish.",
			},
			{
				id: "scorched-signatures",
				label: "Scorched Signatures",
				treatment:
					"An on-card autograph over a design worked with flames along the black field.",
			},
		],
	},
	{
		id: "contenders",
		label: "Panini Contenders",
		brand: "Panini",
		era: "panini",
		since: 2013,
		stock:
			"Standard stock for the base; a chromium Premium Edition and clear acetate parallels also exist.",
		border:
			"THE SIGNATURE: the card is designed to look like an event ADMISSION TICKET - perforated edges, ticket typography, seat and section markings, and a stub.",
		photography: "An action photo set into the ticket layout.",
		typography:
			'Ticket lettering - "Season Ticket" for veterans, "Rookie Ticket" for first-year players.',
		back: "Ticket-styled, with a stat line.",
		variants: [
			{
				id: "season-ticket",
				label: "Season Ticket",
				treatment: 'The veteran base card, marked "Season Ticket".',
			},
			{
				id: "rookie-ticket",
				label: "Rookie Ticket",
				treatment: 'The rookie base card, marked "Rookie Ticket".',
			},
			{
				id: "rookie-ticket-auto",
				label: "Rookie Ticket Auto",
				treatment:
					"THE FLAGSHIP CHASE: the Rookie Ticket with a hard-signed on-card autograph written directly across the ticket face.",
			},
			{
				id: "playoff-ticket",
				label: "Playoff Ticket /249",
				treatment: 'A foil-accented ticket parallel, stamped "/249".',
			},
			{
				id: "ticket-stub",
				label: "Ticket Stub",
				treatment:
					"A die-cut parallel with a torn perforated stub along the top edge.",
			},
			{
				id: "cracked-ice",
				label: "Cracked Ice Ticket /25",
				treatment:
					'A shattered-ice foil pattern over the ticket, stamped "/25".',
			},
			{
				id: "championship",
				label: "Championship Ticket 1/1",
				treatment: 'The top of the ticket ladder, stamped "1/1".',
			},
			{
				id: "clear-ticket",
				label: "Clear Ticket",
				treatment: "The ticket printed on transparent acetate.",
			},
		],
	},
	{
		id: "panini-hoops",
		label: "Panini NBA Hoops",
		brand: "Panini",
		era: "panini",
		since: 2010,
		stock: "Standard low-cost mass-market stock.",
		border:
			"Clean and straightforward - the accessible gateway product, deliberately uncluttered.",
		photography: "Clear action photography with plenty of the player visible.",
		back: "Paper, with a full bio and a complete stat line - fuller than most modern backs.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "purple",
				label: "Purple parallel",
				treatment: "A purple-tinted border treatment.",
			},
			{
				id: "teal",
				label: "Teal parallel",
				treatment: "A teal-tinted border treatment.",
			},
			{
				id: "premium-stock",
				label: "Premium Stock",
				treatment: "A retail chrome version of the base design.",
			},
		],
	},
	{
		id: "prestige",
		label: "Panini Prestige",
		brand: "Panini",
		era: "panini",
		since: 2010,
		stock: "Standard mid-tier stock with foil accents.",
		border: "A clean graphic frame, mid-tier in ambition.",
		photography: "Action photography.",
		back: "Paper, with a stat line.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "color",
				label: "Colored parallel",
				treatment: "A color-tinted foil parallel, serial numbered.",
			},
		],
	},
	{
		id: "threads",
		label: "Panini Threads",
		brand: "Panini",
		era: "panini",
		since: 2010,
		stock: "Premium stock with a leather- or suede-look textured surface.",
		border: "A frame worked to look like stitched material.",
		photography: "An isolated player over the textured field.",
		back: "Textured, with a stat line.",
		variants: [
			BASE,
			{
				id: "rookie-class-auto",
				label: "Rookie Class Auto Patch",
				treatment:
					"A patch window with an on-card autograph, over the leather-look field.",
			},
		],
	},
	{
		id: "gold-standard",
		label: "Panini Gold Standard",
		brand: "Panini",
		era: "panini",
		since: 2011,
		stock: "Premium stock dominated by gold foil.",
		border: "Heavy gold framing across the whole card.",
		photography: "An isolated player over a gold field.",
		back: "Gold-toned, with the serial number.",
		variants: [
			BASE,
			{
				id: "gold-rpa",
				label: "Gold Rookie Patch Auto",
				treatment:
					"A gold-framed patch window with an on-card autograph, serial numbered.",
			},
		],
	},
	{
		id: "noir",
		label: "Panini Noir",
		brand: "Panini",
		era: "panini",
		since: 2016,
		stock: "Premium heavy stock.",
		border: "A luxury frame around a black-and-white image.",
		photography:
			"THE SIGNATURE: black-and-white photography, with color used sparingly or not at all.",
		back: "Minimal, with the serial number.",
		variants: [
			BASE,
			{
				id: "color-accent",
				label: "Color accent",
				treatment:
					"The black-and-white image with a single element picked out in color.",
			},
			{
				id: "auto",
				label: "Autograph",
				treatment: "An on-card autograph over the black-and-white image.",
			},
		],
	},

	// ---------------------------------------------------------------- ERA 6
	{
		id: "2025-26-topps",
		label: "2025-26 Topps Basketball",
		brand: "Topps",
		era: "fanatics",
		since: 2026,
		stock: "Paper base stock.",
		border:
			"The Topps flagship frame of the year - a clean modern border with the brand's shield in a corner.",
		photography: "Crisp modern action photography.",
		back: "Paper, with a full stat line and a short write-up.",
		markers: "A 300-card base set; Topps' return to fully licensed basketball.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "all-kings",
				label: "All-Kings insert",
				treatment: "An ornate insert with a regal frame and heavy foil.",
			},
			{
				id: "mvp-vault",
				label: "MVP Vault insert",
				treatment: "A vault-themed insert with metallic framing.",
			},
			{
				id: "retro-1980",
				label: "1980-81 Topps retro insert",
				treatment:
					"An insert quoting the 1980-81 Topps design - a tall narrow layout with flat color bands and plain block type.",
			},
			{
				id: "rookie-debut-patch",
				label: "Rookie Debut Patch 1/1",
				treatment:
					'A one-of-one with a patch cut from the jersey worn in the player\'s first professional game, stamped "1/1".',
			},
		],
	},
	{
		id: "2025-26-topps-chrome",
		label: "2025-26 Topps Chrome",
		brand: "Topps",
		era: "fanatics",
		since: 2026,
		stock: "Chromium.",
		border: "The flagship Topps frame rendered in chrome.",
		photography: "Crisp modern action photography over a reflective surface.",
		back: "Chrome, with a stat line.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "refractor",
				label: "Refractor",
				treatment: "A rainbow reflective finish across the chrome.",
			},
			{
				id: "chrome-black",
				label: "Chrome Black",
				treatment: "The design executed on a black chrome field.",
			},
		],
	},
	{
		id: "2025-26-topps-finest",
		label: "2025-26 Topps Finest",
		brand: "Topps",
		era: "fanatics",
		since: 2026,
		stock: "Chromium foilboard.",
		border:
			"A three-tier base set - Common, Uncommon and Rare, each numbered and each with its own framing.",
		photography: "Etched action over metallic chrome.",
		back: "Chrome, with a stat line.",
		variants: [
			{ id: "common", label: "Common tier", treatment: "The common tier." },
			{
				id: "uncommon",
				label: "Uncommon tier",
				treatment: "The middle tier, with richer framing.",
			},
			{
				id: "rare",
				label: "Rare tier",
				treatment: "The scarcest tier, low serial numbered.",
			},
			{
				id: "refractor",
				label: "Refractor",
				treatment: "A rainbow reflective finish across the chrome.",
			},
			{
				id: "centurions",
				label: "Centurions insert",
				treatment:
					"A throwback insert quoting a late-1990s Finest design, heavy on etched foil.",
			},
		],
	},
	{
		id: "2025-26-hoops",
		label: "2025-26 NBA Hoops",
		brand: "Topps",
		era: "fanatics",
		since: 2026,
		stock: "Standard low-cost stock.",
		border:
			"Leans on the early Hoops designs - the free-throw-lane court-key border graphic in a team color, revived.",
		photography: "Clear action photography.",
		back: "Paper, with a bio and a full stat line.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "court-key-auto",
				label: "Court-key autograph",
				treatment:
					"The revived court-key design with an on-card autograph across the front.",
			},
		],
	},
	{
		id: "2025-26-prizm-unlicensed",
		label: "2025-26 Panini Prizm (unlicensed)",
		brand: "Panini",
		era: "fanatics",
		since: 2026,
		stock: "Chromium - slick, etched, reflective.",
		border: "A silver frame with the player image breaking past it.",
		photography: "An isolated action photo cut from its background.",
		back: "Standard, with a stat line.",
		markers:
			"IMPORTANT: this product is UNLICENSED. The player's name is used but NO team name, team logo, or team wordmark may appear anywhere on the card, and the jersey must have its team identifiers removed or obscured.",
		variants: [
			BASE,
			{
				id: "silver",
				label: "Silver Prizm",
				treatment: "A silver rainbow refractive finish across the whole card.",
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'A gold prismatic finish, stamped "/10".',
			},
			{
				id: "black",
				label: "Black 1/1",
				treatment: 'A black prismatic finish, stamped "1/1".',
			},
		],
	},
];

export const cardSetsById = new Map(CARD_SETS.map((set) => [set.id, set]));
export const cardErasById = new Map(CARD_ERAS.map((era) => [era.id, era]));

export const CARD_BRANDS: string[] = [
	...new Set(CARD_SETS.map((set) => set.brand)),
].sort();

export const getCardVariant = (
	setId: string,
	variantId: string,
): CardVariant | undefined =>
	cardSetsById.get(setId)?.variants.find((v) => v.id === variantId);

// "1996-97 Topps Chrome · Refractor · 2026" - what shows under the card. The
// season is the one the card DEPICTS, which is the whole point of keeping the
// two axes apart.
export const cardTitle = (
	setId: string,
	variantId: string,
	season: number,
): string => {
	const set = cardSetsById.get(setId);
	if (!set) {
		return String(season);
	}
	const variant = set.variants.find((v) => v.id === variantId);
	const parts = [set.label];
	if (variant && variant.id !== "base") {
		parts.push(variant.label);
	}
	parts.push(String(season));
	return parts.join(" · ");
};
