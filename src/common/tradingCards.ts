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
	| "vintage"
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
	// How a card of this era physically ages. A card rendered factory-perfect
	// reads as a fake of itself - the period is carried as much by the chipping
	// and the centering as by the design.
	wear: string;
};

export const CARD_ERAS: CardEra[] = [
	{
		id: "vintage",
		label: "The Vintage Era (1961-1982)",
		language:
			"Paper, not cardboard-and-gloss: uncoated stock with a visible dot-screen print pattern, flat single-hit inks, and plain white borders. Photography is posed - a portrait or a staged shooting pose taken in an empty gym, never a game. Type is plain block lettering with no foil anywhere. Layout is simple and centered, and the league had no card license for much of it, so team logos are absent, cropped away, or hidden by warmups. It reads as vintage through flat ink, visible print dots, and the complete absence of gloss.",
		wear: "Soft rounded corners, off-center cuts, gently yellowed paper, and small surface creases. Nothing about it looks machine-perfect.",
	},
	{
		id: "star",
		label: "Star Company & Fleer's Return (1983-1989)",
		language:
			"Simple solid single-color or red-white-and-blue borders; posed or plain action photos with no cutouts; thin low-gloss cardboard prone to chipping at the corners; block or plain typography; a small team logo in a corner; simple card backs printed in one or two flat colors on white. It reads as 1980s through border chipping, slightly off centering, muted ink, and a single dominant photo with no graphic treatment.",
		wear: "Chipping along the colored border edges and corners, visibly off-center cutting, and muted low-saturation ink.",
	},
	{
		id: "junkWax",
		label: "The Junk-Wax Boom (1989-1995)",
		language:
			"An explosion of color and graphic experimentation; computer-generated abstract and geometric backgrounds; airbrushed logos; gold-foil nameplates arriving mid-era; glossy stock; very large sets and obvious overproduction. It reads as early 1990s through digital gradients, neon geometrics, foil stripes, and busy backgrounds.",
		wear: "Generally clean stock (these were hoarded in boxes), but gloss scuffs catch the light and the gold-foil nameplates show fine scratches.",
	},
	{
		id: "premium",
		label: "The Premium & Technology Era (1995-2003)",
		language:
			"Technology as the selling point: chromium and foilboard stock, refractive rainbow finishes, acetate and die-cut construction, etched metallic surfaces, and celestial or sci-fi imagery that abandons literal basketball settings. Tiered base sets and serial numbering arrive. It reads as late 1990s through reflective surfaces, metallic etching, and over-designed abstract backgrounds.",
		wear: "Foil edges chip and flake to reveal white beneath, chromium surfaces carry faint scratches and print lines, and acetate cards show edge nicks and a light haze of print snow.",
	},
	{
		id: "rpa",
		label: "Patches, Autos & the RPA (2003-2009)",
		language:
			"Luxury materials and restraint in the graphics: thick premium stock, thin gold-foil framing, clean typography, tight portrait photography on plain or softly blurred backgrounds, and windows cut into the card for jersey swatches and on-card signatures. Everything is serial numbered. It reads as mid-2000s through understated elegance and physical embellishment rather than printed effects.",
		wear: "Dark and gold borders chip to tiny white specks at the corners; the thick stock shows its layered edge; everything else is protected and near-mint.",
	},
	{
		id: "panini",
		label: "The Panini Exclusive Era (2009-2025)",
		language:
			"Chromium is the default surface and the colored parallel rainbow is the organizing idea: the same card exists in dozens of finishes, each rarer than the last. Players are cut out and isolated against patterned or abstract chrome rather than photographed in a scene. It reads as modern through the isolated cut-out player, saturated reflective color, and heavy foil patterning.",
		wear: "Chromium scratches easily - fine surface lines and small edge nicks in the foil, with the occasional print line running through the pattern.",
	},
	{
		id: "fanatics",
		label: "The Fanatics/Topps Return (2025 onward)",
		language:
			"A return to Topps' house style under Fanatics: paper flagship base cards with clean framing, chromium companions carrying the refractor heritage back into basketball, and retro inserts quoting the brand's own older designs. It reads as current through crisp modern photography inside a deliberately classic frame.",
		wear: "Crisp and current, with only the light surface scratching chromium always carries.",
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
	// Only for sets that are not the standard 2.5 x 3.5 - the 1969-71 Topps
	// "tall boys", minis, box-toppers. Left off, the card is standard size.
	proportions?: string;
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
	// ---------------------------------------------------------------- ERA 0
	{
		id: "1961-62-fleer",
		label: "1961-62 Fleer",
		brand: "Fleer",
		era: "vintage",
		since: 1962,
		stock:
			"Thin uncoated paper with a visible dot-screen print pattern and no gloss at all.",
		border: "A plain white border on all four sides, cut unevenly.",
		photography:
			"A posed color portrait or a staged shooting pose taken in an empty gym - never a game photograph.",
		background: "Flat and plain: a bare gym wall or a solid color field.",
		typography:
			"Plain block lettering below the photo carrying the player's name and position.",
		layout: "The team city in small type; no elaborate framing and no foil.",
		back: "One or two flat ink colors on cream paper - a short biography, a single-line stat row, and the card number in a corner.",
		markers:
			"A 66-card set, the template later revived as Fleer's own retro inserts.",
		variants: [
			BASE,
			RC("The first pack-issued card IS the rookie card; no emblem appears."),
		],
	},
	{
		id: "1969-70-topps",
		label: "1969-70 Topps (tall boy)",
		brand: "Topps",
		era: "vintage",
		since: 1970,
		stock: "Heavier uncoated paper stock, matte, with visible print dots.",
		proportions:
			'2.5 x 4 11/16 inches, portrait - the "tall boy" format, dramatically taller than a modern card. This unusual shape is essential and must not be normalized to standard proportions.',
		border: "A plain white border.",
		photography:
			"The player's portrait or posed action sits inside a large OVAL frame cut into the white field, his head sometimes breaking over the top edge of the oval.",
		background: "Plain white outside the oval.",
		typography:
			"The player's name in red across the top, his position in black beneath it, and his team's city in red along the bottom.",
		layout:
			"A small black silhouette of a basketball player in each of the four corners, framing the oval.",
		back: "A baseball-card-style back on cream stock: career statistics, a paragraph of biography, and a small cartoon illustration.",
		markers:
			"IMPORTANT: this set was UNLICENSED. No team logo, team name or team wordmark appears anywhere - only the city. Players are shown in plain warmup jackets or in jerseys with the identifying marks turned away from camera. A 99-card set.",
		variants: [
			BASE,
			RC("No rookie emblem existed; the first card is simply the first card."),
		],
	},
	{
		id: "1970-71-topps",
		label: "1970-71 Topps (tall boy)",
		brand: "Topps",
		era: "vintage",
		since: 1971,
		stock: "Matte uncoated stock with visible print dots.",
		proportions:
			'2.5 x 4 11/16 inches, portrait - the "tall boy" format, far taller than a modern card. Keep this shape.',
		border: "A plain white border.",
		photography:
			"A posed portrait or staged action shot, full length in frame.",
		background: "Plain, with the arena barely suggested.",
		typography:
			"A large graphic basketball sits across the bottom of the card, and the player's name, city and position are printed inside it.",
		layout: "Nothing else competes with the basketball graphic.",
		back: "Cream stock with career statistics, a short biography, and a cartoon panel.",
		markers:
			"IMPORTANT: UNLICENSED - no team logos or wordmarks anywhere, city names only.",
		variants: [BASE, RC("No rookie emblem.")],
	},

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
		layout:
			"Circular team logo at the bottom left, Star logo at the top right.",
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
		layout:
			"Circular team logo at the bottom left, Star logo at the top right.",
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
			RC(
				"No rookie emblem - in this era the first pack-issued card IS the RC.",
			),
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
					'A gold-foil "\'92 Draft Pix" emblem stamped on the front marks this as a rookie.',
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
				treatment:
					"The divisional subset design, over a brick-wall background.",
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

	{
		id: "1992-93-hoops",
		label: "1992-93 NBA Hoops",
		brand: "NBA Hoops",
		era: "junkWax",
		since: 1993,
		stock: "Standard paper stock, mass-produced.",
		border:
			"A white border with a thin team-color rule, plain next to what Fleer and SkyBox were doing the same year.",
		photography: "Straightforward color action.",
		background: "Photographic.",
		typography:
			"The name in block lettering on a team-color bar at the bottom.",
		layout: "The Hoops logo in a corner; cards ordered alphabetically by team.",
		back: "A second color photo with a full stat grid beneath it.",
		markers:
			"A 490-card set across two series - the high-water mark of overproduction.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "team-card",
				label: "Team card",
				treatment:
					"A team-themed card rather than a solo player card, with the roster listed on the back.",
			},
		],
	},
	{
		id: "1992-93-fleer",
		label: "1992-93 Fleer",
		brand: "Fleer",
		era: "junkWax",
		since: 1993,
		stock: "Standard glossy stock.",
		border:
			"A gold frame running the full perimeter - handsome, and notorious for chipping at the corners.",
		photography: "Color action or posed, framed inside the gold.",
		background:
			"Photographic, with the name and team held in boxes filled with a basketball-pebble texture.",
		typography:
			"The player's name inside a basketball-textured box, color-coded to his team.",
		layout: "Team color-coding drives the whole layout.",
		back: "A basketball-pebble patterned background, a large photo cut to the shape of the free-throw lane, and a pink stat panel.",
		markers: "444 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1993-94-fleer",
		label: "1993-94 Fleer",
		brand: "Fleer",
		era: "junkWax",
		since: 1994,
		stock: "UV-coated glossy stock.",
		border: "A white border framing the photo.",
		photography: "Color action, bordered rather than full-bleed.",
		background:
			"A block of fluorescent color sits behind the nameplate - the era's signature loud accent.",
		typography:
			"The player's name at the lower left, over the fluorescent block.",
		layout: "Restrained apart from the fluorescent flash.",
		back: "Bold full-color graphics with a photo, the name, and a complete stat line.",
		markers: "400 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1994-95-fleer",
		label: "1994-95 Fleer",
		brand: "Fleer",
		era: "junkWax",
		since: 1995,
		stock: "Standard glossy stock.",
		border: "A white border.",
		photography: "Color action.",
		background: "Photographic.",
		typography:
			"THE SIGNATURE: the name, team and position sit on an irregularly-shaped patch of team-colored foil at the lower left, the lettering itself also in team color.",
		layout: "The foil patch is the only graphic event on the card.",
		back: "Standard, with a photo and stats.",
		markers: "390 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1993-94-topps",
		label: "1993-94 Topps",
		brand: "Topps",
		era: "junkWax",
		since: 1994,
		stock: "Standard glossy stock.",
		border: "A white border with an inner border color-coded to the team.",
		photography: "Color action.",
		background: "Photographic.",
		typography:
			"The name in white script at the lower left; the team on a solid team-color bar along the very bottom.",
		layout: "Clean and symmetrical.",
		back: "Horizontal: a close-up photo on the right, and stats, biography and career highlights on a beige panel to the left.",
		markers: "396 cards across two series - Topps' return to basketball.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "gold",
				label: "Gold",
				treatment: "A parallel with the design elements restruck in gold foil.",
			},
		],
	},
	{
		id: "1994-95-topps",
		label: "1994-95 Topps",
		brand: "Topps",
		era: "junkWax",
		since: 1995,
		stock: "Standard glossy stock.",
		border:
			"THE SIGNATURE: a jagged, torn-looking white border with an irregular edge framing the photo.",
		photography: "Full-color action inside the jagged frame.",
		background: "Photographic.",
		typography: "The name and team in gold foil along the bottom.",
		layout: "The ragged frame carries the whole design.",
		back: "Standard, with stats.",
		markers: "396 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "spectralight",
				label: "Spectralight",
				treatment:
					"A parallel with a prismatic rainbow sheen worked through the design.",
			},
		],
	},
	{
		id: "1992-93-upper-deck",
		label: "1992-93 Upper Deck",
		brand: "Upper Deck",
		era: "junkWax",
		since: 1993,
		stock: "Semi-gloss premium stock with a gold-foil edge treatment.",
		border:
			"A clean white border. Along the BOTTOM runs a banner striped in the team's two colors, gradient-shaded from one into the other and textured with fine diagonal lines.",
		photography: "Color action.",
		background: "Photographic.",
		typography:
			"The team name gold-foil stamped across the top; the player's name in the bottom banner inside a thin gold-foil outline.",
		layout: "The Upper Deck logo caps the right end of the bottom banner.",
		back: "An action photo down the left side, with stats laid over a ghosted league logo on the right.",
		markers: "510 cards across two series; a fan-favorite design.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1993-94-upper-deck",
		label: "1993-94 Upper Deck",
		brand: "Upper Deck",
		era: "junkWax",
		since: 1994,
		stock: "Glossy UV-coated stock.",
		border:
			"The left edge and the bottom edge are solid bands in the team's colors; the other two sides are clean.",
		photography: "Glossy color action.",
		background: "Photographic.",
		typography:
			"The player's name in the bottom band; the team name running up the colored left band.",
		layout: "An L-shaped color frame around two sides of the photo.",
		back: "A second photo with a full stat block.",
		markers: "510 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1994-95-upper-deck",
		label: "1994-95 Upper Deck",
		brand: "Upper Deck",
		era: "junkWax",
		since: 1995,
		stock: "Glossy stock.",
		border: "Color-coded bars down the side of the card.",
		photography: "Full-color action.",
		background: "Photographic.",
		typography: "The name and team set into the color-coded side bars.",
		layout: "Vertical bars are the organizing element.",
		back: "Photo and stats.",
		markers: "360 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1992-93-skybox",
		label: "1992-93 SkyBox",
		brand: "SkyBox",
		era: "junkWax",
		since: 1993,
		stock: "Glossy stock.",
		border: "Full-bleed, with no border at all.",
		photography:
			"A color action photo blended into computer-generated color screens and graphic shapes.",
		background:
			"THE SIGNATURE: the absolute peak of SkyBox's computer-graphic period - hard-edged digital color fields, gradients and geometric shapes swallowing the photograph.",
		typography: "Digital, angular lettering integrated into the graphics.",
		layout: "The photo and the graphics are indistinguishable from each other.",
		back: "A digital-styled back with stats.",
		markers: "413 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1993-94-skybox-premium",
		label: "1993-94 SkyBox Premium",
		brand: "SkyBox",
		era: "junkWax",
		since: 1994,
		stock: "Glossy premium stock.",
		border:
			"Full-bleed photography with a WIDE white stripe running down one side of the card.",
		photography: "Full-bleed color action.",
		background:
			"Photographic - the retreat from the computer-graphic era begins here.",
		typography:
			"The player's name, position and team printed inside the white side stripe.",
		layout: "The SkyBox Premium logo in foil, superimposed over the photo.",
		back: "A second close-up filling the top half, with statistics and a written scouting report below.",
		markers: "341 cards.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1994-95-skybox-premium",
		label: "1994-95 SkyBox Premium",
		brand: "SkyBox",
		era: "junkWax",
		since: 1995,
		stock: "Glossy stock.",
		border: "Full-bleed.",
		photography: "Full-bleed action.",
		background: "Photographic.",
		typography:
			"The player's name running vertically down the upper-left corner.",
		layout: "Minimal apart from the vertical name.",
		back: "Photo and stats.",
		markers: "350 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "skytech-force",
				label: "SkyTech Force",
				treatment:
					"An insert with a hard-edged technological graphic treatment behind the player.",
			},
		],
	},
	{
		id: "1993-94-stadium-club",
		label: "1993-94 Topps Stadium Club",
		brand: "Topps",
		era: "junkWax",
		since: 1994,
		stock: "Full-bleed glossy premium stock.",
		border: "Borderless - the photograph runs off all four edges.",
		photography:
			"THE SIGNATURE: the best photography in the hobby, full-bleed, shot like magazine sports photography rather than card photography.",
		background: "Photographic, edge to edge.",
		typography:
			"The player's name superimposed low on the card in white with gold foil.",
		layout: "Nothing interrupts the photograph.",
		back: "Borderless, split by a torn-paper effect: a vertical photo on the left, biography on a purple field to the right, and a multicolored stat box carrying a skills-rating breakdown.",
		markers: "360 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "first-day-issue",
				label: "First Day Issue",
				treatment:
					"A scarce parallel identified by a special foil stamp on the front.",
			},
			{
				id: "members-only",
				label: "Members Only",
				treatment:
					"A parallel issued to subscription members, marked with its own foil stamp.",
			},
		],
	},
	{
		id: "1994-95-stadium-club",
		label: "1994-95 Topps Stadium Club",
		brand: "Topps",
		era: "junkWax",
		since: 1995,
		stock: "Full-bleed glossy stock.",
		border: "Borderless.",
		photography: "Full-bleed action photography.",
		background: "Photographic.",
		typography: "The name in foil along the bottom edge.",
		layout: "Photo-first, minimal furniture.",
		back: "Photo and stats.",
		markers: "362 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "first-day-issue",
				label: "First Day Issue",
				treatment: "A parallel marked by a gold-foil stamp of the set logo.",
			},
			{
				id: "beam-team",
				label: "Beam Team",
				treatment:
					"The era's signature insert: the player cut out over a background of hard beams of light radiating from behind him.",
			},
		],
	},
	{
		id: "1994-95-flair",
		label: "1994-95 Flair",
		brand: "Fleer",
		era: "junkWax",
		since: 1995,
		stock:
			"THE SIGNATURE: extra-thick 30-point stock with a polyester laminate on both faces - about twice the thickness of an ordinary card, sold in rigid two-piece hard packs.",
		border: "Borderless.",
		photography:
			"TWO color action photographs blended into one another across the front.",
		background: "The second photo IS the background, dissolved into the first.",
		typography: "The player's name and team gold-foil stamped.",
		layout: "Super-premium and uncluttered; the thickness is the statement.",
		back: "A single full-color action photo with the statistics laid directly over it.",
		markers:
			"326 cards across two series - Fleer's first super-premium basketball product.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "hot-numbers",
				label: "Hot Numbers",
				treatment:
					"An insert built around large graphic numerals behind the player.",
			},
			{
				id: "scoring-power",
				label: "Scoring Power",
				treatment:
					"An insert with a heavy foil treatment and a bold graphic field.",
			},
		],
	},
	{
		id: "1994-classic-draft",
		label: "1994 Classic Draft Picks",
		brand: "Classic",
		era: "junkWax",
		since: 1995,
		stock: "Standard glossy stock.",
		border: "A simple colored frame.",
		photography:
			"A draft-night or posed studio shot - the player in a college uniform or a suit, never a professional jersey.",
		background: "A plain studio backdrop or a flat color block.",
		typography: "Draft-oriented lettering with the pick number called out.",
		layout: "The player's college is named where a professional team would be.",
		back: "Biography and college statistics.",
		markers:
			"IMPORTANT: this product is UNLICENSED. No professional team name, logo, wordmark or uniform may appear. The player is shown in a college uniform or in draft-night formalwear, and his school is named instead of a franchise.",
		variants: [
			BASE,
			RC("Every card in the set is a pre-professional draft-pick card."),
			{
				id: "gold",
				label: "Gold",
				treatment: "A parallel printing with gold-foil accents.",
			},
		],
	},

	// ---------------------------------------------------------------- ERA 3
	{
		id: "1996-97-topps-chrome",
		label: "1996-97 Topps Chrome",
		brand: "Topps",
		era: "premium",
		since: 1997,
		stock: "Chromium - a chrome rendering of the flagship Topps base design.",
		border:
			"Follows the flagship Topps layout of the year, executed in chrome.",
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
				treatment:
					"The middle tier, with a brighter silver metallic treatment.",
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
		photography:
			"The player as the primary focus against a blurred background.",
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

	{
		id: "1995-96-flair",
		label: "1995-96 Flair",
		brand: "Fleer",
		era: "premium",
		since: 1996,
		stock:
			"Extra-thick 30-point stock in rigid hard packs, with the entire front surface struck in etched foil.",
		border: "Borderless.",
		photography: "Color action over the foil field.",
		background:
			"THE SIGNATURE: 100% etched foil across the whole front - the design is physically embossed into the metallic surface.",
		typography: "Foil lettering worked into the etched surface.",
		back: "Full color, with stats.",
		markers: "250 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "hot-numbers",
				label: "Hot Numbers",
				treatment:
					"A lenticular insert: a three-dimensional tornado of numerals swirling behind the player, appearing to move as the card tilts.",
			},
			{
				id: "style",
				label: "Style",
				treatment: "A subset card with a heavier decorative foil treatment.",
			},
		],
	},
	{
		id: "1995-96-fleer",
		label: "1995-96 Fleer",
		brand: "Fleer",
		era: "premium",
		since: 1996,
		stock: "Standard glossy stock.",
		border: "Borderless, running full-bleed.",
		photography: "Color action.",
		background:
			"One of four different background treatments used across the set, so neighboring cards do not match.",
		typography: "Varies with the background treatment.",
		back: "Photo and stats.",
		markers: "350 cards.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1996-97-fleer",
		label: "1996-97 Fleer",
		brand: "Fleer",
		era: "premium",
		since: 1997,
		stock: "Standard glossy stock.",
		border: "Full-bleed.",
		photography: "Full-bleed color action.",
		background: "Photographic.",
		typography:
			"THE SIGNATURE: the player's LAST name in huge ghosted-white letters across the card, with his FIRST name in gold foil laid over the top of it, and the team name in gold foil beneath.",
		layout: "The oversized ghosted surname is the design.",
		back: "Horizontal, on a team-color field with a basketball and the team logo, a player photo, and the stat line.",
		markers: "300 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1997-98-fleer",
		label: "1997-98 Fleer",
		brand: "Fleer",
		era: "premium",
		since: 1998,
		stock:
			'A textured matte "Textured Legend" finish, deliberately built to take an autograph in ink.',
		border: "Full-bleed.",
		photography: "Full-bleed action.",
		background: "Photographic.",
		typography:
			"The player's name in gold-foil block capitals along the bottom, with the team and position in gold-foil script below it.",
		back: "Career statistics.",
		markers: "350 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1998-99-fleer-tradition",
		label: "1998-99 Fleer Tradition",
		brand: "Fleer",
		era: "premium",
		since: 1999,
		stock: "Glossy paper stock.",
		border:
			"A thin clean border - a deliberate step back toward a classic look.",
		photography: "Color action.",
		background: "Photographic.",
		typography: "A foil nameplate.",
		back: "A standard stat back.",
		markers: "150 cards in a single series; the Tradition name arrives here.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "vintage-61",
				label: "Vintage '61",
				treatment:
					"THE RETRO INSERT: the card is rebuilt on the 1961-62 Fleer template - plain white border, posed-looking portrait, flat inks, plain block lettering, no foil anywhere. It should look like a card printed decades before the season it depicts.",
			},
			{
				id: "classic-61",
				label: "Classic '61 /61",
				treatment:
					'The 1961-62 retro design struck in holographic foil and serial numbered, stamped "/61".',
			},
		],
	},
	{
		id: "1995-96-topps",
		label: "1995-96 Topps",
		brand: "Topps",
		era: "premium",
		since: 1996,
		stock: "Standard glossy stock.",
		border: "A white border.",
		photography: "Full-color action.",
		background: "Photographic.",
		typography: "The name in gold, set against a hard black drop shadow.",
		back: "Horizontal, with color head-shots and a stat block.",
		markers: "291 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1996-97-topps",
		label: "1996-97 Topps",
		brand: "Topps",
		era: "premium",
		since: 1997,
		stock: "Standard glossy stock.",
		border: "A white border.",
		photography: "Full-color action.",
		background: "Photographic.",
		typography:
			"The name in gold, set against the streaking motion trail of a basketball flying across the card.",
		back: "Horizontal, with head-shots and career stats.",
		markers: "220 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "at-50",
				label: "League at 50",
				treatment:
					"A silver-foil parallel carrying a golden-anniversary commemorative stamp.",
			},
		],
	},
	{
		id: "1997-98-topps",
		label: "1997-98 Topps",
		brand: "Topps",
		era: "premium",
		since: 1998,
		stock: "Heavy 16-point stock with foil stamping and spot-UV gloss.",
		border: "A white border.",
		photography: "Full-color action.",
		background:
			"The player set against the arcing motion trail of a moving basketball.",
		typography: "The player's name in gold foil over the ball's trail.",
		back: "Horizontal, with a stat grid.",
		markers: "220 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1998-99-topps",
		label: "1998-99 Topps",
		brand: "Topps",
		era: "premium",
		since: 1999,
		stock: "UV-gloss stock with foil stamping.",
		border:
			"THE SIGNATURE: a solid ORANGE frame running the full perimeter around the photo.",
		photography: "Bold in-arena color action, cropped tight inside the orange.",
		background: "The photo fills the frame edge to edge.",
		typography: "The player's name in gold foil.",
		back: "Horizontal, in color, with a stat grid.",
		markers: "257 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1997-98-topps-chrome",
		label: "1997-98 Topps Chrome",
		brand: "Topps",
		era: "premium",
		since: 1998,
		stock: "Chromium - a mirror-bright reflective metal surface.",
		border: "A white-bordered layout struck onto the chrome.",
		photography: "Color action.",
		background:
			"The player against a basketball's motion trail, the whole thing rendered on chromium.",
		typography: "The name in foil over the chrome.",
		back: "Chromium, with a stat grid.",
		markers:
			"220 cards - the chromium restrike of that season's flagship design, now sold in hobby as well as retail.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "refractor",
				label: "Refractor",
				treatment:
					"The chrome surface throws a full rainbow shine when tilted. Nothing printed on the card identifies it - only the finish does.",
			},
			{
				id: "destiny",
				label: "Destiny",
				treatment: "A chromium insert with its own heavy graphic treatment.",
			},
		],
	},
	{
		id: "1995-96-upper-deck",
		label: "1995-96 Upper Deck",
		brand: "Upper Deck",
		era: "premium",
		since: 1996,
		stock: "Glossy stock.",
		border: "Borderless, full-bleed.",
		photography: "Full-color action running to all four edges.",
		background: "Photographic.",
		typography: "The player's name in gold foil along the bottom.",
		back: "A color action shot with a career-statistics GRAPH, and the name and biography set vertically down the left edge in white type.",
		markers: "360 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "electric-court",
				label: "Electric Court",
				treatment:
					"A parallel with an electrified foil treatment worked across the design.",
			},
		],
	},
	{
		id: "1996-97-upper-deck",
		label: "1996-97 Upper Deck",
		brand: "Upper Deck",
		era: "premium",
		since: 1997,
		stock: "Glossy stock.",
		border:
			"A silver border, with a vertical panel of bronze foil textured like a basketball's pebbled surface running down the LEFT side.",
		photography: "A large, clear action photo.",
		background: "Photographic.",
		typography:
			'THE SIGNATURE: the actual DATE of the game the photo was taken is foil-stamped on the front beside the name - the "game dated" gimmick.',
		layout: "The bronze pebbled panel and the silver frame define the card.",
		back: "Player information and stats.",
		markers: "360 cards across two series.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1997-98-upper-deck",
		label: "1997-98 Upper Deck",
		brand: "Upper Deck",
		era: "premium",
		since: 1998,
		stock: "Glossy foil-stamped stock.",
		border: "Photo-forward with restrained framing.",
		photography: "Color action.",
		background: "Photographic.",
		typography: "A foil nameplate carrying the game-date stamp.",
		back: "Photo and stats.",
		markers:
			"360 cards. This is the product that introduced the game-worn jersey card to basketball - a swatch of actual fabric set into the card.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "game-jersey",
				label: "Game Jersey",
				treatment:
					"THE FIRST GAME-WORN JERSEY CARD: a rectangular window cut into the card face holding a real swatch of jersey fabric, the weave and stitching plainly visible, with a line of text on the front certifying it came from a game.",
			},
			{
				id: "jersey-auto",
				label: "Game Jersey Autograph",
				treatment:
					"A jersey-swatch window with an on-card signature written across the front in bold ink, serial numbered to a very small run.",
			},
		],
	},
	{
		id: "1995-96-skybox-premium",
		label: "1995-96 SkyBox Premium",
		brand: "SkyBox",
		era: "premium",
		since: 1996,
		stock: "Glossy stock.",
		border: "Full-bleed.",
		photography: "The player cut out from his background in full color.",
		background:
			"A single flat saturated color field - cyan, magenta, yellow or blue - with a computer-generated FLAME streaking out of the basketball in the player's hands.",
		typography: "Bold graphic lettering.",
		back: "Photo and stats.",
		markers: "301 cards; a partial return to graphic excess.",
		variants: [BASE, RC("No rookie emblem.")],
	},
	{
		id: "1995-96-ultra",
		label: "1995-96 Fleer Ultra",
		brand: "Fleer",
		era: "premium",
		since: 1996,
		stock: "Noticeably thicker than the previous Ultra, glossy.",
		border: "Photo-forward with minimal framing.",
		photography: "Full-color action.",
		background: "Photographic.",
		typography: "The name and team in gold foil along the bottom.",
		back: "Two color photographs plus one full black-and-white photograph, with the stats beneath.",
		markers: "350 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "gold-medallion",
				label: "Gold Medallion",
				treatment:
					"THE ULTRA PARALLEL: the entire background is restruck in gold foil, with a gold medallion emblem marking it.",
			},
		],
	},
	{
		id: "1996-97-ultra",
		label: "1996-97 Fleer Ultra",
		brand: "Fleer",
		era: "premium",
		since: 1997,
		stock: "Heavy foil stock - prone to visible edge wear.",
		border: "Full-bleed.",
		photography: "Full-bleed color action.",
		background: "Photographic under heavy foil.",
		typography: "Large flowing cursive lettering struck in heavy foil.",
		back: "Photo and stats.",
		markers: "300 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "gold-medallion",
				label: "Gold Medallion",
				treatment: "A gold-foil parallel marked with the medallion emblem.",
			},
			{
				id: "platinum-medallion",
				label: "Platinum Medallion",
				treatment:
					"A far scarcer platinum-foil parallel with its own medallion emblem.",
			},
		],
	},
	{
		id: "1997-98-ultra",
		label: "1997-98 Fleer Ultra",
		brand: "Fleer",
		era: "premium",
		since: 1998,
		stock: "Foil-accented premium stock.",
		border: "Photo-forward.",
		photography: "Color action.",
		background: "Photographic, with a colored foil field behind the nameplate.",
		typography: "The name in gold foil.",
		back: "Photo and stats.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "gold-medallion",
				label: "Gold Medallion",
				treatment: "A gold-foil parallel with the medallion emblem.",
			},
			{
				id: "platinum-medallion",
				label: "Platinum Medallion",
				treatment: "A scarce platinum-foil parallel.",
			},
		],
	},
	{
		id: "1998-99-ultra",
		label: "1998-99 Fleer Ultra",
		brand: "Fleer",
		era: "premium",
		since: 1999,
		stock: "Premium foil stock.",
		border: "Borderless.",
		photography: "Dynamic full-bleed action.",
		background: "Photographic.",
		typography: "A foil nameplate.",
		back: "Stats and information.",
		markers: "125 base cards plus a 25-card rookie tier.",
		variants: [
			BASE,
			RC("A short-printed rookie card seeded within the base set."),
			{
				id: "gold-medallion",
				label: "Gold Medallion",
				treatment: "A gold-foil parallel with the medallion emblem.",
			},
			{
				id: "platinum-medallion",
				label: "Platinum Medallion",
				treatment: "A serial-numbered platinum-foil parallel.",
			},
			{
				id: "masterpiece",
				label: "Masterpiece 1/1",
				treatment: 'A one-of-one printing plate style card, stamped "1/1".',
			},
		],
	},
	{
		id: "1995-96-collectors-choice",
		label: "1995-96 UD Collector's Choice",
		brand: "Upper Deck",
		era: "premium",
		since: 1996,
		stock: "Inexpensive glossy stock - this was the entry-level product.",
		border: "A plain white border.",
		photography: "Color action.",
		background: "Photographic.",
		typography:
			"The name, team and position tucked into one lower corner in plain type.",
		back: "A color photo with a full stat line.",
		markers: "410 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "silver-signature",
				label: "Silver Signature",
				treatment:
					"A parallel carrying a facsimile of the player's signature printed across the front in silver foil.",
			},
			{
				id: "gold-signature",
				label: "Gold Signature",
				treatment:
					"The scarcer version, with the facsimile signature struck in gold foil.",
			},
		],
	},
	{
		id: "1996-97-collectors-choice",
		label: "1996-97 UD Collector's Choice",
		brand: "Upper Deck",
		era: "premium",
		since: 1997,
		stock: "Inexpensive glossy stock.",
		border: "A white border.",
		photography: "Color action.",
		background: "Photographic.",
		typography: "Name and team in a lower corner.",
		back: "Color photo and stats.",
		markers: "400 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "silver-signature",
				label: "Silver Signature",
				treatment: "A facsimile signature in silver foil across the front.",
			},
			{
				id: "gold-signature",
				label: "Gold Signature",
				treatment:
					"A facsimile signature in gold foil - the scarcer of the two.",
			},
		],
	},
	{
		id: "1997-98-collectors-choice",
		label: "1997-98 UD Collector's Choice",
		brand: "Upper Deck",
		era: "premium",
		since: 1998,
		stock: "Inexpensive glossy stock.",
		border: "A white border.",
		photography: "Color action.",
		background: "Photographic.",
		typography: "Plain, in a lower corner.",
		back: "Player information and stats.",
		markers: "400 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "silver-signature",
				label: "Silver Signature",
				treatment: "A silver-foil facsimile signature across the front.",
			},
			{
				id: "gold-signature",
				label: "Gold Signature",
				treatment: "A gold-foil facsimile signature.",
			},
		],
	},
	{
		id: "1995-96-stadium-club",
		label: "1995-96 Topps Stadium Club",
		brand: "Topps",
		era: "premium",
		since: 1996,
		stock: "Full-bleed glossy stock with etched foil.",
		border: "Borderless.",
		photography:
			"Full-bleed full-color action - the brand's whole reason to exist.",
		background: "Photographic, edge to edge.",
		typography:
			"The player's name in etched foil set against an exploding starburst; the team name in gold foil along the bottom.",
		back: "A photo-and-stat back.",
		markers: "361 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "members-only",
				label: "Members Only",
				treatment: "A subscription parallel with its own foil stamp.",
			},
			{
				id: "beam-team",
				label: "Beam Team",
				treatment:
					"The famous insert: the player cut out against hard beams of neon light - dark and laser-like in the first series, bright green, yellow and red in the second.",
			},
		],
	},
	{
		id: "1996-97-stadium-club",
		label: "1996-97 Topps Stadium Club",
		brand: "Topps",
		era: "premium",
		since: 1997,
		stock: "Full-bleed glossy stock.",
		border: "Borderless.",
		photography: "Full-bleed color action.",
		background:
			"Photographic, with a gold circular motif worked into the design.",
		typography: "Foil lettering.",
		back: "Photo and stats.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "matrix",
				label: "Matrix",
				treatment:
					"A parallel with a rainbow foil grid pattern shimmering across the surface.",
			},
		],
	},
	{
		id: "1996-spx",
		label: "1996 SPx",
		brand: "Upper Deck",
		era: "premium",
		since: 1996,
		stock:
			"THE SIGNATURE: the entire card is a HOLOGRAM, and the entire card is die-cut - there is no ordinary printed surface anywhere on it.",
		border:
			"Die-cut shaped edges rather than a printed border - the card's own outline is cut to shape.",
		photography:
			"The player rendered holographically inside an oval window that sits on a silver rectangular field, with a small pinpoint marker in each of the four corners.",
		background:
			"Shifting holographic silver that changes with the viewing angle.",
		typography:
			"The set logo in gold foil across the top-center; the name, team and position centered along the bottom.",
		back: "A full-color image inside an oval, biography below, team-colored underlays, and the team logo at the top-center.",
		markers: "A 50-card set - the landmark hologram technology product.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "auto",
				label: "Autograph redemption",
				treatment:
					"A hologram card with an on-card signature - famously seeded at roughly one per thousand boxes.",
			},
		],
	},
	{
		id: "1996-97-spx",
		label: "1996-97 SPx",
		brand: "Upper Deck",
		era: "premium",
		since: 1997,
		stock: "Hologram stock, fully die-cut, sold one card to a pack.",
		border:
			"The whole card is die-cut so that its outline reads as the set's own three-letter logo.",
		photography: "A holographic player image.",
		background: "Shifting hologram.",
		typography: "The set logo in foil.",
		back: "Full-color, with stats.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "gold",
				label: "Gold",
				treatment: "The hologram struck with a gold cast.",
			},
			{
				id: "holoview-heroes",
				label: "Holoview Heroes",
				treatment:
					"A vertical hologram insert with a heavier three-dimensional depth effect.",
			},
		],
	},
	{
		id: "1996-97-sp",
		label: "1996-97 SP",
		brand: "Upper Deck",
		era: "premium",
		since: 1997,
		stock: "Premium glossy stock.",
		border:
			"Two vertical stripes run down the RIGHT side of the card, with a thin foil stripe forming the nameplate.",
		photography: "Color action.",
		background: "Restrained and photographic.",
		typography:
			"The team name runs VERTICALLY up the second stripe; the player's name sits on the foil nameplate along the bottom.",
		layout:
			"A cluster of small set logos at the bottom left, with a larger foil set logo at the top left.",
		back: "A standard photo-and-stat back.",
		markers:
			"146 cards; the rookie cards at the end of the set use a completely different design from the veterans.",
		variants: [
			BASE,
			RC(
				"Rookie cards in this set are visually distinct from the base design - a separate layout used only for the rookie subset.",
			),
			{
				id: "holoview",
				label: "Holoview",
				treatment: "An etched-foil insert with a holographic player image.",
			},
			{
				id: "game-film",
				label: "Game Film",
				treatment:
					"A die-cut insert built to look like a strip of film slide, the player framed inside the sprocket holes.",
			},
		],
	},
	{
		id: "1996-97-metal",
		label: "1996-97 Fleer Metal",
		brand: "Fleer",
		era: "premium",
		since: 1997,
		stock: "Etched silver foil, embossed so the design has physical relief.",
		border: "Borderless.",
		photography: "A full-color action player cut out from his background.",
		background:
			"THE SIGNATURE: actual METAL - riveted plates, girders and industrial steel structures behind the player.",
		typography:
			"The player's name in silver foil, embossed, running up the right side of the card.",
		layout: "The season is carried on the brand logo at the upper left.",
		back: "A full-color action shot over a steel background, with the team logo at the bottom.",
		markers: "250 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "precious-metal",
				label: "Precious Metal",
				treatment:
					"The background becomes solid silver with all color drained out of it.",
			},
		],
	},
	{
		id: "1996-97-z-force",
		label: "1996-97 SkyBox Z-Force",
		brand: "SkyBox",
		era: "premium",
		since: 1997,
		stock: "Glossy stock with foil accents.",
		border: "Loud multicolor graphic borders.",
		photography: "Color action.",
		background:
			'Bold saturated color with the letter "Z" repeated as a graphic motif throughout the design.',
		typography: "Aggressive angular lettering.",
		back: "Photo and stats.",
		markers: "200 cards across two series.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "z-peat",
				label: "Z-Peat",
				treatment: "A parallel restruck in embossed gold foil.",
			},
			{
				id: "big-man-on-court",
				label: "Big Man on Court",
				treatment:
					"A deeply die-cut insert, cut away on three of its four sides, with the insert name across the background in fat lowercase bubble letters - the first word heavy in red and orange, the rest in pale blue.",
			},
			{
				id: "slam-cam",
				label: "Slam Cam",
				treatment: "An insert with a refractive shifting background.",
			},
		],
	},
	{
		id: "1996-97-ex2000",
		label: "1996-97 SkyBox E-X2000",
		brand: "SkyBox",
		era: "premium",
		since: 1997,
		stock:
			"Clear acetate combined with cardboard and foil, die-cut - the immediate predecessor of E-X2001.",
		border: "Die-cut, effectively borderless.",
		photography:
			"The player die-cut and layered over the transparent acetate so the background shows through around him.",
		background: "Futuristic foil architecture behind the acetate.",
		typography: "Foil lettering.",
		back: "Visible through the clear portion, with stats on the opaque area.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "credentials",
				label: "Credentials",
				treatment:
					"A serial-numbered parallel of the acetate card with a distinct color scheme.",
			},
		],
	},
	{
		id: "black-diamond",
		label: "Upper Deck Black Diamond",
		brand: "Upper Deck",
		era: "premium",
		since: 1997,
		stock: "Dark, heavily foiled stock with a faceted diamond texture.",
		border: "A dark premium frame with diamond-cut facets catching the light.",
		photography: "Color action against the dark field.",
		background: "Black and near-black, with refractive diamond faceting.",
		typography: "Restrained foil lettering.",
		layout:
			"THE SIGNATURE: the set is tiered by diamonds - a card is issued at a single, double, triple or quadruple diamond level, each scarcer than the last, and the count of small diamond marks printed on the card front tells you which tier it is.",
		back: "Dark, with stats and the tier marking.",
		variants: [
			{
				id: "single",
				label: "Single Diamond",
				treatment: "The common tier, marked with one diamond emblem.",
			},
			{
				id: "double",
				label: "Double Diamond",
				treatment: "The second tier, marked with two diamond emblems.",
			},
			{
				id: "triple",
				label: "Triple Diamond",
				treatment: "The third tier, marked with three diamond emblems.",
			},
			{
				id: "quadruple",
				label: "Quadruple Diamond",
				treatment:
					"The rarest tier, marked with four diamond emblems and the richest foil treatment.",
			},
			{
				id: "diamond-cut",
				label: "Diamond Cut",
				treatment: "A die-cut card whose edges are cut into faceted points.",
			},
		],
	},
	{
		id: "1999-00-mystique",
		label: "1999-00 Fleer Mystique",
		brand: "Fleer",
		era: "premium",
		since: 2000,
		stock: "Glossy stock with silver foil.",
		border: "Sleek and minimal, built around the foil nameplate.",
		photography: "Color action.",
		background: "Restrained, letting the foil carry the design.",
		typography: "A silver-foil nameplate running along the lower edge.",
		back: "A standard photo-and-stat back.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "gold",
				label: "Gold",
				treatment:
					"The lower nameplate is restruck in gold foil in place of the silver.",
			},
			{
				id: "masterpiece",
				label: "Masterpiece 1/1",
				treatment: 'A one-of-one, stamped "1/1".',
			},
			{
				id: "fresh-ink",
				label: "Fresh Ink",
				treatment:
					"An autograph insert with the signature written directly on the card face.",
			},
		],
	},

	// ---------------------------------------------------------------- ERA 4
	{
		id: "2000-01-ultimate-collection",
		label: "Upper Deck Ultimate Collection",
		brand: "Upper Deck",
		era: "rpa",
		since: 2001,
		stock: "Thick premium stock with a heavy gloss.",
		border: "Clean premium framing with thin foil accent lines.",
		photography: "A tight portrait or a cleanly isolated action shot.",
		background: "Understated - a soft field or a subtle foil pattern.",
		typography: "Small restrained foil lettering.",
		back: "Premium and sparse, with the serial number prominent.",
		markers:
			"Everything in the product is serial numbered, and the scarcest signatures are numbered to match the player's jersey number.",
		variants: [
			{
				id: "base",
				label: "Base (serial numbered)",
				treatment: "Serial numbered on the front.",
			},
			RC("A serial-numbered rookie card."),
			{
				id: "game-jersey",
				label: "Ultimate Game Jersey",
				treatment:
					"A window holding a swatch of game-worn jersey, with the certification line beneath it.",
			},
			{
				id: "patch",
				label: "Ultimate Game Jersey Patch",
				treatment:
					"A larger window holding a multi-color patch rather than a plain swatch, serial numbered.",
			},
			{
				id: "signature-gold",
				label: "Ultimate Signatures Gold /25",
				treatment:
					'An on-card signature in bold ink with gold framing, stamped "/25".',
			},
		],
	},
	{
		id: "2004-05-bazooka",
		label: "2004-05 Topps Bazooka",
		brand: "Topps",
		era: "rpa",
		since: 2005,
		stock:
			"Ordinary paper stock, deliberately cheap and playful - this was the kid-oriented, bubble-gum-branded product of its era.",
		border: "A simple bright frame around the action photo.",
		photography: "Straightforward color action, nothing arty.",
		background: "Photographic, with bright cartoonish accents.",
		typography: "Fat, friendly, comic-styled lettering.",
		layout: "Bright primary colors and a bubble-gum brand mark.",
		back: "A cartoon-adjacent back with a stat line and a highlight blurb.",
		markers:
			"220 subjects. The whole product reads as a comic strip rather than a premium collectible.",
		variants: [
			BASE,
			RC("No rookie emblem."),
			{
				id: "gold-border",
				label: "Gold border",
				treatment: "The frame restruck as a metallic gold border.",
			},
			{
				id: "mini",
				label: "Mini",
				treatment: "A shrunken parallel of the card.",
			},
			{
				id: "comics",
				label: "Bazooka Comics",
				treatment:
					"The player's career highlight drawn out as an actual multi-panel comic strip across the card, in flat comic-book inks with speech balloons.",
			},
		],
	},
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
		photography:
			"An isolated action photo, the player cut from his background.",
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
					'THE BENCHMARK MODERN RPA: a large window holding a big multi-color jersey patch, with a bright-blue on-card autograph signed directly beside it, stamped "/99".',
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
		typography:
			"Donruss lettering with Rated Rookie branding where it applies.",
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
				treatment:
					"The background foil becomes a starfield and nebula pattern.",
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
				treatment:
					"Purple floods along the etched lines across the black base.",
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

	{
		id: "chronicles",
		label: "Panini Chronicles",
		brand: "Panini",
		era: "panini",
		since: 2020,
		stock:
			"MIXED BY DESIGN: each card is printed on whatever stock its own sub-brand uses - chromium, foilboard or plain paper - so no two cards in the product need match.",
		border:
			"Varies completely from card to card. Every card wears the full visual identity of a DIFFERENT Panini brand, and is labeled with that brand's name on the front.",
		photography: "Action, styled to whichever brand the card is imitating.",
		background: "Whatever the imitated brand does.",
		typography: "The imitated brand's own logo and lettering.",
		back: "Styled to the sub-brand, with a stat line.",
		markers:
			"A sampler product: one release housing dozens of separate brand designs side by side, each card announcing which brand it belongs to.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "bronze",
				label: "Bronze",
				treatment: "A bronze-toned parallel finish.",
			},
			{
				id: "pink",
				label: "Pink",
				treatment: "A pink parallel finish.",
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'A gold parallel finish, stamped "/10".',
			},
			{
				id: "black",
				label: "Black 1/1",
				treatment: 'A black parallel finish, stamped "1/1".',
			},
		],
	},
	{
		id: "preferred",
		label: "Panini Preferred",
		brand: "Panini",
		era: "panini",
		since: 2014,
		stock: "Premium thick stock; some cards are two-panel booklets that open.",
		border: "Ornate, with the card itself often die-cut to a crown silhouette.",
		photography: "The player die-cut from his background.",
		background: "Rich foil with regal ornamentation.",
		typography: "Ornate foil lettering.",
		back: "Premium, with the serial number.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "crown-die-cut",
				label: "Crown die-cut",
				treatment:
					"The card's outer edge is die-cut into the shape of a crown, so the card is not a rectangle.",
			},
			{
				id: "blue",
				label: "Blue /49",
				treatment: 'A blue parallel, stamped "/49".',
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'A gold parallel, stamped "/10".',
			},
			{
				id: "booklet",
				label: "Booklet",
				treatment:
					"An oversized two-panel card that opens like a book, with a large patch across one panel and an on-card signature on the other.",
			},
		],
	},
	{
		id: "opulence",
		label: "Panini Opulence",
		brand: "Panini",
		era: "panini",
		since: 2018,
		stock: "Ultra-luxury thick stock with heavy gold ornamentation.",
		border:
			"Elaborate gold framing - the most overtly expensive-looking design in the catalogue.",
		photography: "A cleanly isolated portrait.",
		background: "Deep jewel tones under gold filigree.",
		typography: "Gold foil, formal and ornate.",
		back: "Minimal, with the serial number.",
		variants: [
			{
				id: "base",
				label: "Base",
				treatment: "Serial numbered on the front.",
			},
			RC('Carries the "RC" rookie shield.'),
			{
				id: "silver",
				label: "Silver /25",
				treatment: 'Silver in place of the gold ornament, stamped "/25".',
			},
			{
				id: "holo-gold",
				label: "Holo Gold /10",
				treatment: 'A holographic gold treatment, stamped "/10".',
			},
			{
				id: "emerald",
				label: "Emerald /5",
				treatment: 'A deep green treatment, stamped "/5".',
			},
			{
				id: "platinum",
				label: "Platinum 1/1",
				treatment: 'The one-of-one, stamped "1/1".',
			},
		],
	},
	{
		id: "impeccable",
		label: "Panini Impeccable",
		brand: "Panini",
		era: "panini",
		since: 2018,
		stock:
			"Premium stock; the signature cards in this product are struck on actual METAL, and some embed a real silver or gold bar into the card face.",
		border: "Heavy metallic framing.",
		photography: "An isolated portrait or action shot.",
		background: "Brushed metal.",
		typography: "Engraved-looking metallic lettering.",
		back: "Minimal, with the serial number.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "silver",
				label: "Silver /75",
				treatment: 'A silver treatment, stamped "/75".',
			},
			{
				id: "gold",
				label: "Gold /49",
				treatment: 'A gold treatment, stamped "/49".',
			},
			{
				id: "holo-gold",
				label: "Holo Gold /10",
				treatment: 'A holographic gold treatment, stamped "/10".',
			},
			{
				id: "stainless-stars",
				label: "Stainless Stars",
				treatment:
					"The card is printed on a sheet of actual stainless steel - visibly metal, with a brushed grain and real weight to it.",
			},
			{
				id: "silver-bar",
				label: "Embedded silver bar",
				treatment:
					"A genuine stamped silver bullion bar is set into a window cut in the card face.",
			},
		],
	},
	{
		id: "gala",
		label: "Panini Gala",
		brand: "Panini",
		era: "panini",
		since: 2016,
		stock: "Premium stock with a glamorous high-gloss finish.",
		border: "An elegant frame evoking a film premiere.",
		photography:
			"THE SIGNATURE: wide-angle glamour photography - the player shot like a celebrity on a red carpet rather than an athlete in a game.",
		background: "Deep saturated color with a spotlit, staged feel.",
		typography: "Elegant thin lettering in foil.",
		back: "Minimal and premium, with the serial number.",
		markers: "Extremely short printed - the base cards run to single digits.",
		variants: [
			{
				id: "base",
				label: "Base /8",
				treatment: 'Serial numbered, stamped "/8".',
			},
			{
				id: "purple",
				label: "Purple /40",
				treatment: 'A purple treatment, stamped "/40".',
			},
			{
				id: "jade",
				label: "Jade /25",
				treatment: 'A jade-green treatment, stamped "/25".',
			},
			{
				id: "crimson",
				label: "Crimson /10",
				treatment: 'A deep red treatment, stamped "/10".',
			},
			{
				id: "midnight",
				label: "Midnight 1/1",
				treatment: 'A black treatment, stamped "1/1".',
			},
		],
	},
	{
		id: "one-and-one",
		label: "Panini One and One",
		brand: "Panini",
		era: "panini",
		since: 2022,
		stock:
			"Premium stock; cards ship already sealed in a rigid display holder.",
		border: "A clean modern frame built around one strong photograph.",
		photography:
			"Iconic, poster-like imagery rather than ordinary game action.",
		background: "Simple and bold, letting the photo carry the card.",
		typography: "Modern geometric lettering in foil.",
		back: "Minimal, with the serial number.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "orange",
				label: "Orange /49",
				treatment: 'An orange parallel, stamped "/49".',
			},
			{
				id: "blue",
				label: "Blue /35",
				treatment: 'A blue parallel, stamped "/35".',
			},
			{
				id: "red",
				label: "Red /15",
				treatment: 'A red parallel, stamped "/15".',
			},
			{
				id: "downtown",
				label: "Downtown",
				treatment:
					"A case-hit insert printed on metal: original illustrated artwork placing the player in a stylized version of his city's skyline, in bold poster colors rather than photography.",
			},
			{
				id: "black",
				label: "Black 1/1",
				treatment: 'The one-of-one, stamped "1/1".',
			},
		],
	},
	{
		id: "origins",
		label: "Panini Origins",
		brand: "Panini",
		era: "panini",
		since: 2021,
		stock: "Premium stock with a soft matte-to-foil transition.",
		border: "A clean frame over a deep field.",
		photography: "An isolated player cut from his background.",
		background:
			"THE SIGNATURE: abstract cosmic space - nebulae, star fields and drifting light, with the player floating in it.",
		typography: "Modern lettering in foil.",
		back: "Minimal, with the serial number.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "red",
				label: "Red /299",
				treatment: 'A red cosmic treatment, stamped "/299".',
			},
			{
				id: "turquoise",
				label: "Turquoise /25",
				treatment: 'A turquoise cosmic treatment, stamped "/25".',
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'A gold cosmic treatment, stamped "/10".',
			},
			{
				id: "animal-eyes",
				label: "Animal Eyes",
				treatment:
					"A short-printed insert: the player set inside the enormous close-up eye of a predatory animal, the iris filling the card.",
			},
			{
				id: "black",
				label: "Black 1/1",
				treatment: 'The one-of-one, stamped "1/1".',
			},
		],
	},
	{
		id: "recon",
		label: "Panini Recon",
		brand: "Panini",
		era: "panini",
		since: 2021,
		stock: "Foilboard with a holographic sheen.",
		border: "Bold hard-edged geometric shapes cutting across the card.",
		photography: "An isolated action player.",
		background:
			"Angular geometry in hot pink, purple and blue over a holographic foil field.",
		typography: "Sharp angular lettering.",
		back: "Standard, with a stat line.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "holo-bronze",
				label: "Holo Bronze /299",
				treatment: 'A bronze holographic treatment, stamped "/299".',
			},
			{
				id: "blue",
				label: "Blue /99",
				treatment: 'A blue holographic treatment, stamped "/99".',
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'A gold holographic treatment, stamped "/10".',
			},
			{
				id: "platinum",
				label: "Platinum 1/1",
				treatment: 'The one-of-one, stamped "1/1".',
			},
		],
	},
	{
		id: "certified",
		label: "Panini Certified",
		brand: "Panini",
		era: "panini",
		since: 2011,
		stock:
			'THE SIGNATURE: an etched-foil "mirror" surface - a chromium finish with a fine engraved pattern worked under the reflection.',
		border: "A mirrored foil frame.",
		photography: "An isolated player over the mirror surface.",
		background: "Etched mirror foil.",
		typography: "Foil lettering with the certification seal.",
		back: "Standard, with a stat line.",
		markers:
			"Nearly every card in the product is some flavor of Mirror parallel - that is the brand's organizing idea.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "mirror-blue",
				label: "Mirror Blue",
				treatment: "The etched mirror surface tinted blue.",
			},
			{
				id: "mirror-red",
				label: "Mirror Red",
				treatment: "The etched mirror surface tinted red.",
			},
			{
				id: "mirror-gold",
				label: "Mirror Gold /10",
				treatment: 'The etched mirror surface in gold, stamped "/10".',
			},
			{
				id: "mirror-black",
				label: "Mirror Black 1/1",
				treatment: 'The etched mirror surface in black, stamped "1/1".',
			},
		],
	},
	{
		id: "cornerstones",
		label: "Panini Cornerstones",
		brand: "Panini",
		era: "panini",
		since: 2018,
		stock:
			"Rigid PVC plastic and holographic foilboard rather than card stock - the card is translucent plastic in hand.",
		border: "A clean frame printed onto the plastic.",
		photography: "An isolated player.",
		background: "Holographic foil visible through the plastic.",
		typography: "Foil lettering.",
		back: "Printed on the plastic, with the serial number.",
		markers: "The parallel tiers are named after minerals rather than colors.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "crystal",
				label: "Crystal /75",
				treatment: 'A clear crystalline treatment, stamped "/75".',
			},
			{
				id: "granite",
				label: "Granite /25",
				treatment: 'A speckled stone treatment, stamped "/25".',
			},
			{
				id: "marble",
				label: "Marble /10",
				treatment: 'A veined marble treatment, stamped "/10".',
			},
			{
				id: "quad-relic-auto",
				label: "Quad relic autograph",
				treatment:
					"Four separate jersey-swatch windows, one at each corner of the card, with an on-card signature between them.",
			},
			{
				id: "onyx",
				label: "Onyx 1/1",
				treatment: 'A black stone treatment, stamped "1/1".',
			},
		],
	},
	{
		id: "absolute",
		label: "Panini Absolute Memorabilia",
		brand: "Panini",
		era: "panini",
		since: 2013,
		stock:
			"Premium stock; the base cards ship pre-encased in a sealed rigid holder rather than loose.",
		border: "A bold modern frame with heavy foil.",
		photography: "An isolated action player.",
		background: "Saturated foil color.",
		typography: "Heavy foil lettering.",
		back: "Standard, with a stat line and the serial number.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "red",
				label: "Red /199",
				treatment: 'A red parallel, stamped "/199".',
			},
			{
				id: "purple",
				label: "Purple /25",
				treatment: 'A purple parallel, stamped "/25".',
			},
			{
				id: "gold",
				label: "Gold /10",
				treatment: 'A gold parallel, stamped "/10".',
			},
			{
				id: "tools-of-the-trade",
				label: "Tools of the Trade",
				treatment:
					"A card with several separate memorabilia windows in a row across the front, each holding a different swatch.",
			},
		],
	},
	{
		id: "excalibur",
		label: "Panini Excalibur",
		brand: "Panini",
		era: "panini",
		since: 2015,
		stock:
			"Paper base cards, with the chromium inserts printed on a refractive foil surface.",
		border: "Ornate medieval framing - shields, scrollwork and heraldry.",
		photography: "An isolated player set into the heraldic frame.",
		background:
			"Stone, parchment and banner motifs, with a griffin device recurring.",
		typography: "Blackletter-inflected lettering.",
		back: "Standard, with a stat line.",
		markers:
			"The parallel tiers are named for ranks of nobility rather than colors, ascending in rarity.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "duke",
				label: "Duke /49",
				treatment: 'A higher-rank parallel, stamped "/49".',
			},
			{
				id: "king",
				label: "King /10",
				treatment: 'A near-top-rank parallel, stamped "/10".',
			},
			{
				id: "crusade",
				label: "Crusade",
				treatment:
					"The chromium insert line: the player on a refractive rainbow foil surface inside a crusader-shield frame.",
			},
			{
				id: "kaboom",
				label: "Kaboom!",
				treatment:
					"THE CASE-HIT CHASE CARD: the player redrawn entirely as hand-illustrated comic-book art - bold ink outlines, flat saturated comic colors, dramatic foreshortening - exploding out of a jagged comic starburst that fills the card behind him. No photograph anywhere on it.",
			},
			{
				id: "emperor",
				label: "Emperor 1/1",
				treatment: 'The top rank, stamped "1/1".',
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
		id: "2025-26-topps-chrome-update",
		label: "2025-26 Topps Chrome Update",
		brand: "Topps",
		era: "fanatics",
		since: 2026,
		stock: "Chromium - a mirror-bright reflective surface.",
		border: "A clean chrome frame.",
		photography:
			"In-season photography, deliberately showing the alternate uniforms a team wears mid-year rather than its standard home or road set.",
		background: "Refractive chrome.",
		typography: "Modern foil lettering.",
		back: "Chromium, with a stat line.",
		markers:
			"The mid-season companion to the flagship chromium set - the same look, updated photography.",
		variants: [
			BASE,
			RC(
				'Carries the "RC" rookie shield, using the player\'s first in-game photography.',
			),
			{
				id: "refractor",
				label: "Refractor",
				treatment: "The chrome throws a full rainbow shine when tilted.",
			},
			{
				id: "raywave",
				label: "RayWave Refractor",
				treatment:
					"A refractor patterned with broad rippling waves running through the rainbow.",
			},
			{
				id: "debut-patch-auto",
				label: "Debut Patch Auto 1/1",
				treatment:
					'A one-of-one holding the actual patch worn in the player\'s first professional game, with an on-card signature, stamped "1/1".',
			},
			{
				id: "alter-egos",
				label: "Alter Egos",
				treatment:
					"A short-printed insert: the player redrawn as an illustrated super-powered alter ego, in comic-book rendering rather than photography.",
			},
		],
	},
	{
		id: "2025-26-topps-chrome-black",
		label: "2025-26 Topps Chrome Black",
		brand: "Topps",
		era: "fanatics",
		since: 2026,
		stock: "Dark chromium - a black mirror rather than a silver one.",
		border: "A darkened chrome frame, nearly black at the edges.",
		photography: "Color action, lit against the dark field.",
		background:
			"A busy dark chrome field - the whole card reads black-on-black with reflective highlights.",
		typography: "Foil lettering that glows against the black.",
		back: "Dark chromium, with a stat line.",
		markers:
			"A separate release rather than a parallel of the standard chromium set - the black surface is the product.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "blue-wave",
				label: "Blue Wave /150",
				treatment: 'A blue wave-patterned refractor, stamped "/150".',
			},
			{
				id: "green",
				label: "Green /99",
				treatment: 'A green refractor, stamped "/99".',
			},
			{
				id: "carbon-flare",
				label: "Carbon Flare",
				treatment:
					"An insert with a carbon-fiber weave texture under a burst of colored light.",
			},
			{
				id: "glow-up-auto",
				label: "Glow Up Signatures",
				treatment:
					"An autograph insert whose background luminesces against the black stock, with an on-card signature.",
			},
		],
	},
	{
		id: "2025-26-inception",
		label: "2025-26 Topps Inception",
		brand: "Topps",
		era: "fanatics",
		since: 2026,
		stock: "Thick premium art stock with dramatic color treatment.",
		border:
			"Bold painterly artwork forming the border, closer to a gallery print than a sports card.",
		photography:
			"Upscale, art-directed imagery rather than ordinary game action.",
		background:
			"Large fields of saturated color washed behind the player, art-inspired rather than photographic.",
		typography: "Restrained premium lettering.",
		back: "Premium, with a stat line and the serial number.",
		variants: [
			BASE,
			RC('Carries the "RC" rookie shield.'),
			{
				id: "silver-signings",
				label: "Silver Signings",
				treatment:
					"A horizontal card with the signature hard-signed on the card face in silver ink.",
			},
			{
				id: "gold-ink",
				label: "Gold Ink 1/1",
				treatment:
					'The same on-card signature written in gold ink instead of silver, stamped "1/1".',
			},
			{
				id: "booklet",
				label: "Booklet",
				treatment:
					"An oversized card that opens into two panels, with a patch on one side and a signature on the other.",
			},
		],
	},
	{
		id: "2025-26-signature-series-unlicensed",
		label: "2025-26 Panini Signature Series (unlicensed)",
		brand: "Panini",
		era: "fanatics",
		since: 2026,
		stock: "Premium stock with foil accents.",
		border: "A clean modern frame in neutral color blocks.",
		photography:
			"A tight close-up PORTRAIT, cropped deliberately high so the jersey barely appears in frame.",
		background: "A flat neutral color block.",
		typography:
			"The player's name with his team's CITY beneath it - never a nickname.",
		back: "Minimal, with a short stat line.",
		markers:
			"IMPORTANT: this product is UNLICENSED. Use the player's name and his team's CITY only. NO team nickname, NO team logo, NO team wordmark may appear anywhere on the card, and any jersey visible in the tight crop must have its identifying marks removed. This constraint is the whole visual signature of the product - the tight portrait crop exists precisely to keep the uniform out of frame.",
		variants: [
			{
				id: "base",
				label: "Base /199",
				treatment: 'Serial numbered, stamped "/199".',
			},
			RC("A serial-numbered rookie card."),
			{
				id: "auto",
				label: "On-card autograph",
				treatment:
					"The signature written directly across the portrait in bold ink, serial numbered.",
			},
			{
				id: "kaboom",
				label: "Kaboom!",
				treatment:
					"The player redrawn entirely as hand-illustrated comic art exploding out of a jagged comic starburst - no photograph, and still no team marks.",
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
