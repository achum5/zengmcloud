// Rarity tiers, in one place.
//
// A cell's score is 10-100, higher meaning fewer people would have thought of
// that player. Six tiers rather than four so a lucky obvious answer and a
// genuinely deep cut don't land on the same color, and one definition so the
// cell, the badge and the shared square block can never disagree.
//
// Colors are the app's own semantic set, ordered by how they actually render
// in this theme rather than by Bootstrap's names: gray, cyan, green, yellow,
// orange, red. `primary` is orange here, so it belongs near the hot end.

export type Tier = {
	label: string;
	// The CSS custom property to fill with.
	color: string;
	// Bootstrap utility for a filled badge in the same color.
	badge: string;
};

const TIERS: { min: number; tier: Tier }[] = [
	{
		min: 90,
		tier: {
			label: "Mythic",
			color: "var(--bs-danger)",
			badge: "text-bg-danger",
		},
	},
	{
		min: 75,
		tier: {
			label: "Legendary",
			color: "var(--bs-primary)",
			badge: "text-bg-primary",
		},
	},
	{
		min: 60,
		tier: {
			label: "Epic",
			color: "var(--bs-warning)",
			badge: "text-bg-warning",
		},
	},
	{
		min: 40,
		tier: {
			label: "Rare",
			color: "var(--bs-success)",
			badge: "text-bg-success",
		},
	},
	{
		min: 20,
		tier: { label: "Uncommon", color: "var(--bs-info)", badge: "text-bg-info" },
	},
	{
		min: -Infinity,
		tier: {
			label: "Common",
			color: "var(--bs-secondary)",
			badge: "text-bg-secondary",
		},
	},
];

// An unsolved cell. Deliberately not a tier - it is the absence of one.
export const EMPTY_COLOR = "var(--bs-border-color)";

export const tierOf = (points: number): Tier =>
	TIERS.find((t) => points >= t.min)!.tier;

export const tierColor = (points: number | null | undefined): string =>
	points === null || points === undefined ? EMPTY_COLOR : tierOf(points).color;
