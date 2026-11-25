// The all-time lists on a player's page (All-Time Ranks), basketball: every
// career total, the career rates with what it takes to be on their list, and
// the playoffs' totals. The worker ranks them (worker/util/allTimeRanks.ts),
// the page shows them.

export type AllTimeScope = "totals" | "perGame" | "playoffs";

export type AllTimeFormat = "int" | "dec1" | "pct" | "dec3" | "signed1";

export type AllTimeStat = {
	key: string;
	abbrev: string;
	title: string;
	format: AllTimeFormat;
	// Its column on Player Stats, for the full list, if it has one there.
	table?: "totals" | "advanced";
};

export const ALL_TIME_TOTALS: AllTimeStat[] = [
	{
		key: "pts",
		abbrev: "PTS",
		title: "Points",
		format: "int",
		table: "totals",
	},
	{
		key: "trb",
		abbrev: "REB",
		title: "Rebounds",
		format: "int",
		table: "totals",
	},
	{
		key: "ast",
		abbrev: "AST",
		title: "Assists",
		format: "int",
		table: "totals",
	},
	{
		key: "stl",
		abbrev: "STL",
		title: "Steals",
		format: "int",
		table: "totals",
	},
	{
		key: "blk",
		abbrev: "BLK",
		title: "Blocks",
		format: "int",
		table: "totals",
	},
	{
		key: "tp",
		abbrev: "3PM",
		title: "3-Pointers",
		format: "int",
		table: "totals",
	},
	{ key: "gp", abbrev: "G", title: "Games", format: "int", table: "totals" },
	{
		key: "min",
		abbrev: "MIN",
		title: "Minutes",
		format: "int",
		table: "totals",
	},
	{
		key: "fg",
		abbrev: "FG",
		title: "Field Goals",
		format: "int",
		table: "totals",
	},
	{
		key: "ft",
		abbrev: "FT",
		title: "Free Throws",
		format: "int",
		table: "totals",
	},
	{
		key: "orb",
		abbrev: "ORB",
		title: "Offensive Rebounds",
		format: "int",
		table: "totals",
	},
	{
		key: "drb",
		abbrev: "DRB",
		title: "Defensive Rebounds",
		format: "int",
		table: "totals",
	},
	{
		key: "ws",
		abbrev: "WS",
		title: "Win Shares",
		format: "dec1",
		table: "advanced",
	},
	{
		key: "ows",
		abbrev: "OWS",
		title: "Offensive Win Shares",
		format: "dec1",
		table: "advanced",
	},
	{
		key: "dws",
		abbrev: "DWS",
		title: "Defensive Win Shares",
		format: "dec1",
		table: "advanced",
	},
	{
		key: "vorp",
		abbrev: "VORP",
		title: "Value Over Replacement",
		format: "dec1",
		table: "advanced",
	},
	{ key: "dd", abbrev: "DD", title: "Double-Doubles", format: "int" },
	{ key: "td", abbrev: "TD", title: "Triple-Doubles", format: "int" },
	{
		key: "gs",
		abbrev: "GS",
		title: "Games Started",
		format: "int",
		table: "totals",
	},
	{
		key: "fga",
		abbrev: "FGA",
		title: "Field Goal Attempts",
		format: "int",
		table: "totals",
	},
	{
		key: "tpa",
		abbrev: "3PA",
		title: "3-Point Attempts",
		format: "int",
		table: "totals",
	},
	{
		key: "fta",
		abbrev: "FTA",
		title: "Free Throw Attempts",
		format: "int",
		table: "totals",
	},
	{
		key: "tov",
		abbrev: "TOV",
		title: "Turnovers",
		format: "int",
		table: "totals",
	},
	{
		key: "pf",
		abbrev: "PF",
		title: "Personal Fouls",
		format: "int",
		table: "totals",
	},
];

// Basketball-Reference's career minimums, for a league of 82 games of 48
// minutes (scaled to this league's).
export const ALL_TIME_PER_GAME: (AllTimeStat & {
	min: { stat: "gp" | "fg" | "tp" | "ft" | "min"; value: number };
})[] = [
	{
		key: "ppg",
		abbrev: "PPG",
		title: "Points Per Game",
		format: "dec1",
		min: { stat: "gp", value: 400 },
	},
	{
		key: "rpg",
		abbrev: "RPG",
		title: "Rebounds Per Game",
		format: "dec1",
		min: { stat: "gp", value: 400 },
	},
	{
		key: "apg",
		abbrev: "APG",
		title: "Assists Per Game",
		format: "dec1",
		min: { stat: "gp", value: 400 },
	},
	{
		key: "spg",
		abbrev: "SPG",
		title: "Steals Per Game",
		format: "dec1",
		min: { stat: "gp", value: 400 },
	},
	{
		key: "bpg",
		abbrev: "BPG",
		title: "Blocks Per Game",
		format: "dec1",
		min: { stat: "gp", value: 400 },
	},
	{
		key: "mpg",
		abbrev: "MPG",
		title: "Minutes Per Game",
		format: "dec1",
		min: { stat: "gp", value: 400 },
	},
	{
		key: "fgp",
		abbrev: "FG%",
		title: "Field Goal Percentage",
		format: "pct",
		min: { stat: "fg", value: 2000 },
	},
	{
		key: "tpp",
		abbrev: "3P%",
		title: "3-Point Percentage",
		format: "pct",
		min: { stat: "tp", value: 250 },
	},
	{
		key: "ftp",
		abbrev: "FT%",
		title: "Free Throw Percentage",
		format: "pct",
		min: { stat: "ft", value: 1200 },
	},
	{
		key: "per",
		abbrev: "PER",
		title: "Player Efficiency Rating",
		format: "dec1",
		min: { stat: "min", value: 15000 },
	},
	{
		key: "ws48",
		abbrev: "WS/48",
		title: "Win Shares Per 48 Minutes",
		format: "dec3",
		min: { stat: "min", value: 15000 },
	},
	{
		key: "bpm",
		abbrev: "BPM",
		title: "Box Plus/Minus",
		format: "signed1",
		min: { stat: "min", value: 15000 },
	},
];

export const allTimeStats = (scope: AllTimeScope): AllTimeStat[] =>
	scope === "perGame" ? ALL_TIME_PER_GAME : ALL_TIME_TOTALS;
