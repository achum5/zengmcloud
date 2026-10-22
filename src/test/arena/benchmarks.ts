// Real NBA reference points, mostly 2010-2026, gathered from
// Basketball-Reference standings and continuity pages, Hoops Rumors trade
// lists, NBA.com, CBS, Spotrac and similar. Ranges are what the arena treats as
// "looks like the NBA"; the weakest numbers (dead money, cap-space teams) are
// given wide ones on purpose.
export type Benchmark = {
	label: string;
	nba: string;
	low: number;
	high: number;
	percent?: boolean;
};

export const BENCHMARKS = {
	winSd: { label: "Spread of team wins (SD)", nba: "12.4", low: 10, high: 14 },
	yoyCorr: {
		label: "Wins carried over to next season (correlation)",
		nba: "0.60",
		low: 0.5,
		high: 0.7,
	},
	teams60: {
		label: "60-win teams per season",
		nba: "1.6",
		low: 0.8,
		high: 2.5,
	},
	under25: {
		label: "Sub-25-win teams per season",
		nba: "3.7",
		low: 2,
		high: 5,
	},
	bestRecordTitle: {
		label: "Best record wins the title",
		nba: "35-45%",
		low: 0.3,
		high: 0.5,
		percent: true,
	},
	championWins: { label: "Champion's wins", nba: "59", low: 55, high: 63 },
	distinctChampions: {
		label: "Different champions per 10 seasons",
		nba: "5-7 (8 in the last 8)",
		low: 5,
		high: 8,
	},
	middleShare: {
		label: "Team-seasons in the middle (35-47 wins)",
		nba: "33%",
		low: 0.25,
		high: 0.42,
		percent: true,
	},
	middleStuck: {
		label: "Team-seasons stuck in the middle 4+ years running",
		nba: "2%",
		low: 0,
		high: 0.05,
		percent: true,
	},
	rebuildMedian: {
		label: "Rebuild length, sub-.400 to playoffs (median seasons)",
		nba: "3",
		low: 2,
		high: 4,
	},
	ageTop4: {
		label: "Age of the top 4 teams",
		nba: "27.4",
		low: 26.5,
		high: 28.5,
	},
	ageBottom5: {
		label: "Age of the bottom 5 teams",
		nba: "24.8",
		low: 24,
		high: 25.8,
	},
	continuity: {
		label: "Minutes from returning players",
		nba: "67-70%",
		low: 0.6,
		high: 0.76,
		percent: true,
	},
	trades: { label: "Trades per league year", nba: "55-62", low: 45, high: 70 },
	inSeasonTrades: {
		label: "In-season trades",
		nba: "~27",
		low: 20,
		high: 35,
	},
	offseasonTrades: {
		label: "Offseason trades (draft through free agency)",
		nba: "~30",
		low: 22,
		high: 38,
	},
	deadlineWeek: {
		label: "In-season trades in the deadline's final week",
		nba: "~75%",
		low: 0.6,
		high: 0.9,
		percent: true,
	},
	playersPerTrade: {
		label: "Players per trade",
		nba: "1.7 offseason, 2.9 in-season",
		low: 1.5,
		high: 3.2,
	},
	tradesWithFirst: {
		label: "Trades including a first-round pick",
		nba: "30-40%",
		low: 0.25,
		high: 0.45,
		percent: true,
	},
	firsts: {
		label: "First-round picks traded per year",
		nba: "25-35",
		low: 20,
		high: 40,
	},
	starMoves: {
		label: "Recent All-Stars changing teams per year",
		nba: "~6",
		low: 3.5,
		high: 9,
	},
	starMovesByTrade: {
		label: "...of which by trade",
		nba: "60-65%",
		low: 0.5,
		high: 0.75,
		percent: true,
	},
	taxpayers: {
		label: "Luxury tax teams per season",
		nba: "~6 (2-10)",
		low: 3,
		high: 10,
	},
	capSpaceTeams: {
		label: "Teams with real cap room in free agency (15%+ of cap)",
		nba: "4-8 (2-4 lately)",
		low: 2,
		high: 8,
	},
	deadMoneyMedian: {
		label: "Dead money, median team (share of payroll)",
		nba: "~2%",
		low: 0,
		high: 0.05,
		percent: true,
	},
	top10FaStay: {
		label: "Top-10 free agents who stay with their team",
		nba: "~57%",
		low: 0.45,
		high: 0.7,
		percent: true,
	},
	// Rookie deals ending are most of the top 10 by value, and stay more, as
	// NBA players do on extensions and restricted free agency.
	top10VetFaStay: {
		label: "...veterans only, not off a rookie deal",
		nba: "~57%",
		low: 0.45,
		high: 0.7,
		percent: true,
	},
} satisfies Record<string, Benchmark>;
