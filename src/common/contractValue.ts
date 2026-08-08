// How good a contract is: what a player's production was worth on the open
// market, against what he is actually paid.
//
// The naive version of this - a rating divided by salary - reads well for the
// two players you thought of when you had the idea, and falls apart everywhere
// else. Division makes the denominator dominate, so every minimum-salary end of
// the bench outranks every star: a 40 ovr scrub on the minimum scores higher
// than an MVP on a max, because dividing by a small number is all it takes. It
// also can't say whether a contract is good in any absolute sense, only that it
// is better than another one.
//
// So this follows how the question is actually answered for real contracts
// (see the references below): convert production to wins, price a win in
// dollars, and SUBTRACT. Subtraction is what makes the scale mean something -
// zero is "paid exactly what he was worth", and the units are dollars you saved
// or wasted rather than an index number.
//
//   war          = vorp * WINS_PER_VORP
//   marketValue  = minContract + war * dollarsPerWin
//   surplus      = marketValue - salary
//
// Anchoring on the minimum salary (rather than at zero) is the part that fixes
// the bench: everybody costs at least the minimum whatever they do, so a
// minimum-salary player who produces nothing lands at a surplus of zero -
// correctly neutral, neither a bargain nor a mistake - and only real production
// moves him up.
//
// EXPECT MARKET VALUES ABOVE THE MAX CONTRACT. A superstar season is worth far
// more than any team is allowed to pay for it - that is the entire reason a
// star on a rookie deal is such a prize - so the best player in a league can
// price out at $80M against a $50M max. That is the finding, not a bug in the
// arithmetic. Measured on a simulated league, every player's implied price came
// out at the same $3.40M per win, against $4.80M for the independent real-NBA
// anchor below: close enough to corroborate each other, which is the point of
// keeping both.
//
// References:
//  - Basketball-Reference's glossary, for VORP and its ~2.7 wins per point
//    conversion: https://www.basketball-reference.com/about/glossary.html
//  - FiveThirtyEight, "2016 Is A Great Summer To Be A Mediocre NBA Free Agent",
//    for pricing a win as a share of the cap ON TOP of the minimum salary,
//    which is what makes this work at any cap:
//    https://fivethirtyeight.com/features/2016-is-a-great-summer-to-be-a-mediocre-nba-free-agent

// VORP is points above replacement per 100 team possessions, prorated to a full
// season. Basketball-Reference converts it to wins with this multiplier.
export const WINS_PER_VORP = 2.7;

// FALLBACK ONLY - see getDollarsPerWin, which prefers to measure this from the
// league itself. Real NBA teams pay about 3.2% of the salary cap, on top of the
// minimum salary, per win above replacement. Expressed as a share of the cap
// rather than in dollars so it survives any cap setting: at the default
// $150M cap it prices a win at $4.8M, which puts a 10-win season at $49.2M
// against a $50M max - about right.
export const CAP_SHARE_PER_WIN = 0.032;

export type ContractValueInput = {
	// Season VORP. Undefined for anyone who hasn't played (rookies, offseason
	// signings), which is treated as no production rather than bad production.
	vorp: number | undefined;
	// Contract amount, in thousands, like everything else in the league file.
	salary: number;
};

export type ContractValue = {
	war: number;
	marketValue: number;
	// Positive is a bargain, negative is an overpay, both in thousands.
	surplus: number;
};

// Everything that went into one player's number, so the UI can show the sum
// rather than just its answer. A number nobody can check is a number nobody
// trusts, and this one is derived from enough steps to be worth showing.
export type ContractValueBreakdown = ContractValue & {
	vorp: number;
	salary: number;
	dollarsPerWin: number;
	minContract: number;
};

export type ContractValueSettings = {
	minContract: number;
	salaryCap: number;
};

export const warFromVorp = (vorp: number | undefined): number =>
	vorp === undefined || !Number.isFinite(vorp) ? 0 : vorp * WINS_PER_VORP;

// What one win above replacement costs in this league, measured from the league
// itself: the total payroll ABOVE what it would cost to fill every roster spot
// at the minimum, divided by all the wins above replacement that money bought.
//
// Measuring rather than assuming is what makes this work in a league that isn't
// the modern NBA. A 41-game season, a season only a quarter played, a hard cap,
// a $10M cap, a 50-team league - all of them scale payroll and VORP together,
// so the ratio between them stays honest while any fixed dollar figure would
// not. It is also why the number is recomputed per season instead of stored.
export const getDollarsPerWin = (
	players: readonly ContractValueInput[],
	{ minContract, salaryCap }: ContractValueSettings,
): number => {
	const fallback = CAP_SHARE_PER_WIN * salaryCap;

	let totalWins = 0;
	let aboveFloor = 0;
	for (const p of players) {
		// Only wins above replacement bid the price up. Sub-replacement players
		// are being paid despite their production, not for it, so counting them
		// would cheapen every win in the league.
		totalWins += Math.max(0, warFromVorp(p.vorp));
		aboveFloor += p.salary - minContract;
	}

	if (totalWins <= 0 || aboveFloor <= 0 || !Number.isFinite(aboveFloor)) {
		// Nothing to measure yet - preseason, or a league where everyone is on
		// the minimum. Fall back to the real-NBA share of the cap.
		return fallback;
	}

	return aboveFloor / totalWins;
};

export const getContractValue = (
	{ vorp, salary }: ContractValueInput,
	minContract: number,
	dollarsPerWin: number,
): ContractValue => {
	const war = warFromVorp(vorp);
	// Floored at zero: a sub-replacement player is worth nothing on the open
	// market, but nobody is worth a NEGATIVE salary, and letting market value go
	// below zero would punish bad players twice over.
	const marketValue = Math.max(0, minContract + war * dollarsPerWin);

	return {
		war,
		marketValue,
		surplus: marketValue - salary,
	};
};

// The whole thing in one call, for a set of players that should be priced
// against each other - normally everyone in the league in a given season.
export const getContractValues = <T extends ContractValueInput>(
	players: readonly T[],
	settings: ContractValueSettings,
): Map<T, ContractValue> => {
	const dollarsPerWin = getDollarsPerWin(players, settings);
	return new Map(
		players.map((p) => [
			p,
			getContractValue(p, settings.minContract, dollarsPerWin),
		]),
	);
};
