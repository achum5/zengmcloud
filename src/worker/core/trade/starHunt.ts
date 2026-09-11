// ---------------------------------------------------------------------------
// THE STAR HUNT - a contender in striking distance goes and gets the guy.
//
// Twelve seasons of thirty AI teams produced, across three seeds, zero star
// hunts and one star sale. Not because nobody wanted one: every all-in
// posture, every star-gap flag and every blockbuster ceiling was in place. The
// deals never came together for two mechanical reasons.
//
//   1. THE SEED WAS ON THE WRONG SIDE. A buyer opened by putting a pick or a
//      spare body on the table and letting the partner add what it liked. A
//      partner adds only what keeps the deal good for ITSELF, so the reply to
//      "here is a first" is a role player, never a star - a star would tip the
//      deal against the partner and is filtered out before it is considered.
//      The trades people propose to the AI do this the right way round: name
//      the player you want, then pay. So does this.
//
//   2. THE SALARY MATCH KILLED THE REST. Over twenty simulated seasons the
//      soft-cap match accounted for 92% of every guarded offer that died,
//      and a star carries the biggest salary on the floor. Generic filler was
//      measured and rejected - it moves bad money around the league - but a
//      star hunt is not generic: there are a handful a season, and sending
//      back salary is simply what the deal costs. So the hunter seeds its own
//      side with the ballast the match rule will demand, chosen the way a
//      front office chooses it: the contracts it values least per dollar.
//
// Pure - no database - so the arithmetic is unit-testable.
// ---------------------------------------------------------------------------

// How often a contender in striking distance opens with a hunt rather than
// by shopping its own spare parts. Higher when it has no star at all.
export const HUNT_CHANCE_STAR_GAP = 0.6;
export const HUNT_CHANCE = 0.35;

// The most bodies a hunter sends purely to make the money work. Real
// blockbusters carry two or three; past that the receiving side is being
// handed a roster, not a return.
export const MAX_BALLAST = 3;

// The least outgoing salary that lets a team take on `incoming` without the
// trade being refused on cap grounds. Zero when the team can absorb it.
export const ballastNeeded = ({
	incoming,
	payroll,
	salaryCap,
	salaryCapType,
	softCapTradeSalaryMatch,
}: {
	incoming: number;
	payroll: number;
	salaryCap: number;
	salaryCapType: string;
	// Percent: incoming may be at most this share of outgoing when over the
	// cap after the deal (125 means 125%).
	softCapTradeSalaryMatch: number;
}): number => {
	if (!(incoming > 0) || salaryCapType === "none") {
		return 0;
	}
	// Enough outgoing to finish under the cap satisfies every rule.
	const toGetUnder = Math.max(0, payroll + incoming - salaryCap);
	if (toGetUnder === 0) {
		return 0;
	}
	if (salaryCapType === "hard") {
		return toGetUnder;
	}
	// Soft cap: either finish under the cap, or match within the ratio.
	const match =
		softCapTradeSalaryMatch > 0
			? (incoming * 100) / softCapTradeSalaryMatch
			: incoming;
	return Math.min(toGetUnder, match);
};

// The most salary the OTHER side can take back for the star it sends - the
// seller has a cap too, and most of the hunts that died before this existed
// died there: the hunter matched its own money and then paid in salaried
// players until the seller, over the cap, was taking back more than the rule
// allows. Infinity when the seller can absorb anything.
export const incomingCeiling = ({
	outgoing,
	payroll,
	salaryCap,
	salaryCapType,
	softCapTradeSalaryMatch,
}: {
	// What the seller sends (the star's salary).
	outgoing: number;
	payroll: number;
	salaryCap: number;
	salaryCapType: string;
	softCapTradeSalaryMatch: number;
}): number => {
	if (salaryCapType === "none") {
		return Infinity;
	}
	// Anything that leaves the seller under the cap is fine.
	const toStayUnder = Math.max(0, salaryCap - payroll + outgoing);
	if (salaryCapType === "hard") {
		return toStayUnder;
	}
	const match =
		softCapTradeSalaryMatch > 0
			? (outgoing * softCapTradeSalaryMatch) / 100
			: outgoing;
	return Math.max(toStayUnder, match);
};

// Which contracts to send to reach `needed` without passing `ceiling`: the
// shortest first - an expiring deal costs the team taking it nothing once
// the season ends, which is why real blockbusters are padded with expirings
// - and among equals the ones worth least per dollar, which is how a front
// office picks who goes along in a big deal. Undefined when it cannot be
// reached within MAX_BALLAST bodies - the hunt at this partner is then
// simply off.
export const pickBallast = (
	players: readonly {
		pid: number;
		value: number;
		amount: number;
		// Seasons left on the deal after this one (0 for an expiring contract).
		yearsLeft?: number;
	}[],
	needed: number,
	max = MAX_BALLAST,
	ceiling = Infinity,
): number[] | undefined => {
	if (!(needed > 0)) {
		return [];
	}
	if (needed > ceiling) {
		return undefined;
	}
	const ranked = players
		.filter((p) => p.amount > 0)
		.slice()
		.sort(
			(a, b) =>
				(a.yearsLeft ?? 0) - (b.yearsLeft ?? 0) ||
				Math.max(0, a.value) / a.amount - Math.max(0, b.value) / b.amount ||
				b.amount - a.amount ||
				a.pid - b.pid,
		);
	const out: number[] = [];
	let sent = 0;
	for (const p of ranked) {
		if (out.length >= max) {
			break;
		}
		if (sent + p.amount > ceiling) {
			continue;
		}
		out.push(p.pid);
		sent += p.amount;
		if (sent >= needed) {
			return out;
		}
	}
	return undefined;
};

// WHERE HUNTS DIE, counted so a deep run can say why the blockbusters it
// expected did not happen. Diagnostics only - nothing reads this to decide.
export const huntDiagnostics = new Map<string, number>();

export const noteHunt = (reason: string) => {
	huntDiagnostics.set(reason, (huntDiagnostics.get(reason) ?? 0) + 1);
};
