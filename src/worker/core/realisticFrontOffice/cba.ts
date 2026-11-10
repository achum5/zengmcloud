import type { Player } from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import { getHardCap } from "../../util/getHardCap.ts";

// CBA-LITE (owner's call, 2026-11-10): about 80% of what the NBA's collective
// bargaining agreement does to team-building at about 5% of its complexity.
// With Realistic Front Offices on only. docs/free-agency-overhaul.md.
//
// Amounts scale with the league's settings: the top max tier is maxContract,
// the apron is the luxury tax line, and the hard cap sits a share of the cap
// above it (or lower, where the league's own hard cap is lower).
export const cbaSettings = {
	// Max contract by seasons played, as a share of maxContract: 25%, 30% and
	// 35% of the cap in the NBA.
	maxTiers: [
		{ service: 0, share: 25 / 35 },
		{ service: 7, share: 30 / 35 },
		{ service: 10, share: 1 },
	],
	// Once a season, a team over the cap may sign one outside free agent for
	// up to this share of the cap, for up to midLevelYears, if it stays under
	// the apron.
	midLevel: 0.09,
	midLevelYears: 4,
	// Past the apron, the taxpayer mid-level instead (as the NBA's): up to
	// this share of the cap, for up to taxpayerYears, within the hard cap.
	// Either one a season. With it nearly every team can pay a good free
	// agent something real every summer, which is how the NBA's market clears.
	taxpayerMidLevel: 0.05,
	taxpayerYears: 2,
	// The hard cap: this share of the cap above the luxury tax line.
	hardCapOverTax: 0.1,
	// Yearly raise (or cut), as a share of the first year: with Bird rights,
	// and without.
	raiseOwn: 0.08,
	raiseOther: 0.05,
	// Longest contract: with Bird rights, without, and for a minimum deal.
	yearsOwn: 5,
	yearsOther: 4,
	yearsMinimum: 2,
};

// Seasons a player has played in (regular season, any games).
export const serviceYears = (p: Pick<Player, "stats">) =>
	new Set(
		p.stats
			.filter((row) => !row.playoffs && (row.gp ?? 0) > 0)
			.map((row) => row.season),
	).size;

// The team holding his Bird rights: the one he was last with in a regular
// season, this season or the one before.
export const birdTid = (p: Pick<Player, "stats">, season: number) => {
	const last = p.stats.filter((row) => !row.playoffs).at(-1);
	return last && last.season >= season - 1 ? last.tid : undefined;
};

// The most he may be paid a season, by his tier.
export const maxFor = (service: number) => {
	let share = cbaSettings.maxTiers[0]!.share;
	for (const tier of cbaSettings.maxTiers) {
		if (service >= tier.service) {
			share = tier.share;
		}
	}
	return Math.max(g.get("minContract"), g.get("maxContract") * share);
};

export const midLevelAmount = () => cbaSettings.midLevel * g.get("salaryCap");
export const taxpayerMidLevelAmount = () =>
	cbaSettings.taxpayerMidLevel * g.get("salaryCap");

// Above the apron a team gets no mid-level, and its trades may not bring in
// more salary than they send out.
export const apron = () => g.get("luxuryPayroll");

// No transaction may take a team's payroll past this. The league's own hard
// cap, where it has one for this team, if lower.
export const hardCap = (tid: number) =>
	Math.min(
		getHardCap(tid),
		g.get("luxuryPayroll") + cbaSettings.hardCapOverTax * g.get("salaryCap"),
	);

// Contract amounts are in thousands and rounded; a hair over is the same.
const SLACK = 1;

export type SigningPath =
	| "room"
	| "bird"
	| "midLevel"
	| "taxpayerMidLevel"
	| "minimum";

export type SigningCheck =
	| { ok: true; path: SigningPath }
	| { ok: false; reason: string };

// Whether a team may sign a player for `amount` a season, and how. `payroll`
// is the team's payroll for the season the contract starts, its own unsigned
// free agents' holds included (capHolds); `own` is whether it holds his Bird
// rights.
export const checkSigning = ({
	tid,
	payroll,
	amount,
	service,
	own,
	midLevelUsed,
	rosterSize,
}: {
	tid: number;
	payroll: number;
	amount: number;
	service: number;
	own: boolean;
	midLevelUsed: boolean;
	rosterSize: number;
}): SigningCheck => {
	const minContract = g.get("minContract");
	const minimum = amount <= minContract + SLACK;
	if (!minimum && amount > maxFor(service) + SLACK) {
		return { ok: false, reason: "More than his max contract" };
	}
	const after = payroll + amount;
	const ceiling = hardCap(tid);
	if (minimum) {
		if (after <= ceiling + SLACK || rosterSize < g.get("minRosterSize")) {
			return { ok: true, path: "minimum" };
		}
		return { ok: false, reason: "Over the hard cap" };
	}
	if (after > ceiling + SLACK) {
		return { ok: false, reason: "Over the hard cap" };
	}
	if (after <= g.get("salaryCap") + SLACK) {
		return { ok: true, path: "room" };
	}
	if (own) {
		return { ok: true, path: "bird" };
	}
	if (
		!midLevelUsed &&
		amount <= midLevelAmount() + SLACK &&
		after <= apron() + SLACK
	) {
		return { ok: true, path: "midLevel" };
	}
	// Past the apron (the hard cap checked above): the taxpayer mid-level.
	if (
		!midLevelUsed &&
		amount <= taxpayerMidLevelAmount() + SLACK &&
		after > apron() + SLACK
	) {
		return { ok: true, path: "taxpayerMidLevel" };
	}
	return {
		ok: false,
		reason: midLevelUsed
			? "Over the cap, mid-level already used"
			: after > apron() + SLACK
				? "Over the apron; more than the taxpayer mid-level"
				: "Over the cap; more than the mid-level",
	};
};

// The longest deal, and the largest yearly raise as a share of the first
// year, a signing by this path may have.
export const shapeLimits = (path: SigningPath, own: boolean) => ({
	years:
		path === "minimum"
			? cbaSettings.yearsMinimum
			: path === "midLevel"
				? cbaSettings.midLevelYears
				: path === "taxpayerMidLevel"
					? cbaSettings.taxpayerYears
					: own
						? cbaSettings.yearsOwn
						: cbaSettings.yearsOther,
	raise: own ? cbaSettings.raiseOwn : cbaSettings.raiseOther,
});

// A contract's seasons with a steady raise (negative for a cut) from the
// first year's amount.
export const amountsWithRaise = (first: number, years: number, raise: number) =>
	Array.from({ length: years }, (_, i) => first * (1 + raise * i));

// Whether these amounts keep within the raise limit: each season's change
// at most `raise` of the first year's amount.
export const raisesOk = (amounts: readonly number[], raise: number) => {
	const step = raise * amounts[0]! + SLACK;
	for (let i = 1; i < amounts.length; i++) {
		if (Math.abs(amounts[i]! - amounts[i - 1]!) > step) {
			return false;
		}
	}
	return true;
};

// Whether a trade's salaries are allowed for one side, by the apron and the
// hard cap: above the apron after it, a team may not take in more than it
// sends out; and it may not end past the hard cap unless it takes in no more
// than it sends out.
export const checkTradeSalary = ({
	tid,
	payrollAfter,
	incoming,
	outgoing,
}: {
	tid: number;
	payrollAfter: number;
	incoming: number;
	outgoing: number;
}): { ok: true } | { ok: false; reason: string } => {
	const shedding = incoming <= outgoing + SLACK;
	if (payrollAfter > hardCap(tid) + SLACK && !shedding) {
		return { ok: false, reason: "Over the hard cap" };
	}
	if (payrollAfter > apron() + SLACK && !shedding) {
		return {
			ok: false,
			reason: "Over the apron: must send out as much salary as it takes in",
		};
	}
	return { ok: true };
};
