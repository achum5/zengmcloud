import type { Player } from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import { getHardCap } from "../../util/getHardCap.ts";

// CBA-LITE (owner's call, 2026-11-10): about 80% of what the NBA's collective
// bargaining agreement does to team-building at about 5% of its complexity.
// With Realistic Front Offices on only. docs/free-agency-overhaul.md.
//
// Simplified (owner's call, 2026-11-11, for a casual league mate): stock
// BBGM's contracts plus the mid-level, a hard cap, raises and options, and
// restricted free agency. No max tiers, cap holds, taxpayer mid-level, apron
// or per-exception lengths; Bird rights in the re-signing phase only, as in
// stock (and a restricted free agent's team may match).
//
// Amounts scale with the league's settings: the hard cap sits a share of the
// cap above the luxury tax line (or lower, where the league's own hard cap is
// lower).
export const cbaSettings = {
	// Max contract by seasons played, as a share of maxContract. One for
	// everyone (it was 25%, 30% and 35% of the cap by service).
	maxTiers: [{ service: 0, share: 1 }],
	// Once a season, a team over the cap may sign one outside free agent for
	// up to this share of the cap, within the hard cap.
	midLevel: 0.09,
	// The hard cap: this share of the cap above the luxury tax line.
	hardCapOverTax: 0.1,
	// Yearly raise (or cut), as a share of the first year: re-signing his own
	// player, and any other signing.
	raiseOwn: 0.08,
	raiseOther: 0.05,
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

// No transaction may take a team's payroll past this. The league's own hard
// cap, where it has one for this team, if lower.
export const hardCap = (tid: number) =>
	Math.min(
		getHardCap(tid),
		g.get("luxuryPayroll") + cbaSettings.hardCapOverTax * g.get("salaryCap"),
	);

// Contract amounts are in thousands and rounded; a hair over is the same.
const SLACK = 1;

export type SigningPath = "room" | "bird" | "midLevel" | "minimum";

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
	// Within the hard cap (checked above): the mid-level.
	if (!midLevelUsed && amount <= midLevelAmount() + SLACK) {
		return { ok: true, path: "midLevel" };
	}
	return {
		ok: false,
		reason: midLevelUsed
			? "Over the cap, mid-level already used"
			: "Over the cap; more than the mid-level",
	};
};

// The largest yearly raise, as a share of the first year: more re-signing
// his own player. Any signing may run as long as the league allows.
export const raiseLimit = (own: boolean) =>
	own ? cbaSettings.raiseOwn : cbaSettings.raiseOther;

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

// Whether a trade's salaries are allowed for one side by the hard cap: it may
// not end past the hard cap unless it takes in no more than it sends out.
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
	return { ok: true };
};
