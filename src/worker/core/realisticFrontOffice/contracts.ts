import { PHASE } from "../../../common/constants.ts";
import type { Player, PlayerContract } from "../../../common/types.ts";
import { range } from "../../../common/utils.ts";
import { g, helpers } from "../../util/index.ts";
import { getHardCap } from "../../util/getHardCap.ts";

// CONTRACTS on the same terms a human gets: the lengths a player would sign
// for and what he asks for each, and whether a team may pay it.

// His ask for the length he wants, and more for each year shorter or longer -
// exactly as contract negotiation prices them for a human
// (contractNegotiation/generateContractOptions). `amount` is his ask, mood
// included, for the team asking.
export const contractOptions = (
	p: Pick<Player, "contract" | "ratings">,
	amount: number,
): PlayerContract[] => {
	const ovr = p.ratings.at(-1)!.ovr;
	const growthFactor = 0.15 + (ovr % 10) * 0.01 - 0.05;
	let exp = g.get("season");
	if (g.get("phase") <= PHASE.AFTER_TRADE_DEADLINE) {
		exp -= 1;
	}
	const lengths = range(
		g.get("minContractLength"),
		g.get("maxContractLength") + 1,
	);
	let found = lengths.findIndex((length) => exp + length === p.contract.exp);
	if (found < 0) {
		found = 0;
	}
	const out: PlayerContract[] = [];
	for (const [i, length] of lengths.entries()) {
		const asked = helpers.roundContract(
			amount * (1 + Math.abs(found - i) * growthFactor),
		);
		if (i === found || asked <= g.get("maxContract")) {
			out.push({ amount: asked, exp: exp + length });
		}
	}
	return out;
};

// Whether a team may sign him for this, by the rules a human's signing must
// meet (contractNegotiation/accept): over the cap only for the minimum, unless
// re-signing its own player in a soft cap league, and never past a secondary
// hard cap except for a minimum player while the roster has room.
export const mayPay = ({
	tid,
	payroll,
	amount,
	resigning,
	rosterSize,
}: {
	tid: number;
	payroll: number;
	amount: number;
	resigning: boolean;
	rosterSize: number;
}) => {
	const salaryCapType = g.get("salaryCapType");
	const minContract = g.get("minContract");
	if (salaryCapType !== "none") {
		const birdException = resigning && salaryCapType === "soft";
		if (
			!birdException &&
			payroll + amount - 1 > g.get("salaryCap") &&
			amount - 1 > minContract
		) {
			return false;
		}
	}
	const hardCap = getHardCap(tid);
	if (Number.isFinite(hardCap) && payroll + amount - 1 > hardCap) {
		return amount - 1 <= minContract && rosterSize < g.get("maxRosterSize");
	}
	return true;
};
