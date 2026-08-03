import clsx from "clsx";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { hardCapForTid } from "../../common/getHardCap.ts";
import { HelpPopover } from "./HelpPopover.tsx";

// The league's money rules, as numbers rather than prose: the figures every
// finance page is read against.
//
// They were scattered - the cap and the tax line on League Finances, the
// contract limits on the Free Agents page, nothing anywhere carrying all of
// them - so working out whether a contract was near the max meant remembering a
// number from another screen. One block, same on every page that deals in
// money.
export const SalaryCapInfo = ({
	className,
	tid,
}: {
	className?: string;
	// Whose hard cap to show, for a league that hard-caps only some teams.
	// Defaults to this device's own team.
	tid?: number;
}) => {
	const {
		hardCapAmount,
		hardCapTids,
		hardCapUseLuxuryTax,
		luxuryPayroll,
		luxuryTax,
		maxContract,
		minContract,
		minPayroll,
		salaryCap,
		salaryCapType,
		userTid,
	} = useLocal([
		"hardCapAmount",
		"hardCapTids",
		"hardCapUseLuxuryTax",
		"luxuryPayroll",
		"luxuryTax",
		"maxContract",
		"minContract",
		"minPayroll",
		"salaryCap",
		"salaryCapType",
		"userTid",
	]);

	// Everything here is stored in thousands.
	const money = (amount: number) => helpers.formatCurrency(amount / 1000, "M");

	const hardCap = hardCapForTid(tid ?? userTid, {
		hardCapAmount,
		hardCapTids,
		hardCapUseLuxuryTax,
		luxuryPayroll,
	});

	const items: { label: string; value: string }[] = [];

	if (salaryCapType !== "none") {
		items.push({ label: "Salary cap", value: money(salaryCap) });
	}
	// Only worth its own entry when it is a SECOND ceiling. In a hard-cap league
	// the salary cap already is one.
	if (Number.isFinite(hardCap) && hardCap !== salaryCap) {
		items.push({ label: "Hard cap", value: money(hardCap) });
	}
	items.push({ label: "Min payroll", value: money(minPayroll) });
	if (luxuryTax !== 0) {
		items.push({
			label: `Luxury tax (${luxuryTax}×)`,
			value: money(luxuryPayroll),
		});
	}
	items.push(
		{ label: "Max contract", value: money(maxContract) },
		{ label: "Min contract", value: money(minContract) },
	);

	return (
		<div className={clsx("d-flex flex-wrap align-items-end gap-3", className)}>
			{items.map((item) => (
				<div key={item.label}>
					<div className="text-body-secondary small">{item.label}</div>
					<div className="fw-bold">{item.value}</div>
				</div>
			))}
			<HelpPopover title="Salary cap rules">
				{salaryCapType === "none" ? (
					<p>There is no salary cap in this league.</p>
				) : (
					<p>
						A team over the salary cap can only sign{" "}
						{salaryCapType === "hard" ? "players" : "free agents"} to minimum
						contracts
						{salaryCapType === "soft"
							? ", though it can go over the cap to re-sign its own players and to make certain trades"
							: null}
						.
					</p>
				)}
				<p>
					A team below the minimum payroll is fined the difference at the end of
					the season.
				</p>
				{luxuryTax === 0 ? null : (
					<p>
						A team above the luxury tax limit is fined {luxuryTax} times the
						difference at the end of the season.
					</p>
				)}
			</HelpPopover>
		</div>
	);
};
