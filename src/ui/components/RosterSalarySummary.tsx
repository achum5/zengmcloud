import { HelpPopover } from "./HelpPopover.tsx";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { hardCapForTid } from "../../common/getHardCap.ts";

export const RosterSalarySummary = ({
	capSpace,
	numRosterSpots,
	payroll,
}: {
	capSpace: number;
	numRosterSpots: number;
	payroll: number;
}) => {
	const {
		hardCapAmount,
		hardCapTids,
		hardCapUseLuxuryTax,
		luxuryPayroll,
		maxContract,
		minContract,
		salaryCapType,
		userTid,
	} = useLocal([
		"hardCapAmount",
		"hardCapTids",
		"hardCapUseLuxuryTax",
		"luxuryPayroll",
		"maxContract",
		"minContract",
		"salaryCapType",
		"userTid",
	]);

	const actualCapSpace = capSpace > 0 ? capSpace : 0;

	// The hard cap is the ceiling that actually binds when re-signing, since
	// going over the SOFT cap for your own players is allowed and going over
	// this one isn't. Off in most leagues, in which case there's nothing to say.
	const hardCap = hardCapForTid(userTid, {
		hardCapAmount,
		hardCapTids,
		hardCapUseLuxuryTax,
		luxuryPayroll,
	});
	const hardCapRoom = hardCap / 1000 - payroll;

	return (
		<div className="mb-3">
			You currently have <b>{numRosterSpots}</b> open roster spots
			{salaryCapType === "none" ? (
				<>
					{" "}
					and a <b>{helpers.formatCurrency(payroll, "M")}</b> payroll (luxury
					tax limit: {helpers.formatCurrency(luxuryPayroll / 1000, "M")}).
				</>
			) : (
				<>
					{" "}
					and{" "}
					<b className={actualCapSpace > 0 ? "text-success" : undefined}>
						{helpers.formatCurrency(actualCapSpace, "M")}
					</b>{" "}
					in cap space
					{capSpace < 0 ? (
						<>
							{" "}
							(
							<b className="text-danger">
								{helpers.formatCurrency(Math.abs(capSpace), "M")}
							</b>{" "}
							over the cap)
						</>
					) : null}
					.{" "}
					<HelpPopover title="Cap Space">
						<p>
							"Cap space" is the difference between your current payroll and the
							salary cap.
						</p>
						<p>
							{salaryCapType === "hard"
								? "You "
								: "After the season you can go over the salary cap to re-sign your own players. Besides that, you "}
							can only exceed the salary cap to sign players to minimum
							contracts ({helpers.formatCurrency(minContract / 1000, "M")}
							/year).
						</p>
					</HelpPopover>
				</>
			)}
			{Number.isFinite(hardCap) ? (
				<>
					<br />
					Hard cap room:{" "}
					<b className={hardCapRoom > 0 ? "text-success" : "text-danger"}>
						{helpers.formatCurrency(hardCapRoom, "M")}
					</b>{" "}
					(hard cap: {helpers.formatCurrency(hardCap / 1000, "M")})
				</>
			) : null}
			<br />
			Min contract: {helpers.formatCurrency(minContract / 1000, "M")}
			<br />
			Max contract: {helpers.formatCurrency(maxContract / 1000, "M")}
			<br />
		</div>
	);
};
