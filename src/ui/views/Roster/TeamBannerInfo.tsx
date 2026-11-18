import type { ReactNode } from "react";
import type { View } from "../../../common/types.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { getTeamMov } from "../../components/MovOrDiff.tsx";
import { RecordAndPlayoffs } from "../../components/RecordAndPlayoffs.tsx";
import { ConfLogoAndName } from "../../components/ConfLogoAndName.tsx";
import { PlusMinus } from "../../components/PlusMinus.tsx";
import { CapStatusText } from "../../components/CapStatus.tsx";
import { hardCapForTid } from "../../../common/getHardCap.ts";
import { PayrollAndPenalties, TeamRating } from "./TopStuff.tsx";

// A TEAM'S SEASON IN ITS BANNER (its page, the refreshed look) - all that the
// classic roster page has over the roster, each linked or tapped as there:
// under its name a line (its record to the standings, where it stands, how
// its season went to the bracket), and along the foot its numbers, each a
// small label over it (its rating - tapped, healthy or not -, MOV, age, its
// payroll - tapped, any penalty - with its cap status to its finances, the
// cap, the hard cap, its profit; a college's NIL payroll and prestige).
export const TeamBannerInfo = ({
	capStatus,
	collegeInfo,
	conf,
	currentSeason,
	luxuryTaxAmount,
	minPayrollAmount,
	payroll,
	playoffsByConf,
	profit,
	season,
	t,
	teamOvr,
	tid,
	usePts,
}: Pick<
	View<"roster">,
	| "capStatus"
	| "collegeInfo"
	| "conf"
	| "luxuryTaxAmount"
	| "minPayrollAmount"
	| "payroll"
	| "playoffsByConf"
	| "season"
	| "t"
	| "teamOvr"
	| "tid"
	| "usePts"
> & {
	currentSeason: number;
	profit: number;
}) => {
	const {
		budget,
		college,
		hardCapAmount,
		hardCapTids,
		hardCapUseLuxuryTax,
		luxuryPayroll,
		minPayroll,
		salaryCap,
		salaryCapType,
	} = useLocal([
		"budget",
		"college",
		"hardCapAmount",
		"hardCapTids",
		"hardCapUseLuxuryTax",
		"luxuryPayroll",
		"minPayroll",
		"salaryCap",
		"salaryCapType",
	]);
	const attrs = t.seasonAttrs;
	if (!attrs) {
		return null;
	}
	const current = season === currentSeason;

	// (Short: "East", not "Eastern Conference" - its logo by it, if it has
	// one.)
	const confShort = conf
		? {
				...conf,
				name: conf.name
					.replace(/^(East|West|North|South)ern Conference$/, "$1")
					.replace(/ Conference$/, ""),
			}
		: undefined;
	const where =
		t.rank === undefined ? null : (
			<span>
				{helpers.ordinal(t.rank)} in{" "}
				{playoffsByConf ? (
					confShort ? (
						<ConfLogoAndName conf={confShort} size={16} short />
					) : (
						"conference"
					)
				) : (
					"league"
				)}
				{t.gb >= 0 && t.rank > 1 ? (
					<>
						{" · "}
						{t.gb}{" "}
						<span title={usePts ? "Points Back" : "Games Back"}>
							{usePts ? "PB" : "GB"}
						</span>
					</>
				) : (
					<>
						{" · "}
						{-t.gb}{" "}
						<span title={usePts ? "Points Ahead" : "Games Ahead"}>
							{usePts ? "PA" : "GA"}
						</span>
					</>
				)}
			</span>
		);

	const stats: { label: ReactNode; title?: string; value: ReactNode }[] = [];
	if (teamOvr.type === "current") {
		stats.push({
			label: "Rating",
			value: <TeamRating ovr={t.ovr} ovrCurrent={t.ovrCurrent} />,
		});
	} else if (teamOvr.type === "delayed" && t.ovrDelayed !== undefined) {
		// (An older rating standing in for one withheld - its year said.)
		stats.push({
			label: `Rating ${teamOvr.season}`,
			value: t.ovrDelayed,
		});
	}
	stats.push(
		{
			label: "MOV",
			title: "Average margin of victory",
			value: <PlusMinus>{getTeamMov(t.stats)}</PlusMinus>,
		},
		{
			label: "Age",
			title: "Average age, weighted by minutes played",
			value: attrs.avgAge?.toFixed(1),
		},
	);
	if (payroll !== undefined && college) {
		stats.push({
			label: "NIL payroll",
			value: helpers.formatCurrency(payroll / 1000, "M"),
		});
	}
	if (collegeInfo) {
		stats.push({
			label: "Prestige",
			value: (
				<>
					{collegeInfo.prestige}
					{collegeInfo.coachExp !== undefined ? (
						<div className="tbi-note">
							Your contract thru {collegeInfo.coachExp}
						</div>
					) : null}
				</>
			),
		});
	}
	if (payroll !== undefined && !college) {
		stats.push({
			label: current ? "Payroll" : "End payroll",
			title: current ? undefined : "End of season payroll",
			value: (
				<>
					<PayrollAndPenalties
						isCurrentSeason={current}
						luxuryPayroll={luxuryPayroll / 1000}
						luxuryTaxAmount={
							luxuryTaxAmount !== undefined ? luxuryTaxAmount / 1000 : undefined
						}
						minPayroll={minPayroll / 1000}
						minPayrollAmount={
							minPayrollAmount !== undefined
								? minPayrollAmount / 1000
								: undefined
						}
						payroll={payroll / 1000}
					/>
					{capStatus ? (
						<a
							className="tbi-note"
							href={helpers.leagueUrl([
								"team_finances",
								helpers.getTeamUrlPart(tid),
							])}
						>
							<CapStatusText status={capStatus} />
						</a>
					) : null}
				</>
			),
		});
	}
	if (current && salaryCapType !== "none") {
		stats.push({
			label: "Salary cap",
			value: helpers.formatCurrency(salaryCap / 1000, "M"),
		});
	}
	const hardCap = hardCapForTid(tid, {
		hardCapAmount,
		hardCapTids,
		hardCapUseLuxuryTax,
		luxuryPayroll,
	});
	if (current && Number.isFinite(hardCap)) {
		stats.push({
			label: "Hard cap",
			value: helpers.formatCurrency(hardCap / 1000, "M"),
		});
	}
	if (current && budget) {
		stats.push({
			label: "Profit",
			value: helpers.formatCurrency(profit, "M"),
		});
	}

	return (
		<div className="tbi">
			<div className="tbi-line">
				<span className="tbi-record">
					<RecordAndPlayoffs
						season={season}
						won={attrs.won}
						lost={attrs.lost}
						otl={attrs.otl}
						tied={attrs.tied}
						roundsWonText={t.roundsWonText}
						option="noSeason"
					/>
				</span>
				{where ? (
					<>
						<span className="tbi-dot"> · </span>
						{where}
					</>
				) : null}
			</div>
			<div className="tbi-stats">
				{stats.map(({ label, title, value }, i) => (
					<div key={i} className="tbi-stat">
						<div className="tbi-label" title={title}>
							{label}
						</div>
						<div className="tbi-value">{value}</div>
					</div>
				))}
			</div>
		</div>
	);
};
