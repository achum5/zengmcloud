import type { ReactNode } from "react";
import type { View } from "../../../common/types.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { getTeamMov } from "../../components/MovOrDiff.tsx";

// A TEAM'S SEASON IN ITS BANNER (its page, the refreshed look): under its
// name a line - its record, where it stands, how its season went - and along
// the foot a row of its numbers, each a small label over the number.
export const TeamBannerInfo = ({
	conf,
	payroll,
	playoffsByConf,
	profit,
	season,
	currentSeason,
	t,
	teamOvr,
	usePts,
}: Pick<
	View<"roster">,
	"conf" | "payroll" | "playoffsByConf" | "season" | "t" | "teamOvr" | "usePts"
> & {
	currentSeason: number;
	profit: number;
}) => {
	const { budget, college, hideRatingsOnesDigit } = useLocal([
		"budget",
		"college",
		"hideRatingsOnesDigit",
	]);
	const attrs = t.seasonAttrs;
	if (!attrs) {
		return null;
	}
	const current = season === currentSeason;

	// (Short: "West", not "Western Conference".)
	const confName =
		conf?.abbrev ??
		conf?.name
			.replace(/^(East|West|North|South)ern Conference$/, "$1")
			.replace(/ Conference$/, "");
	const where =
		t.rank === undefined
			? null
			: `${helpers.ordinal(t.rank)} in ${
					playoffsByConf ? (confName ?? "conference") : "league"
				}${t.rank > 1 && t.gb > 0 ? ` · ${t.gb} ${usePts ? "PB" : "GB"}` : ""}`;
	const line = [
		<strong key="record" className="tbi-record">
			{helpers.formatRecord(attrs)}
		</strong>,
		where,
		t.roundsWonText ?? null,
	].filter((x) => x !== null && x !== "");

	const coarse = (value: number) =>
		hideRatingsOnesDigit ? Math.floor(value / 10) : Math.round(value);
	const rating =
		teamOvr.type === "current"
			? coarse(t.ovrCurrent)
			: teamOvr.type === "delayed" && t.ovrDelayed !== undefined
				? coarse(t.ovrDelayed)
				: undefined;
	const mov = getTeamMov(t.stats);
	const stats: [string, ReactNode][] = [];
	if (rating !== undefined) {
		stats.push([
			teamOvr.type === "delayed"
				? `Rating '${String(teamOvr.season).slice(-2)}`
				: "Rating",
			rating,
		]);
	}
	if (mov !== undefined && Number.isFinite(mov)) {
		stats.push(["MOV", `${mov > 0 ? "+" : ""}${mov.toFixed(1)}`]);
	}
	if (attrs.avgAge !== undefined) {
		stats.push(["Age", attrs.avgAge.toFixed(1)]);
	}
	if (payroll !== undefined) {
		stats.push([
			college ? "NIL" : "Payroll",
			helpers.formatCurrency(payroll / 1000, "M", 1),
		]);
	}
	if (current && budget) {
		stats.push(["Profit", helpers.formatCurrency(profit, "M", 1)]);
	}

	return (
		<div className="tbi">
			<div className="tbi-line">
				{line.map((part, i) => (
					<span key={i}>
						{i > 0 ? <span className="tbi-dot"> · </span> : null}
						{part}
					</span>
				))}
			</div>
			{stats.length > 0 ? (
				<div className="tbi-stats">
					{stats.map(([label, value]) => (
						<div key={label} className="tbi-stat">
							<div className="tbi-label">{label}</div>
							<div className="tbi-value">{value}</div>
						</div>
					))}
				</div>
			) : null}
		</div>
	);
};
