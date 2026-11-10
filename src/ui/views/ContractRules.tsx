import clsx from "clsx";
import type { ReactNode } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import type { Col, View } from "../../common/types.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import type { DataTableRow } from "../components/DataTable/index.tsx";
import { wrappedTeamAbbrevLink } from "../components/TeamAbbrevLink.tsx";
import {
	CapStatusBadge,
	exceptionText,
	roomText,
	termsBadges,
} from "../components/CapStatus.tsx";
import { ABBREVS, CAP_STATUS, howMethod } from "../../common/signingHow.ts";

// THE CONTRACT RULES PAGE (realistic front offices' CBA-lite): the rules as
// this league's numbers, every team's cap status, and the season's signings.
// Tables of short phrases, no prose (CLAUDE.md).

const money = (amount: number) => helpers.formatCurrency(amount / 1000, "M");

const Abbr = ({ k }: { k: keyof typeof ABBREVS }) => (
	<abbr title={ABBREVS[k]}>{k}</abbr>
);

const RuleTable = ({
	className,
	rows,
	title,
}: {
	className?: string;
	rows: [ReactNode, ReactNode][];
	title: string;
}) => (
	<div className={clsx("col-md-6 col-xl-4 mb-3", className)}>
		<h3>{title}</h3>
		<table className="table table-sm table-striped mb-0">
			<tbody>
				{rows.map(([rule, value], i) => (
					<tr key={i}>
						<td>{rule}</td>
						<td className="text-end text-wrap">{value}</td>
					</tr>
				))}
			</tbody>
		</table>
	</div>
);

type Rules = View<"contractRules">["rules"];

const RulesTab = ({ rules }: { rules: Rules }) => {
	const years = (n: number) => `${n} ${helpers.plural("yr", n)}`;
	return (
		<div className="row">
			<RuleTable
				title="Cap"
				rows={[
					["Salary cap", money(rules.salaryCap)],
					["Apron (luxury tax line)", money(rules.apron)],
					[
						`Hard cap (apron + ${rules.hardCapPct}% of cap)`,
						money(rules.hardCap),
					],
					...rules.ownHardCaps.map((row): [ReactNode, ReactNode] => [
						`Hard cap, ${row.abbrev} (league setting)`,
						money(row.amount),
					]),
					["Minimum contract", money(rules.minContract)],
					[
						<>
							Mid-level (<Abbr k="MLE" />, {rules.midLevelPct}% of cap)
						</>,
						money(rules.midLevel),
					],
					[
						<>
							Taxpayer mid-level (<Abbr k="TMLE" />, {rules.taxpayerMidLevelPct}
							% of cap)
						</>,
						money(rules.taxpayerMidLevel),
					],
				]}
			/>
			<div className="col-md-6 col-xl-4 mb-3">
				<h3>Max contract</h3>
				<table className="table table-sm table-striped mb-0">
					<thead>
						<tr>
							<th>Seasons played</th>
							<th className="text-end">Max</th>
							<th className="text-end">% of cap</th>
						</tr>
					</thead>
					<tbody>
						{rules.tiers.map((tier) => (
							<tr key={tier.seasons}>
								<td>{tier.seasons}</td>
								<td className="text-end">{money(tier.amount)}</td>
								<td className="text-end">{tier.pct}%</td>
							</tr>
						))}
					</tbody>
				</table>
			</div>
			<RuleTable
				title="Contract length and raises"
				rows={[
					["Own player (Bird rights)", years(rules.years.own)],
					["Other teams' players", years(rules.years.other)],
					[<Abbr key="mle" k="MLE" />, years(rules.years.midLevel)],
					[<Abbr key="tmle" k="TMLE" />, years(rules.years.taxpayerMidLevel)],
					["Minimum", years(rules.years.minimum)],
					["Raise or cut a year, own player", `${rules.raiseOwn}%`],
					["Raise or cut a year, others", `${rules.raiseOther}%`],
					[
						<>
							Option (<Abbr k="PO" /> or <Abbr k="TO" />)
						</>,
						"Final season, 2+ yr deals",
					],
				]}
			/>
			<RuleTable
				title="Ways to sign a player"
				rows={[
					[<Abbr key="room" k="Room" />, "Payroll + holds + deal under cap"],
					[<Abbr key="bird" k="Bird" />, "Own player, up to his max"],
					[<Abbr key="mle" k="MLE" />, "Over cap, stays under apron"],
					[<Abbr key="tmle" k="TMLE" />, "Over apron, under hard cap"],
					["MLE or TMLE", "One a season"],
					[
						<Abbr key="min" k="Min" />,
						"Always (past hard cap: to fill roster)",
					],
					["Hard cap", "No move may go past it"],
				]}
			/>
			<RuleTable
				title="Rights"
				rows={[
					[<Abbr key="bird" k="Bird" />, "Team he played for last season"],
					["Lost by", "Renouncing, a prove-it deal"],
					["Cap hold", "Own free agent, at his ask, until signed or renounced"],
					[<Abbr key="rfa" k="RFA" />, "Off a rookie deal: his team may match"],
					[
						<Abbr key="proveit" k="Prove-it" />,
						`Under ${rules.proveItPct}% of opening ask: ${years(rules.proveItYears)}`,
					],
					[
						<Abbr key="ext" k="Ext" />,
						`Final season, in season: up to ${years(rules.extensionYears)} more`,
					],
					["Options decided", "Before free agency"],
				]}
			/>
			<RuleTable
				title="Trades"
				rows={[
					["Over the apron after it", "Salary in ≤ salary out"],
					["Past the hard cap after it", "Salary in ≤ salary out"],
				]}
			/>
			<div className="col-md-6 col-xl-4 mb-3">
				<h3>Free agency stages</h3>
				<table className="table table-sm mb-0">
					<thead>
						<tr>
							<th />
							<th>Stage</th>
							<th>Who decides</th>
						</tr>
					</thead>
					<tbody>
						{rules.stages.map((name, i) => (
							<tr
								key={name}
								className={i === rules.stage ? "table-primary" : undefined}
							>
								<td>{i + 1}</td>
								<td>{name}</td>
								<td>
									{i === 0
										? "Nobody signs"
										: i === 1
											? `Top ${rules.tier1} free agents`
											: i === 2
												? `Next ${rules.tier2}`
												: i === 3
													? "Everyone else"
													: "Anyone left; asks drop"}
								</td>
							</tr>
						))}
					</tbody>
				</table>
			</div>
		</div>
	);
};

const col = (title: string, desc?: string, number = true): Col => ({
	title,
	desc,
	sortSequence: number ? ["desc", "asc"] : ["asc", "desc"],
	sortType: number ? "number" : undefined,
});

const moneyCell = (amount: number) => ({
	value: roomText(amount),
	sortValue: Number.isFinite(amount) ? amount : Infinity,
	searchValue: money(amount),
});

const statusOrder = Object.keys(CAP_STATUS);

const TeamsTab = ({
	teams,
	userTid,
}: Pick<View<"contractRules">, "teams" | "userTid">) => {
	const cols = [
		col("Team", undefined, false),
		col("Payroll"),
		col("Cap Space", "After cap holds"),
		col("Under Apron", "Room under the apron"),
		col("Under Hard Cap", "Room under the hard cap"),
		col("Exception", "Mid-level exception left this season"),
		col("Status", undefined, false),
	];
	const rows: DataTableRow[] = teams.map((t) => ({
		key: t.tid,
		classNames: { "table-info": t.tid === userTid },
		data: [
			wrappedTeamAbbrevLink({ abbrev: t.abbrev, tid: t.tid }),
			{ value: money(t.payroll), sortValue: t.payroll },
			moneyCell(t.capSpace),
			moneyCell(t.apronRoom),
			moneyCell(t.hardCapRoom),
			{
				value: exceptionText(t.exception),
				sortValue: "amount" in t.exception ? t.exception.amount : 0,
				searchValue: t.exception.kind,
			},
			{
				value: <CapStatusBadge status={t.status} />,
				sortValue: statusOrder.indexOf(t.status),
				searchValue: CAP_STATUS[t.status].label,
			},
		],
	}));
	return (
		<DataTable
			cols={cols}
			defaultSort={[1, "desc"]}
			name="ContractRules:Teams"
			nonfluid
			rows={rows}
		/>
	);
};

const TYPE_LABELS = {
	freeAgent: { short: "FA", long: "Free agent" },
	reSigned: { short: "Re-signed", long: "Re-signed" },
	extension: { short: "Ext", long: ABBREVS.Ext },
	rookie: { short: "Rookie", long: "Rookie contract" },
};

const SigningsTab = ({
	signings,
	season,
}: Pick<View<"contractRules">, "signings" | "season">) => {
	const cols = [
		col("Player", undefined, false),
		col("Team", undefined, false),
		col("When", undefined, false),
		col("Type", undefined, false),
		col("Method", "Room, Bird, MLE, TMLE, Min", false),
		col("Terms", "Option, prove-it", false),
		col("Yrs"),
		col("Total"),
		col("Per Yr"),
	];
	const rows: DataTableRow[] = signings.map((row) => {
		const method = howMethod(row.how);
		const type = TYPE_LABELS[row.type];
		return {
			key: row.eid,
			data: [
				{
					value: (
						<a href={helpers.leagueUrl(["player", row.pid])}>{row.name}</a>
					),
					sortValue: row.name,
					searchValue: row.name,
				},
				wrappedTeamAbbrevLink({
					abbrev: row.abbrev,
					tid: row.tid,
					season: row.season,
				}),
				row.when,
				{
					value: <span title={type.long}>{type.short}</span>,
					sortValue: type.short,
					searchValue: type.short,
				},
				method
					? {
							value: <span title={method.long}>{method.short}</span>,
							sortValue: method.short,
							searchValue: method.short,
						}
					: null,
				row.how
					? {
							value: termsBadges(row.how),
							searchValue: `${row.how.option ?? ""} ${row.how.proveIt ? "prove-it" : ""}`,
						}
					: null,
				row.years,
				{ value: money(row.total), sortValue: row.total },
				{
					value: money(row.total / row.years),
					sortValue: row.total / row.years,
				},
			],
		};
	});
	return (
		<>
			{rows.length === 0 ? <p>No signings in {season}.</p> : null}
			<DataTable
				cols={cols}
				defaultSort={[7, "desc"]}
				name="ContractRules:Signings"
				nonfluid
				pagination
				rows={rows}
			/>
		</>
	);
};

const TAB_OPTIONS = [
	{ key: "rules", label: "Rules" },
	{ key: "teams", label: "Teams" },
	{ key: "signings", label: "Signings" },
];

const ContractRules = ({
	rules,
	season,
	signings,
	tab,
	team,
	teams,
	userTid,
}: View<"contractRules">) => {
	useTitleBar({
		title: "Contract Rules",
		dropdown:
			tab === "signings"
				? {
						view: "contractRules",
						fields: [
							{ key: "tab", options: TAB_OPTIONS, value: tab },
							{
								key: "team",
								options: { type: "teams", all: true },
								value: team,
							},
							{ key: "season", options: { type: "seasons" }, value: season },
						],
					}
				: {
						view: "contractRules",
						fields: [{ key: "tab", options: TAB_OPTIONS, value: tab }],
					},
	});

	if (tab === "teams") {
		return <TeamsTab teams={teams} userTid={userTid} />;
	}
	if (tab === "signings") {
		return <SigningsTab signings={signings} season={season} />;
	}
	return <RulesTab rules={rules} />;
};

export default ContractRules;
