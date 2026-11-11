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
					["Luxury tax line", money(rules.luxuryTax)],
					[
						`Hard cap (tax line + ${rules.hardCapPct}% of cap)`,
						money(rules.hardCap),
					],
					...rules.ownHardCaps.map((row): [ReactNode, ReactNode] => [
						`Hard cap, ${row.abbrev} (league setting)`,
						money(row.amount),
					]),
					["Minimum contract", money(rules.minContract)],
					[
						`Max contract (${rules.maxContractPct}% of cap)`,
						money(rules.maxContract),
					],
					[
						<>
							Mid-level (<Abbr k="MLE" />, {rules.midLevelPct}% of cap)
						</>,
						money(rules.midLevel),
					],
				]}
			/>
			<RuleTable
				title="Contracts"
				rows={[
					["Longest deal", years(rules.maxLength)],
					["Raise or cut a year, re-signing", `${rules.raiseOwn}%`],
					["Raise or cut a year, others", `${rules.raiseOther}%`],
					[
						<>
							Option (<Abbr k="PO" /> or <Abbr k="TO" />)
						</>,
						"Final season, 2+ yr deals",
					],
					["Options decided", "Before free agency"],
				]}
			/>
			<RuleTable
				title="Ways to sign a player"
				rows={[
					[<Abbr key="room" k="Room" />, "Payroll + deal under cap"],
					[<Abbr key="bird" k="Bird" />, "Re-signing your own player"],
					[<Abbr key="mle" k="MLE" />, "Over cap, one a season"],
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
					rules.extensionsOn
						? [
								<Abbr key="ext" k="Ext" />,
								`Final season, in season: up to ${years(rules.extensionYears)} more`,
							]
						: ["Re-signing", "Re-sign Players phase only"],
					[<Abbr key="rfa" k="RFA" />, "Off a rookie deal: his team may match"],
					[
						<Abbr key="proveit" k="Prove-it" />,
						`Under ${rules.proveItPct}% of opening ask: ${years(rules.proveItYears)}`,
					],
				]}
			/>
			<RuleTable
				title="Trades"
				rows={[["Past the hard cap after it", "Salary in ≤ salary out"]]}
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

// HOW IT WORKS: every piece of the contract rules and free agency, in plain
// words with this league's numbers (owner's call, 2026-11-11: the one page
// to be thorough - everywhere else stays short labels).
const GuideSection = ({
	children,
	title,
}: {
	children: ReactNode;
	title: string;
}) => (
	<div className="col-lg-6 mb-4">
		<h3>{title}</h3>
		<ul className="mb-0 ps-3">{children}</ul>
	</div>
);

const GuideTab = ({ rules }: { rules: Rules }) => {
	const years = (n: number) => `${n} ${helpers.plural("year", n)}`;
	const floors = rules.askFloorPcts;
	return (
		<div className="row" style={{ maxWidth: 1100 }}>
			<GuideSection title="The salary cap">
				<li>
					The salary cap is <b>{money(rules.salaryCap)}</b>. Your payroll is
					every contract you pay this season, including players you released and
					still owe.
				</li>
				<li>
					<b>Under the cap</b>, you can sign any free agent whose first-year
					salary fits in your cap space.
				</li>
				<li>
					<b>Over the cap</b>, you can still re-sign your own players (in the
					Re-sign Players phase), use your mid-level exception once a season,
					and sign players for the minimum.
				</li>
				<li>
					Your cap status shows as a badge: Under the cap, Over the cap, Over
					the tax, or At the hard cap.
				</li>
			</GuideSection>
			<GuideSection title="Luxury tax">
				<li>
					The luxury tax line is <b>{money(rules.luxuryTax)}</b>. A team whose
					payroll is above it after the regular season pays{" "}
					{rules.luxuryTaxRate}× the amount over it.
				</li>
				<li>The tax only costs money. It doesn't block any move.</li>
			</GuideSection>
			<GuideSection title="Hard cap">
				<li>
					The hard cap is <b>{money(rules.hardCap)}</b>: the tax line plus{" "}
					{rules.hardCapPct}% of the cap.
					{rules.ownHardCaps.length > 0 ? (
						<>
							{" "}
							This league sets a lower one for{" "}
							{rules.ownHardCaps
								.map((row) => `${row.abbrev} (${money(row.amount)})`)
								.join(", ")}
							.
						</>
					) : null}
				</li>
				<li>
					No signing, re-signing, offer-sheet match or trade can take a team's
					payroll above its hard cap.
				</li>
				<li>
					Contracts already signed can push a team over it (raises, options).
					Then it can only sign players for the minimum, and only to get up to{" "}
					{rules.minRosterSize} players.
				</li>
				<li>
					In a trade, a team that would end up past its hard cap has to send out
					at least as much salary as it takes in.
				</li>
			</GuideSection>
			<GuideSection title="Contract amounts and length">
				<li>
					The minimum contract is <b>{money(rules.minContract)}</b> a year. The
					max is <b>{money(rules.maxContract)}</b> ({rules.maxContractPct}% of
					the cap), the same for every player.
				</li>
				<li>Any contract can run from 1 to {years(rules.maxLength)}.</li>
				<li>
					<b>Raises:</b> each season's salary can go up (or down) by up to{" "}
					{rules.raiseOwn}% of the first year when you re-sign your own player,{" "}
					{rules.raiseOther}% otherwise. A player judges a deal by its average
					salary.
				</li>
				<li>
					<b>Options:</b> on a deal of 2 years or more, the last season can be a
					player option (<Abbr k="PO" />, he decides whether to stay) or a team
					option (<Abbr k="TO" />, you decide). A player option makes an offer
					worth about {rules.playerOptionPct}% more to him. Players really don't
					like team options: one makes an offer worth about{" "}
					{rules.teamOptionPct}% less to him, so his chance of taking it drops a
					lot.
				</li>
				<li>
					Options are decided before free agency. A player opts out if the
					market would pay him clearly more. Your team options show on the
					Re-sign Players page: Pick up, or Decline and he joins your players to
					re-sign. Any you leave are picked up when free agency opens.
				</li>
			</GuideSection>
			<GuideSection title="Ways to sign a player">
				<li>
					<b>
						<Abbr k="Room" />
					</b>
					: your payroll stays under the cap after the signing.
				</li>
				<li>
					<b>
						<Abbr k="Bird" /> rights
					</b>
					: re-signing your own player in the Re-sign Players phase, for any
					amount up to the max, over the cap too (within the hard cap). Also how
					you match an offer sheet.
				</li>
				<li>
					<b>
						Mid-level exception (<Abbr k="MLE" />)
					</b>
					: over the cap, once a season, one player from another team for up to{" "}
					<b>{money(rules.midLevel)}</b> ({rules.midLevelPct}% of the cap) a
					year, as long as it doesn't take you past the hard cap. A team under
					the cap can use it too, when it is more than its room. The Teams tab
					shows who still has theirs.
				</li>
				<li>
					<b>
						Minimum (<Abbr k="Min" />)
					</b>
					: always allowed - past the hard cap only to fill the roster.
				</li>
				<li>
					The Signings tab shows how every signing was made, by these names.
				</li>
			</GuideSection>
			<GuideSection title="Re-signing your players">
				<li>
					When the season ends, your players whose contracts are up are on the
					Re-sign Players page, each with an offer at his asking price and the
					chance he takes it.
				</li>
				<li>
					Change the money and years and the chance updates. Press <b>Submit</b>{" "}
					and it's decided on the spot at that chance: he signs, or he leaves
					for free agency. There is no second try.
				</li>
				<li>
					<b>Decide all now</b> submits every offer you have up. Offers still up
					when free agency opens are decided then. <b>Let go</b> means no offer:
					he becomes a free agent.
				</li>
				<li>
					Your Bird rights end when free agency opens. After that he's a free
					agent like any other, unless he's restricted.
				</li>
				<li>
					Every decision, signed or not, goes in the transaction log with the
					offer and the odds.
				</li>
				{rules.extensionsOn ? (
					<li>
						In his last season you can offer an extension of up to{" "}
						{years(rules.extensionYears)} more.
					</li>
				) : null}
			</GuideSection>
			<GuideSection title="Restricted free agents">
				<li>
					A player finishing his rookie contract is a restricted free agent (
					<Abbr k="RFA" />
					).
				</li>
				<li>
					When he picks another team's offer, it becomes an <b>offer sheet</b>:
					his old team gets the next stage to match it on the same terms (the
					Match button on Free Agents; in multiplayer you also get a
					notification). Matched, he stays; if not, he goes to the new team.
				</li>
				<li>
					At training camp there's no stage left to match in, so he signs
					wherever he chooses.
				</li>
			</GuideSection>
			<GuideSection title="Free agency, in 5 stages">
				<li>
					Free agency runs in {rules.stages.length} stages instead of 30 days.
					Each Play moves to the next one.
				</li>
				<li>
					<b>{rules.stages[0]}:</b> every team makes offers. Nobody signs yet.
				</li>
				<li>
					<b>{rules.stages[1]}:</b> the top {rules.tier1} free agents decide.
					Each shows the offer <b>to beat</b>: the money and years of his best
					offer so far, not the team. Make your final offer. At the end of the
					stage he signs with an offer at his asking price or more, if he has
					one; if not, he waits for the next stage.
				</li>
				<li>
					<b>{rules.stages[2]}:</b> the next {rules.tier2} decide, the same way.
				</li>
				<li>
					<b>{rules.stages[3]}:</b> everyone else decides. A player signs when
					an offer is good enough, or waits for a better one.
				</li>
				<li>
					<b>{rules.stages[4]}:</b> the last stage. A player takes his best
					offer if it's at least his lowest price.
				</li>
				<li>
					Anyone still unsigned can be signed during the season. His price keeps
					falling as the season goes.
				</li>
			</GuideSection>
			<GuideSection title="How a free agent chooses">
				<li>
					<b>His asking price</b> is set when free agency opens, from what teams
					can pay him. During free agency it never goes up. It eases as the
					stages go, but in the five stages never below {floors.join("%, ")}% of
					what he first asked. At training camp his lowest price is at most{" "}
					{money(rules.campAskCap)}.
				</li>
				<li>
					<b>Money:</b> more than his ask helps a little (past{" "}
					{rules.overAskKneePct}% more, hardly at all); less than his ask hurts
					a lot.
				</li>
				<li>
					<b>Interest</b> (0-100, on Free Agents): how much he likes your team -
					winning, his role on it, loyalty to his old team, market size and hype
					- weighted by what he cares about (his mood traits).
				</li>
				<li>
					Every offer gets odds from both. A clearly better offer usually wins;
					close ones are close to a coin flip. The odds you see are your
					offer's.
				</li>
				<li>
					<b>
						<Abbr k="Prove-it" /> deals:
					</b>{" "}
					for less than {rules.proveItPct}% of his opening ask he signs for{" "}
					{years(rules.proveItYears)} at most, and that deal gives no Bird
					rights. From training camp on, any deal under{" "}
					{rules.lateLongPct === 100 ? "" : `${rules.lateLongPct}% of `}his
					opening ask is for {years(rules.proveItYears)} at most.
				</li>
			</GuideSection>
			<GuideSection title="Trades">
				<li>The usual trade rules apply, plus the hard cap rule above.</li>
				<li>
					The Teams tab shows every team's payroll, cap space, room under the
					hard cap and mid-level left.
				</li>
			</GuideSection>
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
		col("Cap Space"),
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
	{ key: "guide", label: "How it works" },
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
	if (tab === "guide") {
		return <GuideTab rules={rules} />;
	}
	return <RulesTab rules={rules} />;
};

export default ContractRules;
