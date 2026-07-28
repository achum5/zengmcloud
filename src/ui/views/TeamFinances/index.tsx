import {
	type ChangeEvent,
	type SubmitEvent,
	Fragment,
	useEffect,
	useState,
	type ReactNode,
} from "react";
import { DataTable } from "../../components/DataTable/index.tsx";
import { MoreLinks } from "../../components/MoreLinks.tsx";
import useTitleBar from "../../hooks/useTitleBar.tsx";
import { helpers } from "../../util/helpers.ts";
import { showNotification } from "../../util/showNotification.ts";
import { toWorker } from "../../util/toWorker.ts";
import { getCols } from "../../../common/getCols.ts";
import { useLocal } from "../../util/local.ts";
import { hardCapForTid } from "../../../common/getHardCap.ts";
import type { View } from "../../../common/types.ts";
import { PHASE } from "../../../common/constants.ts";
import { wrappedPlayerNameLabels } from "../../components/PlayerNameLabels.tsx";
import type { Col, DataTableRow } from "../../components/DataTable/index.tsx";
import {
	DEFAULT_LEVEL,
	MAX_LEVEL,
	coachingEffect,
	facilitiesEffectAttendance,
	facilitiesEffectMood,
	healthEffect,
	levelToAmount,
	levelToEffect,
	scoutingEffectCutoff,
	scoutingEffectStddev,
} from "../../../common/budgetLevels.ts";
import { CurrencyInputGroup } from "../../components/CurrencyInputGroup.tsx";
import { useBlocker } from "../../hooks/useBlocker.ts";
import { gradientStyleFactory } from "../../util/gradientStyleFactory.ts";
import { BarGraph } from "./BarGraph.tsx";
import { HelpPopover } from "../../components/HelpPopover.tsx";
import { getAdjustedTicketPrice } from "../../../common/getAdjustedTicketPrice.ts";

const paddingLeft85 = { paddingLeft: 85 };

const formatTicketPrice = (ticketPrice: number) => {
	// Never show just one decimal place, because it's cents
	if (!Number.isInteger(ticketPrice) && Number.isInteger(ticketPrice * 10)) {
		return ticketPrice.toFixed(2);
	}
	return String(ticketPrice);
};

type BudgetKey = "scouting" | "coaching" | "health" | "facilities";

const roundEffect = (effect: number, includePlusSign: boolean) => {
	return `${includePlusSign && effect >= 0 ? "+" : ""}${effect.toLocaleString(
		"en-US",
		{
			maximumFractionDigits: 2,
			minimumFractionDigits: 0,
		},
	)}`;
};

const GradientStyle = ({
	children,
	level,
}: {
	children: ReactNode;
	level: number;
}) => {
	const gradientStyle = gradientStyleFactory(-0.9, -0.05, 0.05, 0.9);

	return <span style={gradientStyle(levelToEffect(level))}>{children}</span>;
};

const BudgetEffect = ({ type, level }: { type: BudgetKey; level: number }) => {
	if (type === "scouting") {
		const effectCutoff = scoutingEffectCutoff(level);
		const effectStddev = scoutingEffectStddev(level);
		return (
			<>
				Ratings error stddev:{" "}
				<GradientStyle level={level}>
					{roundEffect(effectStddev, false)}
				</GradientStyle>
				<br />
				Maximum ratings error:{" "}
				<GradientStyle level={level}>{effectCutoff}</GradientStyle>
			</>
		);
	}

	if (type === "coaching") {
		const effect = coachingEffect(level);
		if (effect === 0) {
			return "Normal progs";
		}

		return (
			<>
				<GradientStyle level={level}>
					{roundEffect(100 * effect, true)}%
				</GradientStyle>{" "}
				positive progs
				<br />
				<GradientStyle level={level}>
					{roundEffect(-100 * effect, true)}%
				</GradientStyle>{" "}
				negative progs
			</>
		);
	}

	if (type === "health") {
		const effect = healthEffect(level);
		if (effect === 0) {
			return "Normal injury duration";
		}

		return (
			<>
				<GradientStyle level={level}>
					{roundEffect(100 * effect, true)}%
				</GradientStyle>{" "}
				injury duration
			</>
		);
	}

	const effectMood = facilitiesEffectMood(level);
	const effectAttendance = facilitiesEffectAttendance(level);
	return (
		<>
			<GradientStyle level={level}>
				{roundEffect(effectMood, true)}
			</GradientStyle>{" "}
			player mood
			<br />
			<GradientStyle level={level}>
				{roundEffect(100 * effectAttendance, true)}%
			</GradientStyle>{" "}
			ticket demand
		</>
	);
};

const FinancesForm = ({
	autoTicketPrice,
	gameSimInProgress,
	otherTeamTicketPrices,
	t,
	tid,
}: Pick<
	View<"teamFinances">,
	"autoTicketPrice" | "otherTeamTicketPrices" | "t" | "tid"
> & {
	gameSimInProgress: boolean;
}) => {
	const { challengeNoRatings, godMode, salaryCap, spectator, phase, userTid } =
		useLocal([
			"challengeNoRatings",
			"godMode",
			"salaryCap",
			"spectator",
			"phase",
			"userTid",
		]);

	const { dirty, setDirty } = useBlocker();

	const [state, setState] = useState({
		saving: false,
		coaching: String(t.budget.coaching),
		facilities: String(t.budget.facilities),
		health: String(t.budget.health),
		scouting: String(t.budget.scouting),
		ticketPrice: formatTicketPrice(t.budget.ticketPrice),
		adjustForInflation: t.adjustForInflation,
		autoTicketPrice: t.autoTicketPrice,
	});

	useEffect(() => {
		if (!dirty) {
			setState((state2) => ({
				...state2,
				coaching: String(t.budget.coaching),
				facilities: String(t.budget.facilities),
				health: String(t.budget.health),
				scouting: String(t.budget.scouting),
				ticketPrice: formatTicketPrice(t.budget.ticketPrice),
				adjustForInflation: t.adjustForInflation,
				autoTicketPrice: t.autoTicketPrice,
			}));
		}
	}, [dirty, t]);

	const setStateValue = <Name extends Exclude<keyof typeof state, "saving">>(
		name: Name,
		value: (typeof state)[Name],
	) => {
		setState((state2) => ({
			...state2,
			[name]: value,
		}));
		setDirty(true);
	};

	const handleChange =
		(name: Exclude<keyof typeof state, "dirty" | "saving">) =>
		(event: ChangeEvent<HTMLInputElement>) => {
			if (name === "adjustForInflation" || name === "autoTicketPrice") {
				setStateValue(name, event.target.checked);
			} else {
				setStateValue(name, event.target.value);
			}
		};

	const handleSubmit = async (event: SubmitEvent) => {
		event.preventDefault();

		setState((state2) => ({ ...state2, saving: true }));

		const budgetAmounts = {
			// Convert from [millions of dollars] to [thousands of dollars] rounded to the nearest $10k
			coaching: helpers.bound(
				Math.round(helpers.localeParseFloat(state.coaching)),
				1,
				MAX_LEVEL,
			),
			facilities: helpers.bound(
				Math.round(helpers.localeParseFloat(state.facilities)),
				1,
				MAX_LEVEL,
			),
			health: helpers.bound(
				Math.round(helpers.localeParseFloat(state.health)),
				1,
				MAX_LEVEL,
			),
			scouting: helpers.bound(
				Math.round(helpers.localeParseFloat(state.scouting)),
				1,
				MAX_LEVEL,
			),

			// Already in [dollars]
			ticketPrice: helpers.bound(
				helpers.localeParseFloat(
					helpers.localeParseFloat(state.ticketPrice).toFixed(2),
				),
				0,
				Infinity,
			),
		};

		await toWorker("main", "updateBudget", {
			budgetLevels: budgetAmounts,
			adjustForInflation: state.adjustForInflation || state.autoTicketPrice,
			autoTicketPrice: state.autoTicketPrice,
		});

		showNotification({
			type: "success",
			text: "Saved team finance settings.",
		});

		setState((state2) => ({
			...state2,
			saving: false,
		}));
		setDirty(false);
	};

	const warningMessage =
		gameSimInProgress && tid === userTid && !spectator ? (
			<div className="text-danger mb-2">Stop game simulation to edit.</div>
		) : null;

	const formDisabled = gameSimInProgress || tid !== userTid || spectator;

	const expenseCategories: {
		key: BudgetKey;
		title: string;
	}[] = [
		{
			key: "scouting",
			title: "Scouting",
		},
		{
			key: "coaching",
			title: "Coaching",
		},
		{
			key: "health",
			title: "Health",
		},
		{
			key: "facilities",
			title: "Facilities",
		},
	];

	const ticketPriceForRank = state.autoTicketPrice
		? autoTicketPrice
		: helpers.localeParseFloat(state.ticketPrice);
	const ticketPriceRankIndex = otherTeamTicketPrices.findIndex(
		(price) => price < ticketPriceForRank,
	);
	const ticketPriceRank =
		ticketPriceRankIndex < 0
			? otherTeamTicketPrices.length + 1
			: ticketPriceRankIndex + 1;

	const [projectedAttendance, setProjectedAttendance] = useState<
		number | undefined
	>();
	useEffect(() => {
		let mounted = true;
		(async () => {
			const attendance = await toWorker("main", "getProjectedAttendance", {
				tid,
				ticketPrice: ticketPriceForRank,
			});

			if (mounted) {
				setProjectedAttendance(attendance);
			}
		})();

		return () => {
			mounted = false;
		};
	}, [ticketPriceForRank, tid]);

	return (
		<form onSubmit={handleSubmit} className="mb-3">
			{warningMessage}
			<h2>
				Expense levels{" "}
				<HelpPopover title="Expense levels">
					<p>
						Expense levels can be set between 1 and {MAX_LEVEL}. {DEFAULT_LEVEL}{" "}
						is neutral, and at the high end there are diminishing returns to
						increasing the level.
					</p>
					<p>Effects are based on your spending over the past 3 seasons.</p>
				</HelpPopover>
			</h2>
			<div className="d-flex flex-column gap-3">
				{expenseCategories.map((expenseCategory) => {
					const level = state[expenseCategory.key];
					const levelInt = helpers.bound(
						Math.round(helpers.localeParseFloat(state[expenseCategory.key])),
						1,
						MAX_LEVEL,
					);
					const levelThree = t.expenseLevelsLastThree[expenseCategory.key];

					const formDisabledKey =
						formDisabled ||
						(expenseCategory.key === "scouting" && challengeNoRatings);

					return (
						<div key={expenseCategory.key}>
							<h3>{expenseCategory.title} expense level</h3>
							<div className="d-flex">
								<div>
									<div
										className="input-group mb-1"
										style={{
											width: 120,
										}}
									>
										<button
											className="btn btn-secondary"
											type="button"
											disabled={
												formDisabledKey ||
												Number.isNaN(levelInt) ||
												levelInt <= 1
											}
											onClick={() => {
												setStateValue(
													expenseCategory.key,
													String(levelInt - 1),
												);
											}}
										>
											−
										</button>
										<input
											type="text"
											className="form-control text-center"
											disabled={formDisabledKey}
											onChange={handleChange(expenseCategory.key)}
											value={level}
											inputMode="numeric"
										/>
										<button
											className="btn btn-secondary"
											type="button"
											disabled={
												formDisabledKey ||
												Number.isNaN(levelInt) ||
												levelInt >= MAX_LEVEL
											}
											onClick={() => {
												setStateValue(
													expenseCategory.key,
													String(levelInt + 1),
												);
											}}
										>
											+
										</button>
									</div>
									<div>Average of last 3 seasons: {levelThree}</div>
									<div>
										Current annual cost:{" "}
										{Number.isNaN(levelInt)
											? "???"
											: helpers.formatCurrency(
													levelToAmount(levelInt, salaryCap) / 1000,
													"M",
												)}
									</div>
								</div>
								<div className="row ms-3" style={{ width: 350 }}>
									{expenseCategory.key === "scouting" && godMode ? (
										<div className="col-12">
											Scouting does nothing in God Mode because you always see
											accurate ratings.
										</div>
									) : (
										<>
											<div className="col-6">
												<h4>Current effect</h4>
												<BudgetEffect
													type={expenseCategory.key}
													level={levelThree}
												/>
											</div>
											{levelInt !== levelThree ? (
												<div className="col-6">
													<h4>
														After 3 years at level{" "}
														{Number.isNaN(levelInt) ? "???" : levelInt}
													</h4>
													<BudgetEffect
														type={expenseCategory.key}
														level={levelInt}
													/>
												</div>
											) : null}
										</>
									)}
								</div>
							</div>
						</div>
					);
				})}
			</div>
			<h2 className="mt-4">
				Ticket price{" "}
				<HelpPopover title="Revenue Settings">
					Set your ticket price too high, and attendance will decrease and some
					fans will resent you for it. Set it too low, and you're not maximizing
					your profit.
				</HelpPopover>
			</h2>
			<div className="d-flex align-items-center">
				<CurrencyInputGroup
					style={{
						width: 115,
					}}
				>
					{state.autoTicketPrice ? (
						<input
							type="text"
							className="form-control"
							disabled
							value={formatTicketPrice(autoTicketPrice)}
						/>
					) : (
						<input
							type="text"
							className="form-control"
							disabled={formDisabled || state.autoTicketPrice}
							onChange={handleChange("ticketPrice")}
							value={state.ticketPrice}
							inputMode="decimal"
						/>
					)}
				</CurrencyInputGroup>
				<div className="ms-3">
					<div>Ticket price rank: #{ticketPriceRank}</div>
					<div>
						Projected attendance:{" "}
						{projectedAttendance !== undefined
							? helpers.numberWithCommas(projectedAttendance)
							: ""}
					</div>
				</div>
			</div>
			{phase === PHASE.PLAYOFFS ? (
				<div className="mt-1 text-warning">
					Playoffs price:{" "}
					{helpers.formatCurrency(
						getAdjustedTicketPrice(
							state.autoTicketPrice
								? autoTicketPrice
								: helpers.localeParseFloat(state.ticketPrice),
							true,
						),
					)}
				</div>
			) : null}
			<div className="mt-1 d-flex">
				<div className="form-check">
					<label className="form-check-label">
						<input
							className="form-check-input"
							onChange={handleChange("autoTicketPrice")}
							type="checkbox"
							checked={state.autoTicketPrice}
							disabled={formDisabled}
						/>
						Auto ticket price
					</label>
					<HelpPopover title="Auto ticket price" className="ms-1">
						<p>
							When enabled, your ticket price will be set to the maximum value
							possible while still selling out most games.
						</p>
						<p>
							In the playoffs, ticket prices automatically adjust to account for
							increased demand. That happens regardless of the "auto ticket
							price" setting.
						</p>
					</HelpPopover>
				</div>
				<div className="form-check ms-4">
					<label className="form-check-label">
						<input
							className="form-check-input"
							onChange={handleChange("adjustForInflation")}
							type="checkbox"
							checked={state.adjustForInflation || state.autoTicketPrice}
							disabled={formDisabled || state.autoTicketPrice}
						/>
						Auto adjust for inflation
					</label>
					<HelpPopover title="Inflation adjustment" className="ms-1">
						When enabled, your ticket price will automatically change whenever
						the salary cap changes.
					</HelpPopover>
				</div>
			</div>
			{tid === userTid && !spectator ? (
				<div className="mt-4" style={paddingLeft85}>
					<button
						className="btn btn-large btn-primary"
						disabled={formDisabled || state.saving}
						type="submit"
					>
						Save expense levels
						<br />
						and ticket price
					</button>
				</div>
			) : null}
		</form>
	);
};

const PayrollInfo = ({
	luxuryTaxAmount,
	minPayrollAmount,
	payroll,
	tid,
}: Pick<
	View<"teamFinances">,
	"luxuryTaxAmount" | "minPayrollAmount" | "payroll" | "tid"
>) => {
	const {
		hardCapAmount,
		hardCapTids,
		hardCapUseLuxuryTax,
		luxuryPayroll,
		luxuryTax,
		minContract,
		minPayroll,
		salaryCap,
		salaryCapType,
	} = useLocal([
		"hardCapAmount",
		"hardCapTids",
		"hardCapUseLuxuryTax",
		"luxuryPayroll",
		"luxuryTax",
		"minContract",
		"minPayroll",
		"salaryCap",
		"salaryCapType",
	]);

	const parts = [
		<>
			{payroll > minPayroll / 1000 ? "above" : "below"} the minimum payroll
			limit (<b>{helpers.formatCurrency(minPayroll / 1000, "M")}</b>
			{minPayrollAmount > 0 ? (
				<>
					,{" "}
					<span className="text-danger">
						projected penalty: {helpers.formatCurrency(minPayrollAmount, "M")}
					</span>
				</>
			) : null}
			)
		</>,
	];

	if (salaryCapType !== "none") {
		parts.push(
			<>
				{payroll > salaryCap / 1000 ? "above" : "below"} the salary cap (
				<b>{helpers.formatCurrency(salaryCap / 1000, "M")}</b>)
			</>,
		);
	}

	const hardCap = hardCapForTid(tid, {
		hardCapAmount,
		hardCapTids,
		hardCapUseLuxuryTax,
		luxuryPayroll,
	});
	if (Number.isFinite(hardCap)) {
		parts.push(
			<>
				{payroll > hardCap / 1000 ? "above" : "below"} the hard cap (
				<b>{helpers.formatCurrency(hardCap / 1000, "M")}</b>)
			</>,
		);
	}

	if (luxuryTax !== 0) {
		parts.push(
			<>
				{payroll > luxuryPayroll / 1000 ? "above" : "below"} the luxury tax
				limit (<b>{helpers.formatCurrency(luxuryPayroll / 1000, "M")}</b>
				{luxuryTaxAmount > 0 ? (
					<>
						,{" "}
						<span className="text-danger">
							projected penalty: {helpers.formatCurrency(luxuryTaxAmount, "M")}
						</span>
					</>
				) : null}
				)
			</>,
		);
	}

	return (
		<p>
			The current payroll (<b>{helpers.formatCurrency(payroll, "M")}</b>) is{" "}
			{parts.length === 1 ? (
				parts[0]
			) : parts.length <= 2 ? (
				<>
					{parts[0]} and {parts[1]}
				</>
			) : (
				<>
					{parts.slice(0, -1).map((part, i) => (
						<Fragment key={i}>{part}, </Fragment>
					))}
					and {parts.at(-1)}
				</>
			)}
			.{" "}
			{salaryCapType === "hard" ? (
				<HelpPopover title="Payroll Limits">
					<p>
						The salary cap is a hard cap, meaning that you cannot exceed it,
						even when re-signing your own players or making trades. The only
						exception is that you can always sign players to minimum contracts (
						{helpers.formatCurrency(minContract / 1000, "M")}/year), so you are
						never stuck with a team too small to play.
					</p>
					<p>
						Teams with payrolls below the minimum payroll limit will be assessed
						a fine equal to the difference at the end of the season.
					</p>
				</HelpPopover>
			) : salaryCapType === "soft" ? (
				<HelpPopover title="Payroll Limits">
					<p>
						The salary cap is a soft cap, meaning that you can exceed it to
						re-sign your own players, to sign free agents to minimum contracts (
						{helpers.formatCurrency(minContract / 1000, "M")}/year), and when
						making certain trades; however, you cannot exceed the salary cap to
						sign a free agent for more than the minimum.
					</p>
					<p>
						Teams with payrolls below the minimum payroll limit will be assessed
						a fine equal to the difference at the end of the season. Teams with
						payrolls above the luxury tax limit will be assessed a fine equal to{" "}
						{luxuryTax} times the difference at the end of the season.
					</p>
				</HelpPopover>
			) : (
				<HelpPopover title="Payroll Limits">
					<p>
						There is no salary cap, but the minimum payroll and luxury tax
						limits still apply.
					</p>
					<p>
						Teams with payrolls below the minimum payroll limit will be assessed
						a fine equal to the difference at the end of the season. Teams with
						payrolls above the luxury tax limit will be assessed a fine equal to{" "}
						{luxuryTax} times the difference at the end of the season.
					</p>
				</HelpPopover>
			)}
		</p>
	);
};

const highlightZeroNegative = (amount: number) => {
	const formattedValue = helpers.formatCurrency(amount, "M");

	if (amount === 0) {
		return { classNames: "text-body-secondary", value: formattedValue };
	}
	if (amount < 0) {
		return { classNames: "text-danger", value: formattedValue };
	}

	return formattedValue;
};

const TeamFinances = ({
	abbrev,
	autoTicketPrice,
	barData,
	contractTotals,
	contracts,
	luxuryTaxAmount,
	maxStadiumCapacity,
	minPayrollAmount,
	otherTeamTicketPrices,
	payroll,
	salariesSeasons,
	capsBySeason,
	show,
	t,
	tid,
}: View<"teamFinances">) => {
	useTitleBar({
		title: "Team Finances",
		dropdownView: "team_finances",
		dropdownFields: { teams: abbrev, shows: show },
	});

	const {
		budget,
		gameSimInProgress,
		hardCapAmount,
		hardCapTids,
		hardCapUseLuxuryTax,
		luxuryPayroll,
		numGames,
		salaryCap,
		salaryCapType,
	} = useLocal([
		"budget",
		"gameSimInProgress",
		"hardCapAmount",
		"hardCapTids",
		"hardCapUseLuxuryTax",
		"luxuryPayroll",
		"numGames",
		"salaryCap",
		"salaryCapType",
	]);

	// Which players' projected salaries count toward the totals, by row key.
	//
	// Empty by default: the totals are what the team has actually committed, and
	// that has to stay the plain reading of the table. Checking players answers
	// the other question - and one player at a time, because "keep everyone"
	// is rarely the plan. The real question is usually about two or three
	// specific guys, and an all-or-nothing toggle can't ask it.
	const [projectedKeys, setProjectedKeys] = useState<ReadonlySet<number>>(
		new Set(),
	);

	// Who even has a projection to include - a player signed through the last
	// column has nothing to check.
	const projectableKeys = contracts
		.map((p, i) => ({ p, i }))
		.filter(
			({ p }) =>
				!p.released && (p.amountsProjected ?? []).some((x) => x !== undefined),
		)
		.map(({ i }) => i);

	const allProjected =
		projectableKeys.length > 0 &&
		projectableKeys.every((key) => projectedKeys.has(key));

	const toggleAll = () => {
		setProjectedKeys(allProjected ? new Set() : new Set(projectableKeys));
	};

	const toggleOne = (key: number) => {
		const next = new Set(projectedKeys);
		if (next.has(key)) {
			next.delete(key);
		} else {
			next.add(key);
		}
		setProjectedKeys(next);
	};

	const cols: Col[] = [
		{
			title: "",
			desc: "Count this player's projected salary in the totals",
			noSearch: true,
			sortSequence: [],
			width: "1%",
			titleReact: (
				<input
					type="checkbox"
					className="form-check-input m-0"
					checked={allProjected}
					disabled={projectableKeys.length === 0}
					title="Include every projected salary"
					onChange={toggleAll}
				/>
			),
		},
		...getCols(["Pos", "Name", "Cap%"]),
		...salariesSeasons.map(
			(season): Col => ({
				title: String(season),
				sortSequence: ["desc", "asc"],
				sortType: "number",
			}),
		),
	];

	const rows = contracts.map((p, i) => {
		const projectable = projectableKeys.includes(i);
		const data: DataTableRow["data"] = [
			projectable
				? {
						value: (
							<input
								type="checkbox"
								className="form-check-input m-0"
								checked={projectedKeys.has(i)}
								title={`Count ${p.firstName} ${p.lastName}'s projected salary in the totals`}
								onChange={() => toggleOne(i)}
							/>
						),
						sortValue: projectedKeys.has(i) ? 1 : 0,
						searchValue: "",
					}
				: null,
			p.pos,
			wrappedPlayerNameLabels({
				injury: p.injury,
				jerseyNumber: p.jerseyNumber,
				pid: p.pid,
				skills: p.skills,
				style: { fontStyle: p.released ? "italic" : "normal" },
				defaultWatch: p.watch,
				firstName: p.firstName,
				firstNameShort: p.firstNameShort,
				lastName: p.lastName,
			}),
			p.capPct.toFixed(1),
		];

		// Loop through the salaries for the next five years for this player.
		for (let j = 0; j < salariesSeasons.length; j++) {
			const amount = p.amounts[j];
			if (amount !== undefined) {
				const formattedAmount = helpers.formatCurrency(amount, "M");

				if (p.released) {
					data.push({
						value: <i>{formattedAmount}</i>,
						sortValue: amount,
						searchValue: formattedAmount,
					});
				} else {
					data.push({
						value: formattedAmount,
						sortValue: amount,
						searchValue: formattedAmount,
					});
				}
			} else {
				// Past the end of his deal: what keeping him would cost. Muted and
				// asterisked so it never reads as money already committed.
				const projected = p.amountsProjected?.[j];
				if (projected !== undefined) {
					const formattedAmount = helpers.formatCurrency(projected, "M");
					data.push({
						classNames: "text-body-secondary",
						value: `${formattedAmount}*`,
						sortValue: projected,
						searchValue: formattedAmount,
						title: `Projected — ${p.firstName} ${p.lastName} is not under contract in ${salariesSeasons[j]}`,
					});
				} else {
					data.push(null);
				}
			}
		}

		return {
			key: i, // Can't be pid because a player will appear twice if he is cut and re-signed
			data,
		};
	});

	// Projected salaries live alongside the committed ones rather than replacing
	// them, so checking a player adds his to the totals without the table having
	// to be rebuilt.
	const projectedTotals = salariesSeasons.map((_, i) =>
		contracts.reduce(
			(sum, p, key) =>
				sum +
				(p.released || !projectedKeys.has(key)
					? 0
					: (p.amountsProjected?.[i] ?? 0)),
			0,
		),
	);
	const totals = contractTotals.map(
		(amount, i) => amount + (projectedTotals[i] ?? 0),
	);

	// Each column is measured against ITS season's cap, not today's - scheduled
	// events step the cap up every year in a real-players league, so a column
	// five seasons out judged against the current cap is badly wrong.
	const capsFor = (i: number) =>
		capsBySeason?.[i] ?? {
			salaryCap,
			luxuryPayroll,
			hardCapAmount,
			hardCapTids,
			hardCapUseLuxuryTax,
		};
	const hardCapFor = (i: number) => {
		const caps = capsFor(i);
		return hardCapForTid(tid, {
			hardCapAmount: caps.hardCapAmount,
			hardCapTids: caps.hardCapTids,
			hardCapUseLuxuryTax: caps.hardCapUseLuxuryTax,
			luxuryPayroll: caps.luxuryPayroll,
		});
	};
	const anyHardCap = totals.some((_, i) => Number.isFinite(hardCapFor(i)));

	// Marks the totals as carrying money nobody has signed for.
	const star = projectedKeys.size > 0 ? "*" : "";

	const footer = [
		{
			data: ["", "", `Totals${star}`, ""].concat(
				// @ts-expect-error
				totals.map((amount) => highlightZeroNegative(amount)),
			),
		},
		...(anyHardCap
			? [
					{
						data: ["", "", `Hard Cap Space${star}`, ""].concat(
							// @ts-expect-error
							totals.map((amount, i) => {
								const hardCap = hardCapFor(i);
								return Number.isFinite(hardCap)
									? highlightZeroNegative(hardCap / 1000 - amount)
									: null;
							}),
						),
					},
				]
			: []),
		{
			data:
				salaryCapType === "none"
					? ["", "", `Under Luxury Tax${star}`, ""].concat(
							// @ts-expect-error
							totals.map((amount, i) =>
								highlightZeroNegative(capsFor(i).luxuryPayroll / 1000 - amount),
							),
						)
					: ["", "", `Free Cap Space${star}`, ""].concat(
							// @ts-expect-error
							totals.map((amount, i) =>
								highlightZeroNegative(capsFor(i).salaryCap / 1000 - amount),
							),
						),
		},
	];

	// This happens for expansion teams before they have a TeamSeason
	const noSeasonData = barData.length === 0;

	type Row = (typeof barData)[number];
	const classNameOverride = (row: Row) =>
		row.champ ? "bar-graph-4" : undefined;
	const champSuffix = (row: Row) => (row.champ ? ", won championship" : "");

	return (
		<>
			<MoreLinks type="team" page="team_finances" abbrev={abbrev} tid={tid} />

			<PayrollInfo
				luxuryTaxAmount={luxuryTaxAmount}
				minPayrollAmount={minPayrollAmount}
				payroll={payroll}
				tid={tid}
			/>

			{budget ? null : (
				<p className="text-danger">
					The budget is disabled in this league, so most of the information
					usually shown here is hidden. You can change the budget setting in{" "}
					<a href={helpers.leagueUrl(["god_mode"])}>God Mode</a>.
				</p>
			)}

			<h2>Player Salaries</h2>

			<p>
				You can release players from{" "}
				<a href={helpers.leagueUrl(["roster"])}>your roster</a>. Released
				players who are still owed money are <i>shown in italics</i>. Salaries
				marked <span className="text-body-secondary">*</span> are projected, not
				signed. Check a player to count his projection in the totals.
			</p>

			<DataTable
				cols={cols}
				defaultSort={[4, "desc"]}
				name="TeamFinances"
				nonfluid
				footer={footer}
				rows={rows}
			/>

			<div className="row">
				<div className="col-md-3 col-sm-2">
					<h3>Wins</h3>
					<div className="bar-graph-small">
						<BarGraph
							data={barData}
							y={["won"]}
							tooltip={(row) => `${row.season}: ${row.won}${champSuffix(row)}`}
							ylim={[0, numGames]}
							classNameOverride={classNameOverride}
						/>
					</div>
					<br />
					<br />
					<h3>
						Hype{" "}
						<HelpPopover title="Hype">
							"Hype" refers to fans' interest in your team. If your team is
							winning or improving, then hype increases; if your team is losing
							or stagnating, then hype decreases. Hype influences attendance,
							various revenue sources such as merchandising, and the attitude
							players have towards your organization.
						</HelpPopover>
					</h3>
					<div id="bar-graph-hype" className="bar-graph-small">
						<BarGraph
							data={barData}
							y={["hype"]}
							tooltip={(row) =>
								`${row.season}: ${row.hype.toFixed(2)}${champSuffix(row)}`
							}
							ylim={[0, 1]}
							classNameOverride={classNameOverride}
						/>
					</div>
					<br />
					<br />
					<h3>Region Population</h3>
					<div id="bar-graph-pop" className="bar-graph-small">
						<BarGraph
							data={barData}
							y={["pop"]}
							tooltip={(row) =>
								`${row.season}: ${row.pop.toFixed(1)}M${champSuffix(row)}`
							}
							ylim={[0, 20]}
							classNameOverride={classNameOverride}
						/>
					</div>
					<br />
					<br />
					<h3>Average Attendance</h3>
					<div id="bar-graph-att" className="bar-graph-small">
						<BarGraph
							data={barData}
							y={["att"]}
							tooltip={(row) =>
								`${row.season}: ${helpers.numberWithCommas(
									Math.round(row.att),
								)}${champSuffix(row)}`
							}
							ylim={[0, maxStadiumCapacity]}
							classNameOverride={classNameOverride}
						/>
					</div>
				</div>
				{budget && !noSeasonData ? (
					<div className="col-lg-4 col-md-3 col-sm-3 mb-3">
						<h3>Revenue</h3>
						<div id="bar-graph-revenue" className="bar-graph-large">
							<BarGraph
								data={barData}
								y={[
									"revenuesNationalTv",
									"revenuesLocalTv",
									"revenuesTicket",
									"revenuesSponsor",
									"revenuesMerch",
									"revenuesLuxuryTaxShare",
								]}
								tooltip={(row, y) => {
									const text = {
										revenuesNationalTv: "national TV revenue",
										revenuesLocalTv: "local TV revenue",
										revenuesTicket: "ticket revenue",
										revenuesSponsor: "corporate sponsorship revenue",
										revenuesMerch: "merchandising revenue",
										revenuesLuxuryTaxShare: "luxury tax share revenue",
									};

									return `${row.season} ${text[y]}: ${helpers.formatCurrency(
										row[y] / 1000,
										"M",
										1,
									)}`;
								}}
							/>
						</div>
						<br />
						<br />
						<h3>Expenses</h3>
						<div id="bar-graph-expenses" className="bar-graph-large">
							<BarGraph
								data={barData}
								y={[
									"expensesSalary",
									"expensesScouting",
									"expensesCoaching",
									"expensesHealth",
									"expensesFacilities",
									"expensesMinTax",
									"expensesLuxuryTax",
								]}
								tooltip={(row, y) => {
									const text = {
										expensesSalary: "player salaries",
										expensesScouting: "scouting",
										expensesCoaching: "coaching",
										expensesHealth: "health",
										expensesFacilities: "facilities",
										expensesMinTax: "minimum payroll tax",
										expensesLuxuryTax: "luxury tax",
									};

									return `${row.season} ${text[y]}: ${helpers.formatCurrency(
										row[y] / 1000,
										"M",
										1,
									)}`;
								}}
							/>
						</div>
						<br />
						<br />
						<h3>Cash (cumulative)</h3>
						<div id="bar-graph-cash" className="bar-graph-medium">
							<BarGraph
								data={barData}
								y={["cash"]}
								tooltip={(row) =>
									`${row.season}: ${helpers.formatCurrency(
										row.cash,
										"M",
										1,
									)}${champSuffix(row)}`
								}
								classNameOverride={classNameOverride}
							/>
						</div>
					</div>
				) : null}
				{budget ? (
					<div className="col-lg-5 col-md-6 col-sm-7">
						<FinancesForm
							key={tid}
							autoTicketPrice={autoTicketPrice}
							gameSimInProgress={gameSimInProgress}
							otherTeamTicketPrices={otherTeamTicketPrices}
							t={t}
							tid={tid}
						/>
					</div>
				) : null}
			</div>
		</>
	);
};

export default TeamFinances;
