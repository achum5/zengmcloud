import clsx from "clsx";
import { useEffect, useRef, useState } from "react";
import { roundContract } from "../../common/roundContract.ts";
import { chancePercent } from "../../common/signingHow.ts";
import type api from "../../worker/api/index.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { showNotification } from "../util/showNotification.ts";
import { toWorker } from "../util/toWorker.ts";

// Realistic front offices: no instant signing. A team offers money and years,
// and the player chooses among his offers (or tests the market, while
// re-signing) with odds.

export const useRealisticFreeAgency = () => {
	const { realisticFrontOffices } = useLocal(["realisticFrontOffices"]);
	return __SPORT === "basketball" && !!realisticFrontOffices;
};

export type OfferProps = NonNullable<
	Exclude<
		Awaited<ReturnType<typeof api.main.getNegotiationProps>>,
		string
	>["offer"]
>;

export type OfferTerm = OfferProps["terms"][number];

// Amounts are in thousands, as the worker keeps them.
export const formatOfferAmount = (amount: number) =>
	helpers.formatCurrency(amount / 1000, "M");

export const formatChance = (chance: number | undefined) =>
	chance === undefined ? "…" : chancePercent(chance);

// One step of the - and + buttons: 5% of his ask for the length.
export const offerStep = (ask: number, minContract: number) =>
	Math.max(10, roundContract(ask * 0.05, minContract));

export const clampOffer = (
	amount: number,
	minContract: number,
	maxContract: number,
) =>
	Math.min(
		maxContract,
		Math.max(minContract, roundContract(amount, minContract)),
	);

// His chance of taking an offer, asked of the worker as the offer changes.
export const useOfferChance = (
	pid: number,
	amount: number,
	exp: number,
	initial: number | undefined,
	shape?: { raise?: number; option?: "player" | "team" },
) => {
	const raise = shape?.raise;
	const option = shape?.option;
	const [chance, setChance] = useState(initial);
	const request = useRef(0);
	const first = useRef(true);
	useEffect(() => {
		if (first.current) {
			first.current = false;
			return;
		}
		const id = ++request.current;
		setChance(undefined);
		const timer = setTimeout(async () => {
			const value = await toWorker("main", "getOfferChance", {
				pid,
				amount,
				exp,
				raise,
				option,
			});
			if (id === request.current) {
				setChance(value);
			}
		}, 150);
		return () => {
			clearTimeout(timer);
		};
	}, [pid, amount, exp, raise, option]);
	return chance;
};

// - $12.0M + : any amount from the minimum to the maximum contract.
export const AmountStepper = ({
	amount,
	ask,
	disabled,
	maxContract,
	minContract,
	onChange,
	size,
}: {
	amount: number;
	ask: number;
	disabled?: boolean;
	maxContract: number;
	minContract: number;
	onChange: (amount: number) => void;
	size?: "sm";
}) => {
	const step = offerStep(ask, minContract);
	const [text, setText] = useState((amount / 1000).toFixed(2));
	useEffect(() => {
		setText((amount / 1000).toFixed(2));
	}, [amount]);

	const button = clsx("btn btn-light-bordered", size === "sm" && "btn-sm");
	return (
		<div className="input-group flex-nowrap" style={{ width: "auto" }}>
			<button
				type="button"
				className={button}
				disabled={disabled || amount <= minContract}
				onClick={() =>
					onChange(clampOffer(amount - step, minContract, maxContract))
				}
				title={`-${formatOfferAmount(step)}`}
			>
				−
			</button>
			<span
				className={clsx("input-group-text", size === "sm" && "py-0 px-1 small")}
			>
				$
			</span>
			<input
				type="number"
				className={clsx(
					"form-control text-end",
					size === "sm" && "form-control-sm",
				)}
				style={{ width: size === "sm" ? 70 : 90 }}
				disabled={disabled}
				min={minContract / 1000}
				max={maxContract / 1000}
				step={0.05}
				value={text}
				onChange={(event) => {
					setText(event.target.value);
				}}
				onBlur={() => {
					const parsed = Number.parseFloat(text);
					if (Number.isNaN(parsed)) {
						setText((amount / 1000).toFixed(2));
					} else {
						const next = clampOffer(parsed * 1000, minContract, maxContract);
						setText((next / 1000).toFixed(2));
						if (next !== amount) {
							onChange(next);
						}
					}
				}}
				onKeyDown={(event) => {
					if (event.key === "Enter") {
						(event.target as HTMLInputElement).blur();
					}
				}}
			/>
			<span
				className={clsx("input-group-text", size === "sm" && "py-0 px-1 small")}
			>
				M
			</span>
			<button
				type="button"
				className={button}
				disabled={disabled || amount >= maxContract}
				onClick={() =>
					onChange(clampOffer(amount + step, minContract, maxContract))
				}
				title={`+${formatOfferAmount(step)}`}
			>
				+
			</button>
		</div>
	);
};

// Picking a new length moves the amount to his ask for it, unless the amount
// was changed from his ask.
export const amountForYears = (
	terms: OfferTerm[],
	amount: number,
	fromYears: number,
	toYears: number,
) => {
	const from = terms.find((term) => term.years === fromYears);
	const to = terms.find((term) => term.years === toYears);
	if (!to) {
		return amount;
	}
	return !from || from.ask === amount ? to.ask : amount;
};

// 1 yr 2 yrs ...: the lengths, his ask for each in the title, the one he
// wants in bold.
export const TermButtons = ({
	onPick,
	terms,
	wantedYears,
	years,
}: {
	onPick: (years: number) => void;
	terms: OfferTerm[];
	wantedYears: number;
	years: number;
}) => (
	<div className="btn-group mb-3 flex-wrap" role="group">
		{terms.map((option) => (
			<button
				key={option.years}
				type="button"
				className={clsx(
					"btn",
					option.years === years ? "btn-primary" : "btn-light-bordered",
					option.years === wantedYears && "fw-bold",
				)}
				onClick={() => onPick(option.years)}
				title={`Thru ${option.exp}, asking ${formatOfferAmount(option.ask)}${
					option.years === wantedYears ? " (the length he wants)" : ""
				}`}
			>
				{option.years} {helpers.plural("yr", option.years)}
			</button>
		))}
	</div>
);

export type OptionChoice = "" | "player" | "team";

// CBA-lite: a yearly raise (in %, up to maxRaise either way) and an option on
// the final season.
export const ShapePickers = ({
	maxRaise,
	option,
	raise,
	setOption,
	setRaise,
	years,
}: {
	maxRaise: number;
	option: OptionChoice;
	raise: number;
	setOption: (option: OptionChoice) => void;
	setRaise: (raise: number) => void;
	years: number;
}) => (
	<div className="d-flex flex-wrap align-items-center gap-2 mb-3">
		<select
			className="form-select form-select-sm"
			style={{ width: "auto" }}
			value={raise}
			onChange={(event) => setRaise(Number(event.target.value))}
			title="Yearly raise"
		>
			{[-maxRaise, 0, 2.5, maxRaise].map((x) => (
				<option key={x} value={x}>
					{x > 0 ? "+" : ""}
					{x}% a year
				</option>
			))}
		</select>
		<select
			className="form-select form-select-sm"
			style={{ width: "auto" }}
			value={option}
			disabled={years < 2}
			onChange={(event) => setOption(event.target.value as OptionChoice)}
			title="Option on the final season"
		>
			<option value="">No option</option>
			<option value="player">Player option</option>
			<option value="team">Team option</option>
		</select>
	</div>
);

// The offer screen, in place of the list of contracts to sign.
export const OfferEditor = ({
	capSpace,
	close,
	name,
	offer,
	pid,
}: {
	// In millions, as the negotiation header has it.
	capSpace: number;
	close: () => void;
	name: string;
	offer: OfferProps;
	pid: number;
}) => {
	const { gameSimInProgress, minContract, salaryCapType } = useLocal([
		"gameSimInProgress",
		"minContract",
		"salaryCapType",
	]);
	const start = offer.current ?? {
		years: offer.wantedYears,
		amount: offer.terms.find((term) => term.years === offer.wantedYears)!.ask,
	};
	const [years, setYears] = useState(start.years);
	const [amount, setAmount] = useState(start.amount);
	const term =
		offer.terms.find((term) => term.years === years) ?? offer.terms[0]!;
	// Free agency in stages (CBA-lite): a yearly raise and an option on the
	// final season.
	const [raise, setRaise] = useState(0);
	const [option, setOption] = useState<OptionChoice>("");
	const maxRaise = offer.stage?.own ? 8 : 5;
	const shape = offer.stage
		? {
				raise: raise / 100,
				option: option === "" || years < 2 ? undefined : option,
			}
		: undefined;
	const chance = useOfferChance(pid, amount, term.exp, offer.chance, shape);
	const [saving, setSaving] = useState(false);

	// The cap rule a signing has to meet, as in stock: under the cap, or a
	// minimum contract, or re-signing his own player over a soft cap.
	const overCap =
		salaryCapType !== "none" &&
		!(offer.reSigning && salaryCapType === "soft") &&
		amount > capSpace * 1000 + 1 &&
		amount > minContract + 1;

	const changed =
		!offer.current ||
		offer.current.amount !== amount ||
		offer.current.exp !== term.exp;

	const save = async () => {
		setSaving(true);
		try {
			const error = await toWorker("main", "makeFreeAgentOffer", {
				pid,
				amount,
				exp: term.exp,
				...shape,
			});
			if (typeof error === "string") {
				showNotification({ type: "error", text: error });
			} else {
				close();
			}
		} finally {
			setSaving(false);
		}
	};

	return (
		<div style={{ maxWidth: 575 }}>
			<TermButtons
				terms={offer.terms}
				years={years}
				wantedYears={offer.wantedYears}
				onPick={(next) => {
					setAmount(amountForYears(offer.terms, amount, years, next));
					setYears(next);
				}}
			/>

			<div className="d-flex flex-wrap align-items-center gap-3 mb-3">
				<AmountStepper
					amount={amount}
					ask={term.ask}
					maxContract={offer.maxContract}
					minContract={offer.minContract}
					onChange={setAmount}
				/>
				<div className="text-body-secondary">
					Ask {formatOfferAmount(term.ask)} thru {term.exp}
				</div>
			</div>

			{offer.stage ? (
				<ShapePickers
					maxRaise={maxRaise}
					option={option}
					raise={raise}
					setOption={setOption}
					setRaise={setRaise}
					years={years}
				/>
			) : null}
			{offer.stage ? (
				<div className="mb-3" data-testid="offer-stage">
					{offer.stage.toBeat ? (
						<>
							To beat{" "}
							<b>
								{formatOfferAmount(offer.stage.toBeat.amount)} ×{" "}
								{offer.stage.toBeat.years}
							</b>
							{" · "}
						</>
					) : null}
					Decides <b>{offer.stage.decidesLabel}</b>
				</div>
			) : null}

			<div className="d-flex flex-wrap align-items-center gap-2">
				<div className="me-auto">
					Your odds{" "}
					<span className="fs-4 fw-bold" data-testid="offer-odds">
						{formatChance(chance)}
					</span>
					{offer.rank !== undefined ? (
						<span
							className="text-body-secondary ms-2"
							title="Priority among your offers"
						>
							#{offer.rank} of {offer.numOffers}
						</span>
					) : null}
				</div>
				{offer.current ? (
					<button
						type="button"
						className="btn btn-light-bordered"
						disabled={saving || gameSimInProgress}
						onClick={async () => {
							await toWorker("main", "withdrawFreeAgentOffer", pid);
							close();
						}}
					>
						Withdraw
					</button>
				) : null}
				{overCap ? (
					<span className="text-danger small">Over your cap space</span>
				) : null}
				<button
					type="button"
					className="btn btn-success"
					disabled={saving || !changed || overCap || gameSimInProgress}
					onClick={save}
					title={`Offer ${name} ${formatOfferAmount(amount)} thru ${term.exp}`}
				>
					{offer.current ? "Update" : "Offer"}
				</button>
			</div>
		</div>
	);
};
