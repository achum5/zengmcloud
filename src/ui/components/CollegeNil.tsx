import { useState } from "react";
import { helpers } from "../util/helpers.ts";

// Shared bits for college pages: star ratings, interest bars, and the NIL
// haggling panel used for recruits, portal players and renegotiations.

const STAR_COLORS = ["", "#adb5bd", "#adb5bd", "#0d6efd", "#fd7e14", "#dc3545"];

export const Stars = ({ stars }: { stars: number }) => (
	<span style={{ color: STAR_COLORS[stars], whiteSpace: "nowrap" }}>
		{"★".repeat(stars)}
		<span className="text-body-tertiary">{"★".repeat(5 - stars)}</span>
	</span>
);

export const InterestBar = ({ value }: { value: number }) => {
	// Interest runs past 100 for a player a school has worked hard on.
	const pct = helpers.bound(value / 1.25, 0, 100);
	const color =
		value >= 70 ? "bg-success" : value >= 50 ? "bg-warning" : "bg-secondary";
	return (
		<div className="d-flex align-items-center gap-1" style={{ minWidth: 90 }}>
			<div className="progress flex-grow-1" style={{ height: 6 }}>
				<div className={`progress-bar ${color}`} style={{ width: `${pct}%` }} />
			</div>
			<span className="small text-body-secondary" style={{ width: 22 }}>
				{Math.round(value)}
			</span>
		</div>
	);
};

export const fmtNil = (amount: number) =>
	helpers.formatCurrency(amount / 1000, "M");

export const fmtRange = ([lo, hi]: [number, number]) =>
	lo === hi ? String(lo) : `${lo}–${hi}`;

export type NilOutcome =
	| { type: "accepted"; amount: number }
	| { type: "countered"; counter: number; final: boolean; lowball: boolean }
	| { type: "walked" };

const outcomeText = (outcome: NilOutcome) => {
	if (outcome.type === "accepted") {
		return {
			className: "text-success",
			text: `Deal at ${fmtNil(outcome.amount)}/yr.`,
		};
	}
	if (outcome.type === "walked") {
		return { className: "text-danger", text: "He ended talks." };
	}
	return {
		className: outcome.lowball ? "text-danger" : "text-warning",
		text: `${outcome.lowball ? "Insulted. " : ""}He wants ${fmtNil(outcome.counter)}/yr${outcome.final ? " - final offer" : ""}.`,
	};
};

export const Patience = ({ value }: { value: number }) => (
	<span title="Patience" className="text-nowrap">
		{Array.from({ length: Math.max(0, Math.min(7, value)) }, (_, i) => (
			<span key={i} className="text-warning">
				●
			</span>
		))}
		{value <= 0 ? <span className="text-danger">Final</span> : null}
	</span>
);

// Range he's looking for, his standing counter, and an offer box.
export const NilNegotiation = ({
	range,
	room,
	current,
	counter,
	patience,
	walked,
	onOffer,
}: {
	range: [number, number];
	room: number;
	current?: number;
	counter?: number;
	patience?: number;
	walked: boolean;
	onOffer: (amount: number) => Promise<NilOutcome | undefined>;
}) => {
	const [amount, setAmount] = useState(
		String(counter ?? current ?? Math.round((range[0] + range[1]) / 2)),
	);
	const [outcome, setOutcome] = useState<NilOutcome | undefined>();
	const value = Number(amount);
	const valid = Number.isFinite(value) && value >= 0;
	const shown = outcome ? outcomeText(outcome) : undefined;

	const offer = async (x: number) => {
		const result = await onOffer(x);
		if (result) {
			setOutcome(result);
			if (result.type === "countered") {
				setAmount(String(result.counter));
			}
		}
	};

	if (walked) {
		return <p className="text-danger mb-0">He ended talks with you.</p>;
	}

	return (
		<>
			<div className="d-flex flex-wrap gap-3 mb-2">
				<span>
					Wants <b>{fmtNil(range[0])}</b>–<b>{fmtNil(range[1])}</b>/yr
				</span>
				<span className="text-body-secondary">{fmtNil(room)} left</span>
				{patience !== undefined ? <Patience value={patience} /> : null}
			</div>
			{current !== undefined ? (
				<p className="mb-2">
					Current deal: <b>{fmtNil(current)}</b>/yr
				</p>
			) : null}
			<div className="input-group mb-2">
				<span className="input-group-text">$</span>
				<input
					type="number"
					className="form-control"
					min={0}
					step={5}
					value={amount}
					onChange={(event) => setAmount(event.target.value)}
				/>
				<span className="input-group-text">k/yr</span>
				<button
					type="button"
					className="btn btn-primary"
					disabled={!valid}
					onClick={() => void offer(value)}
				>
					Offer
				</button>
			</div>
			{counter !== undefined ? (
				<button
					type="button"
					className="btn btn-success btn-sm mb-2"
					onClick={() => void offer(counter)}
				>
					Accept {fmtNil(counter)}
				</button>
			) : null}
			{shown ? <div className={shown.className}>{shown.text}</div> : null}
		</>
	);
};
