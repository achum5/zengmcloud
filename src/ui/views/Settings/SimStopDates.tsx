import { useState } from "react";
import { PHASE } from "../../../common/constants.ts";
import {
	formatSimStopDays,
	formatStopDate,
	parseSimStopDays,
	type SimStopDays,
} from "../../../common/simStopDays.ts";
import { useLocal } from "../../util/local.ts";

// "Pause Sim On Days" on the NBA calendar: dates picked off a calendar, the
// same each season (common/simStopDays.ts). The setting is still the one
// string, so a league switching the calendar off keeps what it can read.

// The season whose dates the picker shows: the one being played, or in the
// offseason the one coming (named, like the game's seasons, for the year it
// ends in).
const useSeasonYears = () => {
	const { phase, season } = useLocal(["phase", "season"]);
	const end = phase > PHASE.PLAYOFFS ? season + 1 : season;
	return { from: `${end - 1}-10-01`, to: `${end}-06-30` };
};

export const SimStopDatePicker = ({
	disabled,
	id,
	value,
	onChange,
}: {
	disabled: boolean;
	id: string;
	value: string;
	onChange: (value: string) => void;
}) => {
	const [picked, setPicked] = useState("");
	const { from, to } = useSeasonYears();

	const add = () => {
		const month = Number(picked.slice(5, 7));
		const day = Number(picked.slice(8, 10));
		if (!month || !day) {
			return;
		}
		const stops = parseSimStopDays(value);
		onChange(
			formatSimStopDays(
				parseSimStopDays(
					formatSimStopDays({
						...stops,
						dates: [...stops.dates, { month, day }],
					}),
				),
			),
		);
		setPicked("");
	};

	return (
		<div className="d-flex gap-1" style={{ maxWidth: 200 }}>
			<input
				type="date"
				className="form-control"
				id={id}
				disabled={disabled}
				min={from}
				max={to}
				value={picked}
				onChange={(event) => {
					setPicked(event.target.value);
				}}
			/>
			<button
				type="button"
				className="btn btn-secondary"
				disabled={disabled || picked === ""}
				onClick={add}
			>
				Add
			</button>
		</div>
	);
};

// The stops as chips, each one removable, and the trade deadline.
export const SimStopChips = ({
	disabled,
	value,
	onChange,
}: {
	disabled: boolean;
	value: string;
	onChange: (value: string) => void;
}) => {
	const stops = parseSimStopDays(value);
	const set = (next: SimStopDays) => {
		onChange(formatSimStopDays(next));
	};

	const chips = [
		...stops.dates.map((date) => ({
			key: `date-${date.month}-${date.day}`,
			label: formatStopDate(date),
			remove: () => {
				set({ ...stops, dates: stops.dates.filter((d) => d !== date) });
			},
		})),
		...stops.days.map((day) => ({
			key: `day-${day}`,
			label: `Day ${day}`,
			remove: () => {
				set({ ...stops, days: stops.days.filter((d) => d !== day) });
			},
		})),
		...(stops.deadline
			? [
					{
						key: "deadline",
						label: "Trade deadline",
						remove: () => {
							set({ ...stops, deadline: false });
						},
					},
				]
			: []),
	];

	return (
		<div className="d-flex flex-wrap gap-1 mt-2">
			{chips.map((chip) => (
				<span
					key={chip.key}
					className="badge rounded-pill text-bg-secondary d-inline-flex align-items-center gap-1 fw-normal"
					style={{ fontSize: "0.875rem" }}
				>
					{chip.label}
					<button
						type="button"
						className="btn-close btn-close-white"
						style={{ fontSize: "0.5rem" }}
						disabled={disabled}
						aria-label={`Remove ${chip.label}`}
						onClick={chip.remove}
					/>
				</span>
			))}
			{!stops.deadline ? (
				<button
					type="button"
					className="btn btn-sm btn-link p-0 text-decoration-none"
					disabled={disabled}
					onClick={() => {
						set({ ...stops, deadline: true });
					}}
				>
					+ Trade deadline
				</button>
			) : null}
		</div>
	);
};
