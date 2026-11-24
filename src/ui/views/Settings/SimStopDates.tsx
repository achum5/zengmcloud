import { useState } from "react";
import {
	formatSimStopDays,
	formatStopDate,
	parseSimStopDays,
	type SimStopDate,
	type SimStopDays,
} from "../../../common/simStopDays.ts";

// "Pause Sim On Days" on the NBA calendar: dates, the same each season
// (common/simStopDays.ts) - presets for the days a league pauses on, or a
// month and day of its own. No year: a phone's date picker opens on today's
// real date, nowhere near the league's. The setting is still the one string,
// so a league switching the calendar off keeps what it can read.

// The season's months, in its order.
const MONTHS: { month: number; name: string; days: number }[] = [
	{ month: 10, name: "Oct", days: 31 },
	{ month: 11, name: "Nov", days: 30 },
	{ month: 12, name: "Dec", days: 31 },
	{ month: 1, name: "Jan", days: 31 },
	{ month: 2, name: "Feb", days: 29 },
	{ month: 3, name: "Mar", days: 31 },
	{ month: 4, name: "Apr", days: 30 },
	{ month: 5, name: "May", days: 31 },
	{ month: 6, name: "Jun", days: 30 },
];

// Summer signings can be traded (game/tradableOn.ts), then re-signings.
const PRESET_DATES: SimStopDate[] = [
	{ month: 12, day: 15 },
	{ month: 1, day: 15 },
];

const withDate = (stops: SimStopDays, date: SimStopDate) =>
	// Through the parser: in order, and once.
	formatSimStopDays(
		parseSimStopDays(
			formatSimStopDays({ ...stops, dates: [...stops.dates, date] }),
		),
	);

const hasDate = (stops: SimStopDays, date: SimStopDate) =>
	stops.dates.some((d) => d.month === date.month && d.day === date.day);

// A month and a day of the league's own.
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
	const [month, setMonth] = useState("");
	const [day, setDay] = useState("");
	const days = MONTHS.find((m) => String(m.month) === month)?.days ?? 31;

	return (
		<div className="d-flex gap-1">
			<select
				className="form-select"
				id={id}
				aria-label="Month"
				disabled={disabled}
				style={{ width: 84 }}
				value={month}
				onChange={(event) => {
					setMonth(event.target.value);
				}}
			>
				<option value="">Month</option>
				{MONTHS.map((m) => (
					<option key={m.month} value={m.month}>
						{m.name}
					</option>
				))}
			</select>
			<select
				className="form-select"
				aria-label="Day"
				disabled={disabled}
				style={{ width: 72 }}
				value={day}
				onChange={(event) => {
					setDay(event.target.value);
				}}
			>
				<option value="">Day</option>
				{Array.from({ length: days }, (_, i) => (
					<option key={i + 1} value={i + 1}>
						{i + 1}
					</option>
				))}
			</select>
			<button
				type="button"
				className="btn btn-secondary"
				disabled={disabled || month === "" || day === "" || Number(day) > days}
				onClick={() => {
					onChange(
						withDate(parseSimStopDays(value), {
							month: Number(month),
							day: Number(day),
						}),
					);
					setMonth("");
					setDay("");
				}}
			>
				Add
			</button>
		</div>
	);
};

// The stops as chips, each one removable, and the presets not yet in.
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

	const presets = [
		...PRESET_DATES.filter((date) => !hasDate(stops, date)).map((date) => ({
			key: `date-${date.month}-${date.day}`,
			label: formatStopDate(date),
			add: () => {
				onChange(withDate(stops, date));
			},
		})),
		...(!stops.deadline
			? [
					{
						key: "deadline",
						label: "Trade deadline",
						add: () => {
							set({ ...stops, deadline: true });
						},
					},
				]
			: []),
	];

	return (
		<div className="d-flex flex-wrap align-items-center gap-1 mt-2">
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
			{presets.map((preset) => (
				<button
					key={preset.key}
					type="button"
					className="btn btn-sm btn-outline-secondary rounded-pill py-0"
					style={{ fontSize: "0.875rem" }}
					disabled={disabled}
					onClick={preset.add}
				>
					+ {preset.label}
				</button>
			))}
		</div>
	);
};
