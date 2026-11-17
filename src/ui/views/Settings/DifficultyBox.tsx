import { useEffect, type ChangeEvent, type CSSProperties } from "react";
import { DIFFICULTY } from "../../../common/constants.ts";
import type { State } from "./SettingsForm.tsx";
import type { HandleChange, HandleChangeRaw } from "./SettingsFormOptions.tsx";

// ONE BOX FOR DIFFICULTY (owner, 2026-11-17). The main Difficulty sets
// trading, signing and revenue alike, as in stock BBGM, until one of the three
// is set on its own: then all three hold their own values and the main one is
// greyed out (it still names the league's difficulty, for achievements). Sync
// puts all three back on the main one. Smart CPU rides along beneath.

export const DIFFICULTY_VALUES = [
	{ key: String(DIFFICULTY.Easy), value: "Easy" },
	{ key: String(DIFFICULTY.Normal), value: "Normal" },
	{ key: String(DIFFICULTY.Hard), value: "Hard" },
	{ key: String(DIFFICULTY.Insane), value: "Insane" },
];

const PARTS = [
	{ key: "difficultyTrade", name: "Trading" },
	{ key: "difficultySigning", name: "Signing" },
	{ key: "difficultyRevenue", name: "Revenue" },
] as const;

type Props = {
	disabled: boolean;
	handleChange: HandleChange;
	handleChangeRaw: HandleChangeRaw;
	id: string;
	inputStyle: CSSProperties;
	state: State;
};

// Whether any of the three is set on its own.
const isSplit = (state: State) =>
	PARTS.some(({ key }) => state[key].trim() !== "");

// The main difficulty's number, as typed.
const mainValue = (state: State): string => JSON.parse(state.difficulty)[1];

export const DifficultyMain = ({
	disabled,
	handleChange,
	id,
	inputStyle,
	state,
}: Props) => {
	const split = isSplit(state);
	const parsed = JSON.parse(state.difficulty);
	const selectValue =
		parsed[0] || DIFFICULTY_VALUES.every(({ key }) => key !== parsed[1])
			? "custom"
			: parsed[1];
	const onChange = handleChange("difficulty", "floatValuesOrCustom");

	return (
		<div
			className="input-group"
			style={inputStyle}
			title={split ? "Set separately below" : undefined}
		>
			<select
				className="form-select"
				disabled={disabled || split}
				id={id}
				onChange={onChange}
				style={{ width: 60 }}
				value={selectValue}
			>
				{DIFFICULTY_VALUES.map(({ key, value }) => (
					<option key={key} value={key}>
						{value}
					</option>
				))}
				<option value="custom">Custom</option>
			</select>
			<input
				aria-label="Difficulty"
				className="form-control"
				disabled={disabled || split || selectValue !== "custom"}
				inputMode="decimal"
				onChange={onChange}
				type="text"
				value={parsed[1]}
			/>
		</div>
	);
};

export const DifficultyParts = ({
	disabled,
	handleChange,
	handleChangeRaw,
	id,
	inputStyle,
	state,
}: Props) => {
	const split = isSplit(state);
	const main = mainValue(state);

	// Opened split with one left blank (set before Revenue had its own): show
	// the value it has, the main one's, so the greyed-out main sets nothing.
	useEffect(() => {
		if (isSplit(state)) {
			for (const part of PARTS) {
				if (state[part.key].trim() === "") {
					handleChangeRaw(part.key)(mainValue(state));
				}
			}
		}
		// Once, as the box opens.
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, []);

	const onChange =
		(key: (typeof PARTS)[number]["key"]) =>
		(event: ChangeEvent<HTMLInputElement>) => {
			const value = event.target.value;
			if (!split && value.trim() !== "") {
				// The first one set on its own: the others keep the main value, so
				// the main one no longer sets anything here.
				for (const part of PARTS) {
					if (part.key !== key) {
						handleChangeRaw(part.key)(main);
					}
				}
			}
			handleChangeRaw(key)(value);
		};

	return (
		<div className="mt-1">
			{PARTS.map(({ key, name }) => (
				<div className="d-flex align-items-center ps-3 mt-1" key={key}>
					<label className="form-label mb-0 me-auto" htmlFor={`${id}-${key}`}>
						{name}
					</label>
					<input
						className="form-control"
						disabled={disabled}
						id={`${id}-${key}`}
						inputMode="decimal"
						onBlur={() => {
							// Emptied while split: back to what it showed, the main value.
							if (split && state[key].trim() === "") {
								handleChangeRaw(key)(main);
							}
						}}
						onChange={onChange(key)}
						placeholder={main}
						style={inputStyle}
						type="text"
						value={state[key]}
					/>
				</div>
			))}
			{split ? (
				<div className="d-flex justify-content-end mt-1">
					<button
						className="btn btn-sm btn-secondary"
						disabled={disabled}
						onClick={() => {
							for (const part of PARTS) {
								handleChangeRaw(part.key)("");
							}
						}}
						title="Use Difficulty for all three"
						type="button"
					>
						Sync
					</button>
				</div>
			) : null}
			{__SPORT === "basketball" ? (
				<div className="d-flex align-items-center border-top mt-2 pt-2">
					<label
						className="form-label mb-0 me-auto"
						htmlFor={`${id}-smartCpu`}
					>
						Smart CPU
					</label>
					<div
						className="form-check form-switch"
						title={state.smartCpu === "true" ? "Enabled" : "Disabled"}
					>
						<input
							checked={state.smartCpu === "true"}
							className="form-check-input"
							disabled={disabled}
							id={`${id}-smartCpu`}
							onChange={handleChange("smartCpu", "bool")}
							type="checkbox"
						/>
					</div>
				</div>
			) : null}
		</div>
	);
};
