import clsx from "clsx";
import { helpers } from "../../util/helpers.ts";
import { OverlayTrigger, Tooltip } from "react-bootstrap";
import {
	categories,
	type LookingForState,
	type SetLookingForState,
} from "./useLookingForState.ts";
import { ResponsiveTableWrapper } from "../../components/ResponsiveTableWrapper.tsx";

// (Each row's name by the first line of it: on a phone, a row wraps.)
const LABEL = { width: 0, paddingTop: "0.75rem" };

const LookingFor = ({
	disabled,
	state,
	setState,
}: {
	disabled: boolean;
	state: LookingForState;
	setState: SetLookingForState;
}) => {
	return (
		<div>
			<h3 className="mb-0">What are you looking for?</h3>
			<ResponsiveTableWrapper>
				<table>
					<tbody>
						{helpers.entries(categories).map(([categoryKey, category]) => {
							return (
								<tr className="pt-2" key={categoryKey}>
									<td
										style={LABEL}
										className="ps-0 pe-0 pb-0 text-end align-top text-nowrap"
									>
										{category.name}
									</td>
									<td className="p-0 ps-2 pt-2 d-flex flex-wrap gap-2 column-gap-3">
										{category.options.map((option) => {
											const toggleButton = (
												<label
													key={option.key}
													className={clsx(
														"rounded-pill py-1 px-2",
														state[categoryKey][option.key]
															? "bg-secondary"
															: "bg-body-secondary",
													)}
												>
													<input
														type="checkbox"
														className="form-check-input me-1"
														disabled={disabled}
														checked={state[categoryKey][option.key]}
														onChange={() => {
															setState((state) => {
																const checked = !state[categoryKey][option.key];

																const newState = {
																	...state,
																	[categoryKey]: {
																		...state[categoryKey],
																		[option.key]: checked,
																	},
																};

																if (checked && categoryKey === "assets") {
																	if (option.key === "prospects") {
																		newState.assets.bestCurrentPlayers = false;
																	} else if (
																		option.key === "bestCurrentPlayers"
																	) {
																		newState.assets.prospects = false;
																	}
																}

																return newState;
															});
														}}
													/>
													{option.name}
												</label>
											);

											if (option.tooltip === undefined) {
												return toggleButton;
											}

											return (
												<OverlayTrigger
													key={option.key}
													overlay={<Tooltip>{option.tooltip}</Tooltip>}
												>
													{toggleButton}
												</OverlayTrigger>
											);
										})}
									</td>
								</tr>
							);
						})}
						<tr className="pt-2">
							<td
								style={LABEL}
								className="ps-0 pe-0 pb-0 text-end align-top text-nowrap"
							>
								Player
							</td>
							<td className="p-0 ps-2 pt-2 d-flex flex-wrap gap-2 column-gap-3">
								{(
									[
										...(__SPORT === "basketball"
											? ([["per", "PER at least", "15", ""]] as const)
											: []),
										["through", "Contract through", "2025", ""],
										["salaryMost", "Salary at most $", "20", "M"],
									] as const
								).map(([key, label, placeholder, unit]) => (
									<label
										key={key}
										className="d-flex align-items-center gap-1 mb-0"
									>
										{label}
										<input
											type="number"
											inputMode="decimal"
											className="form-control form-control-sm"
											style={{ width: 80 }}
											disabled={disabled}
											placeholder={placeholder}
											value={state.player[key]}
											onChange={(event) => {
												const value = event.target.value;
												setState((state) => ({
													...state,
													player: { ...state.player, [key]: value },
												}));
											}}
										/>
										{unit}
									</label>
								))}
							</td>
						</tr>
					</tbody>
				</table>
			</ResponsiveTableWrapper>
		</div>
	);
};

export default LookingFor;
