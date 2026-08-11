import { useState, type ChangeEvent, type SubmitEvent } from "react";
import useTitleBar from "../../hooks/useTitleBar.tsx";
import { helpers } from "../../util/helpers.ts";
import { showNotification } from "../../util/showNotification.ts";
import { toWorker } from "../../util/toWorker.ts";
import RealData from "./RealData.tsx";
import Storage from "./Storage.tsx";
import type { View } from "../../../common/types.ts";
import {
	DEFAULT_PHASE_CHANGE_REDIRECTS,
	PHASE,
	PHASE_TEXT,
} from "../../../common/constants.ts";
import { MoreLinks } from "../../components/MoreLinks.tsx";
import { useBlocker } from "../../hooks/useBlocker.ts";
import { HelpPopover } from "../../components/HelpPopover.tsx";
import { safeLocalStorage } from "../../util/safeLocalStorage.ts";
import { isSport } from "../../../common/sportFunctions.ts";

const GlobalSettings = (props: View<"globalSettings">) => {
	const [state, setState] = useState(() => {
		const themeLocalStorage = safeLocalStorage.getItem("theme");
		let theme: "dark" | "light" | "default";
		if (themeLocalStorage === "dark") {
			theme = "dark";
		} else if (themeLocalStorage === "light") {
			theme = "light";
		} else {
			theme = "default";
		}

		let units: "metric" | "us" | "default";
		if (props.units === "metric") {
			units = "metric";
		} else if (props.units === "us") {
			units = "us";
		} else {
			units = "default";
		}

		const fullNames = props.fullNames ? "always" : ("abbrev-small" as const);

		return {
			fullNames,
			phaseChangeRedirects: props.phaseChangeRedirects,
			realPlayerPhotos: props.realPlayerPhotos,
			realTeamInfo: props.realTeamInfo,
			recapAIProvider: props.recapAIProvider,
			recapMaxGames: String(props.recapMaxGames),
			recapMaxDays: String(props.recapMaxDays),
			ownGameSimCutoffSeconds: String(props.ownGameSimCutoffSeconds),
			achievementCardsDraftPicks: String(props.achievementCardsDraftPicks),
			cardPromptSafeMode: props.cardPromptSafeMode ? "on" : "off",
			recapMaxPlayers: String(props.recapMaxPlayers),
			theme,
			units,
		};
	});

	const { setDirty } = useBlocker();

	const handleChange =
		(name: string) =>
		(
			event: ChangeEvent<
				HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement
			>,
		) => {
			const value = event.target.value;
			setState((state2) => ({
				...state2,
				[name]: value,
			}));
			setDirty(true);
		};

	const handleFormSubmit = async (event: SubmitEvent) => {
		event.preventDefault();

		if (state.theme === "default") {
			safeLocalStorage.removeItem("theme");
		} else {
			safeLocalStorage.setItem("theme", state.theme);
		}
		if (window.themeCSSLink) {
			window.themeCSSLink.href = window.getThemeFilename(window.getTheme());
		}

		const units = state.units === "default" ? undefined : state.units;
		try {
			await toWorker("main", "updateOptions", {
				fullNames: state.fullNames === "always",
				phaseChangeRedirects: state.phaseChangeRedirects,
				realPlayerPhotos: state.realPlayerPhotos,
				realTeamInfo: state.realTeamInfo,
				recapAIProvider: state.recapAIProvider,
				recapMaxGames: Number(state.recapMaxGames),
				recapMaxPlayers: Number(state.recapMaxPlayers),
				recapMaxDays: Number(state.recapMaxDays),
				ownGameSimCutoffSeconds: Number(state.ownGameSimCutoffSeconds),
				achievementCardsDraftPicks: Number(state.achievementCardsDraftPicks),
				cardPromptSafeMode: state.cardPromptSafeMode === "on",
				units,
			});
			showNotification({
				type: "success",
				text: "Settings successfully updated.",
			});
			setDirty(false);
		} catch (error) {
			showNotification({
				type: "error",
				text: error.message,
				persistent: true,
			});
		}
	};

	useTitleBar({ title: "Global Settings" });

	const phaseChangeRedirects = DEFAULT_PHASE_CHANGE_REDIRECTS.map((phase) => {
		let label;
		if (phase === PHASE.REGULAR_SEASON) {
			label = "Season preview, before regular season";
		} else if (phase === PHASE.DRAFT_LOTTERY) {
			label = "Season summary, after playoffs";
		} else {
			label = helpers.upperCaseFirstLetter(PHASE_TEXT[phase]);
		}

		return {
			phase,
			label,
			checked: state.phaseChangeRedirects.includes(phase),
		};
	});

	return (
		<>
			<MoreLinks type="globalSettings" page="/settings" />

			<form onSubmit={handleFormSubmit}>
				<div className="row">
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label" htmlFor="options-color-scheme">
							Color Scheme
						</label>
						<select
							id="options-color-scheme"
							className="form-select"
							onChange={handleChange("theme")}
							value={state.theme}
						>
							<option value="default">Auto</option>
							<option value="light">Light</option>
							<option value="dark">Dark</option>
						</select>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label" htmlFor="options-units">
							Units
						</label>
						<select
							id="options-units"
							className="form-select"
							onChange={handleChange("units")}
							value={state.units}
						>
							<option value="default">Auto</option>
							<option value="us">US</option>
							<option value="metric">Metric</option>
						</select>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label" htmlFor="options-fullNames">
							Player Name Display
						</label>
						<select
							id="options-fullNames"
							className="form-select"
							onChange={handleChange("fullNames")}
							value={state.fullNames}
						>
							<option value="abbrev-small">
								Abbreviate first names and skills on small screens
							</option>
							<option value="always">Always show full names and skills</option>
						</select>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label" htmlFor="options-recapAIProvider">
							AI Recap Button
						</label>
						<select
							id="options-recapAIProvider"
							className="form-select"
							onChange={handleChange("recapAIProvider")}
							value={state.recapAIProvider}
						>
							<option value="claude">Claude</option>
							<option value="chatgpt">ChatGPT</option>
						</select>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label" htmlFor="options-recapMaxGames">
							AI Recap Max Games
						</label>
						<input
							id="options-recapMaxGames"
							type="number"
							min={1}
							step={1}
							className="form-control"
							onChange={handleChange("recapMaxGames")}
							value={state.recapMaxGames}
						/>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label" htmlFor="options-recapMaxDays">
							AI Recap Max Days
						</label>
						<input
							id="options-recapMaxDays"
							type="number"
							min={1}
							step={1}
							className="form-control"
							onChange={handleChange("recapMaxDays")}
							value={state.recapMaxDays}
						/>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label
							className="form-label"
							htmlFor="options-ownGameSimCutoffSeconds"
						>
							Own Game Sim Cutoff{" "}
							<HelpPopover title="Own Game Sim Cutoff">
								<p>
									Seconds before a scheduled multiplayer sim during which you
									cannot sim or watch your own game, so your result has time to
									reach the device doing the simming. 0 turns the window off.
								</p>
							</HelpPopover>
						</label>
						<input
							id="options-ownGameSimCutoffSeconds"
							type="number"
							min={0}
							step={1}
							className="form-control"
							onChange={handleChange("ownGameSimCutoffSeconds")}
							value={state.ownGameSimCutoffSeconds}
						/>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label" htmlFor="options-cardPromptSafeMode">
							Card Prompt Safe Mode{" "}
							<HelpPopover title="Card Prompt Safe Mode">
								<p>
									Image models refuse to draw a real person or to reproduce a
									real team's logo. On, card prompts describe the player as a
									fictional cartoon character and have the uniform and card
									marks invented from the team's colors instead of copied.
								</p>
								<p>
									Off gives a more faithful card - the franchise's real uniform
									for that season - but is much more likely to be rejected.
								</p>
							</HelpPopover>
						</label>
						<select
							id="options-cardPromptSafeMode"
							className="form-select"
							onChange={handleChange("cardPromptSafeMode")}
							value={state.cardPromptSafeMode}
						>
							<option value="on">On</option>
							<option value="off">Off</option>
						</select>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label
							className="form-label"
							htmlFor="options-achievementCardsDraftPicks"
						>
							Draft Achievement Cards{" "}
							<HelpPopover title="Draft Achievement Cards">
								<p>
									How many top picks of each draft get an achievement card
									(Draft History page). 0 turns draft cards off.
								</p>
							</HelpPopover>
						</label>
						<input
							id="options-achievementCardsDraftPicks"
							type="number"
							min={0}
							step={1}
							className="form-control"
							onChange={handleChange("achievementCardsDraftPicks")}
							value={state.achievementCardsDraftPicks}
						/>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label" htmlFor="options-recapMaxPlayers">
							AI Recap Max Players{" "}
							<HelpPopover title="AI Recap Max Players">
								<p>
									How many players go into each prompt when writing season
									recaps for every player in the league (History &gt; Player
									Recaps).
								</p>
								<p>
									Each player brings their whole career - stats and ratings for
									every season, transactions, and feats - so a bigger number
									means fewer copy/paste rounds but a much longer prompt, and
									less room in the AI's reply for the last players in the batch.
									Lower this if recaps come back truncated or if the last few
									players get skipped.
								</p>
							</HelpPopover>
						</label>
						<input
							id="options-recapMaxPlayers"
							type="number"
							min={1}
							step={1}
							className="form-control"
							onChange={handleChange("recapMaxPlayers")}
							value={state.recapMaxPlayers}
						/>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label">
							Auto UI Redirect{" "}
							<HelpPopover title="Auto UI Redirect">
								<p>
									At different points in the game, the UI automatically
									redirects to a page. For example, when the regular season
									ends, it automatically redirects to the playoff bracket. If
									you find that behavior annoying, you can disable it here.
								</p>
							</HelpPopover>
						</label>
						{phaseChangeRedirects.map(({ checked, label, phase }) => (
							<div key={phase} className="form-check">
								<input
									className="form-check-input"
									type="checkbox"
									id={`options-phaseChangeRedirects-${phase}`}
									checked={checked}
									onChange={() => {
										let phaseChangeRedirects;
										if (checked) {
											phaseChangeRedirects = state.phaseChangeRedirects.filter(
												(phase2) => phase2 !== phase,
											);
										} else {
											phaseChangeRedirects = [
												...state.phaseChangeRedirects,
												phase,
											];
										}

										setState({
											...state,
											phaseChangeRedirects,
										});
									}}
								/>
								<label
									className="form-check-label"
									htmlFor={`options-phaseChangeRedirects-${phase}`}
								>
									{label}
								</label>
							</div>
						))}
						<div className="mt-1">
							<button
								className="btn btn-link p-0"
								type="button"
								onClick={() => {
									setState({
										...state,
										phaseChangeRedirects: DEFAULT_PHASE_CHANGE_REDIRECTS,
									});
								}}
							>
								All
							</button>{" "}
							|{" "}
							<button
								className="btn btn-link p-0"
								type="button"
								onClick={() => {
									setState({
										...state,
										phaseChangeRedirects: [],
									});
								}}
							>
								None
							</button>
						</div>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label">Persistent Storage</label>
						<Storage />
					</div>
				</div>

				{isSport("basketball") ? (
					<>
						<h2>Team and Player Data for "Real Players" Leagues</h2>
						<RealData
							handleChange={handleChange}
							realPlayerPhotos={state.realPlayerPhotos}
							realTeamInfo={state.realTeamInfo}
						/>
					</>
				) : null}

				<button className="btn btn-primary mt-3">Save global settings</button>
			</form>
		</>
	);
};

export default GlobalSettings;
