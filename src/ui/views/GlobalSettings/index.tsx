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
import { local, localActions } from "../../util/local.ts";
import {
	getKeyPlays,
	KEY_PLAY_LIMITS,
	parseKeyPlays,
	setKeyPlays,
} from "../LiveGame/court3d/keyPlays.ts";

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
		const keyPlays = getKeyPlays();

		return {
			fullNames,
			scoresBar: local.getState().showLeagueTopBar ? "show" : ("hide" as const),
			depthChart: local.getState().showDepthChart ? "show" : ("hide" as const),
			closePoints: String(keyPlays.closePoints),
			closeMinutes: String(keyPlays.closeMinutes),
			phaseChangeRedirects: props.phaseChangeRedirects,
			realPlayerPhotos: props.realPlayerPhotos,
			realTeamInfo: props.realTeamInfo,
			recapAIProvider: props.recapAIProvider,
			recapMaxGames: String(props.recapMaxGames),
			recapMaxDays: String(props.recapMaxDays),
			ownGameSimCutoffSeconds: String(props.ownGameSimCutoffSeconds),
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

	const handleLoad = (
		name: "realTeamInfo" | "realPlayerPhotos",
		json: string,
	) => {
		setState((state2) => ({
			...state2,
			[name]: json,
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

		// Device-local, like the color scheme above: saved straight to this browser
		// rather than sent to the worker with the options below. (Key plays on or
		// off, and their length, are picked by the 3D court.)
		localActions.setShowLeagueTopBar(state.scoresBar === "show");
		localActions.setShowDepthChart(state.depthChart === "show");
		setKeyPlays(
			parseKeyPlays({
				...getKeyPlays(),
				closePoints: Number(state.closePoints),
				closeMinutes: Number(state.closeMinutes),
			}),
		);

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
				// (No longer set here: kept as it is.)
				achievementCardsDraftPicks: props.achievementCardsDraftPicks,
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

	const showHide = (name: string, label: string, title?: string) => (
		<div className="col-sm-3 col-6 mb-3">
			<label className="form-label" htmlFor={`options-${name}`} title={title}>
				{label}
			</label>
			<select
				id={`options-${name}`}
				className="form-select"
				onChange={handleChange(name)}
				value={state[name as "scoresBar" | "depthChart"]}
			>
				<option value="show">Show</option>
				<option value="hide">Hide</option>
			</select>
		</div>
	);

	const numberField = (
		name:
			| "closePoints"
			| "closeMinutes"
			| "recapMaxGames"
			| "recapMaxDays"
			| "recapMaxPlayers"
			| "ownGameSimCutoffSeconds",
		label: string,
		{
			min,
			max,
			step = 1,
			title,
		}: { min: number; max?: number; step?: number; title?: string },
	) => (
		<div className="col-sm-3 col-6 mb-3">
			<label className="form-label" htmlFor={`options-${name}`} title={title}>
				{label}
			</label>
			<input
				id={`options-${name}`}
				type="number"
				min={min}
				max={max}
				step={step}
				className="form-control"
				onChange={handleChange(name)}
				value={state[name]}
			/>
		</div>
	);

	return (
		<>
			<MoreLinks type="globalSettings" page="/settings" />

			<form onSubmit={handleFormSubmit}>
				<h2>Display</h2>
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
							Player Names
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
					{showHide(
						"scoresBar",
						"Scores Bar",
						"The recent scores across the top, on this device",
					)}
					{__SPORT === "basketball"
						? showHide("depthChart", "Roster Depth Chart", "On this device")
						: null}
				</div>

				<h2>Games</h2>
				<div className="row">
					{__SPORT === "basketball" || __SPORT === "football" ? (
						<>
							{numberField("closePoints", "Close Game (pts)", {
								min: KEY_PLAY_LIMITS.closePoints[0],
								max: KEY_PLAY_LIMITS.closePoints[1],
								title:
									"Key plays show every play once it is within this many points (0: never)",
							})}
							{numberField("closeMinutes", "Close Game (min left)", {
								min: KEY_PLAY_LIMITS.closeMinutes[0],
								max: KEY_PLAY_LIMITS.closeMinutes[1],
								step: 0.5,
								title: "Minutes left in the fourth or in overtime",
							})}
						</>
					) : null}
					{numberField("ownGameSimCutoffSeconds", "Own Game Sim Cutoff (s)", {
						min: 0,
						title:
							"Multiplayer: seconds before a scheduled sim when you can't sim or watch your own game (0: off)",
					})}
				</div>

				<h2>AI Recaps</h2>
				<div className="row">
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label" htmlFor="options-recapAIProvider">
							AI
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
					{numberField("recapMaxGames", "Max Games", { min: 1 })}
					{numberField("recapMaxDays", "Max Days", { min: 1 })}
					{numberField("recapMaxPlayers", "Max Players", {
						min: 1,
						title:
							"Players per prompt for player recaps - lower it if replies come back cut off",
					})}
				</div>

				<h2>Other</h2>
				<div className="row">
					<div
						className="col-sm-3 col-6 mb-3"
						role="group"
						aria-labelledby="options-phaseChangeRedirects"
					>
						<div className="form-label" id="options-phaseChangeRedirects">
							Auto UI Redirect{" "}
							<HelpPopover title="Auto UI Redirect">
								<p>
									At different points in the game, the UI automatically
									redirects to a page. For example, when the regular season
									ends, it automatically redirects to the playoff bracket. If
									you find that behavior annoying, you can disable it here.
								</p>
							</HelpPopover>
						</div>
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
						<div className="form-label">Persistent Storage</div>
						<Storage />
					</div>
				</div>

				{__SPORT === "basketball" ? (
					<details className="mb-3">
						<summary className="h2">
							Team and Player Data for "Real Players" Leagues
						</summary>
						<RealData
							handleChange={handleChange}
							handleLoad={handleLoad}
							realPlayerPhotos={state.realPlayerPhotos}
							realTeamInfo={state.realTeamInfo}
						/>
					</details>
				) : null}

				<button className="btn btn-primary mt-3">Save global settings</button>
			</form>
		</>
	);
};

export default GlobalSettings;
