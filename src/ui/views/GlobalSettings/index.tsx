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
	getLiveGameView,
	setLiveGameView,
	type LiveGameView,
} from "../LiveGame/court3d/setting.ts";
import {
	getKeyPlays,
	KEY_PLAY_LIMITS,
	KEY_PLAY_PRESETS,
	parseKeyPlays,
	setKeyPlays,
	type KeyPlaySettings,
} from "../LiveGame/court3d/keyPlays.ts";

// What can make the 3D key plays reel (see keyPlays.ts).
const KEY_PLAY_KINDS: [keyof KeyPlaySettings, string][] = [
	["swings", "Biggest swings"],
	["dunks", "Dunks"],
	["threes", "Threes"],
	["blocks", "Blocks"],
	["steals", "Steals"],
	["andOnes", "And-ones"],
	["leadChanges", "Lead changes"],
	["buzzer", "Buzzer-beaters"],
	["baskets", "Other baskets"],
];

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
			leagueTicker: local.getState().leagueTickerEnabled
				? "show"
				: ("hide" as const),
			scoresBar: local.getState().showLeagueTopBar ? "show" : ("hide" as const),
			liveGameView: getLiveGameView() as LiveGameView,
			keyPlays: getKeyPlays(),
			phaseChangeRedirects: props.phaseChangeRedirects,
			realPlayerPhotos: props.realPlayerPhotos,
			realTeamInfo: props.realTeamInfo,
			recapAIProvider: props.recapAIProvider,
			recapMaxGames: String(props.recapMaxGames),
			recapMaxDays: String(props.recapMaxDays),
			ownGameSimCutoffSeconds: String(props.ownGameSimCutoffSeconds),
			achievementCardsDraftPicks: String(props.achievementCardsDraftPicks),
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

	// A length of game for the key plays not among the presets.
	const [customLength, setCustomLength] = useState(
		() =>
			!(KEY_PLAY_PRESETS as readonly number[]).includes(state.keyPlays.minutes),
	);

	const setKeyPlay = (patch: Partial<KeyPlaySettings>) => {
		setState((state2) => ({
			...state2,
			keyPlays: { ...state2.keyPlays, ...patch },
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
		// rather than sent to the worker with the options below.
		localActions.setLeagueTickerEnabled(state.leagueTicker === "show");
		localActions.setShowLeagueTopBar(state.scoresBar === "show");
		setLiveGameView(state.liveGameView);
		setKeyPlays(parseKeyPlays(state.keyPlays));

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
						<label className="form-label" htmlFor="options-leagueTicker">
							League Ticker{" "}
							<HelpPopover title="League Ticker">
								<p>
									The scrolling score bar along the bottom of the screen, on
									this device only.
								</p>
							</HelpPopover>
						</label>
						<select
							id="options-leagueTicker"
							className="form-select"
							onChange={handleChange("leagueTicker")}
							value={state.leagueTicker}
						>
							<option value="show">Show</option>
							<option value="hide">Hide</option>
						</select>
					</div>
					<div className="col-sm-3 col-6 mb-3">
						<label className="form-label" htmlFor="options-scoresBar">
							Scores Bar{" "}
							<HelpPopover title="Scores Bar">
								<p>
									The row of recent scores across the top of the screen, on this
									device only.
								</p>
							</HelpPopover>
						</label>
						<select
							id="options-scoresBar"
							className="form-select"
							onChange={handleChange("scoresBar")}
							value={state.scoresBar}
						>
							<option value="show">Show</option>
							<option value="hide">Hide</option>
						</select>
					</div>
					{__SPORT === "basketball" ? (
						<div className="col-sm-3 col-6 mb-3">
							<label
								className="form-label"
								htmlFor="options-liveGameView"
								title="On this device only"
							>
								Live Game View
							</label>
							<select
								id="options-liveGameView"
								className="form-select"
								onChange={handleChange("liveGameView")}
								value={state.liveGameView}
							>
								<option value="classic">Classic</option>
								<option value="3d">3D</option>
							</select>
						</div>
					) : null}
					{__SPORT === "basketball" && state.liveGameView === "3d" ? (
						<div className="col-12 mb-3">
							<div className="row">
								<div className="col-sm-3 col-6 mb-2">
									<label
										className="form-label"
										htmlFor="options-keyPlays"
										title="Only the big plays, and every play of a close finish. On this device only."
									>
										3D Key Plays
									</label>
									<select
										id="options-keyPlays"
										className="form-select"
										onChange={(event) => {
											setKeyPlay({ on: event.target.value === "on" });
										}}
										value={state.keyPlays.on ? "on" : "off"}
									>
										<option value="off">Off</option>
										<option value="on">On</option>
									</select>
								</div>
								<div className="col-sm-3 col-6 mb-2">
									<label
										className="form-label"
										htmlFor="options-keyPlaysMinutes"
										title="About how long a game takes to watch, at the speed it plays at"
									>
										Game Length (min)
									</label>
									<select
										id="options-keyPlaysMinutes"
										className="form-select"
										value={
											customLength ? "custom" : String(state.keyPlays.minutes)
										}
										onChange={(event) => {
											const v = event.target.value;
											setCustomLength(v === "custom");
											if (v !== "custom") {
												setKeyPlay({ minutes: Number(v) });
											}
										}}
									>
										{KEY_PLAY_PRESETS.map((m) => (
											<option key={m} value={String(m)}>
												{m} min
											</option>
										))}
										<option value="custom">Custom</option>
									</select>
									{customLength ? (
										<input
											type="number"
											className="form-control mt-1"
											aria-label="Game length in minutes"
											min={KEY_PLAY_LIMITS.minutes[0]}
											max={KEY_PLAY_LIMITS.minutes[1]}
											step={0.5}
											value={state.keyPlays.minutes}
											onChange={(event) => {
												setKeyPlay({ minutes: Number(event.target.value) });
											}}
										/>
									) : null}
								</div>
								<div className="col-sm-3 col-6 mb-2">
									<label
										className="form-label"
										htmlFor="options-keyPlaysClosePoints"
										title="Every play once it is this close late in the fourth or in overtime (0: never)"
									>
										Close Finish (pts)
									</label>
									<input
										id="options-keyPlaysClosePoints"
										type="number"
										className="form-control"
										min={KEY_PLAY_LIMITS.closePoints[0]}
										max={KEY_PLAY_LIMITS.closePoints[1]}
										step={1}
										value={state.keyPlays.closePoints}
										onChange={(event) => {
											setKeyPlay({ closePoints: Number(event.target.value) });
										}}
									/>
								</div>
								<div className="col-sm-3 col-6 mb-2">
									<label
										className="form-label"
										htmlFor="options-keyPlaysCloseMinutes"
										title="How many minutes left in the fourth or in overtime a close finish starts"
									>
										Close Finish (min left)
									</label>
									<input
										id="options-keyPlaysCloseMinutes"
										type="number"
										className="form-control"
										min={KEY_PLAY_LIMITS.closeMinutes[0]}
										max={KEY_PLAY_LIMITS.closeMinutes[1]}
										step={0.5}
										value={state.keyPlays.closeMinutes}
										onChange={(event) => {
											setKeyPlay({ closeMinutes: Number(event.target.value) });
										}}
									/>
								</div>
								<div className="col-sm-3 col-6 mb-2">
									<label
										className="form-label"
										htmlFor="options-keyPlaysCloseOnTop"
										title="Extra: a close finish plays on top of the game length instead of using it up, so the pace never gives it away"
									>
										Close Finish Time
									</label>
									<select
										id="options-keyPlaysCloseOnTop"
										className="form-select"
										onChange={(event) => {
											setKeyPlay({
												closeOnTop: event.target.value === "extra",
											});
										}}
										value={state.keyPlays.closeOnTop ? "extra" : "included"}
									>
										<option value="included">In game length</option>
										<option value="extra">Extra</option>
									</select>
								</div>
								<div className="col-12">
									{[
										...KEY_PLAY_KINDS,
										["mine", "Your team first"] as [
											keyof KeyPlaySettings,
											string,
										],
									].map(([k, label]) => (
										<div key={k} className="form-check form-check-inline">
											<input
												className="form-check-input"
												type="checkbox"
												id={`options-keyPlays-${k}`}
												checked={!!state.keyPlays[k]}
												onChange={(event) => {
													setKeyPlay({ [k]: event.target.checked });
												}}
											/>
											<label
												className="form-check-label"
												htmlFor={`options-keyPlays-${k}`}
											>
												{label}
											</label>
										</div>
									))}
								</div>
							</div>
						</div>
					) : null}
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

				{__SPORT === "basketball" ? (
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
