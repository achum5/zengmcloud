import { createWithEqualityFn } from "zustand/traditional";
import { shallow } from "zustand/shallow";
import type { LocalStateUI, GameAttributesLeague } from "../../common/types.ts";
import { defaultGameAttributes } from "../../common/defaultGameAttributes.ts";
import { safeLocalStorage } from "./safeLocalStorage.ts";
import { gameAttributesSyncedToUi } from "../../common/gameAttributesSyncedToUi.ts";

// These are variables that are needed to display parts of the UI not driven explicitly by worker/views/*.js files. Like
// the top navbar, the multi team menu, etc. They come from gameAttributes, the account system, and elsewhere.

type LocalActions = {
	deleteGames: (gids: number[]) => void;
	mergeGames: (games: LocalStateUI["games"]) => void;
	resetLeague: () => void;
	setShowLeagueTopBar: (showLeagueTopBar: boolean) => void;
	setShowDepthChart: (showDepthChart: boolean) => void;
	setSidebarOpen: (sidebarOpen: boolean) => void;
	update: (obj: Partial<LocalStateUI>) => void;
	updateGameAttributes: (
		gameAttributes: Partial<GameAttributesLeague>,
		flagOverrides?: LocalStateUI["flagOverrides"],
	) => void;
};

const defaultUnits: "metric" | "us" =
	window.navigator.language === "en-US" ? "us" : "metric";

const SHOW_DEPTH_CHART_KEY = "bbgmShowDepthChart";

type LocalStateWithActions = LocalStateUI & {
	actions: LocalActions;
};

let initialSidebarOpen: boolean;
if (window.innerWidth >= 1200) {
	// Large screen - default may be saved, if not default to open
	const saved = safeLocalStorage.getItem("sidebarOpen");
	initialSidebarOpen = saved !== "false";
} else {
	initialSidebarOpen = false;
}

let initialShowLeagueTopBar: boolean;
const showTemp = safeLocalStorage.getItem("bbgmShowLeagueTopBar");
if (showTemp === "true") {
	initialShowLeagueTopBar = true;
} else if (showTemp === "false") {
	initialShowLeagueTopBar = false;
} else {
	initialShowLeagueTopBar = true;
}

// On unless this device has said otherwise.
const initialShowDepthChart =
	safeLocalStorage.getItem(SHOW_DEPTH_CHART_KEY) !== "false";

const useLocalRaw = createWithEqualityFn<LocalStateWithActions>(
	(set, get) => ({
		alwaysShowCountry: false,
		budget: defaultGameAttributes.budget,
		challengeNoRatings: false,
		hideRatingsOnesDigit: false,
		hideRatingsOnesDigitExceptProspects: false,
		hideTeamRatings: false,
		teamRatingsDelaySeasons: 0,
		challengeNoTrades: false,
		tradesAfterDeadline: false,
		currencyFormat: defaultGameAttributes.currencyFormat,
		customMenu: undefined,
		draftType: defaultGameAttributes.draftType,
		email: undefined,
		fantasyPoints: undefined,
		flagOverrides: {},
		fullNames: false,
		tickerItems: [],
		leagueTickerVisible: false,
		gameOver: false,
		gameSimInProgress: false,
		games: [],
		gender: defaultGameAttributes.gender,
		gold: undefined,
		godMode: false,
		college: false,
		collegePolls: undefined,
		socialFeed: false,
		hideDisabledTeams: false,
		homeCourtAdvantage: 1,
		keyboardShortcuts: undefined,
		lid: undefined,
		liveGameInProgress: false,
		mpSyncActive: false,
		mpSyncIsHost: false,
		mpSyncHostName: undefined,
		mpSyncReady: false,
		mpSyncReconnecting: false,
		mpAutoPlay: undefined,
		mpSyncUpload: undefined,
		mpSyncUploadOk: 0,
		mpPendingUploads: 0,
		mpSyncHealthy: false,
		mpEditsPaused: false,
		mpCatchUp: undefined,
		mpLiveBroadcast: undefined,
		mpLiveWatchable: undefined,
		mpLiveOffer: undefined,
		mpLiveChat: [],
		pendingDeals: 0,
		mpPhaseReady: undefined,
		mpLotteryReveal: undefined,
		luxuryPayroll: defaultGameAttributes.luxuryPayroll,
		luxuryTax: defaultGameAttributes.luxuryTax,
		maxContract: defaultGameAttributes.maxContract,
		minContract: defaultGameAttributes.minContract,
		minPayroll: defaultGameAttributes.minPayroll,
		neutralSite: defaultGameAttributes.neutralSite,
		numGames: defaultGameAttributes.numGames[0].value,
		numPeriods: defaultGameAttributes.numPeriods,
		numWatchColors: defaultGameAttributes.numWatchColors,
		phase: 0,
		phaseText: "",
		playMenuOptions: [],
		popup: window.location.search === "?w=popup",
		recapAIProvider: "claude",
		quarterLength: defaultGameAttributes.quarterLength,
		realisticFrontOffices: false,
		salaryCap: defaultGameAttributes.salaryCap,
		salaryCapType: defaultGameAttributes.salaryCapType,
		hardCapAmount: defaultGameAttributes.hardCapAmount,
		hardCapTids: defaultGameAttributes.hardCapTids,
		hardCapUseLuxuryTax: defaultGameAttributes.hardCapUseLuxuryTax,
		season: 0,
		showLeagueTopBar: initialShowLeagueTopBar,
		showDepthChart: initialShowDepthChart,
		showNagModal: false,
		sidebarOpen: initialSidebarOpen,
		spectator: false,
		startingSeason: 0,
		statusText: "Idle",
		teamInfoCache: [],
		units: defaultUnits,
		userTid: 0,
		userTids: [],
		username: undefined,
		workerBusy: false,
		title: undefined,
		hideNewWindow: false,
		jumpTo: false,
		jumpToSeason: undefined,
		dropdownCustomOptions: undefined,
		dropdownCustomURL: undefined,
		dropdownView: undefined,
		dropdownFields: {},
		moreInfoAbbrev: undefined,
		moreInfoSeason: undefined,
		moreInfoTid: undefined,
		stickyFooterAd: false,
		stickyFormButtons: false,

		actions: {
			deleteGames(gids: number[]) {
				set((state) => {
					const newGames = state.games.filter(
						(game) => !gids.includes(game.gid),
					);

					return {
						games: newGames,
					};
				});
			},

			mergeGames(games: LocalStateUI["games"]) {
				set((state) => {
					const newGames = state.games.slice();

					for (const game of games) {
						const index = newGames.findIndex(
							(newGame) => newGame.gid === game.gid,
						);
						if (index >= 0) {
							newGames[index] = game;
						} else {
							newGames.push(game);
						}
					}

					return {
						games: newGames,
					};
				});
			},

			// Reset any values specific to a league. statusText and phaseText will be set later, no need to override here and cause UI flicker
			resetLeague() {
				set({
					alwaysShowCountry: false,
					challengeNoRatings: false,
					hideRatingsOnesDigit: false,
					hideRatingsOnesDigitExceptProspects: false,
					hideTeamRatings: false,
					teamRatingsDelaySeasons: 0,
					tradesAfterDeadline: false,
					games: [],
					tickerItems: [],
					leagueTickerVisible: false,
					godMode: false,
					college: false,
					collegePolls: undefined,
					socialFeed: false,
					hideDisabledTeams: false,
					homeCourtAdvantage: 1,

					// Controller.tsx relies on this being undefined (or at least different than the new lid) to trigger calling beforeView.league
					lid: undefined,
					liveGameInProgress: false,
					mpSyncActive: false,
					mpSyncIsHost: false,
					mpSyncHostName: undefined,
					mpSyncReady: false,
					mpSyncReconnecting: false,
					mpAutoPlay: undefined,
					mpSyncUpload: undefined,
					mpSyncUploadOk: 0,
					mpPendingUploads: 0,
					mpSyncHealthy: false,
					mpEditsPaused: false,
					mpCatchUp: undefined,
					mpLiveBroadcast: undefined,
					mpLiveWatchable: undefined,
					mpLiveOffer: undefined,
					mpLiveChat: [],
					pendingDeals: 0,
					mpPhaseReady: undefined,
					mpLotteryReveal: undefined,
					numPeriods: defaultGameAttributes.numPeriods,
					numWatchColors: defaultGameAttributes.numWatchColors,
					phase: 0,
					playMenuOptions: [],
					quarterLength: defaultGameAttributes.quarterLength,
					realisticFrontOffices: false,
					season: 0,
					startingSeason: 0,
					teamInfoCache: [],
					userTid: 0,
					userTids: [],
				});
			},

			setShowLeagueTopBar(showLeagueTopBar: boolean) {
				set({ showLeagueTopBar });
				safeLocalStorage.setItem(
					"bbgmShowLeagueTopBar",
					String(showLeagueTopBar),
				);
			},

			setShowDepthChart(showDepthChart: boolean) {
				set({ showDepthChart });
				safeLocalStorage.setItem(SHOW_DEPTH_CHART_KEY, String(showDepthChart));
			},

			setSidebarOpen(sidebarOpen: boolean) {
				if (window.innerWidth >= 1200) {
					if (sidebarOpen) {
						// Don't save true, cause default is true
						safeLocalStorage.removeItem("sidebarOpen");
					} else {
						safeLocalStorage.setItem("sidebarOpen", "false");
					}
				}

				set({ sidebarOpen });
			},

			update(obj: Partial<LocalStateUI>) {
				if (Object.hasOwn(obj, "units") && obj.units === undefined) {
					obj.units = defaultUnits;
				}

				const email = obj.email;
				// Freestar hashed-email passthrough is ad targeting; with ads disabled
				// window.freestar never loads, so guard against it to avoid throwing.
				if (email && !obj.gold && window.freestar) {
					window.freestar.queue.push(async () => {
						// https://freestarhelp.zendesk.com/hc/en-us/articles/34498258990868-Hashed-Email-Passthrough
						const freestarNormalizeEmail = (email: string) => {
							const EMAIL_REGEX = /^[^\s@]+@[^\s@]+$/;
							if (!EMAIL_REGEX.test(email)) {
								return;
							}

							email = email.trim().toLowerCase();

							const parts = email.split("@");
							let username = parts[0];
							const domain = parts[1];

							if (username !== undefined && domain === "gmail.com") {
								// Remove + and everything after
								const plusIndex = username.indexOf("+");
								if (plusIndex !== -1) {
									username = username.slice(0, plusIndex);
								}

								// Remove periods
								username = username.replace(/\./g, "");

								return `${username}@${domain}`;
							}

							return email;
						};

						const normalizedEmail = freestarNormalizeEmail(email);
						if (normalizedEmail !== undefined) {
							async function hash(
								algorithm: AlgorithmIdentifier,
								data: BufferSource,
							) {
								const buffer = await crypto.subtle.digest(algorithm, data);
								const bytes = new Uint8Array(buffer);

								// Chrome 140, Firefox 133, Safari 18.2 - switch to `bytes.toHex()`
								return [...bytes]
									.map((b) => b.toString(16).padStart(2, "0"))
									.join("");
							}

							const encoder = new TextEncoder();
							const data = encoder.encode(normalizedEmail);
							const sha1 = await hash("SHA-1", data);
							const sha256 = await hash("SHA-256", data);
							window.freestar?.identity?.setIdentity?.({
								hashes: {
									sha1,
									sha256,
								},
							});
						}
					});
				}

				set(obj);
			},

			updateGameAttributes(
				gameAttributes: Partial<GameAttributesLeague>,
				flagOverrides?: LocalStateUI["flagOverrides"],
			) {
				let update = false;

				const updates: Partial<LocalStateUI> = {};

				const state = get();
				for (const key of gameAttributesSyncedToUi) {
					if (
						Object.hasOwn(gameAttributes, key) &&
						state[key] !== gameAttributes[key]
					) {
						// @ts-expect-error
						updates[key] = gameAttributes[key];
						update = true;
					}
				}

				if (flagOverrides) {
					updates.flagOverrides = flagOverrides;
					update = true;
				}

				if (update) {
					set((state) => ({ ...state, ...updates }));
				}
			},
		},
	}),
	shallow,
);

const useLocal = <Key extends keyof LocalStateUI>(keys: Key[]) => {
	const selector = (state: LocalStateUI) => {
		const obj = {} as Pick<LocalStateUI, Key>;
		for (const key of keys) {
			obj[key] = state[key];
		}
		return obj;
	};

	return useLocalRaw(selector);
};

// This assumes the actions object never changes!
const useLocalActions = () => useLocalRaw((state) => state.actions);

const local = useLocalRaw;
const localActions = local.getState().actions;

export { local, localActions, useLocalActions, useLocal };
