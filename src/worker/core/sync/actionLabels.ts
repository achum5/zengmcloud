// Classification of worker API calls for multiplayer, shared between the
// pre-action guard (worker/index.ts) and the sync engines. Pure data - no
// imports, so anything can depend on it without cycles.
//
// Multiplayer "sim authority": while synced, only the device that is in charge
// of simming may advance the shared timeline. These sets classify which API
// calls count as "advancing" so the guard can block them on non-authority
// devices, and so the publish path can tell a timeline advance from an
// ordinary edit (they get different staleness rules - see SyncEngineV2).
//
// Play-menu items that DON'T need sim authority: "stop"/"stopAuto" just halt.
// Drafting your OWN player (main.draftUser) is a separate call that isn't
// sim-authority-locked, so every user can still make their own pick - but the
// draft ADVANCERS (sim one pick / to your next pick / to end) move the shared
// draft past other teams' picks, so only the simmer may run them.
export const PLAY_MENU_SIM_AUTHORITY_EXEMPT = new Set(["stop", "stopAuto"]);

// "actions"-type calls that advance the season/live sim, or advance the shared
// draft past other teams' picks (untilPick = "Sim to this pick", same class as
// playMenu.onePick/untilYourNextPick).
export const ACTIONS_SIM_AUTHORITY_LOCKED = new Set([
	"simGame",
	"liveGame",
	"simToGame",
	"untilPick",
]);

// "toolsMenu"-type calls that advance the shared timeline (auto play, skip-to
// phase jumps). Everything else in Tools (resetDb, dangerZone toggles) is
// local-only and stays open.
export const TOOLS_MENU_SIM_AUTHORITY_LOCKED = new Set([
	"autoPlaySeasons",
	"skipToPlayoffs",
	"skipToBeforeDraft",
	"skipToAfterDraft",
	"skipToPreseason",
]);

// The All-Star weekend is a single shared event (one dunk contest, one 3pt
// contest, one All-Star draft) that the whole league watches - not something
// each device runs its own copy of. So only the sim authority may advance or
// set it up; otherwise a follower just opening the page (which auto-advances
// the contest on a timer) would race the simmer and fork the shared state.
// Kept as its own set so these can be sim-authority-locked WITHOUT driving the
// sim-busy lease (they fire every ~1s, which would flicker the "simming"
// indicator and spam the control doc).
export const ALLSTAR_SIM_AUTHORITY_LOCKED = new Set([
	"dunkSimNext",
	"threeSimNext",
	"dunkUser",
	"dunkSetControlling",
	"contestSetPlayers",
	"allStarDraftAll",
	"allStarDraftOne",
	"allStarDraftUser",
	"allStarDraftReset",
	"allStarDraftSetPlayers",
]);

// "main"-type calls that restructure/advance the league. A single on-the-clock
// pick (draftUser) is deliberately NOT here - every user drafts their own team.
// Per-team expansion-draft protection (updateProtectedPlayers/autoProtect) is
// also open: each user protects their own roster. Everything below is a
// commissioner-class operation: it advances shared time, restructures the
// league, predetermines results, or bulk-rewrites records - so only the device
// in charge of simming may run it, or two devices editing at once would race
// and fork.
export const MAIN_SIM_AUTHORITY_LOCKED = new Set([
	"draftLottery",
	"startExpansionDraft",
	"startFantasyDraft",
	"advanceToPlayerProtection",
	"cancelExpansionDraft",
	"updateExpansionDraftSetup",
	"updateGameAttributes",
	"updateGameAttributesGodMode",
	"setScheduleFromEditor",
	"toggleTradeDeadline",
	"allStarGameNow",
	"updatePlayoffTeams",
	"setForceWin",
	"setForceWinAll",
	"addTeam",
	"updateConfsDivs",
	"regenerateDraftClass",
	"importPlayers",
	"removePlayers",
	"clearInjuries",
	"updateAwards",
	...ALLSTAR_SIM_AUTHORITY_LOCKED,
]);

// Does this API call advance the shared timeline (and so require sim authority)?
export const isSimAuthorityLockedCall = (type: string, name: string): boolean =>
	(type === "playMenu" && !PLAY_MENU_SIM_AUTHORITY_EXEMPT.has(name)) ||
	(type === "actions" && ACTIONS_SIM_AUTHORITY_LOCKED.has(name)) ||
	(type === "toolsMenu" && TOOLS_MENU_SIM_AUTHORITY_LOCKED.has(name)) ||
	(type === "main" && MAIN_SIM_AUTHORITY_LOCKED.has(name));

// A SINGLE game within a day, as opposed to a whole day. The two "actions"
// labels are the dispatched calls; "playMenu.simGame" is what play.ts
// publishes the result under when the sim finishes (a day sim publishes as
// "playMenu.sim"), and what afterAction relabels any interleaved drain of that
// result to. These need sim authority to CALL - see the sets above, and the
// own-game carve-out in worker/index.ts that lets a follower run one for its
// own team - but they are not timeline advances when PUBLISHED (below).
export const SINGLE_GAME_SIM_LABELS: ReadonlySet<string> = new Set([
	"actions.simGame",
	"actions.liveGame",
	"playMenu.simGame",
]);

export const isSingleGameSimLabel = (label: string): boolean =>
	SINGLE_GAME_SIM_LABELS.has(label);

// Same question asked of a changeset's action label ("playMenu.sim",
// "main.proposeTrade", ...) - the form the sync engines see. A timeline
// advance authored on state the room has since moved past must be DISCARDED,
// never republished (republishing a stale sim day is exactly how v1 leagues
// forked); an ordinary edit whose base moved is safe to catch up and retry,
// because it's a whole-record statement of user intent, not a derivation from
// a particular day.
//
// A SINGLE game is not an advance here, even though calling for one needs
// authority. What makes a whole day dangerous to republish is that it is a
// derivation from the entire league at one moment - injuries counted down,
// free agents signed, AI trades made, fourteen games - and none of that is
// true of one game with the rest of the day still to play: it touches its two
// teams, their players and its own schedule row, and the schedule-day fence
// (simDayClaimPolicy.ts) already guarantees nobody else can sim that gid. It
// was being classified as an advance anyway, so a league-mate's own-game sim
// that lost the compare-and-swap to somebody setting a lineup was thrown away
// and the device rolled back to a checkpoint - and because the throwaway
// counted as "not synced", the fence slice was never completed, its lease
// lapsed, and the room's next scheduled sim replayed the game as crash
// recovery. From the person who simmed it: "at 5:40 it simmed on its own as if
// I had never simmed." A lost race for one game rebases like any other edit.
export const isTimelineAdvanceLabel = (label: string): boolean => {
	const dot = label.indexOf(".");
	if (dot === -1) {
		return false;
	}
	if (isSingleGameSimLabel(label)) {
		return false;
	}
	return isSimAuthorityLockedCall(label.slice(0, dot), label.slice(dot + 1));
};
