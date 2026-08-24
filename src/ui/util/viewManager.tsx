import type { UpdateEvents } from "../../common/types.ts";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { type Context, makeRegex, router } from "../router/index.ts";
import { local, localActions } from "./local.ts";
import { realtimeUpdate } from "./realtimeUpdate.ts";
import { toWorker } from "./toWorker.ts";
import { create } from "zustand";
import { routeInfos } from "./routeInfos.ts";

/**
 * Things that might be nice, to improve this:
 *
 * - remove tight coupling with router
 * - automatically push updateEvents to other tabs, if there are any updateEvents
 * - good way to handle navigation+updateEvents, where navigation is only one tab but updateEvents go to other tabs
 * - if this is a refresh, check if an exact same refresh is in queue already. if so, discard
 * - tests
 */

type Action = {
	url?: string;
	refresh: boolean;
	replace?: boolean;
	updateEvents: UpdateEvents;
	raw?: Record<string, unknown>;
};

type ActionWithResolve = Action & {
	resolve: () => void;
};

type State = {
	Component: any;
	loading: boolean;
	idLoaded: string | undefined;
	idLoading: string | undefined;
	inLeague: boolean;
	data: Record<string, any>;
	scrollToTop: boolean;
};

type ViewInfo = {
	Component: any;
	id: string;
	inLeague: boolean;
	context: Context;
};

export const useViewData = create<
	State & {
		actions: {
			startLoading: (idLoading: string) => void;
			doneLoading: (idLoaded: string) => void;
			reset: (state: State) => void;
		};
	}
>((set) => ({
	Component: undefined,
	loading: false,
	idLoaded: undefined,
	idLoading: undefined,
	inLeague: false,
	data: {},
	actions: {
		startLoading: (id: string) => set({ idLoading: id, loading: true }),
		doneLoading: (id: string) =>
			set({ idLoaded: id, idLoading: undefined, loading: false }),
		reset: (state: State) => {
			set(state);
		},
	},
	scrollToTop: false,
}));

const actions = useViewData.getState().actions;

const ErrorMessage = ({ errorMessage }: { errorMessage: string }) => {
	useTitleBar({
		title: "Error",
	});
	return <p>{errorMessage}</p>;
};

class ViewManager {
	queue: ActionWithResolve[];
	viewData: Record<string, unknown>;
	idLoaded: string | undefined;
	processingAction: boolean;

	// Which action the queue is currently held for. Bumped once per initAction,
	// so a release can tell "the view ran and already moved us on" from "the
	// view never ran and nobody is coming".
	actionToken: number;
	routes: {
		id: string;
		regex: RegExp;
	}[];

	// When navigation to a new URL happens (can be from clicking a link in which case it goes directly to fromRouter, or from realtimeUpdate in which case it goes to fromRealtimeUpdate first and then eventually fromRouter) we want to be able to discard any in-progress load. Do that by keeping track of a symbol associated with a navigation.
	lastNavigationSymbol: symbol;

	constructor() {
		this.queue = [];
		this.viewData = {};
		this.processingAction = false;
		this.actionToken = 0;
		this.lastNavigationSymbol = Symbol();

		this.routes = [];
		for (const [path, id] of Object.entries(routeInfos)) {
			const { regex } = makeRegex(path);
			this.routes.push({
				id,
				regex,
			});
		}
	}

	private clearQueue() {
		for (const action of this.queue) {
			action.resolve();
		}
		this.queue = [];
	}

	async fromRouter(viewInfo: ViewInfo) {
		// If coming from initAction, state will contain navigationSymbol, and it will have already been set to this.lastNavigationSymbol
		if (viewInfo.context.state.navigationSymbol) {
			if (
				this.lastNavigationSymbol !== viewInfo.context.state.navigationSymbol
			) {
				// Must have been another navigation before this one processed
				return;
			}
		} else {
			// If coming only from router (like user clicked a link) then we set lastNavigationSymbol here and clear the queue
			this.lastNavigationSymbol = Symbol();
			this.clearQueue();
		}

		await this.processUpdate(viewInfo, this.lastNavigationSymbol);
	}

	fromRealtimeUpdate(action: Action) {
		// Return a promise because sometimes we want to wait for an update to process before continuing. For example, when simming multiple games, we want to update the UI between each day.
		return new Promise<void>((resolve) => {
			let navigationEvent = false;
			if (action.url) {
				// It's a "navigation event" if it is moving to a new page, rather than just changing some parameter of a page (like abbrev or season). So we need to get the id of this url and compare it to idLoaded.
				let id;
				const urlToMatch = action.url.split("?")[0]!.split("#")[0]!;
				for (const route of this.routes) {
					const m = route.regex.exec(urlToMatch);

					if (m) {
						id = route.id;
						break;
					}
				}

				if (id && id !== this.idLoaded) {
					navigationEvent = true;
				}
			}

			const actionWithResolve: ActionWithResolve = {
				...action,
				resolve,
			};

			if (navigationEvent) {
				this.lastNavigationSymbol = Symbol();
				this.clearQueue();
				this.initAction(actionWithResolve);
			} else if (this.queue.length === 0 && !this.processingAction) {
				this.initAction(actionWithResolve);
			} else {
				this.queue.push(actionWithResolve);
			}
		});
	}

	async initAction({
		url,
		refresh,
		replace,
		resolve,
		updateEvents,
		raw,
	}: ActionWithResolve) {
		this.processingAction = true;
		const token = ++this.actionToken;

		const state: any = {
			noTrack: refresh || replace,
			updateEvents,
			navigationSymbol: this.lastNavigationSymbol,
			...raw,
		};

		const actualURL = url ?? window.location.pathname + window.location.search;

		// AND THE CALLER HAS TO BE LET GO TOO. The promise this resolves is what
		// the worker awaits when it calls toUI("realtimeUpdate"), and a
		// navigate that rejects used to leave it pending for the life of the
		// page - so the worker sat waiting on a UI refresh that was never going
		// to answer, with everything behind it waiting too. Resolving in a
		// finally reports the attempt as finished, which it is; the error is
		// logged rather than swallowed.
		try {
			await router.navigate(actualURL, {
				state,
				refresh,

				// Would like to make this `replace: replace || url === undefined,` so it doesn't add a history entry on refreshes, but then Safari errors "Attempt to use history.replaceState() more than 100 times per 30 seconds"
				replace,
			});
		} catch (error) {
			console.error("Failed to navigate for a view update", error);
		} finally {
			// router.navigate runs fromRouter, which waits until the content is displayed, so we can resolve the action here
			resolve();

			// AND THE VIEW MIGHT NEVER HAVE RUN.
			//
			// navigate returns early, silently, in three cases: a blocker
			// refused it, routeMatched declined it, or no route matched. In none
			// of them is route.cb called, so processUpdate - the only thing that
			// releases the queue - never happens, and processingAction stays set
			// with nobody left to clear it. Every later update then parks on the
			// queue for good: the data keeps landing, the page never repaints,
			// and only a real navigation (which clears the queue outright) gets
			// it moving again.
			//
			// The token is what makes this safe to do here. If processUpdate DID
			// run it has already called initNextAction, which either left the
			// flag down or started the next action and bumped the token - so
			// this fires only when nothing else has taken over.
			if (this.processingAction && this.actionToken === token) {
				this.initNextAction();
			}
		}
	}

	initNextAction() {
		this.processingAction = false;

		const nextAction = this.queue.shift();
		if (nextAction) {
			this.initAction(nextAction);
		}
	}

	// WHATEVER HAPPENS IN HERE, THE QUEUE HAS TO BE RELEASED.
	//
	// processingAction is what stops two updates rendering over each other:
	// while it is set, fromRealtimeUpdate parks incoming actions on the queue
	// instead of running them, and initNextAction clears it and picks the next
	// one up. Every `return` in the body below is careful to call
	// initNextAction first - but there was no catch, so a single throw
	// anywhere in it (a worker call rejecting, most likely) left the flag set
	// with nobody to clear it.
	//
	// The page then stops updating. Not the failed update - EVERY update after
	// it, forever, because they all queue behind a flag that will never come
	// down. The data keeps arriving and landing correctly, the screen just
	// stops reflecting it, and the only way out is a real navigation, which
	// clears the queue and starts over. That is exactly what was reported from
	// a synced league: a trade arrives, the notification fires, it appears in
	// transactions, and the page it happened on keeps showing the old picks
	// until you leave the page and come back.
	//
	// So the release moves into a finally, and the individual calls come out.
	// The error is still logged and the spinner still stops - a stuck spinner
	// is a much better failure than a silently frozen page.
	async processUpdate(viewInfo: ViewInfo, navigationSymbol: symbol) {
		try {
			await this.runUpdate(viewInfo, navigationSymbol);
		} catch (error) {
			console.error("Failed to update the current view", error);
			actions.doneLoading(viewInfo.id);
		} finally {
			this.initNextAction();
		}
	}

	private async runUpdate(
		{ Component, context, id, inLeague }: ViewInfo,
		navigationSymbol: symbol,
	) {
		actions.startLoading(id);

		const updateEvents = context.state.updateEvents ?? [];

		let lidUrl: number | undefined;
		if (typeof context.params.lid === "string") {
			const newLidInt = Number.parseInt(context.params.lid);
			if (!Number.isNaN(newLidInt)) {
				lidUrl = newLidInt;
			}
		}

		let prevData;
		if (this.idLoaded !== id) {
			// This is the initial load of a page, so reset viewData and add firstRun update event
			if (!updateEvents.includes("firstRun")) {
				updateEvents.push("firstRun");
			}
			prevData = {};
		} else {
			prevData = {
				...this.viewData,
			};
		}

		const lidCurrent = local.getState().lid;

		// Previously this was only called if necessary (switching to a new league, or leaving a league) but sometimes Safari seems to kill/restart the worker and then league state (g, idb) needs to be reset. And that can happen at any time!
		await toWorker("main", "beforeView", {
			inLeague,
			lidCurrent,
			lidUrl,
		});

		if (!inLeague && lidCurrent !== undefined) {
			localActions.updateGameAttributes({
				lid: undefined,
			});
		}

		if (navigationSymbol !== this.lastNavigationSymbol) {
			return;
		}

		// ctxBBGM is hacky!
		const ctxBBGM = { ...context.state };
		delete ctxBBGM.err; // Can't send Error to worker
		delete ctxBBGM.navigationSymbol; // Can't send Symbol to worker

		// Resolve all the promises before updating the UI to minimize flicker
		const results = await toWorker("main", "runBefore", {
			viewId: id,
			params: context.params,
			ctxBBGM,
			updateEvents,
			prevData,
		});

		if (navigationSymbol !== this.lastNavigationSymbol) {
			return;
		}

		// If results is undefined, it means the league wasn't loaded yet at the time of the request, likely because another league was opening in another tab at the same time. So stop now and wait until we get a signal that there is a new league.
		if (results === undefined) {
			actions.doneLoading(id);
			return;
		}

		// If there was an error before, still show it unless we've received some other data. Otherwise, noop refreshes (return undefined from view, for non-matching updateEvent) would clear the error. Clear it only when some data is returned... which still is not great, because maybe the data is from a runBefore function that's different than the one that produced the error. Ideally would either need to track which runBefore function produced the error, this is a hack. THIS MAY NO LONGER BE TRUE AFTER CONSOLIDATING RUNBEFORE INTO A SINGLE FUNCTION, ideally the worker/views function could then handle conflicts itself. But currently the only ones returning errorMessage have just one function so it's either all or nothing.
		if (results && Object.keys(results).length > 0) {
			delete prevData.errorMessage;
		}

		let NewComponent = Component;

		if (
			prevData.errorMessage ||
			(results && Object.hasOwn(results, "errorMessage"))
		) {
			NewComponent = ErrorMessage;
		}

		const vars = {
			Component: NewComponent,
			data: Object.assign(prevData, results),
			loading: false,
			idLoaded: id,
			idLoading: undefined,
			inLeague,
			scrollToTop: updateEvents.length === 1 && updateEvents[0] === "firstRun",
		};

		if (vars.data && vars.data.redirectUrl !== undefined) {
			// Wait a tick, otherwise there is a race condition on new page loads (such as reloading live_game box score) where initView is called and updates viewInfo while the local.subscribe subscription below is unsubscribed due to updatePage changing.
			await new Promise<void>((resolve) => {
				setTimeout(() => {
					resolve();
				}, 0);
			});

			realtimeUpdate(
				[],
				vars.data.redirectUrl,
				{
					backendRedirect: true,
				},
				true,
			);

			return;
		}

		actions.reset(vars);
		this.idLoaded = id;
		this.viewData = vars.data;
	}
}

export const viewManager = new ViewManager();
