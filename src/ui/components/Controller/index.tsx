import { LazyMotion } from "framer-motion";
import { memo, useCallback, useEffect } from "react";
import { localActions, useLocal } from "../../util/local.ts";
import { autoReconnectSync } from "../../util/autoReconnectSync.ts";
import { rememberLidForPush } from "../../util/pushLid.ts";
import { CommandPalette } from "../CommandPalette/index.tsx";
import { Footer } from "./Footer.tsx";
import { Header } from "./Header.tsx";
import { LeagueTopBar } from "./LeagueTopBar.tsx";
import { LeagueTicker } from "./LeagueTicker.tsx";
import { MultiTeamMenu } from "./MultiTeamMenu.tsx";
import { NagModal } from "./NagModal.tsx";
import { NavBar } from "./NavBar.tsx";
import { Notifications } from "./Notifications.tsx";
import SyncDebugOverlay from "./SyncDebugOverlay.tsx";
import { SideBar } from "./SideBar.tsx";
import { Skyscraper } from "./Skyscraper.tsx";
import { TitleBar } from "./TitleBar.tsx";
import { useViewData } from "../../util/viewManager.tsx";
import { toWorker } from "../../util/toWorker.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import api from "../../api/index.ts";
import { ErrorBoundary } from "../ErrorBoundary.tsx";

const loadFramerMotionFeatures = () =>
	import("../../util/framerMotionFeatures.ts").then((res) => res.default);

const minHeight100 = {
	// Just using h-100 class here results in the sticky ad in the skyscraper becoming unstuck after scrolling down 100% of the viewport, for some reason
	minHeight: "100%",
};

const minWidth0 = {
	// Fix for responsive table not being triggered by flexbox limits, and skyscraper ad overflowing content https://stackoverflow.com/a/36247448/786644
	minWidth: 0,
};

type KeepPreviousRenderWhileUpdatingProps = {
	children: any;
	updating: boolean;
};
const KeepPreviousRenderWhileUpdating = memo(
	(props: KeepPreviousRenderWhileUpdatingProps) => {
		return props.children;
	},
	(
		prevProps: KeepPreviousRenderWhileUpdatingProps,
		nextProps: KeepPreviousRenderWhileUpdatingProps,
	) => {
		// No point in rendering while updating contents
		return nextProps.updating;
	},
);

export const Controller = () => {
	const state = useViewData();

	const { lid, mpSyncActive, mpSyncReconnecting, popup, showNagModal } =
		useLocal([
			"lid",
			"mpSyncActive",
			"mpSyncReconnecting",
			"popup",
			"showNagModal",
		]);

	// If this league was left connected to a shared-league sync room, reconnect
	// after a refresh (which tears down the worker's in-memory sync engine). Also
	// remember this device's lid so a tapped push notification can deep-link into
	// the right league even when the app was fully closed.
	useEffect(() => {
		if (typeof lid === "number") {
			void autoReconnectSync(lid);
			rememberLidForPush(lid);
		}
	}, [lid]);

	// Sync debug logging is OFF by default (the log firehose lags the game). To
	// diagnose a sync issue, opt in from the browser console and refresh:
	//   localStorage.setItem("syncDebugLog", "1")
	useEffect(() => {
		try {
			if (localStorage.getItem("syncDebugLog") === "1") {
				void toWorker("main", "setSyncDebugLogging", true);
			}
		} catch {}
	}, []);

	useEffect(() => {
		if (!mpSyncActive && !mpSyncReconnecting) {
			return;
		}

		let cancelled = false;
		const check = async () => {
			try {
				await toWorker("main", "checkSyncReady", undefined);
			} catch {
				// The worker updates local state on normal failures; if the worker
				// itself is unavailable, the existing reconnect guard still blocks sim.
			}
		};

		void check();
		const intervalID = setInterval(() => {
			if (!cancelled) {
				void check();
			}
		}, 5000);

		return () => {
			cancelled = true;
			clearInterval(intervalID);
		};
	}, [mpSyncActive, mpSyncReconnecting]);

	const closeNagModal = useCallback(() => {
		localActions.update({
			showNagModal: false,
		});
	}, []);

	useEffect(() => {
		if (popup) {
			document.body.style.paddingTop = "8px";
			const css = document.createElement("style");
			css.innerHTML = ".new_window { display: none }";
			document.body.append(css);
		}
	}, [popup]);

	useEffect(() => {
		// Try to show ads on initial render
		api.initAds("uiRendered");
	}, []);

	const {
		Component,
		data,
		idLoading,
		idLoaded,
		inLeague,
		loading: updating,
		scrollToTop,
	} = state;

	// Optimistically use idLoading before it renders, for UI responsiveness in the sidebar
	const sidebarPageID = idLoading ?? idLoaded;

	const pathname = isSport("baseball") ? document.location.pathname : undefined;

	// Scroll to top if this load came from user clicking a link to a new page
	useEffect(() => {
		if (scrollToTop) {
			window.scrollTo(window.pageXOffset, 0);
		}
	}, [idLoaded, scrollToTop]);

	return (
		<LazyMotion strict features={loadFramerMotionFeatures}>
			<NavBar updating={updating} />
			{/* The header above is sticky (in flow), so the page fills the rest of
			    #content via flex-grow rather than percentage heights - #content is
			    min-height:100%, and h-100 chains don't resolve against that. */}
			<div className="d-flex flex-grow-1">
				<SideBar pageID={sidebarPageID} pathname={pathname} />
				<div className="w-100 d-flex flex-column" style={minWidth0}>
					{popup ? null : <LeagueTopBar />}
					<TitleBar />
					<div className="container-fluid position-relative mt-2 flex-grow-1">
						<div className="d-flex" style={minHeight100}>
							<div className="w-100 d-flex flex-column" style={minWidth0}>
								<Header />
								<main id="actual-actual-content" className="clearfix">
									<ErrorBoundary key={idLoaded}>
										{Component ? (
											<KeepPreviousRenderWhileUpdating updating={updating}>
												<Component {...data} />
											</KeepPreviousRenderWhileUpdating>
										) : null}
										{inLeague ? <MultiTeamMenu /> : null}
									</ErrorBoundary>
								</main>
								<Footer />
							</div>
							<Skyscraper />
						</div>
						<CommandPalette />
						<NagModal close={closeNagModal} show={showNagModal} />
					</div>
				</div>
			</div>
			<Notifications />
			<SyncDebugOverlay />
			{/* LAST, and in the flow. The ticker is position:sticky against the
			    document, so it has to be the final child of #content for its natural
			    position to be the bottom of the page. */}
			{popup ? null : <LeagueTicker />}
		</LazyMotion>
	);
};
