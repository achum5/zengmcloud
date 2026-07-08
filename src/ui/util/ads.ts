import { AD_DIVS } from "../../common/constants.ts";
import { localActions } from "./local.ts";

const SKYSCAPER_WIDTH_CUTOFF = 1200 + 190;

class Skyscraper {
	displayed = false;

	updateDislay(initial: boolean) {
		const div = document.getElementById(AD_DIVS.rail);

		if (div) {
			const gold = !!div.dataset.gold;

			if (
				document.documentElement.clientWidth >= SKYSCAPER_WIDTH_CUTOFF &&
				!gold
			) {
				if (!this.displayed) {
					const before = () => {
						div.style.display = "block";
					};
					const after = () => {
						this.displayed = true;
					};

					if (initial) {
						// On initial load, we can batch ad request with others
						before();
						window.freestar.config.enabled_slots.push({
							placementName: AD_DIVS.rail,
							slotId: AD_DIVS.rail,
						});
						after();
					} else {
						window.freestar.queue.push(() => {
							before();
							window.freestar.newAdSlots([
								{
									placementName: AD_DIVS.rail,
									slotId: AD_DIVS.rail,
								},
							]);
							after();
						});
					}
				}
			} else {
				if (this.displayed || gold) {
					window.freestar.queue.push(() => {
						div.style.display = "none";
						window.freestar.deleteAdSlots(AD_DIVS.rail);
						this.displayed = false;
					});
				}
			}
		}
	}
}

type AdState = "none" | "gold" | "initializing" | "initialized";

class Ads {
	skyscraper = new Skyscraper();
	private state: AdState = "none";

	// Kept so the existing call sites (api.initAds) still work. With ads disabled,
	// there's no load-order race to wait for - init() is idempotent, so just run it.
	setLoadingDone(_type: "accountChecked" | "uiRendered") {
		this.init();
	}

	init() {
		if (this.state !== "none") {
			// Must have already ran somehow?
			return;
		}

		// Ads are disabled entirely in this build. Take the ad-free ("gold") path
		// unconditionally: never request Freestar slots, never reveal the hidden ad
		// divs, never add mobile footer padding, never touch window.freestar. Every
		// other method keys off this.state, so setting it here keeps
		// refreshAll()/skyscraper/etc. inert.
		this.state = "gold";
	}

	// This does the opposite of initAds. To be called when a user subscribes to gold or logs in to an account with an active subscription
	stop() {
		// Ads are disabled, so they're never started - and window.freestar never
		// loads. Nothing to tear down.
		if (!window.freestar) {
			return;
		}
		window.freestar.queue.push(() => {
			const divsAll = [
				AD_DIVS.mobile,
				AD_DIVS.leaderboard,
				AD_DIVS.rectangle1,
				AD_DIVS.rectangle2,
			];

			for (const id of divsAll) {
				const div = document.getElementById(id);

				if (div) {
					div.style.display = "none";
				}

				window.freestar.deleteAdSlots(id);
			}

			// Special case for rail, to tell it there is no BBGM gold
			const rail = document.getElementById(AD_DIVS.rail);
			if (rail) {
				rail.dataset.gold = "true";
				this.skyscraper.updateDislay(false);
			}

			localActions.update({
				stickyFooterAd: false,
			});

			// Add margin to footer - do this manually rather than using stickyFooterAd so <Footer> does not have to re-render
			const footer = document.getElementById("main-footer");
			if (footer) {
				footer.style.marginBottom = "";
			}

			const logo = document.getElementById("bbgm-ads-logo");
			if (logo) {
				logo.style.display = "none";
			}

			// Rename to hide from Blockthrough
			for (const id of [...divsAll, AD_DIVS.rail]) {
				const div = document.getElementById(id);

				if (div) {
					div.id = `${id}_disabled`;
				}
			}

			this.state = "gold";
		});
	}

	adBlock() {
		return (
			!window.freestar ||
			!window.freestar.refreshAllSlots ||
			!window.googletag ||
			!window.googletag.pubads
		);
	}

	trackPageview(path: string) {
		// Freestar pageview tracking; with ads disabled window.freestar never loads.
		if (!window.freestar) {
			return;
		}
		// https://freestarhelp.zendesk.com/hc/en-us/articles/34417159798804-Track-Page-Views
		window.freestar.queue.push(() => {
			window.freestar.trackPageview?.({ path });
		});
	}

	refreshAll() {
		if (this.state === "initialized") {
			window.freestar.queue.push(() => {
				window.freestar.refreshAllSlots?.();
			});
		}
	}
}

export const ads = new Ads();
