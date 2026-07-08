import { useEffect, useState } from "react";
import { router } from "../router/index.ts";
import { confirm } from "../util/confirm.tsx";

export const useBlocker = ({
	message = "If you navigate away from this page, you will lose any unsaved changes.",
	okText = "Navigate away",
	cancelText = "Stay here",
	initialDirty = false,
	hardBlock = false,
}: {
	message?: string;
	okText?: string;
	cancelText?: string;
	initialDirty?: boolean;
	// When true, silently block ALL navigation (no confirm) - used to lock a
	// multiplayer follower into a live-sim broadcast until the simmer ends it.
	hardBlock?: boolean;
} = {}) => {
	const [dirty, setDirty] = useState(initialDirty);

	useEffect(() => {
		if (dirty || hardBlock) {
			router.shouldBlock = async (refresh) => {
				// This check is needed because realtimeUpdate triggers a refresh pageview through the router to trigger updating data, but we never consider that "navigating away" from a page. For example when clicking "Save" on League Settings
				if (refresh) {
					return false;
				}

				// A locked follower can't leave at all - no escape hatch, no dialog.
				if (hardBlock) {
					return true;
				}

				const proceed = await confirm(message, {
					okText,
					cancelText,
				});

				return !proceed;
			};

			return () => {
				router.shouldBlock = undefined;
			};
		} else {
			router.shouldBlock = undefined;
		}
	}, [cancelText, dirty, hardBlock, message, okText]);

	return { dirty, setDirty };
};
