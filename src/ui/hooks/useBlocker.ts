import { useEffect, useState } from "react";
import { router } from "../router/index.ts";
import { confirm } from "../util/confirm.tsx";

export const useBlocker = ({
	message = "If you navigate away from this page, you will lose any unsaved changes.",
	okText = "Navigate away",
	cancelText = "Stay here",
	initialDirty = false,
}: {
	message?: string;
	okText?: string;
	cancelText?: string;
	initialDirty?: boolean;
} = {}) => {
	const [dirty, setDirty] = useState(initialDirty);

	useEffect(() => {
		// `dirty` decides whether we block: for the live sim it's true while the
		// game is live and cleared when it ends. (There used to be a `hardBlock`
		// mode that silently refused ALL navigation, to pin a multiplayer viewer
		// inside a broadcast. Viewers can leave any broadcast now, so nothing
		// needs a trap with no way out of it.)
		if (dirty) {
			router.shouldBlock = async (refresh) => {
				// This check is needed because realtimeUpdate triggers a refresh pageview through the router to trigger updating data, but we never consider that "navigating away" from a page. For example when clicking "Save" on League Settings
				if (refresh) {
					return false;
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
	}, [cancelText, dirty, message, okText]);

	return { dirty, setDirty };
};
