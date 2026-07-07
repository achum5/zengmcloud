import { useCallback, type MouseEvent } from "react";
import { Dropdown, Nav } from "react-bootstrap";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { local, useLocal } from "../util/local.ts";
import type { Option } from "../../common/types.ts";
import clsx from "clsx";
import {
	formatKeyboardShortcut,
	useKeyboardShortcuts,
} from "../util/keyboardShortcuts.ts";
import { confirm } from "../util/confirm.tsx";

// Play-menu items that stay available on a device that doesn't hold the wheel:
// "stop"/"stopAuto" just halt, and the draft-advancement items are turn-based
// (any user drafts their own team). Mirrors the guard in worker/index.ts.
const PLAY_MENU_WHEEL_EXEMPT = new Set([
	"stop",
	"stopAuto",
	"onePick",
	"untilYourNextPick",
	"untilEnd",
]);

const handleOptionClick = (option: Option, event: MouseEvent) => {
	if (!option.url) {
		event.preventDefault();
		toWorker("playMenu", option.id as any, undefined);
	}
};

const PlayMenu = ({
	lid,
	spectator,
	options,
}: {
	lid: number | undefined;
	spectator: boolean;
	options: Option[];
}) => {
	useKeyboardShortcuts({
		category: "playMenu",
		callback: useCallback(
			async (action) => {
				const option = options.find(
					(option2) => option2.keyboardShortcut === action,
				);

				if (!option) {
					return;
				}

				if (window.location.pathname.includes("/live_game")) {
					const liveGameInProgress = local.getState().liveGameInProgress;
					if (liveGameInProgress) {
						const proceed = await confirm(
							"Are you sure you meant to press a Play Menu keyboard shortcut while watching a live sim?",
							{
								okText: "Yes",
								cancelText: "Cancel",
							},
						);
						if (!proceed) {
							return;
						}
					}
				}

				if (option.url) {
					realtimeUpdate([], option.url);
				} else {
					toWorker("playMenu", option.id as any, undefined);
				}
			},
			[options],
		),
	});

	const {
		keyboardShortcuts: keyboardShortcutsLocal,
		mpSyncActive,
		mpSyncIsHost,
		mpSyncHostName,
		mpSyncReconnecting,
	} = useLocal([
		"keyboardShortcuts",
		"mpSyncActive",
		"mpSyncIsHost",
		"mpSyncHostName",
		"mpSyncReconnecting",
	]);

	// Season/phase advancement is locked when we're reconnecting/offline, or when
	// we're synced but don't hold the wheel (the worker enforces both). Draft
	// items stay available.
	const locked = mpSyncReconnecting || (mpSyncActive && !mpSyncIsHost);

	if (lid === undefined) {
		return null;
	}

	return (
		<Dropdown
			className={`play-button-wrapper${
				window.mobile ? " dropdown-mobile" : ""
			}`}
			as={Nav.Item}
		>
			<Dropdown.Toggle
				className={clsx(
					"play-button",
					spectator ? "play-button-danger" : "play-button-success",
				)}
				id="play-button"
				as={Nav.Link}
			>
				Play
			</Dropdown.Toggle>
			<Dropdown.Menu>
				{locked ? (
					<Dropdown.Header>
						{mpSyncReconnecting ? "🔄 Reconnecting to the league…" : "🔒"}
					</Dropdown.Header>
				) : null}
				{options.map((option, i) => {
					// Only lock options that actually SIM. A url option just navigates
					// (e.g. "One day (live)" → the daily schedule page, "View draft"),
					// and the exempt ids are turn-based draft advances - never lock those.
					const optionLocked =
						locked &&
						!option.url &&
						!PLAY_MENU_WHEEL_EXEMPT.has(option.id as string);
					return (
						<Dropdown.Item
							key={i}
							href={optionLocked ? undefined : option.url}
							disabled={optionLocked}
							onClick={(event: MouseEvent<any>) =>
								handleOptionClick(option, event)
							}
							className="kbd-parent"
							title={
								optionLocked
									? mpSyncReconnecting
										? "Reconnecting to the league…"
										: `${mpSyncHostName ?? "Another device"} is simming`
									: undefined
							}
						>
							{option.label}
							{option.keyboardShortcut ? (
								<span className="text-body-secondary kbd">
									{formatKeyboardShortcut(
										"playMenu",
										option.keyboardShortcut,
										keyboardShortcutsLocal,
									)}
								</span>
							) : null}
						</Dropdown.Item>
					);
				})}
			</Dropdown.Menu>
		</Dropdown>
	);
};

export default PlayMenu;
