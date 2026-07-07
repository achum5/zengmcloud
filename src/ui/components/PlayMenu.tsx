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
	} = useLocal([
		"keyboardShortcuts",
		"mpSyncActive",
		"mpSyncIsHost",
		"mpSyncHostName",
	]);

	// While synced but not holding the wheel, season/phase advancement is locked
	// to this device (the worker enforces it too). Draft items stay available.
	const locked = mpSyncActive && !mpSyncIsHost;

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
						🔒 {mpSyncHostName ?? "Another device"} has the wheel — take it on
						Multiplayer Sync to sim here
					</Dropdown.Header>
				) : null}
				{options.map((option, i) => {
					const optionLocked =
						locked && !PLAY_MENU_WHEEL_EXEMPT.has(option.id as string);
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
									? `${mpSyncHostName ?? "Another device"} has the wheel. Take it on the Multiplayer Sync page to sim on this device.`
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
