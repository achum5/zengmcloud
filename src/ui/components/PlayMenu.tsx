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

// Play-menu items that stay available on a device that is not in charge of simming:
// "stop"/"stopAuto" just halt. Drafting your own player isn't a play-menu item
// (you click a player), so the draft ADVANCERS here move the shared draft and
// are locked for non-simmers. Mirrors the guard in worker/index.ts.
const PLAY_MENU_SIM_AUTHORITY_EXEMPT = new Set(["stop", "stopAuto"]);

// In a synced league, advancing the shared draft is irreversible for the whole
// room - confirm so the simmer can't fat-finger past someone's pick.
const DRAFT_ADVANCE_CONFIRM: Record<string, string> = {
	onePick: "Sim one pick?",
	untilYourNextPick: "Sim to your next pick?",
	untilEnd: "Sim to the end of the draft?",
};

const confirmDraftAdvance = async (id: string): Promise<boolean> => {
	const message = DRAFT_ADVANCE_CONFIRM[id];
	if (message === undefined) {
		return true;
	}
	return confirm(message, { okText: "Sim", cancelText: "Cancel" });
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
					if (
						local.getState().mpSyncActive &&
						!(await confirmDraftAdvance(option.id as string))
					) {
						return;
					}
					toWorker("playMenu", option.id as any, undefined);
				}
			},
			[options],
		),
	});

	const handleOptionClick = async (option: Option, event: MouseEvent) => {
		if (!option.url) {
			event.preventDefault();
			if (
				local.getState().mpSyncActive &&
				!(await confirmDraftAdvance(option.id as string))
			) {
				return;
			}
			toWorker("playMenu", option.id as any, undefined);
		}
	};

	const {
		keyboardShortcuts: keyboardShortcutsLocal,
		mpSyncActive,
		mpSyncIsHost,
		mpSyncHostName,
		mpSyncReady,
		mpSyncReconnecting,
	} = useLocal([
		"keyboardShortcuts",
		"mpSyncActive",
		"mpSyncIsHost",
		"mpSyncHostName",
		"mpSyncReady",
		"mpSyncReconnecting",
	]);

	// Season/phase advancement is locked when we're reconnecting/offline, or when
	// we're synced but not in charge of simming (the worker enforces both). Draft
	// items stay available.
	const locked =
		mpSyncReconnecting || (mpSyncActive && (!mpSyncIsHost || !mpSyncReady));

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
					<Dropdown.Item href={`/l/${lid}/multiplayer_sync`}>
						<span className="text-body-secondary">
							{mpSyncReconnecting
								? "🔄 Reconnecting to the league…"
								: !mpSyncIsHost
									? `🔒 ${mpSyncHostName ?? "Another device"} is in charge of simming`
									: "Cloud sync is not ready"}
						</span>
					</Dropdown.Item>
				) : null}
				{options.map((option, i) => {
					// Only lock options that actually SIM. A url option just navigates
					// (e.g. "One day (live)" → the daily schedule page, "View draft"),
					// and the exempt ids are turn-based draft advances - never lock those.
					const optionLocked =
						locked &&
						!option.url &&
						!PLAY_MENU_SIM_AUTHORITY_EXEMPT.has(option.id as string);
					return (
						<Dropdown.Item
							key={i}
							href={optionLocked ? undefined : option.url}
							disabled={optionLocked}
							onClick={(event: MouseEvent<any>) =>
								void handleOptionClick(option, event)
							}
							className="kbd-parent"
							title={
								optionLocked
									? mpSyncReconnecting
										? "Reconnecting to the league…"
										: !mpSyncIsHost
											? `${mpSyncHostName ?? "Another device"} is in charge of simming`
											: "Cloud sync is not ready"
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
