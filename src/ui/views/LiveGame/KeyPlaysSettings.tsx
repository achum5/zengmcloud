import { useState, type ReactNode } from "react";
import { Modal } from "../../components/Modal.tsx";
import {
	KEY_PLAY_LIMITS,
	parseKeyPlays,
	setKeyPlays,
	type KeyPlaySettings,
} from "./court3d/keyPlays.ts";
import { CAMERA_ZOOM, getCamera, setCamera } from "./court3d/cameraSetting.ts";
import { setPacing, usePacing } from "./court3d/pacing.ts";

// THE 3D GAME'S SETTINGS, under the gear: whose broadcast it's on (a game on
// its teams' own), how far in the camera is, whether to fast-forward to the
// action, and when a game counts as close - every play of a close finish
// shown on top of the key plays' length. Every kind of big play always
// makes the reel (see getKeyPlays). Saved on this device - the broadcast,
// for this game only.

// One setting to a line: its name, and what it's set to beside it.
const ROW = "d-flex align-items-center justify-content-between gap-3 py-2";

export type BroadcastPick = {
	// The game's teams, visitors first.
	teams: { tid: number; abbrev: string; name: string }[];
	// Whose broadcast it's on.
	tid: number;
	onChange: (tid: number) => void;
};

const KeyPlaysSettings = ({
	value,
	onChange,
	picker,
	label,
	broadcast,
}: {
	value: KeyPlaySettings;
	onChange: (value: KeyPlaySettings) => void;
	// Every play or the key plays, picked in here rather than beside the
	// button (the controls laid over the gamecast's court, short of room).
	picker?: ReactNode;
	// Beside the gear (the key plays' length, when they're on).
	label?: ReactNode;
	// A game on its teams' own broadcasts: whose to watch it on.
	broadcast?: BroadcastPick;
}) => {
	const [show, setShow] = useState(false);
	// What is typed in each number box, while it is being typed - so the box
	// can be emptied to type a new number over it, which a box showing only
	// the saved number snapped straight back from.
	const [typed, setTyped] = useState<
		Partial<Record<"closePoints" | "closeMinutes", string>>
	>({});
	const set = (next: Partial<KeyPlaySettings>) => {
		const v = parseKeyPlays({ ...value, ...next });
		setKeyPlays(v);
		onChange(v);
	};
	const number = (
		key: "closePoints" | "closeMinutes",
		unit: string,
		title: string,
		step: number,
	) => (
		<div className="input-group input-group-sm flex-fill w-auto">
			<input
				id={`kp-${key}`}
				type="number"
				inputMode={step < 1 ? "decimal" : "numeric"}
				className="form-control"
				title={title}
				aria-label={title}
				min={KEY_PLAY_LIMITS[key][0]}
				max={KEY_PLAY_LIMITS[key][1]}
				step={step}
				value={typed[key] ?? value[key]}
				onChange={(e) => {
					const text = e.target.value;
					setTyped((t) => ({ ...t, [key]: text }));
					const n = Number.parseFloat(text);
					if (Number.isFinite(n)) {
						set({ [key]: n });
					}
				}}
				// (Left empty, or out of range: back to what was saved.)
				onBlur={() => {
					setTyped((t) => ({ ...t, [key]: undefined }));
				}}
			/>
			<span className="input-group-text">{unit}</span>
		</div>
	);
	const football = __SPORT === "football";
	// How far in the court's camera is (see cameraSetting.ts) - the court
	// reads it as it draws, so it moves as the slider does.
	const [zoom, setZoom] = useState(() => getCamera().zoom);
	// Whole possessions, or on to the last seconds before the action (see
	// pacing.ts) - read by the court as it draws.
	const { whole } = usePacing();
	return (
		<>
			<button
				type="button"
				className="btn btn-link p-0 ms-2 border-0 text-body-secondary lh-1 d-inline-flex align-items-center"
				title="Settings"
				onClick={() => setShow(true)}
			>
				{label}
				<svg
					width="16"
					height="16"
					viewBox="0 0 16 16"
					fill="currentColor"
					aria-hidden="true"
					style={{ display: "block" }}
				>
					<path d="M8 4.754a3.246 3.246 0 1 0 0 6.492 3.246 3.246 0 0 0 0-6.492M5.754 8a2.246 2.246 0 1 1 4.492 0 2.246 2.246 0 0 1-4.492 0" />
					<path d="M9.796 1.343c-.527-1.79-3.065-1.79-3.592 0l-.094.319a.873.873 0 0 1-1.255.52l-.292-.16c-1.64-.892-3.433.902-2.54 2.541l.159.292a.873.873 0 0 1-.52 1.255l-.319.094c-1.79.527-1.79 3.065 0 3.592l.319.094a.873.873 0 0 1 .52 1.255l-.16.292c-.892 1.64.901 3.434 2.541 2.54l.292-.159a.873.873 0 0 1 1.255.52l.094.319c.527 1.79 3.065 1.79 3.592 0l.094-.319a.873.873 0 0 1 1.255-.52l.292.16c1.64.893 3.434-.902 2.54-2.541l-.159-.292a.873.873 0 0 1 .52-1.255l.319-.094c1.79-.527 1.79-3.065 0-3.592l-.319-.094a.873.873 0 0 1-.52-1.255l.16-.292c.893-1.64-.902-3.433-2.541-2.54l-.292.159a.873.873 0 0 1-1.255-.52zm-2.633.283c.246-.835 1.428-.835 1.674 0l.094.319a1.873 1.873 0 0 0 2.693 1.115l.291-.16c.764-.415 1.6.42 1.184 1.185l-.159.292a1.873 1.873 0 0 0 1.116 2.692l.318.094c.835.246.835 1.428 0 1.674l-.319.094a1.873 1.873 0 0 0-1.115 2.693l.16.291c.415.764-.42 1.6-1.185 1.184l-.291-.159a1.873 1.873 0 0 0-2.693 1.116l-.094.318c-.246.835-1.428.835-1.674 0l-.094-.319a1.873 1.873 0 0 0-2.692-1.115l-.292.16c-.764.415-1.6-.42-1.184-1.185l.159-.291A1.873 1.873 0 0 0 1.945 8.93l-.319-.094c-.835-.246-.835-1.428 0-1.674l.319-.094A1.873 1.873 0 0 0 3.06 4.377l-.16-.292c-.415-.764.42-1.6 1.185-1.184l.292.159a1.873 1.873 0 0 0 2.692-1.115z" />
				</svg>
			</button>
			<Modal show={show} onHide={() => setShow(false)} size="sm">
				<Modal.Header closeButton>
					<Modal.Title>{football ? "Key plays" : "Settings"}</Modal.Title>
				</Modal.Header>
				<Modal.Body className="py-2">
					{broadcast ? (
						<div className={ROW}>
							<span id="c3d-broadcast">Broadcast</span>
							<div
								className="btn-group btn-group-sm"
								role="group"
								aria-labelledby="c3d-broadcast"
							>
								{broadcast.teams.map((t) => (
									<button
										key={t.tid}
										type="button"
										className={
											t.tid === broadcast.tid
												? "btn btn-primary"
												: "btn btn-outline-secondary"
										}
										aria-pressed={t.tid === broadcast.tid}
										title={`${t.name} broadcast`}
										onClick={() => broadcast.onChange(t.tid)}
									>
										{t.abbrev}
									</button>
								))}
							</div>
						</div>
					) : null}
					{picker ? (
						<div className={ROW}>
							<span>Plays</span>
							{picker}
						</div>
					) : null}
					{football ? null : (
						<div className="py-2">
							<div className="d-flex justify-content-between">
								<label htmlFor="c3d-zoom">Camera zoom</label>
								<span className="text-body-secondary">
									{zoom.toFixed(2).replace(/0$/, "")}x
								</span>
							</div>
							<input
								id="c3d-zoom"
								type="range"
								className="form-range d-block"
								min={CAMERA_ZOOM.min}
								max={CAMERA_ZOOM.max}
								step={CAMERA_ZOOM.step}
								value={zoom}
								onChange={(e) => {
									const z = Number(e.target.value);
									setZoom(z);
									setCamera({ zoom: z });
								}}
							/>
						</div>
					)}
					{football ? null : (
						<div className={ROW}>
							<label htmlFor="c3d-skip">Fast-forward to the action</label>
							<div className="form-switch d-flex">
								<input
									id="c3d-skip"
									type="checkbox"
									role="switch"
									className="form-check-input m-0"
									checked={!whole}
									onChange={(e) => {
										setPacing({ whole: !e.target.checked });
									}}
								/>
							</div>
						</div>
					)}
					<div
						className={
							picker || !football ? "border-top mt-2 pt-3 pb-2" : "py-2"
						}
					>
						<div className="mb-2">Close game</div>
						<div className="d-flex gap-2">
							{number("closePoints", "pts", "Within this many points", 1)}
							{number(
								"closeMinutes",
								"min left",
								"With this many minutes left",
								0.5,
							)}
						</div>
					</div>
				</Modal.Body>
				<Modal.Footer>
					<button
						type="button"
						className="btn btn-primary"
						onClick={() => setShow(false)}
					>
						Done
					</button>
				</Modal.Footer>
			</Modal>
		</>
	);
};

export default KeyPlaysSettings;
