import { useState } from "react";
import { Modal } from "../../components/Modal.tsx";
import {
	KEY_PLAY_LIMITS,
	parseKeyPlays,
	setKeyPlays,
	type KeyPlaySettings,
} from "./court3d/keyPlays.ts";

// WHAT THE KEY PLAYS SHOW, picked beside them: a close finish shown in full
// (within so many points, so many minutes from the end), and what else
// makes the reel - saved on this device.

type Toggle = keyof Pick<
	KeyPlaySettings,
	| "dunks"
	| "threes"
	| "blocks"
	| "steals"
	| "andOnes"
	| "leadChanges"
	| "buzzer"
	| "baskets"
	| "swings"
	| "mine"
	| "closeOnTop"
>;
const TOGGLES: [Toggle, string, boolean][] = [
	// (Which, its label, and whether football's reel counts it too.)
	["dunks", "Dunks", false],
	["threes", "Threes", false],
	["blocks", "Blocks", false],
	["steals", "Steals", false],
	["andOnes", "And-ones", false],
	["buzzer", "Buzzer beaters", false],
	["leadChanges", "Lead changes", true],
	["swings", "Big swings", true],
	["baskets", "Other baskets", false],
	["mine", "My team first", true],
];

const KeyPlaysSettings = ({
	value,
	onChange,
}: {
	value: KeyPlaySettings;
	onChange: (value: KeyPlaySettings) => void;
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
		label: string,
		step: number,
	) => (
		<div className="col-6">
			<label className="form-label mb-1" htmlFor={`kp-${key}`}>
				{label}
			</label>
			<input
				id={`kp-${key}`}
				type="number"
				inputMode={step < 1 ? "decimal" : "numeric"}
				className="form-control form-control-sm"
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
		</div>
	);
	const football = __SPORT === "football";
	return (
		<>
			<button
				type="button"
				className="btn btn-link p-0 ms-2 border-0 text-body-secondary lh-1"
				title="Key plays settings"
				onClick={() => setShow(true)}
			>
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
					<Modal.Title>Key plays</Modal.Title>
				</Modal.Header>
				<Modal.Body>
					<div className="fw-bold mb-1">Close game</div>
					<div className="row g-2 mb-2">
						{number("closePoints", "Within (pts)", 1)}
						{number("closeMinutes", "Minutes left", 0.5)}
					</div>
					<div className="form-check mb-3">
						<input
							id="kp-closeOnTop"
							type="checkbox"
							className="form-check-input"
							checked={value.closeOnTop}
							onChange={(e) => set({ closeOnTop: e.target.checked })}
						/>
						<label className="form-check-label" htmlFor="kp-closeOnTop">
							Close finish on top of the length
						</label>
					</div>
					<div className="fw-bold mb-1">In the reel</div>
					{TOGGLES.filter(([, , both]) => !football || both).map(
						([key, label]) => (
							<div key={key} className="form-check">
								<input
									id={`kp-${key}`}
									type="checkbox"
									className="form-check-input"
									checked={value[key]}
									onChange={(e) => set({ [key]: e.target.checked })}
								/>
								<label className="form-check-label" htmlFor={`kp-${key}`}>
									{label}
								</label>
							</div>
						),
					)}
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
