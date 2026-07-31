import { useState } from "react";
import type { FaceConfig } from "facesjs";
import { generate, svgsIndex } from "facesjs";
import { Modal } from "./Modal.tsx";
import { PlayerPicture } from "./PlayerPicture.tsx";
import { toWorker } from "../util/toWorker.ts";
import { showNotification } from "../util/showNotification.ts";
import { updatePlayerFaceData } from "../util/playerFaces.ts";

// `jersey` and `teamColors` have no controls on purpose: the game overrides
// both with the player's team on the way to the screen, so editing them would
// change the stored config and nothing visible. They stay in the JSON untouched.
//
// facesjs's own generation ranges (numberRanges in generate.ts), which aren't
// exported. head.shave is the exception - facesjs tops its random shadow out at
// 0.2, but the useful range for matching a photo runs well past that.
const RANGES = {
	fatness: [0, 1, 0.01],
	"body.size": [0.8, 1.05, 0.01],
	"ear.size": [0.5, 1.5, 0.01],
	"eye.angle": [-10, 15, 1],
	"eyebrow.angle": [-15, 20, 1],
	"head.shave": [0, 1, 0.05],
	"nose.size": [0.5, 1.25, 0.01],
	"smileLine.size": [0.25, 2.25, 0.05],
} satisfies Record<string, [number, number, number]>;

// Female ids are a separate art set; this is a basketball league of men.
const idsFor = (slot: string) =>
	((svgsIndex as Record<string, readonly string[]>)[slot] ?? []).filter(
		(id) => !id.startsWith("female"),
	);

// head.shave is an rgba string, but the only part that ever varies is the alpha
// - it's a five o'clock shadow dial, not a color.
const shaveAlpha = (shave: string | undefined) => {
	const match = /rgba\((?:0,\s*){3}([\d.]+)\)/.exec(shave ?? "");
	return match ? Number.parseFloat(match[1]!) : 0;
};

const Slider = ({
	label,
	max,
	min,
	onChange,
	step,
	value,
}: {
	label: string;
	max: number;
	min: number;
	onChange: (value: number) => void;
	step: number;
	value: number;
}) => (
	<label className="d-block mb-2">
		<div className="d-flex text-body-secondary small">
			<span>{label}</span>
			<span className="ms-auto font-monospace">{value}</span>
		</div>
		<input
			className="form-range"
			max={max}
			min={min}
			onChange={(event) => {
				onChange(Number.parseFloat(event.target.value));
			}}
			step={step}
			type="range"
			value={value}
		/>
	</label>
);

const Picker = ({
	label,
	onChange,
	slot,
	value,
}: {
	label: string;
	onChange: (id: string) => void;
	slot: string;
	value: string;
}) => (
	<label className="d-block mb-2">
		<div className="text-body-secondary small">{label}</div>
		<select
			className="form-select form-select-sm"
			onChange={(event) => {
				onChange(event.target.value);
			}}
			value={value}
		>
			{/* A saved config can name an id this build of faces.js doesn't have.
			    Keep it selectable rather than silently snapping to the first option. */}
			{idsFor(slot).includes(value) ? null : (
				<option value={value}>{value}</option>
			)}
			{idsFor(slot).map((id) => (
				<option key={id} value={id}>
					{id}
				</option>
			))}
		</select>
	</label>
);

const Color = ({
	label,
	onChange,
	value,
}: {
	label: string;
	onChange: (color: string) => void;
	value: string;
}) => (
	<label className="d-block mb-2">
		<div className="text-body-secondary small">{label}</div>
		<div className="d-flex gap-2">
			<input
				className="form-control form-control-color"
				onChange={(event) => {
					onChange(event.target.value);
				}}
				type="color"
				value={value}
			/>
			<input
				className="form-control form-control-sm font-monospace"
				onChange={(event) => {
					onChange(event.target.value);
				}}
				type="text"
				value={value}
			/>
		</div>
	</label>
);

const Check = ({
	label,
	onChange,
	value,
}: {
	label: string;
	onChange: (value: boolean) => void;
	value: boolean;
}) => (
	<div className="form-check mb-2">
		<input
			checked={value}
			className="form-check-input"
			id={`face-check-${label}`}
			onChange={(event) => {
				onChange(event.target.checked);
			}}
			type="checkbox"
		/>
		<label className="form-check-label small" htmlFor={`face-check-${label}`}>
			{label}
		</label>
	</div>
);

export const PlayerFaceModal = ({
	colors,
	initialFace,
	jersey,
	name,
	onHide,
	pid,
}: {
	colors: [string, string, string] | undefined;
	// The player's current face, or undefined if he only ever had an image.
	initialFace: FaceConfig | undefined;
	jersey: string | undefined;
	name: string;
	onHide: () => void;
	pid: number;
}) => {
	// The face is edited as JSON text, and the controls write back into that same
	// text. One source of truth means pasting a config and nudging a slider are
	// the same operation, and the textarea always shows exactly what will save.
	const [text, setText] = useState(() =>
		JSON.stringify(initialFace ?? generate(), undefined, 2),
	);
	const [saving, setSaving] = useState(false);

	let face: FaceConfig | undefined;
	let parseError: string | undefined;
	try {
		face = JSON.parse(text);
	} catch (error) {
		// Curly quotes are what a config pasted out of a chat app usually trips on.
		parseError = /[‘’“”]/.test(text)
			? "Invalid JSON — replace the curly quotes with straight ones."
			: (error as Error).message;
	}

	// A control edit reads the current object, changes one field and writes the
	// whole thing back out, so the textarea stays formatted and in sync.
	const set = (path: string, value: unknown) => {
		if (!face) {
			return;
		}
		const next: Record<string, any> = { ...face };
		const [slot, field] = path.split(".");
		if (field === undefined) {
			next[slot!] = value;
		} else {
			next[slot!] = { ...next[slot!], [field]: value };
		}
		setText(JSON.stringify(next, undefined, 2));
	};

	const slider = (path: keyof typeof RANGES, label: string) => {
		const [slot, field] = path.split(".");
		const current =
			field === undefined
				? (face as any)?.[slot!]
				: (face as any)?.[slot!]?.[field];
		const [min, max, step] = RANGES[path];
		return (
			<Slider
				label={label}
				max={max}
				min={min}
				onChange={(value) => {
					set(path, value);
				}}
				step={step}
				value={typeof current === "number" ? current : min}
			/>
		);
	};

	const picker = (slot: string, label: string) => (
		<Picker
			label={label}
			onChange={(id) => {
				set(`${slot}.id`, id);
			}}
			slot={slot}
			value={(face as any)?.[slot]?.id ?? "none"}
		/>
	);

	return (
		<Modal onHide={onHide} show size="xl" scrollable>
			<Modal.Header closeButton>
				<Modal.Title>{name}</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				<div className="row g-3">
					<div className="col-12 col-md-4">
						<div
							className="position-sticky"
							style={{ top: 0, height: 300, maxWidth: 200 }}
						>
							{face ? (
								<PlayerPicture colors={colors} face={face} jersey={jersey} />
							) : null}
						</div>
					</div>
					<div className="col-12 col-md-8">
						<textarea
							className="form-control font-monospace"
							onChange={(event) => {
								setText(event.target.value);
							}}
							rows={6}
							spellCheck={false}
							style={{ fontSize: "0.8rem" }}
							value={text}
						/>
						{parseError ? (
							<div className="text-danger small mt-1">{parseError}</div>
						) : null}

						<div className="row g-3 mt-0">
							<div className="col-12 col-lg-6">
								{slider("fatness", "Fatness")}
								<Color
									label="Skin"
									onChange={(color) => {
										set("body.color", color);
									}}
									value={(face as any)?.body?.color ?? "#f2d6cb"}
								/>
								{picker("head", "Head")}
								<Slider
									label="Stubble"
									max={RANGES["head.shave"][1]}
									min={RANGES["head.shave"][0]}
									onChange={(value) => {
										set("head.shave", `rgba(0,0,0,${value})`);
									}}
									step={RANGES["head.shave"][2]}
									value={shaveAlpha((face as any)?.head?.shave)}
								/>
								{picker("hair", "Hair")}
								<Color
									label="Hair color"
									onChange={(color) => {
										set("hair.color", color);
									}}
									value={(face as any)?.hair?.color ?? "#272421"}
								/>
								<Check
									label="Flip hair"
									onChange={(value) => {
										set("hair.flip", value);
									}}
									value={!!(face as any)?.hair?.flip}
								/>
								{picker("hairBg", "Hair behind head")}
								{picker("facialHair", "Facial hair")}
								{picker("ear", "Ears")}
								{slider("ear.size", "Ear size")}
							</div>
							<div className="col-12 col-lg-6">
								{picker("eye", "Eyes")}
								{slider("eye.angle", "Eye angle")}
								{picker("eyebrow", "Eyebrows")}
								{slider("eyebrow.angle", "Eyebrow angle")}
								{picker("nose", "Nose")}
								{slider("nose.size", "Nose size")}
								<Check
									label="Flip nose"
									onChange={(value) => {
										set("nose.flip", value);
									}}
									value={!!(face as any)?.nose?.flip}
								/>
								{picker("mouth", "Mouth")}
								<Check
									label="Flip mouth"
									onChange={(value) => {
										set("mouth.flip", value);
									}}
									value={!!(face as any)?.mouth?.flip}
								/>
								{picker("eyeLine", "Eye line")}
								{picker("smileLine", "Smile line")}
								{slider("smileLine.size", "Smile line size")}
								{picker("miscLine", "Other lines")}
								{picker("glasses", "Glasses")}
								{picker("accessories", "Accessories")}
								{picker("body", "Body")}
								{slider("body.size", "Body size")}
							</div>
						</div>
					</div>
				</div>
			</Modal.Body>
			<Modal.Footer>
				<button
					className="btn btn-secondary"
					onClick={() => {
						setText(JSON.stringify(generate(), undefined, 2));
					}}
					type="button"
				>
					Randomize
				</button>
				<button className="btn btn-secondary" onClick={onHide} type="button">
					Cancel
				</button>
				<button
					className="btn btn-primary"
					disabled={!face || saving}
					onClick={async () => {
						if (!face) {
							return;
						}
						setSaving(true);
						try {
							await toWorker("main", "updatePlayerFace", { pid, face });
							updatePlayerFaceData(pid, face);
							showNotification({
								type: "success",
								text: `Face saved for ${name}.`,
							});
							onHide();
						} catch (error) {
							showNotification({
								type: "error",
								text: (error as Error).message,
							});
						} finally {
							setSaving(false);
						}
					}}
					type="button"
				>
					Save
				</button>
			</Modal.Footer>
		</Modal>
	);
};
