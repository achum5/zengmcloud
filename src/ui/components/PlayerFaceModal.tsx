import { useDeferredValue, useMemo, useState } from "react";
import type { FaceConfig, Overrides } from "facesjs";
import { generate, svgsIndex } from "facesjs";
import { Face } from "facesjs/react";
import clsx from "clsx";
import { Modal } from "./Modal.tsx";
import { PlayerPicture } from "./PlayerPicture.tsx";
import { toWorker } from "../util/toWorker.ts";
import { showNotification } from "../util/showNotification.ts";
import { updatePlayerFaceData } from "../util/playerFaces.ts";
import { FACE_CROPS, FULL_FACE } from "../../common/faceCrops.ts";
import { FACE_FROM_PHOTO_PROMPT } from "../../common/faceFromPhotoPrompt.ts";

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

// facesjs's own palette anchors, same list the photo-conversion prompt hands the
// model (tools/faceFromPhoto/PROMPT.md). Start from the nearest one and nudge.
const SKIN_COLORS = [
	"#f2d6cb",
	"#ddb7a0",
	"#fedac7",
	"#f0c5a3",
	"#eab687",
	"#bb876f",
	"#aa816f",
	"#a67358",
	"#ad6453",
	"#74453d",
	"#5c3937",
];

const HAIR_COLORS = [
	"#272421",
	"#0f0902",
	"#1c1008",
	"#3d2314",
	"#2c1608",
	"#5a3825",
	"#cc9966",
	"#b55239",
	"#e9c67b",
	"#d7bf91",
	"#9a9a9a",
	"#c8c8c8",
	"#e8e8e8",
];

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

// The face draws at a fixed 400x600 viewBox, so a crop is a window onto a
// scaled-up copy of the whole thing: blow the face up by `scale`, shift it so
// the feature lands at the origin, and clip to the feature's size.
const THUMB_HEIGHT = 74;

const Thumb = ({
	crop,
	face,
	overrides,
}: {
	crop: [number, number, number, number];
	face: FaceConfig;
	overrides: Overrides | undefined;
}) => {
	const [x, y, w, h] = crop;
	const scale = THUMB_HEIGHT / h;

	return (
		<div
			className="overflow-hidden position-relative"
			style={{ height: THUMB_HEIGHT, width: Math.round(w * scale) }}
		>
			<Face
				face={face}
				ignoreDisplayErrors
				// Only the thumbnails scrolled into view are drawn - facesjs watches
				// each one with an IntersectionObserver, and the gallery is its own
				// scroll box, so a slot with 80 options costs about a screenful.
				lazy
				overrides={overrides}
				style={{
					height: 600 * scale,
					left: -x * scale,
					position: "absolute",
					top: -y * scale,
					width: 400 * scale,
				}}
			/>
		</div>
	);
};

// Every option for one slot, drawn on THIS player and cropped to the feature,
// so picking a nose is a matter of looking at noses rather than reading
// "nose11" and guessing.
const Gallery = ({
	base,
	colors,
	jersey,
	onPick,
	slot,
	value,
}: {
	base: FaceConfig;
	colors: [string, string, string] | undefined;
	jersey: string | undefined;
	onPick: (id: string) => void;
	slot: string;
	value: string;
}) => {
	const crop = FACE_CROPS[slot] ?? FULL_FACE;

	// facesjs redraws a face whenever either object's identity changes, so both
	// have to be stable across renders or every keystroke repaints the strip.
	const rest = JSON.stringify({ ...base, [slot]: undefined });
	const faces = useMemo(
		() =>
			idsFor(slot).map((id) => ({
				id,
				face: {
					...base,
					[slot]: { ...(base as any)[slot], id },
				} as FaceConfig,
			})),
		// `rest` stands in for `base`: the slot being varied is overwritten per
		// option anyway, so a change to it must not rebuild the whole strip.
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[slot, rest],
	);
	const overrides = useMemo(
		() =>
			colors || jersey
				? {
						...(colors ? { teamColors: colors } : {}),
						...(jersey ? { jersey: { id: jersey } } : {}),
					}
				: undefined,
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[colors?.[0], colors?.[1], colors?.[2], jersey],
	);

	return (
		<div
			className="d-flex flex-wrap gap-1 overflow-auto small-scrollbar border rounded p-1 mb-2"
			style={{ maxHeight: 260 }}
		>
			{faces.map((option) => (
				<button
					className={clsx(
						"btn btn-sm p-0 border",
						option.id === value && "border-primary border-2",
					)}
					key={option.id}
					onClick={() => {
						onPick(option.id);
					}}
					title={option.id}
					type="button"
				>
					<Thumb crop={crop} face={option.face} overrides={overrides} />
				</button>
			))}
		</div>
	);
};

const Picker = ({
	base,
	colors,
	jersey,
	label,
	onChange,
	onToggle,
	open,
	slot,
	value,
}: {
	base: FaceConfig | undefined;
	colors: [string, string, string] | undefined;
	jersey: string | undefined;
	label: string;
	onChange: (id: string) => void;
	onToggle: () => void;
	open: boolean;
	slot: string;
	value: string;
}) => (
	<div className="mb-2">
		<div className="text-body-secondary small">{label}</div>
		<div className="d-flex gap-1">
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
			<button
				aria-expanded={open}
				className={clsx(
					"btn btn-sm flex-shrink-0",
					open ? "btn-primary" : "btn-light-bordered",
				)}
				disabled={!base}
				onClick={onToggle}
				title={`Show every ${label.toLowerCase()}`}
				type="button"
			>
				<span className="glyphicon glyphicon-th" />
			</button>
		</div>
		{open && base ? (
			<Gallery
				base={base}
				colors={colors}
				jersey={jersey}
				onPick={onChange}
				slot={slot}
				value={value}
			/>
		) : null}
	</div>
);

const Color = ({
	label,
	onChange,
	presets,
	value,
}: {
	label: string;
	onChange: (color: string) => void;
	presets: string[];
	value: string;
}) => (
	<div className="mb-2">
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
		<div className="d-flex flex-wrap gap-1 mt-1">
			{presets.map((preset) => (
				<button
					className={clsx(
						"btn btn-sm p-0 border",
						preset.toLowerCase() === value.toLowerCase() &&
							"border-primary border-2",
					)}
					key={preset}
					onClick={() => {
						onChange(preset);
					}}
					style={{ backgroundColor: preset, height: 22, width: 22 }}
					title={preset}
					type="button"
				/>
			))}
		</div>
	</div>
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
	imgURL,
	initialFace,
	jersey,
	name,
	onHide,
	pid,
}: {
	colors: [string, string, string] | undefined;
	// The photo currently set on the player, if any. Shown beside the face so
	// there's something to match against while editing - and saving a face
	// replaces it, so this is the last look at what's being given up.
	imgURL: string | undefined;
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
	const [openSlot, setOpenSlot] = useState<string>();
	// Stays "Copied" for the rest of the modal's life. It's a confirmation, not
	// a mode, and a label that flips back after a couple of seconds just makes
	// you wonder whether it worked.
	const [copied, setCopied] = useState(false);

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

	// Redrawing a whole strip of thumbnails costs real time, so let it lag a
	// slider drag rather than stall it. The live preview above is never deferred.
	const deferredText = useDeferredValue(text);
	const galleryBase = useMemo(() => {
		try {
			return JSON.parse(deferredText) as FaceConfig;
		} catch {
			return undefined;
		}
	}, [deferredText]);

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

	// One gallery open at a time: they're tall, and two of them push the live
	// preview off screen.
	const picker = (slot: string, label: string) => (
		<Picker
			base={galleryBase}
			colors={colors}
			jersey={jersey}
			label={label}
			onChange={(id) => {
				set(`${slot}.id`, id);
			}}
			onToggle={() => {
				setOpenSlot((previous) => (previous === slot ? undefined : slot));
			}}
			open={openSlot === slot}
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
						<div className="position-sticky" style={{ top: 0, maxWidth: 200 }}>
							<div style={{ height: 300 }}>
								{face ? (
									<PlayerPicture colors={colors} face={face} jersey={jersey} />
								) : null}
							</div>
							{imgURL ? (
								<>
									<div className="text-body-secondary small mt-2">
										Current photo
									</div>
									<img
										alt=""
										src={imgURL}
										style={{ maxHeight: 300, maxWidth: "100%" }}
									/>
								</>
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
									presets={SKIN_COLORS}
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
									presets={HAIR_COLORS}
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
					className="btn btn-secondary me-auto"
					onClick={async () => {
						try {
							await navigator.clipboard.writeText(FACE_FROM_PHOTO_PROMPT);
							setCopied(true);
						} catch {
							showNotification({
								type: "error",
								text: "Couldn't write to the clipboard.",
							});
						}
					}}
					type="button"
				>
					{copied ? "Copied" : "Copy prompt"}
				</button>
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
