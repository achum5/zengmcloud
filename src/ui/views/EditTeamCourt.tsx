import { useEffect, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import type {
	View,
	CourtStyle,
	CourtImageAdjust,
	CourtImageSlot,
} from "../../common/types.ts";
import LiveCourt from "./LiveGame/LiveCourt.tsx";

const DEFAULT_FLOOR = "#c9a165";
const DEFAULT_LINES = "#f8f5f0";

// A row of a color picker + optional "use default" reset, bound to one
// CourtStyle field. Passing an empty value clears the field (fall back to
// default).
const ColorField = ({
	label,
	value,
	fallback,
	onChange,
	onClear,
	cleared,
}: {
	label: string;
	value: string;
	fallback: string;
	onChange: (value: string) => void;
	onClear?: () => void;
	cleared?: boolean;
}) => (
	<div className="mb-3">
		<label className="form-label mb-1">{label}</label>
		<div className="d-flex align-items-center gap-2">
			<input
				type="color"
				className="form-control form-control-color"
				value={cleared ? fallback : value}
				onChange={(e) => onChange(e.target.value)}
			/>
			{onClear ? (
				<button
					type="button"
					className="btn btn-sm btn-light-bordered"
					onClick={onClear}
					disabled={cleared}
				>
					Default
				</button>
			) : null}
		</div>
	</div>
);

// A text input bound to one CourtStyle field, with an optional companion color
// picker (for text-color fields shown only when the text is non-empty).
const TextField = ({
	label,
	hint,
	value,
	placeholder,
	onChange,
	colorValue,
	colorFallback,
	onColorChange,
}: {
	label: string;
	hint?: string;
	value: string;
	placeholder?: string;
	onChange: (value: string) => void;
	colorValue?: string;
	colorFallback?: string;
	onColorChange?: (value: string) => void;
}) => (
	<div className="mb-3">
		<label className="form-label mb-1">
			{label}{" "}
			{hint ? <span className="text-body-secondary">{hint}</span> : null}
		</label>
		<div className="d-flex align-items-center gap-2">
			<input
				type="text"
				className="form-control"
				value={value}
				placeholder={placeholder}
				onChange={(e) => onChange(e.target.value)}
			/>
			{onColorChange && value ? (
				<input
					type="color"
					className="form-control form-control-color flex-shrink-0"
					title="Text color"
					value={colorValue || colorFallback || "#ffffff"}
					onChange={(e) => onColorChange(e.target.value)}
				/>
			) : null}
		</div>
	</div>
);

// AN IMAGE SLOT, with the knobs that make it yours.
//
// A URL on its own is not customization - it drops the picture wherever the
// court happens to put it, at whatever size the court happens to want. The
// sliders appear once there is an image to move, so an unused slot is still
// one line.
const ImageField = ({
	label,
	hint,
	slot,
	url,
	onURL,
	adjust,
	onAdjust,
	defaultFit = "contain",
}: {
	label: string;
	hint?: string;
	slot: CourtImageSlot;
	url: string;
	onURL: (value: string) => void;
	adjust: CourtImageAdjust | undefined;
	onAdjust: (slot: CourtImageSlot, next: CourtImageAdjust | undefined) => void;
	defaultFit?: "contain" | "fill";
}) => {
	// A URL that does not resolve to an image draws nothing at all, which looks
	// exactly like the feature being broken. Load it here and say so.
	const [broken, setBroken] = useState(false);
	useEffect(() => {
		setBroken(false);
		if (!url) {
			return;
		}
		let stale = false;
		const img = new Image();
		img.onerror = () => {
			if (!stale) {
				setBroken(true);
			}
		};
		img.src = url;
		return () => {
			stale = true;
		};
	}, [url]);

	const set = <K extends keyof CourtImageAdjust>(
		key: K,
		value: CourtImageAdjust[K],
	) => {
		const next: CourtImageAdjust = { ...adjust };
		if (value === undefined) {
			delete next[key];
		} else {
			next[key] = value;
		}
		onAdjust(slot, Object.keys(next).length > 0 ? next : undefined);
	};

	const slider = (
		key: "scale" | "opacity" | "dx" | "dy" | "rotate",
		text: string,
		min: number,
		max: number,
		step: number,
		fallback: number,
		format: (value: number) => string,
	) => (
		<label className="d-flex align-items-center gap-2 mb-1 small">
			<span style={{ width: "4.5rem" }} className="text-body-secondary">
				{text}
			</span>
			<input
				type="range"
				className="form-range flex-grow-1"
				min={min}
				max={max}
				step={step}
				value={adjust?.[key] ?? fallback}
				onChange={(e) => set(key, Number.parseFloat(e.target.value))}
			/>
			<span
				style={{ width: "3.25rem" }}
				className="text-end font-monospace text-body-secondary"
			>
				{format(adjust?.[key] ?? fallback)}
			</span>
		</label>
	);

	return (
		<div className="mb-3">
			<label className="form-label mb-1">
				{label}{" "}
				{hint ? <span className="text-body-secondary">{hint}</span> : null}
			</label>
			<input
				type="text"
				className="form-control"
				value={url}
				placeholder="https://..."
				onChange={(e) => onURL(e.target.value)}
			/>
			{broken ? (
				<div className="text-danger small mt-1">
					That URL didn&rsquo;t load as an image.
				</div>
			) : null}
			{url ? (
				<div className="mt-2 ps-2 border-start">
					{slider(
						"scale",
						"Size",
						0.1,
						4,
						0.05,
						1,
						(v) => `${Math.round(v * 100)}%`,
					)}
					{slider(
						"opacity",
						"Fade",
						0,
						1,
						0.05,
						1,
						(v) => `${Math.round(v * 100)}%`,
					)}
					{slider("dx", "Left/right", -47, 47, 0.5, 0, (v) => `${v} ft`)}
					{slider("dy", "Up/down", -25, 25, 0.5, 0, (v) => `${v} ft`)}
					{slider("rotate", "Rotate", -180, 180, 5, 0, (v) => `${v}\u00b0`)}
					<div className="d-flex align-items-center gap-3 mt-1">
						<div className="form-check form-check-inline mb-0">
							<input
								className="form-check-input"
								type="checkbox"
								id={`${slot}-fill`}
								checked={(adjust?.fit ?? defaultFit) === "fill"}
								onChange={(e) =>
									set("fit", e.target.checked ? "fill" : "contain")
								}
							/>
							<label
								className="form-check-label small"
								htmlFor={`${slot}-fill`}
							>
								Stretch to fit
							</label>
						</div>
						<button
							type="button"
							className="btn btn-sm btn-light-bordered"
							onClick={() => onAdjust(slot, undefined)}
							disabled={adjust === undefined}
						>
							Reset
						</button>
					</div>
				</div>
			) : null}
		</div>
	);
};

const EditTeamCourt = ({
	tid,
	abbrev,
	region,
	name,
	colors,
	imgURL,
	court,
}: View<"editTeamCourt">) => {
	useTitleBar({
		title: `Customize Court`,
		customMenu: undefined,
	});

	const [style, setStyle] = useState<CourtStyle>(court ?? {});
	const [previewFinals, setPreviewFinals] = useState(false);
	const [saving, setSaving] = useState(false);

	// One image slot's size/position knobs.
	const setAdjust = (
		slot: CourtImageSlot,
		next: CourtImageAdjust | undefined,
	) => {
		setStyle((s) => {
			const adjust = { ...s.adjust };
			if (next === undefined) {
				delete adjust[slot];
			} else {
				adjust[slot] = next;
			}
			const out = { ...s };
			if (Object.keys(adjust).length > 0) {
				out.adjust = adjust;
			} else {
				delete out.adjust;
			}
			return out;
		});
	};

	const set = <K extends keyof CourtStyle>(key: K, value: CourtStyle[K]) => {
		setStyle((s) => {
			const next = { ...s };
			if (value === undefined || value === "") {
				delete next[key];
			} else {
				next[key] = value;
			}
			return next;
		});
	};

	const homeTeam = {
		tid,
		abbrev,
		region,
		name,
		colors,
		imgURL,
		court: style,
	};

	const save = async () => {
		setSaving(true);
		try {
			await toWorker("main", "updateTeamCourt", {
				tid,
				court: Object.keys(style).length > 0 ? style : undefined,
			});
			showNotification({
				type: "success",
				text: "Court saved.",
			});
			realtimeUpdate([], helpers.leagueUrl(["manage_teams"]));
		} catch (error) {
			showNotification({
				type: "error",
				text: `Could not save court: ${(error as Error).message}`,
				persistent: true,
			});
		} finally {
			setSaving(false);
		}
	};

	return (
		<>
			<p className="text-body-secondary">
				Design the {region} {name} home court shown during live game
				simulations. It's saved on the team, so everyone in a multiplayer league
				sees it.
			</p>

			<div className="row">
				<div className="col-lg-7 mb-3">
					<LiveCourt
						scene={undefined}
						teams={[undefined, homeTeam]}
						finals={previewFinals}
						season={undefined}
						sceneMs={undefined}
					/>
					<div className="form-check">
						<input
							type="checkbox"
							className="form-check-input"
							id="preview-finals"
							checked={previewFinals}
							onChange={(e) => setPreviewFinals(e.target.checked)}
						/>
						<label className="form-check-label" htmlFor="preview-finals">
							Preview championship (finals) look
						</label>
					</div>
				</div>

				<div className="col-lg-5">
					<ColorField
						label="Floor color"
						value={style.floor ?? DEFAULT_FLOOR}
						fallback={DEFAULT_FLOOR}
						cleared={style.floor === undefined}
						onChange={(v) => set("floor", v)}
						onClear={() => set("floor", undefined)}
					/>

					<div className="mb-3">
						<label className="form-label mb-1">Floor pattern</label>
						<select
							className="form-select"
							value={style.floorPattern ?? "hardwood"}
							onChange={(e) =>
								set(
									"floorPattern",
									e.target.value as CourtStyle["floorPattern"],
								)
							}
						>
							<option value="hardwood">Hardwood planks</option>
							<option value="parquet">Parquet (basketweave)</option>
							<option value="diagonal">Diagonal planks</option>
							<option value="chevron">Chevron</option>
							<option value="solid">Solid</option>
						</select>
					</div>

					<ColorField
						label="Line color"
						value={style.lines ?? DEFAULT_LINES}
						fallback={DEFAULT_LINES}
						cleared={style.lines === undefined}
						onChange={(v) => set("lines", v)}
						onClear={() => set("lines", undefined)}
					/>

					<div className="mb-3">
						<div className="form-check mb-1">
							<input
								type="checkbox"
								className="form-check-input"
								id="paint-key"
								checked={style.paint !== undefined}
								onChange={(e) =>
									set("paint", e.target.checked ? colors[0] : undefined)
								}
							/>
							<label className="form-check-label" htmlFor="paint-key">
								Painted key (colored lane)
							</label>
						</div>
						{style.paint !== undefined ? (
							<input
								type="color"
								className="form-control form-control-color"
								value={style.paint}
								onChange={(e) => set("paint", e.target.value)}
							/>
						) : null}
					</div>

					<ColorField
						label="Rail / sideline color"
						value={style.apron ?? colors[0]}
						fallback={colors[0]}
						cleared={style.apron === undefined}
						onChange={(v) => set("apron", v)}
						onClear={() => set("apron", undefined)}
					/>

					<ColorField
						label="Rail text color"
						value={style.apronText ?? colors[1]}
						fallback={colors[1]}
						cleared={style.apronText === undefined}
						onChange={(v) => set("apronText", v)}
						onClear={() => set("apronText", undefined)}
					/>

					<ImageField
						label="Center logo"
						hint="(blank = team logo)"
						slot="logo"
						url={style.logoURL ?? ""}
						onURL={(v) => set("logoURL", v)}
						adjust={style.adjust?.logo}
						onAdjust={setAdjust}
					/>

					<ImageField
						label="Championship trophy"
						hint="(center court, finals look)"
						slot="trophy"
						url={style.trophyURL ?? ""}
						onURL={(v) => set("trophyURL", v)}
						adjust={style.adjust?.trophy}
						onAdjust={setAdjust}
					/>

					<ImageField
						label="Secondary logo"
						hint="(one in each half)"
						slot="secondary"
						url={style.secondaryLogoURL ?? ""}
						onURL={(v) => set("secondaryLogoURL", v)}
						adjust={style.adjust?.secondary}
						onAdjust={setAdjust}
					/>

					<ImageField
						label="Sideline banner"
						hint="(runs along both sidelines)"
						slot="sideline"
						url={style.sidelineImageURL ?? ""}
						onURL={(v) => set("sidelineImageURL", v)}
						adjust={style.adjust?.sideline}
						onAdjust={setAdjust}
						defaultFit="fill"
					/>

					<hr />
					<h3 className="h6 text-body-secondary">Baselines</h3>

					<ImageField
						label="Baseline rail image"
						hint="(the strip where the team name is)"
						slot="rail"
						url={style.railImageURL ?? ""}
						onURL={(v) => set("railImageURL", v)}
						adjust={style.adjust?.rail}
						onAdjust={setAdjust}
						defaultFit="fill"
					/>

					<div className="form-check mb-3">
						<input
							className="form-check-input"
							type="checkbox"
							id="hide-rail-text"
							checked={style.hideRailText ?? false}
							onChange={(e) =>
								set("hideRailText", e.target.checked ? true : undefined)
							}
							disabled={!!style.railImageURL}
						/>
						<label className="form-check-label" htmlFor="hide-rail-text">
							Hide team name on the rails
						</label>
					</div>

					<ImageField
						label="Baseline floor logo"
						hint="(on the floor in each backcourt)"
						slot="baseline"
						url={style.baselineImageURL ?? ""}
						onURL={(v) => set("baselineImageURL", v)}
						adjust={style.adjust?.baseline}
						onAdjust={setAdjust}
					/>

					<hr />
					<h3 className="h6 text-body-secondary">Arena-floor details</h3>

					<TextField
						label="Center-court script text"
						hint="(above the logo, e.g. \u201cThe Finals\u201d)"
						value={style.centerText ?? ""}
						placeholder="The Finals"
						onChange={(v) => set("centerText", v)}
						colorValue={style.centerTextColor}
						colorFallback={style.apron ?? colors[0]}
						onColorChange={(v) => set("centerTextColor", v)}
					/>

					<ImageField
						label="Quarter-court logo"
						hint="(repeated in the four corners)"
						slot="corner"
						url={style.cornerLogoURL ?? ""}
						onURL={(v) => set("cornerLogoURL", v)}
						adjust={style.adjust?.corner}
						onAdjust={setAdjust}
					/>

					<ImageField
						label="Bench banner"
						hint="(along the bench sideline only)"
						slot="bench"
						url={style.benchImageURL ?? ""}
						onURL={(v) => set("benchImageURL", v)}
						adjust={style.adjust?.bench}
						onAdjust={setAdjust}
						defaultFit="fill"
					/>

					<TextField
						label="Bench sponsor text"
						hint="(e.g. “celtics.com”)"
						value={style.benchText ?? ""}
						placeholder="celtics.com"
						onChange={(v) => set("benchText", v)}
						colorValue={style.benchTextColor}
						colorFallback={style.apronText ?? colors[1]}
						onColorChange={(v) => set("benchTextColor", v)}
					/>

					<div className="d-flex gap-2 mt-3">
						<button
							type="button"
							className="btn btn-primary"
							onClick={save}
							disabled={saving}
						>
							Save court
						</button>
						<button
							type="button"
							className="btn btn-light-bordered"
							onClick={() => setStyle({})}
							disabled={saving}
						>
							Reset to default
						</button>
					</div>
				</div>
			</div>
		</>
	);
};

export default EditTeamCourt;
