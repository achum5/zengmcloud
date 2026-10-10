import { useMemo, useState } from "react";
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
import {
	ImageField,
	PIC,
	pictureFromFile,
} from "../components/CourtImageField.tsx";

const DEFAULT_FLOOR = "#c9a165";
const DEFAULT_LINES = "#f8f5f0";

const PICTURE_KEYS = [
	"logoURL",
	"trophyURL",
	"secondaryLogoURL",
	"sidelineImageURL",
	"baselineImageURL",
	"cornerLogoURL",
	"benchImageURL",
	"railImageURL",
] as const satisfies readonly (keyof CourtStyle)[];

// The court as drawn: its uploaded pictures filled in.
const withPictures = (
	style: CourtStyle,
	pictures: Record<string, string>,
): CourtStyle => {
	const out = { ...style };
	for (const key of PICTURE_KEYS) {
		const v = out[key];
		if (typeof v === "string" && v.startsWith(PIC)) {
			const url = pictures[v.slice(PIC.length)];
			if (url === undefined) {
				delete out[key];
			} else {
				out[key] = url;
			}
		}
	}
	return out;
};

const STYLE_STRINGS = new Set<string>([
	"floor",
	"lines",
	"paint",
	"apron",
	"apronText",
	"centerText",
	"centerTextColor",
	"benchText",
	"benchTextColor",
	...PICTURE_KEYS,
]);
const PATTERNS = new Set([
	"hardwood",
	"parquet",
	"diagonal",
	"chevron",
	"solid",
]);
const SLOTS = new Set<string>([
	"logo",
	"trophy",
	"secondary",
	"sideline",
	"bench",
	"baseline",
	"corner",
	"rail",
]);

// A court typed or pasted in as JSON, checked field by field - or why not.
const parseCourt = (text: string): CourtStyle | string => {
	let raw: unknown;
	try {
		raw = JSON.parse(text);
	} catch {
		return "Not valid JSON.";
	}
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
		return "Expected a JSON object.";
	}
	for (const [key, value] of Object.entries(raw)) {
		if (STYLE_STRINGS.has(key)) {
			if (typeof value !== "string") {
				return `"${key}" should be text.`;
			}
		} else if (key === "floorPattern") {
			if (typeof value !== "string" || !PATTERNS.has(value)) {
				return `"floorPattern" should be one of ${[...PATTERNS].join(", ")}.`;
			}
		} else if (key === "hideRailText") {
			if (typeof value !== "boolean") {
				return `"hideRailText" should be true or false.`;
			}
		} else if (key === "adjust") {
			if (typeof value !== "object" || value === null) {
				return `"adjust" should be an object.`;
			}
			for (const [slot, a] of Object.entries(value)) {
				if (!SLOTS.has(slot)) {
					return `Unknown slot "${slot}" in "adjust".`;
				}
				if (typeof a !== "object" || a === null) {
					return `"adjust.${slot}" should be an object.`;
				}
				for (const [k, v] of Object.entries(a)) {
					if (k === "fit") {
						if (v !== "contain" && v !== "fill") {
							return `"adjust.${slot}.fit" should be "contain" or "fill".`;
						}
					} else if (
						!["scale", "opacity", "dx", "dy", "rotate"].includes(k) ||
						typeof v !== "number" ||
						!Number.isFinite(v)
					) {
						return `"adjust.${slot}.${k}" isn't a number setting.`;
					}
				}
			}
		} else {
			return `Unknown field "${key}".`;
		}
	}
	return raw as CourtStyle;
};

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

const EditTeamCourt = ({
	tid,
	abbrev,
	region,
	name,
	colors,
	imgURL,
	court,
	pictures: picturesSaved,
}: View<"editTeamCourt">) => {
	useTitleBar({
		title: `Customize Court`,
		customMenu: undefined,
	});

	const [style, setStyle] = useState<CourtStyle>(court ?? {});
	const [previewFinals, setPreviewFinals] = useState(false);
	const [saving, setSaving] = useState(false);
	// Pictures by id: the court's own, and any uploaded since.
	const [pictures, setPictures] =
		useState<Record<string, string>>(picturesSaved);
	const [uploaded, setUploaded] = useState<string[]>([]);
	const [json, setJSON] = useState<string | undefined>();
	const [jsonError, setJSONError] = useState<string | undefined>();

	const upload = async (file: File) => {
		try {
			const url = await pictureFromFile(file);
			const id = await toWorker("main", "storeCourtPicture", url);
			setPictures((p) => ({ ...p, [id]: url }));
			setUploaded((u) => [...u, id]);
			return `${PIC}${id}`;
		} catch (error) {
			showNotification({
				type: "error",
				text: `Could not upload: ${(error as Error).message}`,
			});
			return undefined;
		}
	};
	const picture = { pictures, onUpload: upload };

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

	const shownStyle = useMemo(
		() => withPictures(style, pictures),
		[style, pictures],
	);
	const homeTeam = {
		tid,
		abbrev,
		region,
		name,
		colors,
		imgURL,
		court: shownStyle,
	};

	const save = async () => {
		setSaving(true);
		try {
			await toWorker("main", "updateTeamCourt", {
				tid,
				court: Object.keys(style).length > 0 ? style : undefined,
				uploaded,
			});
			setUploaded([]);
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
						{...picture}
					/>

					<ImageField
						label="Championship trophy"
						hint="(center court, finals look)"
						slot="trophy"
						url={style.trophyURL ?? ""}
						onURL={(v) => set("trophyURL", v)}
						adjust={style.adjust?.trophy}
						onAdjust={setAdjust}
						{...picture}
					/>

					<ImageField
						label="Secondary logo"
						hint="(one in each half)"
						slot="secondary"
						url={style.secondaryLogoURL ?? ""}
						onURL={(v) => set("secondaryLogoURL", v)}
						adjust={style.adjust?.secondary}
						onAdjust={setAdjust}
						{...picture}
					/>

					<ImageField
						label="Sideline banner"
						hint="(runs along both sidelines)"
						slot="sideline"
						url={style.sidelineImageURL ?? ""}
						onURL={(v) => set("sidelineImageURL", v)}
						adjust={style.adjust?.sideline}
						onAdjust={setAdjust}
						{...picture}
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
						{...picture}
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
						{...picture}
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
						{...picture}
					/>

					<ImageField
						label="Bench banner"
						hint="(along the bench sideline only)"
						slot="bench"
						url={style.benchImageURL ?? ""}
						onURL={(v) => set("benchImageURL", v)}
						adjust={style.adjust?.bench}
						onAdjust={setAdjust}
						{...picture}
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

					<hr />
					<button
						type="button"
						className="btn btn-sm btn-light-bordered"
						onClick={() => {
							setJSONError(undefined);
							setJSON(
								json === undefined ? JSON.stringify(style, null, 2) : undefined,
							);
						}}
					>
						{json === undefined ? "Edit as JSON" : "Close JSON"}
					</button>
					{json !== undefined ? (
						<div className="mt-2">
							<textarea
								className="form-control font-monospace small"
								rows={12}
								spellCheck={false}
								value={json}
								onChange={(e) => setJSON(e.target.value)}
							/>
							{jsonError ? (
								<div className="text-danger small mt-1">{jsonError}</div>
							) : null}
							<button
								type="button"
								className="btn btn-sm btn-secondary mt-2"
								onClick={() => {
									const parsed = parseCourt(json);
									if (typeof parsed === "string") {
										setJSONError(parsed);
									} else {
										setJSONError(undefined);
										setStyle(parsed);
									}
								}}
							>
								Apply
							</button>
						</div>
					) : null}

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
