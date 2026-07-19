import { useMemo, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import type { View, CourtStyle } from "../../common/types.ts";
import LiveCourt, {
	synthShotSpot,
	DEFAULT_TROPHY_URL,
	type CourtDot,
} from "./LiveGame/LiveCourt.tsx";

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

	// A fixed sample shot chart so the preview shows a realistic court, generated
	// once (positions are random but frozen for a stable preview).
	const dots = useMemo<CourtDot[]>(() => {
		const zones = ["atRim", "lowPost", "midRange", "three"] as const;
		const out: CourtDot[] = [];
		let key = 0;
		for (const t of [0, 1] as const) {
			for (let i = 0; i < 14; i++) {
				const zone = zones[i % zones.length]!;
				const { x, y } = synthShotSpot(t, zone);
				out.push({
					key: key++,
					x,
					y,
					made: Math.random() < 0.45,
					t,
					title: "",
				});
			}
		}
		return out;
	}, []);

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
						dots={dots}
						teams={[undefined, homeTeam]}
						finals={previewFinals}
						season={undefined}
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

					<div className="mb-3">
						<label className="form-label mb-1">
							Center logo URL{" "}
							<span className="text-body-secondary">(blank = team logo)</span>
						</label>
						<input
							type="text"
							className="form-control"
							value={style.logoURL ?? ""}
							placeholder={imgURL ?? "https://…"}
							onChange={(e) => set("logoURL", e.target.value)}
						/>
					</div>

					<div className="mb-3">
						<label className="form-label mb-1">
							Championship trophy URL{" "}
							<span className="text-body-secondary">(blank = default)</span>
						</label>
						<input
							type="text"
							className="form-control"
							value={style.trophyURL ?? ""}
							placeholder={DEFAULT_TROPHY_URL}
							onChange={(e) => set("trophyURL", e.target.value)}
						/>
					</div>

					<div className="mb-3">
						<label className="form-label mb-1">
							Secondary logo URL{" "}
							<span className="text-body-secondary">(shown in each half)</span>
						</label>
						<input
							type="text"
							className="form-control"
							value={style.secondaryLogoURL ?? ""}
							placeholder="https://…"
							onChange={(e) => set("secondaryLogoURL", e.target.value)}
						/>
					</div>

					<div className="mb-3">
						<label className="form-label mb-1">
							Sideline image URL{" "}
							<span className="text-body-secondary">
								(runs along each sideline)
							</span>
						</label>
						<input
							type="text"
							className="form-control"
							value={style.sidelineImageURL ?? ""}
							placeholder="https://… (wide banner)"
							onChange={(e) => set("sidelineImageURL", e.target.value)}
						/>
					</div>

					<hr />
					<h3 className="h6 text-body-secondary">Arena-floor details</h3>

					<TextField
						label="Center-court script text"
						hint="(above the logo, e.g. “The Finals”)"
						value={style.centerText ?? ""}
						placeholder="The Finals"
						onChange={(v) => set("centerText", v)}
						colorValue={style.centerTextColor}
						colorFallback={style.apron ?? colors[0]}
						onColorChange={(v) => set("centerTextColor", v)}
					/>

					<TextField
						label="Baseline logo URL"
						hint="(behind each baseline)"
						value={style.baselineImageURL ?? ""}
						placeholder="https://…"
						onChange={(v) => set("baselineImageURL", v)}
					/>

					<TextField
						label="Quarter-court logo URL"
						hint="(repeated in the four corners)"
						value={style.cornerLogoURL ?? ""}
						placeholder="https://…"
						onChange={(v) => set("cornerLogoURL", v)}
					/>

					<TextField
						label="Bench banner image URL"
						hint="(along the bench sideline only)"
						value={style.benchImageURL ?? ""}
						placeholder="https://… (wide banner)"
						onChange={(v) => set("benchImageURL", v)}
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
